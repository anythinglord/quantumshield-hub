#!/usr/bin/env bash
#
# gen-certs.sh — Local hybrid-PQC mTLS test PKI generator.
#
# ROADMAP Phase 1, Task 1.4. Generates a 3-level certificate chain
# (root CA -> intermediate CA (pathlen:0) -> server + client TLS 1.3 leaves)
# signed with ML-DSA-65 by default (falling back to Ed25519 on hosts without
# native ML-DSA support).
#
# ML-DSA private keys are emitted in seed-only PKCS#8 form
# (ml-dsa.output_formats=seed-only) so that Go 1.27 crypto/x509 can parse them
# for X25519MLKEM768 mTLS. No digest flags are passed (ML-DSA / Ed25519
# determine their own hash internally).
#
# Output layout (under $CERTS_DIR, default ./certs):
#   ca/root-ca.crt|.key               self-signed root CA (CA:TRUE)
#   ca/intermediate-ca.crt|.key       intermediate CA (CA:TRUE, pathlen:0)
#   ca/root-ca.srl                    shared signing serial file
#   server/server.crt|.key            server leaf (serverAuth)
#   server/server-chain.crt           server leaf + intermediate bundle
#   client/client.crt|.key            client leaf (clientAuth)
#   client/client-chain.crt           client leaf + intermediate bundle
#
# Environment overrides:
#   CERTS_DIR          output directory                (default ./certs)
#   SIG_ALG            mldsa-65 | ed25519              (default mldsa-65)
#   ROOT_DAYS          root CA validity in days        (default 3650)
#   INTERMEDIATE_DAYS  intermediate validity in days   (default 1825)
#   LEAF_DAYS          leaf validity in days           (default 825)
#   FORCE              1 = wipe and regenerate         (default unset)
#   SERVER_SANS        OpenSSL SAN list for server leaf (default: see below)
#
# Idempotency: with no FORCE, an existing non-expired PKI is left untouched and
# the script exits 0. FORCE=1 regenerates everything from scratch. If any cert
# has expired, the script self-heals by regenerating the whole PKI.

set -euo pipefail
umask 077

# --- Configuration -----------------------------------------------------------

CERTS_DIR="${CERTS_DIR:-./certs}"
# Detect whether the caller provided SIG_ALG explicitly (even as empty) so we
# can distinguish "user asked for mldsa-65" from "default applied" during the
# ML-DSA feature-detection fallback below.
SIG_ALG_WAS_SET=0
if [ -n "${SIG_ALG+x}" ]; then
	SIG_ALG_WAS_SET=1
fi
SIG_ALG="${SIG_ALG:-mldsa-65}"
ROOT_DAYS="${ROOT_DAYS:-3650}"
INTERMEDIATE_DAYS="${INTERMEDIATE_DAYS:-1825}"
LEAF_DAYS="${LEAF_DAYS:-825}"
FORCE="${FORCE:-}"

DEFAULT_SERVER_SANS="DNS:localhost,DNS:ingestion-service,DNS:ingestion-service.quantumshield.svc.cluster.local,DNS:auth-service,DNS:auth-service.quantumshield.svc.cluster.local,DNS:device-simulator,IP:127.0.0.1,IP:::1"
SERVER_SANS="${SERVER_SANS:-$DEFAULT_SERVER_SANS}"

CA_DIR="$CERTS_DIR/ca"
SERVER_DIR="$CERTS_DIR/server"
CLIENT_DIR="$CERTS_DIR/client"

ROOT_CN="QuantumShield Root CA"
INTERMEDIATE_CN="QuantumShield Intermediate CA"
SERVER_CN="ingestion-service"
CLIENT_CN="device-client"
ORG="QuantumShield"

# --- Preconditions -----------------------------------------------------------

if ! command -v openssl >/dev/null 2>&1; then
	echo "ERROR: openssl not found in PATH" >&2
	exit 1
fi

# --- Signature algorithm resolution & feature detection ----------------------

mldsa_available=0
if openssl list -signature-algorithms 2>/dev/null | grep -qi 'ML-DSA'; then
	mldsa_available=1
fi

case "$SIG_ALG" in
mldsa-65 | mldsa65 | ML-DSA-65)
	if [ "$mldsa_available" -eq 1 ]; then
		KEY_ALG="ML-DSA-65"
		SIG_ALG="mldsa-65"
	elif [ "$SIG_ALG_WAS_SET" -eq 1 ]; then
		echo "ERROR: SIG_ALG=mldsa-65 was requested explicitly but this OpenSSL build has no ML-DSA support." >&2
		echo "       Either upgrade OpenSSL (>= 3.5) or set SIG_ALG=ed25519." >&2
		exit 1
	else
		echo "WARNING: ML-DSA unavailable, using ed25519" >&2
		KEY_ALG="ED25519"
		SIG_ALG="ed25519"
	fi
	;;
ed25519 | ED25519)
	KEY_ALG="ED25519"
	SIG_ALG="ed25519"
	;;
*)
	echo "ERROR: unsupported SIG_ALG='$SIG_ALG' (expected 'mldsa-65' or 'ed25519')" >&2
	exit 1
	;;
esac

# --- Helpers -----------------------------------------------------------------

# gen_key <outfile> — generate a private key for the resolved algorithm,
# seed-only for ML-DSA so Go 1.27 can parse it.
gen_key() {
	local out="$1"
	if [ "$KEY_ALG" = "ML-DSA-65" ]; then
		openssl genpkey -algorithm ML-DSA-65 \
			-provparam ml-dsa.output_formats=seed-only -out "$out"
	else
		openssl genpkey -algorithm ED25519 -out "$out"
	fi
}

# gen_csr <key> <subject> <out>
gen_csr() {
	local key="$1" subj="$2" out="$3"
	openssl req -new -key "$key" -subj "$subj" -out "$out" 2>/dev/null
}

# sign_cert <csr> <ca-crt> <ca-key> <ext-section> <days> <out>
sign_cert() {
	local csr="$1" ca_crt="$2" ca_key="$3" section="$4" days="$5" out="$6"
	openssl x509 -req -in "$csr" \
		-CA "$ca_crt" -CAkey "$ca_key" \
		-CAserial "$CA_DIR/root-ca.srl" -CAcreateserial \
		-days "$days" \
		-extfile "$EXT_FILE" -extensions "$section" \
		-out "$out" 2>/dev/null
}

# pki_is_valid — true when every expected artifact exists and no cert expired.
pki_is_valid() {
	local f
	for f in \
		"$CA_DIR/root-ca.crt" "$CA_DIR/root-ca.key" \
		"$CA_DIR/intermediate-ca.crt" "$CA_DIR/intermediate-ca.key" \
		"$SERVER_DIR/server.crt" "$SERVER_DIR/server-chain.crt" "$SERVER_DIR/server.key" \
		"$CLIENT_DIR/client.crt" "$CLIENT_DIR/client-chain.crt" "$CLIENT_DIR/client.key"; do
		[ -f "$f" ] || return 1
	done
	for f in "$CA_DIR/root-ca.crt" "$CA_DIR/intermediate-ca.crt" \
		"$SERVER_DIR/server.crt" "$CLIENT_DIR/client.crt"; do
		openssl x509 -in "$f" -checkend 0 -noout >/dev/null 2>&1 || return 1
	done
	return 0
}

# --- Idempotency gate --------------------------------------------------------

if [ "$FORCE" = "1" ]; then
	echo "FORCE=1: regenerating PKI under $CERTS_DIR"
	rm -rf "$CA_DIR" "$SERVER_DIR" "$CLIENT_DIR"
elif pki_is_valid; then
	echo "PKI already exists and is valid under $CERTS_DIR — skipping (set FORCE=1 to regenerate)."
	exit 0
else
	# Missing or expired material: clear stale artifacts and self-heal.
	rm -rf "$CA_DIR" "$SERVER_DIR" "$CLIENT_DIR"
fi

mkdir -p "$CA_DIR" "$SERVER_DIR" "$CLIENT_DIR"

# --- Scratch space & extension config ----------------------------------------

TMP_DIR="$(mktemp -d)"
readonly TMP_DIR
trap 'rm -rf "$TMP_DIR"' EXIT
EXT_FILE="$TMP_DIR/extensions.cnf"

cat >"$EXT_FILE" <<EOF
[intermediate_ext]
basicConstraints = critical,CA:TRUE,pathlen:0
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always

[server_ext]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth
subjectAltName = $SERVER_SANS
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always

[client_ext]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = clientAuth
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
EOF

# --- Root CA (self-signed) ---------------------------------------------------

echo "Generating root CA ($ROOT_CN, $KEY_ALG, ${ROOT_DAYS}d)..."
gen_key "$CA_DIR/root-ca.key"
openssl req -x509 -new -key "$CA_DIR/root-ca.key" \
	-subj "/CN=$ROOT_CN/O=$ORG" \
	-days "$ROOT_DAYS" \
	-addext "basicConstraints=critical,CA:TRUE" \
	-addext "keyUsage=critical,keyCertSign,cRLSign" \
	-addext "subjectKeyIdentifier=hash" \
	-out "$CA_DIR/root-ca.crt" 2>/dev/null

# --- Intermediate CA (signed by root, pathlen:0) -----------------------------

echo "Generating intermediate CA ($INTERMEDIATE_CN, ${INTERMEDIATE_DAYS}d)..."
gen_key "$CA_DIR/intermediate-ca.key"
gen_csr "$CA_DIR/intermediate-ca.key" "/CN=$INTERMEDIATE_CN/O=$ORG" "$TMP_DIR/intermediate.csr"
sign_cert "$TMP_DIR/intermediate.csr" \
	"$CA_DIR/root-ca.crt" "$CA_DIR/root-ca.key" \
	"intermediate_ext" "$INTERMEDIATE_DAYS" "$CA_DIR/intermediate-ca.crt"

# --- Server leaf (serverAuth, SANs) ------------------------------------------

echo "Generating server leaf ($SERVER_CN, ${LEAF_DAYS}d)..."
gen_key "$SERVER_DIR/server.key"
gen_csr "$SERVER_DIR/server.key" "/CN=$SERVER_CN/O=$ORG" "$TMP_DIR/server.csr"
sign_cert "$TMP_DIR/server.csr" \
	"$CA_DIR/intermediate-ca.crt" "$CA_DIR/intermediate-ca.key" \
	"server_ext" "$LEAF_DAYS" "$SERVER_DIR/server.crt"
cat "$SERVER_DIR/server.crt" "$CA_DIR/intermediate-ca.crt" >"$SERVER_DIR/server-chain.crt"

# --- Client leaf (clientAuth) ------------------------------------------------

echo "Generating client leaf ($CLIENT_CN, ${LEAF_DAYS}d)..."
gen_key "$CLIENT_DIR/client.key"
gen_csr "$CLIENT_DIR/client.key" "/CN=$CLIENT_CN/O=$ORG" "$TMP_DIR/client.csr"
sign_cert "$TMP_DIR/client.csr" \
	"$CA_DIR/intermediate-ca.crt" "$CA_DIR/intermediate-ca.key" \
	"client_ext" "$LEAF_DAYS" "$CLIENT_DIR/client.crt"
cat "$CLIENT_DIR/client.crt" "$CA_DIR/intermediate-ca.crt" >"$CLIENT_DIR/client-chain.crt"

# --- Summary -----------------------------------------------------------------

echo "PKI generated under $CERTS_DIR (signature: $KEY_ALG):"
echo "  CA:     $CA_DIR/root-ca.crt -> $CA_DIR/intermediate-ca.crt"
echo "  Server: $SERVER_DIR/server-chain.crt ($SERVER_CN)"
echo "  Client: $CLIENT_DIR/client-chain.crt ($CLIENT_CN)"

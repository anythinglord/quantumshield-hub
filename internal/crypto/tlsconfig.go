// Package crypto provides strict post-quantum mTLS policies, PKI file loaders,
// and ML-DSA (FIPS 204) payload-signature helpers for QuantumShield Hub.
//
// Importers SHOULD alias this package (e.g. qscrypto): the default package name
// `crypto` collides with the standard library package.
package crypto

import (
	"crypto/tls"
	"crypto/x509"
)

// curvePreferences returns the explicit key-exchange policy shared by the
// server and client TLS configurations. It pins the hybrid post-quantum
// X25519MLKEM768 group and deliberately omits the Go 1.26 ML-KEM defaults so
// the negotiated group is deterministic and fail-closed (design D2).
func curvePreferences() []tls.CurveID {
	return []tls.CurveID{tls.X25519MLKEM768}
}

// ServerTLSConfig builds the strict server-side mTLS policy: TLS 1.3 minimum,
// mandatory client-certificate verification against clientCAs, and the hybrid
// X25519MLKEM768 key-exchange group. The supplied server certificate is
// attached for use during the handshake.
func ServerTLSConfig(serverCert tls.Certificate, clientCAs *x509.CertPool) *tls.Config {
	return &tls.Config{
		MinVersion:       tls.VersionTLS13,
		ClientAuth:       tls.RequireAndVerifyClientCert,
		ClientCAs:        clientCAs,
		Certificates:     []tls.Certificate{serverCert},
		CurvePreferences: curvePreferences(),
	}
}

// ClientTLSConfig builds the strict client-side mTLS policy: TLS 1.3 minimum,
// the given trust roots and server name, the client identity certificate, and
// the hybrid X25519MLKEM768 key-exchange group.
func ClientTLSConfig(clientCert tls.Certificate, rootCAs *x509.CertPool, serverName string) *tls.Config {
	return &tls.Config{
		MinVersion:       tls.VersionTLS13,
		RootCAs:          rootCAs,
		ServerName:       serverName,
		Certificates:     []tls.Certificate{clientCert},
		CurvePreferences: curvePreferences(),
	}
}

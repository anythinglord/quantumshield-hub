package crypto

import (
	"crypto/mldsa"
	"testing"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
)

// mldsaSets maps each supported wire algorithm to its FIPS 204 parameter set.
var mldsaSets = []struct {
	name string
	alg  apiv1.SignatureAlgorithm
	set  mldsa.Parameters
}{
	{"ML-DSA-44", apiv1.SignatureAlgorithm_ML_DSA_44, mldsa.MLDSA44()},
	{"ML-DSA-65", apiv1.SignatureAlgorithm_ML_DSA_65, mldsa.MLDSA65()},
	{"ML-DSA-87", apiv1.SignatureAlgorithm_ML_DSA_87, mldsa.MLDSA87()},
}

// TestParamsFor verifies the wire-algorithm to parameter-set mapping and that
// unspecified/out-of-range values are rejected with no silent default (spec
// Requirement 4).
func TestParamsFor(t *testing.T) {
	for _, s := range mldsaSets {
		t.Run(s.name, func(t *testing.T) {
			got, err := ParamsFor(s.alg)
			if err != nil {
				t.Fatalf("ParamsFor(%s) error = %v, want nil", s.name, err)
			}
			if got.String() != s.set.String() {
				t.Errorf("ParamsFor(%s) = %s, want %s", s.name, got.String(), s.set.String())
			}
		})
	}

	t.Run("unspecified rejected", func(t *testing.T) {
		if _, err := ParamsFor(apiv1.SignatureAlgorithm_SIGNATURE_ALGORITHM_UNSPECIFIED); err == nil {
			t.Error("ParamsFor(UNSPECIFIED) = nil error, want non-nil")
		}
	})

	t.Run("out of range rejected", func(t *testing.T) {
		for _, alg := range []apiv1.SignatureAlgorithm{-1, 99} {
			if _, err := ParamsFor(alg); err == nil {
				t.Errorf("ParamsFor(%d) = nil error, want non-nil", alg)
			}
		}
	})
}

// TestSignVerifyRoundTrip verifies signatures produced by SignPayload verify
// under VerifyPayload for every parameter set (spec Requirement 6).
func TestSignVerifyRoundTrip(t *testing.T) {
	payload := []byte("quantumshield telemetry payload")

	for _, s := range mldsaSets {
		t.Run(s.name, func(t *testing.T) {
			sk, err := mldsa.GenerateKey(s.set)
			if err != nil {
				t.Fatalf("GenerateKey(%s): %v", s.name, err)
			}

			sig, err := SignPayload(sk, payload, nil)
			if err != nil {
				t.Fatalf("SignPayload(%s) error = %v, want nil", s.name, err)
			}
			if got, want := len(sig), s.set.SignatureSize(); got != want {
				t.Errorf("signature size = %d, want %d", got, want)
			}

			pub := sk.PublicKey().Bytes()
			if got, want := len(pub), s.set.PublicKeySize(); got != want {
				t.Errorf("public key size = %d, want %d", got, want)
			}

			if err := VerifyPayload(s.alg, pub, payload, sig, nil); err != nil {
				t.Errorf("VerifyPayload(%s) round-trip = %v, want nil", s.name, err)
			}
		})
	}
}

// TestVerifyPayload verifies the acceptance path and the adversarial rejection
// paths (spec Requirement 5).
func TestVerifyPayload(t *testing.T) {
	set := mldsa.MLDSA65()
	alg := apiv1.SignatureAlgorithm_ML_DSA_65

	sk, err := mldsa.GenerateKey(set)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	payload := []byte("telemetry")
	sig, err := SignPayload(sk, payload, nil)
	if err != nil {
		t.Fatalf("SignPayload: %v", err)
	}
	pub := sk.PublicKey().Bytes()

	t.Run("valid signature accepted", func(t *testing.T) {
		if err := VerifyPayload(alg, pub, payload, sig, nil); err != nil {
			t.Errorf("VerifyPayload(valid) = %v, want nil", err)
		}
	})

	t.Run("adversarial inputs rejected", func(t *testing.T) {
		tamperedPayload := append([]byte(nil), payload...)
		tamperedPayload[0] ^= 0xFF

		tamperedSig := append([]byte(nil), sig...)
		tamperedSig[len(tamperedSig)-1] ^= 0xFF

		truncatedSig := sig[:len(sig)-1]
		shortKey := []byte{0x01, 0x02, 0x03}

		cases := []struct {
			name string
			alg  apiv1.SignatureAlgorithm
			pub  []byte
			pl   []byte
			sg   []byte
		}{
			{"tampered payload", alg, pub, tamperedPayload, sig},
			{"tampered signature", alg, pub, payload, tamperedSig},
			{"truncated signature", alg, pub, payload, truncatedSig},
			{"wrong-length key", alg, shortKey, payload, sig},
			{"mismatched algorithm", apiv1.SignatureAlgorithm_ML_DSA_44, pub, payload, sig},
		}

		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				if err := VerifyPayload(c.alg, c.pub, c.pl, c.sg, nil); err == nil {
					t.Errorf("VerifyPayload(%s) = nil error, want non-nil rejection", c.name)
				}
			})
		}
	})
}

// TestContextDomainSeparation verifies signatures are bound to the exact
// context and cannot be verified under a different one (spec Requirement 7).
func TestContextDomainSeparation(t *testing.T) {
	set := mldsa.MLDSA44()
	alg := apiv1.SignatureAlgorithm_ML_DSA_44

	sk, err := mldsa.GenerateKey(set)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	payload := []byte("ctx-bound payload")
	signerCtx := &mldsa.Options{Context: "quantumshield.telemetry.v1"}

	sig, err := SignPayload(sk, payload, signerCtx)
	if err != nil {
		t.Fatalf("SignPayload: %v", err)
	}
	pub := sk.PublicKey().Bytes()

	t.Run("matching context accepted", func(t *testing.T) {
		if err := VerifyPayload(alg, pub, payload, sig, signerCtx); err != nil {
			t.Errorf("VerifyPayload(matching context) = %v, want nil", err)
		}
	})

	t.Run("different context rejected", func(t *testing.T) {
		other := &mldsa.Options{Context: "quantumshield.other.v2"}
		if err := VerifyPayload(alg, pub, payload, sig, other); err == nil {
			t.Error("VerifyPayload(different context) = nil error, want non-nil")
		}
	})
}

// TestSignPayloadDefaultsContext verifies nil or empty-context options collapse
// to the shared ContextTelemetryV1 domain (spec Requirement 7 + design AD3).
func TestSignPayloadDefaultsContext(t *testing.T) {
	set := mldsa.MLDSA65()
	alg := apiv1.SignatureAlgorithm_ML_DSA_65

	sk, err := mldsa.GenerateKey(set)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	payload := []byte("defaults")

	sig, err := SignPayload(sk, payload, nil)
	if err != nil {
		t.Fatalf("SignPayload(nil opts): %v", err)
	}
	pub := sk.PublicKey().Bytes()

	t.Run("nil opts verify with nil opts", func(t *testing.T) {
		if err := VerifyPayload(alg, pub, payload, sig, nil); err != nil {
			t.Errorf("VerifyPayload(nil opts) = %v, want nil", err)
		}
	})

	t.Run("nil opts verify with explicit default context", func(t *testing.T) {
		explicit := &mldsa.Options{Context: ContextTelemetryV1}
		if err := VerifyPayload(alg, pub, payload, sig, explicit); err != nil {
			t.Errorf("VerifyPayload(explicit default) = %v, want nil", err)
		}
	})

	t.Run("nil opts rejected under different context", func(t *testing.T) {
		other := &mldsa.Options{Context: "quantumshield.other.v2"}
		if err := VerifyPayload(alg, pub, payload, sig, other); err == nil {
			t.Error("VerifyPayload(different context) = nil error, want non-nil")
		}
	})

	t.Run("empty context options default too", func(t *testing.T) {
		empty := &mldsa.Options{}
		sig2, err := SignPayload(sk, payload, empty)
		if err != nil {
			t.Fatalf("SignPayload(empty opts): %v", err)
		}
		if err := VerifyPayload(alg, pub, payload, sig2, nil); err != nil {
			t.Errorf("VerifyPayload(sig from empty opts) = %v, want nil", err)
		}
	})
}

// TestSignPayloadNilKeyRejected verifies a nil private key never panics and
// returns an error (defensive contract from the design).
func TestSignPayloadNilKeyRejected(t *testing.T) {
	if _, err := SignPayload(nil, []byte("payload"), nil); err == nil {
		t.Error("SignPayload(nil key) = nil error, want non-nil")
	}
}

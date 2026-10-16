package crypto

import (
	"crypto/mldsa"
	"fmt"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
)

// ContextTelemetryV1 is the versioned domain-separation context bound to
// telemetry signatures. It is identical on the signing (edge) and verifying
// (ingestion) sides and is never empty (design D1).
const ContextTelemetryV1 = "quantumshield.telemetry.v1"

// ParamsFor maps a wire SignatureAlgorithm to its FIPS 204 parameter set.
// UNSPECIFIED and out-of-range values are rejected — there is no silent
// default.
func ParamsFor(alg apiv1.SignatureAlgorithm) (mldsa.Parameters, error) {
	switch alg {
	case apiv1.SignatureAlgorithm_ML_DSA_44:
		return mldsa.MLDSA44(), nil
	case apiv1.SignatureAlgorithm_ML_DSA_65:
		return mldsa.MLDSA65(), nil
	case apiv1.SignatureAlgorithm_ML_DSA_87:
		return mldsa.MLDSA87(), nil
	default:
		return mldsa.Parameters{}, fmt.Errorf("crypto: unsupported signature algorithm %s", alg)
	}
}

// SignPayload signs payload with sk using the deterministic ML-DSA algorithm.
// A nil opts (or one with an empty Context) is resolved to ContextTelemetryV1.
// A nil private key is rejected rather than panicking.
func SignPayload(sk *mldsa.PrivateKey, payload []byte, opts *mldsa.Options) ([]byte, error) {
	if sk == nil {
		return nil, fmt.Errorf("crypto: sign payload: nil private key")
	}
	sig, err := sk.SignDeterministic(payload, resolveContext(opts))
	if err != nil {
		return nil, fmt.Errorf("crypto: sign payload: %w", err)
	}
	return sig, nil
}

// VerifyPayload verifies a raw-encoded ML-DSA public key over payload against
// signature using alg's parameter set. A nil opts (or empty Context) resolves
// to ContextTelemetryV1. Any non-nil error means rejection — callers MUST NOT
// match an error string or sentinel (mldsa exports none).
func VerifyPayload(alg apiv1.SignatureAlgorithm, publicKey, payload, signature []byte, opts *mldsa.Options) error {
	params, err := ParamsFor(alg)
	if err != nil {
		return err
	}
	pk, err := mldsa.NewPublicKey(params, publicKey)
	if err != nil {
		return fmt.Errorf("crypto: verify payload: parse public key: %w", err)
	}
	if err := mldsa.Verify(pk, payload, signature, resolveContext(opts)); err != nil {
		return fmt.Errorf("crypto: verify payload: %w", err)
	}
	return nil
}

// resolveContext guarantees a non-empty domain-separation context: nil options
// or options with an empty Context fall back to ContextTelemetryV1, while an
// explicit non-default context is honored unchanged.
func resolveContext(opts *mldsa.Options) *mldsa.Options {
	if opts == nil || opts.Context == "" {
		return &mldsa.Options{Context: ContextTelemetryV1}
	}
	return opts
}

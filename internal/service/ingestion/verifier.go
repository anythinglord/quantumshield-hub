package ingestion

import (
	"context"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
)

// AcceptAllVerifier is the Phase 3 Verifier: an explicit, documented no-op.
//
// Design "Option C" is locked: Phase 3 has no device public keys to verify
// against, so every request is accepted and no signature bytes are inspected.
// This is intentional, not a forgotten check.
//
// Phase 4 swaps it for an AuthBackedVerifier that looks the device public key
// up through DeviceAuthService, caches it, and calls qscrypto.VerifyPayload
// with ContextTelemetryV1. That swap happens in main.go's constructor call;
// the handler and proto contract do not change.
type AcceptAllVerifier struct{}

// NewAcceptAllVerifier returns the Phase 3 accept-all verifier.
//
// It exists so wiring reads as an explicit decision in main.go and so the
// Phase 4 swap is a one-line constructor change (design Option C).
func NewAcceptAllVerifier() AcceptAllVerifier {
	return AcceptAllVerifier{}
}

// Verify implements the Verifier seam by accepting every request.
//
// deviceID, alg, payload and signature are deliberately not inspected: this
// is the no-op half of the Option C seam. It never returns ErrUnknownDevice.
func (AcceptAllVerifier) Verify(
	context.Context,
	string,
	apiv1.SignatureAlgorithm,
	[]byte,
	[]byte,
) error {
	return nil
}

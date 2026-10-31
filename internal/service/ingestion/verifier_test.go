package ingestion

import (
	"context"
	"errors"
	"testing"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
)

// Compile-time proof that the Phase 3 verifier honours the seam and can be
// swapped for the Phase 4 AuthBackedVerifier without touching the handler.
var _ Verifier = AcceptAllVerifier{}

// TestAcceptAllVerifier covers the locked Option C no-op: every device,
// algorithm, payload and signature is accepted in Phase 3.
func TestAcceptAllVerifier(t *testing.T) {
	t.Run("S-003 accepts arbitrary requests", func(t *testing.T) {
		cases := []struct {
			name      string
			deviceID  string
			algorithm apiv1.SignatureAlgorithm
			payload   []byte
			signature []byte
		}{
			{
				name:      "signed ML-DSA-65 request",
				deviceID:  "device-001",
				algorithm: apiv1.SignatureAlgorithm_ML_DSA_65,
				payload:   []byte{0x01, 0x02, 0x03},
				signature: []byte{0xaa, 0xbb},
			},
			{
				name:      "unknown device identity",
				deviceID:  "device-404",
				algorithm: apiv1.SignatureAlgorithm_ML_DSA_44,
				payload:   []byte("payload"),
				signature: []byte("signature"),
			},
			{
				name:      "empty everything",
				deviceID:  "",
				algorithm: apiv1.SignatureAlgorithm_SIGNATURE_ALGORITHM_UNSPECIFIED,
				payload:   nil,
				signature: nil,
			},
		}

		verifier := NewAcceptAllVerifier()
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				err := verifier.Verify(context.Background(), tc.deviceID, tc.algorithm, tc.payload, tc.signature)
				if err != nil {
					t.Fatalf("Verify(%q) error = %v, want nil", tc.deviceID, err)
				}
				if errors.Is(err, ErrUnknownDevice) {
					t.Fatalf("Verify(%q) matched ErrUnknownDevice, want accept-all", tc.deviceID)
				}
			})
		}
	})

	t.Run("S-003 zero value is a usable accept-all verifier", func(t *testing.T) {
		var verifier AcceptAllVerifier
		if err := verifier.Verify(context.Background(), "any", apiv1.SignatureAlgorithm_ML_DSA_87, []byte("p"), []byte("s")); err != nil {
			t.Fatalf("zero-value Verify() error = %v, want nil", err)
		}
	})
}

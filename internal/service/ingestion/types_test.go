package ingestion

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
)

// Compile-time proof that the seam interfaces can be satisfied by plain
// implementations, which is how the engine tests and Phase 4 verifier plug in.
var (
	_ BatchInserter = (*recordingInserter)(nil)
	_ Verifier      = (*recordingVerifier)(nil)
)

// recordingInserter captures the batches handed to the seam.
type recordingInserter struct {
	batches [][]TelemetryRow
}

func (r *recordingInserter) InsertBatch(_ context.Context, rows []TelemetryRow) error {
	r.batches = append(r.batches, rows)
	return nil
}

// recordingVerifier records the verifier arguments and returns a fixed error.
type recordingVerifier struct {
	err       error
	deviceID  string
	algorithm apiv1.SignatureAlgorithm
	payload   []byte
	signature []byte
}

func (v *recordingVerifier) Verify(_ context.Context, deviceID string, alg apiv1.SignatureAlgorithm, payload, signature []byte) error {
	v.deviceID = deviceID
	v.algorithm = alg
	v.payload = payload
	v.signature = signature
	return v.err
}

func TestBatchInserterSeam(t *testing.T) {
	t.Run("S-001 row fields cross the seam verbatim", func(t *testing.T) {
		lat, long := 12.5, -56.25
		ts := time.Date(2026, 10, 31, 12, 0, 0, 123456789, time.UTC)
		row := TelemetryRow{
			DeviceID:     "device-001",
			Time:         ts,
			Sequence:     17,
			TemperatureC: 21.5,
			VoltageV:     3.32,
			CurrentA:     0.75,
			Lat:          &lat,
			Long:         &long,
		}

		var inserter BatchInserter = &recordingInserter{}
		if err := inserter.InsertBatch(context.Background(), []TelemetryRow{row}); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}

		recorder := inserter.(*recordingInserter)
		if len(recorder.batches) != 1 {
			t.Fatalf("recorded %d batches, want 1", len(recorder.batches))
		}
		if len(recorder.batches[0]) != 1 {
			t.Fatalf("recorded %d rows, want 1", len(recorder.batches[0]))
		}

		got := recorder.batches[0][0]
		if got.DeviceID != "device-001" {
			t.Errorf("DeviceID = %q, want %q", got.DeviceID, "device-001")
		}
		if !got.Time.Equal(ts) {
			t.Errorf("Time = %v, want %v", got.Time, ts)
		}
		if got.Sequence != 17 {
			t.Errorf("Sequence = %d, want 17", got.Sequence)
		}
		if got.TemperatureC != 21.5 || got.VoltageV != 3.32 || got.CurrentA != 0.75 {
			t.Errorf("measurements = (%v, %v, %v), want (21.5, 3.32, 0.75)",
				got.TemperatureC, got.VoltageV, got.CurrentA)
		}
		if got.Lat == nil || *got.Lat != lat {
			t.Errorf("Lat = %v, want %v", got.Lat, lat)
		}
		if got.Long == nil || *got.Long != long {
			t.Errorf("Long = %v, want %v", got.Long, long)
		}
	})

	t.Run("S-001 absent GPS stays nil across the seam", func(t *testing.T) {
		row := TelemetryRow{DeviceID: "fixed-node", Sequence: 1}

		inserter := &recordingInserter{}
		if err := inserter.InsertBatch(context.Background(), []TelemetryRow{row}); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}

		got := inserter.batches[0][0]
		if got.Lat != nil {
			t.Errorf("Lat = %v, want nil for a fixed node", got.Lat)
		}
		if got.Long != nil {
			t.Errorf("Long = %v, want nil for a fixed node", got.Long)
		}
	})
}

func TestVerifierSeam(t *testing.T) {
	t.Run("S-002 wrapped ErrUnknownDevice matches with errors.Is", func(t *testing.T) {
		verifier := &recordingVerifier{
			err: fmt.Errorf("key lookup for %q: %w", "device-404", ErrUnknownDevice),
		}

		err := verifier.Verify(context.Background(), "device-404",
			apiv1.SignatureAlgorithm_ML_DSA_65, []byte("payload"), []byte("sig"))
		if err == nil {
			t.Fatal("Verify returned nil for an unknown device")
		}
		if !errors.Is(err, ErrUnknownDevice) {
			t.Errorf("Verify error %v does not match ErrUnknownDevice", err)
		}
	})

	t.Run("S-002 unrelated errors do not match ErrUnknownDevice", func(t *testing.T) {
		signatureErr := errors.New("ingestion: invalid signature")
		verifier := &recordingVerifier{err: fmt.Errorf("ML-DSA verify: %w", signatureErr)}

		err := verifier.Verify(context.Background(), "device-001",
			apiv1.SignatureAlgorithm_ML_DSA_44, []byte("payload"), []byte("sig"))
		if !errors.Is(err, signatureErr) {
			t.Fatalf("Verify error %v does not match the wrapped signature error", err)
		}
		if errors.Is(err, ErrUnknownDevice) {
			t.Errorf("Verify error %v incorrectly matches ErrUnknownDevice", err)
		}
	})

	t.Run("verifier receives the request fields verbatim", func(t *testing.T) {
		payload := []byte{0x01, 0x02, 0x03}
		signature := []byte{0xff, 0xee}
		verifier := &recordingVerifier{}

		if err := verifier.Verify(context.Background(), "device-002",
			apiv1.SignatureAlgorithm_ML_DSA_87, payload, signature); err != nil {
			t.Fatalf("Verify: %v", err)
		}

		if verifier.deviceID != "device-002" {
			t.Errorf("deviceID = %q, want %q", verifier.deviceID, "device-002")
		}
		if verifier.algorithm != apiv1.SignatureAlgorithm_ML_DSA_87 {
			t.Errorf("algorithm = %v, want %v", verifier.algorithm, apiv1.SignatureAlgorithm_ML_DSA_87)
		}
		if string(verifier.payload) != string(payload) {
			t.Errorf("payload = %v, want %v", verifier.payload, payload)
		}
		if string(verifier.signature) != string(signature) {
			t.Errorf("signature = %v, want %v", verifier.signature, signature)
		}
	})
}

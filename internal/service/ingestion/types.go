// Package ingestion implements the telemetry ingestion engine: the seam types
// the persistence layer plugs into, the bidi-stream handler, and the sharded
// worker pool.
//
// The Verifier seam is satisfied in Phase 3 by an explicit documentation-only
// accept-all no-op (design "Option C"); real ML-DSA verification backed by the
// device registry arrives in Phase 4 by swapping one constructor argument in
// main.go, with no handler or proto change.
package ingestion

import (
	"context"
	"errors"
	"time"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
)

// TelemetryRow is one sensor reading flattened out of a received TelemetryBatch.
// Lat and Long are nil when the edge node reports no GPS fix.
type TelemetryRow struct {
	DeviceID     string
	Time         time.Time
	Sequence     int64
	TemperatureC float64
	VoltageV     float64
	CurrentA     float64
	Lat          *float64
	Long         *float64
}

// BatchInserter persists a batch of telemetry rows. The engine depends on this
// seam only; the pgx implementation lives in internal/db.
type BatchInserter interface {
	InsertBatch(ctx context.Context, rows []TelemetryRow) error
}

// Verifier authenticates a signed telemetry payload for a device.
//
// It returns error rather than an ingest status so the crypto seam stays
// decoupled from the transport enum (design Q4). A lookup miss must return an
// error wrapping ErrUnknownDevice; the handler maps that to REJECTED_DEVICE,
// and any other error to REJECTED_SIGNATURE.
type Verifier interface {
	Verify(ctx context.Context, deviceID string, alg apiv1.SignatureAlgorithm, payload, signature []byte) error
}

// ErrUnknownDevice reports that no registered device matches the sender.
// Callers must compare with errors.Is so wrapped lookups keep working.
var ErrUnknownDevice = errors.New("ingestion: unknown device")

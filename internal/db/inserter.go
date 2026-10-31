package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/anythinglord/quantumshield-hub/internal/service/ingestion"
)

// telemetryInsertSQL is the single parameterized INSERT the batch inserter
// uses. Values are always bound through placeholders; SQL is never built by
// concatenation (design A4, AGENTS §3). The column order here must match
// telemetryInsertArgs.
const telemetryInsertSQL = `INSERT INTO device_telemetry
    (device_id, time, sequence, temperature_c, voltage_v, current_a, lat, long)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

// batchSender is the only piece of *pgxpool.Pool the inserter needs. Tests
// substitute it to inspect the queued batch without a live Postgres (A10).
type batchSender interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// pgxBatchInserter implements the engine's BatchInserter seam using pgx
// batches so one network round trip persists a whole worker flush.
type pgxBatchInserter struct {
	sender batchSender
}

// NewBatchInserter adapts a pgx pool to the engine's BatchInserter seam. A
// nil pool is accepted at construction so wiring bugs surface as an error on
// the first non-empty batch instead of a nil-pointer panic.
func NewBatchInserter(pool *pgxpool.Pool) ingestion.BatchInserter {
	var sender batchSender
	if pool != nil {
		sender = pool
	}
	return &pgxBatchInserter{sender: sender}
}

// InsertBatch queues one parameterized INSERT per row, sends them as a single
// pgx batch, and reads every result. An empty or nil slice is a no-op that
// sends zero statements. A canceled context short-circuits before any I/O.
func (i *pgxBatchInserter) InsertBatch(ctx context.Context, rows []ingestion.TelemetryRow) error {
	if len(rows) == 0 {
		return nil
	}
	if i.sender == nil {
		return errors.New("db: insert telemetry batch: nil pool")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("db: insert telemetry batch: %w", err)
	}

	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(telemetryInsertSQL, telemetryInsertArgs(row)...)
	}

	results := i.sender.SendBatch(ctx, batch)
	execErr := consumeBatchResults(results, len(rows))

	if closeErr := results.Close(); closeErr != nil && execErr == nil {
		execErr = fmt.Errorf("db: insert telemetry batch: %w", closeErr)
	}

	return execErr
}

// consumeBatchResults reads one result per queued statement and stops at the
// first error, surfacing it wrapped. Remaining results are drained by Close.
func consumeBatchResults(results pgx.BatchResults, statements int) error {
	for range statements {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("db: insert telemetry batch: %w", err)
		}
	}
	return nil
}

// telemetryInsertArgs flattens a row into the positional parameters of
// telemetryInsertSQL, in the exact column order the INSERT declares.
func telemetryInsertArgs(row ingestion.TelemetryRow) []any {
	return []any{
		row.DeviceID,
		row.Time,
		row.Sequence,
		row.TemperatureC,
		row.VoltageV,
		row.CurrentA,
		row.Lat,
		row.Long,
	}
}

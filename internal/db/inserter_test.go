package db

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/anythinglord/quantumshield-hub/internal/service/ingestion"
)

// fakeBatchResults is a minimal pgx.BatchResults. InsertBatch only calls Exec
// and Close, so the query surface is unreachable in practice.
type fakeBatchResults struct {
	execs    int
	execErr  error
	closeErr error
	closed   bool
}

func (r *fakeBatchResults) Exec() (pgconn.CommandTag, error) {
	r.execs++
	if r.execErr != nil {
		return pgconn.CommandTag{}, r.execErr
	}
	return pgconn.CommandTag{}, nil
}

func (r *fakeBatchResults) Query() (pgx.Rows, error) {
	return nil, errors.New("fake batch results: Query is not supported")
}

func (r *fakeBatchResults) QueryRow() pgx.Row { return nil }

func (r *fakeBatchResults) Close() error {
	r.closed = true
	return r.closeErr
}

// fakeBatchSender captures the queued batch so tests can assert the SQL text
// and bound parameters without a live Postgres (A10).
type fakeBatchSender struct {
	calls   int
	batch   *pgx.Batch
	ctx     context.Context
	results *fakeBatchResults
}

func (s *fakeBatchSender) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	s.calls++
	s.ctx = ctx
	s.batch = b
	if s.results == nil {
		s.results = &fakeBatchResults{}
	}
	return s.results
}

func TestInsertBatchEmptyIsNoOp(t *testing.T) {
	t.Run("S-013 nil slice returns nil and sends nothing", func(t *testing.T) {
		sender := &fakeBatchSender{}
		inserter := &pgxBatchInserter{sender: sender}

		if err := inserter.InsertBatch(context.Background(), nil); err != nil {
			t.Fatalf("InsertBatch(nil) = %v, want nil", err)
		}
		if sender.calls != 0 {
			t.Errorf("SendBatch called %d times for a nil slice, want 0", sender.calls)
		}
	})

	t.Run("S-013 empty slice returns nil and sends nothing", func(t *testing.T) {
		sender := &fakeBatchSender{}
		inserter := &pgxBatchInserter{sender: sender}

		if err := inserter.InsertBatch(context.Background(), []ingestion.TelemetryRow{}); err != nil {
			t.Fatalf("InsertBatch(empty) = %v, want nil", err)
		}
		if sender.calls != 0 {
			t.Errorf("SendBatch called %d times for an empty slice, want 0", sender.calls)
		}
	})
}

func TestInsertBatchQueuesParameterizedStatements(t *testing.T) {
	lat := 48.8566
	long := 2.3522
	rows := []ingestion.TelemetryRow{
		{
			DeviceID:     "device-a",
			Time:         time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
			Sequence:     11,
			TemperatureC: 21.5,
			VoltageV:     3.3,
			CurrentA:     0.75,
			Lat:          &lat,
			Long:         &long,
		},
		{
			DeviceID:     "device-b",
			Time:         time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC),
			Sequence:     12,
			TemperatureC: -4.25,
			VoltageV:     12.0,
			CurrentA:     1.5,
		},
	}

	sender := &fakeBatchSender{}
	inserter := &pgxBatchInserter{sender: sender}

	if err := inserter.InsertBatch(context.Background(), rows); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	if sender.calls != 1 {
		t.Fatalf("SendBatch called %d times, want exactly 1", sender.calls)
	}
	if sender.batch == nil {
		t.Fatal("SendBatch received a nil batch")
	}
	if got := sender.batch.Len(); got != len(rows) {
		t.Fatalf("batch has %d statements, want %d", got, len(rows))
	}
	if sender.results.execs != len(rows) {
		t.Errorf("Exec called %d times, want %d", sender.results.execs, len(rows))
	}
	if !sender.results.closed {
		t.Error("batch results were not closed")
	}

	// The exact literal is the persistence contract: column order must match
	// the positional placeholders and the bound argument order below.
	const wantSQL = `INSERT INTO device_telemetry
    (device_id, time, sequence, temperature_c, voltage_v, current_a, lat, long)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	for i, queued := range sender.batch.QueuedQueries {
		if queued.SQL != wantSQL {
			t.Errorf("statement %d SQL = %q, want %q", i, queued.SQL, wantSQL)
		}

		wantArgs := []any{
			rows[i].DeviceID,
			rows[i].Time,
			rows[i].Sequence,
			rows[i].TemperatureC,
			rows[i].VoltageV,
			rows[i].CurrentA,
			rows[i].Lat,
			rows[i].Long,
		}
		if !reflect.DeepEqual(queued.Arguments, wantArgs) {
			t.Errorf("statement %d arguments = %#v, want %#v", i, queued.Arguments, wantArgs)
		}
	}
}

func TestInsertBatchBindsHostileLiteral(t *testing.T) {
	const hostile = "dev'; DROP TABLE device_telemetry;--"

	sender := &fakeBatchSender{}
	inserter := &pgxBatchInserter{sender: sender}

	row := ingestion.TelemetryRow{
		DeviceID:     hostile,
		Time:         time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC),
		Sequence:     1,
		TemperatureC: 20,
		VoltageV:     3.3,
		CurrentA:     0.5,
	}
	if err := inserter.InsertBatch(context.Background(), []ingestion.TelemetryRow{row}); err != nil {
		t.Fatalf("InsertBatch: %v", err)
	}

	queued := sender.batch.QueuedQueries[0]
	if strings.Contains(queued.SQL, hostile) {
		t.Fatalf("hostile literal was concatenated into SQL: %q", queued.SQL)
	}
	if got := queued.Arguments[0]; got != hostile {
		t.Errorf("first bound parameter = %v, want the hostile string stored verbatim", got)
	}
}

func TestInsertBatchHonorsCanceledContext(t *testing.T) {
	sender := &fakeBatchSender{}
	inserter := &pgxBatchInserter{sender: sender}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := inserter.InsertBatch(ctx, []ingestion.TelemetryRow{{
		DeviceID: "device-a",
		Time:     time.Now(),
	}})
	if err == nil {
		t.Fatal("InsertBatch succeeded with a canceled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error %v does not wrap context.Canceled", err)
	}
	if sender.calls != 0 {
		t.Errorf("SendBatch called %d times with a canceled context, want 0", sender.calls)
	}
}

func TestInsertBatchWrapsExecError(t *testing.T) {
	sentinel := errors.New("exec boom")
	results := &fakeBatchResults{execErr: sentinel}
	sender := &fakeBatchSender{results: results}
	inserter := &pgxBatchInserter{sender: sender}

	err := inserter.InsertBatch(context.Background(), []ingestion.TelemetryRow{{
		DeviceID: "device-a",
		Time:     time.Now(),
	}})
	if !errors.Is(err, sentinel) {
		t.Errorf("error %v does not wrap the exec error", err)
	}
	if !results.closed {
		t.Error("batch results were not closed after an exec error")
	}
}

func TestInsertBatchNilPoolReturnsError(t *testing.T) {
	inserter := NewBatchInserter(nil)

	err := inserter.InsertBatch(context.Background(), []ingestion.TelemetryRow{{
		DeviceID: "device-a",
		Time:     time.Now(),
	}})
	if err == nil {
		t.Fatal("InsertBatch with a nil pool succeeded")
	}
	if !strings.Contains(err.Error(), "nil") {
		t.Errorf("error %q does not report the nil pool", err)
	}
}

// TestInsertBatchRoundTrip is the DB-gated integration proof (A10): it is
// skipped unless TIMESCALE_TEST_DSN is set and -short is off.
func TestInsertBatchRoundTrip(t *testing.T) {
	pool := requireDB(t)
	resetSchema(t, pool)
	if err := Up(context.Background(), pool); err != nil {
		t.Fatalf("Up: %v", err)
	}

	inserter := NewBatchInserter(pool)

	t.Run("S-012 round-trip persists every row with identical values", func(t *testing.T) {
		if _, err := pool.Exec(context.Background(), "TRUNCATE device_telemetry"); err != nil {
			t.Fatalf("truncate device_telemetry: %v", err)
		}

		lat := 48.8566
		long := 2.3522
		want := []ingestion.TelemetryRow{
			{
				DeviceID:     "device-a",
				Time:         time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC),
				Sequence:     11,
				TemperatureC: 21.5,
				VoltageV:     3.3,
				CurrentA:     0.75,
				Lat:          &lat,
				Long:         &long,
			},
			{
				DeviceID:     "device-b",
				Time:         time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC),
				Sequence:     12,
				TemperatureC: -4.25,
				VoltageV:     12,
				CurrentA:     1.5,
			},
		}

		if err := inserter.InsertBatch(context.Background(), want); err != nil {
			t.Fatalf("InsertBatch: %v", err)
		}

		const query = `SELECT device_id, time, sequence, temperature_c, voltage_v, current_a, lat, long
			FROM device_telemetry ORDER BY sequence`
		rows, err := pool.Query(context.Background(), query)
		if err != nil {
			t.Fatalf("query device_telemetry: %v", err)
		}
		defer rows.Close()

		var got []ingestion.TelemetryRow
		for rows.Next() {
			var (
				row       ingestion.TelemetryRow
				latValue  pgtype.Float8
				longValue pgtype.Float8
			)
			if err := rows.Scan(&row.DeviceID, &row.Time, &row.Sequence,
				&row.TemperatureC, &row.VoltageV, &row.CurrentA, &latValue, &longValue); err != nil {
				t.Fatalf("scan telemetry row: %v", err)
			}
			if latValue.Valid {
				v := latValue.Float64
				row.Lat = &v
			}
			if longValue.Valid {
				v := longValue.Float64
				row.Long = &v
			}
			got = append(got, row)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate telemetry rows: %v", err)
		}

		if len(got) != len(want) {
			t.Fatalf("persisted %d rows, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i].DeviceID != want[i].DeviceID {
				t.Errorf("row %d device_id = %q, want %q", i, got[i].DeviceID, want[i].DeviceID)
			}
			if !got[i].Time.Equal(want[i].Time) {
				t.Errorf("row %d time = %v, want %v", i, got[i].Time, want[i].Time)
			}
			if got[i].Sequence != want[i].Sequence {
				t.Errorf("row %d sequence = %d, want %d", i, got[i].Sequence, want[i].Sequence)
			}
			if got[i].TemperatureC != want[i].TemperatureC {
				t.Errorf("row %d temperature_c = %v, want %v", i, got[i].TemperatureC, want[i].TemperatureC)
			}
			if got[i].VoltageV != want[i].VoltageV {
				t.Errorf("row %d voltage_v = %v, want %v", i, got[i].VoltageV, want[i].VoltageV)
			}
			if got[i].CurrentA != want[i].CurrentA {
				t.Errorf("row %d current_a = %v, want %v", i, got[i].CurrentA, want[i].CurrentA)
			}
			if !reflect.DeepEqual(got[i].Lat, want[i].Lat) {
				t.Errorf("row %d lat = %v, want %v", i, got[i].Lat, want[i].Lat)
			}
			if !reflect.DeepEqual(got[i].Long, want[i].Long) {
				t.Errorf("row %d long = %v, want %v", i, got[i].Long, want[i].Long)
			}
		}
	})

	t.Run("S-013 empty batch writes nothing", func(t *testing.T) {
		if _, err := pool.Exec(context.Background(), "TRUNCATE device_telemetry"); err != nil {
			t.Fatalf("truncate device_telemetry: %v", err)
		}

		if err := inserter.InsertBatch(context.Background(), nil); err != nil {
			t.Fatalf("InsertBatch(nil) = %v, want nil", err)
		}
		if got := countQuery(t, pool, "SELECT count(*) FROM device_telemetry"); got != 0 {
			t.Errorf("device_telemetry has %d rows after an empty batch, want 0", got)
		}
	})

	t.Run("S-014 hostile device_id stores verbatim and the table survives", func(t *testing.T) {
		if _, err := pool.Exec(context.Background(), "TRUNCATE device_telemetry"); err != nil {
			t.Fatalf("truncate device_telemetry: %v", err)
		}

		const hostile = "dev'; DROP TABLE device_telemetry;--"
		if err := inserter.InsertBatch(context.Background(), []ingestion.TelemetryRow{{
			DeviceID: hostile,
			Time:     time.Date(2026, 4, 5, 6, 7, 8, 0, time.UTC),
			Sequence: 1,
		}}); err != nil {
			t.Fatalf("InsertBatch(hostile literal): %v", err)
		}

		if !tableExists(t, pool, "device_telemetry") {
			t.Fatal("device_telemetry was dropped by the hostile literal")
		}
		got := countQuery(t, pool, "SELECT count(*) FROM device_telemetry WHERE device_id = $1", hostile)
		if got != 1 {
			t.Errorf("rows with the hostile device_id = %d, want 1", got)
		}
	})
}

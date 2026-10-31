package ingestion

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

// syncInserter is the concurrency-safe fake used by the worker-pool tests.
// InsertBatch may run on several workers at once, so every captured field is
// guarded. gate lets a test pin a worker inside InsertBatch; entered receives
// one token per call so tests can synchronise without sleeping.
type syncInserter struct {
	mu      sync.Mutex
	batches [][]TelemetryRow
	err     error
	gate    <-chan struct{}
	entered chan struct{}
}

func (s *syncInserter) InsertBatch(_ context.Context, rows []TelemetryRow) error {
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
	}
	if s.gate != nil {
		<-s.gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batches = append(s.batches, append([]TelemetryRow(nil), rows...))
	return s.err
}

func (s *syncInserter) rows() []TelemetryRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TelemetryRow, 0, len(s.batches))
	for _, batch := range s.batches {
		out = append(out, batch...)
	}
	return out
}

func (s *syncInserter) batchSizes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	sizes := make([]int, len(s.batches))
	for i, batch := range s.batches {
		sizes[i] = len(batch)
	}
	return sizes
}

func testPoolConfig() WorkerPoolConfig {
	return WorkerPoolConfig{
		Workers:       4,
		QueueCapacity: 64,
		BatchMax:      64,
		BatchWait:     200 * time.Millisecond,
	}
}

func newTestPool(t *testing.T, inserter BatchInserter, cfg WorkerPoolConfig) *WorkerPool {
	t.Helper()
	pool, err := NewWorkerPool(inserter, cfg)
	if err != nil {
		t.Fatalf("NewWorkerPool: %v", err)
	}
	return pool
}

func closePool(t *testing.T, pool *WorkerPool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func row(device string, seq int64) TelemetryRow {
	return TelemetryRow{DeviceID: device, Time: time.Unix(seq, 0).UTC(), Sequence: seq}
}

func sequencesFor(rows []TelemetryRow, device string) []int64 {
	var seqs []int64
	for _, r := range rows {
		if r.DeviceID == device {
			seqs = append(seqs, r.Sequence)
		}
	}
	return seqs
}

func assertSequences(t *testing.T, got []int64, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d rows %v, want %d %v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d sequence = %d, want %d (full: %v)", i, got[i], want[i], got)
		}
	}
}

func TestParseWorkerPoolConfig(t *testing.T) {
	lookup := func(values map[string]string) func(string) string {
		return func(key string) string { return values[key] }
	}

	t.Run("R-005 defaults", func(t *testing.T) {
		got, err := ParseWorkerPoolConfig(lookup(nil))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := WorkerPoolConfig{Workers: 4, QueueCapacity: 256, BatchMax: 64, BatchWait: 200 * time.Millisecond}
		if got != want {
			t.Fatalf("config = %+v, want %+v", got, want)
		}
	})

	t.Run("R-005 env overrides", func(t *testing.T) {
		got, err := ParseWorkerPoolConfig(lookup(map[string]string{
			"INGEST_WORKERS":    "8",
			"INGEST_QUEUE":      "1024",
			"INGEST_BATCH_MAX":  "32",
			"INGEST_BATCH_WAIT": "50ms",
		}))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := WorkerPoolConfig{Workers: 8, QueueCapacity: 1024, BatchMax: 32, BatchWait: 50 * time.Millisecond}
		if got != want {
			t.Fatalf("config = %+v, want %+v", got, want)
		}
	})

	invalid := []struct {
		name   string
		values map[string]string
	}{
		{"R-005 rejects workers below one", map[string]string{"INGEST_WORKERS": "0"}},
		{"R-005 rejects non-numeric workers", map[string]string{"INGEST_WORKERS": "many"}},
		{"R-005 rejects negative queue", map[string]string{"INGEST_QUEUE": "-1"}},
		{"R-005 rejects zero batch max", map[string]string{"INGEST_BATCH_MAX": "0"}},
		{"R-005 rejects negative batch wait", map[string]string{"INGEST_BATCH_WAIT": "-1ms"}},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := ParseWorkerPoolConfig(lookup(tt.values)); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}

func TestNewWorkerPoolRejectsInvalidConfig(t *testing.T) {
	valid := testPoolConfig()

	cases := []struct {
		name     string
		inserter BatchInserter
		cfg      WorkerPoolConfig
	}{
		{"R-005 rejects nil inserter", nil, valid},
		{"R-005 rejects zero workers", &syncInserter{}, WorkerPoolConfig{Workers: 0, QueueCapacity: 1, BatchMax: 1}},
		{"R-005 rejects negative queue capacity", &syncInserter{}, WorkerPoolConfig{Workers: 1, QueueCapacity: -1, BatchMax: 1}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewWorkerPool(tt.inserter, tt.cfg); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}

func TestShardFor(t *testing.T) {
	pool := newTestPool(t, &syncInserter{}, testPoolConfig())
	defer closePool(t, pool)

	t.Run("S-011 deterministic sharding", func(t *testing.T) {
		for _, device := range []string{"device-a", "device-b", "sensor-0001", "node-42"} {
			first := pool.ShardFor(device)
			for i := 0; i < 10; i++ {
				if got := pool.ShardFor(device); got != first {
					t.Fatalf("shard for %q drifted: %d != %d", device, got, first)
				}
			}
			if first < 0 || first >= testPoolConfig().Workers {
				t.Fatalf("shard for %q = %d, want in [0,%d)", device, first, testPoolConfig().Workers)
			}
		}
	})

	t.Run("S-011 spreads devices across workers", func(t *testing.T) {
		seen := make(map[int]struct{})
		for i := 0; i < 64; i++ {
			seen[pool.ShardFor(fmt.Sprintf("device-%03d", i))] = struct{}{}
		}
		if len(seen) != testPoolConfig().Workers {
			t.Fatalf("devices hit %d distinct shards, want %d", len(seen), testPoolConfig().Workers)
		}
	})
}

func TestWorkerPoolPerDeviceOrdering(t *testing.T) {
	t.Run("S-010 per-device ordering", func(t *testing.T) {
		inserter := &syncInserter{}
		pool := newTestPool(t, inserter, testPoolConfig())

		sent := []TelemetryRow{
			row("device-a", 1), row("device-b", 1),
			row("device-a", 2), row("device-c", 1),
			row("device-a", 3), row("device-b", 2),
			row("device-a", 4), row("device-c", 2),
			row("device-a", 5), row("device-b", 3),
		}
		ctx := context.Background()
		for _, r := range sent {
			if err := pool.Enqueue(ctx, r); err != nil {
				t.Fatalf("Enqueue(%s/%d): %v", r.DeviceID, r.Sequence, err)
			}
		}
		closePool(t, pool)

		rows := inserter.rows()
		if len(rows) != len(sent) {
			t.Fatalf("inserted %d rows, want %d", len(rows), len(sent))
		}
		assertSequences(t, sequencesFor(rows, "device-a"), []int64{1, 2, 3, 4, 5})
		assertSequences(t, sequencesFor(rows, "device-b"), []int64{1, 2, 3})
		assertSequences(t, sequencesFor(rows, "device-c"), []int64{1, 2})

		// Interim WU3 behavior: one InsertBatch call per row. WU4 replaces
		// this with size-or-timer batching.
		for i, size := range inserter.batchSizes() {
			if size != 1 {
				t.Fatalf("batch %d has %d rows, want 1 (interim per-row dispatch)", i, size)
			}
		}
	})
}

func TestWorkerPoolBoundedBacklog(t *testing.T) {
	gate := make(chan struct{})
	inserter := &syncInserter{gate: gate, entered: make(chan struct{}, 1)}
	cfg := testPoolConfig()
	cfg.Workers = 1
	cfg.QueueCapacity = 1
	pool := newTestPool(t, inserter, cfg)
	ctx := context.Background()

	// First row is taken by the sole worker, which parks inside InsertBatch.
	if err := pool.Enqueue(ctx, row("device-a", 1)); err != nil {
		t.Fatalf("Enqueue first: %v", err)
	}
	select {
	case <-inserter.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never entered InsertBatch")
	}

	// Second row fills the capacity-one queue.
	if err := pool.Enqueue(ctx, row("device-a", 2)); err != nil {
		t.Fatalf("Enqueue second: %v", err)
	}

	t.Run("S-012 saturated producers block", func(t *testing.T) {
		sent := make(chan error, 1)
		go func() { sent <- pool.Enqueue(ctx, row("device-a", 3)) }()

		select {
		case err := <-sent:
			t.Fatalf("third Enqueue returned %v while queue was full; producers did not block", err)
		case <-time.After(100 * time.Millisecond):
		}

		close(gate)
		select {
		case err := <-sent:
			if err != nil {
				t.Fatalf("blocked Enqueue: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("blocked Enqueue never resumed after capacity freed")
		}
		closePool(t, pool)

		assertSequences(t, sequencesFor(inserter.rows(), "device-a"), []int64{1, 2, 3})
	})
}

func TestWorkerPoolEnqueueHonorsCancellation(t *testing.T) {
	gate := make(chan struct{})
	defer close(gate)
	inserter := &syncInserter{gate: gate, entered: make(chan struct{}, 1)}
	cfg := testPoolConfig()
	cfg.Workers = 1
	cfg.QueueCapacity = 1
	pool := newTestPool(t, inserter, cfg)

	ctx := context.Background()
	if err := pool.Enqueue(ctx, row("device-a", 1)); err != nil {
		t.Fatalf("Enqueue first: %v", err)
	}
	select {
	case <-inserter.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("worker never entered InsertBatch")
	}
	if err := pool.Enqueue(ctx, row("device-a", 2)); err != nil {
		t.Fatalf("Enqueue second: %v", err)
	}

	t.Run("S-012 canceled enqueue returns wrapped context error", func(t *testing.T) {
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		err := pool.Enqueue(canceled, row("device-a", 3))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Enqueue error = %v, want wrapped context.Canceled", err)
		}
	})
}

func TestWorkerPoolRejectsEnqueueAfterClose(t *testing.T) {
	inserter := &syncInserter{}
	pool := newTestPool(t, inserter, testPoolConfig())
	closePool(t, pool)

	t.Run("S-013 enqueue after close is rejected", func(t *testing.T) {
		err := pool.Enqueue(context.Background(), row("device-a", 1))
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("Enqueue after Close = %v, want ErrPoolClosed", err)
		}
	})
}

func TestWorkerPoolCleanDrain(t *testing.T) {
	inserter := &syncInserter{}
	cfg := testPoolConfig()
	cfg.Workers = 2
	cfg.QueueCapacity = 16

	before := runtime.NumGoroutine()
	pool := newTestPool(t, inserter, cfg)

	const total = 40
	for i := 0; i < total; i++ {
		if err := pool.Enqueue(context.Background(), row(fmt.Sprintf("device-%d", i%7), int64(i))); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pool.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	t.Run("S-013 drain inserts every dispatched row", func(t *testing.T) {
		if got := len(inserter.rows()); got != total {
			t.Fatalf("inserted %d rows, want %d", got, total)
		}
	})

	t.Run("S-013 no goroutine leak", func(t *testing.T) {
		deadline := time.Now().Add(2 * time.Second)
		for {
			if runtime.NumGoroutine() <= before+5 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("goroutine leak: before=%d after=%d", before, runtime.NumGoroutine())
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

func TestWorkerPoolRecordsInsertError(t *testing.T) {
	insertErr := errors.New("insert exploded")
	inserter := &syncInserter{err: insertErr}
	cfg := testPoolConfig()
	cfg.Workers = 1
	pool := newTestPool(t, inserter, cfg)

	for i := 1; i <= 3; i++ {
		if err := pool.Enqueue(context.Background(), row("device-a", int64(i))); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	closePool(t, pool)

	t.Run("insert error is recorded and does not stall drain", func(t *testing.T) {
		if got := len(inserter.rows()); got != 3 {
			t.Fatalf("inserted %d rows, want 3 (worker must keep draining after an error)", got)
		}
		err := pool.Err()
		if !errors.Is(err, insertErr) {
			t.Fatalf("Err() = %v, want wrapped insert error", err)
		}
	})
}

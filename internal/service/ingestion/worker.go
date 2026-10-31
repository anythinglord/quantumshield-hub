package ingestion

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"
)

// Worker pool configuration environment variables (design A5). BATCH_MAX and
// BATCH_WAIT are parsed and validated here so the runtime entrypoint reads one
// config object; their accumulation semantics arrive with the batcher.
const (
	envIngestWorkers   = "INGEST_WORKERS"
	envIngestQueue     = "INGEST_QUEUE"
	envIngestBatchMax  = "INGEST_BATCH_MAX"
	envIngestBatchWait = "INGEST_BATCH_WAIT"
)

const (
	defaultWorkers       = 4
	defaultQueueCapacity = 256
	defaultBatchMax      = 64
	defaultBatchWait     = 200 * time.Millisecond
)

// ErrPoolClosed reports an Enqueue that arrived after Close started.
var ErrPoolClosed = errors.New("ingestion: worker pool closed")

// WorkerPoolConfig carries every tuning knob for the sharded worker pool.
type WorkerPoolConfig struct {
	Workers       int
	QueueCapacity int
	BatchMax      int
	BatchWait     time.Duration
}

// DefaultWorkerPoolConfig returns the Phase 3 defaults from design A5.
func DefaultWorkerPoolConfig() WorkerPoolConfig {
	return WorkerPoolConfig{
		Workers:       defaultWorkers,
		QueueCapacity: defaultQueueCapacity,
		BatchMax:      defaultBatchMax,
		BatchWait:     defaultBatchWait,
	}
}

// ParseWorkerPoolConfig starts from the defaults and applies the INGEST_*
// overrides resolved through lookup. lookup is injected (normally os.Getenv) so
// the parsing rules are unit-testable without touching process globals.
func ParseWorkerPoolConfig(lookup func(string) string) (WorkerPoolConfig, error) {
	cfg := DefaultWorkerPoolConfig()

	if value := lookup(envIngestWorkers); value != "" {
		n, err := parseBoundedInt(envIngestWorkers, value, 1)
		if err != nil {
			return WorkerPoolConfig{}, err
		}
		cfg.Workers = n
	}
	if value := lookup(envIngestQueue); value != "" {
		n, err := parseBoundedInt(envIngestQueue, value, 0)
		if err != nil {
			return WorkerPoolConfig{}, err
		}
		cfg.QueueCapacity = n
	}
	if value := lookup(envIngestBatchMax); value != "" {
		n, err := parseBoundedInt(envIngestBatchMax, value, 1)
		if err != nil {
			return WorkerPoolConfig{}, err
		}
		cfg.BatchMax = n
	}
	if value := lookup(envIngestBatchWait); value != "" {
		d, err := time.ParseDuration(value)
		if err != nil {
			return WorkerPoolConfig{}, fmt.Errorf("ingestion: worker pool: parse %s=%q: %w", envIngestBatchWait, value, err)
		}
		if d < 0 {
			return WorkerPoolConfig{}, fmt.Errorf("ingestion: worker pool: %s=%q: must not be negative", envIngestBatchWait, value)
		}
		cfg.BatchWait = d
	}

	return cfg, nil
}

func parseBoundedInt(name, value string, minimum int) (int, error) {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("ingestion: worker pool: parse %s=%q: %w", name, value, err)
	}
	if n < minimum {
		return 0, fmt.Errorf("ingestion: worker pool: %s=%q: must be at least %d", name, value, minimum)
	}
	return n, nil
}

// WorkerPool dispatches telemetry rows to a fixed set of workers. Rows are
// sharded by hash(device_id) % Workers, so every row for one device lands on
// the same worker and per-device FIFO ordering is preserved (design A5).
// Each shard owns a bounded queue: when saturated, Enqueue blocks (selecting on
// ctx) instead of growing memory without limit.
type WorkerPool struct {
	inserter BatchInserter
	cfg      WorkerPoolConfig

	queues []chan TelemetryRow

	mu     sync.RWMutex
	closed bool

	closeOnce sync.Once
	waitOnce  sync.Once
	done      chan struct{}
	wg        sync.WaitGroup

	errMu    sync.Mutex
	firstErr error
}

// NewWorkerPool starts cfg.Workers goroutines and returns a ready pool. The
// caller owns shutdown and must call Close to drain the queues.
func NewWorkerPool(inserter BatchInserter, cfg WorkerPoolConfig) (*WorkerPool, error) {
	if inserter == nil {
		return nil, errors.New("ingestion: worker pool: inserter is nil")
	}
	if cfg.Workers < 1 {
		return nil, fmt.Errorf("ingestion: worker pool: workers=%d: must be at least 1", cfg.Workers)
	}
	if cfg.QueueCapacity < 0 {
		return nil, fmt.Errorf("ingestion: worker pool: queue capacity=%d: must not be negative", cfg.QueueCapacity)
	}
	if cfg.BatchMax < 1 {
		return nil, fmt.Errorf("ingestion: worker pool: batch max=%d: must be at least 1", cfg.BatchMax)
	}
	if cfg.BatchWait < 0 {
		return nil, fmt.Errorf("ingestion: worker pool: batch wait=%s: must not be negative", cfg.BatchWait)
	}

	p := &WorkerPool{
		inserter: inserter,
		cfg:      cfg,
		queues:   make([]chan TelemetryRow, cfg.Workers),
		done:     make(chan struct{}),
	}
	for i := range p.queues {
		p.queues[i] = make(chan TelemetryRow, cfg.QueueCapacity)
	}

	p.wg.Add(cfg.Workers)
	for i := range p.queues {
		go p.run(i)
	}

	return p, nil
}

// ShardFor reports the worker a device is routed to. The mapping is pure and
// stable: same device and worker count always yield the same shard.
func (p *WorkerPool) ShardFor(deviceID string) int {
	return shardIndex(deviceID, len(p.queues))
}

// Enqueue routes row to its device's shard. It blocks while that shard's queue
// is full, returning a wrapped ctx error if the caller gives up first, and
// ErrPoolClosed once Close has started.
func (p *WorkerPool) Enqueue(ctx context.Context, row TelemetryRow) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return ErrPoolClosed
	}

	shard := shardIndex(row.DeviceID, len(p.queues))
	select {
	case p.queues[shard] <- row:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("ingestion: worker pool: enqueue device %q: %w", row.DeviceID, ctx.Err())
	}
}

// Close stops accepting new rows and drains every queued row before returning.
// The WaitGroup guarantee means no worker goroutine outlives a successful
// Close (spec S-013).
func (p *WorkerPool) Close(ctx context.Context) error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		for _, q := range p.queues {
			close(q)
		}
		p.mu.Unlock()
	})

	p.waitOnce.Do(func() {
		go func() {
			p.wg.Wait()
			close(p.done)
		}()
	})

	select {
	case <-p.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("ingestion: worker pool: drain: %w", ctx.Err())
	}
}

// Err returns the first InsertBatch error observed by any worker, wrapped with
// its shard and device context. It is safe to call concurrently with workers.
func (p *WorkerPool) Err() error {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	return p.firstErr
}

func (p *WorkerPool) run(shard int) {
	defer p.wg.Done()
	for row := range p.queues[shard] {
		// Insert work is detached from any request/stream context on purpose:
		// a cancelled client must not drop rows already accepted, and the final
		// drain during shutdown has to run to completion (design A5/A8).
		if err := p.inserter.InsertBatch(context.Background(), []TelemetryRow{row}); err != nil {
			p.recordErr(fmt.Errorf("ingestion: worker pool: shard %d: insert device %q: %w", shard, row.DeviceID, err))
		}
	}
}

func (p *WorkerPool) recordErr(err error) {
	p.errMu.Lock()
	defer p.errMu.Unlock()
	if p.firstErr == nil {
		p.firstErr = err
	}
}

// shardIndex maps a device to a worker with FNV-1a. The hash is computed from
// the device ID bytes only, never Go's randomized map iteration (design A5).
func shardIndex(deviceID string, workers int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(deviceID)) // hash.Hash.Write never fails
	return int(h.Sum32() % uint32(workers))
}

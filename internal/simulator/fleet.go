package simulator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
)

const outBuffer = 1024 // shared fan-in capacity

// ConsumerFunc is the sink seam. Drain out until closed; return non-nil only to abort.
type ConsumerFunc func(ctx context.Context, out <-chan *apiv1.TelemetryBatch) error

// CountingConsumer is the default stub.
type CountingConsumer struct {
	received atomic.Uint64
}

// NewCountingConsumer creates a new counting consumer.
func NewCountingConsumer() *CountingConsumer {
	return &CountingConsumer{}
}

// Consume drains until close and tallies.
func (c *CountingConsumer) Consume(_ context.Context, out <-chan *apiv1.TelemetryBatch) error {
	for range out {
		c.received.Add(1)
	}
	return nil
}

// Received returns total received.
func (c *CountingConsumer) Received() uint64 {
	return c.received.Load()
}

// Fleet runs Devices independent simulated devices feeding one bounded
// fan-in channel. Under a stalled sink, pending ticks are dropped by design
// (D8 / R-003.2): a device blocked sending into the full fan-in stops
// consuming its tick channel, so excess ticks are discarded instead of
// queued unboundedly.
type Fleet struct {
	devices int
	rate    float64
	consume ConsumerFunc
	newTick func(idx int, interval time.Duration) (<-chan time.Time, func())
}

// NewFleet creates a Fleet. nil consume uses unobserved CountingConsumer.
func NewFleet(devices int, rate float64, consume ConsumerFunc) *Fleet {
	if consume == nil {
		c := NewCountingConsumer()
		consume = c.Consume
	}
	return &Fleet{
		devices: devices,
		rate:    rate,
		consume: consume,
		newTick: defaultNewTick,
	}
}

func intervalFor(rate float64) time.Duration {
	d := time.Duration(float64(time.Second) / rate)
	if d < time.Nanosecond {
		return time.Nanosecond
	}
	return d
}

func defaultNewTick(_ int, d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)
	return t.C, func() { t.Stop() }
}

func (f *Fleet) runDevice(ctx context.Context, idx int, out chan<- *apiv1.TelemetryBatch, wg *sync.WaitGroup) {
	defer wg.Done()
	deviceID := fmt.Sprintf("device-%04d", idx)
	g := New(uint64(idx))
	iv := intervalFor(f.rate)
	tick, stop := f.newTick(idx, iv)
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ts := <-tick:
			batch := g.Next(deviceID, ts)
			select {
			case out <- batch:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Run blocks until cancel/abort; nil on clean cancel. Ticks that arrive
// while a device is stalled on a full fan-in are dropped by design
// (D8 / R-003.2).
func (f *Fleet) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	out := make(chan *apiv1.TelemetryBatch, outBuffer)

	var wg sync.WaitGroup
	for i := 0; i < f.devices; i++ {
		wg.Add(1)
		go f.runDevice(runCtx, i, out, &wg)
	}

	closerDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(out)
		close(closerDone)
	}()

	consumeErrCh := make(chan error, 1)
	go func() {
		err := f.consume(runCtx, out)
		if err != nil {
			cancel()
		}
		consumeErrCh <- err
	}()

	// D6 join: wait for the consumer, then cancel devices and join the
	// closer. A select-with-default here would risk dropping a consumer
	// error, so the receive is unconditional.
	consumeErr := <-consumeErrCh
	cancel()
	<-closerDone

	if consumeErr != nil {
		return fmt.Errorf("simulator: consumer failed: %w", consumeErr)
	}
	return nil
}

package simulator

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
)

// tickBoard provides per-idx tick feeds for deterministic tests.
type tickBoard struct {
	mu      sync.Mutex
	chans   map[int]chan time.Time
	stopped chan struct{} // one token per device defer stop()
}

func newTickBoard() *tickBoard {
	return &tickBoard{
		chans:   make(map[int]chan time.Time),
		stopped: make(chan struct{}, 4096),
	}
}

func (tb *tickBoard) bind(f *Fleet) {
	f.newTick = func(idx int, _ time.Duration) (<-chan time.Time, func()) {
		tb.mu.Lock()
		defer tb.mu.Unlock()
		ch, ok := tb.chans[idx]
		if !ok {
			ch = make(chan time.Time, 1)
			tb.chans[idx] = ch
		}
		return ch, func() {
			// stop runs in the device's defer stack after it left the emit
			// loop, so a test that has observed n stops knows no device can
			// send to the fan-in any more.
			select {
			case tb.stopped <- struct{}{}:
			default: // buffer full; keep device exit non-blocking
			}
		}
	}
}

// feed delivers one tick, blocking until the per-device tick channel accepts it.
func (tb *tickBoard) feed(idx int, ts time.Time) {
	tb.mu.Lock()
	ch, ok := tb.chans[idx]
	if !ok {
		ch = make(chan time.Time, 1)
		tb.chans[idx] = ch
	}
	tb.mu.Unlock()
	ch <- ts
}

// feedNB delivers one tick without blocking; drops it when the per-device
// tick channel is full (oversupply feeds for the backpressure scenarios).
func (tb *tickBoard) feedNB(idx int, ts time.Time) {
	tb.mu.Lock()
	ch, ok := tb.chans[idx]
	if !ok {
		ch = make(chan time.Time, 1)
		tb.chans[idx] = ch
	}
	tb.mu.Unlock()
	select {
	case ch <- ts:
	default:
	}
}

// waitStopped waits until n devices have executed their stop() defer
// (i.e. left runDevice's emit loop for good).
func (tb *tickBoard) waitStopped(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-tb.stopped:
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for device stop (%d/%d)", i, n)
		}
	}
}

// waitRun waits for Fleet.Run to return.
func waitRun(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

// recorder collects batches and supports gated (stalled) consumption.
type recorder struct {
	mu        sync.Mutex
	got       []*apiv1.TelemetryBatch
	notify    chan struct{} // buffered cap 1; coalesced receipt signal
	gate      chan struct{} // nil = ungated; block before receive until closed
	gateAfter int           // gate once counter >= gateAfter (checked before receive)
	closed    bool
	counter   atomic.Uint64
}

func newRecorder() *recorder {
	return &recorder{
		notify: make(chan struct{}, 1),
	}
}

func (r *recorder) Consume(_ context.Context, out <-chan *apiv1.TelemetryBatch) error {
	for {
		if r.gate != nil && r.counter.Load() >= uint64(r.gateAfter) {
			<-r.gate // stall here; tests open the gate after cancel+waitStopped
		}
		b, ok := <-out
		if !ok {
			break
		}
		r.mu.Lock()
		r.got = append(r.got, b)
		r.mu.Unlock()
		r.counter.Add(1)
		select {
		case r.notify <- struct{}{}:
		default:
		}
	}
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return nil
}

func (r *recorder) Received() uint64 { return r.counter.Load() }

func (r *recorder) waitNotify(t *testing.T) {
	t.Helper()
	select {
	case <-r.notify:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for notify")
	}
}

func (r *recorder) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func TestIntervalFor(t *testing.T) {
	tests := []struct {
		name string
		rate float64
		want time.Duration
	}{
		{"S-004.1-rate-1", 1.0, time.Second},
		{"S-004.1-rate-5", 5.0, 200 * time.Millisecond},
		{"S-004.1-rate-00001", 0.0001, 10000 * time.Second},
		{"S-004.2-rate-2e9", 2e9, time.Nanosecond},
		{"S-004.2-rate-1e18", 1e18, time.Nanosecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := intervalFor(tt.rate)
			if got != tt.want {
				t.Fatalf("intervalFor(%v) = %v, want %v", tt.rate, got, tt.want)
			}
		})
	}
	t.Run("S-004.2-defaultNewTick-no-panic", func(t *testing.T) {
		ch, stop := defaultNewTick(0, intervalFor(2e9))
		if ch == nil {
			t.Fatal("nil channel")
		}
		stop()
	})
}

func TestRunConcurrentEmission(t *testing.T) {
	t.Run("S-001.1 three devices emit distinct ids", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tb := newTickBoard()
		rec := newRecorder()
		f := NewFleet(3, 1.0, rec.Consume)
		tb.bind(f)
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		ts := time.Unix(0, 0)
		for i := 0; i < 3; i++ {
			tb.feed(i, ts)
			rec.waitNotify(t)
		}
		rec.mu.Lock()
		seen := map[string]bool{}
		for _, b := range rec.got {
			if b != nil {
				seen[b.DeviceId] = true
			}
		}
		rec.mu.Unlock()
		if !seen["device-0000"] || !seen["device-0001"] || !seen["device-0002"] {
			t.Fatalf("missing device ids: %#v", seen)
		}
		cancel()
		if err := waitRun(t, done); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("S-001.2 two hundred distinct device ids", func(t *testing.T) {
		const n = 200
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tb := newTickBoard()
		rec := newRecorder()
		f := NewFleet(n, 1.0, rec.Consume)
		tb.bind(f)
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		ts := time.Unix(0, 0)
		for i := 0; i < n; i++ {
			tb.feed(i, ts)
			rec.waitNotify(t)
		}
		if rec.Received() != n {
			t.Fatalf("received = %d, want %d", rec.Received(), n)
		}
		rec.mu.Lock()
		seen := map[string]bool{}
		for _, b := range rec.got {
			seen[b.DeviceId] = true
		}
		rec.mu.Unlock()
		if len(seen) != n {
			t.Fatalf("distinct ids = %d, want %d", len(seen), n)
		}
		for i := 0; i < n; i++ {
			if id := fmt.Sprintf("device-%04d", i); !seen[id] {
				t.Fatalf("missing %s in %d distinct ids", id, len(seen))
			}
		}
		cancel()
		if err := waitRun(t, done); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("S-006.1 run returns nil on clean cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tb := newTickBoard()
		rec := newRecorder()
		f := NewFleet(2, 1.0, rec.Consume)
		tb.bind(f)
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		tb.feed(0, time.Unix(0, 0))
		rec.waitNotify(t)
		cancel()
		if err := waitRun(t, done); err != nil {
			t.Fatalf("Run returned %v, want nil on clean cancel", err)
		}
	})

	t.Run("S-005.2 device-0007 id", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tb := newTickBoard()
		rec := newRecorder()
		f := NewFleet(10, 1.0, rec.Consume)
		tb.bind(f)
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		tb.feed(7, time.Unix(0, 0))
		rec.waitNotify(t)
		rec.mu.Lock()
		var id string
		if len(rec.got) > 0 {
			id = rec.got[0].DeviceId
		}
		rec.mu.Unlock()
		if id != "device-0007" {
			t.Fatalf("DeviceId = %q, want device-0007", id)
		}
		cancel()
		if err := waitRun(t, done); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestPerDeviceStreamIntegrity(t *testing.T) {
	t.Run("S-001.3 per-device sequences are monotonic", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tb := newTickBoard()
		rec := newRecorder()
		f := NewFleet(2, 1.0, rec.Consume)
		tb.bind(f)
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		base := time.Unix(1000, 0)
		for i := 0; i < 2; i++ {
			for j := 0; j < 5; j++ {
				tb.feed(i, base.Add(time.Duration(j)*time.Second))
				rec.waitNotify(t)
			}
		}
		rec.mu.Lock()
		byDev := map[string][]uint64{}
		for _, b := range rec.got {
			if b != nil && len(b.Readings) > 0 {
				byDev[b.DeviceId] = append(byDev[b.DeviceId], b.Readings[0].Sequence)
			}
		}
		rec.mu.Unlock()
		if len(byDev) != 2 {
			t.Fatalf("devices seen = %d, want 2", len(byDev))
		}
		for dev, seqs := range byDev {
			for k := range seqs {
				if seqs[k] != uint64(k) {
					t.Fatalf("device %s sequence broken at %d: %v", dev, k, seqs)
				}
			}
		}
		cancel()
		if err := waitRun(t, done); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

// runBackpressureCase lets the consumer receive exactly k batches, then
// oversupplies 10x the fan-in capacity with non-blocking feeds while the sink
// is stalled. Returns (k, final received, Run error). The cancellation
// sequence is cancel -> waitStopped -> open gate so the drain bound is exact:
// the fan-in is full while gated, so blocked devices exit deterministically
// on ctx.Done, and once all stop() defers fired no device can push again.
func runBackpressureCase(t *testing.T, k int) (uint64, uint64, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tb := newTickBoard()
	rec := newRecorder()
	rec.gate = make(chan struct{}) // held closed until the drain phase
	rec.gateAfter = k
	f := NewFleet(1, 1.0, rec.Consume)
	tb.bind(f)
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()

	ts := time.Unix(0, 0)
	for i := 0; i < k; i++ {
		tb.feed(0, ts.Add(time.Duration(i)))
		rec.waitNotify(t)
	}
	for i := 0; i < 10*outBuffer; i++ {
		tb.feedNB(0, ts.Add(time.Duration(k+i)))
	}
	stall := rec.Received()
	if stall != uint64(k) {
		t.Fatalf("Received() = %d during stall, want exactly %d", stall, k)
	}
	cancel()
	tb.waitStopped(t, 1)
	close(rec.gate)
	err := waitRun(t, done)
	return stall, rec.Received(), err
}

func TestBoundedBackpressure(t *testing.T) {
	// Spec R-002 locks the fan-in capacity at 1024. Pin the SPEC LITERAL, not
	// the identifier: a bounds assertion that reads outBuffer moves together
	// with a const regression and cannot kill it (task-2.2 W1 precedent).
	t.Run("S-002.1 fan-in capacity pinned to spec literal 1024", func(t *testing.T) {
		if outBuffer != 1024 {
			t.Fatalf("outBuffer = %d, want spec-locked literal 1024", outBuffer)
		}
	})

	t.Run("S-002.1 stall receives exactly k", func(t *testing.T) {
		const k = 5
		stall, _, err := runBackpressureCase(t, k)
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
		if stall != k {
			t.Fatalf("stall received = %d, want %d", stall, k)
		}
	})

	t.Run("S-002.2 drain bounded by fan-in", func(t *testing.T) {
		const k = 5
		stall, final, err := runBackpressureCase(t, k)
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
		if stall != k {
			t.Fatalf("stall received = %d, want %d", stall, k)
		}
		if final > uint64(k+outBuffer) {
			t.Fatalf("final received = %d, want <= k+outBuffer = %d", final, k+outBuffer)
		}
	})
}

func TestTickDropUnderStall(t *testing.T) {
	t.Run("S-003.1 ticks dropped under stalled sink", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tb := newTickBoard()
		rec := newRecorder()
		rec.gate = make(chan struct{}) // gate from the very first receive
		rec.gateAfter = 0
		f := NewFleet(1, 1.0, rec.Consume)
		tb.bind(f)
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		ts := time.Unix(0, 0)
		const extra = 10
		fed := outBuffer + extra
		for i := 0; i < fed; i++ {
			tb.feedNB(0, ts.Add(time.Duration(i)))
		}
		// Cancel while the sink is stalled; then open the gate and drain only
		// what the bounded fan-in actually held (ticks that never fit are
		// dropped by design, D8/R-003.2).
		cancel()
		tb.waitStopped(t, 1)
		close(rec.gate)
		if err := waitRun(t, done); err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
		got := rec.Received()
		// Literal bound (not outBuffer) so a const regression to a larger
		// capacity cannot move the bound along with it (spec R-002 = 1024).
		if got > 1024 {
			t.Fatalf("received = %d, want <= spec-locked 1024", got)
		}
		if got >= uint64(fed) {
			t.Fatalf("expected tick drop: received = %d, fed = %d", got, fed)
		}
	})
}

func TestFleetDeterminism(t *testing.T) {
	runCase := func(t *testing.T) []*apiv1.TelemetryBatch {
		t.Helper()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tb := newTickBoard()
		rec := newRecorder()
		f := NewFleet(2, 1.0, rec.Consume)
		tb.bind(f)
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		base := time.Unix(12345, 0)
		for i := 0; i < 2; i++ {
			for j := 0; j < 3; j++ {
				tb.feed(i, base.Add(time.Duration(j)))
				rec.waitNotify(t)
			}
		}
		// Snapshot under the recorder mutex; blocking feed + waitNotify make
		// the batch count stable (no further ticks are queued).
		rec.mu.Lock()
		got := make([]*apiv1.TelemetryBatch, len(rec.got))
		copy(got, rec.got)
		rec.mu.Unlock()
		cancel()
		if err := waitRun(t, done); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return got
	}
	t.Run("S-005.1 same devices and timestamps yield identical streams", func(t *testing.T) {
		snap1 := runCase(t)
		snap2 := runCase(t)
		if len(snap1) != len(snap2) {
			t.Fatalf("length mismatch: %d vs %d", len(snap1), len(snap2))
		}
		for i := range snap1 {
			if snap1[i].DeviceId != snap2[i].DeviceId {
				t.Fatalf("batch %d DeviceId mismatch: %s vs %s", i, snap1[i].DeviceId, snap2[i].DeviceId)
			}
			if len(snap1[i].Readings) > 0 && len(snap2[i].Readings) > 0 {
				assertSameReading(t, fmt.Sprintf("batch-%d", i), snap1[i].Readings[0], snap2[i].Readings[0])
			}
		}
	})

	// R-005 pins device i to seed uint64(i). Compare the fleet's stream
	// against a hand-driven reference generator for the same index: a
	// constant-seed mutant (New(0) for every device) breaks this, while
	// run-vs-run identity (above) cannot detect it.
	t.Run("S-005.1 fleet stream matches New(uint64(idx))", func(t *testing.T) {
		const idx = 7
		const ticks = 3
		deviceID := fmt.Sprintf("device-%04d", idx)
		base := time.Unix(20000, 0)
		var stamps []time.Time
		for i := 0; i < ticks; i++ {
			stamps = append(stamps, base.Add(time.Duration(i)))
		}
		ref := New(uint64(idx))
		want := make([]*apiv1.TelemetryBatch, 0, ticks)
		for _, ts := range stamps {
			want = append(want, ref.Next(deviceID, ts))
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tb := newTickBoard()
		rec := newRecorder()
		f := NewFleet(idx+1, 1.0, rec.Consume)
		tb.bind(f)
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		for _, ts := range stamps {
			tb.feed(idx, ts)
			rec.waitNotify(t)
		}
		rec.mu.Lock()
		got := make([]*apiv1.TelemetryBatch, len(rec.got))
		copy(got, rec.got)
		rec.mu.Unlock()
		cancel()
		if err := waitRun(t, done); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(got) != len(want) {
			t.Fatalf("received %d batches, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i].DeviceId != deviceID {
				t.Fatalf("batch %d DeviceId = %q, want %q", i, got[i].DeviceId, deviceID)
			}
			assertSameReading(t, fmt.Sprintf("seed-pin-%d", i), got[i].Readings[0], want[i].Readings[0])
		}
	})
}

func TestRunShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	tb := newTickBoard()
	rec := newRecorder()
	f := NewFleet(2, 1.0, rec.Consume)
	tb.bind(f)
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()
	ts := time.Unix(0, 0)
	for i := 0; i < 2; i++ {
		tb.feed(i, ts)
		rec.waitNotify(t)
	}
	before := runtime.NumGoroutine()
	cancel()
	err := waitRun(t, done)
	after := runtime.NumGoroutine()

	t.Run("S-006.1 clean cancel returns nil", func(t *testing.T) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("S-006.3 consumer channel closed and drained", func(t *testing.T) {
		if !rec.isClosed() {
			t.Fatal("consumer not closed")
		}
		if rec.Received() != 2 {
			t.Fatalf("expected 2 received, got %d", rec.Received())
		}
	})
	t.Run("S-006.2 no goroutine leak", func(t *testing.T) {
		if after > before+5 {
			t.Fatalf("goroutine leak: before=%d after=%d", before, after)
		}
	})
}

func TestCountingConsumer(t *testing.T) {
	t.Run("S-007.1 counting consumer tallies and closes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		out := make(chan *apiv1.TelemetryBatch, 2)
		c := NewCountingConsumer()
		done := make(chan error, 1)
		go func() { done <- c.Consume(ctx, out) }()
		out <- &apiv1.TelemetryBatch{DeviceId: "d1"}
		out <- &apiv1.TelemetryBatch{DeviceId: "d2"}
		close(out)
		if err := waitRun(t, done); err != nil {
			t.Fatal(err)
		}
		if c.Received() != 2 {
			t.Fatalf("want 2 got %d", c.Received())
		}
	})
}

func TestAlternateConsumer(t *testing.T) {
	t.Run("S-007.2 alternate consumer receives before cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var got int32
		received := make(chan struct{}, 1)
		alt := func(ctx context.Context, out <-chan *apiv1.TelemetryBatch) error {
			for range out {
				atomic.AddInt32(&got, 1)
				select {
				case received <- struct{}{}:
				default:
				}
			}
			return nil
		}
		tb := newTickBoard()
		f := NewFleet(1, 1.0, alt)
		tb.bind(f)
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		tb.feed(0, time.Unix(0, 0))
		select {
		case <-received: // receipt signal BEFORE cancel — no race on got
		case <-time.After(2 * time.Second):
			t.Fatal("alternate consumer did not receive")
		}
		cancel()
		if err := waitRun(t, done); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if atomic.LoadInt32(&got) == 0 {
			t.Fatal("alternate consumer got nothing")
		}
	})
}

func TestExactTickBatchCorrespondence(t *testing.T) {
	t.Run("S-009.1 one batch per tick", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		tb := newTickBoard()
		rec := newRecorder()
		f := NewFleet(2, 1.0, rec.Consume)
		tb.bind(f)
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		n := 3
		for i := 0; i < 2; i++ {
			for j := 0; j < n; j++ {
				tb.feed(i, time.Unix(int64(j), 0))
				rec.waitNotify(t)
			}
		}
		if rec.Received() != uint64(2*n) {
			t.Fatalf("want %d got %d", 2*n, rec.Received())
		}
		cancel()
		if err := waitRun(t, done); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestScaleSmoke(t *testing.T) {
	t.Run("S-009.2 thousand devices emit", func(t *testing.T) {
		if testing.Short() {
			t.Skip("scale smoke skipped in -short")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		counter := NewCountingConsumer()
		f := NewFleet(1000, 100.0, counter.Consume)
		done := make(chan error, 1)
		go func() { done <- f.Run(ctx) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timeout")
		}
		if counter.Received() == 0 {
			t.Fatal("expected > 0")
		}
	})
}

package ingestion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// Compile-time proof that the handler satisfies the generated service contract.
var _ apiv1.TelemetryIngestionServiceServer = (*Handler)(nil)

// Compile-time proof that the fake stream satisfies the generated bidi contract.
var _ grpc.BidiStreamingServer[apiv1.TelemetryIngestRequest, apiv1.TelemetryIngestResponse] = (*fakeServerStream)(nil)

// serverRecv is one scripted outcome of fakeServerStream.Recv: a request or a
// terminal error.
type serverRecv struct {
	req *apiv1.TelemetryIngestRequest
	err error
}

// fakeServerStream is an in-process stand-in for the generated server-side
// stream. Requests are read from recvCh; closing the channel yields io.EOF the
// way a client half-close does. Every Send is recorded and announced on sentCh
// so tests synchronise without sleeping.
type fakeServerStream struct {
	grpc.ServerStream // unused; the handler only calls Recv, Send and Context

	ctx    context.Context
	recvCh chan serverRecv

	mu      sync.Mutex
	sent    []*apiv1.TelemetryIngestResponse
	sendErr error

	sentCh chan struct{}
}

func newFakeServerStream(ctx context.Context, script []serverRecv) *fakeServerStream {
	ch := make(chan serverRecv, len(script))
	for _, r := range script {
		ch <- r
	}
	close(ch)
	return &fakeServerStream{ctx: ctx, recvCh: ch, sentCh: make(chan struct{}, 64)}
}

// newOpenServerStream returns a stream whose recv half stays open until the
// test sends a terminal outcome (or closes recvCh).
func newOpenServerStream(ctx context.Context) *fakeServerStream {
	return &fakeServerStream{ctx: ctx, recvCh: make(chan serverRecv), sentCh: make(chan struct{}, 64)}
}

func (f *fakeServerStream) Context() context.Context { return f.ctx }

func (f *fakeServerStream) Recv() (*apiv1.TelemetryIngestRequest, error) {
	r, ok := <-f.recvCh
	if !ok {
		return nil, io.EOF
	}
	if r.err != nil {
		return nil, r.err
	}
	return r.req, nil
}

func (f *fakeServerStream) Send(resp *apiv1.TelemetryIngestResponse) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.mu.Lock()
	f.sent = append(f.sent, resp)
	f.mu.Unlock()

	select {
	case f.sentCh <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeServerStream) responses() []*apiv1.TelemetryIngestResponse {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*apiv1.TelemetryIngestResponse, len(f.sent))
	copy(out, f.sent)
	return out
}

func (f *fakeServerStream) responseCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

// waitForSends blocks until n responses have been sent, or fails.
func (f *fakeServerStream) waitForSends(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-f.sentCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for response %d (got %d)", i+1, f.responseCount())
		}
	}
}

// recordingDispatcher captures every row handed to the engine for persistence.
type recordingDispatcher struct {
	mu    sync.Mutex
	rows  []TelemetryRow
	errOn func(TelemetryRow) error
}

func (d *recordingDispatcher) Enqueue(_ context.Context, row TelemetryRow) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.errOn != nil {
		if err := d.errOn(row); err != nil {
			return err
		}
	}
	d.rows = append(d.rows, row)
	return nil
}

func (d *recordingDispatcher) captured() []TelemetryRow {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]TelemetryRow, len(d.rows))
	copy(out, d.rows)
	return out
}

type verifierCall struct {
	deviceID  string
	algorithm apiv1.SignatureAlgorithm
	payload   []byte
	signature []byte
}

// fakeVerifier records its arguments and returns a scripted error.
type fakeVerifier struct {
	mu     sync.Mutex
	err    error
	errFor func(deviceID string) error
	calls  []verifierCall
}

func (v *fakeVerifier) Verify(_ context.Context, deviceID string, alg apiv1.SignatureAlgorithm, payload, signature []byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls = append(v.calls, verifierCall{
		deviceID:  deviceID,
		algorithm: alg,
		payload:   append([]byte(nil), payload...),
		signature: append([]byte(nil), signature...),
	})
	if v.errFor != nil {
		return v.errFor(deviceID)
	}
	return v.err
}

func (v *fakeVerifier) recorded() []verifierCall {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := make([]verifierCall, len(v.calls))
	copy(out, v.calls)
	return out
}

func mustHandler(t *testing.T, dispatcher RowDispatcher, verifier Verifier) *Handler {
	t.Helper()
	h, err := NewHandler(dispatcher, verifier)
	if err != nil {
		t.Fatalf("NewHandler() error = %v, want nil", err)
	}
	return h
}

// readingsPayload marshals a TelemetryBatch holding one reading per sequence.
func readingsPayload(t *testing.T, deviceID string, sequences ...uint64) []byte {
	t.Helper()
	readings := make([]*apiv1.SensorReading, len(sequences))
	for i, seq := range sequences {
		readings[i] = &apiv1.SensorReading{Sequence: seq, TimestampNanos: int64(seq) * 1000}
	}
	return marshalBatch(t, &apiv1.TelemetryBatch{DeviceId: deviceID, Readings: readings})
}

func marshalBatch(t *testing.T, batch *apiv1.TelemetryBatch) []byte {
	t.Helper()
	payload, err := proto.Marshal(batch)
	if err != nil {
		t.Fatalf("proto.Marshal(TelemetryBatch) error = %v", err)
	}
	return payload
}

func ingestRequest(deviceID string, payload []byte) *apiv1.TelemetryIngestRequest {
	return &apiv1.TelemetryIngestRequest{
		DeviceId:           deviceID,
		Payload:            payload,
		Signature:          []byte("signature"),
		SignatureAlgorithm: apiv1.SignatureAlgorithm_ML_DSA_65,
	}
}

func TestNewHandlerRejectsNilDependencies(t *testing.T) {
	t.Run("nil dispatcher and verifier are rejected", func(t *testing.T) {
		if _, err := NewHandler(nil, NewAcceptAllVerifier()); err == nil {
			t.Fatal("NewHandler(nil dispatcher) error = nil, want rejection")
		}
		if _, err := NewHandler(&recordingDispatcher{}, nil); err == nil {
			t.Fatal("NewHandler(nil verifier) error = nil, want rejection")
		}
	})
}

// TestHandlerRowFidelity proves the wire batch is unmarshalled and every
// reading is flattened into a telemetry row without losing fields.
func TestHandlerRowFidelity(t *testing.T) {
	t.Run("S-001 flattens readings with nil-safe GPS", func(t *testing.T) {
		const timestamp = int64(1_700_000_000_000_000_001)
		lat, long := 12.5, -56.25
		batch := &apiv1.TelemetryBatch{
			DeviceId: "payload-device",
			Readings: []*apiv1.SensorReading{
				{
					Sequence:       11,
					TimestampNanos: timestamp,
					TemperatureC:   21.5,
					VoltageV:       3.32,
					CurrentA:       0.75,
					Position:       &apiv1.GpsPosition{Latitude: lat, Longitude: long},
				},
				{
					Sequence:       12,
					TimestampNanos: timestamp + 1,
					TemperatureC:   22.5,
					VoltageV:       3.31,
					CurrentA:       0.76,
				},
			},
		}

		dispatcher := &recordingDispatcher{}
		stream := newFakeServerStream(context.Background(), []serverRecv{
			{req: ingestRequest("device-001", marshalBatch(t, batch))},
		})
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		if err := h.StreamTelemetry(stream); err != nil {
			t.Fatalf("StreamTelemetry() error = %v, want nil", err)
		}

		rows := dispatcher.captured()
		if len(rows) != 2 {
			t.Fatalf("dispatched rows = %d, want 2", len(rows))
		}

		first := rows[0]
		if first.DeviceID != "device-001" {
			t.Errorf("row 0 DeviceID = %q, want request identity %q", first.DeviceID, "device-001")
		}
		if want := time.Unix(0, timestamp); !first.Time.Equal(want) {
			t.Errorf("row 0 Time = %v, want %v", first.Time, want)
		}
		if first.Sequence != 11 {
			t.Errorf("row 0 Sequence = %d, want 11", first.Sequence)
		}
		if first.TemperatureC != 21.5 || first.VoltageV != 3.32 || first.CurrentA != 0.75 {
			t.Errorf("row 0 measurements = (%v, %v, %v), want (21.5, 3.32, 0.75)",
				first.TemperatureC, first.VoltageV, first.CurrentA)
		}
		if first.Lat == nil || *first.Lat != lat {
			t.Errorf("row 0 Lat = %v, want %v", first.Lat, lat)
		}
		if first.Long == nil || *first.Long != long {
			t.Errorf("row 0 Long = %v, want %v", first.Long, long)
		}

		second := rows[1]
		if second.Sequence != 12 {
			t.Errorf("row 1 Sequence = %d, want 12", second.Sequence)
		}
		if second.Lat != nil || second.Long != nil {
			t.Errorf("row 1 GPS = (%v, %v), want nil for a fixed node", second.Lat, second.Long)
		}

		acks := stream.responses()
		if len(acks) != 1 {
			t.Fatalf("responses = %d, want 1", len(acks))
		}
		if got := acks[0].GetStatus(); got != apiv1.IngestStatus_INGEST_STATUS_ACCEPTED {
			t.Errorf("status = %v, want ACCEPTED", got)
		}
		if got := acks[0].GetAckedSequence(); got != 12 {
			t.Errorf("acked sequence = %d, want highest reading sequence 12", got)
		}
	})
}

// TestHandlerAckMonotonicity proves the per-device ack is the highest accepted
// sequence, not simply the last reading seen, and that it never regresses.
func TestHandlerAckMonotonicity(t *testing.T) {
	t.Run("S-001 out-of-order sequences ack the highest without regressing", func(t *testing.T) {
		dispatcher := &recordingDispatcher{}
		stream := newFakeServerStream(context.Background(), []serverRecv{
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 10, 7))},
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 5))},
		})
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		if err := h.StreamTelemetry(stream); err != nil {
			t.Fatalf("StreamTelemetry() error = %v, want nil", err)
		}

		acks := stream.responses()
		if len(acks) != 2 {
			t.Fatalf("responses = %d, want 2", len(acks))
		}
		if got := acks[0].GetAckedSequence(); got != 10 {
			t.Errorf("ack 0 acked sequence = %d, want highest reading 10", got)
		}
		if got := acks[1].GetAckedSequence(); got != 10 {
			t.Errorf("ack 1 acked sequence = %d, want it to stay at 10", got)
		}

		rows := dispatcher.captured()
		if len(rows) != 3 {
			t.Fatalf("dispatched rows = %d, want 3", len(rows))
		}
		if rows[0].Sequence != 10 || rows[1].Sequence != 7 || rows[2].Sequence != 5 {
			t.Errorf("row sequences = [%d %d %d], want send order [10 7 5]",
				rows[0].Sequence, rows[1].Sequence, rows[2].Sequence)
		}
	})
}

// TestHandlerDrainUntilEOF covers the drain contract: exactly one ack per
// received request and no return before the client half-closes.
func TestHandlerDrainUntilEOF(t *testing.T) {
	t.Run("S-005 N requests then io.EOF yields N acks", func(t *testing.T) {
		dispatcher := &recordingDispatcher{}
		stream := newFakeServerStream(context.Background(), []serverRecv{
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 1))},
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 2))},
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 3))},
		})
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		if err := h.StreamTelemetry(stream); err != nil {
			t.Fatalf("StreamTelemetry() error = %v, want nil", err)
		}

		acks := stream.responses()
		if len(acks) != 3 {
			t.Fatalf("responses = %d, want 3", len(acks))
		}
		for i, ack := range acks {
			if got := ack.GetStatus(); got != apiv1.IngestStatus_INGEST_STATUS_ACCEPTED {
				t.Errorf("ack %d status = %v, want ACCEPTED", i, got)
			}
			if got, want := ack.GetAckedSequence(), uint64(i+1); got != want {
				t.Errorf("ack %d acked sequence = %d, want %d", i, got, want)
			}
		}
		if got := len(dispatcher.captured()); got != 3 {
			t.Fatalf("dispatched rows = %d, want 3", got)
		}
	})

	t.Run("S-005 the handler waits for io.EOF before returning", func(t *testing.T) {
		dispatcher := &recordingDispatcher{}
		stream := newOpenServerStream(context.Background())
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		done := make(chan error, 1)
		go func() { done <- h.StreamTelemetry(stream) }()

		stream.recvCh <- serverRecv{req: ingestRequest("device-001", readingsPayload(t, "device-001", 1))}
		stream.waitForSends(t, 1)

		select {
		case err := <-done:
			t.Fatalf("StreamTelemetry() returned %v before the client half-closed", err)
		default:
		}

		stream.recvCh <- serverRecv{req: ingestRequest("device-001", readingsPayload(t, "device-001", 2))}
		stream.waitForSends(t, 1)
		close(stream.recvCh)

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("StreamTelemetry() error = %v, want nil", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("StreamTelemetry did not return after io.EOF")
		}

		acks := stream.responses()
		if len(acks) != 2 {
			t.Fatalf("responses = %d, want 2", len(acks))
		}
		if got := acks[1].GetAckedSequence(); got != 2 {
			t.Errorf("last acked sequence = %d, want 2", got)
		}
	})
}

// TestHandlerClientDisconnect covers the abort path: a non-EOF Recv error is
// wrapped and returned promptly, with no further responses.
func TestHandlerClientDisconnect(t *testing.T) {
	t.Run("S-006 recv failure mid-stream is wrapped and stops the loop", func(t *testing.T) {
		reset := errors.New("connection reset by peer")
		dispatcher := &recordingDispatcher{}
		stream := newFakeServerStream(context.Background(), []serverRecv{
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 1))},
			{err: reset},
		})
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		err := h.StreamTelemetry(stream)
		if err == nil {
			t.Fatal("StreamTelemetry() error = nil, want the recv failure")
		}
		if !errors.Is(err, reset) {
			t.Fatalf("StreamTelemetry() error = %v, want to wrap %v", err, reset)
		}
		if !strings.Contains(err.Error(), "ingestion: stream: receive") {
			t.Fatalf("StreamTelemetry() error = %q, want receive context", err)
		}
		if got := stream.responseCount(); got != 1 {
			t.Fatalf("responses after the failure = %d, want only the first ack", got)
		}
	})

	t.Run("S-006 recv failure before any request sends nothing", func(t *testing.T) {
		dispatcher := &recordingDispatcher{}
		stream := newFakeServerStream(context.Background(), []serverRecv{{err: io.ErrUnexpectedEOF}})
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		err := h.StreamTelemetry(stream)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("StreamTelemetry() error = %v, want to wrap %v", err, io.ErrUnexpectedEOF)
		}
		if got := stream.responseCount(); got != 0 {
			t.Fatalf("responses = %d, want 0", got)
		}
	})

	t.Run("send failure is wrapped and aborts the stream", func(t *testing.T) {
		broken := errors.New("transport write failed")
		dispatcher := &recordingDispatcher{}
		stream := newFakeServerStream(context.Background(), []serverRecv{
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 1))},
		})
		stream.sendErr = broken
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		err := h.StreamTelemetry(stream)
		if !errors.Is(err, broken) {
			t.Fatalf("StreamTelemetry() error = %v, want to wrap %v", err, broken)
		}
		if !strings.Contains(err.Error(), "send ack for device") {
			t.Fatalf("StreamTelemetry() error = %q, want send context", err)
		}
	})
}

// TestHandlerMultiplexedDevices covers per-device ack keying on a single
// stream: one device's sequence must never leak into another device's ack.
func TestHandlerMultiplexedDevices(t *testing.T) {
	t.Run("S-007 interleaved devices keep isolated ack progress", func(t *testing.T) {
		dispatcher := &recordingDispatcher{}
		stream := newFakeServerStream(context.Background(), []serverRecv{
			{req: ingestRequest("device-a", readingsPayload(t, "device-a", 5))},
			{req: ingestRequest("device-b", []byte{0xff, 0x00})},
			{req: ingestRequest("device-a", readingsPayload(t, "device-a", 9))},
			{req: ingestRequest("device-b", readingsPayload(t, "device-b", 7))},
		})
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		if err := h.StreamTelemetry(stream); err != nil {
			t.Fatalf("StreamTelemetry() error = %v, want nil", err)
		}

		acks := stream.responses()
		if len(acks) != 4 {
			t.Fatalf("responses = %d, want 4", len(acks))
		}
		want := []struct {
			status apiv1.IngestStatus
			acked  uint64
		}{
			{apiv1.IngestStatus_INGEST_STATUS_ACCEPTED, 5},
			{apiv1.IngestStatus_INGEST_STATUS_REJECTED_SIGNATURE, 0},
			{apiv1.IngestStatus_INGEST_STATUS_ACCEPTED, 9},
			{apiv1.IngestStatus_INGEST_STATUS_ACCEPTED, 7},
		}
		for i, w := range want {
			if got := acks[i].GetStatus(); got != w.status {
				t.Errorf("ack %d status = %v, want %v", i, got, w.status)
			}
			if got := acks[i].GetAckedSequence(); got != w.acked {
				t.Errorf("ack %d acked sequence = %d, want %d", i, got, w.acked)
			}
		}

		rows := dispatcher.captured()
		if len(rows) != 3 {
			t.Fatalf("dispatched rows = %d, want 3 accepted readings", len(rows))
		}
		if rows[0].DeviceID != "device-a" || rows[0].Sequence != 5 {
			t.Errorf("row 0 = (%q, %d), want (device-a, 5)", rows[0].DeviceID, rows[0].Sequence)
		}
		if rows[1].DeviceID != "device-a" || rows[1].Sequence != 9 {
			t.Errorf("row 1 = (%q, %d), want (device-a, 9)", rows[1].DeviceID, rows[1].Sequence)
		}
		if rows[2].DeviceID != "device-b" || rows[2].Sequence != 7 {
			t.Errorf("row 2 = (%q, %d), want (device-b, 7)", rows[2].DeviceID, rows[2].Sequence)
		}
	})
}

// TestHandlerStatusMapping covers the locked A7 mapping table.
func TestHandlerStatusMapping(t *testing.T) {
	t.Run("S-008 mapping table", func(t *testing.T) {
		configurationErr := errors.New("device registry unavailable")
		cases := []struct {
			name        string
			deviceID    string
			payload     []byte
			verifierErr error
			wantStatus  apiv1.IngestStatus
			wantAcked   uint64
			wantRows    int
		}{
			{
				name:       "empty device id",
				deviceID:   "",
				payload:    mustPayload(t, "device-001", 4),
				wantStatus: apiv1.IngestStatus_INGEST_STATUS_REJECTED_DEVICE,
			},
			{
				name:        "unknown device",
				deviceID:    "device-404",
				payload:     mustPayload(t, "device-404", 4),
				verifierErr: fmt.Errorf("lookup %q: %w", "device-404", ErrUnknownDevice),
				wantStatus:  apiv1.IngestStatus_INGEST_STATUS_REJECTED_DEVICE,
			},
			{
				name:       "corrupt payload",
				deviceID:   "device-001",
				payload:    []byte{0xff, 0x00, 0x00},
				wantStatus: apiv1.IngestStatus_INGEST_STATUS_REJECTED_SIGNATURE,
			},
			{
				name:        "generic verifier error",
				deviceID:    "device-001",
				payload:     mustPayload(t, "device-001", 4),
				verifierErr: fmt.Errorf("ML-DSA verify: %w", configurationErr),
				wantStatus:  apiv1.IngestStatus_INGEST_STATUS_REJECTED_SIGNATURE,
			},
			{
				name:       "accepted",
				deviceID:   "device-001",
				payload:    mustPayload(t, "device-001", 4),
				wantStatus: apiv1.IngestStatus_INGEST_STATUS_ACCEPTED,
				wantAcked:  4,
				wantRows:   1,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				dispatcher := &recordingDispatcher{}
				verifier := &fakeVerifier{err: tc.verifierErr}
				stream := newFakeServerStream(context.Background(), []serverRecv{
					{req: ingestRequest(tc.deviceID, tc.payload)},
				})
				h := mustHandler(t, dispatcher, verifier)

				if err := h.StreamTelemetry(stream); err != nil {
					t.Fatalf("StreamTelemetry() error = %v, want nil", err)
				}

				acks := stream.responses()
				if len(acks) != 1 {
					t.Fatalf("responses = %d, want exactly one ack", len(acks))
				}
				if got := acks[0].GetStatus(); got != tc.wantStatus {
					t.Errorf("status = %v, want %v", got, tc.wantStatus)
				}
				if got := acks[0].GetAckedSequence(); got != tc.wantAcked {
					t.Errorf("acked sequence = %d, want %d", got, tc.wantAcked)
				}
				if got := len(dispatcher.captured()); got != tc.wantRows {
					t.Errorf("dispatched rows = %d, want %d", got, tc.wantRows)
				}
			})
		}
	})
}

// TestHandlerVerifierSwapSeam proves the injected verifier alone decides the
// outcome, which is the Phase 4 swap path (no handler edits).
func TestHandlerVerifierSwapSeam(t *testing.T) {
	t.Run("S-004 injected verifier decides status and dispatch", func(t *testing.T) {
		request := ingestRequest("device-001", readingsPayload(t, "device-001", 3))
		cases := []struct {
			name       string
			verifier   *fakeVerifier
			wantStatus apiv1.IngestStatus
			wantRows   int
		}{
			{
				name:       "accepting verifier",
				verifier:   &fakeVerifier{},
				wantStatus: apiv1.IngestStatus_INGEST_STATUS_ACCEPTED,
				wantRows:   1,
			},
			{
				name:       "rejecting verifier",
				verifier:   &fakeVerifier{err: errors.New("signature mismatch")},
				wantStatus: apiv1.IngestStatus_INGEST_STATUS_REJECTED_SIGNATURE,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				dispatcher := &recordingDispatcher{}
				stream := newFakeServerStream(context.Background(), []serverRecv{{req: request}})
				h := mustHandler(t, dispatcher, tc.verifier)

				if err := h.StreamTelemetry(stream); err != nil {
					t.Fatalf("StreamTelemetry() error = %v, want nil", err)
				}
				if got := stream.responses()[0].GetStatus(); got != tc.wantStatus {
					t.Errorf("status = %v, want %v", got, tc.wantStatus)
				}
				if got := len(dispatcher.captured()); got != tc.wantRows {
					t.Errorf("dispatched rows = %d, want %d", got, tc.wantRows)
				}

				calls := tc.verifier.recorded()
				if len(calls) != 1 {
					t.Fatalf("verifier calls = %d, want 1", len(calls))
				}
				if calls[0].deviceID != "device-001" {
					t.Errorf("verifier device id = %q, want device-001", calls[0].deviceID)
				}
				if calls[0].algorithm != apiv1.SignatureAlgorithm_ML_DSA_65 {
					t.Errorf("verifier algorithm = %v, want ML-DSA-65", calls[0].algorithm)
				}
				if string(calls[0].payload) != string(request.GetPayload()) {
					t.Error("verifier payload does not match the request payload")
				}
				if string(calls[0].signature) != string(request.GetSignature()) {
					t.Error("verifier signature does not match the request signature")
				}
			})
		}
	})
}

// TestHandlerRejectedAckKeepsLastAccepted proves a rejected message still gets
// its one ack, carrying the device's previous accepted sequence.
func TestHandlerRejectedAckKeepsLastAccepted(t *testing.T) {
	t.Run("S-008 rejected ack retains last accepted sequence", func(t *testing.T) {
		dispatcher := &recordingDispatcher{}
		verifier := &fakeVerifier{}
		calls := 0
		verifier.errFor = func(string) error {
			calls++
			if calls == 2 {
				return fmt.Errorf("lookup: %w", ErrUnknownDevice)
			}
			return nil
		}
		stream := newFakeServerStream(context.Background(), []serverRecv{
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 6))},
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 9))},
		})
		h := mustHandler(t, dispatcher, verifier)

		if err := h.StreamTelemetry(stream); err != nil {
			t.Fatalf("StreamTelemetry() error = %v, want nil", err)
		}

		acks := stream.responses()
		if len(acks) != 2 {
			t.Fatalf("responses = %d, want 2", len(acks))
		}
		if got := acks[0].GetStatus(); got != apiv1.IngestStatus_INGEST_STATUS_ACCEPTED {
			t.Errorf("ack 0 status = %v, want ACCEPTED", got)
		}
		if got := acks[0].GetAckedSequence(); got != 6 {
			t.Errorf("ack 0 acked sequence = %d, want 6", got)
		}
		if got := acks[1].GetStatus(); got != apiv1.IngestStatus_INGEST_STATUS_REJECTED_DEVICE {
			t.Errorf("ack 1 status = %v, want REJECTED_DEVICE", got)
		}
		if got := acks[1].GetAckedSequence(); got != 6 {
			t.Errorf("ack 1 acked sequence = %d, want the last accepted 6", got)
		}
		if got := len(dispatcher.captured()); got != 1 {
			t.Errorf("dispatched rows = %d, want 1", got)
		}
	})
}

// TestHandlerEmptyBatch covers a valid batch with no readings: accepted, no
// rows dispatched, and the ack never regresses.
func TestHandlerEmptyBatch(t *testing.T) {
	t.Run("S-008 empty batch is accepted without advancing the ack", func(t *testing.T) {
		dispatcher := &recordingDispatcher{}
		emptyBatch := marshalBatch(t, &apiv1.TelemetryBatch{DeviceId: "device-001"})
		if len(emptyBatch) == 0 {
			t.Fatal("empty batch payload is zero bytes; it must carry the device identity")
		}
		stream := newFakeServerStream(context.Background(), []serverRecv{
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 8))},
			{req: ingestRequest("device-001", emptyBatch)},
		})
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		if err := h.StreamTelemetry(stream); err != nil {
			t.Fatalf("StreamTelemetry() error = %v, want nil", err)
		}

		acks := stream.responses()
		if len(acks) != 2 {
			t.Fatalf("responses = %d, want 2", len(acks))
		}
		for i, ack := range acks {
			if got := ack.GetStatus(); got != apiv1.IngestStatus_INGEST_STATUS_ACCEPTED {
				t.Errorf("ack %d status = %v, want ACCEPTED", i, got)
			}
			if got := ack.GetAckedSequence(); got != 8 {
				t.Errorf("ack %d acked sequence = %d, want 8", i, got)
			}
		}
		if got := len(dispatcher.captured()); got != 1 {
			t.Errorf("dispatched rows = %d, want the single non-empty reading", got)
		}
	})

	t.Run("S-008 first empty batch acknowledges zero", func(t *testing.T) {
		dispatcher := &recordingDispatcher{}
		emptyBatch := marshalBatch(t, &apiv1.TelemetryBatch{DeviceId: "device-001"})
		stream := newFakeServerStream(context.Background(), []serverRecv{
			{req: ingestRequest("device-001", emptyBatch)},
		})
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		if err := h.StreamTelemetry(stream); err != nil {
			t.Fatalf("StreamTelemetry() error = %v, want nil", err)
		}
		acks := stream.responses()
		if len(acks) != 1 {
			t.Fatalf("responses = %d, want 1", len(acks))
		}
		if got := acks[0].GetAckedSequence(); got != 0 {
			t.Errorf("acked sequence = %d, want 0", got)
		}
		if got := len(dispatcher.captured()); got != 0 {
			t.Errorf("dispatched rows = %d, want 0", got)
		}
	})
}

// TestHandlerNeverEmitsThrottled locks the A7 reservation: Phase 3 has no
// throttling, so THROTTLED must never leave the handler.
func TestHandlerNeverEmitsThrottled(t *testing.T) {
	t.Run("S-009 THROTTLED is never emitted", func(t *testing.T) {
		dispatcher := &recordingDispatcher{}
		stream := newFakeServerStream(context.Background(), []serverRecv{
			{req: ingestRequest("", readingsPayload(t, "device-001", 1))},
			{req: ingestRequest("device-001", []byte{0xff})},
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 2))},
			{req: ingestRequest("device-002", readingsPayload(t, "device-002", 3))},
		})
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		if err := h.StreamTelemetry(stream); err != nil {
			t.Fatalf("StreamTelemetry() error = %v, want nil", err)
		}

		acks := stream.responses()
		if len(acks) != 4 {
			t.Fatalf("responses = %d, want 4", len(acks))
		}
		for i, ack := range acks {
			if got := ack.GetStatus(); got == apiv1.IngestStatus_INGEST_STATUS_THROTTLED {
				t.Errorf("ack %d status = THROTTLED, want it reserved and never emitted", i)
			}
		}
	})
}

// TestHandlerDispatchFailure proves a dispatcher error aborts the stream with
// wrapped context instead of silently dropping accepted rows.
func TestHandlerDispatchFailure(t *testing.T) {
	t.Run("dispatcher error is wrapped and aborts", func(t *testing.T) {
		full := errors.New("queue full")
		dispatcher := &recordingDispatcher{errOn: func(TelemetryRow) error { return full }}
		stream := newFakeServerStream(context.Background(), []serverRecv{
			{req: ingestRequest("device-001", readingsPayload(t, "device-001", 1))},
		})
		h := mustHandler(t, dispatcher, NewAcceptAllVerifier())

		err := h.StreamTelemetry(stream)
		if !errors.Is(err, full) {
			t.Fatalf("StreamTelemetry() error = %v, want to wrap %v", err, full)
		}
		if !strings.Contains(err.Error(), "dispatch device") {
			t.Fatalf("StreamTelemetry() error = %q, want dispatch context", err)
		}
		if got := stream.responseCount(); got != 0 {
			t.Fatalf("responses = %d, want none when dispatch fails", got)
		}
	})
}

// TestHandlerBufconn proves the handler is wired to the generated service and
// works end-to-end over a real bidi gRPC stream with the worker pool behind it.
func TestHandlerBufconn(t *testing.T) {
	t.Run("S-005 real bidi stream acks every request and persists rows", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		inserter := &syncInserter{}
		pool, err := NewWorkerPool(inserter, WorkerPoolConfig{
			Workers:       2,
			QueueCapacity: 16,
			BatchMax:      1,
			BatchWait:     0,
		})
		if err != nil {
			t.Fatalf("NewWorkerPool() error = %v", err)
		}
		t.Cleanup(func() {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			if err := pool.Close(closeCtx); err != nil {
				t.Errorf("pool.Close() error = %v", err)
			}
		})

		handler := mustHandler(t, pool, NewAcceptAllVerifier())
		server := grpc.NewServer()
		apiv1.RegisterTelemetryIngestionServiceServer(server, handler)

		listener := bufconn.Listen(1024 * 1024)
		t.Cleanup(func() { _ = listener.Close() })
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)

		conn, err := grpc.NewClient(
			"passthrough:///bufconn",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("grpc.NewClient() error = %v", err)
		}
		t.Cleanup(func() { _ = conn.Close() })

		stream, err := apiv1.NewTelemetryIngestionServiceClient(conn).StreamTelemetry(ctx)
		if err != nil {
			t.Fatalf("StreamTelemetry() client error = %v", err)
		}
		requests := []*apiv1.TelemetryIngestRequest{
			ingestRequest("device-a", readingsPayload(t, "device-a", 1)),
			ingestRequest("device-b", readingsPayload(t, "device-b", 2)),
			ingestRequest("device-a", readingsPayload(t, "device-a", 3)),
		}
		for _, req := range requests {
			if err := stream.Send(req); err != nil {
				t.Fatalf("stream.Send() error = %v", err)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("stream.CloseSend() error = %v", err)
		}

		wantAcks := []struct {
			status apiv1.IngestStatus
			acked  uint64
		}{
			{apiv1.IngestStatus_INGEST_STATUS_ACCEPTED, 1},
			{apiv1.IngestStatus_INGEST_STATUS_ACCEPTED, 2},
			{apiv1.IngestStatus_INGEST_STATUS_ACCEPTED, 3},
		}
		for i, want := range wantAcks {
			ack, err := stream.Recv()
			if err != nil {
				t.Fatalf("stream.Recv() ack %d error = %v", i, err)
			}
			if got := ack.GetStatus(); got != want.status {
				t.Errorf("ack %d status = %v, want %v", i, got, want.status)
			}
			if got := ack.GetAckedSequence(); got != want.acked {
				t.Errorf("ack %d acked sequence = %d, want %d", i, got, want.acked)
			}
		}
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("stream.Recv() after all acks error = %v, want io.EOF", err)
		}

		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := pool.Close(closeCtx); err != nil {
			t.Fatalf("pool.Close() error = %v", err)
		}

		rows := inserter.rows()
		if len(rows) != 3 {
			t.Fatalf("persisted rows = %d, want 3", len(rows))
		}
		sequences := make(map[string][]int64)
		for _, row := range rows {
			sequences[row.DeviceID] = append(sequences[row.DeviceID], row.Sequence)
		}
		if got := sequences["device-a"]; len(got) != 2 || got[0] != 1 || got[1] != 3 {
			t.Errorf("device-a sequences = %v, want [1 3] in order", got)
		}
		if got := sequences["device-b"]; len(got) != 1 || got[0] != 2 {
			t.Errorf("device-b sequences = %v, want [2]", got)
		}
	})
}

// mustPayload marshals a one-reading batch for the mapping table.
func mustPayload(t *testing.T, deviceID string, sequence uint64) []byte {
	t.Helper()
	return readingsPayload(t, deviceID, sequence)
}

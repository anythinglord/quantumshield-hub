package simulator

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"crypto/mldsa"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
	qscrypto "github.com/anythinglord/quantumshield-hub/internal/crypto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// fakeConsumer wires a consumer to a fake bidi stream.
//
// The fake is built lazily inside Consume so it is bound to the consumer's
// stream context: cancelling the consumer tears the fake down exactly like a
// real gRPC stream does. The returned channel delivers the fake as soon as
// the stream is open.
func fakeConsumer(cfg GrpcConsumerConfig, script []recvResult, opts ...fakeStreamOpt) (*GrpcConsumer, <-chan *fakeStream) {
	c := NewGrpcConsumer(cfg)
	ready := make(chan *fakeStream, 1)
	c.streamFn = func(ctx context.Context) (telemetryStream, error) {
		f := newFakeStream(ctx, script, opts...)
		ready <- f
		return f, nil
	}
	return c, ready
}

// failingSendStream wraps the fake harness with a Send that always fails.
// It models a transport that rejects the message instead of accepting it.
type failingSendStream struct {
	*fakeStream
	err error
}

func (f *failingSendStream) Send(*apiv1.TelemetryIngestRequest) error { return f.err }

// TestS001_RequestShapePerBatch proves every batch leaves the consumer as
// exactly one request that carries the batch identity and its payload.
func TestS001_RequestShapePerBatch(t *testing.T) {
	t.Run("one request per batch", func(t *testing.T) {
		batches := []*apiv1.TelemetryBatch{
			{
				DeviceId: "device-0000",
				Readings: []*apiv1.SensorReading{{Sequence: 0, TimestampNanos: 1000, TemperatureC: 21.5}},
			},
			{
				DeviceId: "device-0001",
				Readings: []*apiv1.SensorReading{{Sequence: 7, TimestampNanos: 2000, VoltageV: 3.3}},
			},
		}
		ctx := context.Background()
		// S-001 is the signed wire shape: PQC must be on, otherwise the
		// assertions below would pass vacuously over unsigned requests.
		c, ready := fakeConsumer(GrpcConsumerConfig{PQCEnabled: true}, nil)
		out := make(chan *apiv1.TelemetryBatch, len(batches))
		for _, b := range batches {
			out <- b
		}
		close(out)

		if err := c.Consume(ctx, out); err != nil {
			t.Fatalf("Consume() error = %v, want nil", err)
		}

		sent := (<-ready).sentRequests()
		if len(sent) != len(batches) {
			t.Fatalf("sent requests = %d, want %d", len(sent), len(batches))
		}
		for i, req := range sent {
			want := batches[i]
			if req.GetDeviceId() != want.GetDeviceId() {
				t.Fatalf("request %d device id = %q, want %q", i, req.GetDeviceId(), want.GetDeviceId())
			}
			got := &apiv1.TelemetryBatch{}
			if err := proto.Unmarshal(req.GetPayload(), got); err != nil {
				t.Fatalf("unmarshal request %d payload: %v", i, err)
			}
			if !proto.Equal(got, want) {
				t.Fatalf("request %d payload = %v, want %v", i, got, want)
			}
			if len(req.GetSignature()) == 0 {
				t.Fatalf("request %d signature is empty, want ML-DSA signature", i)
			}
			if req.GetSignatureAlgorithm() != apiv1.SignatureAlgorithm_ML_DSA_65 {
				t.Fatalf("request %d signature algorithm = %v, want ML-DSA-65", i, req.GetSignatureAlgorithm())
			}
			sk, ok := c.deviceKeys[want.GetDeviceId()]
			if !ok {
				t.Fatalf("request %d has no cached key for device %q", i, want.GetDeviceId())
			}
			if err := qscrypto.VerifyPayload(apiv1.SignatureAlgorithm_ML_DSA_65, sk.PublicKey().Bytes(), req.GetPayload(), req.GetSignature(), nil); err != nil {
				t.Fatalf("request %d signature does not verify: %v", i, err)
			}
		}
	})
}

// TestS002_CloseSendExactlyOnceOnOutClose proves the send half is closed
// exactly once when the fan-in channel closes, and that the consumer then
// finishes cleanly.
func TestS002_CloseSendExactlyOnceOnOutClose(t *testing.T) {
	ctx := context.Background()
	c, ready := fakeConsumer(GrpcConsumerConfig{}, nil)
	out := make(chan *apiv1.TelemetryBatch, 1)
	out <- &apiv1.TelemetryBatch{DeviceId: "device-0000"}
	close(out)

	if err := c.Consume(ctx, out); err != nil {
		t.Fatalf("Consume() error = %v, want nil", err)
	}

	fake := <-ready
	// The literal 1 is pinned: a double CloseSend corrupts the stream and
	// a missing one leaves the server waiting for more requests.
	if got := fake.closeSendCalls(); got != 1 {
		t.Fatalf("CloseSend calls = %d, want exactly 1", got)
	}
	// A half-closed stream must be drained: Recv reaches its terminal result
	// (io.EOF) and the consumer joins it before returning.
	select {
	case <-fake.recvDoneCh():
	default:
		t.Fatal("the recv side was not drained after CloseSend")
	}
}

// TestS006_DeriveServerName proves the TLS server name is derived from the
// gRPC address and that an explicit configuration wins over the derivation.
func TestS006_DeriveServerName(t *testing.T) {
	t.Run("derives the server name from the address", func(t *testing.T) {
		tests := []struct {
			name    string
			address string
			want    string
		}{
			{"host name", "localhost:50051", "localhost"},
			{"ip address", "127.0.0.1:50051", "127.0.0.1"},
			{"malformed address is returned unchanged", "not-an-address", "not-an-address"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				c := NewGrpcConsumer(GrpcConsumerConfig{Address: tt.address})
				if got := c.serverName(); got != tt.want {
					t.Fatalf("serverName() = %q, want %q", got, tt.want)
				}
				if got := deriveServerName(tt.address); got != tt.want {
					t.Fatalf("deriveServerName(%q) = %q, want %q", tt.address, got, tt.want)
				}
			})
		}
	})

	t.Run("explicit server name wins over the derivation", func(t *testing.T) {
		c := NewGrpcConsumer(GrpcConsumerConfig{
			Address:       "localhost:50051",
			TLSServerName: "edge.example.com",
		})
		if got := c.serverName(); got != "edge.example.com" {
			t.Fatalf("serverName() = %q, want %q", got, "edge.example.com")
		}
	})
}

// TestS008_WrappedErrorOnStreamFailure proves every stream failure aborts
// the consumer with a wrapped, context-carrying error.
func TestS008_WrappedErrorOnStreamFailure(t *testing.T) {
	t.Run("recv failure aborts with a wrapped error", func(t *testing.T) {
		sentinel := errors.New("server exploded")
		ctx := context.Background()
		c, _ := fakeConsumer(GrpcConsumerConfig{}, []recvResult{{err: sentinel}})
		out := make(chan *apiv1.TelemetryBatch, 1)
		out <- &apiv1.TelemetryBatch{DeviceId: "device-0000"}
		close(out)

		err := c.Consume(ctx, out)
		if err == nil {
			t.Fatal("Consume() error = nil, want a wrapped stream failure")
		}
		if !errors.Is(err, sentinel) {
			t.Fatalf("Consume() error = %v, want it to wrap %v", err, sentinel)
		}
		if !strings.Contains(err.Error(), "simulator: stream") {
			t.Fatalf("Consume() error = %q, want it to carry the simulator: stream context", err)
		}
	})

	t.Run("send failure aborts with a wrapped error", func(t *testing.T) {
		sentinel := errors.New("transport refused the message")
		ctx := context.Background()
		c := NewGrpcConsumer(GrpcConsumerConfig{})
		c.streamFn = func(ctx context.Context) (telemetryStream, error) {
			return &failingSendStream{fakeStream: newFakeStream(ctx, nil), err: sentinel}, nil
		}
		out := make(chan *apiv1.TelemetryBatch, 1)
		out <- &apiv1.TelemetryBatch{DeviceId: "device-0000"}
		close(out)

		err := c.Consume(ctx, out)
		if err == nil {
			t.Fatal("Consume() error = nil, want a wrapped send failure")
		}
		if !errors.Is(err, sentinel) {
			t.Fatalf("Consume() error = %v, want it to wrap %v", err, sentinel)
		}
		if !strings.Contains(err.Error(), "simulator: send") {
			t.Fatalf("Consume() error = %q, want it to carry the simulator: send context", err)
		}
	})

	t.Run("dial failure names the missing certificate", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "client-chain.crt")
		c := NewGrpcConsumer(GrpcConsumerConfig{
			Address: "localhost:50051",
			TLSCert: missing,
			TLSKey:  missing,
			TLSCA:   missing,
		})
		out := make(chan *apiv1.TelemetryBatch)

		err := c.Consume(context.Background(), out)
		if err == nil {
			t.Fatal("Consume() error = nil, want the dial failure to be reported")
		}
		if !strings.Contains(err.Error(), "simulator: stream") {
			t.Fatalf("Consume() error = %q, want it to carry the simulator: stream context", err)
		}
		if !strings.Contains(err.Error(), missing) {
			t.Fatalf("Consume() error = %q, want it to name the missing file %q", err, missing)
		}
	})
}

// TestS009_CleanCancelReturnsNil proves a cancelled context is a clean
// shutdown: the consumer returns nil, never the context error.
func TestS009_CleanCancelReturnsNil(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, ready := fakeConsumer(GrpcConsumerConfig{}, nil)
	out := make(chan *apiv1.TelemetryBatch, 1)
	done := make(chan error, 1)
	go func() { done <- c.Consume(ctx, out) }()

	fake := <-ready
	select {
	case <-fake.recvEnteredCh():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for the recv loop to start")
	}

	cancel()
	close(out)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Consume() error = %v, want nil on a clean cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Consume did not return after the cancel")
	}
}

// TestS010_RecvGoroutineJoined proves the recv goroutine is joined before
// Consume returns, on every exit path.
func TestS010_RecvGoroutineJoined(t *testing.T) {
	t.Run("joined after the fan-in closed", func(t *testing.T) {
		ctx := context.Background()
		c, ready := fakeConsumer(GrpcConsumerConfig{}, nil)
		out := make(chan *apiv1.TelemetryBatch, 1)
		out <- &apiv1.TelemetryBatch{DeviceId: "device-0000"}
		close(out)

		if err := c.Consume(ctx, out); err != nil {
			t.Fatalf("Consume() error = %v, want nil", err)
		}
		select {
		case <-(<-ready).recvDoneCh():
		default:
			t.Fatal("the recv goroutine was not joined before Consume returned")
		}
	})

	t.Run("joined after a clean cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		c, ready := fakeConsumer(GrpcConsumerConfig{}, nil)
		out := make(chan *apiv1.TelemetryBatch, 1)
		done := make(chan error, 1)
		go func() { done <- c.Consume(ctx, out) }()

		fake := <-ready
		select {
		case <-fake.recvEnteredCh():
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for the recv loop to start")
		}

		cancel()
		close(out)

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Consume() error = %v, want nil on a clean cancel", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Consume did not return after the cancel")
		}
		select {
		case <-fake.recvDoneCh():
		default:
			t.Fatal("the recv goroutine was not joined before Consume returned")
		}
	})
}

// TestS004_LazyKeygenPerDeviceID proves keys are generated lazily, one per
// distinct device_id, with no keygen before batches and reuse on the same
// device.
func TestS004_LazyKeygenPerDeviceID(t *testing.T) {
	var keygenCalls int32
	var keys []*mldsa.PrivateKey
	c := NewGrpcConsumer(GrpcConsumerConfig{PQCEnabled: true})
	c.keygenFn = func() (*mldsa.PrivateKey, error) {
		atomic.AddInt32(&keygenCalls, 1)
		sk, err := mldsa.GenerateKey(mldsa.MLDSA65())
		if err != nil {
			return nil, err
		}
		keys = append(keys, sk)
		return sk, nil
	}
	ctx := context.Background()
	fake := newFakeStream(ctx, nil)
	c.streamFn = func(context.Context) (telemetryStream, error) { return fake, nil }

	if atomic.LoadInt32(&keygenCalls) != 0 {
		t.Fatalf("keygen calls before any batch = %d, want 0", atomic.LoadInt32(&keygenCalls))
	}

	out := make(chan *apiv1.TelemetryBatch, 4)
	out <- &apiv1.TelemetryBatch{DeviceId: "device-0000"}
	out <- &apiv1.TelemetryBatch{DeviceId: "device-0001"}
	out <- &apiv1.TelemetryBatch{DeviceId: "device-0000"} // reuse
	close(out)

	if err := c.Consume(ctx, out); err != nil {
		t.Fatalf("Consume() error = %v, want nil", err)
	}

	if atomic.LoadInt32(&keygenCalls) != 2 {
		t.Fatalf("keygen calls = %d, want 2 (one per distinct device id)", atomic.LoadInt32(&keygenCalls))
	}

	sent := fake.sentRequests()
	if len(sent) != 3 {
		t.Fatalf("sent requests = %d, want 3", len(sent))
	}
	if len(sent[0].GetSignature()) == 0 || sent[0].GetSignatureAlgorithm() != apiv1.SignatureAlgorithm_ML_DSA_65 {
		t.Fatal("first request must be signed with ML-DSA-65")
	}
	if len(sent[1].GetSignature()) == 0 || sent[1].GetSignatureAlgorithm() != apiv1.SignatureAlgorithm_ML_DSA_65 {
		t.Fatal("second request must be signed with ML-DSA-65")
	}
	if len(sent[2].GetSignature()) == 0 || sent[2].GetSignatureAlgorithm() != apiv1.SignatureAlgorithm_ML_DSA_65 {
		t.Fatal("third request must reuse the key and be signed")
	}
	if sent[0].GetSignatureAlgorithm() != sent[1].GetSignatureAlgorithm() {
		t.Fatal("distinct devices use ML-DSA-65")
	}

	// Distinct devices must hold distinct keys, and a repeat device must
	// reuse the exact key its first batch was signed with (deterministic
	// identity is reuse, not deterministic keygen).
	if len(keys) != 2 {
		t.Fatalf("generated keys = %d, want 2", len(keys))
	}
	if bytes.Equal(keys[0].PublicKey().Bytes(), keys[1].PublicKey().Bytes()) {
		t.Fatal("distinct devices received the same public key")
	}
	for _, tc := range []struct {
		name string
		key  *mldsa.PrivateKey
		req  *apiv1.TelemetryIngestRequest
	}{
		{"device-0000 first batch", keys[0], sent[0]},
		{"device-0001 first batch", keys[1], sent[1]},
		{"device-0000 reuses its key", keys[0], sent[2]},
	} {
		if err := qscrypto.VerifyPayload(apiv1.SignatureAlgorithm_ML_DSA_65, tc.key.PublicKey().Bytes(), tc.req.GetPayload(), tc.req.GetSignature(), nil); err != nil {
			t.Fatalf("%s: signature does not verify with the expected key: %v", tc.name, err)
		}
	}
}

// TestS005_UnsignedModeWireFields proves unsigned mode leaves the signature
// empty, sets UNSPECIFIED, and skips key generation entirely.
func TestS005_UnsignedModeWireFields(t *testing.T) {
	var keygenCalls int32
	c := NewGrpcConsumer(GrpcConsumerConfig{PQCEnabled: false})
	c.keygenFn = func() (*mldsa.PrivateKey, error) {
		atomic.AddInt32(&keygenCalls, 1)
		return mldsa.GenerateKey(mldsa.MLDSA65())
	}
	ctx := context.Background()
	fake := newFakeStream(ctx, nil)
	c.streamFn = func(context.Context) (telemetryStream, error) { return fake, nil }

	out := make(chan *apiv1.TelemetryBatch, 1)
	out <- &apiv1.TelemetryBatch{DeviceId: "device-0000"}
	close(out)

	if err := c.Consume(ctx, out); err != nil {
		t.Fatalf("Consume() error = %v, want nil", err)
	}

	if atomic.LoadInt32(&keygenCalls) != 0 {
		t.Fatalf("keygen calls = %d, want 0 in unsigned mode", atomic.LoadInt32(&keygenCalls))
	}

	sent := fake.sentRequests()
	if len(sent) != 1 {
		t.Fatalf("sent requests = %d, want 1", len(sent))
	}
	req := sent[0]
	if len(req.GetSignature()) != 0 {
		t.Fatalf("signature = %v, want empty/nil in unsigned mode", req.GetSignature())
	}
	if req.GetSignatureAlgorithm() != apiv1.SignatureAlgorithm_SIGNATURE_ALGORITHM_UNSPECIFIED {
		t.Fatalf("signature algorithm = %v, want UNSPECIFIED", req.GetSignatureAlgorithm())
	}
}

// TestS011_AckCountsByStatus proves acks are counted per IngestStatus with
// the literal expected counts pinned (a wrong enum mapping must fail).
func TestS011_AckCountsByStatus(t *testing.T) {
	ctx := context.Background()
	c, ready := fakeConsumer(GrpcConsumerConfig{}, []recvResult{
		{resp: &apiv1.TelemetryIngestResponse{AckedSequence: 1, Status: apiv1.IngestStatus_INGEST_STATUS_ACCEPTED}},
		{resp: &apiv1.TelemetryIngestResponse{AckedSequence: 2, Status: apiv1.IngestStatus_INGEST_STATUS_REJECTED_SIGNATURE}},
		{resp: &apiv1.TelemetryIngestResponse{AckedSequence: 3, Status: apiv1.IngestStatus_INGEST_STATUS_ACCEPTED}},
		{resp: &apiv1.TelemetryIngestResponse{AckedSequence: 4, Status: apiv1.IngestStatus_INGEST_STATUS_THROTTLED}},
		{resp: &apiv1.TelemetryIngestResponse{AckedSequence: 5, Status: apiv1.IngestStatus_INGEST_STATUS_REJECTED_DEVICE}},
	})
	out := make(chan *apiv1.TelemetryBatch, 5)
	for i := 0; i < 5; i++ {
		out <- &apiv1.TelemetryBatch{DeviceId: fmt.Sprintf("device-%04d", i)}
	}
	close(out)

	if err := c.Consume(ctx, out); err != nil {
		t.Fatalf("Consume() error = %v, want nil", err)
	}
	<-ready

	acks := c.Acks()
	// Pin literal expected counts: do not derive from enum numeric values.
	if got := acks[apiv1.IngestStatus_INGEST_STATUS_ACCEPTED]; got != 2 {
		t.Fatalf("accepted count = %d, want literal 2", got)
	}
	if got := acks[apiv1.IngestStatus_INGEST_STATUS_REJECTED_SIGNATURE]; got != 1 {
		t.Fatalf("rejected_signature count = %d, want literal 1", got)
	}
	if got := acks[apiv1.IngestStatus_INGEST_STATUS_THROTTLED]; got != 1 {
		t.Fatalf("throttled count = %d, want literal 1", got)
	}
	if got := acks[apiv1.IngestStatus_INGEST_STATUS_REJECTED_DEVICE]; got != 1 {
		t.Fatalf("rejected_device count = %d, want literal 1", got)
	}
	if got := acks[apiv1.IngestStatus_INGEST_STATUS_UNSPECIFIED]; got != 0 {
		t.Fatalf("unspecified count = %d, want literal 0", got)
	}
}

// TestS012_ProducersBlockNotDrop proves that a bounded fan-in channel makes
// the producer block instead of dropping while the gRPC transport is stalled,
// and that every batch arrives in order once the consumer resumes.
//
// The producer is driven directly into a channel sized with the fleet's
// pinned capacity (outBuffer = 1024) instead of through Fleet: the fleet
// deliberately discards pending ticks when a device is stalled on a full
// fan-in (D8 / R-003.2), so counting batches through the fleet cannot prove
// the zero-drop consumer contract.
func TestS012_ProducersBlockNotDrop(t *testing.T) {
	const total = 1500 // must exceed the pinned fan-in capacity
	if total <= outBuffer {
		t.Fatalf("total = %d must exceed the pinned fan-in capacity %d", total, outBuffer)
	}

	ctx := context.Background()
	gate := make(chan struct{})
	fake := newFakeStream(ctx, nil, withSendGate(gate))
	c := NewGrpcConsumer(GrpcConsumerConfig{})
	c.streamFn = func(context.Context) (telemetryStream, error) { return fake, nil }

	// The fan-in channel carries the fleet's real capacity.
	out := make(chan *apiv1.TelemetryBatch, outBuffer)

	produced := make(chan struct{})
	go func() {
		defer close(produced)
		for i := 0; i < total; i++ {
			out <- &apiv1.TelemetryBatch{
				DeviceId: "device-0000",
				Readings: []*apiv1.SensorReading{{Sequence: uint64(i)}},
			}
		}
		close(out)
	}()

	done := make(chan error, 1)
	go func() { done <- c.Consume(ctx, out) }()

	// The consumer picked the first batch and parked in the gated Send.
	select {
	case <-fake.sendEnteredCh():
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for the first send to reach the transport")
	}

	// One batch is parked in the transport and the fan-in is full (capacity
	// 1024): the producer must block on the remaining batches, never drop
	// them. If it finishes here, batches were lost.
	select {
	case <-produced:
		t.Fatalf("producer finished all %d batches while the stream was stalled", total)
	case <-time.After(100 * time.Millisecond):
	}
	if got := len(fake.sentRequests()); got != 1 {
		t.Fatalf("forwarded sends while stalled = %d, want only the parked batch", got)
	}

	// Resume the transport: the consumer drains the fan-in, the producer
	// completes and closes it, and Consume half-closes and drains the stream.
	close(gate)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Consume() error = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Consume did not return after the transport resumed")
	}
	select {
	case <-produced:
	default:
		t.Fatal("the producer did not finish after the transport resumed")
	}

	sent := fake.sentRequests()
	if len(sent) != total {
		t.Fatalf("sent batches = %d, want exactly %d (zero drops)", len(sent), total)
	}
	// Single producer into a FIFO channel: per-device order must be intact.
	for i, req := range sent {
		if req.GetDeviceId() != "device-0000" {
			t.Fatalf("batch %d device id = %q, want device-0000", i, req.GetDeviceId())
		}
		got := &apiv1.TelemetryBatch{}
		if err := proto.Unmarshal(req.GetPayload(), got); err != nil {
			t.Fatalf("unmarshal batch %d: %v", i, err)
		}
		if seq := got.GetReadings()[0].GetSequence(); seq != uint64(i) {
			t.Fatalf("batch %d sequence = %d, want %d (order preserved)", i, seq, i)
		}
	}
}

// TestS007_TLSRealTransport proves the consumer works over a real gRPC
// server with TLS and that a server name / CA mismatch fails the handshake.
func TestS007_TLSRealTransport(t *testing.T) {
	t.Run("bufconn", func(t *testing.T) {
		serverCert, serverCAPool := newTestTLSMaterial(t)

		type ingestServer struct {
			apiv1.UnimplementedTelemetryIngestionServiceServer
			received []*apiv1.TelemetryIngestRequest
		}
		srvImpl := &struct {
			received []*apiv1.TelemetryIngestRequest
		}{}
		gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(qscrypto.ServerTLSConfig(serverCert, serverCAPool))))
		apiv1.RegisterTelemetryIngestionServiceServer(gs, &bufconnServer{impl: srvImpl})

		lis := bufconn.Listen(1024 * 1024)
		defer lis.Close()
		go func() { _ = gs.Serve(lis) }()
		defer gs.Stop()

		clientCert := serverCert // same cert works as client (has clientAuth)
		c := NewGrpcConsumer(GrpcConsumerConfig{
			Address:       "bufconn",
			TLSServerName: "localhost",
			PQCEnabled:    true,
		})

		c.streamFn = func(ctx context.Context) (telemetryStream, error) {
			tlsCfg := qscrypto.ClientTLSConfig(clientCert, serverCAPool, "localhost")
			conn, err := grpc.NewClient("passthrough:///bufconn",
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
				grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
			)
			if err != nil {
				return nil, err
			}
			stream, err := apiv1.NewTelemetryIngestionServiceClient(conn).StreamTelemetry(ctx)
			if err != nil {
				_ = conn.Close()
				return nil, err
			}
			c.setConn(conn)
			return stream, nil
		}
		c.keygenFn = c.generateDeviceKey

		batches := []*apiv1.TelemetryBatch{
			{DeviceId: "dev1", Readings: []*apiv1.SensorReading{{Sequence: 1, TimestampNanos: 1000, TemperatureC: 20}}},
		}
		out := make(chan *apiv1.TelemetryBatch, len(batches))
		for _, b := range batches {
			out <- b
		}
		close(out)

		if err := c.Consume(context.Background(), out); err != nil {
			t.Fatalf("Consume() error = %v, want nil", err)
		}
		if got := c.Received(); got != 1 {
			t.Fatalf("received = %d, want 1", got)
		}
		if len(srvImpl.received) != 1 {
			t.Fatalf("server received = %d, want 1", len(srvImpl.received))
		}
		req := srvImpl.received[0]
		if req.GetDeviceId() != "dev1" {
			t.Fatalf("device id = %q", req.GetDeviceId())
		}
		if req.GetSignatureAlgorithm() != apiv1.SignatureAlgorithm_ML_DSA_65 {
			t.Fatalf("alg = %v", req.GetSignatureAlgorithm())
		}
		if len(req.GetSignature()) == 0 {
			t.Fatal("empty signature")
		}
		gotBatch := &apiv1.TelemetryBatch{}
		if err := proto.Unmarshal(req.GetPayload(), gotBatch); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		sk, ok := c.deviceKeys[req.GetDeviceId()]
		if !ok {
			t.Fatal("no key cached")
		}
		if err := qscrypto.VerifyPayload(apiv1.SignatureAlgorithm_ML_DSA_65, sk.PublicKey().Bytes(), req.GetPayload(), req.GetSignature(), nil); err != nil {
			t.Fatalf("verify: %v", err)
		}
	})

	t.Run("tcp-loopback-derives-servername", func(t *testing.T) {
		serverCert, serverCAPool := newTestTLSMaterial(t)
		gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(qscrypto.ServerTLSConfig(serverCert, serverCAPool))))
		apiv1.RegisterTelemetryIngestionServiceServer(gs, &ingestServerTCP{})
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer lis.Close()
		go func() { _ = gs.Serve(lis) }()
		defer gs.Stop()

		clientCert := serverCert
		c := NewGrpcConsumer(GrpcConsumerConfig{
			Address:       lis.Addr().String(),
			TLSServerName: "",
			PQCEnabled:    true,
		})
		c.streamFn = func(ctx context.Context) (telemetryStream, error) {
			tlsCfg := qscrypto.ClientTLSConfig(clientCert, serverCAPool, deriveServerName(c.cfg.Address))
			conn, err := grpc.NewClient(c.cfg.Address,
				grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
			)
			if err != nil {
				return nil, err
			}
			stream, err := apiv1.NewTelemetryIngestionServiceClient(conn).StreamTelemetry(ctx)
			if err != nil {
				_ = conn.Close()
				return nil, err
			}
			c.setConn(conn)
			return stream, nil
		}
		c.keygenFn = c.generateDeviceKey

		batches := []*apiv1.TelemetryBatch{{DeviceId: "d", Readings: []*apiv1.SensorReading{{Sequence: 1}}}}
		out := make(chan *apiv1.TelemetryBatch, 1)
		out <- batches[0]
		close(out)
		if err := c.Consume(context.Background(), out); err != nil {
			t.Fatalf("Consume() error = %v", err)
		}
	})

	t.Run("server-name-mismatch-fails", func(t *testing.T) {
		serverCert, serverCAPool := newTestTLSMaterial(t)
		gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(qscrypto.ServerTLSConfig(serverCert, serverCAPool))))
		apiv1.RegisterTelemetryIngestionServiceServer(gs, &ingestServerTCP{})
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer lis.Close()
		go func() { _ = gs.Serve(lis) }()
		defer gs.Stop()

		clientCert := serverCert
		c := NewGrpcConsumer(GrpcConsumerConfig{
			Address:       lis.Addr().String(),
			TLSServerName: "wrong.example.com",
			PQCEnabled:    true,
		})
		c.streamFn = func(ctx context.Context) (telemetryStream, error) {
			tlsCfg := qscrypto.ClientTLSConfig(clientCert, serverCAPool, c.serverName())
			conn, err := grpc.NewClient(c.cfg.Address,
				grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
			)
			if err != nil {
				return nil, err
			}
			stream, err := apiv1.NewTelemetryIngestionServiceClient(conn).StreamTelemetry(ctx)
			if err != nil {
				_ = conn.Close()
				return nil, err
			}
			c.setConn(conn)
			return stream, nil
		}
		c.keygenFn = c.generateDeviceKey

		out := make(chan *apiv1.TelemetryBatch)
		close(out)
		err = c.Consume(context.Background(), out)
		if err == nil {
			t.Fatal("expected error on server name mismatch")
		}
	})

	t.Run("ca-mismatch-fails", func(t *testing.T) {
		serverCert, serverCAPool := newTestTLSMaterial(t)
		gs := grpc.NewServer(grpc.Creds(credentials.NewTLS(qscrypto.ServerTLSConfig(serverCert, serverCAPool))))
		apiv1.RegisterTelemetryIngestionServiceServer(gs, &ingestServerTCP{})
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer lis.Close()
		go func() { _ = gs.Serve(lis) }()
		defer gs.Stop()

		clientCert := serverCert
		c := NewGrpcConsumer(GrpcConsumerConfig{
			Address:       lis.Addr().String(),
			TLSServerName: deriveServerName(lis.Addr().String()),
			PQCEnabled:    true,
		})
		c.streamFn = func(ctx context.Context) (telemetryStream, error) {
			tlsCfg := qscrypto.ClientTLSConfig(clientCert, x509.NewCertPool(), c.serverName())
			conn, err := grpc.NewClient(c.cfg.Address,
				grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
			)
			if err != nil {
				return nil, err
			}
			stream, err := apiv1.NewTelemetryIngestionServiceClient(conn).StreamTelemetry(ctx)
			if err != nil {
				_ = conn.Close()
				return nil, err
			}
			c.setConn(conn)
			return stream, nil
		}
		c.keygenFn = c.generateDeviceKey

		out := make(chan *apiv1.TelemetryBatch)
		close(out)
		err = c.Consume(context.Background(), out)
		if err == nil {
			t.Fatal("expected error on CA mismatch")
		}
	})
}

type ingestServerTCP struct {
	apiv1.UnimplementedTelemetryIngestionServiceServer
}

func (s *ingestServerTCP) StreamTelemetry(stream apiv1.TelemetryIngestionService_StreamTelemetryServer) error {
	for {
		_, err := stream.Recv()
		if err == nil {
			_ = stream.Send(&apiv1.TelemetryIngestResponse{AckedSequence: 1, Status: apiv1.IngestStatus_INGEST_STATUS_ACCEPTED})
			continue
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
}

type bufconnServer struct {
	apiv1.UnimplementedTelemetryIngestionServiceServer
	impl *struct {
		received []*apiv1.TelemetryIngestRequest
	}
}

func (s *bufconnServer) StreamTelemetry(stream apiv1.TelemetryIngestionService_StreamTelemetryServer) error {
	for {
		req, err := stream.Recv()
		if err == nil {
			if s.impl != nil {
				s.impl.received = append(s.impl.received, req)
			}
			_ = stream.Send(&apiv1.TelemetryIngestResponse{AckedSequence: 1, Status: apiv1.IngestStatus_INGEST_STATUS_ACCEPTED})
			continue
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
}

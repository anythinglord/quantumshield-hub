// Test-only harness for the task 2.4 gRPC streaming client.
//
// It provides two building blocks the consumer unit (WU2) and the real
// transport unit (WU3) are built on:
//
//   - fakeStream: an in-process stand-in for the generated bidirectional
//     stream client. It records what the simulator sent, scripts what the
//     server answers, counts CloseSend calls, and can stall a Send to model
//     transport backpressure.
//   - newTestTLSMaterial: a throwaway Ed25519 CA and leaf certificate built
//     in memory. No openssl, no gen-certs.sh, no committed certificate
//     files.
//
// The file is a same-package test file on purpose: the consumer seam is
// unexported, so the fake has to live next to it.
package simulator

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
	"google.golang.org/grpc"
)

// Compile-time proof that the fake honours the generated client contract.
var _ grpc.BidiStreamingClient[apiv1.TelemetryIngestRequest, apiv1.TelemetryIngestResponse] = (*fakeStream)(nil)

// recvResult is one scripted outcome of fakeStream.Recv: either a response
// or a terminal error. A nil response together with a nil error is
// normalized to io.EOF (the server closed the stream cleanly).
type recvResult struct {
	resp *apiv1.TelemetryIngestResponse
	err  error
}

// fakeStream is an in-process stand-in for the generated bidi stream client.
//
// The embedded grpc.ClientStream is deliberately left nil: the simulator seam
// only exercises Send, Recv and CloseSend, so a nil embedded interface turns
// an accidental Header/Trailer/Context/SendMsg/RecvMsg call into an
// immediate panic instead of a silently wrong zero value.
//
// Recv state machine:
//
//	scripted response     -> keep the stream open, serve the next entry
//	scripted error        -> terminal, the stream is aborted
//	script exhausted and
//	CloseSend was seen    -> terminal io.EOF (the server drained the request)
//	script exhausted and
//	the send half is open -> block until the stream context is canceled
//
// Send records the request on entry and then blocks on the optional send
// gate. That mirrors gRPC flow control: the message is already handed to
// the transport (and recorded), but Send does not return until the transport
// accepts traffic again. The gate is the backpressure lever the producer
// scenarios need.
type fakeStream struct {
	grpc.ClientStream

	mu             sync.Mutex
	sent           []*apiv1.TelemetryIngestRequest
	script         []recvResult
	closeSendCount int
	closeSent      bool

	ctx         context.Context
	sendGate    chan struct{}
	sendEntered chan struct{} // coalesced: a Send is about to park on the gate
	recvEntered chan struct{} // coalesced: a Recv was entered
	closeNotify chan struct{} // closed once, by the first CloseSend
	recvDone    chan struct{} // closed once, when Recv returned a terminal result

	closeOnce    sync.Once
	recvDoneOnce sync.Once
}

// fakeStreamOpt customizes a fakeStream at construction time.
type fakeStreamOpt func(*fakeStream)

// withSendGate makes Send block until gate is closed. A nil gate keeps the
// ungated behaviour (Send returns immediately).
func withSendGate(gate chan struct{}) fakeStreamOpt {
	return func(f *fakeStream) {
		f.sendGate = gate
	}
}

// newFakeStream returns a fake bidi stream bound to ctx. The script is
// consumed by Recv in order; it models the server-side answers the test
// wants to observe.
func newFakeStream(ctx context.Context, script []recvResult, opts ...fakeStreamOpt) *fakeStream {
	f := &fakeStream{
		script:      script,
		ctx:         ctx,
		sendEntered: make(chan struct{}, 1),
		recvEntered: make(chan struct{}, 1),
		closeNotify: make(chan struct{}),
		recvDone:    make(chan struct{}),
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// Send records req and returns nil. With a send gate configured the call
// blocks until the gate is closed, so the caller observes transport
// backpressure instead of an unbounded send loop.
func (f *fakeStream) Send(req *apiv1.TelemetryIngestRequest) error {
	f.mu.Lock()
	f.sent = append(f.sent, req)
	gate := f.sendGate
	f.mu.Unlock()

	if gate == nil {
		return nil
	}
	// Coalesced signal: the test can wait until the send is parked on the
	// gate without polling.
	select {
	case f.sendEntered <- struct{}{}:
	default:
	}
	<-gate
	return nil
}

// Recv serves the next scripted result, or ends the stream. See the type
// documentation for the full state machine.
func (f *fakeStream) Recv() (*apiv1.TelemetryIngestResponse, error) {
	select {
	case f.recvEntered <- struct{}{}:
	default:
	}

	f.mu.Lock()
	if len(f.script) > 0 {
		r := f.script[0]
		f.script = f.script[1:]
		f.mu.Unlock()
		if r.err != nil {
			f.finishRecv()
			return nil, r.err
		}
		if r.resp == nil {
			f.finishRecv()
			return nil, io.EOF
		}
		return r.resp, nil
	}
	closeSent := f.closeSent
	f.mu.Unlock()

	if closeSent {
		f.finishRecv()
		return nil, io.EOF
	}
	// The server has nothing left to say and the send half is still open:
	// wait for the stream to end.
	select {
	case <-f.ctx.Done():
		f.finishRecv()
		return nil, f.ctx.Err()
	case <-f.closeNotify:
		f.finishRecv()
		return nil, io.EOF
	}
}

// CloseSend closes the send half of the stream, counts the call, and wakes a
// parked Recv with io.EOF so the consumer can drain and join.
func (f *fakeStream) CloseSend() error {
	f.mu.Lock()
	f.closeSendCount++
	f.closeSent = true
	f.mu.Unlock()

	f.closeOnce.Do(func() { close(f.closeNotify) })
	return nil
}

// finishRecv closes recvDone exactly once. It marks the end of the Recv side
// of the stream, which is the point the consumer's recv goroutine exits on.
func (f *fakeStream) finishRecv() {
	f.recvDoneOnce.Do(func() { close(f.recvDone) })
}

// sentRequests returns a copy of the recorded requests, in send order.
func (f *fakeStream) sentRequests() []*apiv1.TelemetryIngestRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]*apiv1.TelemetryIngestRequest, len(f.sent))
	copy(out, f.sent)
	return out
}

// closeSendCalls returns how many times CloseSend was called.
func (f *fakeStream) closeSendCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.closeSendCount
}

// recvDoneCh returns the channel that is closed when the Recv side of the
// stream reached its terminal result.
func (f *fakeStream) recvDoneCh() <-chan struct{} {
	return f.recvDone
}

// sendEnteredCh returns the coalesced signal emitted when a Send parks on
// the send gate.
func (f *fakeStream) sendEnteredCh() <-chan struct{} {
	return f.sendEntered
}

// recvEnteredCh returns the coalesced signal emitted when a Recv is entered.
func (f *fakeStream) recvEnteredCh() <-chan struct{} {
	return f.recvEntered
}

// waitRecvDone blocks until the Recv side of the stream is finished. A hang
// here means the consumer never joined its recv goroutine; the go test
// timeout surfaces it.
func (f *fakeStream) waitRecvDone() {
	<-f.recvDone
}

// newTestTLSMaterial builds a throwaway Ed25519 CA and a leaf certificate
// signed by it, entirely in memory: no openssl, no gen-certs.sh, no
// committed certificate files.
//
// The leaf carries SAN DNS:localhost and IP:127.0.0.1 and is valid for both
// ExtKeyUsageServerAuth and ExtKeyUsageClientAuth, so the same certificate
// can act as the simulator's client identity and as the server identity of
// the in-test TLS server.
//
// The material never leaves the process: it is generated per test and
// discarded with it, so there is nothing to clean up.
func newTestTLSMaterial(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	cert, pool, _ := newTestTLSMaterialWithCA(t)
	return cert, pool
}

// newTestTLSMaterialWithCA is like newTestTLSMaterial but also returns the
// generated CA certificate, so callers that need to persist the trust roots
// to disk (e.g. the file-based dialStream coverage test) can encode them.
func newTestTLSMaterialWithCA(t *testing.T) (tls.Certificate, *x509.CertPool, *x509.Certificate) {
	t.Helper()

	caPub, caPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "quantumshield-test-ca"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPub, caPriv)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}

	leafPub, leafPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCert, leafPub, caPriv)
	if err != nil {
		t.Fatalf("create leaf certificate: %v", err)
	}
	leafKeyDER, err := x509.MarshalPKCS8PrivateKey(leafPriv)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: leafKeyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("build key pair: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)

	return cert, pool, caCert
}

// TestHarness_FakeStreamRecordsSends proves the fake bidi stream records
// what the client sent and counts CloseSend calls.
func TestHarness_FakeStreamRecordsSends(t *testing.T) {
	t.Run("records sends in order and counts close send", func(t *testing.T) {
		fake := newFakeStream(context.Background(), []recvResult{
			{resp: &apiv1.TelemetryIngestResponse{AckedSequence: 7, Status: apiv1.IngestStatus_INGEST_STATUS_ACCEPTED}},
		})
		reqs := []*apiv1.TelemetryIngestRequest{
			{DeviceId: "device-0000"},
			{DeviceId: "device-0001"},
		}
		for _, req := range reqs {
			if err := fake.Send(req); err != nil {
				t.Fatalf("Send(%q) error = %v, want nil", req.GetDeviceId(), err)
			}
		}
		if err := fake.CloseSend(); err != nil {
			t.Fatalf("CloseSend() error = %v, want nil", err)
		}

		sent := fake.sentRequests()
		if len(sent) != len(reqs) {
			t.Fatalf("recorded sends = %d, want %d", len(sent), len(reqs))
		}
		for i, req := range reqs {
			if sent[i].GetDeviceId() != req.GetDeviceId() {
				t.Fatalf("recorded send %d device id = %q, want %q", i, sent[i].GetDeviceId(), req.GetDeviceId())
			}
		}
		if got := fake.closeSendCalls(); got != 1 {
			t.Fatalf("closeSendCalls() = %d, want 1", got)
		}
	})

	t.Run("gated send blocks until the gate opens", func(t *testing.T) {
		gate := make(chan struct{})
		fake := newFakeStream(context.Background(), nil, withSendGate(gate))
		result := make(chan error, 1)
		go func() {
			result <- fake.Send(&apiv1.TelemetryIngestRequest{DeviceId: "device-0000"})
		}()

		select {
		case <-fake.sendEnteredCh():
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for the send to reach the gate")
		}
		select {
		case err := <-result:
			t.Fatalf("Send returned %v while the gate was closed, want blocked", err)
		default:
		}
		if got := len(fake.sentRequests()); got != 1 {
			t.Fatalf("recorded sends = %d while gated, want 1", got)
		}

		close(gate)
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("Send error = %v, want nil", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for the gated send to complete")
		}
	})
}

// TestHarness_FakeStreamRecv proves the fake Recv serves the scripted
// results, ends the stream exactly once, and blocks until the stream
// context is canceled when the server has nothing left to say.
func TestHarness_FakeStreamRecv(t *testing.T) {
	t.Run("serves the script then reports end of stream", func(t *testing.T) {
		fake := newFakeStream(context.Background(), []recvResult{
			{resp: &apiv1.TelemetryIngestResponse{AckedSequence: 1, Status: apiv1.IngestStatus_INGEST_STATUS_ACCEPTED}},
			{resp: &apiv1.TelemetryIngestResponse{AckedSequence: 2, Status: apiv1.IngestStatus_INGEST_STATUS_REJECTED_SIGNATURE}},
		})

		first, err := fake.Recv()
		if err != nil {
			t.Fatalf("first Recv() error = %v, want nil", err)
		}
		if first.GetAckedSequence() != 1 {
			t.Fatalf("acked sequence = %d, want 1", first.GetAckedSequence())
		}
		if first.GetStatus() != apiv1.IngestStatus_INGEST_STATUS_ACCEPTED {
			t.Fatalf("status = %v, want %v", first.GetStatus(), apiv1.IngestStatus_INGEST_STATUS_ACCEPTED)
		}

		second, err := fake.Recv()
		if err != nil {
			t.Fatalf("second Recv() error = %v, want nil", err)
		}
		if second.GetStatus() != apiv1.IngestStatus_INGEST_STATUS_REJECTED_SIGNATURE {
			t.Fatalf("status = %v, want %v", second.GetStatus(), apiv1.IngestStatus_INGEST_STATUS_REJECTED_SIGNATURE)
		}

		// Closing the send half ends the stream: the next Recv reports EOF.
		if err := fake.CloseSend(); err != nil {
			t.Fatalf("CloseSend() error = %v, want nil", err)
		}
		if _, err := fake.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("Recv() after CloseSend error = %v, want io.EOF", err)
		}
		// A second terminal Recv must not panic (recvDone is closed once).
		if _, err := fake.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("repeated terminal Recv() error = %v, want io.EOF", err)
		}
		select {
		case <-fake.recvDoneCh():
		default:
			t.Fatal("recvDone not closed after the terminal Recv")
		}
	})

	t.Run("blocks until the stream context is canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fake := newFakeStream(ctx, nil)
		result := make(chan error, 1)
		go func() {
			_, err := fake.Recv()
			result <- err
		}()

		select {
		case <-fake.recvEnteredCh():
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Recv to be entered")
		}
		select {
		case err := <-result:
			t.Fatalf("Recv returned %v before the context was canceled, want blocked", err)
		default:
		}

		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Recv error = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for Recv to unblock")
		}
		fake.waitRecvDone()
	})
}

// TestHarness_TestTLSMaterial proves the in-memory certificate material is
// usable on both sides of the mTLS handshake: the leaf verifies against the
// returned CA pool for localhost and 127.0.0.1 and is valid for server and
// for client authentication.
func TestHarness_TestTLSMaterial(t *testing.T) {
	cert, pool := newTestTLSMaterial(t)
	if len(cert.Certificate) == 0 {
		t.Fatal("no certificate in the returned key pair")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf certificate: %v", err)
	}

	for _, tc := range []struct {
		name     string
		host     string
		keyUsage x509.ExtKeyUsage
	}{
		{"dns name for server auth", "localhost", x509.ExtKeyUsageServerAuth},
		{"dns name for client auth", "localhost", x509.ExtKeyUsageClientAuth},
		{"ip san for server auth", "127.0.0.1", x509.ExtKeyUsageServerAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := leaf.Verify(x509.VerifyOptions{
				Roots:     pool,
				DNSName:   tc.host,
				KeyUsages: []x509.ExtKeyUsage{tc.keyUsage},
			}); err != nil {
				t.Fatalf("verify leaf for %q: %v", tc.host, err)
			}
		})
	}
}

package simulator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"

	"crypto/mldsa"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
	qscrypto "github.com/anythinglord/quantumshield-hub/internal/crypto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

// telemetryStream is the generated bidirectional stream client type of
// TelemetryIngestionService.StreamTelemetry.
type telemetryStream = grpc.BidiStreamingClient[apiv1.TelemetryIngestRequest, apiv1.TelemetryIngestResponse]

// GrpcConsumerConfig holds the transport settings of a GrpcConsumer.
type GrpcConsumerConfig struct {
	// Address is the gRPC target, host:port.
	Address string
	// TLSCert and TLSKey point at the PEM client identity (chain and key).
	TLSCert string
	TLSKey  string
	// TLSCA points at the PEM bundle of trusted server roots.
	TLSCA string
	// TLSServerName overrides the TLS server name. When empty the host is
	// derived from Address (design D4).
	TLSServerName string
	// PQCEnabled toggles per-payload ML-DSA signing (design D2). When false
	// the signature is left empty and the algorithm is UNSPECIFIED.
	PQCEnabled bool
}

// GrpcConsumer streams telemetry batches to a TelemetryIngestionService
// server over a single bidirectional gRPC stream.
//
// It implements the fleet's ConsumerFunc seam: batches are read from the
// fan-in channel, signed, and sent; acknowledgements are drained by a
// concurrent recv goroutine that is always joined before Consume returns.
type GrpcConsumer struct {
	cfg GrpcConsumerConfig

	// streamFn opens the bidirectional stream. It is the test seam: the
	// default implementation dials the server with the strict mTLS policy
	// and loads the TLS material lazily (design A4).
	streamFn func(ctx context.Context) (telemetryStream, error)

	// keygenFn generates an ML-DSA key (test seam). When nil, the default
	// implementation uses mldsa.GenerateKey for the ML-DSA-65 parameters.
	keygenFn func() (*mldsa.PrivateKey, error)

	// received counts the batches handed to the stream.
	received atomic.Uint64

	// acks counts TelemetryIngestResponse statuses by enum value (design A3).
	// The recv goroutine is the sole writer; atomic reads are race-free.
	acks [5]atomic.Uint64

	// device keys are provisioned lazily per device_id and cached for the
	// lifetime of the consumer (design D1).
	keyMu      sync.Mutex
	deviceKeys map[string]*mldsa.PrivateKey

	connMu sync.Mutex
	conn   *grpc.ClientConn
}

// NewGrpcConsumer creates a consumer for cfg.
//
// The constructor is pure: no file is read, no key is generated and no
// connection is opened until Consume runs.
func NewGrpcConsumer(cfg GrpcConsumerConfig) *GrpcConsumer {
	c := &GrpcConsumer{
		cfg:        cfg,
		deviceKeys: make(map[string]*mldsa.PrivateKey),
	}
	c.streamFn = c.dialStream
	c.keygenFn = c.generateDeviceKey
	return c
}

// Consume implements the fleet's ConsumerFunc seam.
//
// Every batch read from out is marshalled and sent on the stream; the
// acknowledgements are drained by a concurrent goroutine. The consumer
// returns nil on a clean shutdown (the fan-in closed or the context
// cancelled) and a wrapped error when the stream fails.
func (c *GrpcConsumer) Consume(ctx context.Context, out <-chan *apiv1.TelemetryBatch) error {
	// sctx is canceled on every exit path, so the recv goroutine is always
	// released even when the send loop aborts.
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer c.closeConn()

	stream, err := c.streamFn(sctx)
	if err != nil {
		return fmt.Errorf("simulator: stream: %w", err)
	}

	// The recv outcome travels on a capacity-1 buffered channel (design A1):
	// the write never blocks, so the recv goroutine always exits even when
	// nobody reads the value, and the send loop is woken as soon as the
	// stream breaks.
	recvErrCh := make(chan error, 1)
	recvDoneCh := make(chan struct{})
	go c.recvLoop(sctx, stream, recvErrCh, recvDoneCh)

	var (
		recvErr    error
		recvDone   bool
		halfClosed bool
	)
	for !recvDone {
		select {
		case recvErr = <-recvErrCh:
			// The recv goroutine closed recvDoneCh before publishing the
			// outcome, so receiving the value is the join proof.
			recvDone = true
		case batch, ok := <-out:
			if !ok {
				// Half-close the stream exactly once, in this branch only:
				// a deferred CloseSend would double-close it on the abort
				// paths.
				halfClosed = true
				if err := stream.CloseSend(); err != nil {
					return fmt.Errorf("simulator: close send: %w", err)
				}
				// Drain the remaining acknowledgements. The value is read
				// only when the wake path did not consume it already.
				if !recvDone {
					recvErr = <-recvErrCh
					recvDone = true
				}
				continue
			}
			if err := c.sendBatch(stream, batch); err != nil {
				if sctx.Err() != nil {
					// Shutdown race: the stream died because it is being
					// torn down, which is a clean shutdown.
					return nil
				}
				return err
			}
		}
	}

	// Join: the recv goroutine closed recvDoneCh before publishing its
	// outcome, so this receive never blocks.
	<-recvDoneCh
	return c.mapRecvOutcome(sctx, recvErr, halfClosed)
}

// sendBatch marshals the batch and signs it, then sends the request.
//
// When PQCEnabled the signature covers the marshalled payload bytes using
// the device's lazily generated ML-DSA-65 key; otherwise the signature is
// left empty and the algorithm is UNSPECIFIED with no key generation.
func (c *GrpcConsumer) sendBatch(stream telemetryStream, batch *apiv1.TelemetryBatch) error {
	payload, err := proto.Marshal(batch)
	if err != nil {
		return fmt.Errorf("simulator: marshal batch: %w", err)
	}
	req := &apiv1.TelemetryIngestRequest{
		DeviceId: batch.GetDeviceId(),
		Payload:  payload,
	}

	if c.cfg.PQCEnabled {
		sk, err := c.deviceKey(batch.GetDeviceId())
		if err != nil {
			return err
		}
		sig, err := qscrypto.SignPayload(sk, payload, nil)
		if err != nil {
			return fmt.Errorf("simulator: sign payload: %w", err)
		}
		req.Signature = sig
		req.SignatureAlgorithm = apiv1.SignatureAlgorithm_ML_DSA_65
	} else {
		req.Signature = nil
		req.SignatureAlgorithm = apiv1.SignatureAlgorithm_SIGNATURE_ALGORITHM_UNSPECIFIED
	}

	if err := stream.Send(req); err != nil {
		return fmt.Errorf("simulator: send: %w", err)
	}
	c.received.Add(1)
	return nil
}

// deviceKey returns the cached ML-DSA key for deviceID, generating it on
// first use (design D1).
func (c *GrpcConsumer) deviceKey(deviceID string) (*mldsa.PrivateKey, error) {
	// Hold the lock across keygen so it is single-flight per device id:
	// concurrent first-batches for the same device cannot invoke keygenFn
	// more than once (design A2/D1).
	c.keyMu.Lock()
	defer c.keyMu.Unlock()

	if sk, ok := c.deviceKeys[deviceID]; ok {
		return sk, nil
	}

	sk, err := c.keygenFn()
	if err != nil {
		return nil, fmt.Errorf("simulator: generate device key: %w", err)
	}

	// Re-check in case a nested caller populated the cache; the lock is
	// held, so this is defensive only.
	if existing, ok := c.deviceKeys[deviceID]; ok {
		return existing, nil
	}
	c.deviceKeys[deviceID] = sk
	return sk, nil
}

// generateDeviceKey generates an ML-DSA-65 key for a device.
func (c *GrpcConsumer) generateDeviceKey() (*mldsa.PrivateKey, error) {
	params, err := qscrypto.ParamsFor(apiv1.SignatureAlgorithm_ML_DSA_65)
	if err != nil {
		return nil, err
	}
	sk, err := mldsa.GenerateKey(params)
	if err != nil {
		return nil, err
	}
	return sk, nil
}

// recvLoop drains the acknowledgements until the stream ends.
//
// io.EOF is the clean end of a server stream and is reported as a nil
// error; a stream torn down by the shutdown path is a clean end too, never
// a failure. The last two statements close recvDoneCh and publish the
// outcome, so a reader of the outcome knows the loop is over. The recv
// goroutine is the sole writer of ack counters (design A3).
func (c *GrpcConsumer) recvLoop(sctx context.Context, stream telemetryStream, recvErrCh chan<- error, recvDoneCh chan<- struct{}) {
	var recvErr error
	for {
		resp, err := stream.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				recvErr = err
			}
			break
		}
		if resp != nil {
			status := resp.GetStatus()
			if int(status) >= 0 && int(status) < len(c.acks) {
				c.acks[status].Add(1)
			}
		}
	}
	if sctx.Err() != nil {
		// The stream was torn down by the shutdown path.
		recvErr = nil
	}
	close(recvDoneCh)
	recvErrCh <- recvErr
}

// mapRecvOutcome maps the recv outcome onto the Consume contract: a clean
// shutdown returns nil, a premature end of stream aborts.
func (c *GrpcConsumer) mapRecvOutcome(sctx context.Context, recvErr error, halfClosed bool) error {
	if sctx.Err() != nil {
		// The context was cancelled: a clean shutdown, never ctx.Err().
		return nil
	}
	if recvErr == nil {
		if halfClosed {
			// The server drained the stream after we half-closed it.
			return nil
		}
		return fmt.Errorf("simulator: stream closed by server")
	}
	return fmt.Errorf("simulator: stream: %w", recvErr)
}

// Received returns the number of batches handed to the stream.
func (c *GrpcConsumer) Received() uint64 {
	return c.received.Load()
}

// Acks returns a snapshot of ack counts by IngestStatus after the recv loop
// has joined (design A3).
func (c *GrpcConsumer) Acks() map[apiv1.IngestStatus]uint64 {
	out := make(map[apiv1.IngestStatus]uint64, len(c.acks))
	for i := range c.acks {
		if v := c.acks[i].Load(); v != 0 {
			out[apiv1.IngestStatus(i)] = v
		}
	}
	return out
}

// dialStream is the default stream factory: it loads the TLS material
// lazily (design A4), dials with the strict client policy and opens the
// bidirectional stream.
func (c *GrpcConsumer) dialStream(ctx context.Context) (telemetryStream, error) {
	cert, err := qscrypto.LoadKeyPair(c.cfg.TLSCert, c.cfg.TLSKey)
	if err != nil {
		return nil, fmt.Errorf("simulator: client certificate: %w", err)
	}
	pool, err := qscrypto.LoadCertPool(c.cfg.TLSCA)
	if err != nil {
		return nil, fmt.Errorf("simulator: ca bundle: %w", err)
	}
	tlsCfg := qscrypto.ClientTLSConfig(cert, pool, c.serverName())
	conn, err := grpc.NewClient(c.cfg.Address, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	if err != nil {
		return nil, fmt.Errorf("simulator: dial %q: %w", c.cfg.Address, err)
	}
	stream, err := apiv1.NewTelemetryIngestionServiceClient(conn).StreamTelemetry(ctx)
	if err != nil {
		// The stream never opened, so the connection has no other user.
		_ = conn.Close()
		return nil, fmt.Errorf("simulator: open stream: %w", err)
	}
	c.setConn(conn)
	return stream, nil
}

// serverName returns the TLS server name: the explicit configuration wins,
// otherwise the host is derived from the address (design D4).
func (c *GrpcConsumer) serverName() string {
	if c.cfg.TLSServerName != "" {
		return c.cfg.TLSServerName
	}
	return deriveServerName(c.cfg.Address)
}

// deriveServerName extracts the host part of a host:port address. An
// address that cannot be split is returned unchanged.
func deriveServerName(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}
	return host
}

// setConn stores the dialed connection so it can be released when the
// consumer is done with it.
func (c *GrpcConsumer) setConn(conn *grpc.ClientConn) {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	c.conn = conn
}

// closeConn releases the dialed connection, if any.
func (c *GrpcConsumer) closeConn() {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()

	if conn == nil {
		return
	}
	// The stream is already finished at this point; a close error carries
	// no additional information for the caller.
	_ = conn.Close()
}

package ingestion

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// Compile-time proof that the handler can be registered as the generated
// TelemetryIngestionService implementation.
var _ apiv1.TelemetryIngestionServiceServer = (*Handler)(nil)

// RowDispatcher accepts one flattened telemetry row for asynchronous
// persistence. WorkerPool implements it; tests inject a synchronous fake.
type RowDispatcher interface {
	Enqueue(ctx context.Context, row TelemetryRow) error
}

// Handler serves the bidirectional StreamTelemetry RPC.
//
// It drains Recv until io.EOF, sends exactly one response per received request
// (design A6), and never returns before the client half-closes: the simulator
// treats an early server close as a stream failure. Every request is verified
// through the Verifier seam, flattened into TelemetryRows, and handed to the
// RowDispatcher (the sharded worker pool).
//
// One stream multiplexes every device, so acked_sequence is tracked per
// device_id, not per stream (design A6).
type Handler struct {
	apiv1.UnimplementedTelemetryIngestionServiceServer

	dispatcher RowDispatcher
	verifier   Verifier
}

// NewHandler returns a handler that verifies requests through verifier and
// dispatches accepted rows through dispatcher.
//
// The verifier is constructor-injected so Phase 4 can swap AcceptAllVerifier
// for AuthBackedVerifier without touching this file (design Option C).
func NewHandler(dispatcher RowDispatcher, verifier Verifier) (*Handler, error) {
	if dispatcher == nil {
		return nil, errors.New("ingestion: handler: dispatcher is nil")
	}
	if verifier == nil {
		return nil, errors.New("ingestion: handler: verifier is nil")
	}
	return &Handler{dispatcher: dispatcher, verifier: verifier}, nil
}

// StreamTelemetry drains the stream to io.EOF and acknowledges every request.
//
// The stream context drives verification and dispatch so a canceled client
// releases the handler promptly. Errors are wrapped with %w; io.EOF is the
// only clean terminal condition.
func (h *Handler) StreamTelemetry(stream grpc.BidiStreamingServer[apiv1.TelemetryIngestRequest, apiv1.TelemetryIngestResponse]) error {
	ctx := stream.Context()
	ackedSequences := make(map[string]uint64)

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("ingestion: stream: receive: %w", err)
		}

		resp, err := h.handle(ctx, req, ackedSequences)
		if err != nil {
			return err
		}
		if err := stream.Send(resp); err != nil {
			return fmt.Errorf("ingestion: stream: send ack for device %q: %w", req.GetDeviceId(), err)
		}
	}
}

// handle maps one request to its acknowledgement (design A7).
//
// The current sequence of a device is only advanced after every reading of an
// accepted request has been enqueued, so a rejected or failed request never
// moves the ack forward.
func (h *Handler) handle(
	ctx context.Context,
	req *apiv1.TelemetryIngestRequest,
	ackedSequences map[string]uint64,
) (*apiv1.TelemetryIngestResponse, error) {
	deviceID := req.GetDeviceId()
	lastAcked := ackedSequences[deviceID]

	if deviceID == "" {
		return rejectedAck(lastAcked, apiv1.IngestStatus_INGEST_STATUS_REJECTED_DEVICE), nil
	}

	batch := &apiv1.TelemetryBatch{}
	if err := proto.Unmarshal(req.GetPayload(), batch); err != nil {
		// The signature covers the exact serialized batch, so a payload that
		// cannot be unmarshalled cannot be trusted either.
		return rejectedAck(lastAcked, apiv1.IngestStatus_INGEST_STATUS_REJECTED_SIGNATURE), nil
	}

	if err := h.verifier.Verify(ctx, deviceID, req.GetSignatureAlgorithm(), req.GetPayload(), req.GetSignature()); err != nil {
		status := apiv1.IngestStatus_INGEST_STATUS_REJECTED_SIGNATURE
		if errors.Is(err, ErrUnknownDevice) {
			status = apiv1.IngestStatus_INGEST_STATUS_REJECTED_DEVICE
		}
		return rejectedAck(lastAcked, status), nil
	}

	rows, highest := flattenReadings(deviceID, batch.GetReadings())
	for _, row := range rows {
		if err := h.dispatcher.Enqueue(ctx, row); err != nil {
			return nil, fmt.Errorf("ingestion: stream: dispatch device %q sequence %d: %w", deviceID, row.Sequence, err)
		}
	}

	if highest > lastAcked {
		lastAcked = highest
	}
	ackedSequences[deviceID] = lastAcked

	return &apiv1.TelemetryIngestResponse{
		AckedSequence: lastAcked,
		Status:        apiv1.IngestStatus_INGEST_STATUS_ACCEPTED,
	}, nil
}

// rejectedAck builds the single acknowledgement owed to a rejected request. It
// carries the device's last accepted sequence so the edge node can tell what
// the server has durably accepted (design A6/A7).
func rejectedAck(acked uint64, status apiv1.IngestStatus) *apiv1.TelemetryIngestResponse {
	return &apiv1.TelemetryIngestResponse{AckedSequence: acked, Status: status}
}

// flattenReadings converts one signed TelemetryBatch into database rows.
//
// The device identity from the request is authoritative (it is the identity
// the verifier authenticated). GPS is optional: a reading without a position
// leaves Lat/Long nil. The highest sequence in the batch is returned for the
// per-device ack (discovery #844).
func flattenReadings(deviceID string, readings []*apiv1.SensorReading) ([]TelemetryRow, uint64) {
	rows := make([]TelemetryRow, 0, len(readings))
	var highest uint64

	for _, reading := range readings {
		if reading == nil {
			continue
		}
		row := TelemetryRow{
			DeviceID:     deviceID,
			Time:         time.Unix(0, reading.GetTimestampNanos()).UTC(),
			Sequence:     int64(reading.GetSequence()),
			TemperatureC: reading.GetTemperatureC(),
			VoltageV:     reading.GetVoltageV(),
			CurrentA:     reading.GetCurrentA(),
		}
		if position := reading.GetPosition(); position != nil {
			lat, long := position.GetLatitude(), position.GetLongitude()
			row.Lat = &lat
			row.Long = &long
		}
		rows = append(rows, row)
		if sequence := reading.GetSequence(); sequence > highest {
			highest = sequence
		}
	}

	return rows, highest
}

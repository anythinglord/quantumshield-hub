// Package simulator generates deterministic, realistically-ranged synthetic
// telemetry (temperature, voltage, current, WGS84 position) as typed api/v1
// messages for the QuantumShield device simulator.
//
// NOT SAFE FOR CONCURRENT USE: math/rand/v2's *rand.Rand must be driven by a
// single goroutine. Create ONE Generator per device and keep it for the whole
// device/stream lifetime — recreating a Generator resets Sequence to 0 and
// breaks the ingestion side's acked_sequence contract (telemetry.proto,
// TelemetryIngestResponse). Synchronization is deliberately NOT provided:
// task 2.3 owns per-device goroutines; a shared lock would serialize sampling
// and couple device streams.
//
// Determinism: New(seed) builds its source as rand.NewPCG(seed, streamSalt),
// so the same seed and call sequence always replays the identical stream;
// different seeds diverge.
//
// Interval conventions: sensor metrics use lo+(hi-lo)*r.Float64() and are
// HALF-OPEN — [lo, hi), upper bound EXCLUSIVE (range tests assert v < hi).
// GPS position is base ± jitter, then clamped into the CLOSED WGS84 ranges
// [-90, 90] / [-180, 180], so a clamped reading may equal ±90/±180 exactly.
package simulator

import (
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
)

// streamSalt is the fixed second PCG word: seed → one deterministic stream.
const streamSalt = 0x9E3779B97F4A7C15

// Metric bounds are HALF-OPEN [min, max) (R-002); jitter and WGS84 clamp
// bounds follow the CLOSED GPS convention (R-003).
const (
	tempMinC, tempMaxC           = 30.0, 95.0    // °C — idle..thermal-throttle
	voltageMinV, voltageMaxV     = 3.0, 4.2      // V — Li-ion cutoff..full charge
	currentMinA, currentMaxA     = 0.05, 2.0     // A — sleep floor..actuator burst
	jitterDeg                    = 0.001         // ±0.001° per GPS axis (~100 m)
	defaultLatMin, defaultLatMax = -60.0, 60.0   // New() base band (D2), half-open
	defaultLonMin, defaultLonMax = -180.0, 180.0 // half-open → base < 180, WGS84-safe
	wgs84LatMin, wgs84LatMax     = -90.0, 90.0   // CLOSED clamp bounds
	wgs84LonMin, wgs84LonMax     = -180.0, 180.0 // CLOSED clamp bounds
)

// Generator produces synthetic SensorReadings for exactly ONE simulated edge
// device. It is NOT safe for concurrent use: use one Generator per device,
// owned by that device's goroutine, and keep it for the device's entire
// stream lifetime — a mid-stream restart resets Sequence and breaks
// acked_sequence (telemetry.proto).
type Generator struct {
	r       *rand.Rand // seeded PCG; single-goroutine ownership (see package doc)
	seq     uint64     // next Sequence; starts 0, ++ per reading, never reset
	baseLat float64    // WGS84 deg; valid by construction (D2) or validated (D3)
	baseLon float64
}

// New returns a Generator whose base position is a deterministic function of
// seed (two PCG draws: lat ∈ [-60,60), lon ∈ [-180,180) — D2). It never fails.
func New(seed uint64) *Generator {
	r := rand.New(rand.NewPCG(seed, streamSalt))
	g := &Generator{r: r}
	g.baseLat = g.nextFloat(defaultLatMin, defaultLatMax)
	g.baseLon = g.nextFloat(defaultLonMin, defaultLonMax)
	return g
}

// NewWithPosition anchors a Generator at the explicit WGS84 base, or returns
// (nil, error) when lat/lon is non-finite or out of bounds — rejected, never
// clamped (R-001).
func NewWithPosition(seed uint64, lat, lon float64) (*Generator, error) {
	if err := validateBase(lat, lon); err != nil {
		return nil, err
	}
	return &Generator{r: rand.New(rand.NewPCG(seed, streamSalt)), baseLat: lat, baseLon: lon}, nil
}

// validateBase rejects non-finite coordinates FIRST (NaN fails every
// comparison — 2.1 D-5), then the CLOSED WGS84 bounds. Plain fmt.Errorf with
// a `simulator:` prefix and no %w: there is no wrapped cause (design D3).
func validateBase(lat, lon float64) error {
	if math.IsNaN(lat) || math.IsInf(lat, 0) {
		return fmt.Errorf("simulator: latitude %v is not a finite number", lat)
	}
	if math.IsNaN(lon) || math.IsInf(lon, 0) {
		return fmt.Errorf("simulator: longitude %v is not a finite number", lon)
	}
	if lat < wgs84LatMin || lat > wgs84LatMax {
		return fmt.Errorf("simulator: latitude %v out of WGS84 range [-90, 90]", lat)
	}
	if lon < wgs84LonMin || lon > wgs84LonMax {
		return fmt.Errorf("simulator: longitude %v out of WGS84 range [-180, 180]", lon)
	}
	return nil
}

// nextFloat returns a half-open uniform draw in [lo, hi): lo+(hi-lo)*u.
// math/rand/v2 has no Float64N (discovery #692).
func (g *Generator) nextFloat(lo, hi float64) float64 {
	return lo + (hi-lo)*g.r.Float64()
}

// clampLat clamps v into the CLOSED WGS84 latitude range [-90, 90].
func clampLat(v float64) float64 { return math.Max(wgs84LatMin, math.Min(wgs84LatMax, v)) }

// clampLon clamps v into the CLOSED WGS84 longitude range [-180, 180].
func clampLon(v float64) float64 { return math.Max(wgs84LonMin, math.Min(wgs84LonMax, v)) }

// NextReading draws the next SensorReading: uniform metrics, position =
// base ± jitter clamped into WGS84, TimestampNanos = ts.UnixNano() (caller
// owns the clock, R-006), Sequence +1 per reading for the lifetime (R-005).
func (g *Generator) NextReading(ts time.Time) *apiv1.SensorReading {
	// Draw order is pinned to proto field order (design "Order pinned").
	reading := &apiv1.SensorReading{
		Sequence:       g.seq,
		TimestampNanos: ts.UnixNano(), // pure passthrough; caller owns the clock (R-006)
		TemperatureC:   g.nextFloat(tempMinC, tempMaxC),
		VoltageV:       g.nextFloat(voltageMinV, voltageMaxV),
		CurrentA:       g.nextFloat(currentMinA, currentMaxA),
		Position: &apiv1.GpsPosition{
			Latitude:  clampLat(g.nextFloat(g.baseLat-jitterDeg, g.baseLat+jitterDeg)),
			Longitude: clampLon(g.nextFloat(g.baseLon-jitterDeg, g.baseLon+jitterDeg)),
		},
	}
	g.seq++ // single home of the counter: every reading, either method (R-005)
	return reading
}

// Next wraps exactly one reading for deviceID, delegating to NextReading so
// both methods share the single sequence counter (R-004).
func (g *Generator) Next(deviceID string, ts time.Time) *apiv1.TelemetryBatch {
	return &apiv1.TelemetryBatch{
		DeviceId: deviceID,
		Readings: []*apiv1.SensorReading{g.NextReading(ts)},
	}
}

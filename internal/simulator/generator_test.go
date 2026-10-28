package simulator

import (
	"math"
	"reflect"
	"testing"
	"time"

	apiv1 "github.com/anythinglord/quantumshield-hub/api/v1"
)

// assertHalfOpen: lo <= v < hi (metrics, R-002). Never <= on hi (S-002.2).
func assertHalfOpen(t *testing.T, label string, v, lo, hi float64) {
	t.Helper()
	if v < lo || v >= hi {
		t.Errorf("%s = %v, want [%v, %v)", label, v, lo, hi)
	}
}

// assertClosed: lo <= v <= hi (WGS84 GPS, R-003 — clamp may hit the bound).
func assertClosed(t *testing.T, label string, v, lo, hi float64) {
	t.Helper()
	if v < lo || v > hi {
		t.Errorf("%s = %v, want [%v, %v]", label, v, lo, hi)
	}
}

// assertValidPosition: Position populated inside closed WGS84 bounds (R-004).
func assertValidPosition(t *testing.T, label string, pos *apiv1.GpsPosition) {
	t.Helper()
	if pos == nil {
		t.Fatalf("%s: Position nil, want populated WGS84 position", label)
	}
	assertClosed(t, label+" lat", pos.Latitude, wgs84LatMin, wgs84LatMax)
	assertClosed(t, label+" lon", pos.Longitude, wgs84LonMin, wgs84LonMax)
}

// assertSameReading: field-wise equality (D5 — no protobuf runtime import).
func assertSameReading(t *testing.T, label string, got, want *apiv1.SensorReading) {
	t.Helper()
	if got.Sequence != want.Sequence || got.TimestampNanos != want.TimestampNanos {
		t.Errorf("%s (Sequence, TimestampNanos) = (%d, %d), want (%d, %d)", label,
			got.Sequence, got.TimestampNanos, want.Sequence, want.TimestampNanos)
	}
	if got.TemperatureC != want.TemperatureC || got.VoltageV != want.VoltageV || got.CurrentA != want.CurrentA {
		t.Errorf("%s metrics = (%v, %v, %v), want (%v, %v, %v)", label,
			got.TemperatureC, got.VoltageV, got.CurrentA, want.TemperatureC, want.VoltageV, want.CurrentA)
	}
	if (got.Position == nil) != (want.Position == nil) {
		t.Fatalf("%s Position nil = %v, want %v", label, got.Position == nil, want.Position == nil)
	}
	if got.Position != nil && (got.Position.Latitude != want.Position.Latitude || got.Position.Longitude != want.Position.Longitude) {
		t.Errorf("%s Position = (%v, %v), want (%v, %v)", label,
			got.Position.Latitude, got.Position.Longitude, want.Position.Latitude, want.Position.Longitude)
	}
}

// TestNew covers R-001/S-001.1: New never fails; base derives from the seed.
func TestNew(t *testing.T) {
	t.Run("S-001.1 default base within WGS84", func(t *testing.T) {
		for _, seed := range []uint64{0, 1, 7, 42, math.MaxUint64} {
			g := New(seed)
			if g == nil {
				t.Fatalf("New(%d) = nil, want Generator", seed)
			}
			assertClosed(t, "baseLat", g.baseLat, wgs84LatMin, wgs84LatMax)
			assertClosed(t, "baseLon", g.baseLon, wgs84LonMin, wgs84LonMax)
		}
	})
}

// TestNewWithPosition covers R-001: valid explicit base centers the stream
// (S-001.2); out-of-bounds (S-001.3) and non-finite (S-001.4) rejected with
// (nil, error) — finite check precedes bounds (D3, NaN fails comparisons).
func TestNewWithPosition(t *testing.T) {
	t.Run("S-001.2 valid explicit base centers readings", func(t *testing.T) {
		const lat, lon = 40.4, -3.7
		g, err := NewWithPosition(7, lat, lon)
		if err != nil || g == nil {
			t.Fatalf("NewWithPosition(7, %v, %v) = (%v, %v), want non-nil, nil err", lat, lon, g, err)
		}
		for i := 0; i < 1000; i++ {
			p := g.NextReading(time.Unix(1760000000+int64(i), 0)).Position
			if math.Abs(p.Latitude-lat) > jitterDeg || math.Abs(p.Longitude-lon) > jitterDeg {
				t.Fatalf("reading %d = (%v, %v), want base (%v, %v) +/- %v", i, p.Latitude, p.Longitude, lat, lon, jitterDeg)
			}
		}
	})

	for _, tt := range []struct {
		name     string
		lat, lon float64
	}{
		{"S-001.3 latitude 91", 91, 0},
		{"S-001.3 longitude 181", 0, 181},
		{"S-001.3 latitude -90.5", -90.5, 0},
		{"S-001.4 latitude NaN", math.NaN(), 0},
		{"S-001.4 longitude +Inf", 0, math.Inf(1)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g, err := NewWithPosition(7, tt.lat, tt.lon)
			if g != nil || err == nil {
				t.Fatalf("NewWithPosition(7, %v, %v) = (%v, %v), want (nil, error)", tt.lat, tt.lon, g, err)
			}
		})
	}
}

// TestNextReadingValueRanges covers R-002/S-002.1 (+ ROADMAP GPS mandate):
// 1000 fixed-seed readings per row; metrics half-open, GPS closed. Table +
// ID-led t.Run + 1000-loop is the S-011.1 evidence.
func TestNextReadingValueRanges(t *testing.T) {
	g, ts := New(2026), time.Unix(1760000000, 0)
	tests := []struct {
		name   string
		closed bool
		lo, hi float64
		get    func(*apiv1.SensorReading) float64
	}{
		{"S-002.1 temperature [30,95)", false, tempMinC, tempMaxC, func(r *apiv1.SensorReading) float64 { return r.TemperatureC }},
		{"S-002.1 voltage [3.0,4.2)", false, voltageMinV, voltageMaxV, func(r *apiv1.SensorReading) float64 { return r.VoltageV }},
		{"S-002.1 current [0.05,2.0)", false, currentMinA, currentMaxA, func(r *apiv1.SensorReading) float64 { return r.CurrentA }},
		{"S-002.1 latitude [-90,90]", true, wgs84LatMin, wgs84LatMax, func(r *apiv1.SensorReading) float64 { return r.Position.Latitude }},
		{"S-002.1 longitude [-180,180]", true, wgs84LonMin, wgs84LonMax, func(r *apiv1.SensorReading) float64 { return r.Position.Longitude }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 1000; i++ {
				v := tt.get(g.NextReading(ts))
				if tt.closed {
					assertClosed(t, tt.name, v, tt.lo, tt.hi)
				} else {
					assertHalfOpen(t, tt.name, v, tt.lo, tt.hi)
				}
			}
		})
	}
}

// TestNextReadingPosition covers R-003: jitter <= 0.001 deg from base
// (S-003.1); pole/antimeridian bases clamp onto exactly +/-90/180 (S-003.2/3).
func TestNextReadingPosition(t *testing.T) {
	t.Run("S-003.1 jitter magnitude within 0.001 deg", func(t *testing.T) {
		g, err := NewWithPosition(3, 12.34, -56.78)
		if err != nil {
			t.Fatalf("NewWithPosition: %v", err)
		}
		ts := time.Unix(1760000000, 0)
		for i := 0; i < 1000; i++ {
			p := g.NextReading(ts).Position
			if d := math.Abs(p.Latitude - g.baseLat); d > jitterDeg || math.Abs(p.Longitude-g.baseLon) > jitterDeg {
				t.Fatalf("reading %d = (%v, %v) outside base +/- %v", i, p.Latitude, p.Longitude, jitterDeg)
			}
		}
	})

	for _, tt := range []struct { // jitter crossing the bound must clamp ON it
		name   string
		coords [][2]float64 // (lat, lon) bases to probe
		bound  float64
		axis   func(*apiv1.GpsPosition) float64
	}{
		{"S-003.2 pole clamp reaches exactly 90", [][2]float64{{90, 0}, {90 - 1e-9, 0}}, 90, func(p *apiv1.GpsPosition) float64 { return p.Latitude }},
		{"S-003.3 antimeridian clamp reaches exactly 180", [][2]float64{{0, 180}, {0, 180 - 1e-9}}, 180, func(p *apiv1.GpsPosition) float64 { return p.Longitude }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, c := range tt.coords {
				g, err := NewWithPosition(9, c[0], c[1])
				if err != nil {
					t.Fatalf("NewWithPosition(%v, %v): %v", c[0], c[1], err)
				}
				ts, sawClamped := time.Unix(1760000000, 0), false
				for i := 0; i < 1000; i++ {
					v := tt.axis(g.NextReading(ts).Position)
					if v > tt.bound {
						t.Fatalf("base %v value = %v, want <= %v", c, v, tt.bound)
					}
					sawClamped = sawClamped || v == tt.bound
				}
				if !sawClamped {
					t.Errorf("base %v: no reading clamped to exactly %v in 1000 samples", c, tt.bound)
				}
			}
		})
	}
}

// TestNextBatchShape covers R-004: identity with exactly one reading
// (S-004.1); Position populated on both Next and NextReading (S-004.2).
func TestNextBatchShape(t *testing.T) {
	g, ts := New(11), time.Unix(1760000000, 0)
	t.Run("S-004.1 batch carries identity and exactly one reading", func(t *testing.T) {
		batch := g.Next("dev-1", ts)
		if batch == nil {
			t.Fatal("Next(dev-1, ts) = nil, want batch")
		}
		if batch.DeviceId != "dev-1" {
			t.Errorf("batch.DeviceId = %q, want %q", batch.DeviceId, "dev-1")
		}
		if len(batch.Readings) != 1 || batch.Readings[0] == nil {
			t.Fatalf("batch.Readings = %v, want exactly 1 non-nil reading", batch.Readings)
		}
	})
	t.Run("S-004.2 position populated on both paths", func(t *testing.T) {
		assertValidPosition(t, "Next path", g.Next("dev-1", ts).Readings[0].Position)
		assertValidPosition(t, "NextReading path", g.NextReading(ts).Position)
	})
}

// TestSequence covers R-004/R-005: shared counter across methods (S-004.3),
// strict 0..999 over 1000 readings (S-005.1), no mid-stream reset (S-005.2).
func TestSequence(t *testing.T) {
	ts := time.Unix(1760000000, 0)
	t.Run("S-004.3 interleaved methods share the counter", func(t *testing.T) {
		g := New(5)
		for i := uint64(0); i < 100; i++ {
			var r *apiv1.SensorReading
			if i%2 == 0 { // exactly one counter-advancing call per iteration
				r = g.Next("dev-i", ts).Readings[0]
			} else {
				r = g.NextReading(ts)
			}
			if r.Sequence != i {
				t.Fatalf("interleaved reading %d Sequence = %d, want %d", i, r.Sequence, i)
			}
		}
	})
	t.Run("S-005.1 one thousand readings yield exactly 0..999", func(t *testing.T) {
		g := New(6)
		for i := uint64(0); i < 1000; i++ {
			if got := g.NextReading(ts).Sequence; got != i {
				t.Fatalf("reading %d Sequence = %d, want %d", i, got, i)
			}
		}
	})
	t.Run("S-005.2 counter never resets", func(t *testing.T) {
		g := New(8)
		for i := 0; i < 100; i++ {
			g.NextReading(ts)
		}
		if got := g.NextReading(ts).Sequence; got != 100 {
			t.Fatalf("reading after 100 prior = Sequence %d, want 100", got)
		}
	})
}

// TestTimestampPassthrough covers R-006/S-006.1: TimestampNanos ==
// ts.UnixNano() exactly; the package never calls time.Now().
func TestTimestampPassthrough(t *testing.T) {
	g := New(12)
	for _, tt := range []struct {
		name string
		ts   time.Time
	}{
		{"S-006.1 wall time", time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)},
		{"S-006.1 zero time", time.Time{}},
		{"S-006.1 unix epoch", time.Unix(0, 0)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := g.NextReading(tt.ts).TimestampNanos; got != tt.ts.UnixNano() {
				t.Errorf("TimestampNanos = %d, want %d", got, tt.ts.UnixNano())
			}
		})
	}
}

// TestDeterminism covers R-007: same seed replays identical streams
// field-wise (S-007.1, D5); different seeds diverge (S-007.2).
func TestDeterminism(t *testing.T) {
	t.Run("S-007.1 same seed replays identical streams", func(t *testing.T) {
		a, b := New(7), New(7)
		for i := 0; i < 100; i++ {
			ts := time.Unix(1760000000+int64(i), 0)
			assertSameReading(t, "paired reading", b.NextReading(ts), a.NextReading(ts))
		}
	})
	t.Run("S-007.2 different seeds diverge", func(t *testing.T) {
		ra := New(1).NextReading(time.Unix(1760000000, 0))
		rb := New(2).NextReading(time.Unix(1760000000, 0))
		if ra.TemperatureC == rb.TemperatureC && ra.VoltageV == rb.VoltageV &&
			ra.CurrentA == rb.CurrentA && ra.Position.Latitude == rb.Position.Latitude &&
			ra.Position.Longitude == rb.Position.Longitude {
			t.Error("New(1) vs New(2) first readings identical, want >= 1 differing field")
		}
	})
}

// TestReadingFields covers R-008: all fields finite/populated (S-008.1);
// GpsPosition has exactly the two proto fields (S-008.2 — counted via
// protobuf tags because protobuf-go's message state inflates NumField).
func TestReadingFields(t *testing.T) {
	t.Run("S-008.1 all reading fields populated and finite", func(t *testing.T) {
		r := New(13).NextReading(time.Unix(1760000000, 0))
		if r == nil {
			t.Fatal("NextReading = nil, want reading")
		}
		assertValidPosition(t, "reading", r.Position)
		for _, v := range []float64{r.TemperatureC, r.VoltageV, r.CurrentA, r.Position.Latitude, r.Position.Longitude} {
			if math.IsNaN(v) || math.IsInf(v, 0) {
				t.Errorf("reading field = %v, want finite", v)
			}
		}
	})
	t.Run("S-008.2 gps position exposes only latitude and longitude", func(t *testing.T) {
		typ := reflect.TypeOf(apiv1.GpsPosition{})
		var fields []string
		for i := 0; i < typ.NumField(); i++ {
			if typ.Field(i).Tag.Get("protobuf") != "" {
				fields = append(fields, typ.Field(i).Name)
			}
		}
		if len(fields) != 2 || fields[0] != "Latitude" || fields[1] != "Longitude" {
			t.Errorf("GpsPosition proto fields = %v, want [Latitude Longitude] (no altitude)", fields)
		}
	})
}

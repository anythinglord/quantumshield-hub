// Command device-simulator is the concurrent IoT edge-node simulator for
// QuantumShield Hub. The CLI surface (flags, validation, exit codes), the
// synthetic data engine, and the concurrent per-device fleet are wired here;
// the gRPC streaming client lands in Task 2.4 behind the consumer seam.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"

	"github.com/anythinglord/quantumshield-hub/internal/simulator"
)

// Config holds the validated simulator run parameters.
type Config struct {
	Devices    int     // number of simulated edge devices; 1..maxDevices
	Rate       float64 // events/sec PER DEVICE (total ≈ Devices×Rate)
	Address    string  // gRPC target host:port
	PQCEnabled bool    // payload ML-DSA signing toggle; transport is always mTLS
}

// Exit codes (locked by spec R-004).
const (
	exitOK      = 0
	exitRuntime = 1
	exitUsage   = 2

	maxDevices = 10000
)

// simulateFn is the seam run() invokes; tests override it to exercise the
// runtime-failure path (D-4).
var simulateFn = simulate

// parse binds the CLI flags and parses args. Flag output (usage on --help, the
// error + usage on failure) is captured in an internal buffer instead of being
// written straight to stderr: run() must route help to stdout (R-005/S-005.1)
// but errors to stderr, and the flag set cannot un-write to its own output.
// extra carries the positional arguments left after parsing.
func parse(args []string) (cfg Config, extra []string, flagOut []byte, err error) {
	fs := flag.NewFlagSet("device-simulator", flag.ContinueOnError)
	var buf bytes.Buffer
	fs.SetOutput(&buf)

	fs.IntVar(&cfg.Devices, "devices", 1, "number of simulated edge devices (1..10000)")
	fs.Float64Var(&cfg.Rate, "rate", 1.0, "events/sec per device (total ≈ devices × rate)")
	fs.StringVar(&cfg.Address, "address", "localhost:50051", "gRPC target host:port")
	fs.BoolVar(&cfg.PQCEnabled, "pqc-enabled", true, "sign payloads with ML-DSA (transport is always mTLS)")

	if err := fs.Parse(args); err != nil {
		return Config{}, nil, buf.Bytes(), err
	}
	return cfg, fs.Args(), buf.Bytes(), nil
}

// validate enforces the semantic rules of R-003. The finite check MUST precede
// the non-positive check (D-5): NaN fails every comparison, so a bare
// `rate <= 0` test would let NaN slip through.
func validate(cfg Config) error {
	if cfg.Devices < 1 {
		return fmt.Errorf("invalid --devices %d: must be >= 1", cfg.Devices)
	}
	if cfg.Devices > maxDevices {
		return fmt.Errorf("invalid --devices %d: must be <= %d", cfg.Devices, maxDevices)
	}
	if math.IsNaN(cfg.Rate) || math.IsInf(cfg.Rate, 0) {
		return fmt.Errorf("invalid --rate %v: must be a finite number > 0", cfg.Rate)
	}
	if cfg.Rate <= 0 {
		return fmt.Errorf("invalid --rate %v: must be > 0", cfg.Rate)
	}
	if cfg.Address == "" {
		return fmt.Errorf("invalid --address: must not be empty")
	}
	return nil
}

// run is the testable entrypoint seam. It parses, validates, then invokes the
// simulate seam, mapping every outcome to the locked exit codes (R-004).
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	cfg, extra, flagOut, err := parse(args)
	switch {
	case errors.Is(err, flag.ErrHelp):
		// Help is a success path; its usage text belongs on stdout. Buffer
		// writes are non-actionable, so the errors are deliberately discarded.
		_, _ = stdout.Write(flagOut)
		return exitOK
	case err != nil:
		// The flag set already formatted the error + usage into the buffer.
		_, _ = stderr.Write(flagOut)
		return exitUsage
	case len(extra) > 0:
		// Parse stops at the first positional argument and leaves the buffer
		// empty, so run supplies the diagnostic itself.
		fmt.Fprintf(stderr, "device-simulator: unexpected positional arguments: %v\n", extra)
		return exitUsage
	}

	if err := validate(cfg); err != nil {
		fmt.Fprintf(stderr, "device-simulator: %v\n", err)
		return exitUsage
	}
	if err := simulateFn(ctx, cfg); err != nil {
		fmt.Fprintf(stderr, "device-simulator: %v\n", err)
		return exitRuntime
	}
	return exitOK
}

// simulate runs the concurrent per-device fleet until ctx is cancelled. In
// Task 2.3 the sink is an unobserved counting consumer (the fan-in drain
// seam); Task 2.4 swaps in the gRPC streaming consumer without touching this
// wiring. cfg.Address and cfg.PQCEnabled are carried for that consumer.
func simulate(ctx context.Context, cfg Config) error {
	log.Printf("device-simulator: starting %d device(s) at %g events/sec/device → %s (pqc_signing=%t)",
		cfg.Devices, cfg.Rate, cfg.Address, cfg.PQCEnabled)
	counter := simulator.NewCountingConsumer()
	fleet := simulator.NewFleet(cfg.Devices, cfg.Rate, counter.Consume)
	if err := fleet.Run(ctx); err != nil {
		return fmt.Errorf("simulate: %w", err)
	}
	log.Printf("device-simulator: fleet stopped after %d batch(es)", counter.Received())
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop() // explicit: os.Exit below skips deferred calls
	os.Exit(code)
}

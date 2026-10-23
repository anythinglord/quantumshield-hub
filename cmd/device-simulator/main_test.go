package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"math"
	"os"
	"strings"
	"testing"
)

// TestMain silences the standard logger: the simulate stub logs the effective
// config, which would otherwise pollute test output. Mirrors the setup pattern
// in internal/crypto/pki_integration_test.go.
func TestMain(m *testing.M) {
	log.SetOutput(io.Discard)
	os.Exit(m.Run())
}

// assertConfig compares the parsed Config against want. NaN/±Inf rates are
// checked separately because they never compare equal to a literal.
func assertConfig(t *testing.T, args []string, got, want Config, nan bool, inf int) {
	t.Helper()
	switch {
	case nan:
		if !math.IsNaN(got.Rate) {
			t.Errorf("parse(%v) Rate = %v, want NaN", args, got.Rate)
		}
	case inf != 0:
		if !math.IsInf(got.Rate, inf) {
			t.Errorf("parse(%v) Rate = %v, want Inf sign %d", args, got.Rate, inf)
		}
	default:
		if got != want {
			t.Errorf("parse(%v) cfg = %+v, want %+v", args, got, want)
		}
	}
}

// assertOutput checks stdout/stderr expectations for a run case. A want value is
// "empty" (must be empty), "text" (must be non-empty), a substring that must
// appear, or "" to skip the check (R-005 stream routing).
func assertOutput(t *testing.T, label, stdout, stderr, wantOut, wantErr string) {
	t.Helper()
	checkStream(t, label, "stdout", stdout, wantOut)
	checkStream(t, label, "stderr", stderr, wantErr)
}

func checkStream(t *testing.T, label, stream, got, want string) {
	t.Helper()
	switch {
	case want == "empty":
		if got != "" {
			t.Errorf("%s: %s = %q, want empty", label, stream, got)
		}
	case want == "text":
		if got == "" {
			t.Errorf("%s: %s empty, want non-empty output", label, stream)
		}
	case want != "":
		if !strings.Contains(got, want) {
			t.Errorf("%s: %s = %q, want substring %q", label, stream, got, want)
		}
	}
}

// TestParse covers R-001 (flag contract/defaults), R-007 (defaults/overrides)
// and the parse-level facets of R-003/R-004: NaN/±Inf are accepted by the flag
// parser (semantic rejection is validate's job), unknown flags and non-numeric
// values error out, and --help surfaces as flag.ErrHelp.
func TestParse(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		want        Config
		extra       []string
		errKind     string // "", "any", "errhelp"
		nan         bool
		inf         int // 0 none, +1 +Inf, -1 -Inf
		wantFlagOut bool
	}{
		{name: "defaults", want: Config{1, 1.0, "localhost:50051", true, "certs/client/client-chain.crt", "certs/client/client.key", "certs/ca/root-ca.crt", ""}},                                                              // S-001.1/S-007.1
		{name: "mixed long/short forms", args: []string{"--devices=7", "-rate", "2.5"}, want: Config{7, 2.5, "localhost:50051", true, "certs/client/client-chain.crt", "certs/client/client.key", "certs/ca/root-ca.crt", ""}}, // S-001.2
		{name: "bare bool consumed", args: []string{"--devices=3", "--pqc-enabled"}, want: Config{3, 1.0, "localhost:50051", true, "certs/client/client-chain.crt", "certs/client/client.key", "certs/ca/root-ca.crt", ""}},    // S-001.3
		{name: "bool false", args: []string{"--pqc-enabled=false"}, want: Config{1, 1.0, "localhost:50051", false, "certs/client/client-chain.crt", "certs/client/client.key", "certs/ca/root-ca.crt", ""}},                    // S-001.4
		{name: "NaN parses at flag level", args: []string{"--rate=NaN"}, nan: true}, // S-003.6
		{name: "Inf parses at flag level", args: []string{"--rate=+Inf"}, inf: 1},   // S-003.7
		{name: "unknown flag", args: []string{"--unknown-flag=x"}, errKind: "any"},  // S-003.10
		{name: "non-numeric rate", args: []string{"--rate=abc"}, errKind: "any"},    // S-004.2
		{name: "extra positional args", args: []string{"extra1", "extra2"}, want: Config{1, 1.0, "localhost:50051", true, "certs/client/client-chain.crt", "certs/client/client.key", "certs/ca/root-ca.crt", ""}, extra: []string{"extra1", "extra2"}}, // S-003.11
		{name: "help returns ErrHelp", args: []string{"--help"}, errKind: "errhelp", wantFlagOut: true}, // S-004.1
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, extra, flagOut, err := parse(tt.args)

			switch tt.errKind {
			case "errhelp":
				if !errors.Is(err, flag.ErrHelp) {
					t.Fatalf("parse(%v) error = %v, want flag.ErrHelp", tt.args, err)
				}
			case "any":
				if err == nil || errors.Is(err, flag.ErrHelp) {
					t.Fatalf("parse(%v) error = %v, want a non-help parse error", tt.args, err)
				}
			default:
				if err != nil {
					t.Fatalf("parse(%v) error = %v, want nil", tt.args, err)
				}
				assertConfig(t, tt.args, cfg, tt.want, tt.nan, tt.inf)
			}

			if len(extra) != len(tt.extra) {
				t.Fatalf("parse(%v) extra = %v, want %v", tt.args, extra, tt.extra)
			}
			for i, want := range tt.extra {
				if extra[i] != want {
					t.Errorf("parse(%v) extra[%d] = %q, want %q", tt.args, i, extra[i], want)
				}
			}
			if tt.wantFlagOut && len(flagOut) == 0 {
				t.Errorf("parse(%v) flagOut = empty, want captured help/usage text", tt.args)
			}
		})
	}
}

// TestValidate covers R-003: every boundary and the mandated diagnostic
// substrings. The NaN/±Inf cases pin decision D-5 (finite-check precedes the
// non-positive check).
func TestValidate(t *testing.T) {
	const addr = "localhost:50051"
	tests := []struct {
		name    string
		cfg     Config
		wantErr string // substring; empty means want nil
	}{
		{"devices zero", Config{0, 1, addr, false, "c", "k", "ca", ""}, ">= 1"},                                                                        // S-003.1
		{"devices over cap", Config{10001, 1, addr, false, "c", "k", "ca", ""}, "<= 10000"},                                                            // S-003.2
		{"devices cap boundary ok", Config{10000, 1, addr, false, "c", "k", "ca", ""}, ""},                                                             // S-003.3
		{"rate zero", Config{1, 0, addr, false, "c", "k", "ca", ""}, "> 0"},                                                                            // S-003.4
		{"rate negative", Config{1, -1, addr, false, "c", "k", "ca", ""}, "> 0"},                                                                       // S-003.5
		{"rate NaN", Config{1, math.NaN(), addr, false, "c", "k", "ca", ""}, "finite number > 0"},                                                      // S-003.6
		{"rate plus Inf", Config{1, math.Inf(1), addr, false, "c", "k", "ca", ""}, "finite number > 0"},                                                // S-003.7
		{"rate minus Inf", Config{1, math.Inf(-1), addr, false, "c", "k", "ca", ""}, "finite number > 0"},                                              // S-003.7
		{"rate tiny positive ok", Config{1, 0.0001, addr, false, "c", "k", "ca", ""}, ""},                                                              // S-003.8
		{"address empty", Config{1, 1, "", false, "c", "k", "ca", ""}, "must not be empty"},                                                            // S-003.9
		{"default config valid", Config{1, 1, addr, true, "certs/client/client-chain.crt", "certs/client/client.key", "certs/ca/root-ca.crt", ""}, ""}, // S-007.1
		{"tls-cert empty", Config{1, 1, addr, true, "", "certs/client/client.key", "certs/ca/root-ca.crt", ""}, "invalid --tls-cert: must not be empty"},
		{"tls-key empty", Config{1, 1, addr, true, "certs/client/client-chain.crt", "", "certs/ca/root-ca.crt", ""}, "invalid --tls-key: must not be empty"},
		{"tls-ca empty", Config{1, 1, addr, true, "certs/client/client-chain.crt", "certs/client/client.key", "", ""}, "invalid --tls-ca: must not be empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate(tt.cfg)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validate(%+v) = %v, want nil", tt.cfg, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validate(%+v) = nil, want error containing %q", tt.cfg, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("validate(%+v) = %q, want substring %q", tt.cfg, err.Error(), tt.wantErr)
			}
		})
	}
}

// TestRun covers the end-to-end seam R-006 plus R-004 (exit codes) and R-005
// (stream routing): help→stdout/0, parse+validation+extra-args errors→stderr/2,
// runtime seam failure→1, success→0. Contexts are pre-cancelled (no sleeps, no
// signals); simulateFn is overridden only where the 2.1 stub cannot reach a path
// (D-4), restoring it via t.Cleanup. Deliberately not parallel.
func TestRun(t *testing.T) {
	var overridesCfg, perDeviceCfg, maxMinCfg Config // capture targets for overrideSim cases

	tests := []struct {
		name        string
		args        []string
		overrideSim bool
		simErr      error
		captured    *Config
		wantCode    int
		out, errOut string
		wantCfg     *Config
	}{
		{name: "help exits zero usage on stdout", args: []string{"--help"}, wantCode: exitOK, out: "text", errOut: "empty"},                                 // S-004.1/S-005.1
		{name: "unknown flag exits two stderr", args: []string{"--unknown-flag=x"}, wantCode: exitUsage, out: "empty", errOut: "text"},                      // S-003.10/S-005.2
		{name: "devices zero exits two stdout empty", args: []string{"--devices=0"}, wantCode: exitUsage, out: "empty", errOut: ">= 1"},                     // S-003.1/S-005.3
		{name: "extra args exits two", args: []string{"extra1", "extra2"}, wantCode: exitUsage, errOut: "unexpected positional arguments"},                  // S-003.11/S-005.4
		{name: "trailing args ignored regression", args: []string{"extra1", "--devices=2"}, wantCode: exitUsage, errOut: "unexpected positional arguments"}, // S-003.11 first-positional
		{name: "rate abc exits two", args: []string{"--rate=abc"}, wantCode: exitUsage, out: "empty", errOut: "text"},                                       // S-004.2
		{name: "defaults happy path", overrideSim: true, wantCode: exitOK, out: "empty", errOut: "empty"},                                                   // S-006.1/S-004.5
		{name: "simulate receives overrides", args: []string{"--devices=4", "--rate=2.5", "--address=10.0.0.5:50051", "--pqc-enabled=false"}, overrideSim: true, captured: &overridesCfg, wantCode: exitOK, wantCfg: &Config{4, 2.5, "10.0.0.5:50051", false, "certs/client/client-chain.crt", "certs/client/client.key", "certs/ca/root-ca.crt", ""}}, // S-007.2
		{name: "rate per-device preserved", args: []string{"--devices=2", "--rate=5.0"}, overrideSim: true, captured: &perDeviceCfg, wantCode: exitOK, wantCfg: &Config{2, 5.0, "localhost:50051", true, "certs/client/client-chain.crt", "certs/client/client.key", "certs/ca/root-ca.crt", ""}},                                                      // S-002.1
		{name: "devices max rate min", args: []string{"--devices=10000", "--rate=0.0001"}, overrideSim: true, captured: &maxMinCfg, wantCode: exitOK, wantCfg: &Config{10000, 0.0001, "localhost:50051", true, "certs/client/client-chain.crt", "certs/client/client.key", "certs/ca/root-ca.crt", ""}},                                                // S-002.2/S-003.3/S-003.8
		{name: "tls flags parsed", args: []string{"--tls-cert=/tmp/c.crt", "--tls-key=/tmp/c.key", "--tls-ca=/tmp/ca.crt", "--tls-server-name=example.com"}, overrideSim: true, captured: &overridesCfg, wantCode: exitOK, wantCfg: &Config{1, 1.0, "localhost:50051", true, "/tmp/c.crt", "/tmp/c.key", "/tmp/ca.crt", "example.com"}},
		{name: "tls flags parsed space form", args: []string{"--tls-cert", "/tmp/c2.crt", "--tls-server-name", "server.local"}, overrideSim: true, captured: &overridesCfg, wantCode: exitOK, wantCfg: &Config{1, 1.0, "localhost:50051", true, "/tmp/c2.crt", "certs/client/client.key", "certs/ca/root-ca.crt", "server.local"}},
		{name: "simulate error exits one", overrideSim: true, simErr: errors.New("simulate boom"), wantCode: exitRuntime, errOut: "simulate boom"}, // S-004.4/S-009.1
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.overrideSim {
				dest, simErr := tt.captured, tt.simErr
				simulateFn = func(_ context.Context, cfg Config) error {
					if dest != nil {
						*dest = cfg
					}
					return simErr
				}
				t.Cleanup(func() { simulateFn = simulate })
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			var stdout, stderr bytes.Buffer
			code := run(ctx, tt.args, &stdout, &stderr)
			if code != tt.wantCode {
				t.Fatalf("run(%v) = %d, want %d (stderr=%q)", tt.args, code, tt.wantCode, stderr.String())
			}
			assertOutput(t, tt.name, stdout.String(), stderr.String(), tt.out, tt.errOut)
			if tt.captured != nil && tt.wantCfg != nil && *tt.captured != *tt.wantCfg {
				t.Errorf("simulate cfg = %+v, want %+v", *tt.captured, *tt.wantCfg)
			}
		})
	}
}

func TestS003_TLSFlagDefaultsLiteral(t *testing.T) {
	cfg, extra, flagOut, err := parse([]string{})
	if err != nil {
		t.Fatalf("parse() error = %v", err)
	}
	if len(extra) != 0 {
		t.Fatalf("extra = %v, want empty", extra)
	}
	_ = flagOut
	want := Config{Devices: 1, Rate: 1.0, Address: "localhost:50051", PQCEnabled: true,
		TLSCert: "certs/client/client-chain.crt", TLSKey: "certs/client/client.key",
		TLSCA: "certs/ca/root-ca.crt", TLSServerName: ""}
	if cfg != want {
		t.Fatalf("parse defaults = %+v, want %+v", cfg, want)
	}
}

func TestS003_NoOsGetenvInParseValidate(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	if strings.Contains(s, "os.Getenv") || strings.Contains(s, "os.LookupEnv") {
		t.Fatalf("parse/validate must not use env vars")
	}
}

func TestS003_ValidateRejectsEmptyTLSPaths(t *testing.T) {
	addr := "localhost:50051"
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{"empty cert", Config{1, 1, addr, true, "", "k", "ca", ""}, "invalid --tls-cert: must not be empty"},
		{"empty key", Config{1, 1, addr, true, "c", "", "ca", ""}, "invalid --tls-key: must not be empty"},
		{"empty ca", Config{1, 1, addr, true, "c", "k", "", ""}, "invalid --tls-ca: must not be empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate(tt.cfg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validate(%+v) = %v, want %q", tt.cfg, err, tt.wantErr)
			}
		})
	}
}

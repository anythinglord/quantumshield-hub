package db

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// envPoolLookup turns a fixed map into the lookup function parsePoolConfig
// accepts, so the POOL_* rules can be exercised without process globals.
func envPoolLookup(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

// clearPoolEnv blanks every pool environment variable for the duration of the
// test. t.Setenv restores the previous values on cleanup.
func clearPoolEnv(t *testing.T) {
	t.Helper()

	for _, key := range []string{
		envDatabaseURL,
		envPoolMaxConns,
		envPoolMinConns,
		envPoolMaxConnLifetime,
		envPoolMaxConnIdleTime,
	} {
		t.Setenv(key, "")
	}
}

func TestParsePoolConfig(t *testing.T) {
	// sslmode=disable keeps ParseConfig from depending on the host's TLS env.
	const dsn = "postgres://qs:pw@localhost:5432/qs?sslmode=disable"

	t.Run("S-008 only DATABASE_URL keeps the library defaults", func(t *testing.T) {
		want, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatalf("control pgxpool.ParseConfig: %v", err)
		}

		got, err := parsePoolConfig(dsn, envPoolLookup(nil))
		if err != nil {
			t.Fatalf("parsePoolConfig: %v", err)
		}

		if got.MaxConns != want.MaxConns {
			t.Errorf("MaxConns = %d, want library default %d", got.MaxConns, want.MaxConns)
		}
		if got.MinConns != want.MinConns {
			t.Errorf("MinConns = %d, want library default %d", got.MinConns, want.MinConns)
		}
		if got.MaxConnLifetime != want.MaxConnLifetime {
			t.Errorf("MaxConnLifetime = %v, want library default %v", got.MaxConnLifetime, want.MaxConnLifetime)
		}
		if got.MaxConnIdleTime != want.MaxConnIdleTime {
			t.Errorf("MaxConnIdleTime = %v, want library default %v", got.MaxConnIdleTime, want.MaxConnIdleTime)
		}
	})

	t.Run("S-009 every POOL_* override reaches the parsed config", func(t *testing.T) {
		got, err := parsePoolConfig(dsn, envPoolLookup(map[string]string{
			envPoolMaxConns:        "8",
			envPoolMinConns:        "3",
			envPoolMaxConnLifetime: "45m",
			envPoolMaxConnIdleTime: "5m",
		}))
		if err != nil {
			t.Fatalf("parsePoolConfig: %v", err)
		}

		if got.MaxConns != 8 {
			t.Errorf("MaxConns = %d, want 8", got.MaxConns)
		}
		if got.MinConns != 3 {
			t.Errorf("MinConns = %d, want 3", got.MinConns)
		}
		if got.MaxConnLifetime != 45*time.Minute {
			t.Errorf("MaxConnLifetime = %v, want 45m", got.MaxConnLifetime)
		}
		if got.MaxConnIdleTime != 5*time.Minute {
			t.Errorf("MaxConnIdleTime = %v, want 5m", got.MaxConnIdleTime)
		}
	})

	t.Run("S-010 malformed DSN fails with a wrapped error", func(t *testing.T) {
		// The malformed percent escape makes url.Parse fail before any I/O.
		_, err := parsePoolConfig("postgres://%zz", envPoolLookup(nil))
		if err == nil {
			t.Fatal("parsePoolConfig accepted a malformed DSN")
		}
		if !strings.Contains(err.Error(), envDatabaseURL) {
			t.Errorf("error %q does not name %s", err, envDatabaseURL)
		}
	})

	t.Run("S-010 missing DSN fails before parsing", func(t *testing.T) {
		_, err := parsePoolConfig("", envPoolLookup(nil))
		if err == nil {
			t.Fatal("parsePoolConfig accepted an empty DSN")
		}
		if !strings.Contains(err.Error(), envDatabaseURL) {
			t.Errorf("error %q does not name %s", err, envDatabaseURL)
		}
	})
}

func TestParsePoolConfigRejectsInvalidOverrides(t *testing.T) {
	const dsn = "postgres://qs:pw@localhost:5432/qs?sslmode=disable"

	tests := []struct {
		name      string
		key       string
		value     string
		wantInMsg string
	}{
		{
			name:      "max conns is not a number",
			key:       envPoolMaxConns,
			value:     "many",
			wantInMsg: envPoolMaxConns,
		},
		{
			name:      "max conns is zero",
			key:       envPoolMaxConns,
			value:     "0",
			wantInMsg: envPoolMaxConns,
		},
		{
			name:      "min conns is negative",
			key:       envPoolMinConns,
			value:     "-1",
			wantInMsg: envPoolMinConns,
		},
		{
			name:      "lifetime is not a duration",
			key:       envPoolMaxConnLifetime,
			value:     "soon",
			wantInMsg: envPoolMaxConnLifetime,
		},
		{
			name:      "idle time is negative",
			key:       envPoolMaxConnIdleTime,
			value:     "-5m",
			wantInMsg: envPoolMaxConnIdleTime,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parsePoolConfig(dsn, envPoolLookup(map[string]string{tt.key: tt.value}))
			if err == nil {
				t.Fatalf("parsePoolConfig accepted %s=%q", tt.key, tt.value)
			}
			if !strings.Contains(err.Error(), tt.wantInMsg) {
				t.Errorf("error %q does not name %s", err, tt.wantInMsg)
			}
		})
	}
}

func TestNewPoolRejectsBadEnv(t *testing.T) {
	t.Run("S-010 malformed DATABASE_URL aborts boot", func(t *testing.T) {
		clearPoolEnv(t)
		t.Setenv(envDatabaseURL, "postgres://%zz")

		pool, err := NewPool(context.Background())
		if pool != nil {
			pool.Close()
			t.Fatal("NewPool returned a pool for a malformed DATABASE_URL")
		}
		if err == nil {
			t.Fatal("NewPool accepted a malformed DATABASE_URL")
		}
		if !strings.Contains(err.Error(), envDatabaseURL) {
			t.Errorf("error %q does not name %s", err, envDatabaseURL)
		}
	})

	t.Run("S-010 missing DATABASE_URL aborts boot", func(t *testing.T) {
		clearPoolEnv(t)

		pool, err := NewPool(context.Background())
		if pool != nil {
			pool.Close()
			t.Fatal("NewPool returned a pool without DATABASE_URL")
		}
		if err == nil {
			t.Fatal("NewPool succeeded without DATABASE_URL")
		}
		if !strings.Contains(err.Error(), envDatabaseURL) {
			t.Errorf("error %q does not name %s", err, envDatabaseURL)
		}
	})
}

// TestNewPoolPingFailsFast needs no Postgres: 127.0.0.1:1 is the classic
// "nothing listens here" address, so Ping fails without waiting. It is skipped
// under -short because it still opens a TCP connection attempt (A10).
func TestNewPoolPingFailsFast(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping unreachable-database ping test in short mode")
	}

	clearPoolEnv(t)
	t.Setenv(envDatabaseURL, "postgres://qs:pw@127.0.0.1:1/qs?sslmode=disable&connect_timeout=1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := NewPool(ctx)
	if pool != nil {
		pool.Close()
		t.Fatal("NewPool returned a pool for an unreachable database")
	}
	if err == nil {
		t.Fatal("NewPool succeeded against an unreachable database")
	}
	if !strings.Contains(err.Error(), "ping") {
		t.Errorf("error %q does not mention the failed ping", err)
	}
}

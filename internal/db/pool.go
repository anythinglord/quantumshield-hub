package db

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool configuration environment variables (design A3). DATABASE_URL carries
// the connection string; the POOL_* variables override pgxpool defaults. No
// DSN or credential is ever hardcoded (AGENTS §3).
const (
	envDatabaseURL         = "DATABASE_URL"
	envPoolMaxConns        = "POOL_MAX_CONNS"
	envPoolMinConns        = "POOL_MIN_CONNS"
	envPoolMaxConnLifetime = "POOL_MAX_CONN_LIFETIME"
	envPoolMaxConnIdleTime = "POOL_MAX_CONN_IDLE_TIME"
)

// NewPool parses DATABASE_URL, applies the POOL_* overrides, opens the pool,
// and pings it once. A bad configuration or an unreachable database therefore
// fails at boot instead of on the first request (design A3). On a failed ping
// the partially opened pool is closed before returning so no connections leak.
func NewPool(ctx context.Context) (*pgxpool.Pool, error) {
	cfg, err := parsePoolConfig(os.Getenv(envDatabaseURL), os.Getenv)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("db: pool: open: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("db: pool: ping: %w", err)
	}

	return pool, nil
}

// parsePoolConfig parses dsn with pgxpool's defaults and applies every POOL_*
// override returned by lookup. lookup is injected so the override rules are
// unit-testable without reading or mutating process globals.
func parsePoolConfig(dsn string, lookup func(string) string) (*pgxpool.Config, error) {
	if dsn == "" {
		return nil, fmt.Errorf("db: pool: %s is not set", envDatabaseURL)
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("db: pool: parse %s: %w", envDatabaseURL, err)
	}

	if value := lookup(envPoolMaxConns); value != "" {
		n, err := parseConnCount(envPoolMaxConns, value, 1)
		if err != nil {
			return nil, err
		}
		cfg.MaxConns = n
	}
	if value := lookup(envPoolMinConns); value != "" {
		n, err := parseConnCount(envPoolMinConns, value, 0)
		if err != nil {
			return nil, err
		}
		cfg.MinConns = n
	}
	if value := lookup(envPoolMaxConnLifetime); value != "" {
		d, err := parseDurationOverride(envPoolMaxConnLifetime, value)
		if err != nil {
			return nil, err
		}
		cfg.MaxConnLifetime = d
	}
	if value := lookup(envPoolMaxConnIdleTime); value != "" {
		d, err := parseDurationOverride(envPoolMaxConnIdleTime, value)
		if err != nil {
			return nil, err
		}
		cfg.MaxConnIdleTime = d
	}

	return cfg, nil
}

// parseConnCount parses a pooled-connection count and enforces a lower bound.
func parseConnCount(name, value string, minimum int64) (int32, error) {
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("db: pool: parse %s=%q: %w", name, value, err)
	}
	if n < minimum {
		return 0, fmt.Errorf("db: pool: %s=%q: must be at least %d", name, value, minimum)
	}
	return int32(n), nil
}

// parseDurationOverride parses a Go duration and rejects negative values.
func parseDurationOverride(name, value string) (time.Duration, error) {
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("db: pool: parse %s=%q: %w", name, value, err)
	}
	if d < 0 {
		return 0, fmt.Errorf("db: pool: %s=%q: must not be negative", name, value)
	}
	return d, nil
}

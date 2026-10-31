package db

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const dbGateEnv = "TIMESCALE_TEST_DSN"

// dbGateReason reports why the DB-backed tests must be skipped, or "" when
// they may run. It is the single source of truth for the A10 gate so the
// gating rules themselves are unit-testable without a Postgres instance.
func dbGateReason(short bool, dsn string) string {
	switch {
	case short:
		return "short mode (-short)"
	case dsn == "":
		return dbGateEnv + " not set"
	default:
		return ""
	}
}

// requireDB returns a pool for the dedicated test database or skips the test.
func requireDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv(dbGateEnv)
	if reason := dbGateReason(testing.Short(), dsn); reason != "" {
		t.Skipf("skipping DB test: %s", reason)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open test pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping test database: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

// resetSchema drops everything the migration runner owns so each scenario
// starts from the same empty baseline. The test DSN is expected to point at a
// disposable database (see TIMESCALE_TEST_DSN in the README).
func resetSchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	ctx := context.Background()
	statements := []string{
		"DROP TABLE IF EXISTS device_telemetry CASCADE",
		"DROP TABLE IF EXISTS devices CASCADE",
		"DROP TABLE IF EXISTS schema_migrations CASCADE",
	}
	for _, stmt := range statements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("reset schema (%s): %v", stmt, err)
		}
	}
}

func TestDBGate(t *testing.T) {
	tests := []struct {
		name       string
		short      bool
		dsn        string
		wantReason string
	}{
		{
			name:       "S-016 empty DSN skips DB tests",
			short:      false,
			dsn:        "",
			wantReason: "TIMESCALE_TEST_DSN not set",
		},
		{
			name:       "S-017 short mode skips DB tests",
			short:      true,
			dsn:        "postgres://example/db",
			wantReason: "short mode (-short)",
		},
		{
			name:       "S-018 DSN in full mode opens the gate",
			short:      false,
			dsn:        "postgres://example/db",
			wantReason: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := dbGateReason(tt.short, tt.dsn); got != tt.wantReason {
				t.Fatalf("dbGateReason(%v, %q) = %q, want %q", tt.short, tt.dsn, got, tt.wantReason)
			}
		})
	}
}

func TestLoadMigrations(t *testing.T) {
	t.Run("orders migrations by numeric version", func(t *testing.T) {
		fsys := fstest.MapFS{
			"migrations/003_third.sql": &fstest.MapFile{Data: []byte("SELECT 3;")},
			"migrations/001_init.sql":  &fstest.MapFile{Data: []byte("SELECT 1;")},
			"migrations/002_more.sql":  &fstest.MapFile{Data: []byte("SELECT 2;")},
		}

		got, err := loadMigrations(fsys)
		if err != nil {
			t.Fatalf("loadMigrations: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("loadMigrations returned %d migrations, want 3", len(got))
		}

		wantVersions := []int64{1, 2, 3}
		wantNames := []string{"001_init.sql", "002_more.sql", "003_third.sql"}
		for i := range got {
			if got[i].version != wantVersions[i] {
				t.Errorf("migration %d version = %d, want %d", i, got[i].version, wantVersions[i])
			}
			if got[i].name != wantNames[i] {
				t.Errorf("migration %d name = %q, want %q", i, got[i].name, wantNames[i])
			}
			if got[i].sql == "" {
				t.Errorf("migration %d (%s) has empty SQL body", i, got[i].name)
			}
		}
	})

	t.Run("rejects a filename without an NNN prefix", func(t *testing.T) {
		fsys := fstest.MapFS{
			"migrations/init.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
		}

		_, err := loadMigrations(fsys)
		if err == nil {
			t.Fatal("loadMigrations accepted a filename without an NNN prefix")
		}
		if !strings.Contains(err.Error(), "init.sql") {
			t.Errorf("error %q does not name the offending file", err)
		}
	})

	t.Run("rejects duplicate versions", func(t *testing.T) {
		fsys := fstest.MapFS{
			"migrations/001_first.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
			"migrations/001_again.sql": &fstest.MapFile{Data: []byte("SELECT 2;")},
		}

		_, err := loadMigrations(fsys)
		if err == nil {
			t.Fatal("loadMigrations accepted duplicate version 1")
		}
		if !strings.Contains(err.Error(), "duplicate") {
			t.Errorf("error %q does not report a duplicate version", err)
		}
	})

	t.Run("checksums the exact file bytes", func(t *testing.T) {
		fsys := fstest.MapFS{
			// SHA-256("abc") — a well-known digest, so the assertion proves the
			// checksum is derived from the raw bytes rather than a copy.
			"migrations/001_abc.sql": &fstest.MapFile{Data: []byte("abc")},
		}

		got, err := loadMigrations(fsys)
		if err != nil {
			t.Fatalf("loadMigrations: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("loadMigrations returned %d migrations, want 1", len(got))
		}

		const want = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
		if got[0].checksum != want {
			t.Errorf("checksum = %q, want %q", got[0].checksum, want)
		}
	})

	t.Run("loads the embedded migration files", func(t *testing.T) {
		got, err := loadMigrations(migrationFS)
		if err != nil {
			t.Fatalf("loadMigrations(embedded): %v", err)
		}
		if len(got) == 0 {
			t.Fatal("no migrations embedded in the binary")
		}
		if got[0].version != 1 || got[0].name != "001_init.sql" {
			t.Errorf("first embedded migration = (%d, %q), want (1, %q)", got[0].version, got[0].name, "001_init.sql")
		}
	})
}

func TestUp(t *testing.T) {
	pool := requireDB(t)

	t.Run("S-001 fresh apply records every migration in order", func(t *testing.T) {
		resetSchema(t, pool)

		embedded, err := loadMigrations(migrationFS)
		if err != nil {
			t.Fatalf("loadMigrations: %v", err)
		}
		if err := Up(context.Background(), pool); err != nil {
			t.Fatalf("Up: %v", err)
		}

		applied := appliedMigrations(t, pool)
		if len(applied) != len(embedded) {
			t.Fatalf("schema_migrations has %d rows, want %d", len(applied), len(embedded))
		}
		for _, m := range embedded {
			got, ok := applied[m.version]
			if !ok {
				t.Errorf("version %d (%s) not recorded", m.version, m.name)
				continue
			}
			if got != m.checksum {
				t.Errorf("version %d checksum = %q, want %q", m.version, got, m.checksum)
			}
		}

		if !tableExists(t, pool, "device_telemetry") {
			t.Error("device_telemetry was not created by the fresh apply")
		}
	})

	t.Run("S-002 re-run is idempotent", func(t *testing.T) {
		resetSchema(t, pool)

		if err := Up(context.Background(), pool); err != nil {
			t.Fatalf("first Up: %v", err)
		}
		before := appliedAt(t, pool)

		if err := Up(context.Background(), pool); err != nil {
			t.Fatalf("second Up: %v", err)
		}
		after := appliedAt(t, pool)

		if len(after) != len(before) {
			t.Fatalf("schema_migrations grew from %d to %d rows on re-run", len(before), len(after))
		}
		for version, first := range before {
			if second, ok := after[version]; !ok || !second.Equal(first) {
				t.Errorf("version %d was re-applied (applied_at %v -> %v)", version, first, second)
			}
		}
	})

	t.Run("S-003 checksum drift aborts without overwriting", func(t *testing.T) {
		resetSchema(t, pool)

		if err := Up(context.Background(), pool); err != nil {
			t.Fatalf("first Up: %v", err)
		}
		if _, err := pool.Exec(context.Background(),
			"UPDATE schema_migrations SET checksum = 'deadbeef' WHERE version = 1"); err != nil {
			t.Fatalf("tamper checksum: %v", err)
		}

		err := Up(context.Background(), pool)
		if err == nil {
			t.Fatal("Up accepted a divergent checksum")
		}
		if !errors.Is(err, errChecksumMismatch) {
			t.Errorf("Up error %v does not wrap errChecksumMismatch", err)
		}
		if !strings.Contains(err.Error(), "001_init.sql") {
			t.Errorf("Up error %q does not name the drifted migration", err)
		}

		if got := appliedMigrations(t, pool)[1]; got != "deadbeef" {
			t.Errorf("recorded checksum = %q, want the tampered value to be preserved", got)
		}
		if !tableExists(t, pool, "device_telemetry") {
			t.Error("checksum drift dropped or rewrote the applied schema")
		}
	})

	t.Run("S-004 concurrent runners serialize on the advisory lock", func(t *testing.T) {
		resetSchema(t, pool)

		dsn := os.Getenv(dbGateEnv)
		second, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			t.Fatalf("open second pool: %v", err)
		}
		t.Cleanup(second.Close)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		start := make(chan struct{})
		errs := make(chan error, 2)
		var wg sync.WaitGroup
		for _, p := range []*pgxpool.Pool{pool, second} {
			wg.Add(1)
			go func(p *pgxpool.Pool) {
				defer wg.Done()
				<-start
				errs <- Up(ctx, p)
			}(p)
		}
		close(start)
		wg.Wait()
		close(errs)

		for err := range errs {
			if err != nil {
				t.Errorf("concurrent Up: %v", err)
			}
		}

		embedded, err := loadMigrations(migrationFS)
		if err != nil {
			t.Fatalf("loadMigrations: %v", err)
		}
		if got := len(appliedMigrations(t, pool)); got != len(embedded) {
			t.Errorf("schema_migrations has %d rows after concurrent apply, want %d", got, len(embedded))
		}
	})

	t.Run("S-005 schema objects and hypertable exist", func(t *testing.T) {
		resetSchema(t, pool)

		if err := Up(context.Background(), pool); err != nil {
			t.Fatalf("Up: %v", err)
		}

		if got := countQuery(t, pool, "SELECT count(*) FROM pg_extension WHERE extname = 'timescaledb'"); got != 1 {
			t.Errorf("timescaledb extension count = %d, want 1", got)
		}
		if !tableExists(t, pool, "devices") {
			t.Error("devices table does not exist")
		}
		if !tableExists(t, pool, "device_telemetry") {
			t.Error("device_telemetry table does not exist")
		}

		const hypertableQuery = `SELECT count(*) FROM timescaledb_information.hypertables
			WHERE hypertable_schema = current_schema() AND hypertable_name = 'device_telemetry'`
		if got := countQuery(t, pool, hypertableQuery); got != 1 {
			t.Errorf("device_telemetry hypertable count = %d, want 1", got)
		}

		const dimensionQuery = `SELECT count(*) FROM timescaledb_information.dimensions
			WHERE hypertable_schema = current_schema() AND hypertable_name = 'device_telemetry'
			  AND column_name = 'time'`
		if got := countQuery(t, pool, dimensionQuery); got != 1 {
			t.Errorf("time dimension count = %d, want 1", got)
		}
	})

	t.Run("S-006 unknown device rows insert with zero device rows", func(t *testing.T) {
		resetSchema(t, pool)

		if err := Up(context.Background(), pool); err != nil {
			t.Fatalf("Up: %v", err)
		}

		const insert = `INSERT INTO device_telemetry
			(device_id, time, sequence, temperature_c, voltage_v, current_a)
			VALUES ($1, $2, $3, $4, $5, $6)`
		if _, err := pool.Exec(context.Background(), insert,
			"unregistered-device", time.Now().UTC(), int64(1), 21.5, 3.3, 0.75); err != nil {
			t.Fatalf("insert telemetry for unknown device: %v", err)
		}

		if got := countQuery(t, pool, "SELECT count(*) FROM devices"); got != 0 {
			t.Errorf("devices row count = %d, want 0", got)
		}
		if got := countQuery(t, pool, "SELECT count(*) FROM device_telemetry"); got != 1 {
			t.Errorf("device_telemetry row count = %d, want 1", got)
		}
	})

	t.Run("S-007 rows partition by time into multiple chunks", func(t *testing.T) {
		resetSchema(t, pool)

		if err := Up(context.Background(), pool); err != nil {
			t.Fatalf("Up: %v", err)
		}

		first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		second := first.Add(60 * 24 * time.Hour)
		const insert = `INSERT INTO device_telemetry
			(device_id, time, sequence, temperature_c, voltage_v, current_a)
			VALUES ($1, $2, $3, $4, $5, $6)`
		for i, ts := range []time.Time{first, second} {
			if _, err := pool.Exec(context.Background(), insert,
				"partition-device", ts, int64(i), 20.0, 3.3, 0.5); err != nil {
				t.Fatalf("insert row %d: %v", i, err)
			}
		}

		const chunkQuery = `SELECT count(*) FROM timescaledb_information.chunks
			WHERE hypertable_schema = current_schema() AND hypertable_name = 'device_telemetry'`
		if got := countQuery(t, pool, chunkQuery); got < 2 {
			t.Errorf("chunk count = %d, want at least 2 for timestamps 60 days apart", got)
		}
	})
}

func appliedMigrations(t *testing.T, pool *pgxpool.Pool) map[int64]string {
	t.Helper()

	rows, err := pool.Query(context.Background(), "SELECT version, checksum FROM schema_migrations")
	if err != nil {
		t.Fatalf("query schema_migrations: %v", err)
	}
	defer rows.Close()

	applied := make(map[int64]string)
	for rows.Next() {
		var version int64
		var checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			t.Fatalf("scan schema_migrations: %v", err)
		}
		applied[version] = checksum
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate schema_migrations: %v", err)
	}

	return applied
}

func appliedAt(t *testing.T, pool *pgxpool.Pool) map[int64]time.Time {
	t.Helper()

	rows, err := pool.Query(context.Background(), "SELECT version, applied_at FROM schema_migrations")
	if err != nil {
		t.Fatalf("query applied_at: %v", err)
	}
	defer rows.Close()

	applied := make(map[int64]time.Time)
	for rows.Next() {
		var version int64
		var at time.Time
		if err := rows.Scan(&version, &at); err != nil {
			t.Fatalf("scan applied_at: %v", err)
		}
		applied[version] = at
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate applied_at: %v", err)
	}

	return applied
}

func tableExists(t *testing.T, pool *pgxpool.Pool, name string) bool {
	t.Helper()

	const query = `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name = $1`
	return countQuery(t, pool, query, name) == 1
}

func countQuery(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()

	var count int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	return count
}

// Package db owns the PostgreSQL/TimescaleDB persistence layer: the embedded
// forward-only migration runner, the connection pool, and the batch inserter.
package db

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationFS embeds every SQL migration into the binary so the service can
// migrate itself at boot without shipping loose files.
//
//go:embed migrations/*.sql
var migrationFS embed.FS

const (
	migrationDir = "migrations"

	// advisoryLockKey serialises concurrent runners ("QSHP"). Any fixed value
	// works as long as every replicator uses the same one.
	advisoryLockKey int64 = 0x51534850
)

// errChecksumMismatch marks a migration whose embedded bytes no longer match
// the checksum recorded at apply time.
var errChecksumMismatch = errors.New("db: migration checksum mismatch")

var migrationNamePattern = regexp.MustCompile(`^([0-9]{3})_[A-Za-z0-9_-]+\.sql$`)

// migration is one ordered, checksummed SQL file.
type migration struct {
	version  int64
	name     string
	checksum string
	sql      string
}

// Up applies every embedded migration that has not been recorded yet, in
// ascending version order, under a PostgreSQL advisory lock. It is forward
// only and safe to call concurrently from several processes.
func Up(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return errors.New("db: migrate: nil pool")
	}

	migrations, err := loadMigrations(migrationFS)
	if err != nil {
		return err
	}
	if len(migrations) == 0 {
		return errors.New("db: migrate: no embedded migrations")
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("db: migrate: acquire connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("db: migrate: acquire advisory lock: %w", err)
	}
	defer func() {
		// The session-scoped lock must be released even when ctx has already
		// been canceled, hence WithoutCancel. A failed unlock is not
		// actionable here; Release returns the session to the pool.
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", advisoryLockKey)
	}()

	if _, err := conn.Exec(ctx, createMigrationsTableSQL); err != nil {
		return fmt.Errorf("db: migrate: create schema_migrations: %w", err)
	}

	applied, err := loadApplied(ctx, conn)
	if err != nil {
		return err
	}

	for _, m := range migrations {
		recorded, ok := applied[m.version]
		switch {
		case ok && recorded != m.checksum:
			return fmt.Errorf("db: migrate: %s (version %d): recorded checksum %q, embedded checksum %q: %w",
				m.name, m.version, recorded, m.checksum, errChecksumMismatch)
		case ok:
			continue
		}

		if err := applyOne(ctx, conn, m); err != nil {
			return err
		}
	}

	return nil
}

const createMigrationsTableSQL = `CREATE TABLE IF NOT EXISTS schema_migrations (
    version    BIGINT PRIMARY KEY,
    checksum   TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
)`

// loadMigrations reads, validates, checksums, and orders the migrations in dir.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, migrationDir)
	if err != nil {
		return nil, fmt.Errorf("db: migrate: read %s: %w", migrationDir, err)
	}

	seen := make(map[int64]string, len(entries))
	migrations := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		match := migrationNamePattern.FindStringSubmatch(name)
		if match == nil {
			return nil, fmt.Errorf("db: migrate: invalid migration filename %q: want NNN_name.sql", name)
		}

		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("db: migrate: parse version of %q: %w", name, err)
		}
		if previous, ok := seen[version]; ok {
			return nil, fmt.Errorf("db: migrate: duplicate version %d: %q and %q", version, previous, name)
		}
		seen[version] = name

		body, err := fs.ReadFile(fsys, path.Join(migrationDir, name))
		if err != nil {
			return nil, fmt.Errorf("db: migrate: read %q: %w", name, err)
		}

		migrations = append(migrations, migration{
			version:  version,
			name:     name,
			checksum: checksumOf(body),
			sql:      string(body),
		})
	}

	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })

	return migrations, nil
}

// checksumOf hashes the exact file bytes as lowercase hex SHA-256.
func checksumOf(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func loadApplied(ctx context.Context, conn *pgxpool.Conn) (map[int64]string, error) {
	rows, err := conn.Query(ctx, "SELECT version, checksum FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("db: migrate: read schema_migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int64]string)
	for rows.Next() {
		var version int64
		var checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			return nil, fmt.Errorf("db: migrate: scan schema_migrations: %w", err)
		}
		applied[version] = checksum
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("db: migrate: iterate schema_migrations: %w", err)
	}

	return applied, nil
}

// applyOne runs one migration and its bookkeeping row in a single transaction.
func applyOne(ctx context.Context, conn *pgxpool.Conn, m migration) (err error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db: migrate: begin %s: %w", m.name, err)
	}
	defer func() {
		if err == nil {
			return
		}
		// Roll back on the way out; WithoutCancel keeps cleanup working when
		// the failure was a canceled context.
		if rollbackErr := tx.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("db: migrate: rollback %s: %w", m.name, rollbackErr))
		}
	}()

	// A migration file holds several statements; the simple protocol is the
	// only one that accepts multi-statement queries (the extended protocol
	// rejects them during Parse). The statements still run inside tx.
	if _, err = tx.Exec(ctx, m.sql, pgx.QueryExecModeSimpleProtocol); err != nil {
		return fmt.Errorf("db: migrate: apply %s: %w", m.name, err)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO schema_migrations (version, checksum) VALUES ($1, $2)", m.version, m.checksum); err != nil {
		return fmt.Errorf("db: migrate: record %s: %w", m.name, err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("db: migrate: commit %s: %w", m.name, err)
	}

	return nil
}

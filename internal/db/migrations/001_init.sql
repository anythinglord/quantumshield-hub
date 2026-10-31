-- 001_init.sql — Phase 3 foundation schema (ROADMAP 3.1).
--
-- Forward-only: an applied migration is immutable. Schema changes always land
-- in a new NNN_*.sql file; editing this one after release aborts startup with
-- a checksum-mismatch error (see internal/db/migrate.go).

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Device identities. Phase 4 owns writes (RegisterDevice); Phase 3 ingestion
-- must work with zero rows here, which is why device_telemetry carries no FK
-- to this table (design Q1).
CREATE TABLE IF NOT EXISTS devices (
    device_id           TEXT PRIMARY KEY,
    -- apiv1.SignatureAlgorithm numeric values: 0 UNSPECIFIED, 1 ML_DSA_44,
    -- 2 ML_DSA_65, 3 ML_DSA_87.
    signature_algorithm SMALLINT NOT NULL DEFAULT 0
        CHECK (signature_algorithm BETWEEN 0 AND 3),
    -- Raw ML-DSA public key bytes; nullable until Phase 4 registration.
    public_key          BYTEA,
    -- apiv1.DeviceStatus numeric values: 0 UNSPECIFIED, 1 ACTIVE,
    -- 2 SUSPENDED, 3 REVOKED.
    status              SMALLINT NOT NULL DEFAULT 0
        CHECK (status BETWEEN 0 AND 3),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per sensor reading. `time` is the hypertable partitioning column.
CREATE TABLE IF NOT EXISTS device_telemetry (
    device_id     TEXT NOT NULL,
    time          TIMESTAMPTZ NOT NULL,
    sequence      BIGINT,
    temperature_c DOUBLE PRECISION,
    voltage_v     DOUBLE PRECISION,
    current_a     DOUBLE PRECISION,
    lat           DOUBLE PRECISION NULL,
    long          DOUBLE PRECISION NULL
);

-- TimescaleDB >= 2.13 range partitioning on the time column.
SELECT create_hypertable('device_telemetry', by_range('time'));

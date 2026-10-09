# Instructions & Guidelines for AI Coding Agents

This document defines the architectural patterns, coding standards, toolings, and system invariants for **QuantumShield Hub**. Any AI assistant or agent working on this repository MUST follow these rules to maintain code quality and security standards.

---

## 1. Project Context & Philosophy

**QuantumShield Hub** is a cloud-native, high-throughput IoT telemetry ingestion system built with **Go**, **gRPC/Protobuf**, **PostgreSQL/TimescaleDB**, **Kubernetes**, **Terraform**, and **Post-Quantum Cryptography (PQC)**.

* **Primary Language:** Go 1.27.0 (pinned in `go.mod`). ML-DSA is available in the standard library as `crypto/mldsa` (Go 1.27+); `tls.X25519MLKEM768` has been available since Go 1.23.
* **Core Paradigm:** Zero-Trust Security, Asynchronous Streaming, Concurrent Worker Pools, and Observability-First Design.
* **Key Constraints:**
  * Strict mTLS transport (TLS 1.3 with Hybrid PQC KEM: X25519 + ML-KEM-768).
  * Payloads signed at Edge with ML-DSA (Dilithium).
  * High-concurrency, non-blocking Go channels and goroutines.
  * Time-series optimization via TimescaleDB hypertables.

---

## 2. Directory & Architecture Map

> Target layout: many of these paths land in later `ROADMAP.md` phases. If a file does not exist yet, that is expected — do not create it speculatively; follow the phase plan.

```text
quantumshield-hub/
├── .github/workflows/               # CI pipeline (Phase 1.6)
├── api/
│   └── v1/
│       └── telemetry.proto          # Protocol Buffers contract
├── build/
│   ├── Dockerfile.ingestion         # Multistage build for Ingestion Service
│   ├── Dockerfile.auth              # Multistage build for Auth Service (Phase 6)
│   └── Dockerfile.simulator         # Multistage build for Node Simulator
├── cmd/
│   ├── auth-service/
│   │   └── main.go                  # Device Auth Service entrypoint (Phase 4)
│   ├── ingestion-service/
│   │   └── main.go                  # Service entrypoint
│   └── device-simulator/
│       └── main.go                  # Concurrent CLI node simulator
├── docker-compose.yml               # Local integration stack (Phase 3.5)
├── internal/
│   ├── config/                      # Environment and flag parsing
│   ├── crypto/                      # mTLS configuration and PQC signature helpers
│   ├── db/                          # TimescaleDB pool (pgxpool) + SQL migrations (Phase 3.1)
│   ├── metrics/                     # Prometheus custom collector metrics
│   ├── service/                     # gRPC handlers and worker pool logic
│   └── simulator/                   # Synthetic edge device data engine (Phase 2)
├── k8s/
│   ├── base/                        # Deployments, Services, ConfigMaps, Secrets
│   ├── monitoring/dashboards/       # Grafana dashboard JSON (Phase 7.2)
│   └── overlays/                    # Environment customizations (Kustomize)
├── terraform/                       # Infrastructure provisioner scripts
│   ├── main.tf
│   ├── variables.tf
│   └── outputs.tf
├── scripts/                         # Certificate generation & helper shell scripts
├── CHANGELOG.md                     # Generated from Conventional Commits (git-cliff)
├── cliff.toml                       # git-cliff changelog configuration
├── Makefile                         # Build, test, and run automation tasks
└── README.md                        # Documentation
```

## 3. Coding Standards & Conventions

### Go Guidelines
* **Conformity:** Follow Standard Go Project Layout and official [Effective Go](https://golang.org/doc/effective_go) guidelines.
* **Error Handling:** 
  * Always wrap errors with context: `fmt.Errorf("context message: %w", err)`.
  * Never discard errors with `_` unless the discard is deliberate and conventional (`defer f.Close()`, `recover()` inside a deferred handler) — comment why when it isn't obvious.
  * Do NOT use `panic()` in production code. Fail fast inside `main()` or initial setup functions only.
* **Concurrency:**
  * Always use `context.Context` for cancellation propagation, timeouts, and deadlines across gRPC streams.
  * Ensure goroutines exit cleanly. Use `sync.WaitGroup` or channel closing signals.
  * Avoid global state; inject dependencies explicitly into structs.
* **Database Interactions:**
  * Use `github.com/jackc/pgx/v5/pgxpool` for PostgreSQL connections.
  * Use parameterized queries or prepared statements exclusively—NEVER construct raw SQL strings via concatenation.
* **Testing:**
  * New packages ship with `_test.go` — tests are part of the task, not a final-phase afterthought.
  * `go test ./...` must stay green on every change.
  * Prefer table-driven tests; keep tests in the same package unless black-box behavior matters.

### Cryptography & Security Rules
* **TLS Requirements:** Minimum TLS version MUST be `tls.VersionTLS13`.
* **PQC Integration:** Use `tls.X25519MLKEM768` (or Cloudflare `circl` bindings) for key exchange.
* **Secret Hygiene:** Never hardcode certificates, private keys, database passwords, or connection strings in Go code, YAML, or HCL files. Use environment variables or Kubernetes Secrets.

### gRPC & Protocol Buffers
* **Proto Definitions:** Modifying `api/v1/*.proto` requires running `make proto` to update Go bindings.
* **Backward Compatibility:** Never renumber existing Protobuf fields (`1`, `2`, `3`). Only add new fields with incremented field tags.

### Commit Convention (Conventional Commits)
* Every commit MUST follow [Conventional Commits](https://www.conventionalcommits.org/): `<type>(<scope>): <description>` (e.g. `feat(crypto): add ML-DSA verification wrapper`).
* Allowed types: `feat`, `fix`, `perf`, `refactor`, `docs`, `test`, `build`, `ci`, `chore`, `revert`. Mark breaking changes with `!` (e.g. `feat(api)!: ...`).
* This convention drives `CHANGELOG.md` generation via `git-cliff` (`cliff.toml`). A non-conventional commit is treated as a defect and lands in the "Other" section.
* Releases are tagged `v0.X.0` at each completed ROADMAP phase (minor per phase). Do not tag per task.

## 4. Development Workflow & Commands

### Git Workflow
* Branch per ROADMAP phase (`ft/phase-1`, `ft/phase-2`, ...) created from `develop`. Do not create a branch per task.
* Tasks land as one or more Conventional Commits on the phase branch — tests and docs travel with the code.
* Release cadence: one `v0.X.0` per completed phase. Flow: merge phase branch → `develop`, then `develop` → `main`, tag `v0.X.0` on `main`.
* `main` is stable and deployable; `develop` is the integration area and may be dirty between phases.

Agents should execute or suggest running these commands via the `Makefile`:

> Targets are created in `ROADMAP.md` Phase 1 (build automation) and Phases 5–6 (dev environment). Until a target exists, do not invent ad-hoc shell substitutes — report the missing target instead.

```bash
# Generate Go code from Protocol Buffers
make proto

# Generate mTLS and PQC test certificates
make certs

# Run unit and integration tests
make test

# Format and lint Go code
make lint

# Regenerate CHANGELOG.md from Conventional Commits (requires git-cliff)
make changelog

# Spin up local development environment (Docker + Kind + Terraform)
make dev-up

# Tear down local development infrastructure
make dev-down
```

## 5. Agent Safety & Execution Constraints

* No Destructive Actions: Do NOT execute commands that modify system host states outside the project tree without explicit user consent.

* Deterministic Outputs: Keep imports cleanly grouped (std, 3rd-party, internal). Run go fmt and go mod tidy after modifying Go code.

* Documentation: Any addition of a microservice, gRPC RPC method, or environment variable must be reflected in README.md.
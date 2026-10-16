# QuantumShield Hub

Post-Quantum Encrypted Telemetry & IoT Control Plane for Critical Infrastructure

[![CI](https://github.com/anythinglord/quantumshield-hub/actions/workflows/ci.yml/badge.svg)](https://github.com/anythinglord/quantumshield-hub/actions/workflows/ci.yml)

## Overview

QuantumShield Hub is a cloud-native, zero-trust ingestion and telemetry control plane designed for high-concurrency IoT nodes operating in critical infrastructure environments (energy grids, vehicular fleets, industrial sensors).

Traditional transport security protocols remain vulnerable to "Harvest Now, Decrypt Later" attacks, where encrypted traffic captured today could be retroactively decrypted once fault-tolerant quantum computers emerge. QuantumShield Hub mitigates this threat vector by implementing hybrid Post-Quantum Cryptography (PQC) at the TLS transport layer alongside lattice-based digital signatures for payload integrity.

The platform is capable of processing thousands of gRPC streaming events per second using Go worker pools, storing time-series metrics in TimescaleDB, and exposing deep operational insights via Prometheus and Grafana.

## Status

Early stage — repository bootstrap. No service is running yet. The implementation plan lives in `ROADMAP.md` (currently at Phase 1). Nothing here is production-ready.

## Architecture & System Design

```text
                     +-------------------------------------------------------+
                     |                 Edge / Client Layer                   |
                     |                                                       |
                     |  +-------------------------------------------------+  |
                     |  |         Simulated IoT Node Pool (Go)            |  |
                     |  |  (Metrics Gen + ML-DSA Signing + mTLS Client)   |  |
                     |  +------------------------+------------------------+  |
                     +---------------------------|---------------------------+
                                                  |
                                                  | gRPC Stream over mTLS 
                                                  | (TLS 1.3: X25519 + ML-KEM-768)
                                                  v
+---------------------------------------------------------------------------------------------------+
| Kubernetes Cluster (QuantumShield Control Plane)                                                  |
|                                                                                                   |
|   +-------------------------------------------------------------------------------------------+   |
|   | Envoy / API Gateway (Ingress Routing, TLS Passthrough)                                    |   |
|   +--------------------------------------------+----------------------------------------------+   |
|                                                |                                                  |
|                                                | gRPC stream (mTLS: X25519ML-KEM-768)             |
|                                                v                                                  |
|   +-------------------------------------------------------------------------------------------+   |
|   | Ingestion Service (Go Worker Pool)                                                        |   |
|   |  - Deserializes Protobuf                                                                  |   |
|   |  - Verifies ML-DSA Signatures                                                             |   |
|   |  - Dispatches to Time-Series Buffer                                                       |   |
|   +--------------------------------------------+----------------------------------------------+   |
|                                                |                                                  |
+------------------------------------------------|--------------------------------------------------+
                                                 |
                        +------------------------+------------------------+
                        | Writes                                          | Scrapes /metrics
                        v                                                 v
     +------------------------------------+             +------------------------------------+
     | PostgreSQL + TimescaleDB           |             | Prometheus & Grafana               |
     | (Telemetry Hypertables)            |             | (Observability Suite)              |
     +------------------------------------+             +------------------------------------+
```

### Components

| Component | Path | Roadmap phase |
|-----------|------|---------------|
| Device simulator (edge) | `cmd/device-simulator`, `internal/simulator` | Phase 2 |
| Ingestion service | `cmd/ingestion-service`, `internal/service` | Phase 3 |
| Device auth service | `cmd/auth-service` | Phase 4 |
| Time-series storage | `internal/db` (TimescaleDB hypertables) | Phase 3 |
| Observability | `internal/metrics` (Prometheus + Grafana) | Phase 7 |

## Security & Transport Model

**Decision: TLS terminates at the Ingestion Service, not at the gateway.** The ingress performs L4 routing with TLS passthrough, so the hybrid PQC handshake (X25519 + ML-KEM-768) is end-to-end between edge nodes and the Go service, which enforces `RequireAndVerifyClientCert` with TLS 1.3.

Why: terminating TLS at the perimeter would leave the internal hop outside the PQC envelope, defeating the harvest-now-decrypt-later mitigation that motivates the project. Zero-trust means no implicit trust inside the boundary.

| Hop | Protection |
|-----|-----------|
| Edge → Ingestion | mTLS with hybrid KEM (X25519 + ML-KEM-768); payloads signed with ML-DSA |
| Ingestion → Auth service | Internal mTLS; device status and key lookups |
| Ingestion → TimescaleDB | Private cluster network; credentials via Kubernetes Secrets |

Rejected alternative: terminating TLS at Envoy and re-encrypting internally. It simplifies certificate operations, but adds a classically-encrypted internal hop and depends on gateway-side PQC support.

## Getting Started

Prerequisites: Go 1.27+, Docker, `protoc` (plus Kind + Terraform for the local cluster).

```bash
make proto    # generate gRPC bindings
make certs    # local mTLS + PQC test PKI
make test     # unit + integration tests
make lint     # gofmt + go vet
make dev-up   # local stack (Docker + Kind + Terraform)
make dev-down # tear down local infrastructure
```

These targets are implemented across ROADMAP Phase 1 (build automation: `proto`, `certs`, `test`, `lint`) and Phase 5 (dev environment: `dev-up`, `dev-down`). Until the dev targets exist, see `AGENTS.md` §4 for the full command list.

## Configuration & Environment Variables

Every new environment variable or service must be documented in this README (rule enforced by `AGENTS.md` §5).

### Local test PKI (`make certs`)

`make certs` runs `scripts/gen-certs.sh` to generate a local hybrid-PQC mTLS PKI — root CA → intermediate CA (`pathlen:0`) → server + client TLS 1.3 leaves — under `certs/`. Certificates are ML-DSA-65-signed by default (Ed25519 fallback when OpenSSL lacks ML-DSA); all generated material is git-ignored. The script honors these variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `CERTS_DIR` | `./certs` | Output directory for the generated PKI |
| `SIG_ALG` | `mldsa-65` | Signature algorithm: `mldsa-65` (PQC) or `ed25519` |
| `ROOT_DAYS` | `3650` | Root CA validity in days |
| `INTERMEDIATE_DAYS` | `1825` | Intermediate CA validity in days |
| `LEAF_DAYS` | `825` | Leaf certificate validity in days |
| `FORCE` | unset | Set to `1` to wipe and regenerate an existing PKI |
| `SERVER_SANS` | `DNS:localhost,DNS:ingestion-service,DNS:ingestion-service.quantumshield.svc.cluster.local,DNS:auth-service,DNS:auth-service.quantumshield.svc.cluster.local,DNS:device-simulator,IP:127.0.0.1,IP:::1` | Server leaf SAN list; fully replaces the default |

## Continuous Integration

CI runs on every push and every pull request via [`.github/workflows/ci.yml`](.github/workflows/ci.yml):

- **`lint-test`** — mirrors the local Definition of Done: `make lint` (gofmt + `go vet` + staticcheck) then `make test`.
- **`changelog-stale`** — runs only for `main`-bound events (PR base `main` or push to `main`). It regenerates `CHANGELOG.md` with a pinned git-cliff and fails if the committed file differs. Fix by running `make changelog` locally, committing **only** `CHANGELOG.md`, and pushing again.

Tool versions are pinned to match the local toolchain (staticcheck 2026.2.1, git-cliff 2.14.2) so lint and changelog output are byte-identical between local and CI runs.

## Changelog

`CHANGELOG.md` is generated from Git history with [git-cliff](https://git-cliff.org); the configuration lives in `cliff.toml`.

- Commits follow [Conventional Commits](https://www.conventionalcommits.org/) (`feat`, `fix`, `docs`, `chore`, …).
- Each completed ROADMAP phase is tagged `v0.X.0` (minor per phase). Per-task detail comes from commit scopes, not from individual tags.

```bash
git cliff --output CHANGELOG.md   # regenerate the changelog
```

Requires `git-cliff` (`cargo install git-cliff`).

## Documentation

| File | Purpose |
|------|---------|
| `AGENTS.md` | Architecture rules, coding standards, and agent constraints |
| `ROADMAP.md` | Phased implementation plan with acceptance criteria |

## License

This project is licensed under the Apache License 2.0 — see the [LICENSE](LICENSE) file for details.

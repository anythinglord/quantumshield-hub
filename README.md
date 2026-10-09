# QuantumShield Hub

Post-Quantum Encrypted Telemetry & IoT Control Plane for Critical Infrastructure

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

These targets are implemented in ROADMAP Phase 1 (build automation) and Phase 5 (dev environment). Until they exist, see `AGENTS.md` §4 for the full command list.

## Configuration & Environment Variables

None defined yet. Every new environment variable or service must be documented in this README (rule enforced by `AGENTS.md` §5).

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

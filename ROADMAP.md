# QuantumShield Hub Roadmap

Phased implementation plan. Phases are sequential: contracts → edge → persistence → auth → infrastructure → orchestration → observability.

**Definition of Done — applies to every task:**

- [ ] Code compiles and `go test ./...` is green (new logic ships with tests).
- [ ] `gofmt` and `go vet` clean (via `make lint` once available).
- [ ] `AGENTS.md` directory map updated if paths were added.
- [ ] `README.md` updated if a service, RPC method, or environment variable changed.

---

## 📋 Phase Breakdown & Task Lists

### Phase 1: Contracts, PKI & Base Cryptography
> **Goal:** Establish project conventions, generate PKI infrastructure (mTLS + PQC), and define gRPC interface contracts.

- [x] **Task 1.1: Project Initialization**
  - `go.mod` already exists with module path `quantumshield-hub` — replace it with the canonical repository path (e.g. `github.com/<owner>/quantumshield-hub`) before the first cross-package import lands; renaming later touches every import.
  - Create the standard project layout according to `AGENTS.md` §2 (target layout).
- [x] **Task 1.2: Protocol Buffer Specifications**
  - Create `api/v1/telemetry.proto` defining:
    - `TelemetryIngestionService` (Bidirectional gRPC streaming).
    - `DeviceAuthService` (Registration, PQC key lookup, and status checks).
    - Data messages with fields for ML-DSA (Dilithium) signatures and sensor payload metrics.
- [x] **Task 1.3: Build Automation**
  - Create `Makefile` targets `proto` (protoc-gen-go + protoc-gen-go-grpc), `test`, and `lint` (gofmt + go vet) so the Definition of Done is runnable from day one.
  - Add a `changelog` target wrapping `git cliff --output CHANGELOG.md` (config committed as `cliff.toml`).
- [ ] **Task 1.4: Public Key Infrastructure (PKI)**
  - Write `scripts/gen-certs.sh` to generate local Root CAs, intermediate authorities, and TLS 1.3 client/server certificates.
- [ ] **Task 1.5: Cryptographic Utilities Package**
  - Implement `internal/crypto`:
    - `crypto/tls` configuration for strict mTLS (`RequireAndVerifyClientCert`) with hybrid PQC curve support (`tls.X25519MLKEM768`).
    - Signature verification wrappers for ML-DSA (Dilithium) payloads.
  - Tests: assert `MinVersion == tls.VersionTLS13` and `ClientAuth == RequireAndVerifyClientCert`; ML-DSA sign→verify round-trip plus tampered-payload rejection.

- [ ] **Task 1.6: CI Pipeline**
  - GitHub Actions workflow running `gofmt` check, `go vet`, and `go test ./...` on every push and pull request.
  - Fail the build if `CHANGELOG.md` is stale (generated output differs from the committed file).
  - Keeps the Definition of Done enforced for all later phases.

- [ ] **Task 1.7: Repository Hygiene**
  - Add a `LICENSE`.
  - Keep `AGENTS.md` and `ROADMAP.md` tracked (contract files must be visible to clones and CI) — do not re-add them to `.gitignore`.
  - Keep `CHANGELOG.md` and `cliff.toml` tracked as well.

---

### Phase 2: Edge Node & Device Simulator
> **Goal:** Build a concurrent IoT client simulator in Go to test payload generation, PQC signing, and gRPC streaming independently.

- [ ] **Task 2.1: CLI Framework**
  - Implement the simulator CLI entrypoint at `cmd/device-simulator/main.go` supporting flags (`--devices`, `--rate`, `--address`, `--pqc-enabled`).
- [ ] **Task 2.2: Synthetic Data Engine**
  - Develop `internal/simulator` to generate realistic sensor metrics (CPU temperature, voltage, current, GPS coordinates).
  - Tests: table-driven coverage of value ranges (temperature, voltage, GPS bounds).
- [ ] **Task 2.3: Concurrency Control**
  - Utilize Goroutines and Channels to emulate hundreds of independent edge devices running concurrently.
- [ ] **Task 2.4: gRPC Streaming Client**
  - Integrate gRPC client streaming with mTLS certificate loading and per-event ML-DSA signature generation.

---

### Phase 3: Persistence Layer & Ingestion Engine
> **Goal:** Build the time-series storage layer and high-throughput ingestion core capable of non-blocking stream processing.

- [ ] **Task 3.1: Database Schema & Migrations**
  - Write SQL migrations in `internal/db/migrations`:
    - Enable `timescaledb` extension.
    - Create `devices` table for device identities and ML-DSA public keys.
    - Create `device_telemetry` table converted into a time-partitioned **Hypertable**.
- [ ] **Task 3.2: Connection Pooling**
  - Configure `pgxpool` in `internal/db` with optimized connection parameters.
- [ ] **Task 3.3: Worker Pool Pattern Implementation**
  - Implement a bounded worker pool in `internal/service/ingestion`:
    - Buffered channels receiving gRPC stream payloads.
    - Worker goroutines executing batch inserts into TimescaleDB.
  - Tests: all workers drain on channel close with no goroutine leaks; batches flush on N payloads.
- [ ] **Task 3.4: Ingestion Service Entrypoint**
  - Build `cmd/ingestion-service/main.go` bootstrapping the mTLS-enabled gRPC server.
- [ ] **Task 3.5: Local Integration Test**
  - Verify ingestion end-to-end using `docker-compose.yml` (PostgreSQL/TimescaleDB + Ingestion Service + Simulator).

---

### Phase 4: Device Auth Service & Inter-Service Mesh
> **Goal:** Decouple security management and identity validation into a dedicated microservice.

- [ ] **Task 4.1: Auth Service Entrypoint**
  - Implement `cmd/auth-service/main.go` exposing gRPC handlers for `DeviceAuthService`.
- [ ] **Task 4.2: Device Lifecycle & Key Management**
  - Implement registration RPCs to save ML-DSA public keys and manage device status.
- [ ] **Task 4.3: Inter-Service gRPC Communication**
  - Connect `Ingestion Service` to `Auth Service` via internal gRPC calls to validate device active status before accepting telemetry streams.
- [ ] **Task 4.4: In-Memory Caching**
  - Implement an in-memory LRU/sync.Map cache inside the Ingestion Service to prevent network overhead on every stream event.
  - Tests: hit/miss/eviction paths and stale-entry invalidation.

---

### Phase 5: Infrastructure as Code (Terraform + Kind)
> **Goal:** Automate cluster provisioning and environment declaration locally.

- [ ] **Task 5.1: Terraform Modules**
  - Write Terraform modules in `terraform/`:
    - `kind_cluster` module to provision a local Kubernetes cluster with port mappings.
    - `namespaces` module to isolate `quantumshield` and `monitoring` environments.
- [ ] **Task 5.2: Helm Provider Integrations**
  - Configure Terraform Helm providers to install CRDs (Prometheus Operator, Ingress Controllers).
- [ ] **Task 5.3: Outputs & Variables**
  - Parameterize cluster configurations via `terraform.tfvars`.
  - Wire `make dev-up` / `make dev-down` to Terraform + Kind (closes the command list in `AGENTS.md` §4).

---

### Phase 6: Kubernetes Orchestration & Gateway
> **Goal:** Package services into minimal container images and deploy them onto Kubernetes with ingress routing and autoscaling.

- [ ] **Task 6.1: Multistage Container Builds**
  - Create optimized Dockerfiles in `build/`:
    - `Dockerfile.ingestion` (Distroless/Static Go build).
    - `Dockerfile.auth`.
    - `Dockerfile.simulator`.
- [ ] **Task 6.2: Declarative Manifests**
  - Create Kubernetes manifests in `k8s/base`:
    - `StatefulSet` for TimescaleDB with persistent volume claims (`PVC`).
    - `Deployments` & `Services` for microservices.
    - `ConfigMaps` and `Secrets` for mTLS certs and DB credentials.
- [ ] **Task 6.3: Ingress Gateway**
  - Configure Envoy/NGINX Ingress Gateway with TLS Passthrough / mTLS termination at cluster perimeter.
- [ ] **Task 6.4: Horizontal Pod Autoscaler (HPA)**
  - Define HPA scaling rules triggered by CPU utilization and metric thresholds.

---

### Phase 7: Observability, Benchmarking & Release Hardening
> **Goal:** Instrument metrics, conduct stress tests, and harden the repository for review.

- [ ] **Task 7.1: Prometheus Instrumentation**
  - Integrate `prometheus/client_golang` in `internal/metrics`:
    - Counter: Total events ingested.
    - Histogram: PQC verification latency distribution.
    - Gauge: Active gRPC stream count.
- [ ] **Task 7.2: Grafana Visualizations**
  - Build dashboard JSON files in `k8s/monitoring/dashboards` charting throughput, system memory, and PQC verification overhead.
- [ ] **Task 7.3: Load Testing & Benchmarking**
  - Stress-test cluster with 500+ simulated nodes to record p99 latencies and throughput metrics.
- [ ] **Task 7.4: Documentation Updates**
  - Document benchmark results and embed Grafana performance charts in `README.md`.
- [ ] **Task 7.5: Final Code Audit**
  - Enforce coverage floor: `go test -cover ./internal/...` ≥ 70%.
  - Ensure formatting (`gofmt`), `go vet`, and full compliance with `AGENTS.md`.
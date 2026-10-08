# QuantumShield Hub

Post-Quantum Encrypted Telemetry & IoT Control Plane for Critical Infrastructure

## Overview

QuantumShield Hub is a cloud-native, zero-trust ingestion and telemetry control plane designed for high-concurrency IoT nodes operating in critical infrastructure environments (energy grids, vehicular fleets, industrial sensors).

Traditional transport security protocols remain vulnerable to "Harvest Now, Decrypt Later" attacks, where encrypted traffic captured today could be retroactively decrypted once fault-tolerant quantum computers emerge. QuantumShield Hub mitigates this threat vector by implementing hybrid Post-Quantum Cryptography (PQC) at the TLS transport layer alongside lattice-based digital signatures for payload integrity.

The platform is capable of processing thousands of gRPC streaming events per second using Go worker pools, storing time-series metrics in TimescaleDB, and exposing deep operational insights via Prometheus and Grafana.

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
|   | Envoy / API Gateway (mTLS Termination & Ingress Routing)                                  |   |
|   +--------------------------------------------+----------------------------------------------+   |
|                                                |                                                  |
|                                                | gRPC (Internal Cluster Mesh)                     |
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
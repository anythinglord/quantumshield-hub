# QuantumShield Hub — build automation (ROADMAP Phase 1, Task 1.3).
#
# Canonical developer/CI interface for the Definition of Done: run `make`
# (or `make help`) to list the available targets.

# Resolve the Go bin directory where `go install` drops protoc plugins.
GOBIN          := $(shell go env GOPATH)/bin
PROTO_DIR      := api/v1
PROTO_FILE     := $(PROTO_DIR)/telemetry.proto
PLUGIN_GO      := $(GOBIN)/protoc-gen-go
PLUGIN_GO_GRPC := $(GOBIN)/protoc-gen-go-grpc

# Make the plugins discoverable by protoc even when GOPATH/bin is not on PATH.
export PATH := $(GOBIN):$(PATH)

.DEFAULT_GOAL := help
.PHONY: help proto test lint fmt changelog staticcheck tools

help: ## List available targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "};{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

proto: ## Generate Go + gRPC bindings (api/v1, package apiv1)
	protoc -I . \
	  --go_out=. --go_opt=paths=source_relative \
	  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	  $(PROTO_FILE)

test: ## Run go test ./...
	go test ./...

lint: ## gofmt check + go vet (read-only)
	@test -z "$$(gofmt -l .)" || { echo "gofmt: unformatted files:"; gofmt -l .; exit 1; }
	go vet ./...

fmt: ## Format all Go code in place
	gofmt -w .

changelog: ## Regenerate CHANGELOG.md from Conventional Commits
	git cliff --output CHANGELOG.md

staticcheck: ## Run staticcheck (optional; not part of lint)
	staticcheck ./...

tools: ## Install pinned protoc plugins into $(GOBIN)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.11
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2

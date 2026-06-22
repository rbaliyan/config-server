# Container runtime: podman (default) or docker. Override with DOCKER=docker.
DOCKER := env("DOCKER", "podman")

# PostgreSQL test container settings (for the integration-tagged ownership suite)
PG_CONTAINER := "config-server-postgres-test"
PG_IMAGE := "postgres:16"
PG_PORT := "5434"
PG_USER := "config_test"
PG_PASS := "config_test"
PG_DB := "config_test"

# Redis test container settings (for the integration-tagged transport suite)
REDIS_CONTAINER := "config-server-redis-test"
REDIS_IMAGE := "redis:7"
REDIS_PORT := "6380"

# Install tools via mise
setup:
    mise install

# Generate protobuf code.
#
# Output dir is `proto/` (not `.`) so paths=source_relative resolves the input
# `proto/config/v1/config.proto` to `proto/config/v1/*.pb.go` rather than
# `./config/v1/*.pb.go`. grpc-gateway's .proto deps are picked up from the
# active module cache via `go list` so the path tracks go.mod automatically.
# googleapis is vendored into third_party/ to avoid a network dependency.
proto:
    protoc \
        --go_out=proto --go_opt=paths=source_relative \
        --go-grpc_out=proto --go-grpc_opt=paths=source_relative \
        --grpc-gateway_out=proto --grpc-gateway_opt=paths=source_relative,generate_unbound_methods=true \
        -I proto \
        -I third_party/googleapis \
        -I "$(go env GOMODCACHE)/github.com/grpc-ecosystem/grpc-gateway/v2@$(go list -m github.com/grpc-ecosystem/grpc-gateway/v2 | awk '{print $2}')" \
        proto/config/v1/config.proto

# Download googleapis for proto imports
proto-deps:
    @mkdir -p third_party/googleapis/google/api
    @curl -sL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto -o third_party/googleapis/google/api/annotations.proto
    @curl -sL https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto -o third_party/googleapis/google/api/http.proto

# Generate proto with local third_party (outputs to proto dir)
proto-local:
    protoc \
        --go_out=proto --go_opt=paths=source_relative \
        --go-grpc_out=proto --go-grpc_opt=paths=source_relative \
        --grpc-gateway_out=proto --grpc-gateway_opt=paths=source_relative,generate_unbound_methods=true \
        -I proto \
        -I third_party/googleapis \
        proto/config/v1/config.proto

# Fast in-process smoke subset (bufconn gRPC + httptest HTTP + memory store).
# No Docker/network: runs in a couple of seconds. Used as the pre-merge gate
# that the slower CI jobs depend on. Keep these tests fast and hermetic.
smoke:
    go test -run '^TestSmoke' -timeout 60s ./...

# Run tests (root module + opa nested module)
test:
    go test -v ./...
    cd authorizer/opa && go test -v ./...

# Run tests with race detector (root module + opa nested module)
test-race:
    go test -race ./...
    cd authorizer/opa && go test -race ./...

# Run integration-tagged tests, spinning up throwaway PostgreSQL + Redis via podman
test-integration: pg-start redis-start
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just pg-stop; just redis-stop' EXIT
    POSTGRES_DSN="postgres://{{PG_USER}}:{{PG_PASS}}@localhost:{{PG_PORT}}/{{PG_DB}}?sslmode=disable" \
    REDIS_ADDR="localhost:{{REDIS_PORT}}" \
        go test -race -tags=integration ./...

# Run integration-tagged tests against an external POSTGRES_DSN (skips if unset)
test-integration-only:
    go test -race -tags=integration ./...

# Start a PostgreSQL test container and wait for readiness.
pg-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{PG_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{PG_CONTAINER}} > /dev/null
    fi
    {{DOCKER}} run -d --name {{PG_CONTAINER}} \
        -e POSTGRES_USER={{PG_USER}} -e POSTGRES_PASSWORD={{PG_PASS}} -e POSTGRES_DB={{PG_DB}} \
        -p {{PG_PORT}}:5432 {{PG_IMAGE}} > /dev/null
    for i in $(seq 1 30); do
        if {{DOCKER}} exec {{PG_CONTAINER}} pg_isready -U {{PG_USER}} > /dev/null 2>&1; then
            exit 0
        fi
        sleep 1
    done
    echo "postgres did not become ready" >&2
    exit 1

# Stop and remove the PostgreSQL test container.
pg-stop:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{PG_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{PG_CONTAINER}} > /dev/null
    fi

# Start a Redis test container and wait for readiness.
redis-start:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{REDIS_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{REDIS_CONTAINER}} > /dev/null
    fi
    {{DOCKER}} run -d --name {{REDIS_CONTAINER}} -p {{REDIS_PORT}}:6379 {{REDIS_IMAGE}} > /dev/null
    for i in $(seq 1 30); do
        if {{DOCKER}} exec {{REDIS_CONTAINER}} redis-cli ping > /dev/null 2>&1; then
            exit 0
        fi
        sleep 1
    done
    echo "redis did not become ready" >&2
    exit 1

# Stop and remove the Redis test container.
redis-stop:
    #!/usr/bin/env bash
    set -euo pipefail
    if {{DOCKER}} ps -a --format '{{"{{.Names}}"}}' | grep -q "^{{REDIS_CONTAINER}}$"; then
        {{DOCKER}} rm -f {{REDIS_CONTAINER}} > /dev/null
    fi

# Run tests with coverage (root module + opa nested module)
test-cover:
    go test -cover ./...
    cd authorizer/opa && go test -cover ./...

# Enforce a minimum per-package coverage threshold (80%) across both modules
test-cover-gate:
    #!/usr/bin/env bash
    # Threshold is enforced per tested package over the root module and the
    # authorizer/opa nested module, and kept in sync with CI's coverage gate.
    # Packages with no test files (examples/*, generated proto/*) are excluded:
    # `go test -cover` prints a "?"/bare line with 0.0% for them, which the
    # awk filter below skips because it only inspects lines starting with "ok".
    set -euo pipefail
    threshold=80.0
    out=$(mktemp)
    trap 'rm -f "$out"' EXIT
    go test -cover ./... | tee "$out"
    ( cd authorizer/opa && go test -cover ./... ) | tee -a "$out"
    # Evaluate each TESTED package's coverage figure against the threshold.
    # Only lines beginning with "ok" carry a real result; packages with no test
    # files (examples/*, generated proto/*) print a "?"/bare line with 0.0% and
    # are intentionally skipped.
    awk -v t="$threshold" '
        $1 == "ok" {
            pkg = $2
            cov = ""
            for (i = 1; i <= NF; i++) {
                if ($i == "coverage:") { c = $(i+1); sub(/%/, "", c); cov = c + 0 }
            }
            if (cov == "") next
            if (cov < t) { printf "FAIL %s: %.1f%% < %.1f%%\n", pkg, cov, t; fail = 1 }
            else         { printf "ok   %s: %.1f%%\n", pkg, cov }
        }
        END {
            if (fail) { print "coverage gate failed (threshold " t "%)"; exit 1 }
            print "coverage gate passed (threshold " t "%)"
        }
    ' "$out"

# Run benchmarks (10 runs each, with allocation stats) and save to bench.txt
bench:
    go test -run '^$' -bench=. -benchmem -count=10 ./... | tee bench.txt

# Build all packages
build:
    go build ./...

# Tidy dependencies
tidy:
    go mod tidy

# Format code
fmt:
    go fmt ./...

# Lint code
lint:
    golangci-lint run ./...

# Run vulnerability check
vulncheck:
    go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Check for outdated dependencies
depcheck:
    go list -m -u all | grep '\[' || echo "All dependencies are up to date"

# Create and push a new release tag (bumps patch version)
release:
    ./scripts/release.sh

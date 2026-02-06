# Install tools via mise
setup:
    mise install

# Generate protobuf code
proto:
    protoc \
        --go_out=. --go_opt=paths=source_relative \
        --go-grpc_out=. --go-grpc_opt=paths=source_relative \
        --grpc-gateway_out=. --grpc-gateway_opt=paths=source_relative,generate_unbound_methods=true \
        -I proto \
        -I $(go env GOPATH)/pkg/mod/github.com/grpc-ecosystem/grpc-gateway/v2@v2.25.1 \
        -I $(go env GOPATH)/pkg/mod/github.com/googleapis/googleapis@v0.0.0-20250115164207-1a7da9e5054f \
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

# Run tests
test:
    go test -v ./...

# Run tests with race detector
test-race:
    go test -race ./...

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
    go vet ./...

# Create and push a new release tag (bumps patch version)
release:
    #!/usr/bin/env bash
    set -euo pipefail
    latest=$(git describe --tags --abbrev=0 2>/dev/null || echo "v0.0.0")
    echo "Current version: $latest"
    version=${latest#v}
    IFS='.' read -r major minor patch <<< "$version"
    new_patch=$((patch + 1))
    new_version="v${major}.${minor}.${new_patch}"
    echo "New version: $new_version"
    git tag -a "$new_version" -m "Release $new_version"
    git push origin "$new_version"
    echo "Released $new_version"

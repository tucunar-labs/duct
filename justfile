set shell := ["bash", "-euo", "pipefail", "-c"]

# duckdb-go exposes Arrow only with this tag (needed by modules/engine-duckdb).
export GOFLAGS := "-tags=duckdb_arrow"

default:
    @just --list

# Fast gate: generate, format, vet, lint, short tests (+ Python if present).
check: generate-check fmt-check vet lint test python-check

# templ + Tailwind (modules/web); needs `mise install` for both tools.
generate:
    go generate ./...

# Run generate and fail if it changed any generated file that is checked in
# (*_templ.go, static/app.css), i.e. someone forgot to regenerate.
generate-check:
    #!/usr/bin/env bash
    set -euo pipefail
    snap() { find . -type f \( -name '*_templ.go' -o -path './modules/web/static/app.css' \) -print0 | sort -z | xargs -0 -r sha256sum; }
    before=$(snap)
    go generate ./...
    after=$(snap)
    if [ "$before" != "$after" ]; then
        echo "generated files were stale; commit the regenerated ones:"
        diff <(echo "$before") <(echo "$after") || true
        exit 1
    fi

# Rebuild modules/web/static/editor.js (CodeMirror 6 via esbuild-as-library;
# fetches the npm tarballs pinned and checksummed in tools/bundle/web/package-lock.json).
bundle:
    go run ./tools/bundle

fmt:
    gofmt -w .

fmt-check:
    @test -z "$(gofmt -l .)" || { echo "gofmt needed:"; gofmt -l .; exit 1; }

vet:
    go vet ./...

lint:
    @if command -v golangci-lint >/dev/null; then golangci-lint run ./...; else echo "golangci-lint not installed, skipping"; fi

test:
    go test ./... -short -timeout 3m

# Placeholder: tests tagged `integration` (testcontainers) live in test/integration.
test-integration:
    go test -tags "integration duckdb_arrow" ./test/integration/...

# examples/demo as an e2e test with the real binary (ODBC step needs podman, unixODBC
# and psqlODBC; it is skipped when missing or with DUCT_E2E_NO_ODBC=1).
e2e:
    go test -tags "e2e duckdb_arrow" ./test/e2e/... -run TestDemo -v -count=1 -timeout 20m

python-check:
    @if [ -d python ] && command -v uv >/dev/null; then cd python && uv run ruff check; else echo "no python/ or uv, skipping"; fi

build:
    mkdir -p bin
    go build -ldflags "-X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" -o bin/duct ./cmd/duct

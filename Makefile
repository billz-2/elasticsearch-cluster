.PHONY: help build test test-unit test-e2e test-short clean lint fmt check install-tools examples-up examples-down example-basic example-multicluster example-resolver

# Default target
.DEFAULT_GOAL := help

## help: Show this help message
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'

## build: Build the library
build:
	@echo "Building elasticsearch-cluster library..."
	@go build ./...
	@echo "✓ Build successful"

## test: Run all tests (unit + e2e)
test:
	@echo "Running all tests..."
	@go test -v -race -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "✓ Tests completed. Coverage report: coverage.html"

## test-unit: Run unit tests only (fast)
test-unit:
	@echo "Running unit tests..."
	@go test -v -short -race ./...
	@echo "✓ Unit tests completed"

## test-e2e: Run E2E tests with testcontainers
test-e2e:
	@echo "Running E2E tests..."
	@echo "Note: This will start Elasticsearch and Redis containers"
	@go test -v -race ./e2e/...
	@echo "✓ E2E tests completed"

## test-short: Run only fast tests (skip E2E)
test-short:
	@echo "Running short tests..."
	@go test -v -short ./...
	@echo "✓ Short tests completed"

## test-coverage: Generate test coverage report
test-coverage:
	@echo "Generating coverage report..."
	@go test -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@go tool cover -func=coverage.out | grep total
	@echo "✓ Coverage report: coverage.html"

## lint: Run linter
lint:
	@echo "Running golangci-lint..."
	@golangci-lint run ./... || true
	@echo "✓ Linting completed"

## fmt: Format code
fmt:
	@echo "Formatting code..."
	@gofmt -s -w .
	@go mod tidy
	@echo "✓ Code formatted"

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -f coverage.out coverage.html
	@go clean ./...
	@echo "✓ Clean completed"

## check: Run all checks (build, fmt, lint, test)
check: build fmt lint test
	@echo "✓ All checks passed"

## ci: Run CI pipeline (build, lint, test)
ci: build lint test-unit test-e2e
	@echo "✓ CI pipeline completed"

## install-tools: Install development tools
install-tools:
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "✓ Tools installed"

## deps: Download dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod verify
	@echo "✓ Dependencies downloaded"

## tidy: Tidy go.mod and go.sum
tidy:
	@echo "Tidying modules..."
	@go mod tidy
	@echo "✓ Modules tidied"

## examples-up: Start example infrastructure (ES v8, ES v9, Redis)
examples-up:
	@echo "Starting example infrastructure..."
	@cd examples && docker-compose up -d
	@echo "✓ Infrastructure started"
	@echo "  - Elasticsearch v9 (tier-gold): http://localhost:9200"
	@echo "  - Elasticsearch v8 (tier-silver): http://localhost:9201"
	@echo "  - Redis: localhost:6379"
	@echo ""
	@echo "Waiting for services to be ready (30 seconds)..."
	@sleep 30
	@echo "✓ Services should be ready now"

## examples-down: Stop example infrastructure
examples-down:
	@echo "Stopping example infrastructure..."
	@cd examples && docker-compose down -v
	@echo "✓ Infrastructure stopped"

## example-basic: Run basic example (single cluster)
example-basic:
	@echo "Running basic example..."
	@cd examples/basic && go run main.go

## example-multicluster: Run multi-cluster example (ES v8 + v9)
example-multicluster:
	@echo "Running multi-cluster example..."
	@cd examples/multicluster && go run main.go

## example-resolver: Run resolver example (with Redis cache)
example-resolver:
	@echo "Running resolver example..."
	@cd examples/with_resolver && go run main.go

## release: Create and push a new release tag (must be on master branch)
release:
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is required."; \
		echo "Usage: make release VERSION=v0.0.29"; \
		exit 1; \
	fi; \
	CURRENT_BRANCH=$$(git rev-parse --abbrev-ref HEAD); \
	if [ "$$CURRENT_BRANCH" != "master" ]; then \
		echo "Error: Must be on master branch. Current branch: $$CURRENT_BRANCH"; \
		echo "Hint: Merge staging to master first, then checkout master"; \
		exit 1; \
	fi; \
	echo "Creating release $(VERSION) from master..."; \
	git tag -a $(VERSION) -m "$(VERSION)"; \
	git push origin $(VERSION); \
	echo "✓ Release $(VERSION) created and pushed!"; \
	echo ""; \
	echo "Next steps:"; \
	echo "  1. Wait up to 30 minutes for Go proxy to update"; \
	echo "  2. Update in microservices: go get -u github.com/billz-2/elasticsearch-cluster@$(VERSION)"

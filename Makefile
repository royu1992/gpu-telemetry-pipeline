# ==============================================================================
# GPU Telemetry Pipeline — Makefile
# ==============================================================================
#
# Quick-start commands:
#   make build            – compile all four Go binaries
#   make test             – run the full unit-test suite
#   make coverage         – run tests and open an HTML coverage report
#   make compose-up       – start the full stack locally with docker-compose
#   make k8s-up           – create a kind cluster and install all Helm charts
#   make generate-openapi – regenerate OpenAPI specs into docs/api/ from swag annotations
#
# All docker targets default to building for the local registry unless
# REGISTRY is overridden:
#   make docker-build REGISTRY=ghcr.io/myorg TAG=v1.2.3

# ── Configurable variables ─────────────────────────────────────────────────────
REGISTRY     ?= localhost:5001
TAG          ?= latest
NAMESPACE    ?= gpu-telemetry
CLUSTER_NAME ?= gpu-telemetry
MODULE       := github.com/royu1992/gpu-telemetry-pipeline

BIN := bin

# ── OS-portable helpers ─────────────────────────────────────────────────────
# On Windows, GNU Make invokes cmd.exe; on Unix/macOS it uses /bin/sh.
# All macros below abstract away shell differences so every target works on
# both platforms without modification.
ifeq ($(OS),Windows_NT)
    # File / directory operations
    MKDIR        = powershell -Command "New-Item -ItemType Directory -Force -Path $(1) | Out-Null"
    RMBIN        = powershell -Command "Remove-Item -Recurse -Force $(BIN),coverage.out,coverage.html -ErrorAction SilentlyContinue; exit 0"
    RMRF_DOCS    = powershell -Command "Remove-Item -Recurse -Force docs/api -ErrorAction SilentlyContinue; exit 0"
    # Tool installation
    INSTALL_SWAG = powershell -Command "if (-not (Get-Command swag -ErrorAction SilentlyContinue)) { go install github.com/swaggo/swag/cmd/swag@v1.16.4 }"
    INSTALL_LINT = powershell -Command "if (-not (Get-Command golangci-lint -ErrorAction SilentlyContinue)) { go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest }"
    # Helm chart loop — PowerShell equivalent of 'for chart in charts/*/'
    HELM_LINT_ALL = powershell -Command "Get-ChildItem -Directory charts | ForEach-Object { Write-Host ('==> Linting ' + $$_.FullName); helm lint $$_.FullName; if ($$LASTEXITCODE -ne 0) { exit 1 } }"
else
    # File / directory operations
    MKDIR        = mkdir -p $(1)
    RMBIN        = rm -rf $(BIN) coverage.out coverage.html
    RMRF_DOCS    = rm -rf docs/api
    # Tool installation
    INSTALL_SWAG = command -v swag > /dev/null 2>&1 || go install github.com/swaggo/swag/cmd/swag@v1.16.4
    INSTALL_LINT = command -v golangci-lint > /dev/null 2>&1 || \
        curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh \
        | sh -s -- -b $$(go env GOPATH)/bin
    # Helm chart loop
    HELM_LINT_ALL = for chart in charts/*/; do echo "==> Linting $$chart"; helm lint "$$chart" || exit 1; done
endif

# ── Phony target declarations ─────────────────────────────────────────────────
.PHONY: help \
        build build-local \
        test test-short coverage \
        generate-openapi \
        generate-openapi-gateway generate-openapi-message-queue \
        generate-openapi-collector generate-openapi-streamer \
        docker-build docker-push \
        docker-build-api-gateway docker-build-collector \
        docker-build-message-queue docker-build-streamer \
        compose-up compose-down compose-logs compose-ps \
		kind-create kind-delete kind-load \
		helm-lint helm-install helm-uninstall helm-upgrade \
        k8s-up k8s-down \
        fmt vet lint \
        clean

# ==============================================================================
# Help
# ==============================================================================

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-30s\033[0m %s\n", $$1, $$2}'

# ==============================================================================
# Go build
# ==============================================================================

build: ## Build all service binaries (linux/amd64) into ./bin
	$(call MKDIR,$(BIN))
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o $(BIN)/api-gateway   ./cmd/api-gateway
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o $(BIN)/collector     ./cmd/collector
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o $(BIN)/message-queue ./cmd/message_queue
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o $(BIN)/streamer      ./cmd/streamer

build-local: ## Build binaries for the host OS (skips cross-compilation)
	$(call MKDIR,$(BIN))
	go build -o $(BIN)/api-gateway   ./cmd/api-gateway
	go build -o $(BIN)/collector     ./cmd/collector
	go build -o $(BIN)/message-queue ./cmd/message_queue
	go build -o $(BIN)/streamer      ./cmd/streamer

# ==============================================================================
# Tests & coverage
# ==============================================================================

test: ## Run the full unit-test suite with race detection
	go test -v -race ./...

test-short: ## Run tests, skipping integration / slow tests
	go test -short -race ./...

coverage: ## Run tests and produce coverage.out + coverage.html
	go test -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	@echo ""
	@echo "HTML report written to coverage.html"

# ==============================================================================
# OpenAPI spec generation
#
# swag (https://github.com/swaggo/swag) parses @Summary/@Param/@Router
# comments in each service's source and emits a per-service OpenAPI 3 spec.
# The tool is installed on demand so contributors do not need it pre-installed.
#
# Output files (YAML + generated Go package, one canonical location each):
#   docs/api/api-gateway/   – API Gateway spec + Go package
#   docs/api/message-queue/ – Message Queue spec + Go package
#   docs/api/collector/     – Collector spec + Go package
#   docs/api/streamer/      – Streamer spec + Go package
# ==============================================================================

generate-openapi: ## Regenerate OpenAPI specs for all four services
	$(INSTALL_SWAG)
	$(RMRF_DOCS)
	swag init \
		--generalInfo main.go \
		--dir cmd/api-gateway,internal/api-gateway/server,internal/api-gateway/cache,internal/api-gateway/metrics,internal/store \
		--output docs/api/api-gateway \
		--outputTypes go,yaml \
		--instanceName openapi_api_gateway \
		--parseInternal
	swag init \
		--generalInfo main.go \
		--dir cmd/message_queue,internal/message_queue/queue,internal/message_queue/model,internal/model \
		--output docs/api/message-queue \
		--outputTypes go,yaml \
		--instanceName openapi_message_queue \
		--parseInternal
	swag init \
		--generalInfo main.go \
		--dir cmd/collector,internal/collector/server,internal/collector/metrics \
		--output docs/api/collector \
		--outputTypes go,yaml \
		--instanceName openapi_collector \
		--parseInternal
	swag init \
		--generalInfo main.go \
		--dir cmd/streamer,internal/streamer/server,internal/streamer/metrics \
		--output docs/api/streamer \
		--outputTypes go,yaml \
		--instanceName openapi_streamer \
		--parseInternal
	@echo "OpenAPI specs written to docs/api/"

generate-openapi-gateway: ## Regenerate only the API Gateway spec
	$(INSTALL_SWAG)
	swag init --generalInfo main.go --dir cmd/api-gateway,internal/api-gateway/server,internal/api-gateway/cache,internal/api-gateway/metrics,internal/store --output docs/api/api-gateway --outputTypes go,yaml --instanceName openapi_api_gateway --parseInternal

generate-openapi-message-queue: ## Regenerate only the Message Queue spec
	$(INSTALL_SWAG)
	swag init --generalInfo main.go --dir cmd/message_queue,internal/message_queue/queue,internal/message_queue/model,internal/model --output docs/api/message-queue --outputTypes go,yaml --instanceName openapi_message_queue --parseInternal

generate-openapi-collector: ## Regenerate only the Collector spec
	$(INSTALL_SWAG)
	swag init --generalInfo main.go --dir cmd/collector,internal/collector/server --output docs/api/collector --outputTypes go,yaml --instanceName openapi_collector --parseInternal

generate-openapi-streamer: ## Regenerate only the Streamer spec
	$(INSTALL_SWAG)
	swag init --generalInfo main.go --dir cmd/streamer,internal/streamer/server --output docs/api/streamer --outputTypes go,yaml --instanceName openapi_streamer --parseInternal

# ==============================================================================
# Docker
# ==============================================================================

docker-build: ## Build Docker images for all four services
	docker build -t $(REGISTRY)/api-gateway:$(TAG)   -f build/api-gateway/Dockerfile   .
	docker build -t $(REGISTRY)/collector:$(TAG)     -f build/collector/Dockerfile     .
	docker build -t $(REGISTRY)/message-queue:$(TAG) -f build/message-queue/Dockerfile .
	docker build -t $(REGISTRY)/streamer:$(TAG)      -f build/streamer/Dockerfile      .

docker-push: docker-build ## Push Docker images to REGISTRY
	docker push $(REGISTRY)/api-gateway:$(TAG)
	docker push $(REGISTRY)/collector:$(TAG)
	docker push $(REGISTRY)/message-queue:$(TAG)
	docker push $(REGISTRY)/streamer:$(TAG)

docker-build-api-gateway: ## Build only the api-gateway image
	docker build -t $(REGISTRY)/api-gateway:$(TAG) -f build/api-gateway/Dockerfile .

docker-build-collector: ## Build only the collector image
	docker build -t $(REGISTRY)/collector:$(TAG) -f build/collector/Dockerfile .

docker-build-message-queue: ## Build only the message-queue image
	docker build -t $(REGISTRY)/message-queue:$(TAG) -f build/message-queue/Dockerfile .

docker-build-streamer: ## Build only the streamer image
	docker build -t $(REGISTRY)/streamer:$(TAG) -f build/streamer/Dockerfile .

# ==============================================================================
# docker-compose (local smoke-test)
# ==============================================================================

compose-up: ## Build images and start all services (detached)
	docker compose up --build -d

compose-down: ## Stop all services and remove volumes
	docker compose down -v

compose-logs: ## Follow logs for all services
	docker compose logs -f

compose-ps: ## Show the status of all compose services
	docker compose ps

# ==============================================================================
# kind (Kubernetes in Docker)
# ==============================================================================

kind-create: ## Create a local kind cluster from kind-config.yaml
	kind create cluster --config kind-config.yaml --name $(CLUSTER_NAME)
	kubectl cluster-info --context kind-$(CLUSTER_NAME)

kind-delete: ## Delete the local kind cluster
	kind delete cluster --name $(CLUSTER_NAME)

kind-load: docker-build ## Build and load all images into the kind cluster
	kind load docker-image $(REGISTRY)/api-gateway:$(TAG)   --name $(CLUSTER_NAME)
	kind load docker-image $(REGISTRY)/collector:$(TAG)     --name $(CLUSTER_NAME)
	kind load docker-image $(REGISTRY)/message-queue:$(TAG) --name $(CLUSTER_NAME)
	kind load docker-image $(REGISTRY)/streamer:$(TAG)      --name $(CLUSTER_NAME)

# ==============================================================================
# Kubernetes helpers
# ==============================================================================

# `k8s-create-csv-configmap` target removed — CSV is now included inside the
# `streamer` runtime image, so creating a large ConfigMap is no longer needed.

# ==============================================================================
# Helm
# ==============================================================================

helm-lint: ## Validate all Helm chart syntax
	$(HELM_LINT_ALL)

helm-install: ## Install all charts into the cluster (waits for each)
	kubectl create namespace $(NAMESPACE) --dry-run=client -o yaml | kubectl apply -f -
	helm upgrade --install postgres      charts/postgres      -n $(NAMESPACE) --wait
	helm upgrade --install message-queue charts/message-queue -n $(NAMESPACE) --wait --set image.repository=$(REGISTRY)/message-queue --set image.tag=$(TAG)
	helm upgrade --install collector     charts/collector     -n $(NAMESPACE) --wait --set image.repository=$(REGISTRY)/collector --set image.tag=$(TAG)
	helm upgrade --install streamer      charts/streamer      -n $(NAMESPACE) --wait --set image.repository=$(REGISTRY)/streamer --set image.tag=$(TAG)
	helm upgrade --install api-gateway   charts/api-gateway   -n $(NAMESPACE) --wait --set image.repository=$(REGISTRY)/api-gateway --set image.tag=$(TAG)
	@echo ""
	@echo "All components are running. API Gateway is available at http://localhost:9090"
	@echo "Run: curl http://localhost:9090/gpus"

helm-uninstall: ## Uninstall all Helm releases from the cluster
	helm uninstall api-gateway   -n $(NAMESPACE) || true
	helm uninstall streamer      -n $(NAMESPACE) || true
	helm uninstall collector     -n $(NAMESPACE) || true
	helm uninstall message-queue -n $(NAMESPACE) || true
	helm uninstall postgres      -n $(NAMESPACE) || true

helm-upgrade: ## Upgrade all releases with current chart values
	helm upgrade postgres      charts/postgres      -n $(NAMESPACE)
	helm upgrade message-queue charts/message-queue -n $(NAMESPACE)
	helm upgrade collector     charts/collector     -n $(NAMESPACE)
	helm upgrade streamer      charts/streamer      -n $(NAMESPACE)
	helm upgrade api-gateway   charts/api-gateway   -n $(NAMESPACE)

# ==============================================================================
# Full local Kubernetes workflow
# ==============================================================================

k8s-up: kind-create kind-load helm-install ## One-shot: create cluster, load images, install charts

k8s-down: kind-delete ## Tear down the kind cluster entirely

# ==============================================================================
# Code quality
# ==============================================================================

fmt: ## Format all Go source files
	go fmt ./...

vet: ## Run go vet across the module
	go vet ./...

lint: ## Run golangci-lint (installs if absent)
	$(INSTALL_LINT)
	golangci-lint run ./...

# ==============================================================================
# Clean
# ==============================================================================

clean: ## Remove compiled binaries and coverage files
	$(RMBIN)

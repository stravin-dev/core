GO := ./scripts/go.sh
DAEMONS := edge node pack ctrl hook
BINARIES := $(DAEMONS) admin
FLOCI_TEST_TOOLS := floci-test-s3-sync floci-test-ddb-sync
BIN_DIR := bin
PRE_COMMIT ?= $(shell if command -v pre-commit >/dev/null 2>&1; then echo pre-commit; elif command -v asdf >/dev/null 2>&1; then echo "asdf exec pre-commit"; else echo pre-commit; fi)
DOCKER_COMPOSE ?= $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; else echo "docker-compose"; fi)
HOST_GOOS ?= $(shell uname -s | tr '[:upper:]' '[:lower:]')
HOST_GOARCH ?= $(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')

.PHONY: test e2e vet lint build build-loadgen build-chaos load-test chaos-up chaos-down chaos-e2e docker-build build-floci-test-tools multi-region-up multi-region-down multi-region-e2e clean pre-commit install-hooks

DEVS ?= 4
AGENTS ?= 6
DURATION ?= 30s
HTTP_URL ?= http://127.0.0.1:8080
SSH_ADDR ?= 127.0.0.1:2222
SSH_KEY ?=
TENANT ?= local
REPOS ?= 3
AUTH_TOKEN ?= dev:stravin-local-dev-token
OTEL_ENDPOINT ?=
SEED ?= 0
SCENARIOS ?=
LOADGEN_ARGS ?=

test:
	$(GO) test -race -v ./...

e2e:
	./scripts/e2e.sh

vet:
	$(GO) vet ./...

lint: vet
	@if command -v gofmt >/dev/null 2>&1; then \
		unformatted="$$(gofmt -l .)"; \
	else \
		unformatted="$$(docker run --rm -v "$(CURDIR):/workspace" -w /workspace $${STRAVIN_GO_IMAGE:-golang:1.26-bookworm} gofmt -l .)"; \
	fi; \
	if [ -n "$$unformatted" ]; then \
		echo "Unformatted Go files:"; \
		echo "$$unformatted"; \
		exit 1; \
	fi

build:
	@mkdir -p $(BIN_DIR)
	@for d in $(BINARIES); do \
		echo "Building stravin-$$d..."; \
		$(GO) build -o $(BIN_DIR)/stravin-$$d ./cmd/stravin-$$d; \
	done

build-loadgen:
	@mkdir -p $(BIN_DIR)
	@echo "Building stravin-loadgen ($(HOST_GOOS)/$(HOST_GOARCH))..."
	CGO_ENABLED=0 GOOS=$(HOST_GOOS) GOARCH=$(HOST_GOARCH) $(GO) build -o $(BIN_DIR)/stravin-loadgen ./cmd/stravin-loadgen

build-chaos:
	@mkdir -p $(BIN_DIR)
	@echo "Building stravin-chaos ($(HOST_GOOS)/$(HOST_GOARCH))..."
	CGO_ENABLED=0 GOOS=$(HOST_GOOS) GOARCH=$(HOST_GOARCH) $(GO) build -o $(BIN_DIR)/stravin-chaos ./cmd/stravin-chaos

load-test: build-loadgen
	./$(BIN_DIR)/stravin-loadgen \
		--devs $(DEVS) \
		--agents $(AGENTS) \
		--duration $(DURATION) \
		--http-url "$(HTTP_URL)" \
		--ssh-addr "$(SSH_ADDR)" \
		$(if $(SSH_KEY),--ssh-key "$(SSH_KEY)",) \
		--tenant "$(TENANT)" \
		--repos $(REPOS) \
		--auth-token "$(AUTH_TOKEN)" \
		--otel-endpoint "$(OTEL_ENDPOINT)" \
		--seed $(SEED) \
		$(if $(SCENARIOS),--scenarios "$(SCENARIOS)",) \
		$(LOADGEN_ARGS)

chaos-up:
	$(DOCKER_COMPOSE) -f docker-compose.chaos.yml $(if $(filter 1 true yes,$(TELEMETRY)),--profile telemetry,) up -d --build

chaos-down:
	$(DOCKER_COMPOSE) -f docker-compose.chaos.yml --profile telemetry down -v --remove-orphans

chaos-e2e:
	./scripts/chaos-e2e.sh

docker-build:
	@for d in $(DAEMONS); do \
		echo "Building Docker target $$d..."; \
		docker build --target $$d -t stravin-$$d:local .; \
	done

build-floci-test-tools:
	@for tool in $(FLOCI_TEST_TOOLS); do \
		echo "Building Floci test tool Docker target $$tool..."; \
		docker build -f Dockerfile.floci-test --target $$tool -t $$tool:local .; \
	done

multi-region-up:
	$(DOCKER_COMPOSE) -f docker-compose.multi-region.yml up -d --build

multi-region-down:
	$(DOCKER_COMPOSE) -f docker-compose.multi-region.yml down -v --remove-orphans

multi-region-e2e:
	./scripts/multi-region-e2e.sh

clean:
	rm -rf $(BIN_DIR)

install-hooks:
	$(PRE_COMMIT) install

pre-commit:
	$(PRE_COMMIT) run --all-files

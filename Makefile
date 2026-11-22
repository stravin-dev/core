GO := ./scripts/go.sh
DAEMONS := edge node pack ctrl hook
BINARIES := $(DAEMONS) admin
FLOCI_TEST_TOOLS := floci-test-s3-sync floci-test-ddb-sync
BIN_DIR := bin
PRE_COMMIT ?= $(shell if command -v pre-commit >/dev/null 2>&1; then echo pre-commit; elif command -v asdf >/dev/null 2>&1; then echo "asdf exec pre-commit"; else echo pre-commit; fi)
DOCKER_COMPOSE ?= $(shell if docker compose version >/dev/null 2>&1; then echo "docker compose"; else echo "docker-compose"; fi)

.PHONY: test e2e vet lint build docker-build build-floci-test-tools multi-region-up multi-region-down multi-region-e2e clean pre-commit install-hooks

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
		unformatted="$$(docker run --rm -v "$(CURDIR):/workspace" -w /workspace $${STRAVIN_GO_IMAGE:-golang:1.24-bookworm} gofmt -l .)"; \
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

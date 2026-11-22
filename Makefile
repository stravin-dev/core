GO := ./scripts/go.sh
DAEMONS := edge node pack ctrl hook
BIN_DIR := bin

.PHONY: test vet lint build docker-build clean

test:
	$(GO) test -race -v ./...

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
	@for d in $(DAEMONS); do \
		echo "Building stravin-$$d..."; \
		$(GO) build -o $(BIN_DIR)/stravin-$$d ./cmd/stravin-$$d; \
	done

docker-build:
	@for d in $(DAEMONS); do \
		echo "Building Docker target $$d..."; \
		docker build --target $$d -t stravin-$$d:local .; \
	done

clean:
	rm -rf $(BIN_DIR)

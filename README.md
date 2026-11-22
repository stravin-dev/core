# core

Stateless, multi-region Git storage engine in Go backed by S3 and DynamoDB.

## Local Development Setup

Tool versions are pinned in `.tool-versions` for [asdf](https://asdf-vm.com/):

```bash
asdf install
```

### Pre-commit Hooks

Install the Git pre-commit hooks:

```bash
make install-hooks
# or: pre-commit install
```

Run all pre-commit checks across the repository:

```bash
make pre-commit
# or: pre-commit run --all-files
```

Hooks run Go formatting (`gofmt`, `goimports`), static analysis (`go vet`, `golangci-lint`), `go mod tidy -diff`, and repository hygiene checks. If `go` or `golangci-lint` are not installed on the host (or when `STRAVIN_FORCE_DOCKER_GO=1`), the hooks transparently fall back to Docker (`golang:1.24-bookworm` via `./scripts/go.sh` and `golangci/golangci-lint:v2.15.0`).

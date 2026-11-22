# syntax=docker/dockerfile:1

FROM golang:1.24-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/stravin-edge ./cmd/stravin-edge \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/stravin-node ./cmd/stravin-node \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/stravin-pack ./cmd/stravin-pack \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/stravin-ctrl ./cmd/stravin-ctrl \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/stravin-hook ./cmd/stravin-hook

FROM alpine:3.21 AS runtime-base
RUN apk add --no-cache ca-certificates \
 && addgroup -S stravin \
 && adduser -S -G stravin stravin

FROM runtime-base AS runtime-git
# Install git binary for GPLv2 OS subprocess boundary (never linked via cgo).
RUN apk add --no-cache git

FROM runtime-base AS edge
COPY --from=builder /out/stravin-edge /usr/local/bin/stravin-edge
USER stravin
EXPOSE 2222 8080
ENTRYPOINT ["/usr/local/bin/stravin-edge"]

FROM runtime-git AS node
COPY --from=builder /out/stravin-node /usr/local/bin/stravin-node
USER stravin
EXPOSE 8081 9090
ENTRYPOINT ["/usr/local/bin/stravin-node"]

FROM runtime-git AS pack
COPY --from=builder /out/stravin-pack /usr/local/bin/stravin-pack
USER stravin
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/stravin-pack"]

FROM runtime-base AS ctrl
COPY --from=builder /out/stravin-ctrl /usr/local/bin/stravin-ctrl
USER stravin
EXPOSE 8083
ENTRYPOINT ["/usr/local/bin/stravin-ctrl"]

FROM runtime-base AS hook
COPY --from=builder /out/stravin-hook /usr/local/bin/stravin-hook
USER stravin
EXPOSE 8084
ENTRYPOINT ["/usr/local/bin/stravin-hook"]

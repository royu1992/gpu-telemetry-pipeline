# ─── Stage 1: Build ────────────────────────────────────────────────────────
FROM golang:1.25-alpine3.21 AS builder
WORKDIR /workspace

# Cache dependency downloads as a separate layer so rebuilds are fast when
# only application source files change.
COPY go.mod go.sum ./
RUN go mod download

# Copy the full source tree and compile a fully static binary.
# CGO_ENABLED=0 removes any C-library dependency; GOARCH matches the cluster.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
    -o /out/api-gateway ./cmd/api-gateway

# ─── Stage 2: Runtime ───────────────────────────────────────────────────────
FROM alpine:3.21

# wget is used for container health-checks in docker-compose.
RUN apk add --no-cache wget ca-certificates tzdata && \
    addgroup -S app && adduser -S app -G app

WORKDIR /app
COPY --from=builder /out/api-gateway .

USER app
EXPOSE 9090
ENTRYPOINT ["/app/api-gateway"]

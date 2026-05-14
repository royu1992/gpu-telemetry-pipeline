# ─── Stage 1: Build ────────────────────────────────────────────────────────
FROM golang:1.25-alpine3.21 AS builder
WORKDIR /workspace

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
    -o /out/streamer ./cmd/streamer

# ─── Stage 2: Runtime ───────────────────────────────────────────────────────
FROM alpine:3.21

RUN apk add --no-cache wget ca-certificates tzdata && \
    addgroup -S app && adduser -S app -G app

WORKDIR /app
COPY --from=builder /out/streamer .
# Include the sample CSV inside the runtime image so Kind deployments do not
# require hostPath mounts or ConfigMaps. Place it under /data to match
# the `STREAMER_CSV_PATH` used by the deployment.
COPY docs/dcgm_metrics_20250718_134233.csv /data/dcgm_metrics_20250718_134233.csv
RUN mkdir -p /data && chown -R app:app /data

USER app
EXPOSE 8081
ENTRYPOINT ["/app/streamer"]

# ── Build stage ────────────────────────────────────────────────────────
FROM golang:1.26-alpine AS builder

# gcc + musl-dev only needed if CGO is ever re-enabled
RUN apk add --no-cache ca-certificates

WORKDIR /src

# Cache dependency downloads in a separate layer
COPY cmd/go.mod cmd/go.sum ./
RUN go mod download

# Copy source and build a static binary
COPY cmd/ .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -trimpath -o /drainpipe .

# ── Runtime stage ──────────────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot

LABEL org.opencontainers.image.title="drainpipe" \
      org.opencontainers.image.description="Steampipe export to PostgreSQL — drain cloud resources into your own database" \
      org.opencontainers.image.source="https://github.com/justmiles/drainpipe"

COPY --from=builder /drainpipe /drainpipe

USER nonroot:nonroot

ENTRYPOINT ["/drainpipe"]
CMD ["drain"]

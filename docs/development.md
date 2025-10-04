# Development

## Prerequisites

- **Go 1.25+** (via [devbox](https://www.jetify.com/devbox) or system install)
- **Docker** (for PostgreSQL)
- **AWS credentials** configured (`~/.aws/config` or environment)

## Devbox Setup

This project uses [Devbox](https://www.jetify.com/devbox) to manage Go, GCC, Steampipe, and Powerpipe in an isolated environment.

```bash
devbox shell
```

## Building

```bash
devbox run build
```

This compiles the Go binary to `./bin/drainpipe`.

## Development Stack

The `docker-compose.yml` provides PostgreSQL and pgweb for local development:

```bash
docker compose up -d      # Start PostgreSQL + pgweb
docker compose down        # Stop
```

| Service | URL / Connection |
|---------|-----------------|
| PostgreSQL | `localhost:5432` (user: `cmdb`, password: `cmdb_dev`, db: `cmdb`) |
| pgweb UI | http://localhost:54654 |

## Running

```bash
# Quick drain with devbox
devbox run drain

# Or run the binary directly
AWS_PROFILE=my-profile AWS_REGIONS=us-east-1 ./bin/drainpipe drain
```

## Testing

```bash
# Unit tests
devbox run test:unit

# Integration tests (requires Docker Compose stack)
devbox run test:integration
```

## Docker Image

Build and run Drainpipe as a Docker container:

```bash
docker build -t drainpipe .

docker run --rm \
  -e DB_HOST=host.docker.internal \
  -e AWS_PROFILE=my-profile \
  drainpipe drain
```

## Project Structure

```
drainpipe/
├── cmd/
│   ├── main.go                          # CLI entry point + worker pool orchestration
│   ├── internal/
│   │   ├── config/
│   │   │   ├── config.go               # Database config from env
│   │   │   └── drainpipe.go            # YAML config loader
│   │   ├── exporter/exporter.go         # Steampipe plugin wrapper
│   │   ├── importer/importer.go         # Staging-table import pattern
│   │   ├── match/match.go              # Glob table matching
│   │   ├── provider/
│   │   │   ├── provider.go             # Provider interface + registry
│   │   │   └── aws.go                  # AWS provider + org + SSO support
│   │   └── schema/schema.go            # Dynamic PG schema management
│   ├── go.mod
│   └── go.sum
├── docs/                                # Documentation
├── example.yaml                         # Annotated example config
├── docker-compose.yml
├── devbox.json
├── Dockerfile
├── LICENSE                              # AGPL-3.0
└── README.md
```

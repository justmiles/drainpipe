# Development

## Prerequisites

- **Go 1.25+** (via [devbox](https://www.jetify.com/devbox) or system install)
- **Docker** (for PostgreSQL)
- **Steampipe plugin binaries** (downloaded automatically or installed via `steampipe plugin install`)

## Devbox Setup

This project uses [Devbox](https://www.jetify.com/devbox) to manage Go, GCC, Steampipe, and Powerpipe in an isolated environment.

```bash
devbox shell
```

## Building

```bash
devbox run build
```

This compiles the Go binary to `./bin/drainpipe`. The binary is a **thin host** — it does not embed any Steampipe plugins. Plugins are loaded as separate processes at runtime.

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

### Plugin Binaries

Drainpipe needs Steampipe plugin binaries at runtime. They are resolved in this order:

1. **Drainpipe cache** — `~/.drainpipe/plugins/<org>/<name>/<version>/`
2. **Steampipe install** — `~/.steampipe/plugins/hub.steampipe.io/plugins/<org>/<name>@<version>/`
3. **GitHub download** — Automatic download from GitHub Releases (requires explicit version, not `latest`)
4. **Explicit path** — Set `plugin_path:` in config

For development, the easiest way to get plugin binaries is via Steampipe:

```bash
steampipe plugin install aws
steampipe plugin install cloudflare
```

## Testing

```bash
# Unit tests
devbox run test:unit

# Integration tests (requires Docker Compose stack)
devbox run test:integration
```

## Project Structure

```
drainpipe/
├── cmd/
│   ├── main.go                          # CLI entry point + worker pool orchestration
│   ├── internal/
│   │   ├── config/
│   │   │   ├── config.go               # Database config from env
│   │   │   └── collector.go            # YAML config loader + known provider defaults
│   │   ├── exporter/exporter.go         # Out-of-process plugin gRPC client
│   │   ├── importer/importer.go         # Staging-table import pattern
│   │   ├── match/match.go              # Glob table matching
│   │   ├── pluginmanager/manager.go     # Plugin binary download/cache/resolution
│   │   ├── provider/
│   │   │   ├── provider.go             # Natural key helpers + multi-account interface
│   │   │   └── aws.go                  # AWS Organizations discovery + STS role assumption
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

## Adding a New Provider

Adding support for a new Steampipe plugin requires **zero Go code**. Create a config block:

```yaml
plugin: turbot/gcp@0.45.0
connection:
  project: my-gcp-project
  credentials: "/path/to/credentials.json"
identity_table: gcp_project
identity_column: project_id
natural_key: self_link
tables:
  - "gcp_compute_*"
```

If the plugin binary is not already installed, Drainpipe will attempt to download it from GitHub Releases.

To add a new **known provider shorthand** (so users can write `provider: gcp` instead of the full plugin block), add an entry to `KnownProviders` in `cmd/internal/config/collector.go`:

```go
var KnownProviders = map[string]ProviderDefaults{
    // ...existing entries...
    "gcp": {
        Plugin:         "turbot/gcp@latest",
        IdentityTable:  "gcp_project",
        IdentityColumn: "project_id",
        NaturalKey:     "self_link",
    },
}
```

## Architecture

See [architecture.md](architecture.md) for details on the out-of-process plugin model, gRPC communication, and data flow.

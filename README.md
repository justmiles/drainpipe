# Drainpipe

Drain cloud resources into PostgreSQL using [Steampipe](https://steampipe.io) plugins.

Drainpipe discovers tables dynamically, creates and evolves your database schema at runtime, and uses a staging-table import pattern for idempotent, account-scoped upserts with soft-delete tracking.

## Install

### Binary

Download the latest release from [GitHub Releases](https://github.com/justmiles/drainpipe/releases):

```bash
# Example for Linux amd64
curl -sL https://github.com/justmiles/drainpipe/releases/latest/download/drainpipe_linux_amd64.tar.gz | tar xz
sudo mv drainpipe /usr/local/bin/
```

### Docker

```bash
docker pull ghcr.io/justmiles/drainpipe:latest
```

## Quickstart

```bash
# Start PostgreSQL
docker compose up -d

# Build
devbox run build

# Drain resources from a single AWS account
AWS_PROFILE=my-profile AWS_REGIONS=us-east-1 ./bin/drainpipe drain

# Browse your data at http://localhost:54654
```

## Documentation

See the [docs/](docs/) directory for detailed guides:

- [Architecture](docs/architecture.md) — how Drainpipe works under the hood
- [Configuration](docs/configuration.md) — YAML config reference, multi-account modes, and filtering
- [CLI Reference](docs/cli.md) — commands, flags, and environment variables
- [Development](docs/development.md) — building, testing, and Docker

## License

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE).

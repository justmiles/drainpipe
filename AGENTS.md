# AGENTS.md

This file provides guidance to AI Agents when working with code in this repository.

## Agents Docs

- Coding Conventions: @docs/agents/CodingConventions.md
- Agents Directive: @docs/agents/OperationalDoctrine.md
- GoDocs: @docs/agents/GoDocs.md

## Behavior Guidance

### Git Operations

- By default, use origin/main as the base for new branches.
- Create branches using the following naming scheme:
    ```
    [user]/[issue number]-[brief description in kebab-case]
    ```
- Git commit messages should be a brief single line message.

### Answering Questions
- When asked a question, consider the answer and perform any exploration of the codebase required to provide a quality answer.
- When asked a question, do not write or modify code. Simply answer the question.

### Communication
- Be direct and straight forward.
- DO NO be overly dramatic or jump to conclusions. e.g. don't say "Critical Memory Safety Issue Found" unless you are certain that is true. If you are not certain, then frame it "Potential Memory Issue Found".
- DO NOT be sycophantic or use unnecessary flattery. Avoid phrases like "You're absolutely right".

## Development environment

This project uses [devbox](https://www.jetify.com/devbox). `devbox shell` is
interactive-only and `devbox run --` doesn't reliably forward arbitrary
commands, so for non-interactive use (agents, scripts), load the devbox
environment into the current shell first:

eval "$(devbox shellenv)" && <command>

## Development Commands

### Building and Testing
- `devbox run test:unit` - Run unit test suite
- `devbox run test:integration` - Run integration tests (requires Docker Compose stack running)
- `devbox run build` - builds ./bin/drainpipe

### Code Quality and Generation
- `devbox run lint` - Run linter (includes tidy and custom checks)
  `lint` supports passing `LINT_PATH`, which sets the path used by golangci-lint
- `devbox run lint -e LINT_PATH=./path/to/lint/...` - Run linter for a specific module

## Architecture Overview

Drainpipe runs Steampipe plugins as **out-of-process child processes**, communicating over gRPC (HashiCorp go-plugin protocol). This isolates each plugin's dependencies and lets you add new providers via config alone — no Go code needed.

```
Config → Plugin Manager → Exporter → Importer → PostgreSQL
                             │            │
                         gRPC call   staging table
                             ▼         upsert
                       steampipe-plugin-<name> (process)
```

**Data flow:**
1. Config loaded (HCL or YAML)
2. Plugin binary resolved: drainpipe cache → steampipe install → GitHub download
3. Plugin launched as child process; `SetAllConnectionConfigs` passes HCL creds
4. `GetSchema` discovers table columns; `Execute` streams rows via gRPC
5. Rows written to staging table; upserted into live table on natural key + `_source_account`
6. Missing rows marked `_deleted_at`; new columns auto-added to live table

**Drainpipe-managed columns** (on every collected table):

| Column | Purpose |
|---|---|
| `_source_account` | Account/subscription/tenant scope |
| `_first_seen_at` | First collection timestamp |
| `_last_seen_at` | Last collection timestamp |
| `_deleted_at` | Set when resource disappears |


**Environment Variables**

| Var | Default |
|---|---|
| `DB_HOST` | `localhost` |
| `DB_PORT` | `5432` |
| `DB_NAME` | `cmdb` |
| `DB_USER` | `cmdb` |
| `DB_PASSWORD` | `cmdb_dev` |
| `DB_SSLMODE` | `disable` |

## Configuration

HCL is the preferred format; YAML is legacy but still supported.

### HCL structure (three block types)

```hcl
# 1. Credentials passed directly to the Steampipe plugin
connection "aws" {
  plugin  = "turbot/aws@latest"
  profile = "my-profile"
  regions = ["us-east-1", "us-west-2"]
}

# 2. Plugin-level settings (memory, rate limiters)
plugin "aws" {
  memory_max_mb = 2048
  limiter "global_concurrency" { max_concurrency = 250 }
}

# 3. Collection job — references a connection
drainpipe "aws_inventory" {
  connection    = "aws"
  concurrency   = 5
  retries       = 3
  retry_delay   = "10s"
  table_timeout = "30m"
  strict        = true

  tables = ["aws_ec2_*", "aws_s3_*"]

  table "aws_ecs_task_definition" {
    where   = { status = "ACTIVE" }
    columns = ["task_definition_arn", "family", "status"]
  }

  table "aws_cost_by_service" {
    key = ["account_id", "region", "service"]   # composite key
  }
}
```

### Per-table options

| Field | Description |
|---|---|
| `key` | Override natural key (list; supports composite) |
| `where` | Server-side equality filters on key columns |
| `columns` | Restrict to this column subset |
| `filter_query { column, query }` | Pre-filter via Postgres query; runs after normal tables |

### Known providers

| Provider | Plugin | Identity Table | Identity Column | Natural Key |
|---|---|---|---|---|
| `aws` | `turbot/aws@latest` | `aws_sts_caller_identity` | `account_id` | `arn` |
| `azure` | `turbot/azure@latest` | `azure_subscription` | `subscription_id` | `id` |
| `cloudflare` | `turbot/cloudflare@latest` | `cloudflare_account` | `id` | `id` |

To add a new known provider shorthand, edit `KnownProviders` in `cmd/internal/config/collector.go`.

### Multi-account modes (AWS)

**SSO / explicit accounts:**
```hcl
drainpipe "multi" {
  connection = "aws"
  accounts { name = "prod";    profile = "prod.ReadOnly" }
  accounts { name = "staging"; profile = "staging.ReadOnly"; regions = ["us-east-1"] }
  tables = ["aws_ec2_instance"]
}
```

**AWS Organizations (STS role assumption):**
```hcl
drainpipe "org" {
  connection = "aws"
  org {
    role_name        = "ReadOnlyCollectorRole"
    admin_account_id = "123456789012"
    override { match_account_names = ["*-dev"]; tables = ["aws_ec2_instance"] }
    override { match_account_ids   = ["999999999999"]; skip = true }
  }
  tables = ["aws_ec2_*"]
}
```

## Plugin Binary Resolution Order

1. `~/.drainpipe/plugins/<org>/<name>/<version>/`
2. `~/.steampipe/plugins/hub.steampipe.io/plugins/<org>/<name>@<version>/`
3. GitHub Releases download (requires explicit version, not `latest`)
4. `plugin_path:` in config (bypasses all resolution)

For development, install via Steampipe: `steampipe plugin install aws`

## Natural Key Resolution (precedence)

1. Per-table `key` in config
2. Drainpipe-level `natural_key` in config (e.g., `arn` for AWS, `id` for Azure)
3. `GetCallKeyColumnList` required columns from plugin schema
4. Known provider defaults (`KnownProviders` map)
5. Tables with none of the above are skipped (or fail in `strict` mode)

## Concurrency & Retry

- Worker pool processes `(account, table)` pairs at configurable concurrency
- Transient errors retry with exponential backoff + jitter within `table_timeout` budget
- `context deadline exceeded` and `context canceled` fail immediately (no retry)
- Progress logged every 30s as structured JSON

## Workflows

### Add a new Steampipe provider

Full instructions in `.agents/workflows/add-steampipe-provider.md`. Summary:

1. Use any supported Steampipe plugin via YAML/HCL config — no Go code required
2. If a **known provider shorthand** is wanted (`provider: gcp`), add to `KnownProviders` in `cmd/internal/config/collector.go`
3. Verify: `go run ./cmd list-providers` and `go run ./cmd list-tables --provider <name>`
4. Test: `go run ./cmd drain --config your.hcl`

### Key Directories


### Special Considerations
- Byzantine fault tolerance is a core design principle
- Cryptographic operations require careful handling (see crypto library docs)
- Performance is critical - prefer efficient data structures and algorithms
- Network messages must be authenticated and validated
- State consistency is paramount - use proper synchronization primitives
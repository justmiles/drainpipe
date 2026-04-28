# Architecture

## Overview

Drainpipe runs Steampipe plugins as **out-of-process child processes**, communicating over gRPC using the [HashiCorp go-plugin](https://github.com/hashicorp/go-plugin) protocol. It queries cloud provider APIs through these plugins and writes the results into PostgreSQL tables that it creates and evolves automatically.

```
┌─────────────────────────────────────────────────────────────────────┐
│  drainpipe (thin host binary)                                       │
│                                                                     │
│  Config ──► Plugin Manager ──► Exporter ──► Importer ──► PostgreSQL │
│  (YAML)     (download /        (gRPC to      (Staging     (Live     │
│              cache binary)      plugin        table        tables)   │
│                                 process)      pattern)               │
│                                    │                                 │
│  Worker Pool (concurrency N)       │ gRPC (hashicorp/go-plugin)     │
│  ┌──────┐ ┌──────┐ ┌──────┐       │                                 │
│  │ W1   │ │ W2   │ │ WN   │       ▼                                 │
│  └──────┘ └──────┘ └──────┘    ┌──────────────────────┐             │
│                                │ steampipe-plugin-aws  │ (process)   │
│                                │ steampipe-plugin-...  │ (process)   │
│                                └──────────────────────┘             │
└─────────────────────────────────────────────────────────────────────┘
```

### Why Out-of-Process?

The previous architecture compiled Steampipe plugins directly into the drainpipe binary via Go imports. This caused:

- **Diamond dependency conflicts** — plugins shared `go.mod`, so one plugin's SDK version broke another (e.g., Cloudflare's S3 SDK vs AWS's S3 SDK).
- **Massive binary** — every plugin and all transitive dependencies baked in.
- **Adding a provider = code change** — new `.go` file, rebuild everything.

The out-of-process model isolates each plugin in its own process with its own dependencies, matching how Steampipe itself works. Adding a new provider is now a YAML config change — no Go code needed.

## Packages

| Package          | Responsibility                                                           |
| ---------------- | ------------------------------------------------------------------------ |
| `pluginmanager`  | Downloads, caches, and resolves Steampipe plugin binaries                |
| `exporter`       | Launches plugin processes and communicates via gRPC for batch export     |
| `importer`       | Staging-table upsert pattern, scoped by `_source_account`               |
| `schema`         | Dynamic PostgreSQL table creation and schema evolution                   |
| `match`          | Glob-based table pattern matching with fuzzy suggestions                 |
| `config`         | Database connection, drainpipe YAML config, and known provider defaults  |
| `provider`       | AWS multi-account discovery (Organizations + STS) and natural key logic  |

## Data Flow

1. **Config** — Drainpipe reads one or more YAML config documents. Each defines a plugin (or provider shorthand), connection settings, tables, and account targets.
2. **Plugin Manager** — Resolves the plugin binary: checks the drainpipe cache (`~/.drainpipe/plugins/`), Steampipe's install directory (`~/.steampipe/plugins/`), or downloads from GitHub Releases.
3. **Exporter** — Launches the plugin binary as a child process using `hashicorp/go-plugin`, connects via gRPC, sets connection config, and streams rows over a channel.
4. **Importer** — Writes rows into a temporary staging table, then performs an upsert into the live table using natural keys. Marks disappeared resources with a `_deleted_at` timestamp.
5. **Schema** — Before each table is collected, ensures the PostgreSQL table exists and adds any new columns discovered from the plugin schema.
6. **Cleanup** — After collection completes, all plugin child processes are terminated.

## Plugin Communication

Drainpipe uses the same gRPC protocol as Steampipe itself. The key SDK components:

| Component | SDK Location | Purpose |
| --- | --- | --- |
| Handshake config | `grpc/shared.Handshake` | Magic cookie for process authentication |
| Plugin map | `grpc/shared.WrapperPlugin` | go-plugin registration entry |
| gRPC client | `grpc.PluginClient` | High-level client wrapping `hashicorp/go-plugin` |
| Proto definitions | `grpc/proto/` | `GetSchema`, `Execute`, `SetAllConnectionConfigs`, etc. |

The exporter calls:
1. `SetAllConnectionConfigs` — pass HCL credentials to the plugin
2. `SetCacheOptions` — disable caching (batch export, not interactive)
3. `GetSchema` — discover table schemas and columns
4. `Execute` — stream rows from the plugin via gRPC

## Plugin Binary Resolution

The plugin manager resolves binaries in this order:

1. **Drainpipe cache** — `~/.drainpipe/plugins/<org>/<name>/<version>/steampipe-plugin-<name>.plugin`
2. **Steampipe install** — `~/.steampipe/plugins/hub.steampipe.io/plugins/<org>/<name>@<version>/steampipe-plugin-<name>.plugin`
3. **GitHub Releases** — Downloads `steampipe-plugin-<name>_<os>_<arch>.gz` from `github.com/<org>/steampipe-plugin-<name>/releases/`
4. **Explicit path** — User can set `plugin_path:` in config to bypass resolution entirely

## Known Provider Defaults

When using the `provider:` shorthand, Drainpipe maps it to default plugin settings:

| Provider | Plugin | Identity Table | Identity Column | Natural Key |
| --- | --- | --- | --- | --- |
| `aws` | `turbot/aws@latest` | `aws_sts_caller_identity` | `account_id` | `arn` |
| `azure` | `turbot/azure@latest` | `azure_subscription` | `subscription_id` | `id` |
| `cloudflare` | `turbot/cloudflare@latest` | `cloudflare_account` | `id` | `id` |

## Drainpipe-Managed Columns

Every row in every table includes these tracking columns, managed by Drainpipe (not the Steampipe plugin):

| Column            | Purpose                                                  |
| ----------------- | -------------------------------------------------------- |
| `_source_account` | Scopes data by AWS account (or Azure subscription, etc.) |
| `_first_seen_at`  | When the resource was first collected                    |
| `_last_seen_at`   | When the resource was last seen                          |
| `_deleted_at`     | Set when a resource disappears from the cloud provider   |

## Natural Key Resolution

Tables need a natural key for upserts (insert-or-update). Resolution is config-driven via the `natural_key` field:

1. **Preferred key** — If the table has a column matching `natural_key` (e.g., `arn` for AWS, `id` for Azure), use it
2. **`GetCallKeyColumnList`** — Fallback: use required key columns from the plugin schema
3. **Unsupported** — Tables with neither are skipped

## Concurrency Model

Drainpipe uses a **table-level worker pool**. Work items are `(account, table)` pairs processed by `N` concurrent workers (configurable via `concurrency`).

Progress is logged every 30 seconds:

```json
{
  "completed": 45,
  "failed": 3,
  "total": 320,
  "percent": 15,
  "message": "progress"
}
```

## Retry Behavior

| Error Type                              | Behavior                                |
| --------------------------------------- | --------------------------------------- |
| Transient (API errors, throttling)      | Retry with exponential backoff + jitter |
| Timeout (`context deadline exceeded`)   | Fail immediately, no retry              |
| Context canceled (Ctrl+C, strict abort) | Fail immediately                        |

The `table_timeout` is a single **overall budget** for all attempts — retries happen within it, not with fresh timeouts.

## AWS Multi-Account

AWS multi-account support (Organizations discovery + STS role assumption) runs in the **host process** using the AWS SDK directly. This is separate from the plugin — the host discovers accounts and generates HCL connection configs with temporary credentials, then passes them to the out-of-process plugin.

This keeps the host binary lightweight (only needs `aws-sdk-go-v2/config`, `organizations`, and `sts`) while the heavy plugin dependencies stay isolated in the plugin process.

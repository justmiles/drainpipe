# Architecture

## Overview

Drainpipe runs Steampipe plugins **in-process** (no external Steampipe daemon), queries cloud provider APIs, and writes the results into PostgreSQL tables that it creates and evolves automatically.

```
┌─────────────────────────────────────────────────────────────────────┐
│  drainpipe                                                          │
│                                                                     │
│  Config ──► Provider ──► Exporter ──► Importer ──► PostgreSQL       │
│  (YAML)     (AWS)        (Steampipe    (Staging     (Live tables    │
│                           plugin,       table        with tracking   │
│                           in-process)   pattern)     columns)        │
│                                                                     │
│  Worker Pool (concurrency N)                                        │
│  ┌──────┐ ┌──────┐ ┌──────┐                                        │
│  │ W1   │ │ W2   │ │ WN   │  ← (account, table) work items         │
│  └──────┘ └──────┘ └──────┘                                        │
└─────────────────────────────────────────────────────────────────────┘
```

## Packages

| Package | Responsibility |
|---------|---------------|
| `provider` | Cloud provider abstraction (credentials, account identity, natural keys) |
| `exporter` | Wraps Steampipe plugins in-process for batch data export |
| `importer` | Staging-table upsert pattern, scoped by `_source_account` |
| `schema` | Dynamic PostgreSQL table creation and schema evolution |
| `match` | Glob-based table pattern matching with fuzzy suggestions |
| `config` | Database connection and drainpipe YAML configuration |

## Data Flow

1. **Config** — Drainpipe reads one or more YAML config documents. Each defines a provider, regions, tables, and account targets.
2. **Provider** — Resolves credentials and account identity. In org mode, discovers member accounts and assumes roles.
3. **Exporter** — Initializes the Steampipe plugin in-process, sets connection config, and streams rows over a channel.
4. **Importer** — Writes rows into a temporary staging table, then performs an upsert into the live table using natural keys. Marks disappeared resources with a `_deleted_at` timestamp.
5. **Schema** — Before each table is collected, ensures the PostgreSQL table exists and adds any new columns discovered from the plugin schema.

## Drainpipe-Managed Columns

Every row in every table includes these tracking columns, managed by Drainpipe (not the Steampipe plugin):

| Column | Purpose |
|--------|---------|
| `_source_account` | Scopes data by AWS account (or Azure subscription, etc.) |
| `_first_seen_at` | When the resource was first collected |
| `_last_seen_at` | When the resource was last seen |
| `_deleted_at` | Set when a resource disappears from the cloud provider |

## Natural Key Resolution

Tables need a natural key for upserts (insert-or-update). For AWS:

1. **`arn`** — preferred when available (globally unique across accounts and regions)
2. **`GetCallKeyColumnList`** — fallback for tables without `arn`
3. **Unsupported** — tables with neither are skipped

## Concurrency Model

Drainpipe uses a **table-level worker pool**. Work items are `(account, table)` pairs processed by `N` concurrent workers (configurable via `concurrency`).

Progress is logged every 30 seconds:

```json
{"completed":45,"failed":3,"total":320,"percent":15,"message":"progress"}
```

## Retry Behavior

| Error Type | Behavior |
|---|---|
| Transient (API errors, throttling) | Retry with exponential backoff + jitter |
| Timeout (`context deadline exceeded`) | Fail immediately, no retry |
| Context canceled (Ctrl+C, strict abort) | Fail immediately |

The `table_timeout` is a single **overall budget** for all attempts — retries happen within it, not with fresh timeouts.

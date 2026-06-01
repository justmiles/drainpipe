# Configuration

Drainpipe supports HCL (recommended) and YAML configuration files. By default, it looks for `drainpipe.hcl` or `drainpipe.yaml` in the current directory.

```bash
drainpipe drain --config ./drainpipe.hcl
```

See [`example.hcl`](../example.hcl) for a fully annotated reference config.

## HCL Configuration (Recommended)

HCL config files use three block types that map naturally to Steampipe's own configuration language:

| Block | Purpose |
|-------|---------|
| `connection` | Steampipe plugin credentials — passed directly to the plugin as HCL |
| `plugin` | Plugin-level settings: memory limits, rate limiters |
| `drainpipe` | Operational settings: tables, concurrency, multi-account orchestration |

### Connections

A `connection` block configures a Steampipe plugin. All attributes except `plugin` are forwarded to the plugin as native HCL connection config — no translation layer.

```hcl
connection "aws" {
  plugin  = "turbot/aws@latest"
  profile = "my-profile"
  regions = ["us-east-1", "us-west-2"]
  max_error_retry_attempts = 10
}

connection "cloudflare" {
  plugin = "turbot/cloudflare@1.5.1"
  token  = "${CLOUDFLARE_API_TOKEN}"
}
```

This directly supports every attribute the Steampipe plugin accepts, including provider-specific ones like `max_error_retry_attempts`, `access_key`, `secret_key`, `endpoint_url`, etc.

### Plugin Settings and Rate Limiters

The `plugin` block sets plugin-level options like memory limits and rate limiters. Rate limiters are sent to the plugin via gRPC and override any limiters compiled into the plugin with the same name.

```hcl
plugin "aws" {
  memory_max_mb = 2048

  limiter "global_concurrency" {
    max_concurrency = 250
  }

  limiter "global_rate" {
    bucket_size = 1000
    fill_rate   = 500
    scope       = ["connection", "region"]
  }

  limiter "s3_throttle" {
    bucket_size = 100
    fill_rate   = 50
    scope       = ["connection", "service"]
    where       = "service = 's3'"
  }
}
```

#### Rate Limiter Options

| Argument | Default | Description |
|----------|---------|-------------|
| `bucket_size` | unlimited | Max burst size (token-bucket algorithm) |
| `fill_rate` | unlimited | Tokens refilled per second |
| `max_concurrency` | unlimited | Max parallel List/Get/Hydrate functions |
| `scope` | `[]` | Grouping context: `connection`, `table`, `function_name`, region, etc. |
| `where` | none | SQL-style filter for targeting specific scopes |

Rate limiters use a **token-bucket algorithm**. When multiple limiters apply to a function call, the one requiring the longest wait takes effect. See the [Steampipe rate limiting guide](https://steampipe.io/docs/guides/limiter) for details on scopes, where clauses, and tuning strategies.

### Drainpipe Jobs

Each `drainpipe` block is an independent collection job. It references a connection and specifies which tables to collect along with operational settings.

```hcl
drainpipe "aws_inventory" {
  connection    = "aws"
  concurrency   = 5
  retries       = 3
  retry_delay   = "10s"
  table_timeout = "30m"
  strict        = true

  tables = ["aws_ec2_*", "aws_s3_*"]

  table "aws_ecs_task_definition" {
    where = {
      status = "ACTIVE"
    }
  }
}
```

#### Drainpipe Fields

| Field | Required | Description |
|-------|----------|-------------|
| `connection` | Yes (or `provider`) | Name of a `connection` block to use |
| `provider` | No | Provider shorthand (`aws`, `azure`, `cloudflare`) — alternative to connection |
| `plugin_path` | No | Explicit path to a plugin binary (bypasses download) |
| `identity_table` | No | Table to query for account identity (e.g., `aws_sts_caller_identity`) |
| `identity_column` | No | Column containing the account ID (e.g., `account_id`) |
| `natural_key` | No | Preferred natural key column (e.g., `arn`, `id`) |
| `account_id` | No | Explicit account ID (skips identity resolution) |
| `concurrency` | No | Max concurrent table exports (default: 1) |
| `retries` | No | Max retries per table on transient errors (default: 3) |
| `retry_delay` | No | Initial backoff delay, doubles per retry (default: `"10s"`) |
| `table_timeout` | No | Per-table export timeout (default: `"10m"`) |
| `strict` | No | Abort on any failure or unsupported table (default: false) |
| `tables` | Yes | List of table name patterns (glob patterns like `"aws_*"` supported) |
| `table` blocks | No | Named table blocks for per-table options — see [Table Blocks](#table-blocks) |

### Table Blocks

`table` blocks let you configure per-table options for any table in the job. A table block is identified by the exact table name (not a glob pattern). All fields are optional.

```hcl
table "<table_name>" {
  key          = ["<column>", ...]   # custom natural key (list)
  where        = { <col> = "<val>" } # server-side equality filters
  columns      = ["<col>", ...]      # explicit column subset to collect
  filter_query { ... }               # dynamic pre-filter from Postgres query
}
```

#### Table Block Fields

| Field | Description |
|-------|-------------|
| `key` | Override the natural key for this table. Accepts a list of column names; supports composite keys. Required for tables without `arn` or `id`. |
| `where` | Server-side equality filters on key columns. Only key columns are supported — see the [Steampipe Hub](https://hub.steampipe.io/plugins) for each table's key columns. |
| `columns` | Restrict collection to this explicit list of columns (plus key columns and `_ctx`, which are always included). |
| `filter_query` | Dynamic pre-filter: runs a SQL query against the destination Postgres and calls the plugin once per result value. See [Dynamic Pre-Filtering](#dynamic-pre-filtering-filter_query) below. |

#### `key` — Custom Natural Key

Some tables don't have a standard `arn` or `id` column, so drainpipe can't auto-discover their natural key. Specifying `key` bypasses auto-discovery and also allows the table to be collected even if it would otherwise be excluded as "unsupported".

Supports **composite keys** — list multiple columns when no single column is unique:

```hcl
drainpipe "aws_inventory" {
  connection = "aws"

  # Single custom key
  table "aws_costoptimizationhub_recommendation" {
    key = ["recommendation_id"]
  }

  # Composite key
  table "aws_cost_by_service" {
    key = ["account_id", "region", "service"]
  }
}
```

In YAML:

```yaml
provider: aws
tables:
  - table: aws_costoptimizationhub_recommendation
    key: [recommendation_id]
  - table: aws_cost_by_service
    key: [account_id, region, service]
```

> **Note:** The `key` columns (together with `_source_account`) become the primary key in the destination Postgres table. Ensure the columns you specify uniquely identify each row.

#### `where` — Server-Side Filtering

Apply server-side equality filters on key columns to reduce the data returned by the plugin:

```hcl
drainpipe "filtered" {
  connection = "aws"

  tables = ["aws_s3_bucket"]  # no filter — collect all

  table "aws_ecs_task_definition" {
    where = {
      status = "ACTIVE"
    }
  }

  table "aws_ec2_instance" {
    where = {
      instance_state = "running"
      instance_type  = "t3.micro"
    }
  }
}
```

> **Note:** Only key columns support filtering. Refer to the table documentation in the [Steampipe Hub](https://hub.steampipe.io/plugins) for available key columns.

#### Dynamic Pre-Filtering (`filter_query`)

Some tables return massive result sets when queried without key-column quals (e.g. `aws_ecs_task_definition` lists every revision ever created). A `filter_query` block lets you pre-filter collection by running a SQL query against the **destination Postgres** to resolve qual values dynamically.

```hcl
drainpipe "aws_inventory" {
  connection = "aws"

  tables = ["aws_ecs_service"]

  table "aws_ecs_task_definition" {
    filter_query {
      column = "task_definition_arn"
      query  = "SELECT DISTINCT task_definition FROM aws_ecs_service WHERE _deleted_at IS NULL"
    }
    columns = ["task_definition_arn", "family", "revision", "status"]
  }
}
```

| Field | Description |
|-------|-------------|
| `column` | The key column on the target Steampipe table to filter |
| `query` | SQL query run against destination Postgres; the first column of each result row becomes an equality qual value |

**How it works:** Drainpipe runs the `query` against Postgres, then calls the plugin's Execute once per returned value with `column = value` as a qual. This turns a full table scan into N targeted API calls.

Tables with `filter_query` are automatically collected **after** normal tables in the same job, so dependency data is available when the query runs.

`filter_query` can be combined with static `where` filters — both are merged into each Export call.

#### `columns` — Column Subset

By default drainpipe collects every column the plugin exposes. Use `columns` to restrict collection to a specific subset — useful for wide tables or when you only need certain fields.

Key columns and `_ctx` are always included regardless of what `columns` specifies.

```hcl
table "aws_ecs_task_definition" {
  columns = ["task_definition_arn", "family", "revision", "status", "container_definitions"]
}
```

In YAML:

```yaml
tables:
  - table: aws_ecs_task_definition
    columns: [task_definition_arn, family, revision, status]
```

### Multi-Account Modes

#### Explicit Accounts (SSO)

```hcl
drainpipe "sso_accounts" {
  connection = "aws"

  accounts {
    name    = "Production"
    profile = "prod.AWSOrgAdmin"
  }

  accounts {
    name    = "Development"
    profile = "dev.AWSOrgAdmin"
    regions = ["us-east-1"]  # override connection regions
  }

  tables = ["aws_ec2_instance", "aws_s3_bucket"]
}
```

#### AWS Organizations

```hcl
drainpipe "org_inventory" {
  connection = "aws"

  org {
    role_name        = "ReadOnlyCollectorRole"
    admin_account_id = "123456789012"
    organizations    = ["ou-xxxx-xxxxxxxx"]

    override {
      match_account_names = ["*-dev", "*-sandbox"]
      tables              = ["aws_ec2_instance", "aws_s3_bucket"]
    }

    override {
      match_account_ids = ["999999999999"]
      skip              = true
    }
  }

  tables = ["aws_ec2_*", "aws_s3_*"]
}
```

### Provider Shorthand

For quick setups without a separate connection block:

```hcl
drainpipe "quick" {
  provider = "aws"
  tables   = ["aws_s3_bucket"]
}
```

Known providers and their defaults:

| Provider | Plugin | Identity Table | Identity Column | Natural Key |
|----------|--------|----------------|-----------------|-------------|
| `aws` | `turbot/aws@latest` | `aws_sts_caller_identity` | `account_id` | `arn` |
| `azure` | `turbot/azure@latest` | `azure_subscription` | `subscription_id` | `id` |
| `cloudflare` | `turbot/cloudflare@latest` | `cloudflare_account` | `id` | `id` |

### Plugin Specifier Format

The `plugin` attribute in a `connection` block accepts several formats:

| Format | Example | Resolved as |
|--------|---------|-------------|
| `org/name@version` | `turbot/aws@1.30.0` | Org: turbot, Name: aws, Version: 1.30.0 |
| `org/name` | `turbot/cloudflare` | Version defaults to `latest` |
| `name@version` | `aws@1.30.0` | Org defaults to `turbot` |
| `name` | `aws` | Org: turbot, Version: latest |

### Multi-Config Support

Use multiple `--config` flags to load from several files:

```bash
drainpipe drain --config aws.hcl --config cloudflare.hcl
```

You can also mix YAML and HCL files:

```bash
drainpipe drain --config legacy.yaml --config new.hcl
```

## YAML Configuration (Legacy)

YAML configuration continues to work and is auto-detected by file extension (`.yaml` / `.yml`). The format is unchanged from previous versions.

Simple form — a plain string for each table:

```yaml
provider: aws
connection:
  profile: my-profile
  regions:
    - us-east-1
    - us-west-2
tables:
  - "aws_ec2_*"
  - "aws_s3_bucket"
```

Object form — use a mapping when you need per-table options:

```yaml
provider: aws
tables:
  - "aws_ec2_*"                          # plain string, no options

  - table: aws_costoptimizationhub_recommendation
    key: [recommendation_id]              # custom natural key

  - table: aws_ecs_task_definition
    key: [task_definition_arn]            # custom key
    where:
      status: ACTIVE                      # server-side filter
    columns: [task_definition_arn, family, revision, status]

  - table: aws_ecs_task_definition
    filter_query:
      column: task_definition_arn
      query: "SELECT DISTINCT task_definition FROM aws_ecs_service WHERE _deleted_at IS NULL"
    columns: [task_definition_arn, family, revision, status]
```

See [`example.yaml`](../example.yaml) for the full YAML reference.

> **Migration note:** YAML configs cannot express rate limiters or plugin-level settings. Switch to HCL to access these features.

## Strict Mode

When `strict` is set to `true`:

- Configured table patterns matching no supported tables causes a fatal exit
- Any table failure after retries aborts remaining work

## Precedence Rules

| Setting | Priority |
|---------|----------|
| Plugin | `connection.plugin` > `provider` shorthand mapping |
| Connection | explicit connection attributes > env vars |
| Identity | explicit `identity_table`/`identity_column` > known provider defaults |
| Natural key | per-table `key` > drainpipe-level `natural_key` > auto-discovered `GetCallKeyColumnList` > known provider defaults |
| Tables | config `tables` (required) |
| Where filters | per-table `where` in config (no CLI equivalent) |
| Provider | `--provider` flag > config `provider` > `aws` |

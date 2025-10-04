# Configuration

Drainpipe supports an optional YAML configuration file for managing complex multi-account setups. By default, it looks for `drainpipe.yaml` in the current directory.

```bash
drainpipe drain --config ./drainpipe.yaml
```

See [`example.yaml`](../example.yaml) for a fully annotated reference config.

## Full Reference

```yaml
# Provider
provider: aws

# Concurrency & reliability
concurrency: 5         # Max concurrent table exports (default: 1)
retries: 3             # Max retries per table on transient errors (default: 3)
retry_delay: 10s       # Initial backoff delay, doubles per retry (default: 10s)
table_timeout: 30m     # Per-table export timeout (default: 10m)
strict: false          # Abort on any failure or unsupported table (default: false)

# Regions to target
regions:
  - us-east-1
  - us-west-2
  - eu-west-1

# Tables to target (glob patterns supported)
# Supports both plain strings and objects with where filters
tables:
  - "aws_ec2_instance"
  - "aws_s3_bucket"
  - "aws_iam_user"
  - "aws_lambda_function"
  - table: aws_ecs_task_definition    # object form with where filter
    where:
      status: ACTIVE
```

## Per-Table Filtering (`where`)

Use the object form to apply **server-side filters** on key columns. Filters are pushed to the cloud API, reducing API calls, network traffic, and processing time.

```yaml
tables:
  - aws_s3_bucket                         # no filter — collect all
  - table: aws_ecs_task_definition        # only active task definitions
    where:
      status: ACTIVE
  - table: aws_ec2_instance               # multiple filters (AND)
    where:
      instance_state: running
      instance_type: t3.micro
```

> **Note:** Only key columns support filtering. Refer to the table documentation in the [Steampipe Hub](https://hub.steampipe.io/plugins) for available key columns (e.g., [aws_ec2_instance](https://hub.steampipe.io/plugins/turbot/aws/tables/aws_ec2_instance#inspect)).

## Multi-Account Modes

### Explicit Accounts (SSO)

List accounts with named profiles. Best for development/testing with AWS SSO.

```yaml
accounts:
  - name: Production
    profile: prod.AWSOrgAdmin
  - name: Development
    profile: dev.AWSOrgAdmin
```

### AWS Organizations

Discovers accounts automatically via `organizations:ListAccounts`. Assumes a role in each member account with fresh STS credentials. Best for production service accounts.

```yaml
org:
  role_name: AWSOrgAdmin
  admin_account_id: "718245426055"
```

If both `accounts` and `org` are present, `accounts` takes precedence.

## Multi-Config Support

You can define multiple independent configuration blocks in two ways:

### YAML multi-document (`---` separator)

```yaml
# Config block 1: Organization accounts only
provider: aws
profile: management
tables:
  - aws_organizations_account
accounts:
  - name: justmiles
    profile: justmiles.AWSOrgAdmin
---
# Config block 2: Full resource inventory
provider: aws
concurrency: 5
tables:
  - aws_ec2_instance
  - aws_s3_bucket
  - table: aws_ecs_task_definition
    where:
      status: ACTIVE
```

### Multiple `--config` flags

```bash
drainpipe drain --config org.yaml --config workloads.yaml
```

Each config block operates independently with its own provider, profile, regions, tables, and accounts. All blocks share a single database connection and worker pool. The pool uses the **maximum** concurrency, retries, and timeout values from any config block.

## Precedence Rules

| Setting | Priority |
|---------|----------|
| Tables | config `tables` > all supported |
| Where filters | per-table `where` in config (no CLI equivalent) |
| Provider | `--provider` flag > config `provider` > `aws` |
| Regions | config `regions` > `AWS_REGIONS` env > all enabled |
| Profile | config `profile` > `AWS_PROFILE` env > default chain |

## Strict Mode

When `strict: true`:

- Configured table patterns matching no supported tables → **process exits**
- Any table failure after retries → **remaining work aborted**

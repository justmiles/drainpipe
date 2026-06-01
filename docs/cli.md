# CLI Reference

```
drainpipe <command> [options]

Commands:
  drain            Export resources into PostgreSQL
  validate         Validate config: download plugins, check table names and key columns
  list-tables      List available tables for a provider
  list-providers   List known providers and their default plugins
  download-plugins Download plugin binaries for a config file
```

## `validate`

Validate a configuration without connecting to cloud providers or databases. Downloads plugins as needed and checks that every table pattern resolves to at least one supported table with valid key columns.

```bash
# Validate the default config file
drainpipe validate

# Validate a specific config file
drainpipe validate --config ./drainpipe.hcl

# Validate multiple config files
drainpipe validate --config aws.hcl --config cloudflare.hcl
```

### What it checks

| Check | Severity | Description |
|-------|----------|-------------|
| Table exists | Error | Exact table names in config exist in the plugin schema |
| Glob matches tables | Warning | Glob patterns like `aws_ec2_*` match at least one table |
| Natural key | Error | Each matched table has a discoverable key (or an explicit `key` block) |
| `key` columns exist | Error | Explicitly listed `key` columns are present in the plugin schema |
| `where` columns | Warning | Filter columns exist and are key columns on that table |
| `columns` subset | Warning | Explicit column list references valid plugin columns |
| `filter_query.column` | Error/Warning | Filter query column exists and is a key column |

Exits with code 0 if no errors, 1 if any errors are found.

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config` | `-c` | `drainpipe.hcl` | Config file path (repeatable) |

## `drain`

Export cloud resources into PostgreSQL.

```bash
# Target specific tables (glob patterns)
drainpipe drain --tables "aws_ec2_*,aws_s3_bucket"

# Broad wildcard to explicitly collect all supported tables
drainpipe drain --tables "aws_*"

# Specify provider and config
drainpipe drain --provider aws --config ./drainpipe.yaml

# Multiple config files
drainpipe drain --config org.yaml --config workloads.yaml
```

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config` | `-c` | `drainpipe.yaml` | Config file path (repeatable) |
| `--provider` | `-p` | `aws` | Provider name (known shorthand or plugin spec) |
| `--tables` | `-t` | *(required)* | Comma-separated table patterns (required if not in config) |

## `list-tables`

List available tables for a provider. Requires the plugin binary to be available.

```bash
# Show supported tables (with discoverable natural keys)
drainpipe list-tables

# Show all tables including unsupported ones
drainpipe list-tables --unsupported

# List tables for a specific provider
drainpipe list-tables --provider azure
```

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--provider` | `-p` | `aws` | Provider name or plugin spec |
| `--unsupported` | | | Also show tables without discoverable natural keys |

## `list-providers`

List known providers and their default plugin mappings.

```bash
drainpipe list-providers
```

Output:

```
Known providers (shorthand → plugin):

  aws             turbot/aws@latest
  azure           turbot/azure@latest
  cloudflare      turbot/cloudflare@latest

Use any Steampipe plugin with 'plugin: org/name@version' in your config.
```

## Environment Variables

### Database

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_NAME` | `cmdb` | Database name |
| `DB_USER` | `cmdb` | Database user |
| `DB_PASSWORD` | `cmdb_dev` | Database password |
| `DB_SSLMODE` | `disable` | SSL mode |

### AWS

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_PROFILE` | *(default chain)* | AWS named profile |
| `AWS_REGIONS` | *(all enabled)* | Comma-separated regions to drain |
| `AWS_ORG_ROLE_NAME` | *(unset = single mode)* | IAM role name to assume in member accounts |
| `AWS_ORG_ADMIN_ACCOUNT_ID` | *(optional)* | Admin account ID to skip |

> **Note:** Config file settings take precedence over environment variables for profile, regions, and org settings. The `connection:` map in config takes precedence over legacy fields.

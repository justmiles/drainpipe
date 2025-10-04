# CLI Reference

```
drainpipe <command> [options]

Commands:
  drain            Export resources into PostgreSQL
  list-tables      List available tables for a provider
  list-providers   List registered providers
```

## `drain`

Export cloud resources into PostgreSQL.

```bash
# Target all supported tables
drainpipe drain

# Target specific tables (glob patterns)
drainpipe drain --tables "aws_ec2_*,aws_s3_bucket"

# Specify provider and config
drainpipe drain --provider aws --config ./drainpipe.yaml

# Multiple config files
drainpipe drain --config org.yaml --config workloads.yaml
```

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config` | `-c` | `drainpipe.yaml` | Config file path (repeatable) |
| `--provider` | `-p` | `aws` | Provider name |
| `--tables` | `-t` | *(all supported)* | Comma-separated table patterns |

## `list-tables`

List available tables for a provider.

```bash
# Show supported tables (with discoverable natural keys)
drainpipe list-tables

# Show all tables including unsupported ones
drainpipe list-tables --unsupported
```

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--provider` | `-p` | `aws` | Provider name |
| `--unsupported` | | | Also show tables without discoverable natural keys |

## `list-providers`

List registered providers.

```bash
drainpipe list-providers
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

> **Note:** Config file settings take precedence over environment variables for profile, regions, and org settings.

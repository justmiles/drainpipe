# Drainpipe HCL Configuration
#
# Place this file as drainpipe.hcl in your working directory,
# or specify a path with: drainpipe drain --config /path/to/config.hcl
#
# HCL files use three block types:
#   connection  — Steampipe plugin credentials (passed to the plugin as-is)
#   plugin      — Plugin-level settings: memory limits, rate limiters
#   drainpipe   — Operational settings: tables, concurrency, multi-account

# ── Connections ────────────────────────────────────────────────────
# Each connection block configures a Steampipe plugin. All attributes
# other than "plugin" are forwarded to the plugin as HCL connection config.

connection "aws" {
  plugin  = "turbot/aws@latest"
  profile = "my-profile"
  regions = ["us-east-1", "us-west-2"]
}

connection "cloudflare" {
  plugin = "turbot/cloudflare@1.5.1"
  # Environment variable references are kept as-is for plugin expansion
  token = "${CLOUDFLARE_API_TOKEN}"
}

# ── Plugin settings (optional) ────────────────────────────────────
# Rate limiters are sent to the plugin via gRPC SetRateLimiters.
# They override any limiters compiled into the plugin with the same name.

plugin "aws" {
  memory_max_mb = 2048

  # Global concurrency cap across all connections
  limiter "global_concurrency" {
    max_concurrency = 250
  }

  # Rate-limit all API calls across connections and regions
  limiter "global_rate" {
    bucket_size = 1000
    fill_rate   = 500
    scope       = ["connection", "region"]
  }

  # Tighter limit for S3 specifically
  limiter "s3_throttle" {
    bucket_size = 100
    fill_rate   = 50
    scope       = ["connection", "service"]
    where       = "service = 's3'"
  }
}

# ── Drainpipe jobs ─────────────────────────────────────────────────
# Each drainpipe block is an independent collection job.

# Simple single-account AWS collection
drainpipe "aws_basic" {
  connection    = "aws"
  concurrency   = 5
  retries       = 3
  retry_delay   = "10s"
  table_timeout = "30m"
  strict        = true

  tables = [
    "aws_ec2_*",
    "aws_s3_*",
    "aws_iam_*",
    "aws_rds_*",
  ]

  # Object form with server-side filtering
  table "aws_ecs_task_definition" {
    where = {
      status = "ACTIVE"
    }
  }
}

# Cloudflare collection
drainpipe "cloudflare_basic" {
  connection  = "cloudflare"
  natural_key = "id"
  tables      = ["cloudflare_*"]
}

# ── Dynamic pre-filtering (filter_query) ─────────────────────────
# Some tables are too large when queried without key-column quals.
# A filter_query runs SQL against the destination Postgres to resolve
# qual values dynamically, turning a full table scan into N targeted
# API calls. Tables with filter_query run after normal tables.

drainpipe "ecs_focused" {
  connection = "aws"

  # Collect services first (small table)
  tables = ["aws_ecs_service"]

  # Only collect task definitions that are actually deployed
  table "aws_ecs_task_definition" {
    filter_query {
      column = "task_definition_arn"
      query  = "SELECT DISTINCT task_definition FROM aws_ecs_service WHERE _deleted_at IS NULL"
    }
    columns = ["task_definition_arn", "family", "revision", "status"]
  }
}

# ── Multi-account: explicit accounts ──────────────────────────────

drainpipe "aws_explicit_accounts" {
  connection    = "aws"
  concurrency   = 3
  table_timeout = "30m"

  accounts {
    name    = "prod"
    profile = "prod.ReadOnlyRole"
  }

  accounts {
    name    = "staging"
    profile = "staging.ReadOnlyRole"
    regions = ["us-east-1"]
  }

  tables = ["aws_ec2_instance", "aws_s3_bucket"]
}

# ── Multi-account: AWS Organizations ──────────────────────────────

drainpipe "aws_org_mode" {
  connection    = "aws"
  concurrency   = 5
  retries       = 3
  table_timeout = "30m"

  org {
    role_name        = "ReadOnlyCollectorRole"
    admin_account_id = "123456789012"

    # Per-account overrides (first match wins)
    override {
      match_account_names = ["*-dev", "*-sandbox", "*-test"]
      tables              = ["aws_ec2_instance", "aws_s3_bucket"]
    }

    override {
      match_account_ids = ["999999999999"]
      skip              = true
    }
  }

  tables = ["aws_ec2_*", "aws_s3_*", "aws_iam_*"]
}

# ── Provider shorthand ────────────────────────────────────────────
# For quick setups without a separate connection block, use provider
# directly. Known providers: aws, azure, cloudflare.

drainpipe "quick_aws" {
  provider = "aws"
  tables   = ["aws_s3_bucket"]
}

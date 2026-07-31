---
description: How to add a new Steampipe provider to Drainpipe
---

# Add a New Steampipe Provider

This workflow adds a new cloud provider (e.g., Azure, Cloudflare, GCP) to Drainpipe. Use `cmd/internal/provider/aws.go` as the reference implementation.

## Prerequisites

- The Steampipe plugin Go module for the provider exists (e.g., `github.com/turbot/steampipe-plugin-azure`)
- You know the provider's authentication model (env vars, config files, etc.)
- You know which identity table can resolve the account/subscription/tenant ID

---

## Steps

### 1. Add the Steampipe Plugin Dependency

// turbo
```bash
cd cmd && go get github.com/turbot/steampipe-plugin-<provider>@latest
```

### 2. Create the Provider File

Create `cmd/internal/provider/<provider>.go` implementing the `Provider` interface.

**Required structure** (use `aws.go` as template):

```go
package provider

import (
    "context"
    "fmt"

    "<steampipe-plugin-import-path>/<provider>"
    "github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
    "github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func init() {
    Register(&<Provider>Provider{})
}

type <Provider>Provider struct {
    // Provider-specific config fields (auth, regions, etc.)
}

func (p *<Provider>Provider) Name() string { return "<provider>" }

func (p *<Provider>Provider) PluginFunc() plugin.PluginFunc {
    return <provider>.Plugin
}

func (p *<Provider>Provider) DefaultConnectionConfig() string {
    // Return HCL config body for the plugin connection.
    // Read credentials from struct fields first, fall back to env vars.
    return ""
}

func (p *<Provider>Provider) ResolveAccount(ctx context.Context, queryFunc QueryFunc) (string, error) {
    // Query an identity table to get the account/subscription/tenant ID.
    // Example for AWS: queries aws_sts_caller_identity for account_id
    // Example for Azure: could query azure_subscription for subscription_id
    row, err := queryFunc(ctx, "<identity_table>")
    if err != nil {
        return "", fmt.Errorf("querying identity: %w", err)
    }
    id, ok := row["<id_column>"]
    if !ok || id == nil {
        return "", fmt.Errorf("<id_column> not found")
    }
    return fmt.Sprintf("%v", id), nil
}

func (p *<Provider>Provider) NaturalKeyColumns(tableName string, schema *proto.TableSchema) []string {
    // Option A: Use the default (GetCallKeyColumnList required keys)
    return DefaultNaturalKeyColumns(schema)

    // Option B: Prefer a globally unique column like "id" or "arn"
    // if schema != nil {
    //     for _, col := range schema.Columns {
    //         if col.Name == "id" { return []string{"id"} }
    //     }
    // }
    // return DefaultNaturalKeyColumns(schema)
}
```

**Key decisions to make for each provider:**

| Decision | Description |
|---|---|
| Identity table | Which Steampipe table returns the account/subscription ID? |
| Natural key strategy | Use `DefaultNaturalKeyColumns` or prefer a globally unique column (like `arn` for AWS)? |
| Connection config | What HCL fields does the plugin need? (credentials, regions, tenant ID, etc.) |
| Multi-account support | Does this provider need `MultiAccountProvider`? (see step 3) |

### 3. (Optional) Implement MultiAccountProvider

If the provider supports multi-account/subscription/tenant collection, implement the `MultiAccountProvider` interface on the same struct:

```go
func (p *<Provider>Provider) DiscoverAccounts(ctx context.Context) ([]AccountInfo, error) {
    // List accounts/subscriptions/tenants.
    // Return nil for single-account fallback.
}

func (p *<Provider>Provider) AssumeAccountRole(ctx context.Context, account AccountInfo) (*AccountConfig, error) {
    // Obtain credentials for a specific account.
    // Return an AccountConfig with the HCL ConnectionConfig.
}
```

### 4. Verify the Provider Registers

The `init()` function auto-registers the provider. Verify it appears:

// turbo
```bash
cd cmd && go run . list-providers
```

The new provider name should appear in the output.

### 5. Verify Table Listing

// turbo
```bash
cd cmd && go run . list-tables --provider <provider>
```

This confirms the plugin loads correctly and tables are discoverable. Only tables with natural keys (supported tables) are shown by default. Use `--unsupported` to see all tables.

### 6. Update `collector.yaml` Configuration

Add a config block for the new provider. Use `collector.yaml.example` as a reference:

```yaml
provider: <provider>

# Provider-specific settings
regions:
  - us-east-1

tables:
  - "<provider>_*"
```

### 7. Test Collection

Start the local PostgreSQL (if not running):

// turbo
```bash
docker compose up -d postgres
```

Run a collection against a small set of tables:

```bash
cd cmd && go run . collect --provider <provider> --tables "<provider>_<some_table>"
```

Verify:
- Tables are created in PostgreSQL with the correct schema
- Data is imported with `_collector_account`, `_first_seen_at`, `_last_seen_at` tracking columns
- Natural keys are correctly identified (no duplicate key errors)

### 8. Update `go.mod` Tidying

// turbo
```bash
cd cmd && go mod tidy
```

---

## Reference Files

| File | Purpose |
|---|---|
| `cmd/internal/provider/provider.go` | `Provider` interface, `MultiAccountProvider` interface, registry |
| `cmd/internal/provider/aws.go` | Reference implementation (full example) |
| `cmd/internal/exporter/exporter.go` | Plugin execution engine (provider-agnostic) |
| `cmd/internal/config/collector.go` | YAML config structure (`CollectorConfig`) |
| `collector.yaml.example` | Example configuration file |
| `docs/architecture.md` | Full system architecture documentation |

## Checklist

- [ ] Plugin dependency added to `go.mod`
- [ ] Provider file created at `cmd/internal/provider/<provider>.go`
- [ ] `init()` registers the provider
- [ ] `Provider` interface fully implemented: `Name`, `PluginFunc`, `DefaultConnectionConfig`, `ResolveAccount`, `NaturalKeyColumns`
- [ ] (If multi-account) `MultiAccountProvider` implemented: `DiscoverAccounts`, `AssumeAccountRole`
- [ ] Provider appears in `list-providers` output
- [ ] Tables appear in `list-tables --provider <provider>` output
- [ ] Test collection succeeds against at least one table
- [ ] `go mod tidy` run
- [ ] `collector.yaml.example` updated with provider example (if desired)

package config

import (
	"fmt"
	"strings"
)

// ProviderDefaults holds the default plugin settings for a known provider name.
// When a user specifies "provider: aws" without explicit plugin fields, these
// defaults are applied.
type ProviderDefaults struct {
	Plugin         string // e.g. "turbot/aws@latest"
	IdentityTable  string // e.g. "aws_sts_caller_identity"
	IdentityColumn string // e.g. "account_id"
	NaturalKey     string // e.g. "arn"
	// TableKeys overrides the natural key for specific tables where the default
	// key resolution produces duplicates. Takes precedence over NaturalKey and
	// plugin-advertised key columns, but is overridden by per-table key in config.
	TableKeys map[string][]string
	// DefaultLimiters are applied to the plugin when the user has not configured
	// any limiters via a plugin {} block. They set a safe baseline for providers
	// with strict API rate limits. User-defined limiters always take precedence.
	DefaultLimiters []RateLimiterDef
	// SourceAccountQual, when set, causes drainpipe to inject the resolved
	// source account as an equality qual under this column name for every table
	// query. Use for providers whose tables require an account/subscription ID
	// qual that drainpipe already knows from the identity lookup.
	SourceAccountQual string
	// DefaultFilterQueries provides filter_query defaults for tables that
	// require a dynamic qual (e.g. zone_id) resolved from an already-collected
	// table. Only applied when the user has not configured an explicit
	// filter_query for the table. User config always takes full precedence.
	DefaultFilterQueries map[string]*FilterQuery
	// SourceAccountQualExcludeTables is the set of tables that must NOT receive
	// the automatic SourceAccountQual injection. Use for tables where the
	// provider API treats the account and zone/resource qualifiers as mutually
	// exclusive (e.g. Cloudflare's logpush API rejects requests that supply
	// both account_id and zone_id).
	SourceAccountQualExcludeTables map[string]bool
}

// KnownProviders maps short provider names to their default plugin settings.
var KnownProviders = map[string]ProviderDefaults{
	"aws": {
		Plugin:         "turbot/aws@latest",
		IdentityTable:  "aws_sts_caller_identity",
		IdentityColumn: "account_id",
		NaturalKey:     "arn",
	},
	"azure": {
		Plugin:         "turbot/azure@latest",
		IdentityTable:  "azure_subscription",
		IdentityColumn: "subscription_id",
		NaturalKey:     "id",
	},
	"cloudflare": {
		Plugin:         "turbot/cloudflare@latest",
		IdentityTable:  "cloudflare_account",
		IdentityColumn: "id",
		NaturalKey:     "id",
		// Managed transforms are zone-scoped: the same transform id (e.g.
		// "add_visitor_location_headers") appears in every zone, so id alone
		// is not unique. Load balancer pools and monitors are account-level
		// but the plugin iterates them per zone, producing duplicate id values;
		// those are handled by DISTINCT ON deduplication in the importer.
		TableKeys: map[string][]string{
			"cloudflare_managed_transform": {"id", "zone_id"},
			// Zone settings share the same id (e.g. "ssl", "cache_level") across
			// every zone; zone_id is required to uniquely identify a row.
			"cloudflare_zone_setting": {"id", "zone_id"},
			// Logpush job IDs are per-zone integers; the same integer can identify
			// different jobs in different zones.
			"cloudflare_logpush_job": {"id", "zone_id"},
		},
		// Cloudflare enforces 1,200 requests per 5-minute window (~4 req/s
		// average). The Steampipe plugin fans out many individual Cloudflare
		// HTTP calls per Execute RPC (one per zone/resource), so two concurrent
		// executes can easily burst past the ceiling. FillRate 2 req/s yields
		// ~600 req/5 min, giving ample headroom for that fan-out.
		// Override via a plugin "cloudflare" { limiter … } block in your HCL.
		DefaultLimiters: []RateLimiterDef{
			{Name: "cloudflare_concurrency", MaxConcurrency: 2},
			{Name: "cloudflare_rate", FillRate: 2, BucketSize: 5},
		},
		// The Cloudflare plugin requires account_id as a qual on several tables.
		// Drainpipe injects the resolved source account automatically so users
		// don't have to hard-code it in their where blocks.
		SourceAccountQual: "account_id",
		// cloudflare_logpush_job also requires zone_id. We resolve zone IDs
		// from the already-collected cloudflare_zone table via filter_query,
		// which also ensures zone collection runs first due to sort ordering.
		DefaultFilterQueries: map[string]*FilterQuery{
			"cloudflare_logpush_job": {
				Column: "zone_id",
				Query:  "SELECT id FROM cloudflare_zone WHERE _deleted_at IS NULL",
			},
		},
		// Cloudflare's logpush API rejects requests that supply both account_id
		// and zone_id. Since logpush jobs are queried per zone via filter_query,
		// suppress the automatic account_id injection for that table.
		SourceAccountQualExcludeTables: map[string]bool{
			"cloudflare_logpush_job": true,
		},
	},
}

// ResolvePluginSpec returns the plugin specifier for this config, merging
// legacy provider shorthand with explicit plugin fields. Returns the effective
// plugin spec and whether this config has a valid plugin configuration.
func (c *DrainpipeConfig) ResolvePluginSpec() (string, bool) {
	if c.Plugin != "" {
		return c.Plugin, true
	}
	if c.Provider != "" {
		if defaults, ok := KnownProviders[c.Provider]; ok {
			return defaults.Plugin, true
		}
	}
	return "", false
}

// ResolveIdentity returns the identity table and column for account resolution.
func (c *DrainpipeConfig) ResolveIdentity() (table, column string) {
	table = c.IdentityTable
	column = c.IdentityColumn
	if table == "" || column == "" {
		if defaults, ok := KnownProviders[c.Provider]; ok {
			if table == "" {
				table = defaults.IdentityTable
			}
			if column == "" {
				column = defaults.IdentityColumn
			}
		}
	}
	return table, column
}

// ResolveSourceAccountQual returns the column name that should receive the
// source account value as an automatic equality qual on every table query.
// Returns "" when the provider has no such requirement.
func (c *DrainpipeConfig) ResolveSourceAccountQual() string {
	if defaults, ok := KnownProviders[c.Provider]; ok {
		return defaults.SourceAccountQual
	}
	return ""
}

// ResolveDefaultFilterQueries returns provider-level default filter_query
// entries for specific tables. Only applied when the user has not configured
// an explicit filter_query for the table.
func (c *DrainpipeConfig) ResolveDefaultFilterQueries() map[string]*FilterQuery {
	if defaults, ok := KnownProviders[c.Provider]; ok {
		return defaults.DefaultFilterQueries
	}
	return nil
}

// ResolveSourceAccountQualExcludeTables returns the set of tables that must
// not receive the automatic SourceAccountQual injection. Returns nil when the
// provider has no such exclusions.
func (c *DrainpipeConfig) ResolveSourceAccountQualExcludeTables() map[string]bool {
	if defaults, ok := KnownProviders[c.Provider]; ok {
		return defaults.SourceAccountQualExcludeTables
	}
	return nil
}

// ResolveDefaultLimiters returns the provider's default rate limiter
// definitions. Returns nil when the provider is unknown or has no defaults.
// These are only applied when the user has not configured any limiters via a
// plugin {} block — user-defined limiters always take full precedence.
func (c *DrainpipeConfig) ResolveDefaultLimiters() []RateLimiterDef {
	if defaults, ok := KnownProviders[c.Provider]; ok {
		return defaults.DefaultLimiters
	}
	return nil
}

// ResolveProviderTableKeys returns per-table key overrides from the known
// provider defaults. These take precedence over the provider-level NaturalKey
// but are themselves overridden by per-table key entries in the config.
func (c *DrainpipeConfig) ResolveProviderTableKeys() map[string][]string {
	if defaults, ok := KnownProviders[c.Provider]; ok {
		return defaults.TableKeys
	}
	return nil
}

// ResolveNaturalKey returns the preferred natural key column name.
func (c *DrainpipeConfig) ResolveNaturalKey() string {
	if c.NaturalKey != "" {
		return c.NaturalKey
	}
	if defaults, ok := KnownProviders[c.Provider]; ok {
		return defaults.NaturalKey
	}
	return ""
}

// ResolveConnectionHCL builds the HCL connection config string from the
// typed connection fields and the Extra catch-all map. Orchestration-only
// fields (accounts, org, organizations, assume_role_name) are excluded.
func (c *DrainpipeConfig) ResolveConnectionHCL() string {
	conn := make(map[string]interface{})

	// Copy Extra entries first (arbitrary plugin config)
	for k, v := range c.Connection.Extra {
		conn[k] = v
	}

	// Add typed credential fields (override Extra if both present)
	if c.Connection.Profile != "" {
		conn["profile"] = c.Connection.Profile
	}
	if len(c.Connection.Regions) > 0 {
		regions := make([]interface{}, len(c.Connection.Regions))
		for i, r := range c.Connection.Regions {
			regions[i] = r
		}
		conn["regions"] = regions
	}

	return connectionConfigFromMap(conn)
}

// connectionConfigFromMap converts a key-value map into HCL connection config.
func connectionConfigFromMap(m map[string]interface{}) string {
	if len(m) == 0 {
		return ""
	}
	var parts []string
	for key, val := range m {
		parts = append(parts, fmt.Sprintf("  %s = %s", key, hclValue(val)))
	}
	return strings.Join(parts, "\n")
}

// hclValue converts a Go value to its HCL literal representation.
func hclValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("%q", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case float64:
		// YAML unmarshals numbers as float64
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case []interface{}:
		items := make([]string, len(val))
		for i, item := range val {
			items[i] = hclValue(item)
		}
		return "[" + strings.Join(items, ", ") + "]"
	case []string:
		items := make([]string, len(val))
		for i, s := range val {
			items[i] = fmt.Sprintf("%q", s)
		}
		return "[" + strings.Join(items, ", ") + "]"
	default:
		return fmt.Sprintf("%q", fmt.Sprint(val))
	}
}

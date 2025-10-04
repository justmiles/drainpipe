// Package provider defines the Provider interface and a registry for
// Steampipe plugin providers (AWS, Azure, Cloudflare, etc.).
package provider

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// Provider represents a cloud provider backed by a Steampipe plugin.
type Provider interface {
	// Name returns the provider's short name (e.g., "aws", "azure", "cloudflare").
	Name() string

	// PluginFunc returns the Steampipe plugin constructor.
	PluginFunc() plugin.PluginFunc

	// DefaultConnectionConfig returns the default HCL connection config body.
	DefaultConnectionConfig() string

	// ResolveAccount returns a unique identifier for the account/subscription/tenant
	// that this collection run targets. This is stored in the `_source_account`
	// column and used to scope upserts and soft-deletes so that runs for different
	// accounts don't interfere with each other.
	//
	// Implementations:
	//   AWS:        STS GetCallerIdentity → account ID
	//   Azure:      subscription ID from connection config
	//   Cloudflare: account ID from API
	ResolveAccount(ctx context.Context, queryFunc QueryFunc) (string, error)

	// NaturalKeyColumns returns the natural key column names for a table,
	// derived from the plugin schema. The default implementation uses
	// GetCallKeyColumnList (required key columns). Providers can override
	// for tables that need special handling.
	NaturalKeyColumns(tableName string, schema *proto.TableSchema) []string
}

// QueryFunc is a function that exports a single table and returns one row.
// Used by ResolveAccount to query identity tables without importing the
// exporter package (avoids circular dependency).
type QueryFunc func(ctx context.Context, tableName string) (map[string]interface{}, error)

// AccountInfo holds metadata about an account discovered from an organization.
// It contains no credentials — those are obtained lazily via AssumeAccountRole.
type AccountInfo struct {
	AccountID   string // AWS account ID, Azure subscription, etc.
	AccountName string // Human-readable name
}

// AccountConfig describes a single account to collect from, with credentials.
type AccountConfig struct {
	AccountID        string // AWS account ID (for logging)
	AccountName      string // Human-readable name (e.g., from Organizations)
	ConnectionConfig string // HCL config body for the Steampipe plugin
}

// MultiAccountProvider is an optional interface. Providers that support
// multi-account collection (e.g., via AWS Organizations) implement this.
//
// The two-phase design enables lazy credential refresh:
//   - DiscoverAccounts: list accounts (metadata only, no credentials)
//   - AssumeAccountRole: obtain credentials just-in-time per account
type MultiAccountProvider interface {
	// DiscoverAccounts lists accounts in the organization.
	// Returns nil (single-account fallback) when org mode is not configured.
	DiscoverAccounts(ctx context.Context) ([]AccountInfo, error)

	// AssumeAccountRole obtains temporary credentials for a specific account.
	// Called just-in-time by workers, so credentials are always fresh.
	AssumeAccountRole(ctx context.Context, account AccountInfo) (*AccountConfig, error)
}

// DefaultNaturalKeyColumns extracts natural key columns from a TableSchema
// using the GetCallKeyColumnList. Returns columns where require == "required".
// This is the universal default — providers can override per-table.
func DefaultNaturalKeyColumns(schema *proto.TableSchema) []string {
	if schema == nil || len(schema.GetCallKeyColumnList) == 0 {
		return nil
	}

	var keys []string
	for _, kc := range schema.GetCallKeyColumnList {
		if kc.Require == "required" {
			keys = append(keys, kc.Name)
		}
	}
	return keys
}

// registry holds all registered providers, keyed by name.
var registry = map[string]Provider{}

// Register adds a provider to the global registry.
func Register(p Provider) {
	registry[p.Name()] = p
}

// Get returns a registered provider by name, or nil if not found.
func Get(name string) Provider {
	return registry[name]
}

// All returns all registered providers.
func All() map[string]Provider {
	return registry
}

// Names returns the names of all registered providers.
func Names() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}

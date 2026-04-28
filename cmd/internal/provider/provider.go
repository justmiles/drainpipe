// Package provider holds multi-account discovery logic and natural key helpers
// for cloud providers. With the out-of-process plugin architecture, provider-
// specific plugin loading has moved to the exporter; this package retains only
// the host-side logic that requires direct cloud SDK access.
package provider

import (
	"context"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

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

// MultiAccountProvider is an optional interface for providers that support
// multi-account collection (e.g., via AWS Organizations).
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

// NaturalKeyColumns returns the natural key columns for a table, preferring
// the given preferredKey if the table has that column, otherwise falling back
// to GetCallKeyColumnList required columns.
func NaturalKeyColumns(tableName string, schema *proto.TableSchema, preferredKey string) []string {
	if preferredKey != "" && schema != nil {
		for _, col := range schema.Columns {
			if col.Name == preferredKey {
				return []string{preferredKey}
			}
		}
	}
	return DefaultNaturalKeyColumns(schema)
}

// DefaultNaturalKeyColumns extracts natural key columns from a TableSchema
// using the GetCallKeyColumnList. Returns columns where require == "required".
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

package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// FilterQuery defines a dynamic pre-filter for table collection. The SQL
// query runs against the destination Postgres before collection begins;
// each result value becomes an equality qual on Column, turning a full
// table scan into N targeted API calls.
type FilterQuery struct {
	Column string `yaml:"column"`
	Query  string `yaml:"query"`
}

// TableEntry represents a table in the config. It supports two YAML forms:
//
//   - "aws_s3_bucket"                          (plain string, no filter)
//   - table: aws_ecs_task_definition             (object form with optional where)
//     where:
//     status: ACTIVE
type TableEntry struct {
	Name        string            `yaml:"table"`
	Where       map[string]string `yaml:"where"`
	Columns     []string          `yaml:"columns"`
	FilterQuery *FilterQuery      `yaml:"filter_query"`
	// Key overrides the natural key for this specific table, bypassing both the
	// drainpipe-level natural_key and auto-discovery. Supports composite keys.
	// Example: key: [recommendation_id] or key: [account_id, region, name]
	Key []string `yaml:"key"`
}

// UnmarshalYAML allows a TableEntry to be specified as either a plain string
// or a mapping with "table" and optional "where" keys.
func (t *TableEntry) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		t.Name = value.Value
		return nil
	}
	// Decode as mapping
	type raw TableEntry // avoid recursion
	var r raw
	if err := value.Decode(&r); err != nil {
		return err
	}
	*t = TableEntry(r)
	return nil
}

// TableNames returns just the table names from a slice of TableEntry.
func TableNames(entries []TableEntry) []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	return names
}

// TableEntryMap returns a map from table name → TableEntry for quick lookup.
func TableEntryMap(entries []TableEntry) map[string]TableEntry {
	m := make(map[string]TableEntry, len(entries))
	for _, e := range entries {
		m[e.Name] = e
	}
	return m
}

// DrainpipeConfig holds the full drainpipe configuration, typically loaded
// from a drainpipe.yaml file. Supports both provider-shorthand and explicit
// plugin-based out-of-process config.
type DrainpipeConfig struct {
	// ── Provider / plugin selection ──────────────────────────────
	Provider   string `yaml:"provider"`
	Plugin     string `yaml:"plugin"`      // e.g. "turbot/aws@1.30.0"
	PluginPath string `yaml:"plugin_path"` // explicit binary path (overrides download)

	// ── Connection config ────────────────────────────────────────
	// All provider-specific settings live here: credentials, regions,
	// multi-account orchestration, and arbitrary Steampipe plugin config.
	Connection ConnectionConfig `yaml:"connection"`

	// ── Identity & natural keys ─────────────────────────────────
	IdentityTable  string `yaml:"identity_table"`  // e.g. "aws_sts_caller_identity"
	IdentityColumn string `yaml:"identity_column"` // e.g. "account_id"
	NaturalKey     string `yaml:"natural_key"`     // preferred natural key column (e.g. "arn", "id")

	// ── Shared fields ───────────────────────────────────────────
	Tables        []TableEntry  `yaml:"tables"`
	Concurrency   int           `yaml:"concurrency"`
	Retries       *int          `yaml:"retries"`
	RetryDelay    time.Duration `yaml:"retry_delay"`
	TableTimeout  time.Duration `yaml:"table_timeout"`
	Strict        bool          `yaml:"strict"`
	DeepHydration *bool         `yaml:"deep_hydration"` // nil = true (default); false skips hydrate-only columns
}

// ConnectionConfig holds all provider-specific configuration. Typed fields
// cover well-known keys (credentials, AWS multi-account). The Extra map
// captures any additional key-value pairs that get passed through to the
// Steampipe plugin as HCL connection config.
type ConnectionConfig struct {
	// Steampipe connection credentials (also passed through as HCL)
	Profile string   `yaml:"profile,omitempty"`
	Regions []string `yaml:"regions,omitempty"`

	// Provider identity (drainpipe-only, NOT passed to steampipe)
	AccountID string `yaml:"account_id,omitempty"`

	// AWS multi-account orchestration (drainpipe-only, NOT passed to steampipe)
	Accounts       []AccountEntry `yaml:"accounts,omitempty"`
	Org            *OrgConfig     `yaml:"org,omitempty"`
	Organizations  []string       `yaml:"organizations,omitempty"`
	AssumeRoleName string         `yaml:"assume_role_name,omitempty"`

	// Arbitrary Steampipe plugin config (token, max_error_retry_attempts, …)
	Extra map[string]interface{} `yaml:",inline"`
}

// ProviderDefaults holds the default plugin settings for a known provider name.
// When a user specifies "provider: aws" without explicit plugin fields, these
// defaults are applied.
type ProviderDefaults struct {
	Plugin         string // e.g. "turbot/aws@latest"
	IdentityTable  string // e.g. "aws_sts_caller_identity"
	IdentityColumn string // e.g. "account_id"
	NaturalKey     string // e.g. "arn"
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

// AccountEntry defines an explicit account to collect from.
// Use this for SSO profiles or hand-picked accounts.
type AccountEntry struct {
	Name    string   `yaml:"name"`
	Profile string   `yaml:"profile"`
	Regions []string `yaml:"regions"`
}

// OrgConfig holds multi-account discovery configuration. This struct is
// shared across providers: for AWS, Organizations holds OU IDs and
// RoleName/AssumeRoleName control STS AssumeRole; for Azure, Organizations
// holds tenant IDs (the role fields are unused since the same service
// principal credentials work across all subscriptions within a tenant).
type OrgConfig struct {
	RoleName       string        `yaml:"role_name"`
	AdminAccountID string        `yaml:"admin_account_id"`
	Organizations  []string      `yaml:"organizations"`    // AWS: OU IDs; Azure: tenant IDs
	AssumeRoleName string        `yaml:"assume_role_name"`
	Overrides      []OrgOverride `yaml:"overrides"`
}

// OrgOverride defines a per-account table override.
type OrgOverride struct {
	Match  OverrideMatch `yaml:"match"`
	Tables []TableEntry  `yaml:"tables"`
	Skip   bool          `yaml:"skip"`
}

// OverrideMatch defines which accounts an override applies to.
type OverrideMatch struct {
	AccountNames []string `yaml:"account_names"`
	AccountIDs   []string `yaml:"account_ids"`
}

// LoadDrainpipeConfig reads a drainpipe.yaml file, which may contain multiple
// YAML documents separated by "---". Each document is a complete config.
// Returns nil (not an error) if the file doesn't exist.
func LoadDrainpipeConfig(filePath string) ([]*DrainpipeConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	decoder := yaml.NewDecoder(strings.NewReader(string(data)))

	var configs []*DrainpipeConfig
	for {
		var cfg DrainpipeConfig
		err := decoder.Decode(&cfg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("parsing config file %s: %w", filePath, err)
		}
		configs = append(configs, &cfg)
	}

	if len(configs) == 0 {
		return nil, nil
	}
	return configs, nil
}

// LoadAllDrainpipeConfigs loads one or more config files. Each file may
// contain multiple YAML documents separated by "---".
// Returns nil if no files exist.
func LoadAllDrainpipeConfigs(filePaths []string) ([]*DrainpipeConfig, error) {
	var all []*DrainpipeConfig
	for _, fp := range filePaths {
		fp = strings.TrimSpace(fp)
		if fp == "" {
			continue
		}
		configs, err := LoadDrainpipeConfig(fp)
		if err != nil {
			return nil, fmt.Errorf("loading %s: %w", fp, err)
		}
		all = append(all, configs...)
	}
	if len(all) == 0 {
		return nil, nil
	}
	return all, nil
}

// EffectiveOrg returns the resolved OrgConfig, merging the shorthand
// fields (connection.organizations, connection.assume_role_name) into the
// nested connection.org block. Shorthand fields act as defaults; the nested
// org block fields take precedence.
func (c *DrainpipeConfig) EffectiveOrg() *OrgConfig {
	if c == nil {
		return nil
	}

	conn := &c.Connection
	hasShorthand := len(conn.Organizations) > 0 || conn.AssumeRoleName != ""
	if conn.Org == nil && !hasShorthand {
		return nil
	}

	var result OrgConfig
	if conn.Org != nil {
		result = *conn.Org
	}

	if len(result.Organizations) == 0 && len(conn.Organizations) > 0 {
		result.Organizations = conn.Organizations
	}
	if result.AssumeRoleName == "" && conn.AssumeRoleName != "" {
		result.AssumeRoleName = conn.AssumeRoleName
	}
	if result.RoleName == "" && result.AssumeRoleName != "" {
		result.RoleName = result.AssumeRoleName
	}

	return &result
}

// TablesForAccount resolves the table entries for a specific account by
// checking overrides in order. Returns:
//   - entries: table entries to collect (nil = use default)
//   - skip: true if the account should be skipped entirely
func (c *DrainpipeConfig) TablesForAccount(accountID, accountName string) (entries []TableEntry, skip bool) {
	if c == nil || c.Connection.Org == nil {
		return c.defaultTables(), false
	}

	for _, override := range c.Connection.Org.Overrides {
		if matchesAccount(override.Match, accountID, accountName) {
			if override.Skip {
				return nil, true
			}
			if len(override.Tables) > 0 {
				return override.Tables, false
			}
		}
	}

	return c.defaultTables(), false
}

// defaultTables returns the top-level tables config, or nil if unset.
func (c *DrainpipeConfig) defaultTables() []TableEntry {
	if c == nil || len(c.Tables) == 0 {
		return nil
	}
	return c.Tables
}

// matchesAccount checks if an override matches the given account.
func matchesAccount(m OverrideMatch, accountID, accountName string) bool {
	for _, id := range m.AccountIDs {
		if id == accountID {
			return true
		}
	}

	for _, pattern := range m.AccountNames {
		if ok, _ := path.Match(pattern, accountName); ok {
			return true
		}
	}

	return false
}

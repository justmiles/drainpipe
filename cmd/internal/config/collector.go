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

// TableEntry represents a table in the config. It supports two YAML forms:
//
//   - "aws_s3_bucket"                          (plain string, no filter)
//   - table: aws_ecs_task_definition             (object form with optional where)
//     where:
//     status: ACTIVE
type TableEntry struct {
	Name  string            `yaml:"table"`
	Where map[string]string `yaml:"where"`
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
// from a drainpipe.yaml file.
type DrainpipeConfig struct {
	Provider      string         `yaml:"provider"`
	Profile       string         `yaml:"profile"`
	Regions       []string       `yaml:"regions"`
	Tables        []TableEntry   `yaml:"tables"`
	Concurrency   int            `yaml:"concurrency"`
	Retries       int            `yaml:"retries"`
	RetryDelay    time.Duration  `yaml:"retry_delay"`
	TableTimeout  time.Duration  `yaml:"table_timeout"`
	Strict        bool           `yaml:"strict"`
	Accounts      []AccountEntry `yaml:"accounts"`
	Org           *OrgConfig     `yaml:"org"`
	Organizations []string       `yaml:"organizations"` // OU IDs to discover accounts from
	AssumeRoleName string        `yaml:"assume_role_name"` // IAM role name to assume in each account
}

// AccountEntry defines an explicit account to collect from.
// Use this for SSO profiles or hand-picked accounts.
type AccountEntry struct {
	Name    string   `yaml:"name"`
	Profile string   `yaml:"profile"`
	Regions []string `yaml:"regions"`
}

// OrgConfig holds AWS Organizations configuration.
type OrgConfig struct {
	RoleName       string        `yaml:"role_name"`
	AdminAccountID string        `yaml:"admin_account_id"`
	Organizations  []string      `yaml:"organizations"`   // OU IDs to discover accounts from
	AssumeRoleName string        `yaml:"assume_role_name"` // IAM role name to assume in each account
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

// EffectiveOrg returns the resolved OrgConfig, merging the top-level shorthand
// fields (organizations, assume_role_name) into the nested org block.
// Top-level fields act as defaults; the nested org block fields take precedence.
func (c *DrainpipeConfig) EffectiveOrg() *OrgConfig {
	if c == nil {
		return nil
	}

	hasShorthand := len(c.Organizations) > 0 || c.AssumeRoleName != ""
	if c.Org == nil && !hasShorthand {
		return nil
	}

	// Start with a copy of the nested org config (or empty)
	var result OrgConfig
	if c.Org != nil {
		result = *c.Org
	}

	// Merge top-level shorthand fields as defaults
	if len(result.Organizations) == 0 && len(c.Organizations) > 0 {
		result.Organizations = c.Organizations
	}
	if result.AssumeRoleName == "" && c.AssumeRoleName != "" {
		result.AssumeRoleName = c.AssumeRoleName
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
	if c == nil || c.Org == nil {
		return c.defaultTables(), false
	}

	for _, override := range c.Org.Overrides {
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
	// Check explicit account IDs
	for _, id := range m.AccountIDs {
		if id == accountID {
			return true
		}
	}

	// Check account name patterns (glob)
	for _, pattern := range m.AccountNames {
		if ok, _ := path.Match(pattern, accountName); ok {
			return true
		}
	}

	return false
}

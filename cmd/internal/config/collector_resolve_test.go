package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---------- ResolvePluginSpec ----------

func TestResolvePluginSpec_ExplicitPlugin(t *testing.T) {
	cfg := &DrainpipeConfig{Plugin: "turbot/cloudflare@1.5.1"}
	spec, ok := cfg.ResolvePluginSpec()
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if spec != "turbot/cloudflare@1.5.1" {
		t.Errorf("spec = %q, want turbot/cloudflare@1.5.1", spec)
	}
}

func TestResolvePluginSpec_KnownProvider(t *testing.T) {
	cfg := &DrainpipeConfig{Provider: "aws"}
	spec, ok := cfg.ResolvePluginSpec()
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if spec != "turbot/aws@latest" {
		t.Errorf("spec = %q, want turbot/aws@latest", spec)
	}
}

func TestResolvePluginSpec_UnknownProvider(t *testing.T) {
	cfg := &DrainpipeConfig{Provider: "unknown_provider"}
	_, ok := cfg.ResolvePluginSpec()
	if ok {
		t.Error("ok = true, want false for unknown provider")
	}
}

func TestResolvePluginSpec_PluginOverridesProvider(t *testing.T) {
	cfg := &DrainpipeConfig{
		Provider: "aws",
		Plugin:   "myorg/custom-aws@2.0.0",
	}
	spec, ok := cfg.ResolvePluginSpec()
	if !ok || spec != "myorg/custom-aws@2.0.0" {
		t.Errorf("Plugin should take precedence: spec = %q, ok = %v", spec, ok)
	}
}

// ---------- ResolveIdentity ----------

func TestResolveIdentity_Explicit(t *testing.T) {
	cfg := &DrainpipeConfig{
		IdentityTable:  "my_identity",
		IdentityColumn: "my_id",
	}
	table, col := cfg.ResolveIdentity()
	if table != "my_identity" || col != "my_id" {
		t.Errorf("got (%q, %q), want (my_identity, my_id)", table, col)
	}
}

func TestResolveIdentity_FromKnownProvider(t *testing.T) {
	cfg := &DrainpipeConfig{Provider: "azure"}
	table, col := cfg.ResolveIdentity()
	if table != "azure_subscription" || col != "subscription_id" {
		t.Errorf("got (%q, %q), want (azure_subscription, subscription_id)", table, col)
	}
}

func TestResolveIdentity_Partial(t *testing.T) {
	cfg := &DrainpipeConfig{
		Provider:      "aws",
		IdentityTable: "custom_identity",
	}
	table, col := cfg.ResolveIdentity()
	if table != "custom_identity" {
		t.Errorf("table = %q, want custom_identity", table)
	}
	if col != "account_id" {
		t.Errorf("col = %q, want account_id (from known provider)", col)
	}
}

// ---------- ResolveNaturalKey ----------

func TestResolveNaturalKey_Explicit(t *testing.T) {
	cfg := &DrainpipeConfig{NaturalKey: "resource_id"}
	if got := cfg.ResolveNaturalKey(); got != "resource_id" {
		t.Errorf("got %q, want resource_id", got)
	}
}

func TestResolveNaturalKey_FromKnownProvider(t *testing.T) {
	cfg := &DrainpipeConfig{Provider: "cloudflare"}
	if got := cfg.ResolveNaturalKey(); got != "id" {
		t.Errorf("got %q, want id", got)
	}
}

func TestResolveNaturalKey_UnknownProvider(t *testing.T) {
	cfg := &DrainpipeConfig{Provider: "unknown"}
	if got := cfg.ResolveNaturalKey(); got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

// ---------- ResolveCrossAccountFilterExcludeTables ----------

func TestResolveCrossAccountFilterExcludeTables_AWSExcludesOrgAccountTable(t *testing.T) {
	cfg := &DrainpipeConfig{Provider: "aws"}
	got := cfg.ResolveCrossAccountFilterExcludeTables()
	if !got["aws_organizations_account"] {
		t.Error(`aws_organizations_account should be excluded from the cross-account filter`)
	}
}

func TestResolveCrossAccountFilterExcludeTables_UnknownProvider(t *testing.T) {
	cfg := &DrainpipeConfig{Provider: "unknown"}
	if got := cfg.ResolveCrossAccountFilterExcludeTables(); got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

// ---------- ResolveConnectionHCL ----------

func TestResolveConnectionHCL_FromExtra(t *testing.T) {
	cfg := &DrainpipeConfig{
		Connection: ConnectionConfig{
			Extra: map[string]interface{}{
				"token": "my-token",
			},
		},
	}
	hcl := cfg.ResolveConnectionHCL()
	if hcl == "" {
		t.Fatal("got empty HCL")
	}
	if !strings.Contains(hcl, `token = "my-token"`) {
		t.Errorf("HCL = %q, missing token", hcl)
	}
}

func TestResolveConnectionHCL_Profile(t *testing.T) {
	cfg := &DrainpipeConfig{
		Connection: ConnectionConfig{
			Profile: "my-profile",
		},
	}
	hcl := cfg.ResolveConnectionHCL()
	if !strings.Contains(hcl, `profile = "my-profile"`) {
		t.Errorf("HCL = %q, missing profile", hcl)
	}
}

func TestResolveConnectionHCL_Regions(t *testing.T) {
	cfg := &DrainpipeConfig{
		Connection: ConnectionConfig{
			Regions: []string{"us-east-1", "eu-west-1"},
		},
	}
	hcl := cfg.ResolveConnectionHCL()
	if !strings.Contains(hcl, "regions") {
		t.Errorf("HCL = %q, missing regions", hcl)
	}
	if !strings.Contains(hcl, `"us-east-1"`) || !strings.Contains(hcl, `"eu-west-1"`) {
		t.Errorf("HCL = %q, missing region values", hcl)
	}
}

func TestResolveConnectionHCL_ExcludesOrchestration(t *testing.T) {
	cfg := &DrainpipeConfig{
		Connection: ConnectionConfig{
			Profile: "my-profile",
			Accounts: []AccountEntry{
				{Name: "prod", Profile: "prod-sso"},
			},
			Org: &OrgConfig{RoleName: "MyRole"},
		},
	}
	hcl := cfg.ResolveConnectionHCL()
	if strings.Contains(hcl, "accounts") {
		t.Errorf("HCL should not contain orchestration field 'accounts': %q", hcl)
	}
	if strings.Contains(hcl, "org") {
		t.Errorf("HCL should not contain orchestration field 'org': %q", hcl)
	}
	if !strings.Contains(hcl, `profile = "my-profile"`) {
		t.Errorf("HCL should contain profile: %q", hcl)
	}
}

func TestResolveConnectionHCL_Empty(t *testing.T) {
	cfg := &DrainpipeConfig{}
	hcl := cfg.ResolveConnectionHCL()
	if hcl != "" {
		t.Errorf("got %q, want empty for no config", hcl)
	}
}

func TestResolveConnectionHCL_YAMLRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drainpipe.yaml")
	data := []byte(`
plugin: turbot/cloudflare@1.5.1
connection:
  token: "${CLOUDFLARE_API_TOKEN}"
tables:
  - "cloudflare_*"
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	configs, err := LoadDrainpipeConfig(path)
	if err != nil {
		t.Fatalf("LoadDrainpipeConfig() error = %v", err)
	}
	cfg := configs[0]
	hcl := cfg.ResolveConnectionHCL()
	if !strings.Contains(hcl, `token = "${CLOUDFLARE_API_TOKEN}"`) {
		t.Errorf("HCL = %q, missing token from Extra", hcl)
	}
}

// ---------- New config format YAML loading ----------

func TestLoadDrainpipeConfig_NewPluginFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drainpipe.yaml")
	data := []byte(`
plugin: turbot/cloudflare@1.5.1
connection:
  token: "${CLOUDFLARE_API_TOKEN}"
identity_table: cloudflare_account
identity_column: id
natural_key: id
tables:
  - "cloudflare_*"
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	configs, err := LoadDrainpipeConfig(path)
	if err != nil {
		t.Fatalf("LoadDrainpipeConfig() error = %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("len = %d, want 1", len(configs))
	}
	cfg := configs[0]
	if cfg.Plugin != "turbot/cloudflare@1.5.1" {
		t.Errorf("Plugin = %q", cfg.Plugin)
	}
	if cfg.IdentityTable != "cloudflare_account" {
		t.Errorf("IdentityTable = %q", cfg.IdentityTable)
	}
	if cfg.IdentityColumn != "id" {
		t.Errorf("IdentityColumn = %q", cfg.IdentityColumn)
	}
	if cfg.NaturalKey != "id" {
		t.Errorf("NaturalKey = %q", cfg.NaturalKey)
	}
	if cfg.Connection.Extra == nil || cfg.Connection.Extra["token"] != "${CLOUDFLARE_API_TOKEN}" {
		t.Errorf("Connection.Extra = %v", cfg.Connection.Extra)
	}
}

// ---------- TableEntry.Key ----------

func TestLoadDrainpipeConfig_TableEntryKey_Single(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key.yaml")
	data := []byte(`
provider: aws
tables:
  - table: aws_costoptimizationhub_recommendation
    key: [recommendation_id]
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	configs, err := LoadDrainpipeConfig(path)
	if err != nil {
		t.Fatalf("LoadDrainpipeConfig() error = %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("len(configs) = %d, want 1", len(configs))
	}
	tables := configs[0].Tables
	if len(tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(tables))
	}
	if tables[0].Name != "aws_costoptimizationhub_recommendation" {
		t.Errorf("Name = %q", tables[0].Name)
	}
	if len(tables[0].Key) != 1 || tables[0].Key[0] != "recommendation_id" {
		t.Errorf("Key = %v, want [recommendation_id]", tables[0].Key)
	}
}

func TestLoadDrainpipeConfig_TableEntryKey_Composite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "composite_key.yaml")
	data := []byte(`
provider: aws
tables:
  - table: aws_some_table
    key: [account_id, region, name]
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	configs, err := LoadDrainpipeConfig(path)
	if err != nil {
		t.Fatalf("LoadDrainpipeConfig() error = %v", err)
	}
	tables := configs[0].Tables
	if len(tables[0].Key) != 3 {
		t.Fatalf("len(Key) = %d, want 3", len(tables[0].Key))
	}
	want := []string{"account_id", "region", "name"}
	for i, k := range want {
		if tables[0].Key[i] != k {
			t.Errorf("Key[%d] = %q, want %q", i, tables[0].Key[i], k)
		}
	}
}

func TestLoadDrainpipeConfig_TableEntryKey_AbsentIsNil(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nokey.yaml")
	data := []byte(`
provider: aws
tables:
  - table: aws_s3_bucket
    where:
      region: us-east-1
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	configs, err := LoadDrainpipeConfig(path)
	if err != nil {
		t.Fatalf("LoadDrainpipeConfig() error = %v", err)
	}
	tables := configs[0].Tables
	if len(tables[0].Key) != 0 {
		t.Errorf("Key = %v, want nil/empty when not specified", tables[0].Key)
	}
}

func TestLoadDrainpipeConfig_TableEntryKey_WithWhere(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "key_where.yaml")
	data := []byte(`
provider: aws
tables:
  - table: aws_costoptimizationhub_recommendation
    key: [recommendation_id]
    where:
      status: active
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	configs, err := LoadDrainpipeConfig(path)
	if err != nil {
		t.Fatalf("LoadDrainpipeConfig() error = %v", err)
	}
	tableEntry := configs[0].Tables[0]
	if len(tableEntry.Key) != 1 || tableEntry.Key[0] != "recommendation_id" {
		t.Errorf("Key = %v", tableEntry.Key)
	}
	if tableEntry.Where["status"] != "active" {
		t.Errorf("Where[status] = %q", tableEntry.Where["status"])
	}
}

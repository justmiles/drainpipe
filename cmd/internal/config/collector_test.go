package config

import (
	"os"
	"path/filepath"
	"testing"
)

// ---------- LoadDrainpipeConfig ----------

func TestLoadDrainpipeConfig_ValidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drainpipe.yaml")
	data := []byte(`
provider: aws
profile: dev-profile
regions:
  - us-east-1
  - eu-west-1
tables:
  - aws_s3_bucket
  - aws_ec2_instance
concurrency: 4
retries: 5
strict: true
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	configs, err := LoadDrainpipeConfig(path)
	if err != nil {
		t.Fatalf("LoadDrainpipeConfig() error = %v", err)
	}
	if len(configs) != 1 {
		t.Fatalf("LoadDrainpipeConfig() returned %d configs, want 1", len(configs))
	}
	cfg := configs[0]
	if cfg.Provider != "aws" {
		t.Errorf("Provider = %q, want %q", cfg.Provider, "aws")
	}
	if cfg.Profile != "dev-profile" {
		t.Errorf("Profile = %q, want %q", cfg.Profile, "dev-profile")
	}
	if len(cfg.Regions) != 2 {
		t.Errorf("len(Regions) = %d, want 2", len(cfg.Regions))
	}
	if len(cfg.Tables) != 2 {
		t.Errorf("len(Tables) = %d, want 2", len(cfg.Tables))
	}
	if cfg.Tables[0].Name != "aws_s3_bucket" {
		t.Errorf("Tables[0].Name = %q, want %q", cfg.Tables[0].Name, "aws_s3_bucket")
	}
	if cfg.Concurrency != 4 {
		t.Errorf("Concurrency = %d, want 4", cfg.Concurrency)
	}
	if cfg.Retries != 5 {
		t.Errorf("Retries = %d, want 5", cfg.Retries)
	}
	if !cfg.Strict {
		t.Error("Strict = false, want true")
	}
}

func TestLoadDrainpipeConfig_MissingFile(t *testing.T) {
	configs, err := LoadDrainpipeConfig("/nonexistent/path/drainpipe.yaml")
	if err != nil {
		t.Fatalf("Expected nil error for missing file, got %v", err)
	}
	if configs != nil {
		t.Error("Expected nil for missing file")
	}
}

func TestLoadDrainpipeConfig_MalformedYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("{{{{invalid yaml"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadDrainpipeConfig(path)
	if err == nil {
		t.Error("Expected error for malformed YAML, got nil")
	}
}

func TestLoadDrainpipeConfig_WithAccounts(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drainpipe.yaml")
	data := []byte(`
provider: aws
accounts:
  - name: production
    profile: prod-sso
    regions: [us-east-1]
  - name: staging
    profile: staging-sso
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
	cfg := configs[0]
	if len(cfg.Accounts) != 2 {
		t.Fatalf("len(Accounts) = %d, want 2", len(cfg.Accounts))
	}
	if cfg.Accounts[0].Name != "production" {
		t.Errorf("Accounts[0].Name = %q, want %q", cfg.Accounts[0].Name, "production")
	}
	if cfg.Accounts[0].Profile != "prod-sso" {
		t.Errorf("Accounts[0].Profile = %q, want %q", cfg.Accounts[0].Profile, "prod-sso")
	}
	if len(cfg.Accounts[0].Regions) != 1 {
		t.Errorf("len(Accounts[0].Regions) = %d, want 1", len(cfg.Accounts[0].Regions))
	}
}

func TestLoadDrainpipeConfig_WithOrgOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "drainpipe.yaml")
	data := []byte(`
provider: aws
org:
  role_name: OrganizationAccountAccessRole
  admin_account_id: "111111111111"
  overrides:
    - match:
        account_ids: ["222222222222"]
      tables: [aws_s3_bucket]
    - match:
        account_names: ["sandbox-*"]
      skip: true
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
	cfg := configs[0]
	if cfg.Org == nil {
		t.Fatal("Org is nil, expected config")
	}
	if cfg.Org.RoleName != "OrganizationAccountAccessRole" {
		t.Errorf("Org.RoleName = %q", cfg.Org.RoleName)
	}
	if len(cfg.Org.Overrides) != 2 {
		t.Fatalf("len(Org.Overrides) = %d, want 2", len(cfg.Org.Overrides))
	}
	if !cfg.Org.Overrides[1].Skip {
		t.Error("Overrides[1].Skip = false, want true")
	}
}

// ---------- Multi-document YAML ----------

func TestLoadDrainpipeConfig_MultiDocument(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multi.yaml")
	data := []byte(`
provider: aws
profile: management
tables:
  - aws_organizations_account
---
provider: aws
concurrency: 5
tables:
  - aws_ec2_instance
  - aws_s3_bucket
accounts:
  - name: prod
    profile: prod-sso
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	configs, err := LoadDrainpipeConfig(path)
	if err != nil {
		t.Fatalf("LoadDrainpipeConfig() error = %v", err)
	}
	if len(configs) != 2 {
		t.Fatalf("len(configs) = %d, want 2", len(configs))
	}

	if configs[0].Profile != "management" {
		t.Errorf("configs[0].Profile = %q, want %q", configs[0].Profile, "management")
	}
	if len(configs[0].Tables) != 1 || configs[0].Tables[0].Name != "aws_organizations_account" {
		t.Errorf("configs[0].Tables = %v", configs[0].Tables)
	}
	if configs[1].Concurrency != 5 {
		t.Errorf("configs[1].Concurrency = %d, want 5", configs[1].Concurrency)
	}
	if len(configs[1].Tables) != 2 {
		t.Errorf("configs[1].Tables = %v", configs[1].Tables)
	}
	if len(configs[1].Accounts) != 1 || configs[1].Accounts[0].Name != "prod" {
		t.Errorf("configs[1].Accounts = %v", configs[1].Accounts)
	}
}

// ---------- TableEntry flexible YAML ----------

func TestLoadDrainpipeConfig_TableEntryMixed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mixed.yaml")
	data := []byte(`
provider: aws
tables:
  - aws_s3_bucket
  - table: aws_ecs_task_definition
    where:
      status: ACTIVE
  - aws_ec2_instance
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
	if len(tables) != 3 {
		t.Fatalf("len(Tables) = %d, want 3", len(tables))
	}

	// Plain string entry
	if tables[0].Name != "aws_s3_bucket" {
		t.Errorf("tables[0].Name = %q", tables[0].Name)
	}
	if tables[0].Where != nil {
		t.Errorf("tables[0].Where = %v, want nil", tables[0].Where)
	}

	// Object entry with where
	if tables[1].Name != "aws_ecs_task_definition" {
		t.Errorf("tables[1].Name = %q", tables[1].Name)
	}
	if tables[1].Where == nil || tables[1].Where["status"] != "ACTIVE" {
		t.Errorf("tables[1].Where = %v, want {status: ACTIVE}", tables[1].Where)
	}

	// Plain string entry
	if tables[2].Name != "aws_ec2_instance" {
		t.Errorf("tables[2].Name = %q", tables[2].Name)
	}
}

func TestLoadDrainpipeConfig_TableEntryMultipleWhere(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "multiwhere.yaml")
	data := []byte(`
provider: aws
tables:
  - table: aws_ec2_instance
    where:
      instance_state: running
      instance_type: t3.micro
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	configs, err := LoadDrainpipeConfig(path)
	if err != nil {
		t.Fatalf("LoadDrainpipeConfig() error = %v", err)
	}
	tables := configs[0].Tables
	if len(tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(tables))
	}
	if tables[0].Name != "aws_ec2_instance" {
		t.Errorf("Name = %q", tables[0].Name)
	}
	if len(tables[0].Where) != 2 {
		t.Errorf("len(Where) = %d, want 2", len(tables[0].Where))
	}
	if tables[0].Where["instance_state"] != "running" {
		t.Errorf("Where[instance_state] = %q", tables[0].Where["instance_state"])
	}
	if tables[0].Where["instance_type"] != "t3.micro" {
		t.Errorf("Where[instance_type] = %q", tables[0].Where["instance_type"])
	}
}

// ---------- TableNames / TableEntryMap ----------

func TestTableNames(t *testing.T) {
	entries := []TableEntry{
		{Name: "aws_s3_bucket"},
		{Name: "aws_ec2_instance", Where: map[string]string{"status": "running"}},
	}
	names := TableNames(entries)
	if len(names) != 2 || names[0] != "aws_s3_bucket" || names[1] != "aws_ec2_instance" {
		t.Errorf("TableNames() = %v", names)
	}
}

func TestTableEntryMap(t *testing.T) {
	entries := []TableEntry{
		{Name: "aws_s3_bucket"},
		{Name: "aws_ecs_task_definition", Where: map[string]string{"status": "ACTIVE"}},
	}
	m := TableEntryMap(entries)
	if len(m) != 2 {
		t.Fatalf("len(map) = %d, want 2", len(m))
	}
	if m["aws_ecs_task_definition"].Where["status"] != "ACTIVE" {
		t.Errorf("map lookup failed for where filter")
	}
}

// ---------- LoadAllDrainpipeConfigs ----------

func TestLoadAllDrainpipeConfigs_MultipleFiles(t *testing.T) {
	dir := t.TempDir()

	path1 := filepath.Join(dir, "a.yaml")
	if err := os.WriteFile(path1, []byte("provider: aws\ntables: [aws_s3_bucket]\n"), 0644); err != nil {
		t.Fatal(err)
	}
	path2 := filepath.Join(dir, "b.yaml")
	if err := os.WriteFile(path2, []byte("provider: azure\ntables: [azure_vm]\n"), 0644); err != nil {
		t.Fatal(err)
	}

	configs, err := LoadAllDrainpipeConfigs([]string{path1, path2})
	if err != nil {
		t.Fatalf("LoadAllDrainpipeConfigs() error = %v", err)
	}
	if len(configs) != 2 {
		t.Fatalf("len(configs) = %d, want 2", len(configs))
	}
	if configs[0].Provider != "aws" {
		t.Errorf("configs[0].Provider = %q", configs[0].Provider)
	}
	if configs[1].Provider != "azure" {
		t.Errorf("configs[1].Provider = %q", configs[1].Provider)
	}
}

func TestLoadAllDrainpipeConfigs_MixedMultiDocAndFiles(t *testing.T) {
	dir := t.TempDir()

	path1 := filepath.Join(dir, "multi.yaml")
	if err := os.WriteFile(path1, []byte("provider: aws\n---\nprovider: gcp\n"), 0644); err != nil {
		t.Fatal(err)
	}
	path2 := filepath.Join(dir, "single.yaml")
	if err := os.WriteFile(path2, []byte("provider: azure\n"), 0644); err != nil {
		t.Fatal(err)
	}

	configs, err := LoadAllDrainpipeConfigs([]string{path1, path2})
	if err != nil {
		t.Fatalf("LoadAllDrainpipeConfigs() error = %v", err)
	}
	if len(configs) != 3 {
		t.Fatalf("len(configs) = %d, want 3", len(configs))
	}
}

func TestLoadAllDrainpipeConfigs_MissingFiles(t *testing.T) {
	configs, err := LoadAllDrainpipeConfigs([]string{"/nonexistent/a.yaml", "/nonexistent/b.yaml"})
	if err != nil {
		t.Fatalf("expected nil error for missing files, got %v", err)
	}
	if configs != nil {
		t.Error("expected nil for all missing files")
	}
}

func TestLoadAllDrainpipeConfigs_EmptyPaths(t *testing.T) {
	configs, err := LoadAllDrainpipeConfigs([]string{"", "  ", ""})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if configs != nil {
		t.Error("expected nil for empty paths")
	}
}

// ---------- TablesForAccount ----------

func te(name string) TableEntry { return TableEntry{Name: name} }

func TestTablesForAccount_NoOverrides(t *testing.T) {
	cfg := &DrainpipeConfig{
		Tables: []TableEntry{te("aws_s3_bucket")},
	}
	entries, skip := cfg.TablesForAccount("123", "myaccount")
	if skip {
		t.Error("skip = true, want false")
	}
	if len(entries) != 1 || entries[0].Name != "aws_s3_bucket" {
		t.Errorf("entries = %v, want [{aws_s3_bucket}]", entries)
	}
}

func TestTablesForAccount_NilConfig(t *testing.T) {
	var cfg *DrainpipeConfig
	entries, skip := cfg.TablesForAccount("123", "test")
	if skip {
		t.Error("skip = true, want false for nil config")
	}
	if entries != nil {
		t.Errorf("entries = %v, want nil for nil config", entries)
	}
}

func TestTablesForAccount_MatchByAccountID(t *testing.T) {
	cfg := &DrainpipeConfig{
		Tables: []TableEntry{te("aws_s3_bucket")},
		Org: &OrgConfig{
			Overrides: []OrgOverride{
				{
					Match:  OverrideMatch{AccountIDs: []string{"222222222222"}},
					Tables: []TableEntry{te("aws_ec2_instance")},
				},
			},
		},
	}

	entries, skip := cfg.TablesForAccount("222222222222", "prod")
	if skip {
		t.Error("skip = true, want false")
	}
	if len(entries) != 1 || entries[0].Name != "aws_ec2_instance" {
		t.Errorf("entries = %v, want [{aws_ec2_instance}]", entries)
	}
}

func TestTablesForAccount_MatchByNameGlob(t *testing.T) {
	cfg := &DrainpipeConfig{
		Tables: []TableEntry{te("aws_s3_bucket")},
		Org: &OrgConfig{
			Overrides: []OrgOverride{
				{
					Match:  OverrideMatch{AccountNames: []string{"sandbox-*"}},
					Tables: []TableEntry{te("aws_vpc")},
				},
			},
		},
	}

	entries, skip := cfg.TablesForAccount("999", "sandbox-dev")
	if skip {
		t.Error("skip = true, want false")
	}
	if len(entries) != 1 || entries[0].Name != "aws_vpc" {
		t.Errorf("entries = %v, want [{aws_vpc}]", entries)
	}
}

func TestTablesForAccount_Skip(t *testing.T) {
	cfg := &DrainpipeConfig{
		Org: &OrgConfig{
			Overrides: []OrgOverride{
				{
					Match: OverrideMatch{AccountIDs: []string{"333"}},
					Skip:  true,
				},
			},
		},
	}

	_, skip := cfg.TablesForAccount("333", "skipme")
	if !skip {
		t.Error("skip = false, want true")
	}
}

func TestTablesForAccount_NoMatch_ReturnsDefault(t *testing.T) {
	cfg := &DrainpipeConfig{
		Tables: []TableEntry{te("aws_default")},
		Org: &OrgConfig{
			Overrides: []OrgOverride{
				{
					Match:  OverrideMatch{AccountIDs: []string{"999"}},
					Tables: []TableEntry{te("aws_special")},
				},
			},
		},
	}

	entries, skip := cfg.TablesForAccount("111", "other")
	if skip {
		t.Error("skip = true, want false")
	}
	if len(entries) != 1 || entries[0].Name != "aws_default" {
		t.Errorf("entries = %v, want [{aws_default}]", entries)
	}
}

func TestTablesForAccount_NoDefaultTables_ReturnsNil(t *testing.T) {
	cfg := &DrainpipeConfig{
		Org: &OrgConfig{
			Overrides: []OrgOverride{
				{
					Match:  OverrideMatch{AccountIDs: []string{"999"}},
					Tables: []TableEntry{te("aws_special")},
				},
			},
		},
	}

	entries, skip := cfg.TablesForAccount("111", "other")
	if skip {
		t.Error("skip = true, want false")
	}
	if entries != nil {
		t.Errorf("entries = %v, want nil", entries)
	}
}

// ---------- matchesAccount ----------

func TestMatchesAccount_ByID(t *testing.T) {
	m := OverrideMatch{AccountIDs: []string{"111", "222"}}
	if !matchesAccount(m, "222", "") {
		t.Error("expected match by ID")
	}
}

func TestMatchesAccount_ByNameGlob(t *testing.T) {
	m := OverrideMatch{AccountNames: []string{"prod-*"}}
	if !matchesAccount(m, "", "prod-east") {
		t.Error("expected match by name glob")
	}
}

func TestMatchesAccount_NoMatch(t *testing.T) {
	m := OverrideMatch{
		AccountIDs:   []string{"999"},
		AccountNames: []string{"staging-*"},
	}
	if matchesAccount(m, "111", "production") {
		t.Error("expected no match")
	}
}

func TestMatchesAccount_EmptyMatch(t *testing.T) {
	m := OverrideMatch{}
	if matchesAccount(m, "111", "any") {
		t.Error("empty match should not match anything")
	}
}

func TestMatchesAccount_ExactName(t *testing.T) {
	m := OverrideMatch{AccountNames: []string{"production"}}
	if !matchesAccount(m, "", "production") {
		t.Error("expected exact name match")
	}
	if matchesAccount(m, "", "production-east") {
		t.Error("exact name should not match prefix")
	}
}

// ---------- defaultTables ----------

func TestDefaultTables_Nil(t *testing.T) {
	var cfg *DrainpipeConfig
	if got := cfg.defaultTables(); got != nil {
		t.Errorf("defaultTables() on nil = %v, want nil", got)
	}
}

func TestDefaultTables_Empty(t *testing.T) {
	cfg := &DrainpipeConfig{}
	if got := cfg.defaultTables(); got != nil {
		t.Errorf("defaultTables() on empty = %v, want nil", got)
	}
}

func TestDefaultTables_Present(t *testing.T) {
	cfg := &DrainpipeConfig{Tables: []TableEntry{te("aws_s3_bucket")}}
	got := cfg.defaultTables()
	if len(got) != 1 || got[0].Name != "aws_s3_bucket" {
		t.Errorf("defaultTables() = %v, want [{aws_s3_bucket}]", got)
	}
}

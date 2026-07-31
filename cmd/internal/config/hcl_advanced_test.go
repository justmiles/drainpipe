package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadHCLConfig_ConnectionExtraAttrs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "aws" {
  plugin                   = "turbot/aws@latest"
  profile                  = "my-profile"
  regions                  = ["us-east-1"]
  max_error_retry_attempts = 10
}

drainpipe "test" {
  connection = "aws"
  tables     = ["aws_s3_bucket"]
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadHCLConfig(path)
	if err != nil {
		t.Fatalf("LoadHCLConfig() error = %v", err)
	}
	cfg := result.Configs[0]
	if cfg.Connection.Profile != "my-profile" {
		t.Errorf("Profile = %q, want my-profile", cfg.Connection.Profile)
	}
	if len(cfg.Connection.Regions) != 1 || cfg.Connection.Regions[0] != "us-east-1" {
		t.Errorf("Regions = %v, want [us-east-1]", cfg.Connection.Regions)
	}

	retries, ok := cfg.Connection.Extra["max_error_retry_attempts"]
	if !ok {
		t.Fatal("Extra missing max_error_retry_attempts")
	}
	if retries != int64(10) {
		t.Errorf("max_error_retry_attempts = %v (%T), want 10", retries, retries)
	}

	if _, hasPlugin := cfg.Connection.Extra["plugin"]; hasPlugin {
		t.Error("Extra should NOT contain 'plugin' (it is a typed field, not pass-through)")
	}

	hcl := cfg.ResolveConnectionHCL()
	if strings.Contains(hcl, "plugin") {
		t.Errorf("ResolveConnectionHCL() should not contain 'plugin', got: %q", hcl)
	}
}

func TestLoadHCLConfig_WithFilterQuery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "aws" {
  plugin = "turbot/aws@latest"
}

drainpipe "test" {
  connection = "aws"

  tables = ["aws_ecs_service"]

  table "aws_ecs_task_definition" {
    filter_query {
      column = "task_definition_arn"
      query  = "SELECT DISTINCT task_definition FROM aws_ecs_service WHERE _deleted_at IS NULL"
    }
    columns = ["task_definition_arn", "family", "revision", "status"]
  }
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadHCLConfig(path)
	if err != nil {
		t.Fatalf("LoadHCLConfig() error = %v", err)
	}
	cfg := result.Configs[0]

	if len(cfg.Tables) != 2 {
		t.Fatalf("len(Tables) = %d, want 2", len(cfg.Tables))
	}

	if cfg.Tables[0].Name != "aws_ecs_service" {
		t.Errorf("Tables[0].Name = %q, want aws_ecs_service", cfg.Tables[0].Name)
	}
	if cfg.Tables[0].FilterQuery != nil {
		t.Error("Tables[0].FilterQuery should be nil")
	}

	td := cfg.Tables[1]
	if td.Name != "aws_ecs_task_definition" {
		t.Errorf("Tables[1].Name = %q, want aws_ecs_task_definition", td.Name)
	}
	if td.FilterQuery == nil {
		t.Fatal("Tables[1].FilterQuery is nil")
	}
	if td.FilterQuery.Column != "task_definition_arn" {
		t.Errorf("FilterQuery.Column = %q, want task_definition_arn", td.FilterQuery.Column)
	}
	if td.FilterQuery.Query != "SELECT DISTINCT task_definition FROM aws_ecs_service WHERE _deleted_at IS NULL" {
		t.Errorf("FilterQuery.Query = %q", td.FilterQuery.Query)
	}
	if len(td.Columns) != 4 {
		t.Errorf("len(Columns) = %d, want 4", len(td.Columns))
	}
}

func TestLoadHCLConfig_FilterQueryWithWhere(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "aws" {
  plugin = "turbot/aws@latest"
}

drainpipe "test" {
  connection = "aws"

  table "aws_ecs_task_definition" {
    where = {
      status = "ACTIVE"
    }
    filter_query {
      column = "task_definition_arn"
      query  = "SELECT DISTINCT task_definition FROM aws_ecs_service"
    }
  }
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadHCLConfig(path)
	if err != nil {
		t.Fatalf("LoadHCLConfig() error = %v", err)
	}
	cfg := result.Configs[0]

	if len(cfg.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(cfg.Tables))
	}

	td := cfg.Tables[0]
	if td.Where["status"] != "ACTIVE" {
		t.Errorf("Where[status] = %q, want ACTIVE", td.Where["status"])
	}
	if td.FilterQuery == nil {
		t.Fatal("FilterQuery is nil")
	}
	if td.FilterQuery.Column != "task_definition_arn" {
		t.Errorf("FilterQuery.Column = %q", td.FilterQuery.Column)
	}
}

func TestLoadHCLConfig_EnvVarExpansion(t *testing.T) {
	t.Setenv("DRAINPIPE_TEST_TENANT", "my-tenant-id")
	t.Setenv("DRAINPIPE_TEST_CLIENT", "my-client-id")
	t.Setenv("DRAINPIPE_TEST_SECRET", "s3cret!")

	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "azure" {
  plugin        = "turbot/azure@latest"
  tenant_id     = "${DRAINPIPE_TEST_TENANT}"
  client_id     = "${DRAINPIPE_TEST_CLIENT}"
  client_secret = "${DRAINPIPE_TEST_SECRET}"
}

drainpipe "test" {
  connection = "azure"
  tables     = ["azure_subscription"]
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadHCLConfig(path)
	if err != nil {
		t.Fatalf("LoadHCLConfig() error = %v", err)
	}
	cfg := result.Configs[0]

	if cfg.Connection.Extra["tenant_id"] != "my-tenant-id" {
		t.Errorf("tenant_id = %q, want my-tenant-id", cfg.Connection.Extra["tenant_id"])
	}
	if cfg.Connection.Extra["client_id"] != "my-client-id" {
		t.Errorf("client_id = %q, want my-client-id", cfg.Connection.Extra["client_id"])
	}
	if cfg.Connection.Extra["client_secret"] != "s3cret!" {
		t.Errorf("client_secret = %q, want s3cret!", cfg.Connection.Extra["client_secret"])
	}

	hcl := cfg.ResolveConnectionHCL()
	if !strings.Contains(hcl, `tenant_id = "my-tenant-id"`) {
		t.Errorf("ResolveConnectionHCL() missing expanded tenant_id: %q", hcl)
	}
}

func TestLoadHCLConfig_EnvVarMixed(t *testing.T) {
	t.Setenv("DRAINPIPE_TEST_TOKEN", "tok-12345")

	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "cf" {
  plugin = "turbot/cloudflare@latest"
  token  = "${DRAINPIPE_TEST_TOKEN}"
}

drainpipe "test" {
  connection  = "cf"
  concurrency = 3
  tables      = ["cloudflare_zone"]
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadHCLConfig(path)
	if err != nil {
		t.Fatalf("LoadHCLConfig() error = %v", err)
	}
	cfg := result.Configs[0]

	if cfg.Connection.Extra["token"] != "tok-12345" {
		t.Errorf("token = %q, want tok-12345", cfg.Connection.Extra["token"])
	}
	if cfg.Concurrency != 3 {
		t.Errorf("Concurrency = %d, want 3", cfg.Concurrency)
	}
}

func TestLoadHCLConfig_MultipleJobs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "aws" {
  plugin = "turbot/aws@latest"
}

connection "cloudflare" {
  plugin = "turbot/cloudflare@latest"
}

drainpipe "aws_inv" {
  connection = "aws"
  tables     = ["aws_s3_bucket"]
}

drainpipe "cf_inv" {
  connection  = "cloudflare"
  concurrency = 5
  tables      = ["cloudflare_zone"]
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadHCLConfig(path)
	if err != nil {
		t.Fatalf("LoadHCLConfig() error = %v", err)
	}
	if len(result.Configs) != 2 {
		t.Fatalf("len(Configs) = %d, want 2", len(result.Configs))
	}
	if result.Configs[0].Plugin != "turbot/aws@latest" {
		t.Errorf("Configs[0].Plugin = %q", result.Configs[0].Plugin)
	}
	if result.Configs[1].Concurrency != 5 {
		t.Errorf("Configs[1].Concurrency = %d, want 5", result.Configs[1].Concurrency)
	}
}

// ---------- HCLTableBlock.Key ----------

func TestLoadHCLConfig_TableBlock_Key_Single(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "aws" {
  plugin = "turbot/aws@latest"
}

drainpipe "test" {
  connection = "aws"

  table "aws_costoptimizationhub_recommendation" {
    key = ["recommendation_id"]
  }
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadHCLConfig(path)
	if err != nil {
		t.Fatalf("LoadHCLConfig() error = %v", err)
	}
	cfg := result.Configs[0]
	if len(cfg.Tables) != 1 {
		t.Fatalf("len(Tables) = %d, want 1", len(cfg.Tables))
	}
	tableEntry := cfg.Tables[0]
	if tableEntry.Name != "aws_costoptimizationhub_recommendation" {
		t.Errorf("Name = %q", tableEntry.Name)
	}
	if len(tableEntry.Key) != 1 || tableEntry.Key[0] != "recommendation_id" {
		t.Errorf("Key = %v, want [recommendation_id]", tableEntry.Key)
	}
}

func TestLoadHCLConfig_TableBlock_Key_Composite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "aws" {
  plugin = "turbot/aws@latest"
}

drainpipe "test" {
  connection = "aws"

  table "aws_some_table" {
    key = ["account_id", "region", "name"]
  }
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadHCLConfig(path)
	if err != nil {
		t.Fatalf("LoadHCLConfig() error = %v", err)
	}
	tableEntry := result.Configs[0].Tables[0]
	if len(tableEntry.Key) != 3 {
		t.Fatalf("len(Key) = %d, want 3", len(tableEntry.Key))
	}
	want := []string{"account_id", "region", "name"}
	for i, k := range want {
		if tableEntry.Key[i] != k {
			t.Errorf("Key[%d] = %q, want %q", i, tableEntry.Key[i], k)
		}
	}
}

func TestLoadHCLConfig_TableBlock_Key_WithWhere(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "aws" {
  plugin = "turbot/aws@latest"
}

drainpipe "test" {
  connection = "aws"

  table "aws_costoptimizationhub_recommendation" {
    key = ["recommendation_id"]
    where = {
      status = "active"
    }
  }
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadHCLConfig(path)
	if err != nil {
		t.Fatalf("LoadHCLConfig() error = %v", err)
	}
	tableEntry := result.Configs[0].Tables[0]
	if len(tableEntry.Key) != 1 || tableEntry.Key[0] != "recommendation_id" {
		t.Errorf("Key = %v", tableEntry.Key)
	}
	if tableEntry.Where["status"] != "active" {
		t.Errorf("Where[status] = %q", tableEntry.Where["status"])
	}
}

func TestLoadHCLConfig_TableBlock_Key_Absent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "aws" {
  plugin = "turbot/aws@latest"
}

drainpipe "test" {
  connection = "aws"

  table "aws_s3_bucket" {
    where = {
      region = "us-east-1"
    }
  }
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadHCLConfig(path)
	if err != nil {
		t.Fatalf("LoadHCLConfig() error = %v", err)
	}
	tableEntry := result.Configs[0].Tables[0]
	if len(tableEntry.Key) != 0 {
		t.Errorf("Key = %v, want empty when not specified", tableEntry.Key)
	}
}

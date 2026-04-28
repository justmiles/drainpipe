package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadHCLConfig_Simple(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "cloudflare" {
  plugin = "turbot/cloudflare@latest"
}

drainpipe "test" {
  connection  = "cloudflare"
  concurrency = 5
  retries     = 3
  retry_delay = "10s"
  strict      = true

  tables = ["cloudflare_zone", "cloudflare_account"]
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	result, err := LoadHCLConfig(path)
	if err != nil {
		t.Fatalf("LoadHCLConfig() error = %v", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	if len(result.Configs) != 1 {
		t.Fatalf("len(Configs) = %d, want 1", len(result.Configs))
	}

	cfg := result.Configs[0]
	if cfg.Plugin != "turbot/cloudflare@latest" {
		t.Errorf("Plugin = %q, want turbot/cloudflare@latest", cfg.Plugin)
	}
	if cfg.Provider != "cloudflare" {
		t.Errorf("Provider = %q, want cloudflare (inferred from plugin)", cfg.Provider)
	}
	if cfg.Concurrency != 5 {
		t.Errorf("Concurrency = %d, want 5", cfg.Concurrency)
	}
	if cfg.Retries != 3 {
		t.Errorf("Retries = %d, want 3", cfg.Retries)
	}
	if cfg.RetryDelay.Seconds() != 10 {
		t.Errorf("RetryDelay = %v, want 10s", cfg.RetryDelay)
	}
	if !cfg.Strict {
		t.Error("Strict = false, want true")
	}
	if len(cfg.Tables) != 2 {
		t.Fatalf("len(Tables) = %d, want 2", len(cfg.Tables))
	}
	if cfg.Tables[0].Name != "cloudflare_zone" {
		t.Errorf("Tables[0].Name = %q, want cloudflare_zone", cfg.Tables[0].Name)
	}
}

func TestLoadHCLConfig_WithRateLimiters(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "aws" {
  plugin  = "turbot/aws@latest"
  regions = ["us-east-1", "us-west-2"]
}

plugin "aws" {
  memory_max_mb = 2048

  limiter "global_rate" {
    bucket_size     = 1000
    fill_rate       = 500
    max_concurrency = 50
    scope           = ["connection", "region"]
  }

  limiter "s3_throttle" {
    bucket_size = 100
    fill_rate   = 50
    where       = "service = 's3'"
  }
}

drainpipe "inventory" {
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

	limiters, ok := result.RateLimiters["aws"]
	if !ok {
		t.Fatal("no rate limiters for plugin 'aws'")
	}
	if len(limiters) != 2 {
		t.Fatalf("len(limiters) = %d, want 2", len(limiters))
	}
	if limiters[0].Name != "global_rate" {
		t.Errorf("limiters[0].Name = %q, want global_rate", limiters[0].Name)
	}
	if limiters[0].BucketSize != 1000 {
		t.Errorf("limiters[0].BucketSize = %d, want 1000", limiters[0].BucketSize)
	}
	if limiters[0].MaxConcurrency != 50 {
		t.Errorf("limiters[0].MaxConcurrency = %d, want 50", limiters[0].MaxConcurrency)
	}
	if len(limiters[0].Scope) != 2 {
		t.Errorf("limiters[0].Scope = %v, want [connection region]", limiters[0].Scope)
	}
	if limiters[1].Where != "service = 's3'" {
		t.Errorf("limiters[1].Where = %q", limiters[1].Where)
	}

	cfg := result.Configs[0]
	if cfg.Connection.Regions[0] != "us-east-1" {
		t.Errorf("Regions[0] = %q, want us-east-1", cfg.Connection.Regions[0])
	}
}

func TestLoadHCLConfig_WithOrg(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "aws" {
  plugin = "turbot/aws@latest"
  regions = ["us-east-1", "us-west-2"]
}

drainpipe "inventory" {
  connection  = "aws"
  concurrency = 2
  strict      = true

  org {
    assume_role_name = "cmdb-agent"
    organizations = [
      "ou-568j-vck1nu4x",
      "ou-568j-3ukb66hn",
    ]

    override {
      match_account_names = ["*-dev", "*-sandbox"]
      tables              = ["aws_ec2_instance", "aws_s3_bucket"]
    }

    override {
      match_account_ids = ["999999999999"]
      skip              = true
    }
  }

  tables = ["aws_ec2_instance", "aws_s3_bucket", "aws_vpc"]
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

	if cfg.Connection.Org == nil {
		t.Fatal("Org is nil")
	}
	if cfg.Connection.Org.AssumeRoleName != "cmdb-agent" {
		t.Errorf("AssumeRoleName = %q", cfg.Connection.Org.AssumeRoleName)
	}
	if len(cfg.Connection.Org.Organizations) != 2 {
		t.Fatalf("len(Organizations) = %d, want 2", len(cfg.Connection.Org.Organizations))
	}
	if len(cfg.Connection.Org.Overrides) != 2 {
		t.Fatalf("len(Overrides) = %d, want 2", len(cfg.Connection.Org.Overrides))
	}
	if !cfg.Connection.Org.Overrides[1].Skip {
		t.Error("Overrides[1].Skip = false, want true")
	}
	if cfg.Connection.Org.Overrides[0].Match.AccountNames[0] != "*-dev" {
		t.Errorf("Overrides[0].AccountNames[0] = %q", cfg.Connection.Org.Overrides[0].Match.AccountNames[0])
	}
}

func TestLoadHCLConfig_WithTableBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
connection "aws" {
  plugin = "turbot/aws@latest"
}

drainpipe "test" {
  connection = "aws"

  tables = ["aws_s3_bucket", "aws_vpc"]

  table "aws_ecs_task_definition" {
    where = {
      status = "ACTIVE"
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

	if len(cfg.Tables) != 3 {
		t.Fatalf("len(Tables) = %d, want 3", len(cfg.Tables))
	}
	if cfg.Tables[2].Name != "aws_ecs_task_definition" {
		t.Errorf("Tables[2].Name = %q", cfg.Tables[2].Name)
	}
	if cfg.Tables[2].Where["status"] != "ACTIVE" {
		t.Errorf("Tables[2].Where = %v", cfg.Tables[2].Where)
	}
}

func TestLoadHCLConfig_ProviderShorthand(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
drainpipe "test" {
  provider = "aws"
  tables   = ["aws_s3_bucket"]
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
	if cfg.Provider != "aws" {
		t.Errorf("Provider = %q, want aws", cfg.Provider)
	}
}

func TestLoadHCLConfig_MissingFile(t *testing.T) {
	result, err := LoadHCLConfig("/nonexistent/test.hcl")
	if err != nil {
		t.Fatalf("expected nil error for missing file, got %v", err)
	}
	if result != nil {
		t.Error("expected nil result for missing file")
	}
}

func TestLoadHCLConfig_UndefinedConnection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.hcl")
	data := []byte(`
drainpipe "test" {
  connection = "nonexistent"
  tables     = ["foo"]
}
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadHCLConfig(path)
	if err == nil {
		t.Error("expected error for undefined connection reference, got nil")
	}
}

func TestLoadAllConfigs_MixedFormats(t *testing.T) {
	dir := t.TempDir()

	yamlPath := filepath.Join(dir, "aws.yaml")
	if err := os.WriteFile(yamlPath, []byte("provider: aws\ntables: [aws_s3_bucket]\n"), 0644); err != nil {
		t.Fatal(err)
	}

	hclPath := filepath.Join(dir, "cf.hcl")
	hclData := []byte(`
connection "cloudflare" {
  plugin = "turbot/cloudflare@latest"
}
drainpipe "cf" {
  connection = "cloudflare"
  tables     = ["cloudflare_zone"]
}
`)
	if err := os.WriteFile(hclPath, hclData, 0644); err != nil {
		t.Fatal(err)
	}

	configs, limiters, err := LoadAllConfigs([]string{yamlPath, hclPath})
	if err != nil {
		t.Fatalf("LoadAllConfigs() error = %v", err)
	}
	if len(configs) != 2 {
		t.Fatalf("len(configs) = %d, want 2", len(configs))
	}
	if configs[0].Provider != "aws" {
		t.Errorf("configs[0].Provider = %q, want aws", configs[0].Provider)
	}
	if configs[1].Plugin != "turbot/cloudflare@latest" {
		t.Errorf("configs[1].Plugin = %q", configs[1].Plugin)
	}
	if limiters == nil {
		t.Error("limiters should not be nil")
	}
}

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
	if containsStr(hcl, "plugin") {
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

	// First table is from the `tables` list, no filter_query
	if cfg.Tables[0].Name != "aws_ecs_service" {
		t.Errorf("Tables[0].Name = %q, want aws_ecs_service", cfg.Tables[0].Name)
	}
	if cfg.Tables[0].FilterQuery != nil {
		t.Error("Tables[0].FilterQuery should be nil")
	}

	// Second table is from a table block with filter_query
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

package provider

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// ---------- NaturalKeyColumns ----------

func TestAWS_NaturalKeyColumns_WithARN(t *testing.T) {
	p := &AWSProvider{}
	schema := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "name"},
			{Name: "arn"},
			{Name: "region"},
		},
		GetCallKeyColumnList: []*proto.KeyColumn{
			{Name: "name", Require: "required"},
		},
	}

	got := p.NaturalKeyColumns("aws_s3_bucket", schema)
	if len(got) != 1 || got[0] != "arn" {
		t.Errorf("NaturalKeyColumns() = %v, want [arn]", got)
	}
}

func TestAWS_NaturalKeyColumns_WithoutARN(t *testing.T) {
	p := &AWSProvider{}
	schema := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "name"},
			{Name: "region"},
		},
		GetCallKeyColumnList: []*proto.KeyColumn{
			{Name: "name", Require: "required"},
		},
	}

	got := p.NaturalKeyColumns("aws_sts_caller_identity", schema)
	if len(got) != 1 || got[0] != "name" {
		t.Errorf("NaturalKeyColumns() = %v, want [name]", got)
	}
}

func TestAWS_NaturalKeyColumns_NilSchema(t *testing.T) {
	p := &AWSProvider{}
	got := p.NaturalKeyColumns("aws_foo", nil)
	if got != nil {
		t.Errorf("NaturalKeyColumns(nil) = %v, want nil", got)
	}
}

func TestAWS_NaturalKeyColumns_NoKeysNoARN(t *testing.T) {
	p := &AWSProvider{}
	schema := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "name"},
		},
	}
	got := p.NaturalKeyColumns("aws_foo", schema)
	if got != nil {
		t.Errorf("NaturalKeyColumns(no arn, no keys) = %v, want nil", got)
	}
}

// ---------- DefaultConnectionConfig ----------

func TestAWS_DefaultConnectionConfig_Empty(t *testing.T) {
	// Ensure env vars don't interfere
	os.Unsetenv("AWS_PROFILE")
	os.Unsetenv("AWS_REGIONS")
	p := &AWSProvider{}
	got := p.DefaultConnectionConfig()
	if got != "" {
		t.Errorf("DefaultConnectionConfig() = %q, want empty", got)
	}
}

func TestAWS_DefaultConnectionConfig_WithProfile(t *testing.T) {
	os.Unsetenv("AWS_PROFILE")
	os.Unsetenv("AWS_REGIONS")
	p := &AWSProvider{Profile: "my-profile"}
	got := p.DefaultConnectionConfig()
	if !strings.Contains(got, `profile = "my-profile"`) {
		t.Errorf("DefaultConnectionConfig() = %q, missing profile", got)
	}
}

func TestAWS_DefaultConnectionConfig_WithRegions(t *testing.T) {
	os.Unsetenv("AWS_PROFILE")
	os.Unsetenv("AWS_REGIONS")
	p := &AWSProvider{Regions: []string{"us-east-1", "eu-west-1"}}
	got := p.DefaultConnectionConfig()
	if !strings.Contains(got, "regions") {
		t.Errorf("DefaultConnectionConfig() = %q, missing regions", got)
	}
	if !strings.Contains(got, `"us-east-1"`) || !strings.Contains(got, `"eu-west-1"`) {
		t.Errorf("DefaultConnectionConfig() = %q, missing region values", got)
	}
}

func TestAWS_DefaultConnectionConfig_WithBoth(t *testing.T) {
	os.Unsetenv("AWS_PROFILE")
	os.Unsetenv("AWS_REGIONS")
	p := &AWSProvider{
		Profile: "prod",
		Regions: []string{"us-west-2"},
	}
	got := p.DefaultConnectionConfig()
	if !strings.Contains(got, `profile = "prod"`) {
		t.Errorf("missing profile in %q", got)
	}
	if !strings.Contains(got, `"us-west-2"`) {
		t.Errorf("missing region in %q", got)
	}
}

// ---------- ResolveAccount ----------

func TestAWS_ResolveAccount_Success(t *testing.T) {
	p := &AWSProvider{}
	queryFunc := func(ctx context.Context, table string) (map[string]interface{}, error) {
		if table != "aws_sts_caller_identity" {
			t.Errorf("Unexpected table queried: %s", table)
		}
		return map[string]interface{}{"account_id": "123456789012"}, nil
	}

	acct, err := p.ResolveAccount(context.Background(), queryFunc)
	if err != nil {
		t.Fatalf("ResolveAccount() error = %v", err)
	}
	if acct != "123456789012" {
		t.Errorf("ResolveAccount() = %q, want %q", acct, "123456789012")
	}
}

func TestAWS_ResolveAccount_NilRow(t *testing.T) {
	p := &AWSProvider{}
	queryFunc := func(ctx context.Context, table string) (map[string]interface{}, error) {
		return nil, nil
	}

	_, err := p.ResolveAccount(context.Background(), queryFunc)
	if err == nil {
		t.Error("ResolveAccount() should error on nil row")
	}
}

func TestAWS_ResolveAccount_MissingAccountID(t *testing.T) {
	p := &AWSProvider{}
	queryFunc := func(ctx context.Context, table string) (map[string]interface{}, error) {
		return map[string]interface{}{"user_id": "AIDA..."}, nil
	}

	_, err := p.ResolveAccount(context.Background(), queryFunc)
	if err == nil {
		t.Error("ResolveAccount() should error on missing account_id")
	}
}

func TestAWS_ResolveAccount_QueryError(t *testing.T) {
	p := &AWSProvider{}
	queryFunc := func(ctx context.Context, table string) (map[string]interface{}, error) {
		return nil, fmt.Errorf("connection refused")
	}

	_, err := p.ResolveAccount(context.Background(), queryFunc)
	if err == nil {
		t.Error("ResolveAccount() should propagate query error")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error = %v, want to contain 'connection refused'", err)
	}
}

func TestAWS_ResolveAccount_NilAccountID(t *testing.T) {
	p := &AWSProvider{}
	queryFunc := func(ctx context.Context, table string) (map[string]interface{}, error) {
		return map[string]interface{}{"account_id": nil}, nil
	}

	_, err := p.ResolveAccount(context.Background(), queryFunc)
	if err == nil {
		t.Error("ResolveAccount() should error on nil account_id value")
	}
}

// ---------- resolveProfile ----------

func TestAWS_ResolveProfile_StructField(t *testing.T) {
	t.Setenv("AWS_PROFILE", "env-profile")
	p := &AWSProvider{Profile: "struct-profile"}
	if got := p.resolveProfile(); got != "struct-profile" {
		t.Errorf("resolveProfile() = %q, want struct-profile (struct takes priority)", got)
	}
}

func TestAWS_ResolveProfile_EnvFallback(t *testing.T) {
	t.Setenv("AWS_PROFILE", "env-profile")
	p := &AWSProvider{}
	if got := p.resolveProfile(); got != "env-profile" {
		t.Errorf("resolveProfile() = %q, want env-profile", got)
	}
}

func TestAWS_ResolveProfile_Empty(t *testing.T) {
	os.Unsetenv("AWS_PROFILE")
	p := &AWSProvider{}
	if got := p.resolveProfile(); got != "" {
		t.Errorf("resolveProfile() = %q, want empty", got)
	}
}

// ---------- resolveRegions ----------

func TestAWS_ResolveRegions_StructField(t *testing.T) {
	t.Setenv("AWS_REGIONS", "ap-southeast-1")
	p := &AWSProvider{Regions: []string{"us-east-1"}}
	got := p.resolveRegions()
	if len(got) != 1 || got[0] != "us-east-1" {
		t.Errorf("resolveRegions() = %v, want [us-east-1] (struct takes priority)", got)
	}
}

func TestAWS_ResolveRegions_EnvFallback(t *testing.T) {
	t.Setenv("AWS_REGIONS", "us-west-2, eu-central-1")
	p := &AWSProvider{}
	got := p.resolveRegions()
	if len(got) != 2 {
		t.Fatalf("resolveRegions() = %v, want 2 regions", got)
	}
	if got[0] != "us-west-2" || got[1] != "eu-central-1" {
		t.Errorf("resolveRegions() = %v, want [us-west-2, eu-central-1]", got)
	}
}

func TestAWS_ResolveRegions_Empty(t *testing.T) {
	os.Unsetenv("AWS_REGIONS")
	p := &AWSProvider{}
	got := p.resolveRegions()
	if got != nil {
		t.Errorf("resolveRegions() = %v, want nil", got)
	}
}

// ---------- regionsHCL ----------

func TestRegionsHCL_Empty(t *testing.T) {
	got := regionsHCL(nil)
	if got != nil {
		t.Errorf("regionsHCL(nil) = %v, want nil", got)
	}
}

func TestRegionsHCL_SingleRegion(t *testing.T) {
	got := regionsHCL([]string{"us-east-1"})
	if len(got) != 1 {
		t.Fatalf("regionsHCL len = %d, want 1", len(got))
	}
	want := `  regions = ["us-east-1"]`
	if got[0] != want {
		t.Errorf("regionsHCL = %q, want %q", got[0], want)
	}
}

func TestRegionsHCL_MultipleRegions(t *testing.T) {
	got := regionsHCL([]string{"us-east-1", "eu-west-1"})
	if len(got) != 1 {
		t.Fatalf("regionsHCL len = %d, want 1", len(got))
	}
	want := `  regions = ["us-east-1", "eu-west-1"]`
	if got[0] != want {
		t.Errorf("regionsHCL = %q, want %q", got[0], want)
	}
}

// ---------- stringVal / strPtr ----------

func TestStringVal_Nil(t *testing.T) {
	if got := stringVal(nil); got != "" {
		t.Errorf("stringVal(nil) = %q, want empty", got)
	}
}

func TestStringVal_NonNil(t *testing.T) {
	s := "hello"
	if got := stringVal(&s); got != "hello" {
		t.Errorf("stringVal() = %q, want %q", got, "hello")
	}
}

func TestStrPtr(t *testing.T) {
	p := strPtr("test")
	if p == nil || *p != "test" {
		t.Errorf("strPtr('test') = %v, want pointer to 'test'", p)
	}
}

// ---------- Name / PluginFunc ----------

func TestAWS_Name(t *testing.T) {
	p := &AWSProvider{}
	if got := p.Name(); got != "aws" {
		t.Errorf("Name() = %q, want %q", got, "aws")
	}
}

func TestAWS_PluginFunc(t *testing.T) {
	p := &AWSProvider{}
	if p.PluginFunc() == nil {
		t.Error("PluginFunc() = nil, want non-nil")
	}
}

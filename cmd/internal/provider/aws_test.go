package provider

import (
	"os"
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// ---------- NaturalKeyColumns (now config-driven) ----------

func TestNaturalKeyColumns_WithPreferredKey(t *testing.T) {
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

	got := NaturalKeyColumns("aws_s3_bucket", schema, "arn")
	if len(got) != 1 || got[0] != "arn" {
		t.Errorf("NaturalKeyColumns() = %v, want [arn]", got)
	}
}

func TestNaturalKeyColumns_PreferredKeyMissing(t *testing.T) {
	schema := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "name"},
			{Name: "region"},
		},
		GetCallKeyColumnList: []*proto.KeyColumn{
			{Name: "name", Require: "required"},
		},
	}

	got := NaturalKeyColumns("aws_sts_caller_identity", schema, "arn")
	if len(got) != 1 || got[0] != "name" {
		t.Errorf("NaturalKeyColumns() = %v, want [name]", got)
	}
}

func TestNaturalKeyColumns_NilSchema(t *testing.T) {
	got := NaturalKeyColumns("aws_foo", nil, "arn")
	if got != nil {
		t.Errorf("NaturalKeyColumns(nil) = %v, want nil", got)
	}
}

func TestNaturalKeyColumns_NoPreferredNoKeys(t *testing.T) {
	schema := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "name"},
		},
	}
	got := NaturalKeyColumns("aws_foo", schema, "")
	if got != nil {
		t.Errorf("NaturalKeyColumns(no pref, no keys) = %v, want nil", got)
	}
}

func TestNaturalKeyColumns_IDPreferred(t *testing.T) {
	schema := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "id"},
			{Name: "name"},
		},
		GetCallKeyColumnList: []*proto.KeyColumn{
			{Name: "name", Require: "required"},
		},
	}

	got := NaturalKeyColumns("azure_resource_group", schema, "id")
	if len(got) != 1 || got[0] != "id" {
		t.Errorf("NaturalKeyColumns() = %v, want [id]", got)
	}
}

// ---------- AWSMultiAccount helpers ----------

func TestAWSMultiAccount_ResolveProfile_StructField(t *testing.T) {
	t.Setenv("AWS_PROFILE", "env-profile")
	p := &AWSMultiAccount{Profile: "struct-profile"}
	if got := p.resolveProfile(); got != "struct-profile" {
		t.Errorf("resolveProfile() = %q, want struct-profile (struct takes priority)", got)
	}
}

func TestAWSMultiAccount_ResolveProfile_EnvFallback(t *testing.T) {
	t.Setenv("AWS_PROFILE", "env-profile")
	p := &AWSMultiAccount{}
	if got := p.resolveProfile(); got != "env-profile" {
		t.Errorf("resolveProfile() = %q, want env-profile", got)
	}
}

func TestAWSMultiAccount_ResolveProfile_Empty(t *testing.T) {
	os.Unsetenv("AWS_PROFILE")
	p := &AWSMultiAccount{}
	if got := p.resolveProfile(); got != "" {
		t.Errorf("resolveProfile() = %q, want empty", got)
	}
}

func TestAWSMultiAccount_ResolveRegions_StructField(t *testing.T) {
	t.Setenv("AWS_REGIONS", "ap-southeast-1")
	p := &AWSMultiAccount{Regions: []string{"us-east-1"}}
	got := p.resolveRegions()
	if len(got) != 1 || got[0] != "us-east-1" {
		t.Errorf("resolveRegions() = %v, want [us-east-1] (struct takes priority)", got)
	}
}

func TestAWSMultiAccount_ResolveRegions_EnvFallback(t *testing.T) {
	t.Setenv("AWS_REGIONS", "us-west-2, eu-central-1")
	p := &AWSMultiAccount{}
	got := p.resolveRegions()
	if len(got) != 2 {
		t.Fatalf("resolveRegions() = %v, want 2 regions", got)
	}
	if got[0] != "us-west-2" || got[1] != "eu-central-1" {
		t.Errorf("resolveRegions() = %v, want [us-west-2, eu-central-1]", got)
	}
}

func TestAWSMultiAccount_ResolveRegions_Empty(t *testing.T) {
	os.Unsetenv("AWS_REGIONS")
	p := &AWSMultiAccount{}
	got := p.resolveRegions()
	if got != nil {
		t.Errorf("resolveRegions() = %v, want nil", got)
	}
}

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

func TestNewAWSMultiAccount(t *testing.T) {
	ma := NewAWSMultiAccount("my-profile", []string{"us-east-1"}, &OrgSettings{
		RoleName:       "MyRole",
		AdminAccountID: "123456789012",
		Organizations:  []string{"ou-1234"},
	})
	if ma.Profile != "my-profile" {
		t.Errorf("Profile = %q, want my-profile", ma.Profile)
	}
	if ma.OrgRoleName != "MyRole" {
		t.Errorf("OrgRoleName = %q, want MyRole", ma.OrgRoleName)
	}
	if len(ma.Regions) != 1 || ma.Regions[0] != "us-east-1" {
		t.Errorf("Regions = %v, want [us-east-1]", ma.Regions)
	}
}

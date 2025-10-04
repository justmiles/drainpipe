package provider

import (
	"reflect"
	"sort"
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// ---------- DefaultNaturalKeyColumns ----------

func TestDefaultNaturalKeyColumns_NilSchema(t *testing.T) {
	got := DefaultNaturalKeyColumns(nil)
	if got != nil {
		t.Errorf("DefaultNaturalKeyColumns(nil) = %v, want nil", got)
	}
}

func TestDefaultNaturalKeyColumns_EmptyKeyList(t *testing.T) {
	schema := &proto.TableSchema{
		Columns:              []*proto.ColumnDefinition{{Name: "id"}},
		GetCallKeyColumnList: nil,
	}
	got := DefaultNaturalKeyColumns(schema)
	if got != nil {
		t.Errorf("DefaultNaturalKeyColumns(empty keys) = %v, want nil", got)
	}
}

func TestDefaultNaturalKeyColumns_RequiredOnly(t *testing.T) {
	schema := &proto.TableSchema{
		GetCallKeyColumnList: []*proto.KeyColumn{
			{Name: "arn", Require: "required"},
			{Name: "region", Require: "optional"},
			{Name: "name", Require: "required"},
		},
	}
	got := DefaultNaturalKeyColumns(schema)
	want := []string{"arn", "name"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DefaultNaturalKeyColumns() = %v, want %v", got, want)
	}
}

func TestDefaultNaturalKeyColumns_AllOptional(t *testing.T) {
	schema := &proto.TableSchema{
		GetCallKeyColumnList: []*proto.KeyColumn{
			{Name: "region", Require: "optional"},
		},
	}
	got := DefaultNaturalKeyColumns(schema)
	if len(got) != 0 {
		t.Errorf("DefaultNaturalKeyColumns(all optional) = %v, want empty", got)
	}
}

// ---------- Registry ----------

func TestRegistry_RegisterAndGet(t *testing.T) {
	// Save and restore the registry to avoid test pollution.
	original := make(map[string]Provider)
	for k, v := range registry {
		original[k] = v
	}
	defer func() {
		for k := range registry {
			delete(registry, k)
		}
		for k, v := range original {
			registry[k] = v
		}
	}()

	// The AWS provider registers itself via init().
	prov := Get("aws")
	if prov == nil {
		t.Fatal("Get('aws') = nil, want non-nil (registered via init)")
	}
	if prov.Name() != "aws" {
		t.Errorf("Get('aws').Name() = %q, want %q", prov.Name(), "aws")
	}
}

func TestRegistry_UnknownProvider(t *testing.T) {
	if got := Get("nonexistent_provider"); got != nil {
		t.Errorf("Get('nonexistent') = %v, want nil", got)
	}
}

func TestRegistry_Names(t *testing.T) {
	names := Names()
	if len(names) == 0 {
		t.Fatal("Names() returned empty, expected at least 'aws'")
	}
	sort.Strings(names)
	found := false
	for _, n := range names {
		if n == "aws" {
			found = true
		}
	}
	if !found {
		t.Errorf("Names() = %v, expected to contain 'aws'", names)
	}
}

func TestRegistry_All(t *testing.T) {
	all := All()
	if len(all) == 0 {
		t.Fatal("All() returned empty map")
	}
	if _, ok := all["aws"]; !ok {
		t.Error("All() does not contain 'aws' provider")
	}
}

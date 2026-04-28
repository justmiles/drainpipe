package provider

import (
	"reflect"
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

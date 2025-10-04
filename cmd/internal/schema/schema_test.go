package schema

import (
	"reflect"
	"sort"
	"testing"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// ---------- DrainpipeColumnNames ----------

func TestDrainpipeColumnNames(t *testing.T) {
	got := DrainpipeColumnNames()
	want := []string{"_source_account", "_first_seen_at", "_last_seen_at", "_deleted_at"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("DrainpipeColumnNames() = %v, want %v", got, want)
	}
}

func TestDrainpipeColumnNames_Length(t *testing.T) {
	got := DrainpipeColumnNames()
	if len(got) != 4 {
		t.Errorf("DrainpipeColumnNames() length = %d, want 4", len(got))
	}
}

// ---------- TableColumns ----------

func TestTableColumns(t *testing.T) {
	schema := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "zipcode", Type: proto.ColumnType_STRING},
			{Name: "arn", Type: proto.ColumnType_STRING},
			{Name: "name", Type: proto.ColumnType_STRING},
		},
	}
	got := TableColumns(schema)
	// schemaToColumns sorts alphabetically
	want := []string{"arn", "name", "zipcode"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("TableColumns() = %v, want %v", got, want)
	}
}

func TestTableColumns_EmptySchema(t *testing.T) {
	schema := &proto.TableSchema{Columns: nil}
	got := TableColumns(schema)
	if len(got) != 0 {
		t.Errorf("TableColumns(empty) = %v, want empty", got)
	}
}

// ---------- schemaToColumns ----------

func TestSchemaToColumns_Sorted(t *testing.T) {
	schema := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "zzz", Type: proto.ColumnType_STRING},
			{Name: "aaa", Type: proto.ColumnType_INT},
			{Name: "mmm", Type: proto.ColumnType_BOOL},
		},
	}
	cols := schemaToColumns(schema)
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	if !sort.StringsAreSorted(names) {
		t.Errorf("schemaToColumns() not sorted: %v", names)
	}
}

func TestSchemaToColumns_Types(t *testing.T) {
	schema := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "a", Type: proto.ColumnType_STRING},
			{Name: "b", Type: proto.ColumnType_INT},
			{Name: "c", Type: proto.ColumnType_BOOL},
			{Name: "d", Type: proto.ColumnType_JSON},
		},
	}
	cols := schemaToColumns(schema)
	typeMap := map[string]string{}
	for _, c := range cols {
		typeMap[c.Name] = c.PGType
	}

	checks := map[string]string{
		"a": "TEXT",
		"b": "BIGINT",
		"c": "BOOLEAN",
		"d": "JSONB",
	}
	for name, wantType := range checks {
		if gotType := typeMap[name]; gotType != wantType {
			t.Errorf("column %q type = %q, want %q", name, gotType, wantType)
		}
	}
}

// ---------- protoTypeToPG ----------

func TestProtoTypeToPG(t *testing.T) {
	cases := []struct {
		input proto.ColumnType
		want  string
	}{
		{proto.ColumnType_BOOL, "BOOLEAN"},
		{proto.ColumnType_INT, "BIGINT"},
		{proto.ColumnType_DOUBLE, "DOUBLE PRECISION"},
		{proto.ColumnType_STRING, "TEXT"},
		{proto.ColumnType_JSON, "JSONB"},
		{proto.ColumnType_TIMESTAMP, "TIMESTAMPTZ"},
		{proto.ColumnType_DATETIME, "TIMESTAMPTZ"},
		{proto.ColumnType_IPADDR, "INET"},
		{proto.ColumnType_CIDR, "CIDR"},
		{proto.ColumnType_INET, "INET"},
		{proto.ColumnType_LTREE, "TEXT"},
	}

	for _, tc := range cases {
		t.Run(tc.input.String(), func(t *testing.T) {
			got := protoTypeToPG(tc.input)
			if got != tc.want {
				t.Errorf("protoTypeToPG(%v) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestProtoTypeToPG_UnknownType(t *testing.T) {
	// Use an extremely large int that doesn't map to any known ColumnType.
	got := protoTypeToPG(proto.ColumnType(99999))
	if got != "TEXT" {
		t.Errorf("protoTypeToPG(unknown) = %q, want %q", got, "TEXT")
	}
}

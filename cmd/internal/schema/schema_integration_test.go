//go:build integration

package schema

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"

	"github.com/justmiles/drainpipe/cmd/internal/testutil"
)

func TestIntegration_EnsureTable_CreatesNewTable(t *testing.T) {
	pool := testutil.NewTestPool(t)
	tableName := "test_ensure_create"
	testutil.DropTable(t, pool, tableName)
	t.Cleanup(func() { testutil.DropTable(t, pool, tableName) })

	mgr := New(pool, zerolog.Nop())
	schema := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "arn", Type: proto.ColumnType_STRING},
			{Name: "name", Type: proto.ColumnType_STRING},
			{Name: "region", Type: proto.ColumnType_STRING},
			{Name: "enabled", Type: proto.ColumnType_BOOL},
			{Name: "count", Type: proto.ColumnType_INT},
		},
	}

	err := mgr.EnsureTable(context.Background(), tableName, schema, []string{"arn"})
	if err != nil {
		t.Fatalf("EnsureTable() error = %v", err)
	}

	// Verify table exists
	exists, err := mgr.tableExists(context.Background(), tableName)
	if err != nil {
		t.Fatalf("tableExists() error = %v", err)
	}
	if !exists {
		t.Fatal("table should exist after EnsureTable")
	}

	// Verify columns: 5 data + 4 drainpipe = 9 expected
	cols, err := mgr.existingColumns(context.Background(), tableName)
	if err != nil {
		t.Fatalf("existingColumns() error = %v", err)
	}

	expectedCols := []string{"arn", "name", "region", "enabled", "count",
		"_source_account", "_first_seen_at", "_last_seen_at", "_deleted_at"}
	for _, col := range expectedCols {
		if !cols[col] {
			t.Errorf("missing column %q", col)
		}
	}
}

func TestIntegration_EnsureTable_AddsNewColumns(t *testing.T) {
	pool := testutil.NewTestPool(t)
	tableName := "test_ensure_evolve"
	testutil.DropTable(t, pool, tableName)
	t.Cleanup(func() { testutil.DropTable(t, pool, tableName) })

	mgr := New(pool, zerolog.Nop())

	// Initial schema with 2 columns
	schema1 := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "arn", Type: proto.ColumnType_STRING},
			{Name: "name", Type: proto.ColumnType_STRING},
		},
	}
	if err := mgr.EnsureTable(context.Background(), tableName, schema1, []string{"arn"}); err != nil {
		t.Fatalf("EnsureTable(v1) error = %v", err)
	}

	// Evolved schema with 3 columns (added "tags")
	schema2 := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "arn", Type: proto.ColumnType_STRING},
			{Name: "name", Type: proto.ColumnType_STRING},
			{Name: "tags", Type: proto.ColumnType_JSON},
		},
	}
	if err := mgr.EnsureTable(context.Background(), tableName, schema2, []string{"arn"}); err != nil {
		t.Fatalf("EnsureTable(v2) error = %v", err)
	}

	cols, err := mgr.existingColumns(context.Background(), tableName)
	if err != nil {
		t.Fatalf("existingColumns() error = %v", err)
	}
	if !cols["tags"] {
		t.Error("column 'tags' should have been added by schema evolution")
	}
}

func TestIntegration_EnsureTable_Idempotent(t *testing.T) {
	pool := testutil.NewTestPool(t)
	tableName := "test_ensure_idempotent"
	testutil.DropTable(t, pool, tableName)
	t.Cleanup(func() { testutil.DropTable(t, pool, tableName) })

	mgr := New(pool, zerolog.Nop())
	schema := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "arn", Type: proto.ColumnType_STRING},
		},
	}

	// Call twice — should be a no-op the second time
	if err := mgr.EnsureTable(context.Background(), tableName, schema, []string{"arn"}); err != nil {
		t.Fatalf("EnsureTable(1) error = %v", err)
	}
	if err := mgr.EnsureTable(context.Background(), tableName, schema, []string{"arn"}); err != nil {
		t.Fatalf("EnsureTable(2) error = %v", err)
	}
}

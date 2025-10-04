//go:build integration

package importer

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"

	"github.com/justmiles/drainpipe/cmd/internal/exporter"
	"github.com/justmiles/drainpipe/cmd/internal/schema"
	"github.com/justmiles/drainpipe/cmd/internal/testutil"
)

// setupTable creates a live table with known columns for testing.
func setupTable(t *testing.T, tableName string) {
	t.Helper()
	pool := testutil.NewTestPool(t)
	testutil.DropTable(t, pool, tableName)
	t.Cleanup(func() { testutil.DropTable(t, pool, tableName) })

	mgr := schema.New(pool, zerolog.Nop())
	ps := &proto.TableSchema{
		Columns: []*proto.ColumnDefinition{
			{Name: "arn", Type: proto.ColumnType_STRING},
			{Name: "name", Type: proto.ColumnType_STRING},
			{Name: "region", Type: proto.ColumnType_STRING},
		},
	}
	if err := mgr.EnsureTable(context.Background(), tableName, ps, []string{"arn"}); err != nil {
		t.Fatalf("setup EnsureTable: %v", err)
	}
}

// feedRows sends rows into a channel and closes it.
func feedRows(rows ...exporter.Row) <-chan exporter.Row {
	ch := make(chan exporter.Row, len(rows))
	for _, r := range rows {
		ch <- r
	}
	close(ch)
	return ch
}

func TestIntegration_Import_InsertsRows(t *testing.T) {
	table := "test_import_insert"
	setupTable(t, table)
	pool := testutil.NewTestPool(t)

	imp := New(pool, "acct-111", zerolog.Nop())
	columns := []string{"arn", "name", "region"}
	rows := feedRows(
		exporter.Row{"arn": "arn:aws:s3:::bucket-a", "name": "bucket-a", "region": "us-east-1"},
		exporter.Row{"arn": "arn:aws:s3:::bucket-b", "name": "bucket-b", "region": "us-west-2"},
	)

	result, err := imp.Import(context.Background(), table, []string{"arn"}, columns, rows)
	if err != nil {
		t.Fatalf("Import() error = %v", err)
	}
	if result.Rows != 2 {
		t.Errorf("Rows = %d, want 2", result.Rows)
	}

	// Verify rows exist in DB
	var count int
	err = pool.QueryRow(context.Background(),
		"SELECT count(*) FROM "+table+" WHERE _source_account = $1 AND _deleted_at IS NULL",
		"acct-111").Scan(&count)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if count != 2 {
		t.Errorf("DB row count = %d, want 2", count)
	}
}

func TestIntegration_Import_UpsertsExisting(t *testing.T) {
	table := "test_import_upsert"
	setupTable(t, table)
	pool := testutil.NewTestPool(t)
	columns := []string{"arn", "name", "region"}

	imp := New(pool, "acct-222", zerolog.Nop())

	// First import
	rows1 := feedRows(
		exporter.Row{"arn": "arn:aws:s3:::bucket-x", "name": "bucket-x", "region": "us-east-1"},
	)
	if _, err := imp.Import(context.Background(), table, []string{"arn"}, columns, rows1); err != nil {
		t.Fatalf("Import(1) error = %v", err)
	}

	// Second import: same ARN, different name
	rows2 := feedRows(
		exporter.Row{"arn": "arn:aws:s3:::bucket-x", "name": "bucket-x-renamed", "region": "us-east-1"},
	)
	result, err := imp.Import(context.Background(), table, []string{"arn"}, columns, rows2)
	if err != nil {
		t.Fatalf("Import(2) error = %v", err)
	}
	if result.Rows != 1 {
		t.Errorf("Upserted rows = %d, want 1", result.Rows)
	}

	// Verify the name was updated (not duplicated)
	var name string
	err = pool.QueryRow(context.Background(),
		"SELECT name FROM "+table+" WHERE arn = $1 AND _source_account = $2",
		"arn:aws:s3:::bucket-x", "acct-222").Scan(&name)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if name != "bucket-x-renamed" {
		t.Errorf("name = %q, want %q", name, "bucket-x-renamed")
	}
}

func TestIntegration_Import_SoftDeletes(t *testing.T) {
	table := "test_import_softdelete"
	setupTable(t, table)
	pool := testutil.NewTestPool(t)
	columns := []string{"arn", "name", "region"}

	imp := New(pool, "acct-333", zerolog.Nop())

	// Import 3 rows
	rows1 := feedRows(
		exporter.Row{"arn": "arn:1", "name": "a", "region": "us-east-1"},
		exporter.Row{"arn": "arn:2", "name": "b", "region": "us-east-1"},
		exporter.Row{"arn": "arn:3", "name": "c", "region": "us-east-1"},
	)
	if _, err := imp.Import(context.Background(), table, []string{"arn"}, columns, rows1); err != nil {
		t.Fatalf("Import(1) error = %v", err)
	}

	// Re-import with only 2 rows (arn:3 should be soft-deleted)
	rows2 := feedRows(
		exporter.Row{"arn": "arn:1", "name": "a", "region": "us-east-1"},
		exporter.Row{"arn": "arn:2", "name": "b", "region": "us-east-1"},
	)
	result, err := imp.Import(context.Background(), table, []string{"arn"}, columns, rows2)
	if err != nil {
		t.Fatalf("Import(2) error = %v", err)
	}
	if result.Deleted != 1 {
		t.Errorf("Deleted = %d, want 1", result.Deleted)
	}

	// Verify arn:3 is soft-deleted
	var deletedAt interface{}
	err = pool.QueryRow(context.Background(),
		"SELECT _deleted_at FROM "+table+" WHERE arn = $1 AND _source_account = $2",
		"arn:3", "acct-333").Scan(&deletedAt)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if deletedAt == nil {
		t.Error("arn:3 _deleted_at = nil, want non-nil")
	}
}

func TestIntegration_Import_ScopedByDrainpipeAccount(t *testing.T) {
	table := "test_import_scoped"
	setupTable(t, table)
	pool := testutil.NewTestPool(t)
	columns := []string{"arn", "name", "region"}

	// Import for account A
	impA := New(pool, "acct-A", zerolog.Nop())
	rowsA := feedRows(
		exporter.Row{"arn": "arn:A1", "name": "a1", "region": "us-east-1"},
		exporter.Row{"arn": "arn:A2", "name": "a2", "region": "us-east-1"},
	)
	if _, err := impA.Import(context.Background(), table, []string{"arn"}, columns, rowsA); err != nil {
		t.Fatalf("Import(A) error = %v", err)
	}

	// Import for account B
	impB := New(pool, "acct-B", zerolog.Nop())
	rowsB := feedRows(
		exporter.Row{"arn": "arn:B1", "name": "b1", "region": "eu-west-1"},
	)
	if _, err := impB.Import(context.Background(), table, []string{"arn"}, columns, rowsB); err != nil {
		t.Fatalf("Import(B) error = %v", err)
	}

	// Re-import account A with only 1 row → should soft-delete A2 but NOT B1
	rowsA2 := feedRows(
		exporter.Row{"arn": "arn:A1", "name": "a1", "region": "us-east-1"},
	)
	result, err := impA.Import(context.Background(), table, []string{"arn"}, columns, rowsA2)
	if err != nil {
		t.Fatalf("Import(A2) error = %v", err)
	}
	if result.Deleted != 1 {
		t.Errorf("Deleted = %d, want 1", result.Deleted)
	}

	// Verify B's row is still alive
	var count int
	err = pool.QueryRow(context.Background(),
		"SELECT count(*) FROM "+table+" WHERE _source_account = 'acct-B' AND _deleted_at IS NULL",
	).Scan(&count)
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if count != 1 {
		t.Errorf("acct-B live rows = %d, want 1 (should be untouched)", count)
	}
}

func TestIntegration_Import_EmptyChannel(t *testing.T) {
	table := "test_import_empty"
	setupTable(t, table)
	pool := testutil.NewTestPool(t)
	columns := []string{"arn", "name", "region"}

	imp := New(pool, "acct-empty", zerolog.Nop())
	rows := feedRows() // no rows

	result, err := imp.Import(context.Background(), table, []string{"arn"}, columns, rows)
	if err != nil {
		t.Fatalf("Import(empty) error = %v", err)
	}
	if result.Rows != 0 {
		t.Errorf("Rows = %d, want 0", result.Rows)
	}
}

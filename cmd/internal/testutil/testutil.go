// Package testutil provides shared helpers for integration tests that
// require a live PostgreSQL instance (the Docker cmdb-postgres container).
package testutil

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultDSN is the connection string for the Docker dev database.
const DefaultDSN = "postgres://cmdb:cmdb_dev@localhost:5432/cmdb?sslmode=disable"

// dsn returns the database URL to use, preferring the TEST_DATABASE_URL
// environment variable over the default. This allows integration tests to
// run in environments where localhost:5432 is not reachable (e.g., when
// Postgres runs in a Docker container on a bridge network).
func dsn() string {
	if v := os.Getenv("TEST_DATABASE_URL"); v != "" {
		return v
	}
	return DefaultDSN
}

// NewTestPool creates a pgxpool connected to the dev database.
// It registers a cleanup function to close the pool when the test finishes.
// Calls t.Fatal if the connection cannot be established.
func NewTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn())
	if err != nil {
		t.Fatalf("testutil: failed to connect to PostgreSQL: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("testutil: failed to ping PostgreSQL: %v", err)
	}

	t.Cleanup(func() { pool.Close() })
	return pool
}

// DropTable drops a table if it exists. Used for test cleanup.
func DropTable(t *testing.T, pool *pgxpool.Pool, name string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", name))
	if err != nil {
		t.Fatalf("testutil: failed to drop table %s: %v", name, err)
	}
}

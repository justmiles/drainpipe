// Package schema manages PostgreSQL table creation and evolution
// based on Steampipe plugin schemas discovered at runtime.
package schema

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
)

// drainpipeColumns are appended to every managed table.
// _source_account scopes data by collection account/subscription/tenant.
// Tracking columns record when resources were first/last seen and when deleted.
var drainpipeColumns = []ColumnDef{
	{Name: "_source_account", PGType: "TEXT NOT NULL DEFAULT ''"},
	{Name: "_first_seen_at", PGType: "TIMESTAMPTZ NOT NULL DEFAULT now()"},
	{Name: "_last_seen_at", PGType: "TIMESTAMPTZ NOT NULL DEFAULT now()"},
	{Name: "_deleted_at", PGType: "TIMESTAMPTZ"},
}

// DrainpipeColumnNames returns the names of drainpipe-managed columns
// (the ones that should be excluded from staging tables).
func DrainpipeColumnNames() []string {
	names := make([]string, len(drainpipeColumns))
	for i, c := range drainpipeColumns {
		names[i] = c.Name
	}
	return names
}

// ColumnDef represents a PostgreSQL column.
type ColumnDef struct {
	Name   string
	PGType string
}

// Manager handles dynamic table creation and schema evolution.
type Manager struct {
	pool   *pgxpool.Pool
	logger zerolog.Logger
}

// New creates a new schema Manager.
func New(pool *pgxpool.Pool, logger zerolog.Logger) *Manager {
	return &Manager{pool: pool, logger: logger}
}

// EnsureTable creates or updates a PostgreSQL table to match the Steampipe
// plugin schema. The primary key is (_source_account, ...naturalKeys...).
func (m *Manager) EnsureTable(ctx context.Context, pgTable string, pluginSchema *proto.TableSchema, naturalKeys []string) error {
	log := m.logger.With().Str("table", pgTable).Logger()

	pluginCols := schemaToColumns(pluginSchema)

	exists, err := m.tableExists(ctx, pgTable)
	if err != nil {
		return fmt.Errorf("checking table existence: %w", err)
	}

	if !exists {
		if err := m.createTable(ctx, pgTable, pluginCols, naturalKeys); err != nil {
			return fmt.Errorf("creating table: %w", err)
		}
		log.Info().
			Int("columns", len(pluginCols)).
			Strs("natural_keys", naturalKeys).
			Msg("table created")
		return nil
	}

	// Table exists — check for new columns
	existingCols, err := m.existingColumns(ctx, pgTable)
	if err != nil {
		return fmt.Errorf("getting existing columns: %w", err)
	}

	var added int
	allCols := append(pluginCols, drainpipeColumns...)
	for _, col := range allCols {
		if !existingCols[col.Name] {
			if err := m.addColumn(ctx, pgTable, col); err != nil {
				log.Warn().Err(err).Str("column", col.Name).Msg("failed to add column")
				continue
			}
			added++
		}
	}

	if added > 0 {
		log.Info().Int("columns_added", added).Msg("table updated")
	} else {
		log.Debug().Msg("table schema up to date")
	}

	return nil
}

// TableColumns returns the ordered list of data column names (excluding
// drainpipe-managed columns) for a given plugin schema.
func TableColumns(pluginSchema *proto.TableSchema) []string {
	cols := schemaToColumns(pluginSchema)
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	return names
}

// createTable builds and executes a CREATE TABLE statement.
// Primary key includes _source_account + natural keys.
func (m *Manager) createTable(ctx context.Context, pgTable string, pluginCols []ColumnDef, naturalKeys []string) error {
	var colDefs []string
	for _, col := range pluginCols {
		colDefs = append(colDefs, fmt.Sprintf("  %s %s", col.Name, col.PGType))
	}
	for _, col := range drainpipeColumns {
		colDefs = append(colDefs, fmt.Sprintf("  %s %s", col.Name, col.PGType))
	}

	// Primary key = (_source_account, ...naturalKeys...)
	if len(naturalKeys) > 0 {
		pkCols := append([]string{"_source_account"}, naturalKeys...)
		colDefs = append(colDefs, fmt.Sprintf("  PRIMARY KEY (%s)", strings.Join(pkCols, ", ")))
	}

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)", pgTable, strings.Join(colDefs, ",\n"))

	_, err := m.pool.Exec(ctx, sql)
	return err
}

func (m *Manager) tableExists(ctx context.Context, pgTable string) (bool, error) {
	var exists bool
	err := m.pool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
		pgTable,
	).Scan(&exists)
	return exists, err
}

func (m *Manager) existingColumns(ctx context.Context, pgTable string) (map[string]bool, error) {
	rows, err := m.pool.Query(ctx,
		"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1",
		pgTable,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	return cols, rows.Err()
}

func (m *Manager) addColumn(ctx context.Context, pgTable string, col ColumnDef) error {
	sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", pgTable, col.Name, col.PGType)
	_, err := m.pool.Exec(ctx, sql)
	return err
}

func schemaToColumns(s *proto.TableSchema) []ColumnDef {
	cols := make([]ColumnDef, 0, len(s.Columns))
	for _, c := range s.Columns {
		cols = append(cols, ColumnDef{
			Name:   c.Name,
			PGType: protoTypeToPG(c.Type),
		})
	}
	sort.Slice(cols, func(i, j int) bool {
		return cols[i].Name < cols[j].Name
	})
	return cols
}

func protoTypeToPG(ct proto.ColumnType) string {
	switch ct {
	case proto.ColumnType_BOOL:
		return "BOOLEAN"
	case proto.ColumnType_INT:
		return "BIGINT"
	case proto.ColumnType_DOUBLE:
		return "DOUBLE PRECISION"
	case proto.ColumnType_STRING:
		return "TEXT"
	case proto.ColumnType_JSON:
		return "JSONB"
	case proto.ColumnType_TIMESTAMP, proto.ColumnType_DATETIME:
		return "TIMESTAMPTZ"
	case proto.ColumnType_IPADDR:
		return "INET"
	case proto.ColumnType_CIDR:
		return "CIDR"
	case proto.ColumnType_INET:
		return "INET"
	case proto.ColumnType_LTREE:
		return "TEXT"
	default:
		return "TEXT"
	}
}

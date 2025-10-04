package importer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/justmiles/drainpipe/cmd/internal/exporter"
	"github.com/justmiles/drainpipe/cmd/internal/schema"
)

// Importer handles the staging table import pattern for loading data into PostgreSQL.
type Importer struct {
	pool             *pgxpool.Pool
	sourceAccount string
	logger           zerolog.Logger
}

// New creates a new Importer.
// sourceAccount is the resolved account ID for scoping operations.
func New(pool *pgxpool.Pool, sourceAccount string, logger zerolog.Logger) *Importer {
	return &Importer{
		pool:             pool,
		sourceAccount: sourceAccount,
		logger:           logger,
	}
}

// ImportResult contains stats from a table import.
type ImportResult struct {
	Table    string
	Rows     int64
	Deleted  int64
	Duration time.Duration
}

// Import executes the 4-step staging table import for a single table.
// `columns` is the full list of data columns from the plugin schema.
// `naturalKeys` are the columns used for deduplication (from GetCallKeyColumnList).
// All operations happen within a single transaction, scoped to _source_account.
func (imp *Importer) Import(ctx context.Context, pgTable string, naturalKeys []string, columns []string, rows <-chan exporter.Row) (*ImportResult, error) {
	start := time.Now()
	log := imp.logger.With().Str("table", pgTable).Logger()

	tx, err := imp.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	stagingTable := "staging_" + pgTable

	// Step 1: Create temp staging table
	if err := imp.createStagingTable(ctx, tx, stagingTable, pgTable); err != nil {
		return nil, fmt.Errorf("creating staging table: %w", err)
	}
	log.Debug().Msg("staging table created")

	// Step 2: Load rows into staging table
	rowCount, err := imp.loadStaging(ctx, tx, stagingTable, columns, rows)
	if err != nil {
		return nil, fmt.Errorf("loading staging table: %w", err)
	}
	log.Info().Int64("rows", rowCount).Msg("staging table loaded")

	// Step 3: Upsert from staging → live (scoped to _source_account)
	upserted, err := imp.upsert(ctx, tx, stagingTable, pgTable, naturalKeys, columns)
	if err != nil {
		return nil, fmt.Errorf("upserting: %w", err)
	}
	log.Info().Int64("upserted", upserted).Msg("upsert complete")

	// Step 4: Soft-delete resources not in staging (scoped to _source_account)
	deleted, err := imp.softDelete(ctx, tx, stagingTable, pgTable, naturalKeys)
	if err != nil {
		return nil, fmt.Errorf("soft-deleting: %w", err)
	}
	if deleted > 0 {
		log.Info().Int64("deleted", deleted).Msg("soft-deletes applied")
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing transaction: %w", err)
	}

	return &ImportResult{
		Table:    pgTable,
		Rows:     upserted,
		Deleted:  deleted,
		Duration: time.Since(start),
	}, nil
}

// createStagingTable creates a temp table matching the live table but without
// the drainpipe-managed columns (tracking + _source_account).
func (imp *Importer) createStagingTable(ctx context.Context, tx pgx.Tx, stagingTable, liveTable string) error {
	sql := fmt.Sprintf(
		`CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP`,
		stagingTable, liveTable,
	)
	if _, err := tx.Exec(ctx, sql); err != nil {
		return err
	}

	// Drop drainpipe-managed columns from staging
	for _, col := range schema.DrainpipeColumnNames() {
		dropSQL := fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s", stagingTable, col)
		if _, err := tx.Exec(ctx, dropSQL); err != nil {
			return err
		}
	}

	return nil
}

// loadStaging inserts rows into the staging table.
func (imp *Importer) loadStaging(ctx context.Context, tx pgx.Tx, stagingTable string, columns []string, rows <-chan exporter.Row) (int64, error) {
	var count int64
	var batch [][]interface{}
	const batchSize = 500

	for row := range rows {
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			val := row[col]
			if val != nil {
				switch v := val.(type) {
				case map[string]interface{}, []interface{}:
					b, _ := json.Marshal(v)
					values[i] = string(b)
				default:
					values[i] = v
				}
			}
		}
		batch = append(batch, values)
		count++

		if len(batch) >= batchSize {
			if err := imp.insertBatch(ctx, tx, stagingTable, columns, batch); err != nil {
				return count, err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := imp.insertBatch(ctx, tx, stagingTable, columns, batch); err != nil {
			return count, err
		}
	}

	return count, nil
}

func (imp *Importer) insertBatch(ctx context.Context, tx pgx.Tx, table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	numCols := len(columns)
	var valuePlaceholders []string
	var allArgs []interface{}

	for i, row := range rows {
		var placeholders []string
		for j := range row {
			placeholders = append(placeholders, fmt.Sprintf("$%d", i*numCols+j+1))
		}
		valuePlaceholders = append(valuePlaceholders, "("+strings.Join(placeholders, ", ")+")")
		allArgs = append(allArgs, row...)
	}

	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		table,
		strings.Join(columns, ", "),
		strings.Join(valuePlaceholders, ", "),
	)

	_, err := tx.Exec(ctx, sql, allArgs...)
	return err
}

// upsert merges staging rows into the live table, scoped to _source_account.
// The INSERT sets _source_account on every row.
func (imp *Importer) upsert(ctx context.Context, tx pgx.Tx, stagingTable, pgTable string, naturalKeys, columns []string) (int64, error) {
	colList := strings.Join(columns, ", ")

	// PK includes _source_account + natural keys
	pkCols := append([]string{"_source_account"}, naturalKeys...)
	pkList := strings.Join(pkCols, ", ")

	// SET clause: update all non-key columns + tracking
	var setClauses []string
	for _, col := range columns {
		if !contains(naturalKeys, col) {
			setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}
	setClauses = append(setClauses, "_last_seen_at = now()")
	setClauses = append(setClauses, "_deleted_at = NULL")

	// The staging table doesn't have _source_account, so we inject it as a literal
	sql := fmt.Sprintf(`
		INSERT INTO %s (_source_account, %s, _first_seen_at, _last_seen_at, _deleted_at)
		SELECT $1, %s, now(), now(), NULL
		FROM %s
		ON CONFLICT (%s) DO UPDATE SET
			%s
	`,
		pgTable, colList,
		colList,
		stagingTable,
		pkList,
		strings.Join(setClauses, ",\n\t\t\t"),
	)

	tag, err := tx.Exec(ctx, sql, imp.sourceAccount)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// softDelete marks resources that are in the live table for THIS account
// but not in the current staging table.
func (imp *Importer) softDelete(ctx context.Context, tx pgx.Tx, stagingTable, pgTable string, naturalKeys []string) (int64, error) {
	var conditions []string
	for _, key := range naturalKeys {
		conditions = append(conditions, fmt.Sprintf("live.%s = staging.%s", key, key))
	}

	sql := fmt.Sprintf(`
		UPDATE %s AS live
		SET _deleted_at = now()
		WHERE live._source_account = $1
		  AND live._deleted_at IS NULL
		  AND NOT EXISTS (
			SELECT 1 FROM %s AS staging
			WHERE %s
		)
	`,
		pgTable,
		stagingTable,
		strings.Join(conditions, " AND "),
	)

	tag, err := tx.Exec(ctx, sql, imp.sourceAccount)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

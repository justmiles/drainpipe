package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"

	"github.com/justmiles/drainpipe/cmd/internal/config"
	"github.com/justmiles/drainpipe/cmd/internal/exporter"
	"github.com/justmiles/drainpipe/cmd/internal/importer"
	"github.com/justmiles/drainpipe/cmd/internal/schema"
)

// progress tracks table collection counters for periodic logging and final summary.
type progress struct {
	totalTables     atomic.Int64
	completedTables atomic.Int64
	failedTables    atomic.Int64
}

// log emits a structured progress event at INFO level.
func (p *progress) log(logger zerolog.Logger) {
	completed := p.completedTables.Load()
	failed := p.failedTables.Load()
	total := p.totalTables.Load()
	pct := 0
	if total > 0 {
		pct = int((completed + failed) * 100 / total)
	}
	logger.Info().
		Int64("completed", completed).
		Int64("failed", failed).
		Int64("total", total).
		Int("percent", pct).
		Msg("progress")
}

// workItem bundles all parameters for a single (account, table) collection operation.
type workItem struct {
	exp                    *exporter.Exporter
	sourceAccount          string
	tableName              string
	naturalKeys            []string
	where                  map[string]string
	columns                []string
	filterQuery            *config.FilterQuery
	deepHydration          bool
	skipCrossAccountFilter bool
	accountName            string
	pool                   *pgxpool.Pool
	logger                 zerolog.Logger
}

// runWorkerPool starts the worker pool (Phase 2) that processes account jobs concurrently.
func runWorkerPool(
	ctx context.Context,
	cancel context.CancelFunc,
	allJobs []accountJob,
	totalTables, maxConcurrency int,
	anyStrict bool,
	pool *pgxpool.Pool,
	schemaMgr *schema.Manager,
	logger zerolog.Logger,
) {
	logger.Info().
		Int("accounts", len(allJobs)).
		Int("total_tables", totalTables).
		Int("concurrency", maxConcurrency).
		Msg("starting collection")

	var prog progress
	prog.totalTables.Store(int64(totalTables))

	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				prog.log(logger)
			case <-ctx.Done():
				return
			}
		}
	}()

	overallStart := time.Now()
	jobCh := make(chan accountJob, len(allJobs))
	for _, job := range allJobs {
		jobCh <- job
	}
	close(jobCh)

	var wg sync.WaitGroup
	for range min(maxConcurrency, len(allJobs)) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobCh {
				if ctx.Err() != nil {
					return
				}

				acctLog := job.logger

				exp, err := exporter.New(
					job.pluginAlias,
					job.pluginName,
					job.binaryPath,
					acctLog.With().Str("component", "exporter").Logger(),
				)
				if err != nil {
					acctLog.Error().Err(err).Msg("failed to launch plugin, skipping account")
					continue
				}

				connConfig := job.connConfig
				accountID := job.accountID
				if job.orgAccount != nil {
					awsMA := buildAWSMultiAccount(job.drainpipeCfg)
					if awsMA != nil {
						acctCfg, err := awsMA.AssumeAccountRole(ctx, *job.orgAccount)
						if err != nil {
							exp.Close()
							acctLog.Warn().Err(err).Msg("skipping account: failed to assume role")
							continue
						}
						connConfig = acctCfg.ConnectionConfig
						accountID = acctCfg.AccountID
					}
				}

				if err := exp.SetConnectionConfig(connConfig); err != nil {
					exp.Close()
					acctLog.Error().Err(err).Msg("failed to configure plugin, skipping account")
					continue
				}

				if err := exp.SetRateLimiters(job.rateLimiters); err != nil {
					acctLog.Warn().Err(err).Msg("failed to set rate limiters (continuing without)")
				}

				sourceAccount := accountID
				if sourceAccount == "" && job.identityTable != "" && job.identityColumn != "" {
					row, err := exp.QueryOneRow(ctx, job.identityTable)
					if err != nil {
						exp.Close()
						acctLog.Error().Err(err).Str("identity_table", job.identityTable).Msg("failed to resolve account identity, skipping")
						continue
					}
					if row == nil {
						exp.Close()
						acctLog.Error().Str("identity_table", job.identityTable).Msg("identity table returned no data, skipping")
						continue
					}
					val, ok := row[job.identityColumn]
					if !ok || val == nil {
						exp.Close()
						acctLog.Error().Str("identity_column", job.identityColumn).Msg("identity column not found, skipping")
						continue
					}
					sourceAccount = fmt.Sprintf("%v", val)
				}
				acctLog.Info().Str("source_account", sourceAccount).Int("tables", len(job.tables)).Msg("collecting account")

				for _, tableName := range job.tables {
					if ctx.Err() != nil {
						exp.Close()
						return
					}

					tableWhere := job.where[tableName]
					if job.sourceAccountQual != "" && sourceAccount != "" && !job.sourceAccountQualExclude[tableName] {
						if _, alreadySet := tableWhere[job.sourceAccountQual]; !alreadySet {
							merged := make(map[string]string, len(tableWhere)+1)
							for k, v := range tableWhere {
								merged[k] = v
							}
							merged[job.sourceAccountQual] = sourceAccount
							tableWhere = merged
						}
					}

					item := workItem{
						exp:                    exp,
						sourceAccount:          sourceAccount,
						tableName:              tableName,
						naturalKeys:            job.supported[tableName],
						where:                  tableWhere,
						columns:                job.columns[tableName],
						filterQuery:            job.filterQueries[tableName],
						deepHydration:          job.deepHydration,
						skipCrossAccountFilter: job.crossAccountFilterExclude[tableName],
						accountName:            job.accountName,
						pool:                   pool,
						logger: acctLog.With().
							Str("table", tableName).
							Logger(),
					}

					err := collectTableWithRetry(ctx, item, schemaMgr, job.retries, job.retryDelay, job.tableTimeout)
					if err != nil {
						prog.failedTables.Add(1)
						item.logger.Error().Err(err).Msg("table failed after retries")
						if anyStrict {
							logger.Error().Msg("strict mode: aborting due to table failure")
							cancel()
						}
					} else {
						prog.completedTables.Add(1)
					}
				}

				exp.Close()
			}
		}()
	}

	wg.Wait()
	cancel()
	<-progressDone

	elapsed := time.Since(overallStart)
	logger.Info().
		Int64("completed", prog.completedTables.Load()).
		Int64("failed", prog.failedTables.Load()).
		Int64("total", prog.totalTables.Load()).
		Dur("duration", elapsed).
		Str("elapsed", elapsed.Round(time.Second).String()).
		Msg("collection complete")
}

func collectTableWithRetry(
	ctx context.Context,
	item workItem,
	schemaMgr *schema.Manager,
	maxRetries int,
	baseDelay time.Duration,
	tableTimeout time.Duration,
) error {
	ctx, cancel := context.WithTimeout(ctx, tableTimeout)
	defer cancel()

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			if lastErr != nil {
				return fmt.Errorf("timeout after %d attempts: %w", attempt, lastErr)
			}
			return ctx.Err()
		}

		if attempt > 0 {
			needsReconnect := item.exp.Exited()
			if !needsReconnect && strings.Contains(lastErr.Error(), "context canceled") {
				needsReconnect = true
			}
			if needsReconnect {
				item.logger.Warn().Bool("exited", item.exp.Exited()).Msg("plugin unhealthy, attempting reconnect")
				if err := item.exp.Reconnect(); err != nil {
					return fmt.Errorf("plugin reconnect failed: %w", err)
				}
			}

			delay := baseDelay * time.Duration(1<<(attempt-1))
			if delay > 2*time.Minute {
				delay = 2 * time.Minute
			}
			jitter := time.Duration(rand.Int63n(int64(delay / 2)))
			delay += jitter
			item.logger.Warn().
				Err(lastErr).
				Int("attempt", attempt+1).
				Int("max_attempts", maxRetries+1).
				Dur("backoff", delay).
				Msg("retrying table")
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return fmt.Errorf("timeout waiting for retry backoff: %w", lastErr)
			}
		}

		lastErr = collectTable(ctx, item, schemaMgr)
		if lastErr == nil {
			return nil
		}

		if ctx.Err() != nil {
			return fmt.Errorf("timeout during attempt %d: %w", attempt+1, lastErr)
		}
	}
	return lastErr
}

func collectTable(ctx context.Context, item workItem, schemaMgr *schema.Manager) error {
	if item.filterQuery != nil {
		return collectTableFiltered(ctx, item, schemaMgr)
	}

	pluginSchema, err := item.exp.GetSchema(item.tableName)
	if err != nil {
		return fmt.Errorf("get schema: %w", err)
	}

	columns, exportColumns := resolveColumns(pluginSchema, item)

	item.logger.Info().Int("columns", len(columns)).Strs("keys", item.naturalKeys).Msg("starting export")
	tableStart := time.Now()

	rowCh, errCh := item.exp.Export(ctx, item.tableName, exportColumns, item.where)
	imp := importer.New(item.pool, item.sourceAccount, item.skipCrossAccountFilter, item.logger.With().Str("component", "importer").Logger())
	result, err := imp.Import(ctx, item.tableName, item.naturalKeys, columns, rowCh)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}

	select {
	case exportErr := <-errCh:
		if exportErr != nil {
			return fmt.Errorf("export: %w", exportErr)
		}
	default:
	}

	item.logger.Info().
		Int64("rows", result.Rows).
		Int64("deleted", result.Deleted).
		Dur("duration", time.Since(tableStart)).
		Msg("table complete")

	return nil
}

// collectTableFiltered runs the filter_query SQL against Postgres to get
// qual values, then calls Export once per value and merges all rows.
// No error returns are expected during normal operation.
func collectTableFiltered(ctx context.Context, item workItem, schemaMgr *schema.Manager) error {
	fq := item.filterQuery

	rows, err := item.pool.Query(ctx, fq.Query)
	if err != nil {
		return fmt.Errorf("filter_query SQL: %w", err)
	}
	var filterValues []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			rows.Close()
			return fmt.Errorf("filter_query scan: %w", err)
		}
		filterValues = append(filterValues, v)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("filter_query iteration: %w", err)
	}

	if len(filterValues) == 0 {
		item.logger.Warn().Str("filter_column", fq.Column).Msg("filter_query returned no values, skipping collection")
		return nil
	}

	item.logger.Info().
		Str("filter_column", fq.Column).
		Int("filter_values", len(filterValues)).
		Msg("filter_query resolved")

	pluginSchema, err := item.exp.GetSchema(item.tableName)
	if err != nil {
		return fmt.Errorf("get schema: %w", err)
	}

	columns, exportColumns := resolveColumns(pluginSchema, item)

	item.logger.Info().Int("columns", len(columns)).Strs("keys", item.naturalKeys).Msg("starting filtered export")
	tableStart := time.Now()

	mergedRows := make(chan exporter.Row, 256)
	mergedErr := make(chan error, 1)

	go func() {
		defer close(mergedRows)

		for _, val := range filterValues {
			if ctx.Err() != nil {
				return
			}

			where := mergeWhere(item.where, fq.Column, val)
			rowCh, errCh := item.exp.Export(ctx, item.tableName, exportColumns, where)

			for row := range rowCh {
				select {
				case mergedRows <- row:
				case <-ctx.Done():
					return
				}
			}

			if exportErr := <-errCh; exportErr != nil {
				select {
				case mergedErr <- fmt.Errorf("export with %s=%s: %w", fq.Column, val, exportErr):
				default:
				}
				return
			}
		}
	}()

	imp := importer.New(item.pool, item.sourceAccount, item.skipCrossAccountFilter, item.logger.With().Str("component", "importer").Logger())
	result, err := imp.Import(ctx, item.tableName, item.naturalKeys, columns, mergedRows)
	if err != nil {
		return fmt.Errorf("import: %w", err)
	}

	select {
	case exportErr := <-mergedErr:
		if exportErr != nil {
			return exportErr
		}
	default:
	}

	item.logger.Info().
		Int64("rows", result.Rows).
		Int64("deleted", result.Deleted).
		Int("filter_values", len(filterValues)).
		Dur("duration", time.Since(tableStart)).
		Msg("filtered table complete")

	return nil
}

// resolveColumns computes the import and export column lists from the plugin
// schema, applying deep_hydration and explicit column filters.
func resolveColumns(pluginSchema *proto.TableSchema, item workItem) (columns, exportColumns []string) {
	columns = schema.TableColumns(pluginSchema)

	if !item.deepHydration {
		hydrateColumns := make(map[string]bool)
		for _, col := range pluginSchema.Columns {
			if col.GetHydrate() != "" {
				hydrateColumns[col.Name] = true
			}
		}
		if len(hydrateColumns) > 0 {
			var filtered []string
			for _, c := range columns {
				if !hydrateColumns[c] {
					filtered = append(filtered, c)
				}
			}
			item.logger.Info().Int("hydrate_columns_skipped", len(columns)-len(filtered)).Msg("deep_hydration disabled")
			columns = filtered
		}
	}

	exportColumns = columns
	if len(item.columns) > 0 {
		colSet := make(map[string]bool, len(item.columns))
		for _, c := range item.columns {
			colSet[c] = true
		}
		colSet["_ctx"] = true
		for _, k := range item.naturalKeys {
			colSet[k] = true
		}
		var filtered []string
		for _, c := range columns {
			if colSet[c] {
				filtered = append(filtered, c)
			}
		}
		columns = filtered
		exportColumns = filtered
	}

	return columns, exportColumns
}

// mergeWhere creates a new where map with the filter_query column added.
func mergeWhere(base map[string]string, column, value string) map[string]string {
	merged := make(map[string]string, len(base)+1)
	for k, v := range base {
		merged[k] = v
	}
	merged[column] = value
	return merged
}

package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"

	"github.com/justmiles/drainpipe/cmd/internal/config"
	"github.com/justmiles/drainpipe/cmd/internal/exporter"
	"github.com/justmiles/drainpipe/cmd/internal/importer"
	"github.com/justmiles/drainpipe/cmd/internal/match"
	"github.com/justmiles/drainpipe/cmd/internal/provider"
	"github.com/justmiles/drainpipe/cmd/internal/schema"
)

func main() {
	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("service", "drainpipe").
		Logger()

	// Redirect Go's default log package (used by Steampipe SDK) through zerolog.
	// This ensures all output is structured JSON.
	log.SetFlags(0)
	log.SetOutput(&zerologWriter{logger: logger.With().Str("source", "steampipe-sdk").Logger()})

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "drain":
		runDrain(logger)
	case "list-tables":
		runListTables(logger)
	case "list-providers":
		runListProviders()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Drainpipe — Steampipe export to PostgreSQL

Usage:
  drainpipe <command> [options]

Commands:
  drain            Export resources into PostgreSQL
  list-tables      List available tables for a provider
  list-providers   List registered providers

Drain options:
  --config, -c     Config file path (default: drainpipe.yaml if it exists)
  --provider, -p   Provider name (default: aws)
  --tables, -t     Comma-separated table patterns (overrides config file)
                   Examples: "aws_ec2_*", "aws_s3_bucket", "aws_*"
                   Only matches tables with discoverable natural keys.

List-tables options:
  --provider, -p   Provider name (default: aws)
  --unsupported    Also show tables without discoverable natural keys

Environment variables:
  DB_HOST / DB_PORT / DB_NAME / DB_USER / DB_PASSWORD
  AWS_PROFILE              AWS profile for credentials
  AWS_REGIONS              Comma-separated AWS regions to collect
  AWS_ORG_ROLE_NAME        Role to assume in each member account (enables org mode)
  AWS_ORG_ADMIN_ACCOUNT_ID Admin account ID to skip during org collection
`)
}

func runDrain(logger zerolog.Logger) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	flags := parseFlags(os.Args[2:])
	configPathRaw := flagOrDefault(flags, "config", "drainpipe.yaml")
	providerNameFlag := flagOrDefault(flags, "provider", "")
	tablePatternsFlag := flagOrDefault(flags, "tables", "")

	// ── Load config(s) ────────────────────────────────────────────────
	// Multiple files: --config a.yaml --config b.yaml (comma-joined by parseFlags)
	// Multi-document: a single file with "---" separators
	var configPaths []string
	for _, p := range strings.Split(configPathRaw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			configPaths = append(configPaths, p)
		}
	}

	configs, err := config.LoadAllDrainpipeConfigs(configPaths)
	if err != nil {
		logger.Fatal().Err(err).Strs("configs", configPaths).Msg("failed to load config")
	}

	// If no config files found, create a synthetic config from CLI flags
	if len(configs) == 0 {
		provName := providerNameFlag
		if provName == "" {
			provName = "aws"
		}
		synthetic := &config.DrainpipeConfig{
			Provider: provName,
		}
		if tablePatternsFlag != "" {
			for _, p := range strings.Split(tablePatternsFlag, ",") {
				p = strings.TrimSpace(p)
				if p != "" {
					synthetic.Tables = append(synthetic.Tables, config.TableEntry{Name: p})
				}
			}
		}
		configs = []*config.DrainpipeConfig{synthetic}
		logger.Info().Str("provider", provName).Msg("no config file; using CLI flags")
	} else {
		logger.Info().Int("config_blocks", len(configs)).Strs("files", configPaths).Msg("loaded drainpipe config(s)")
	}

	// ── Connect to PostgreSQL ─────────────────────────────────────────
	dbCfg := config.Load()
	pool, err := pgxpool.New(ctx, dbCfg.DSN())
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to PostgreSQL")
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		logger.Fatal().Err(err).Msg("failed to ping PostgreSQL")
	}
	logger.Info().Msg("connected to PostgreSQL")

	// ── Phase 1: Process each config block and build work items ───────
	type accountSetup struct {
		connConfig   string
		accountID    string
		accountName  string
		tableEntries []config.TableEntry // tables with optional where filters
		orgAccount   *provider.AccountInfo
	}

	var allWorkItems []workItem
	ensuredTables := make(map[string]bool)
	schemaMgr := schema.New(pool, logger.With().Str("component", "schema").Logger())

	// Track maximum settings across all config blocks for the shared worker pool
	maxConcurrency := 1
	maxRetries := 3
	maxRetryDelay := 10 * time.Second
	maxTableTimeout := 10 * time.Minute
	anyStrict := false

	for cfgIdx, drainpipeCfg := range configs {
		cfgLog := logger.With().Int("config_block", cfgIdx+1).Logger()

		// Resolve provider
		providerName := drainpipeCfg.Provider
		if providerName == "" {
			providerName = "aws"
		}
		prov := provider.Get(providerName)
		if prov == nil {
			cfgLog.Fatal().Str("provider", providerName).Strs("available", provider.Names()).Msg("unknown provider")
		}
		cfgLog = cfgLog.With().Str("provider", providerName).Logger()

		// Resolve operational settings
		concurrency := 1
		retries := 3
		retryDelay := 10 * time.Second
		tableTimeout := 10 * time.Minute
		strict := false

		if drainpipeCfg.Concurrency > 0 {
			concurrency = drainpipeCfg.Concurrency
		}
		if drainpipeCfg.Retries > 0 {
			retries = drainpipeCfg.Retries
		}
		if drainpipeCfg.RetryDelay > 0 {
			retryDelay = drainpipeCfg.RetryDelay
		}
		if drainpipeCfg.TableTimeout > 0 {
			tableTimeout = drainpipeCfg.TableTimeout
		}
		strict = drainpipeCfg.Strict

		// Track maximums for the shared worker pool
		if concurrency > maxConcurrency {
			maxConcurrency = concurrency
		}
		if retries > maxRetries {
			maxRetries = retries
		}
		if retryDelay > maxRetryDelay {
			maxRetryDelay = retryDelay
		}
		if tableTimeout > maxTableTimeout {
			maxTableTimeout = tableTimeout
		}
		if strict {
			anyStrict = true
		}

		// Apply config to the provider (org settings, regions, profile)
		// Create a fresh provider instance to avoid cross-contamination between config blocks
		if awsProv, ok := prov.(*provider.AWSProvider); ok {
			// Clone so each config block has its own settings
			cloned := *awsProv
			if drainpipeCfg.Profile != "" {
				cloned.Profile = drainpipeCfg.Profile
			}
			effectiveOrg := drainpipeCfg.EffectiveOrg()
			if effectiveOrg != nil {
				cloned.OrgRoleName = effectiveOrg.RoleName
				cloned.AssumeRoleName = effectiveOrg.AssumeRoleName
				cloned.OrgAdminAccountID = effectiveOrg.AdminAccountID
				cloned.Organizations = effectiveOrg.Organizations
			}
			if len(drainpipeCfg.Regions) > 0 {
				cloned.Regions = drainpipeCfg.Regions
			}
			prov = &cloned
		}

		// Carry table entries (with where filters) from config
		var cfgTableEntries []config.TableEntry
		if len(drainpipeCfg.Tables) > 0 {
			cfgTableEntries = drainpipeCfg.Tables
		}

		// Build account setups for this config block
		var accountSetups []accountSetup

		if len(drainpipeCfg.Accounts) > 0 {
			// Explicit accounts from config (SSO / named profiles)
			defaultRegions := drainpipeCfg.Regions
			for _, entry := range drainpipeCfg.Accounts {
				regions := entry.Regions
				if len(regions) == 0 {
					regions = defaultRegions
				}
				var configParts []string
				if entry.Profile != "" {
					configParts = append(configParts, fmt.Sprintf(`  profile = %q`, entry.Profile))
				}
				if len(regions) > 0 {
					quoted := make([]string, len(regions))
					for i, r := range regions {
						quoted[i] = fmt.Sprintf("%q", r)
					}
					configParts = append(configParts, fmt.Sprintf("  regions = [%s]", strings.Join(quoted, ", ")))
				}

				acctEntries := cfgTableEntries
				if len(acctEntries) == 0 {
					overrides, skip := drainpipeCfg.TablesForAccount("", entry.Name)
					if skip {
						cfgLog.Info().Str("account_name", entry.Name).Msg("skipping account (config override)")
						continue
					}
					if len(overrides) > 0 {
						acctEntries = overrides
					}
				}

				accountSetups = append(accountSetups, accountSetup{
					connConfig:   strings.Join(configParts, "\n"),
					accountName:  entry.Name,
					tableEntries: acctEntries,
				})
			}
			cfgLog.Info().Int("accounts", len(accountSetups)).Msg("using explicit accounts from config")
		} else if mp, ok := prov.(provider.MultiAccountProvider); ok {
			// Org discovery (metadata only, no credentials yet)
			accounts, err := mp.DiscoverAccounts(ctx)
			if err != nil {
				cfgLog.Fatal().Err(err).Msg("failed to discover accounts")
			}
			if len(accounts) > 0 {
				for _, acct := range accounts {
					acctEntries := cfgTableEntries
					if len(acctEntries) == 0 {
						overrides, skip := drainpipeCfg.TablesForAccount(acct.AccountID, acct.AccountName)
						if skip {
							cfgLog.Info().Str("account_id", acct.AccountID).Msg("skipping account (config override)")
							continue
						}
						if len(overrides) > 0 {
							acctEntries = overrides
						}
					}
					acctCopy := acct // capture for closure
					accountSetups = append(accountSetups, accountSetup{
						accountID:    acct.AccountID,
						accountName:  acct.AccountName,
						tableEntries: acctEntries,
						orgAccount:   &acctCopy,
					})
				}
				cfgLog.Info().Int("accounts", len(accountSetups)).Msg("multi-account mode: collecting from organization")
			}
		}

		if len(accountSetups) == 0 {
			// Single-account mode
			connConfig := prov.DefaultConnectionConfig()
			accountSetups = append(accountSetups, accountSetup{
				connConfig:   connConfig,
				tableEntries: cfgTableEntries,
			})
		}

		// Build work items for each account in this config block
		for _, setup := range accountSetups {
			acctLog := cfgLog.With().
				Str("account_id", setup.accountID).
				Str("account_name", setup.accountName).
				Logger()

			// Lazy credential refresh for org mode
			connConfig := setup.connConfig
			accountID := setup.accountID
			if setup.orgAccount != nil {
				if mp, ok := prov.(provider.MultiAccountProvider); ok {
					acctCfg, err := mp.AssumeAccountRole(ctx, *setup.orgAccount)
					if err != nil {
						acctLog.Warn().Err(err).Msg("skipping account: failed to assume role")
						continue
					}
					connConfig = acctCfg.ConnectionConfig
					accountID = acctCfg.AccountID
				}
			}

			// Initialize the plugin for this account
			exp := exporter.New(prov.Name(), prov.PluginFunc(), acctLog.With().Str("component", "exporter").Logger())
			if err := exp.SetConnectionConfig(connConfig); err != nil {
				acctLog.Error().Err(err).Msg("failed to configure plugin, skipping account")
				continue
			}

			// Resolve account identity
			sourceAccount := accountID
			if sourceAccount == "" {
				queryFunc := func(ctx context.Context, table string) (map[string]interface{}, error) {
					return exp.QueryOneRow(ctx, table)
				}
				acct, err := prov.ResolveAccount(ctx, queryFunc)
				if err != nil {
					acctLog.Error().Err(err).Msg("failed to resolve account identity, skipping")
					continue
				}
				sourceAccount = acct
			}
			acctLog.Info().Str("source_account", sourceAccount).Msg("account identity resolved")

			// Discover supported tables
			supported, err := supportedTables(exp, prov)
			if err != nil {
				acctLog.Error().Err(err).Msg("failed to discover supported tables, skipping")
				continue
			}

			// Build where-filter lookup from table entries
			entryMap := config.TableEntryMap(setup.tableEntries)

			// Resolve table patterns
			var tables []string
			if len(setup.tableEntries) > 0 {
				patterns := config.TableNames(setup.tableEntries)
				supportedNames := make([]string, 0, len(supported))
				for name := range supported {
					supportedNames = append(supportedNames, name)
				}
				sort.Strings(supportedNames)
				tables = match.Tables(supportedNames, patterns)

				if len(tables) == 0 {
					suggestions := match.Suggest(supportedNames, patterns, 3)
					if strict {
						cfgLog.Fatal().
							Str("account", setup.accountName).
							Strs("patterns", patterns).
							Strs("did_you_mean", suggestions).
							Msg("strict mode: configured table patterns matched no supported tables")
					}
					acctLog.Warn().
						Strs("patterns", patterns).
						Strs("did_you_mean", suggestions).
						Msg("no supported tables matched, skipping account")
					continue
				}
			} else {
				for name := range supported {
					tables = append(tables, name)
				}
				sort.Strings(tables)
			}

			acctLog.Info().Int("tables", len(tables)).Msg("enqueuing tables for collection")

			for _, tableName := range tables {
				// Ensure table schema once (not per-account or per-config)
				if !ensuredTables[tableName] {
					pluginSchema, err := exp.GetSchema(tableName)
					if err != nil {
						acctLog.Error().Err(err).Str("table", tableName).Msg("failed to get schema, skipping table")
						continue
					}
					if err := schemaMgr.EnsureTable(ctx, tableName, pluginSchema, supported[tableName]); err != nil {
						acctLog.Error().Err(err).Str("table", tableName).Msg("failed to ensure table schema, skipping table")
						continue
					}
					ensuredTables[tableName] = true
				}

				// Look up where filter for this table
				var where map[string]string
				if te, ok := entryMap[tableName]; ok {
					where = te.Where
				}

				allWorkItems = append(allWorkItems, workItem{
					exp:              exp,
					sourceAccount: sourceAccount,
					tableName:        tableName,
					naturalKeys:      supported[tableName],
					where:            where,
					accountName:      setup.accountName,
					pool:             pool,
					prov:             prov,
					logger: acctLog.With().
						Str("table", tableName).
						Logger(),
				})
			}
		}
	}

	if len(allWorkItems) == 0 {
		logger.Warn().Msg("no work items to process")
		return
	}

	// ── Phase 2: Process work items with shared worker pool ────────────
	logger.Info().
		Int("total_tables", len(allWorkItems)).
		Int("concurrency", maxConcurrency).
		Int("max_retries", maxRetries).
		Msg("starting collection")

	var prog progress
	prog.totalTables.Store(int64(len(allWorkItems)))

	// Periodic progress reporter
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
	sem := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	for _, item := range allWorkItems {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{} // acquire slot
		wg.Add(1)
		go func(item workItem) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			err := collectTableWithRetry(ctx, item, schemaMgr, maxRetries, maxRetryDelay, maxTableTimeout)
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
		}(item)
	}

	wg.Wait()
	cancel() // stop progress reporter
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

// ── Work item processing ──────────────────────────────────────────────

// collectTableWithRetry runs a single table export+import with retry and backoff.
// The table_timeout is a single overall budget — retries continue within it.
func collectTableWithRetry(
	ctx context.Context,
	item workItem,
	schemaMgr *schema.Manager,
	maxRetries int,
	baseDelay time.Duration,
	tableTimeout time.Duration,
) error {
	// Create a single timeout context that covers ALL attempts.
	// Retries happen within this budget, not with fresh timeouts.
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
			delay := baseDelay * time.Duration(1<<(attempt-1))
			// Cap backoff at 2 minutes to avoid waiting too long
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

		// If the overall timeout fired, report it clearly
		if ctx.Err() != nil {
			return fmt.Errorf("timeout during attempt %d: %w", attempt+1, lastErr)
		}
	}
	return lastErr
}

// collectTable runs a single table export+import cycle.
// The context should already carry the overall timeout from collectTableWithRetry.
func collectTable(ctx context.Context, item workItem, schemaMgr *schema.Manager) error {
	pluginSchema, err := item.exp.GetSchema(item.tableName)
	if err != nil {
		return fmt.Errorf("get schema: %w", err)
	}

	columns := schema.TableColumns(pluginSchema)
	item.logger.Info().Int("columns", len(columns)).Strs("keys", item.naturalKeys).Msg("starting export")
	tableStart := time.Now()

	rowCh, errCh := item.exp.Export(ctx, item.tableName, item.where)
	imp := importer.New(item.pool, item.sourceAccount, item.logger.With().Str("component", "importer").Logger())
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

// ── Progress tracker ──────────────────────────────────────────────────

type progress struct {
	totalTables     atomic.Int64
	completedTables atomic.Int64
	failedTables    atomic.Int64
}

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

// ── Work item type (must be declared at package level for method access) ──

type workItem struct {
	exp              *exporter.Exporter
	sourceAccount string
	tableName        string
	naturalKeys      []string
	where            map[string]string
	accountName      string
	pool             *pgxpool.Pool
	prov             provider.Provider
	logger           zerolog.Logger
}

func runListTables(logger zerolog.Logger) {
	flags := parseFlags(os.Args[2:])
	providerName := flagOrDefault(flags, "provider", "aws")
	showUnsupported := flagHas(flags, "unsupported")

	prov := provider.Get(providerName)
	if prov == nil {
		logger.Fatal().Str("provider", providerName).Msg("unknown provider")
	}

	exp := exporter.New(prov.Name(), prov.PluginFunc(), logger)
	if err := exp.SetConnectionConfig(prov.DefaultConnectionConfig()); err != nil {
		logger.Fatal().Err(err).Msg("failed to configure plugin")
	}

	supported, err := supportedTables(exp, prov)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to discover tables")
	}

	if showUnsupported {
		// Show all tables, marking supported ones
		allTables, err := exp.ListTables()
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to list tables")
		}
		sort.Strings(allTables)

		for _, t := range allTables {
			if keys, ok := supported[t]; ok {
				fmt.Printf("  %-50s keys: %s\n", t, strings.Join(keys, ", "))
			} else {
				fmt.Printf("  %-50s (unsupported)\n", t)
			}
		}
		fmt.Printf("\n%d tables total, %d supported\n", len(allTables), len(supported))
	} else {
		// Only show supported tables with their keys
		names := make([]string, 0, len(supported))
		for name := range supported {
			names = append(names, name)
		}
		sort.Strings(names)

		for _, t := range names {
			fmt.Printf("  %-50s keys: %s\n", t, strings.Join(supported[t], ", "))
		}
		fmt.Printf("\n%d supported tables\n", len(names))
	}
}

// supportedTables returns a map of table name → natural key columns for all
// tables in the plugin that have discoverable natural keys.
func supportedTables(exp *exporter.Exporter, prov provider.Provider) (map[string][]string, error) {
	allSchemas, err := exp.GetAllSchemas()
	if err != nil {
		return nil, err
	}

	result := make(map[string][]string)
	for name, tableSchema := range allSchemas {
		keys := prov.NaturalKeyColumns(name, tableSchema)
		if len(keys) > 0 {
			result[name] = keys
		}
	}
	return result, nil
}

func runListProviders() {
	names := provider.Names()
	sort.Strings(names)
	for _, name := range names {
		fmt.Println(name)
	}
}

// parseFlags parses CLI flags. Repeated flags are accumulated with comma-joining.
func parseFlags(args []string) map[string]string {
	flags := map[string]string{}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			continue
		}
		arg = strings.TrimLeft(arg, "-")

		var key, val string
		if idx := strings.Index(arg, "="); idx >= 0 {
			key = arg[:idx]
			val = arg[idx+1:]
		} else {
			key = arg
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				val = args[i+1]
				i++
			}
		}

		// Normalize short flags
		switch key {
		case "p":
			key = "provider"
		case "t":
			key = "tables"
		case "c":
			key = "config"
		}

		// Accumulate repeated flags with comma
		if existing, ok := flags[key]; ok && existing != "" {
			flags[key] = existing + "," + val
		} else {
			flags[key] = val
		}
	}
	return flags
}

func flagOrDefault(flags map[string]string, key, defaultVal string) string {
	if v, ok := flags[key]; ok && v != "" {
		return v
	}
	return defaultVal
}

// flagHas returns true if a boolean flag was passed (e.g., --unsupported).
func flagHas(flags map[string]string, key string) bool {
	_, ok := flags[key]
	return ok
}

// ── Steampipe SDK log adapter ─────────────────────────────────────────

// zerologWriter adapts Go's standard log.Printf output (used by Steampipe SDK)
// into structured zerolog JSON. The SDK uses "[LEVEL] message" format.
type zerologWriter struct {
	logger zerolog.Logger
}

func (w *zerologWriter) Write(p []byte) (n int, err error) {
	msg := strings.TrimSpace(string(p))
	if msg == "" {
		return len(p), nil
	}

	// Parse SDK log format: "[LEVEL] message" or just "message"
	level, body := parseSteampipeLog(msg)

	switch level {
	case "TRACE":
		// Drop TRACE — extremely noisy and not useful in production
		return len(p), nil
	case "INFO":
		w.logger.Info().Msg(body)
	case "WARN":
		w.logger.Warn().Msg(body)
	case "ERROR":
		w.logger.Error().Msg(body)
	default:
		w.logger.Debug().Msg(msg)
	}
	return len(p), nil
}

// parseSteampipeLog extracts the level and body from a Steampipe SDK log line.
// Format: "[LEVEL] message" or "timestamp: [LEVEL] message"
func parseSteampipeLog(msg string) (string, string) {
	// Try to find [LEVEL] pattern
	for _, lvl := range []string{"TRACE", "INFO", "WARN", "ERROR"} {
		tag := "[" + lvl + "]"
		if idx := strings.Index(msg, tag); idx >= 0 {
			body := strings.TrimSpace(msg[idx+len(tag):])
			return lvl, body
		}
	}
	return "", msg
}

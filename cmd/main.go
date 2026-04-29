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

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"

	"github.com/justmiles/drainpipe/cmd/internal/config"
	"github.com/justmiles/drainpipe/cmd/internal/exporter"
	"github.com/justmiles/drainpipe/cmd/internal/importer"
	"github.com/justmiles/drainpipe/cmd/internal/match"
	"github.com/justmiles/drainpipe/cmd/internal/pluginmanager"
	"github.com/justmiles/drainpipe/cmd/internal/provider"
	"github.com/justmiles/drainpipe/cmd/internal/schema"
)

func main() {
	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("service", "drainpipe").
		Logger()

	// Redirect Go's default log package (used by Steampipe SDK) through zerolog.
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
	case "download-plugins":
		runDownloadPlugins(logger)
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
  drain              Export resources into PostgreSQL
  list-tables        List available tables for a provider
  list-providers     List known providers and their default plugins
  download-plugins   Download plugin binaries for a config file

Drain options:
  --config, -c     Config file path(s), .hcl or .yaml (default: drainpipe.hcl or drainpipe.yaml)
  --provider, -p   Provider name (default: aws)
  --tables, -t     Comma-separated table patterns (overrides config file)
                   Examples: "aws_ec2_*", "aws_s3_bucket", "aws_*"
                   Only matches tables with discoverable natural keys.

List-tables options:
  --provider, -p   Provider name or plugin spec (default: aws)

Download-plugins options:
  --config, -c     Config file path (default: drainpipe.yaml)

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
	configPathRaw := flagOrDefault(flags, "config", defaultConfigPath())
	providerNameFlag := flagOrDefault(flags, "provider", "")
	tablePatternsFlag := flagOrDefault(flags, "tables", "")

	// ── Load config(s) ────────────────────────────────────────────────
	var configPaths []string
	for _, p := range strings.Split(configPathRaw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			configPaths = append(configPaths, p)
		}
	}

	configs, rateLimiters, err := config.LoadAllConfigs(configPaths)
	if err != nil {
		logger.Fatal().Err(err).Strs("configs", configPaths).Msg("failed to load config")
	}

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

	// ── Plugin manager ───────────────────────────────────────────────
	pluginMgr := pluginmanager.NewManager("", logger.With().Str("component", "pluginmanager").Logger())

	// ── Phase 1: Build account jobs from config ─────────────────────
	// Each accountJob describes one account to collect. Plugin processes are
	// launched lazily inside workers so only maxConcurrency plugins exist at
	// any time, keeping memory bounded.
	type accountJob struct {
		pluginAlias    string
		pluginName     string
		binaryPath     string
		connConfig     string
		accountID      string
		accountName    string
		tableEntries   []config.TableEntry
		orgAccount     *provider.AccountInfo
		drainpipeCfg   *config.DrainpipeConfig
		preferredKey   string
		identityTable  string
		identityColumn string
		strict         bool
		deepHydration  bool
		rateLimiters   []*proto.RateLimiterDefinition
		logger         zerolog.Logger

		// Resolved during schema validation (Phase 1b)
		tables       []string                           // ordered table names to collect
		supported    map[string][]string                // table → natural key columns
		where        map[string]map[string]string       // table → where filters
		columns      map[string][]string                // table → explicit column list (nil = all)
		filterQueries map[string]*config.FilterQuery    // table → dynamic pre-filter (nil = normal)
	}

	var allJobs []accountJob
	schemaMgr := schema.New(pool, logger.With().Str("component", "schema").Logger())

	maxConcurrency := 1
	maxRetries := 3
	maxRetryDelay := 10 * time.Second
	maxTableTimeout := 10 * time.Minute
	anyStrict := false

	for cfgIdx, drainpipeCfg := range configs {
		cfgLog := logger.With().Int("config_block", cfgIdx+1).Logger()

		// Resolve plugin specifier
		pluginSpec, ok := drainpipeCfg.ResolvePluginSpec()
		if !ok {
			provName := drainpipeCfg.Provider
			if provName == "" {
				provName = "aws"
			}
			cfgLog.Fatal().Str("provider", provName).
				Strs("known", knownProviderNames()).
				Msg("unknown provider; specify 'plugin' field or use a known provider name")
		}

		pluginRef, err := pluginmanager.ParsePluginRef(pluginSpec)
		if err != nil {
			cfgLog.Fatal().Err(err).Str("plugin", pluginSpec).Msg("invalid plugin specifier")
		}

		cfgLog = cfgLog.With().
			Str("plugin", pluginRef.Org+"/"+pluginRef.Name).
			Str("version", pluginRef.Version).
			Logger()

		// Resolve plugin binary
		var binaryPath string
		if drainpipeCfg.PluginPath != "" {
			binaryPath, err = pluginMgr.EnsurePluginFromPath(drainpipeCfg.PluginPath)
		} else {
			binaryPath, err = pluginMgr.EnsurePlugin(pluginRef)
		}
		if err != nil {
			cfgLog.Fatal().Err(err).Msg("failed to resolve plugin binary")
		}

		cfgLog.Info().Str("binary", binaryPath).Msg("plugin binary resolved")

		// Resolve config-driven settings
		preferredKey := drainpipeCfg.ResolveNaturalKey()
		identityTable, identityColumn := drainpipeCfg.ResolveIdentity()

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

		deepHydration := true
		if drainpipeCfg.DeepHydration != nil {
			deepHydration = *drainpipeCfg.DeepHydration
		}

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

		var cfgTableEntries []config.TableEntry
		if len(drainpipeCfg.Tables) > 0 {
			cfgTableEntries = drainpipeCfg.Tables
		}

		// Build lightweight account setups (no plugin processes yet)
		type accountSetup struct {
			connConfig   string
			accountID    string
			accountName  string
			tableEntries []config.TableEntry
			orgAccount   *provider.AccountInfo
		}

		var accountSetups []accountSetup

		if drainpipeCfg.Provider == "aws" || pluginRef.Name == "aws" {
			awsMA := buildAWSMultiAccount(drainpipeCfg)

			if len(drainpipeCfg.Connection.Accounts) > 0 {
				defaultRegions := drainpipeCfg.Connection.Regions
				for _, entry := range drainpipeCfg.Connection.Accounts {
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

			} else if awsMA != nil {
				accounts, err := awsMA.DiscoverAccounts(ctx)
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
						acctCopy := acct
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
		} else if drainpipeCfg.Provider == "azure" || pluginRef.Name == "azure" {
			azureMP := buildAzureMultiSubscription(drainpipeCfg)
			if azureMP != nil {
				subs, err := azureMP.DiscoverSubscriptions(ctx)
				if err != nil {
					cfgLog.Fatal().Err(err).Msg("failed to discover Azure subscriptions")
				}
				for _, sub := range subs {
					acctEntries := cfgTableEntries
					if len(acctEntries) == 0 {
						overrides, skip := drainpipeCfg.TablesForAccount(sub.SubscriptionID, sub.DisplayName)
						if skip {
							cfgLog.Info().Str("subscription_id", sub.SubscriptionID).Msg("skipping subscription (config override)")
							continue
						}
						if len(overrides) > 0 {
							acctEntries = overrides
						}
					}
					accountSetups = append(accountSetups, accountSetup{
						connConfig:   sub.ConnectionConfig,
						accountID:    sub.SubscriptionID,
						accountName:  sub.DisplayName,
						tableEntries: acctEntries,
					})
				}
				cfgLog.Info().Int("subscriptions", len(accountSetups)).Msg("multi-subscription mode: collecting from Azure tenants")
			}
		}

		if len(accountSetups) == 0 {
			connConfig := drainpipeCfg.ResolveConnectionHCL()
			accountSetups = append(accountSetups, accountSetup{
				connConfig:   connConfig,
				accountID:    drainpipeCfg.Connection.AccountID,
				tableEntries: cfgTableEntries,
			})
		}

		var limiterDefs []*proto.RateLimiterDefinition
		if rateLimiters != nil {
			for _, rl := range rateLimiters[pluginRef.Name] {
				limiterDefs = append(limiterDefs, &proto.RateLimiterDefinition{
					Name:           rl.Name,
					BucketSize:     rl.BucketSize,
					FillRate:       float32(rl.FillRate),
					MaxConcurrency: rl.MaxConcurrency,
					Scope:          rl.Scope,
					Where:          rl.Where,
				})
			}
		}

		for _, setup := range accountSetups {
			allJobs = append(allJobs, accountJob{
				pluginAlias:    pluginRef.Name,
				pluginName:     pluginRef.PluginName(),
				binaryPath:     binaryPath,
				connConfig:     setup.connConfig,
				accountID:      setup.accountID,
				accountName:    setup.accountName,
				tableEntries:   setup.tableEntries,
				orgAccount:     setup.orgAccount,
				drainpipeCfg:   drainpipeCfg,
				preferredKey:   preferredKey,
				identityTable:  identityTable,
				identityColumn: identityColumn,
				strict:         strict,
				deepHydration:  deepHydration,
				rateLimiters:   limiterDefs,
				logger: cfgLog.With().
					Str("account_id", setup.accountID).
					Str("account_name", setup.accountName).
					Logger(),
			})
		}
	}

	if len(allJobs) == 0 {
		logger.Warn().Msg("no accounts to process")
		return
	}

	// ── Phase 1b: Validate schemas with a temporary plugin ───────────
	// Launch one plugin per unique binary to discover supported tables,
	// resolve table lists, ensure DB schemas, then shut it down. This
	// avoids repeating schema work across 160+ accounts.
	type pluginInfo struct {
		alias      string
		name       string
		binaryPath string
	}
	seenPlugins := make(map[string]bool)
	var uniquePlugins []pluginInfo
	for _, job := range allJobs {
		if !seenPlugins[job.binaryPath] {
			seenPlugins[job.binaryPath] = true
			uniquePlugins = append(uniquePlugins, pluginInfo{
				alias:      job.pluginAlias,
				name:       job.pluginName,
				binaryPath: job.binaryPath,
			})
		}
	}

	pluginSupported := make(map[string]map[string][]string)    // binaryPath → supported tables
	pluginExporters := make(map[string]*exporter.Exporter) // binaryPath → temp exporter for schema

	for _, pi := range uniquePlugins {
		schemaLog := logger.With().Str("plugin", pi.alias).Logger()
		schemaLog.Info().Msg("validating table schemas")

		exp, err := exporter.New(pi.alias, pi.name, pi.binaryPath, schemaLog)
		if err != nil {
			schemaLog.Fatal().Err(err).Msg("failed to launch plugin for schema validation")
		}
		if err := exp.SetConnectionConfig(""); err != nil {
			exp.Close()
			schemaLog.Fatal().Err(err).Msg("failed to configure plugin for schema validation")
		}

		// Find the preferredKey for this plugin from the first matching job
		var preferredKey string
		for i := range allJobs {
			if allJobs[i].binaryPath == pi.binaryPath {
				preferredKey = allJobs[i].preferredKey
				break
			}
		}

		supported, err := supportedTables(exp, preferredKey)
		if err != nil {
			exp.Close()
			schemaLog.Fatal().Err(err).Msg("failed to discover supported tables")
		}

		pluginSupported[pi.binaryPath] = supported
		pluginExporters[pi.binaryPath] = exp
		schemaLog.Info().Int("supported_tables", len(supported)).Msg("discovered plugin tables")
	}

	// Resolve table lists and where filters for each job BEFORE
	// ensuring schemas, so we only create tables that will be used.
	totalTables := 0
	var resolvedJobs []accountJob
	neededTables := make(map[string]map[string]bool) // binaryPath → set of table names

	for i := range allJobs {
		job := &allJobs[i]
		supported := pluginSupported[job.binaryPath]
		job.supported = supported

		entryMap := config.TableEntryMap(job.tableEntries)

		if len(job.tableEntries) > 0 {
			patterns := config.TableNames(job.tableEntries)
			supportedNames := make([]string, 0, len(supported))
			for name := range supported {
				supportedNames = append(supportedNames, name)
			}
			sort.Strings(supportedNames)
			tables := match.Tables(supportedNames, patterns)

			if len(tables) == 0 {
				suggestions := match.Suggest(supportedNames, patterns, 3)
				if job.strict {
					job.logger.Fatal().
						Strs("patterns", patterns).
						Strs("did_you_mean", suggestions).
						Msg("strict mode: configured table patterns matched no supported tables")
				}
				job.logger.Warn().
					Strs("patterns", patterns).
					Strs("did_you_mean", suggestions).
					Msg("no supported tables matched, skipping account")
				continue
			}
			job.tables = tables
		} else {
			tables := make([]string, 0, len(supported))
			for name := range supported {
				tables = append(tables, name)
			}
			sort.Strings(tables)
			job.tables = tables
		}

		job.where = make(map[string]map[string]string)
		job.columns = make(map[string][]string)
		job.filterQueries = make(map[string]*config.FilterQuery)
		for _, tableName := range job.tables {
			if te, ok := entryMap[tableName]; ok {
				if len(te.Where) > 0 {
					job.where[tableName] = te.Where
				}
				if len(te.Columns) > 0 {
					job.columns[tableName] = te.Columns
				}
				if te.FilterQuery != nil {
					job.filterQueries[tableName] = te.FilterQuery
				}
			}
		}

		// Sort tables so that filter_query tables run after their
		// dependencies have been collected and imported.
		sort.SliceStable(job.tables, func(i, j int) bool {
			_, iHas := job.filterQueries[job.tables[i]]
			_, jHas := job.filterQueries[job.tables[j]]
			return !iHas && jHas
		})

		if neededTables[job.binaryPath] == nil {
			neededTables[job.binaryPath] = make(map[string]bool)
		}
		for _, t := range job.tables {
			neededTables[job.binaryPath][t] = true
		}

		totalTables += len(job.tables)
		resolvedJobs = append(resolvedJobs, *job)
	}
	allJobs = resolvedJobs

	if len(allJobs) == 0 {
		for _, exp := range pluginExporters {
			exp.Close()
		}
		logger.Warn().Msg("no accounts with matching tables to process")
		return
	}

	// Ensure DB schemas only for tables that will actually be collected.
	for binaryPath, tables := range neededTables {
		exp := pluginExporters[binaryPath]
		supported := pluginSupported[binaryPath]
		for tableName := range tables {
			pluginSchema, err := exp.GetSchema(tableName)
			if err != nil {
				logger.Warn().Err(err).Str("table", tableName).Msg("failed to get schema, skipping")
				continue
			}
			if err := schemaMgr.EnsureTable(ctx, tableName, pluginSchema, supported[tableName]); err != nil {
				logger.Warn().Err(err).Str("table", tableName).Msg("failed to ensure table schema")
			}
		}
		exp.Close()
	}
	logger.Info().Int("tables", len(neededTables)).Msg("schema validation complete")

	// ── Phase 2: Worker pool processes account jobs ───────────────────
	// Each worker launches a fresh plugin process per account.
	logger.Info().
		Int("accounts", len(allJobs)).
		Int("total_tables", totalTables).
		Int("concurrency", maxConcurrency).
		Int("max_retries", maxRetries).
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

				// Fresh plugin process per account — guarantees no stale
				// credentials from a previous account's session.
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

				// Resolve credentials for org mode (just-in-time)
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

				// Resolve account identity
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

					item := workItem{
						exp:           exp,
						sourceAccount: sourceAccount,
						tableName:     tableName,
						naturalKeys:   job.supported[tableName],
						where:         job.where[tableName],
						columns:       job.columns[tableName],
						filterQuery:   job.filterQueries[tableName],
						deepHydration: job.deepHydration,
						accountName:   job.accountName,
						pool:          pool,
						logger: acctLog.With().
							Str("table", tableName).
							Logger(),
					}

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


// buildAWSMultiAccount constructs an AWSMultiAccount from config if org mode
// is configured. Returns nil if not in org mode.
func buildAWSMultiAccount(cfg *config.DrainpipeConfig) *provider.AWSMultiAccount {
	effectiveOrg := cfg.EffectiveOrg()
	if effectiveOrg == nil && len(cfg.Connection.Accounts) == 0 {
		return nil
	}

	var orgSettings *provider.OrgSettings
	if effectiveOrg != nil {
		orgSettings = &provider.OrgSettings{
			RoleName:       effectiveOrg.RoleName,
			AssumeRoleName: effectiveOrg.AssumeRoleName,
			AdminAccountID: effectiveOrg.AdminAccountID,
			Organizations:  effectiveOrg.Organizations,
		}
	}

	return provider.NewAWSMultiAccount(cfg.Connection.Profile, cfg.Connection.Regions, orgSettings)
}

// buildAzureMultiSubscription constructs an AzureMultiSubscription from config
// if the org block contains tenant IDs (via the organizations field). Returns
// nil when multi-subscription mode is not configured.
//
// The org {} block is reused across providers: for AWS, organizations holds OU
// IDs; for Azure, it holds tenant IDs. The service principal credentials
// (client_id, client_secret) are read from the connection's extra attributes.
func buildAzureMultiSubscription(cfg *config.DrainpipeConfig) *provider.AzureMultiSubscription {
	effectiveOrg := cfg.EffectiveOrg()
	if effectiveOrg == nil || len(effectiveOrg.Organizations) == 0 {
		return nil
	}

	clientID, _ := cfg.Connection.Extra["client_id"].(string)
	clientSecret, _ := cfg.Connection.Extra["client_secret"].(string)
	if clientID == "" || clientSecret == "" {
		return nil
	}

	return provider.NewAzureMultiSubscription(clientID, clientSecret, effectiveOrg.Organizations)
}

// ── Work item processing ──────────────────────────────────────────────

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

// collectTableFiltered runs the filter_query SQL against Postgres to get
// qual values, then calls Export once per value and merges all rows into
// a single import stream.
func collectTableFiltered(ctx context.Context, item workItem, schemaMgr *schema.Manager) error {
	fq := item.filterQuery

	// Run the filter query against destination Postgres.
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

	// Merge rows from N Export calls into a single channel for the importer.
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

	imp := importer.New(item.pool, item.sourceAccount, item.logger.With().Str("component", "importer").Logger())
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

// mergeWhere creates a new where map with the filter_query column added
// to any existing static where filters.
func mergeWhere(base map[string]string, column, value string) map[string]string {
	merged := make(map[string]string, len(base)+1)
	for k, v := range base {
		merged[k] = v
	}
	merged[column] = value
	return merged
}

// ── supportedTables ───────────────────────────────────────────────────

// supportedTables returns a map of table name → natural key columns for all
// tables that have discoverable natural keys.
func supportedTables(exp *exporter.Exporter, preferredKey string) (map[string][]string, error) {
	allSchemas, err := exp.GetAllSchemas()
	if err != nil {
		return nil, err
	}

	result := make(map[string][]string)
	for name, tableSchema := range allSchemas {
		keys := provider.NaturalKeyColumns(name, tableSchema, preferredKey)
		if len(keys) > 0 {
			result[name] = keys
		}
	}
	return result, nil
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

// ── Work item type ────────────────────────────────────────────────────

type workItem struct {
	exp           *exporter.Exporter
	sourceAccount string
	tableName     string
	naturalKeys   []string
	where         map[string]string
	columns       []string // explicit column subset (nil = all)
	filterQuery   *config.FilterQuery
	deepHydration bool
	accountName   string
	pool          *pgxpool.Pool
	logger        zerolog.Logger
}

func runListTables(logger zerolog.Logger) {
	flags := parseFlags(os.Args[2:])
	providerName := flagOrDefault(flags, "provider", "aws")
	showUnsupported := flagHas(flags, "unsupported")

	// Resolve plugin spec
	var pluginSpec string
	if defaults, ok := config.KnownProviders[providerName]; ok {
		pluginSpec = defaults.Plugin
	} else {
		pluginSpec = providerName
	}

	pluginRef, err := pluginmanager.ParsePluginRef(pluginSpec)
	if err != nil {
		logger.Fatal().Err(err).Str("plugin", pluginSpec).Msg("invalid plugin specifier")
	}

	pluginMgr := pluginmanager.NewManager("", logger)
	binaryPath, err := pluginMgr.EnsurePlugin(pluginRef)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to resolve plugin binary")
	}

	exp, err := exporter.New(pluginRef.Name, pluginRef.PluginName(), binaryPath, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to launch plugin")
	}
	defer exp.Close()

	// Configure with empty/default connection
	cfg := &config.DrainpipeConfig{Provider: providerName}
	if err := exp.SetConnectionConfig(cfg.ResolveConnectionHCL()); err != nil {
		logger.Fatal().Err(err).Msg("failed to configure plugin")
	}

	preferredKey := cfg.ResolveNaturalKey()
	supported, err := supportedTables(exp, preferredKey)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to discover tables")
	}

	if showUnsupported {
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

func runListProviders() {
	fmt.Println("Known providers (shorthand → plugin):")
	fmt.Println()
	for name, defaults := range config.KnownProviders {
		fmt.Printf("  %-15s %s\n", name, defaults.Plugin)
	}
	fmt.Println()
	fmt.Println("Use any Steampipe plugin with 'plugin: org/name@version' in your config.")
}

func runDownloadPlugins(logger zerolog.Logger) {
	flags := parseFlags(os.Args[2:])
	configPathRaw := flagOrDefault(flags, "config", defaultConfigPath())

	var configPaths []string
	for _, p := range strings.Split(configPathRaw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			configPaths = append(configPaths, p)
		}
	}

	configs, _, err := config.LoadAllConfigs(configPaths)
	if err != nil {
		logger.Fatal().Err(err).Strs("configs", configPaths).Msg("failed to load config")
	}
	if len(configs) == 0 {
		logger.Fatal().Strs("configs", configPaths).Msg("no config blocks found")
	}

	pluginMgr := pluginmanager.NewManager("", logger.With().Str("component", "pluginmanager").Logger())

	seen := make(map[string]bool)
	for i, cfg := range configs {
		cfgLog := logger.With().Int("config_block", i+1).Logger()

		pluginSpec, ok := cfg.ResolvePluginSpec()
		if !ok {
			cfgLog.Warn().Str("provider", cfg.Provider).Msg("unknown provider, skipping")
			continue
		}

		if cfg.PluginPath != "" {
			cfgLog.Info().Str("path", cfg.PluginPath).Msg("plugin_path set, skipping download")
			continue
		}

		if seen[pluginSpec] {
			continue
		}
		seen[pluginSpec] = true

		ref, err := pluginmanager.ParsePluginRef(pluginSpec)
		if err != nil {
			cfgLog.Fatal().Err(err).Str("plugin", pluginSpec).Msg("invalid plugin specifier")
		}

		cfgLog.Info().
			Str("plugin", ref.Org+"/"+ref.Name).
			Str("version", ref.Version).
			Msg("ensuring plugin")

		binaryPath, err := pluginMgr.EnsurePlugin(ref)
		if err != nil {
			cfgLog.Fatal().Err(err).Msg("failed to download plugin")
		}

		cfgLog.Info().Str("path", binaryPath).Msg("plugin ready")
	}

	logger.Info().Int("plugins", len(seen)).Msg("all plugins downloaded")
}

func defaultConfigPath() string {
	if _, err := os.Stat("drainpipe.hcl"); err == nil {
		return "drainpipe.hcl"
	}
	return "drainpipe.yaml"
}

func knownProviderNames() []string {
	names := make([]string, 0, len(config.KnownProviders))
	for name := range config.KnownProviders {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
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

		switch key {
		case "p":
			key = "provider"
		case "t":
			key = "tables"
		case "c":
			key = "config"
		}

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

func flagHas(flags map[string]string, key string) bool {
	_, ok := flags[key]
	return ok
}

// ── Steampipe SDK log adapter ─────────────────────────────────────────

type zerologWriter struct {
	logger zerolog.Logger
}

func (w *zerologWriter) Write(p []byte) (n int, err error) {
	msg := strings.TrimSpace(string(p))
	if msg == "" {
		return len(p), nil
	}

	level, body := parseSteampipeLog(msg)

	switch level {
	case "TRACE":
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

func parseSteampipeLog(msg string) (string, string) {
	for _, lvl := range []string{"TRACE", "INFO", "WARN", "ERROR"} {
		tag := "[" + lvl + "]"
		if idx := strings.Index(msg, tag); idx >= 0 {
			body := strings.TrimSpace(msg[idx+len(tag):])
			return lvl, body
		}
	}
	return "", msg
}

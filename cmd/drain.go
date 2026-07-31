package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"

	"github.com/justmiles/drainpipe/cmd/internal/config"
	"github.com/justmiles/drainpipe/cmd/internal/pluginmanager"
	"github.com/justmiles/drainpipe/cmd/internal/provider"
	"github.com/justmiles/drainpipe/cmd/internal/schema"
)

// accountJob describes one account to collect from one plugin binary.
// Tables/supported/where/columns/filterQueries are resolved during Phase 1b.
type accountJob struct {
	pluginAlias       string
	pluginName        string
	binaryPath        string
	connConfig        string
	accountID         string
	accountName       string
	tableEntries      []config.TableEntry
	orgAccount        *provider.AccountInfo
	drainpipeCfg      *config.DrainpipeConfig
	preferredKey      string
	identityTable     string
	identityColumn    string
	sourceAccountQual string
	strict            bool
	deepHydration     bool
	retries           int
	retryDelay        time.Duration
	tableTimeout      time.Duration
	rateLimiters      []*proto.RateLimiterDefinition
	logger            zerolog.Logger

	// Resolved during schema validation (Phase 1b)
	tables        []string
	supported     map[string][]string
	where         map[string]map[string]string
	columns       map[string][]string
	filterQueries map[string]*config.FilterQuery
}

// accountSetup is a transient structure used while building accountJobs.
type accountSetup struct {
	connConfig   string
	accountID    string
	accountName  string
	tableEntries []config.TableEntry
	orgAccount   *provider.AccountInfo
}

func runDrain(logger zerolog.Logger) {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	flags := parseFlags(os.Args[2:])
	configPathRaw := flagOrDefault(flags, "config", defaultConfigPath())
	providerNameFlag := flagOrDefault(flags, "provider", "")
	tablePatternsFlag := flagOrDefault(flags, "tables", "")

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
		if tablePatternsFlag == "" {
			logger.Fatal().Msg("no tables specified; use --tables or create a config file with 'tables' entries")
		}
		synthetic := &config.DrainpipeConfig{Provider: provName}
		for _, p := range strings.Split(tablePatternsFlag, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				synthetic.Tables = append(synthetic.Tables, config.TableEntry{Name: p})
			}
		}
		configs = []*config.DrainpipeConfig{synthetic}
		logger.Info().Str("provider", provName).Msg("no config file; using CLI flags")
	} else {
		logger.Info().Int("config_blocks", len(configs)).Strs("files", configPaths).Msg("loaded drainpipe config(s)")
	}

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

	pluginMgr := pluginmanager.NewManager("", logger.With().Str("component", "pluginmanager").Logger())
	schemaMgr := schema.New(pool, logger.With().Str("component", "schema").Logger())

	allJobs, maxConcurrency, anyStrict := buildAllJobs(ctx, configs, rateLimiters, pluginMgr, logger)
	if len(allJobs) == 0 {
		logger.Warn().Msg("no accounts to process")
		return
	}

	allJobs, totalTables := validateSchemas(ctx, allJobs, pluginMgr, schemaMgr, logger, anyStrict)
	if len(allJobs) == 0 {
		logger.Warn().Msg("no accounts with matching tables to process")
		return
	}

	runWorkerPool(ctx, cancel, allJobs, totalTables, maxConcurrency, anyStrict, pool, schemaMgr, logger)
}

// buildAllJobs iterates over all DrainpipeConfigs, resolves plugin binaries,
// discovers accounts (multi-account/org modes), and returns all account jobs.
func buildAllJobs(
	ctx context.Context,
	configs []*config.DrainpipeConfig,
	rateLimiters map[string][]config.RateLimiterDef,
	pluginMgr *pluginmanager.Manager,
	logger zerolog.Logger,
) ([]accountJob, int, bool) {
	var allJobs []accountJob
	maxConcurrency := 1
	anyStrict := false

	for cfgIdx, drainpipeCfg := range configs {
		cfgLog := logger.With().Int("config_block", cfgIdx+1).Logger()

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

		preferredKey := drainpipeCfg.ResolveNaturalKey()
		identityTable, identityColumn := drainpipeCfg.ResolveIdentity()
		sourceAccountQual := drainpipeCfg.ResolveSourceAccountQual()

		concurrency := 1
		retries := 3
		retryDelay := 10 * time.Second
		tableTimeout := 10 * time.Minute
		strict := false
		deepHydration := true

		if drainpipeCfg.Concurrency > 0 {
			concurrency = drainpipeCfg.Concurrency
		}
		if drainpipeCfg.Retries != nil {
			retries = *drainpipeCfg.Retries
		}
		if drainpipeCfg.RetryDelay > 0 {
			retryDelay = drainpipeCfg.RetryDelay
		}
		if drainpipeCfg.TableTimeout > 0 {
			tableTimeout = drainpipeCfg.TableTimeout
		}
		strict = drainpipeCfg.Strict
		if drainpipeCfg.DeepHydration != nil {
			deepHydration = *drainpipeCfg.DeepHydration
		}

		if concurrency > maxConcurrency {
			maxConcurrency = concurrency
		}
		if strict {
			anyStrict = true
		}

		var cfgTableEntries []config.TableEntry
		if len(drainpipeCfg.Tables) > 0 {
			cfgTableEntries = drainpipeCfg.Tables
		}

		accountSetups := buildAccountSetups(ctx, drainpipeCfg, pluginRef, cfgTableEntries, cfgLog)

		// Resolve effective rate limiters.
		effectiveLimiters := rateLimiters[pluginRef.Name]
		if len(effectiveLimiters) == 0 {
			effectiveLimiters = drainpipeCfg.ResolveDefaultLimiters()
			if len(effectiveLimiters) > 0 {
				cfgLog.Info().
					Str("provider", drainpipeCfg.Provider).
					Int("limiters", len(effectiveLimiters)).
					Msg("applying provider default rate limiters (override via plugin {} block)")
			}
		}

		var limiterDefs []*proto.RateLimiterDefinition
		for _, rl := range effectiveLimiters {
			limiterDefs = append(limiterDefs, &proto.RateLimiterDefinition{
				Name:           rl.Name,
				BucketSize:     rl.BucketSize,
				FillRate:       float32(rl.FillRate),
				MaxConcurrency: rl.MaxConcurrency,
				Scope:          rl.Scope,
				Where:          rl.Where,
			})
		}

		for _, setup := range accountSetups {
			allJobs = append(allJobs, accountJob{
				pluginAlias:       pluginRef.Name,
				pluginName:        pluginRef.PluginName(),
				binaryPath:        binaryPath,
				connConfig:        setup.connConfig,
				accountID:         setup.accountID,
				accountName:       setup.accountName,
				tableEntries:      setup.tableEntries,
				orgAccount:        setup.orgAccount,
				drainpipeCfg:      drainpipeCfg,
				preferredKey:      preferredKey,
				identityTable:     identityTable,
				identityColumn:    identityColumn,
				sourceAccountQual: sourceAccountQual,
				strict:            strict,
				retries:           retries,
				retryDelay:        retryDelay,
				tableTimeout:      tableTimeout,
				deepHydration:     deepHydration,
				rateLimiters:      limiterDefs,
				logger: cfgLog.With().
					Str("account_id", setup.accountID).
					Str("account_name", setup.accountName).
					Logger(),
			})
		}
	}

	return allJobs, maxConcurrency, anyStrict
}

// buildAccountSetups resolves the list of accounts/subscriptions for a given
// DrainpipeConfig. It handles AWS explicit accounts, AWS org discovery, Azure
// multi-subscription, and falls back to a single-account setup.
func buildAccountSetups(
	ctx context.Context,
	drainpipeCfg *config.DrainpipeConfig,
	pluginRef pluginmanager.PluginRef,
	cfgTableEntries []config.TableEntry,
	cfgLog zerolog.Logger,
) []accountSetup {
	var setups []accountSetup

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

				setups = append(setups, accountSetup{
					connConfig:   strings.Join(configParts, "\n"),
					accountName:  entry.Name,
					tableEntries: acctEntries,
				})
			}
			cfgLog.Info().Int("accounts", len(setups)).Msg("using explicit accounts from config")

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
					setups = append(setups, accountSetup{
						accountID:    acct.AccountID,
						accountName:  acct.AccountName,
						tableEntries: acctEntries,
						orgAccount:   &acctCopy,
					})
				}
				cfgLog.Info().Int("accounts", len(setups)).Msg("multi-account mode: collecting from organization")
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
				setups = append(setups, accountSetup{
					connConfig:   sub.ConnectionConfig,
					accountID:    sub.SubscriptionID,
					accountName:  sub.DisplayName,
					tableEntries: acctEntries,
				})
			}
			cfgLog.Info().Int("subscriptions", len(setups)).Msg("multi-subscription mode: collecting from Azure tenants")
		}
	}

	if len(setups) == 0 {
		connConfig := drainpipeCfg.ResolveConnectionHCL()
		setups = append(setups, accountSetup{
			connConfig:   connConfig,
			accountID:    drainpipeCfg.Connection.AccountID,
			tableEntries: cfgTableEntries,
		})
	}

	return setups
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
// if the org block contains tenant IDs. Returns nil when not configured.
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


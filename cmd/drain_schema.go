package main

import (
	"context"
	"sort"

	"github.com/rs/zerolog"

	"github.com/justmiles/drainpipe/cmd/internal/config"
	"github.com/justmiles/drainpipe/cmd/internal/exporter"
	"github.com/justmiles/drainpipe/cmd/internal/match"
	"github.com/justmiles/drainpipe/cmd/internal/pluginmanager"
	"github.com/justmiles/drainpipe/cmd/internal/provider"
	"github.com/justmiles/drainpipe/cmd/internal/schema"
)

// validateSchemas launches one temporary plugin process per unique binary to
// discover supported tables, resolves each job's table list, ensures DB schemas
// for tables that will be collected, and returns the resolved jobs with total count.
func validateSchemas(
	ctx context.Context,
	allJobs []accountJob,
	pluginMgr *pluginmanager.Manager,
	schemaMgr *schema.Manager,
	logger zerolog.Logger,
	anyStrict bool,
) ([]accountJob, int) {
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

	pluginSupported := make(map[string]map[string][]string)
	pluginExporters := make(map[string]*exporter.Exporter)

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

		var preferredKey string
		var tableKeyOverrides map[string][]string
		for i := range allJobs {
			if allJobs[i].binaryPath == pi.binaryPath {
				preferredKey = allJobs[i].preferredKey
				tableKeyOverrides = allJobs[i].drainpipeCfg.ResolveProviderTableKeys()
				break
			}
		}

		supported, err := supportedTables(exp, preferredKey, tableKeyOverrides)
		if err != nil {
			exp.Close()
			schemaLog.Fatal().Err(err).Msg("failed to discover supported tables")
		}

		pluginSupported[pi.binaryPath] = supported
		pluginExporters[pi.binaryPath] = exp
		schemaLog.Info().Int("supported_tables", len(supported)).Msg("discovered plugin tables")
	}

	totalTables := 0
	var resolvedJobs []accountJob
	neededTables := make(map[string]map[string]bool)

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
			for _, te := range job.tableEntries {
				if len(te.Key) > 0 {
					if _, already := supported[te.Name]; !already {
						supportedNames = append(supportedNames, te.Name)
					}
				}
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
			job.logger.Fatal().
				Str("account_id", job.accountID).
				Str("account_name", job.accountName).
				Msg("no tables configured; specify table patterns via --tables flag or 'tables' in config")
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
				if len(te.Key) > 0 {
					job.supported[tableName] = te.Key
				}
			}
		}

		defaultFQs := job.drainpipeCfg.ResolveDefaultFilterQueries()
		for _, tableName := range job.tables {
			if _, hasExplicit := job.filterQueries[tableName]; !hasExplicit {
				if fq, hasDefault := defaultFQs[tableName]; hasDefault {
					job.filterQueries[tableName] = fq
				}
			}
		}

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

	if len(resolvedJobs) == 0 {
		for _, exp := range pluginExporters {
			exp.Close()
		}
		return nil, 0
	}

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

	return resolvedJobs, totalTables
}

// supportedTables returns a map of table name → natural key columns for all
// tables that have discoverable natural keys. tableKeyOverrides take precedence.
func supportedTables(exp *exporter.Exporter, preferredKey string, tableKeyOverrides map[string][]string) (map[string][]string, error) {
	allSchemas, err := exp.GetAllSchemas()
	if err != nil {
		return nil, err
	}

	result := make(map[string][]string)
	for name, tableSchema := range allSchemas {
		if keys, ok := tableKeyOverrides[name]; ok {
			result[name] = keys
			continue
		}
		keys := provider.NaturalKeyColumns(name, tableSchema, preferredKey)
		if len(keys) > 0 {
			result[name] = keys
		}
	}
	return result, nil
}


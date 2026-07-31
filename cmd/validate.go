package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/rs/zerolog"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"

	"github.com/justmiles/drainpipe/cmd/internal/config"
	"github.com/justmiles/drainpipe/cmd/internal/exporter"
	"github.com/justmiles/drainpipe/cmd/internal/match"
	"github.com/justmiles/drainpipe/cmd/internal/pluginmanager"
	"github.com/justmiles/drainpipe/cmd/internal/provider"
)

// validationIssue describes a single finding from config validation.
type validationIssue struct {
	// severity is "error" or "warning".
	severity string
	// block identifies the config block (e.g. "block 1 (turbot/aws@latest)").
	block string
	// table is the table name, if the issue is table-specific.
	table string
	// msg is the human-readable description of the issue.
	msg string
}

// runValidate loads configs, downloads plugins (without real credentials), and
// checks that every table pattern resolves to at least one supported table with
// valid key columns.
func runValidate(logger zerolog.Logger) {
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

	var allIssues []validationIssue
	totalTables := 0

	for cfgIdx, drainpipeCfg := range configs {
		cfgLog := logger.With().Int("config_block", cfgIdx+1).Logger()
		issues, n, ok := validateConfigBlock(cfgIdx, drainpipeCfg, pluginMgr, cfgLog, &allIssues)
		if !ok {
			continue
		}
		allIssues = append(allIssues, issues...)
		totalTables += n
	}

	printValidationResults(allIssues, len(configs), totalTables, logger)
}

// validateConfigBlock validates a single config block: resolves the plugin,
// downloads the binary, discovers schema, and checks all configured tables.
// Returns (issues, tableCount, ok). ok=false means a fatal error was added to allIssues.
func validateConfigBlock(
	cfgIdx int,
	drainpipeCfg *config.DrainpipeConfig,
	pluginMgr *pluginmanager.Manager,
	cfgLog zerolog.Logger,
	allIssues *[]validationIssue,
) ([]validationIssue, int, bool) {
	pluginSpec, ok := drainpipeCfg.ResolvePluginSpec()
	if !ok {
		provName := drainpipeCfg.Provider
		if provName == "" {
			provName = "(unset)"
		}
		*allIssues = append(*allIssues, validationIssue{
			severity: "error",
			block:    fmt.Sprintf("block %d", cfgIdx+1),
			msg:      fmt.Sprintf("unknown provider %q; specify 'plugin' field or use a known provider name", provName),
		})
		return nil, 0, false
	}

	pluginRef, err := pluginmanager.ParsePluginRef(pluginSpec)
	if err != nil {
		*allIssues = append(*allIssues, validationIssue{
			severity: "error",
			block:    fmt.Sprintf("block %d", cfgIdx+1),
			msg:      fmt.Sprintf("invalid plugin specifier %q: %v", pluginSpec, err),
		})
		return nil, 0, false
	}

	blockName := fmt.Sprintf("block %d (%s/%s@%s)", cfgIdx+1, pluginRef.Org, pluginRef.Name, pluginRef.Version)
	cfgLog = cfgLog.With().Str("plugin", pluginRef.Org+"/"+pluginRef.Name).Str("version", pluginRef.Version).Logger()

	var binaryPath string
	if drainpipeCfg.PluginPath != "" {
		binaryPath, err = pluginMgr.EnsurePluginFromPath(drainpipeCfg.PluginPath)
	} else {
		binaryPath, err = pluginMgr.EnsurePlugin(pluginRef)
	}
	if err != nil {
		*allIssues = append(*allIssues, validationIssue{
			severity: "error", block: blockName,
			msg: fmt.Sprintf("failed to resolve plugin binary: %v", err),
		})
		return nil, 0, false
	}
	cfgLog.Info().Str("binary", binaryPath).Msg("plugin binary resolved")

	exp, err := exporter.New(pluginRef.Name, pluginRef.PluginName(), binaryPath, cfgLog)
	if err != nil {
		*allIssues = append(*allIssues, validationIssue{
			severity: "error", block: blockName,
			msg: fmt.Sprintf("failed to launch plugin: %v", err),
		})
		return nil, 0, false
	}
	if err := exp.SetConnectionConfig(""); err != nil {
		exp.Close()
		*allIssues = append(*allIssues, validationIssue{
			severity: "error", block: blockName,
			msg: fmt.Sprintf("failed to configure plugin for schema discovery: %v", err),
		})
		return nil, 0, false
	}

	allSchemas, err := exp.GetAllSchemas()
	exp.Close()
	if err != nil {
		*allIssues = append(*allIssues, validationIssue{
			severity: "error", block: blockName,
			msg: fmt.Sprintf("failed to get plugin schema: %v", err),
		})
		return nil, 0, false
	}

	preferredKey := drainpipeCfg.ResolveNaturalKey()
	providerTableKeys := drainpipeCfg.ResolveProviderTableKeys()

	supported := make(map[string][]string)
	for name, tableSchema := range allSchemas {
		if keys, ok2 := providerTableKeys[name]; ok2 {
			supported[name] = keys
			continue
		}
		keys := provider.NaturalKeyColumns(name, tableSchema, preferredKey)
		if len(keys) > 0 {
			supported[name] = keys
		}
	}

	cfgLog.Info().Int("total_tables", len(allSchemas)).Int("supported_tables", len(supported)).Msg("plugin schema discovered")

	if len(drainpipeCfg.Tables) == 0 {
		return []validationIssue{{severity: "error", block: blockName, msg: "no tables configured"}}, 0, true
	}

	issues, n := validateConfigTables(drainpipeCfg, allSchemas, supported, preferredKey, blockName)
	return issues, n, true
}

// validateConfigTables checks all configured table patterns and per-table options.
func validateConfigTables(
	drainpipeCfg *config.DrainpipeConfig,
	allSchemas map[string]*proto.TableSchema,
	supported map[string][]string,
	preferredKey, blockName string,
) ([]validationIssue, int) {
	var issues []validationIssue

	entryMap := config.TableEntryMap(drainpipeCfg.Tables)

	allTableNames := make([]string, 0, len(allSchemas))
	for name := range allSchemas {
		allTableNames = append(allTableNames, name)
	}
	for _, te := range drainpipeCfg.Tables {
		if len(te.Key) > 0 {
			if _, exists := allSchemas[te.Name]; !exists {
				allTableNames = append(allTableNames, te.Name)
			}
		}
	}
	sort.Strings(allTableNames)

	patterns := config.TableNames(drainpipeCfg.Tables)
	matched := match.Tables(allTableNames, patterns)

	for _, pat := range patterns {
		if strings.ContainsAny(pat, "*?") {
			if len(match.Tables(allTableNames, []string{pat})) == 0 {
				suggestions := match.Suggest(allTableNames, []string{pat}, 3)
				msg := fmt.Sprintf("pattern %q matched no tables in plugin schema", pat)
				if len(suggestions) > 0 {
					msg += fmt.Sprintf(" (did you mean: %s?)", strings.Join(suggestions, ", "))
				}
				issues = append(issues, validationIssue{severity: "warning", block: blockName, msg: msg})
			}
		} else {
			if _, exists := allSchemas[pat]; !exists {
				suggestions := match.Suggest(allTableNames, []string{pat}, 3)
				msg := fmt.Sprintf("table %q not found in plugin schema", pat)
				if len(suggestions) > 0 {
					msg += fmt.Sprintf(" (did you mean: %s?)", strings.Join(suggestions, ", "))
				}
				issues = append(issues, validationIssue{severity: "error", block: blockName, table: pat, msg: msg})
			}
		}
	}

	totalTables := 0
	for _, tableName := range matched {
		totalTables++
		te, hasEntry := entryMap[tableName]
		tableSchema := allSchemas[tableName]

		var effectiveKey []string
		if hasEntry && len(te.Key) > 0 {
			effectiveKey = te.Key
		} else if keys, ok := supported[tableName]; ok {
			effectiveKey = keys
		}
		if len(effectiveKey) == 0 {
			issues = append(issues, validationIssue{
				severity: "error", block: blockName, table: tableName,
				msg: fmt.Sprintf("table %q has no discoverable natural key; add a 'key' block to specify one", tableName),
			})
		}

		if tableSchema == nil || !hasEntry {
			continue
		}

		issues = append(issues, validateTableColumns(tableName, te, tableSchema, preferredKey, blockName)...)
	}

	return issues, totalTables
}

// validateTableColumns checks key, where, columns, and filter_query for a single table.
func validateTableColumns(
	tableName string,
	te config.TableEntry,
	tableSchema *proto.TableSchema,
	preferredKey, blockName string,
) []validationIssue {
	var issues []validationIssue

	schemaColSet := make(map[string]bool, len(tableSchema.Columns))
	for _, col := range tableSchema.Columns {
		schemaColSet[col.Name] = true
	}
	keyColSet := make(map[string]bool)
	for _, kc := range tableSchema.GetCallKeyColumnList {
		keyColSet[kc.Name] = true
	}
	if preferredKey != "" && schemaColSet[preferredKey] {
		keyColSet[preferredKey] = true
	}

	for _, k := range te.Key {
		if !schemaColSet[k] {
			issues = append(issues, validationIssue{
				severity: "error", block: blockName, table: tableName,
				msg: fmt.Sprintf("table %q: key column %q does not exist in plugin schema", tableName, k),
			})
		}
	}

	for col := range te.Where {
		if !keyColSet[col] && !schemaColSet[col] {
			issues = append(issues, validationIssue{
				severity: "warning", block: blockName, table: tableName,
				msg: fmt.Sprintf("table %q: where column %q does not exist in plugin schema", tableName, col),
			})
		} else if !keyColSet[col] {
			issues = append(issues, validationIssue{
				severity: "warning", block: blockName, table: tableName,
				msg: fmt.Sprintf("table %q: where column %q exists but is not a key column; filtering may not work as expected", tableName, col),
			})
		}
	}

	for _, col := range te.Columns {
		if !schemaColSet[col] {
			issues = append(issues, validationIssue{
				severity: "warning", block: blockName, table: tableName,
				msg: fmt.Sprintf("table %q: column %q in 'columns' list does not exist in plugin schema", tableName, col),
			})
		}
	}

	if te.FilterQuery != nil {
		col := te.FilterQuery.Column
		if !schemaColSet[col] {
			issues = append(issues, validationIssue{
				severity: "error", block: blockName, table: tableName,
				msg: fmt.Sprintf("table %q: filter_query column %q does not exist in plugin schema", tableName, col),
			})
		} else if !keyColSet[col] {
			issues = append(issues, validationIssue{
				severity: "warning", block: blockName, table: tableName,
				msg: fmt.Sprintf("table %q: filter_query column %q exists but is not a key column; filtering may be inefficient", tableName, col),
			})
		}
	}

	return issues
}

// printValidationResults writes all validation issues to stderr and logs a
// summary. Exits with code 1 if any errors are present.
func printValidationResults(allIssues []validationIssue, numConfigs, totalTables int, logger zerolog.Logger) {
	errCount, warnCount := 0, 0
	for _, issue := range allIssues {
		switch issue.severity {
		case "error":
			errCount++
			fmt.Fprintf(os.Stderr, "  ✗ [%s] %s\n", issue.block, issue.msg)
		case "warning":
			warnCount++
			fmt.Fprintf(os.Stderr, "  ⚠ [%s] %s\n", issue.block, issue.msg)
		}
	}
	if len(allIssues) > 0 {
		fmt.Fprintln(os.Stderr)
	}
	summary := fmt.Sprintf("validation complete: %d config blocks, %d tables checked, %d errors, %d warnings",
		numConfigs, totalTables, errCount, warnCount)
	if errCount > 0 {
		logger.Error().Msg(summary)
		os.Exit(1)
	}
	logger.Info().Msg(summary)
}


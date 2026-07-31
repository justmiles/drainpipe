package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/rs/zerolog"

	"github.com/justmiles/drainpipe/cmd/internal/config"
	"github.com/justmiles/drainpipe/cmd/internal/exporter"
	"github.com/justmiles/drainpipe/cmd/internal/pluginmanager"
)

// runListTables discovers and prints supported tables for a given provider,
// showing the natural key columns for each table.
func runListTables(logger zerolog.Logger) {
	flags := parseFlags(os.Args[2:])
	providerName := flagOrDefault(flags, "provider", "aws")
	showUnsupported := flagHas(flags, "unsupported")

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

	cfg := &config.DrainpipeConfig{Provider: providerName}
	if err := exp.SetConnectionConfig(cfg.ResolveConnectionHCL()); err != nil {
		logger.Fatal().Err(err).Msg("failed to configure plugin")
	}

	preferredKey := cfg.ResolveNaturalKey()
	supported, err := supportedTables(exp, preferredKey, cfg.ResolveProviderTableKeys())
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

// runListProviders prints all known provider shorthand names and their
// associated plugin specifiers.
func runListProviders() {
	fmt.Println("Known providers (shorthand → plugin):")
	fmt.Println()
	for name, defaults := range config.KnownProviders {
		fmt.Printf("  %-15s %s\n", name, defaults.Plugin)
	}
	fmt.Println()
	fmt.Println("Use any Steampipe plugin with 'plugin: org/name@version' in your config.")
}

// runDownloadPlugins loads all configs and pre-downloads any plugin binaries
// not already present in the cache or local Steampipe install directory.
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


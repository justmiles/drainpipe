package main

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/rs/zerolog"

	"github.com/justmiles/drainpipe/cmd/internal/config"
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
	case "validate":
		runValidate(logger)
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
  validate           Validate config: download plugins, check table names and key columns
  list-tables        List available tables for a provider
  list-providers     List known providers and their default plugins
  download-plugins   Download plugin binaries for a config file

Drain options:
  --config, -c     Config file path(s), .hcl or .yaml (default: drainpipe.hcl or drainpipe.yaml)
  --provider, -p   Provider name (default: aws)
  --tables, -t     Comma-separated table patterns (required if not in config)
                   Examples: "aws_ec2_*", "aws_s3_bucket", "aws_*"
                   Only matches tables with discoverable natural keys.

Validate options:
  --config, -c     Config file path(s), .hcl or .yaml (default: drainpipe.hcl or drainpipe.yaml)

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

package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/gohcl"
	"github.com/hashicorp/hcl/v2/hclparse"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"
)

// ── Top-level HCL file structure ────────────────────────────────────

// HCLFile represents the complete parsed HCL configuration file.
type HCLFile struct {
	Connections []HCLConnection `hcl:"connection,block"`
	Plugins     []HCLPlugin     `hcl:"plugin,block"`
	Drainpipes  []HCLDrainpipe  `hcl:"drainpipe,block"`
}

// ── connection block ────────────────────────────────────────────────

// HCLConnection maps to a Steampipe `connection` block. Known fields
// are decoded explicitly; everything else is captured as HCL body
// remainder and forwarded to the plugin as-is.
type HCLConnection struct {
	Name   string   `hcl:"name,label"`
	Plugin string   `hcl:"plugin"`
	Remain hcl.Body `hcl:",remain"`
}

// ── plugin block with limiter sub-blocks ────────────────────────────

type HCLPlugin struct {
	Name        string       `hcl:"name,label"`
	Source      string       `hcl:"source,optional"`
	MemoryMaxMB int          `hcl:"memory_max_mb,optional"`
	Limiters    []HCLLimiter `hcl:"limiter,block"`
}

type HCLLimiter struct {
	Name           string   `hcl:"name,label"`
	BucketSize     int64    `hcl:"bucket_size,optional"`
	FillRate       float64  `hcl:"fill_rate,optional"`
	MaxConcurrency int64    `hcl:"max_concurrency,optional"`
	Scope          []string `hcl:"scope,optional"`
	Where          string   `hcl:"where,optional"`
}

// ── drainpipe block ─────────────────────────────────────────────────

type HCLDrainpipe struct {
	Name       string `hcl:"name,label"`
	Connection string `hcl:"connection,optional"`
	Provider   string `hcl:"provider,optional"`
	PluginPath string `hcl:"plugin_path,optional"`

	IdentityTable  string `hcl:"identity_table,optional"`
	IdentityColumn string `hcl:"identity_column,optional"`
	NaturalKey     string `hcl:"natural_key,optional"`

	AccountID string `hcl:"account_id,optional"`

	Concurrency   int    `hcl:"concurrency,optional"`
	Retries       int    `hcl:"retries,optional"`
	RetryDelay    string `hcl:"retry_delay,optional"`
	TableTimeout  string `hcl:"table_timeout,optional"`
	Strict        bool   `hcl:"strict,optional"`
	DeepHydration *bool  `hcl:"deep_hydration,optional"`

	Tables       []string         `hcl:"tables,optional"`
	TableBlocks  []HCLTableBlock  `hcl:"table,block"`
	AccountBlock []HCLAccount     `hcl:"accounts,block"`
	Org          *HCLOrg          `hcl:"org,block"`
}

type HCLTableBlock struct {
	Name        string          `hcl:"name,label"`
	Where       map[string]string `hcl:"where,optional"`
	Columns     []string          `hcl:"columns,optional"`
	FilterQuery *HCLFilterQuery   `hcl:"filter_query,block"`
}

type HCLFilterQuery struct {
	Column string `hcl:"column"`
	Query  string `hcl:"query"`
}

type HCLAccount struct {
	Name    string   `hcl:"name"`
	Profile string   `hcl:"profile,optional"`
	Regions []string `hcl:"regions,optional"`
}

// HCLOrg configures multi-account/multi-subscription discovery. The org {}
// block is provider-agnostic: for AWS, organizations holds OU IDs and
// role_name/assume_role_name control STS AssumeRole; for Azure, organizations
// holds tenant IDs (role fields are unused since the same service principal
// credentials work across all subscriptions).
//
// Overrides use match_account_ids and match_account_names to target specific
// accounts (AWS) or subscriptions (Azure) for table customization or skipping.
type HCLOrg struct {
	RoleName       string        `hcl:"role_name,optional"`       // AWS only: IAM role name
	AssumeRoleName string        `hcl:"assume_role_name,optional"` // AWS only: alias for role_name
	AdminAccountID string        `hcl:"admin_account_id,optional"` // AWS only: management account to skip
	Organizations  []string      `hcl:"organizations,optional"`   // AWS: OU IDs; Azure: tenant IDs
	Overrides      []HCLOverride `hcl:"override,block"`
}

type HCLOverride struct {
	MatchAccountNames []string     `hcl:"match_account_names,optional"`
	MatchAccountIDs   []string     `hcl:"match_account_ids,optional"`
	Tables            []string     `hcl:"tables,optional"`
	Skip              bool         `hcl:"skip,optional"`
}

// ── Rate limiter definitions (passed to plugins via gRPC) ───────────

type RateLimiterDef struct {
	Name           string
	BucketSize     int64
	FillRate       float64
	MaxConcurrency int64
	Scope          []string
	Where          string
}

// HCLResult is the output of loading an HCL config file. It contains
// the DrainpipeConfigs (compatible with the existing pipeline) plus
// any rate limiter definitions that need to be sent to plugins.
type HCLResult struct {
	Configs      []*DrainpipeConfig
	RateLimiters map[string][]RateLimiterDef // plugin short name → limiters
}

// ── Loader ──────────────────────────────────────────────────────────

// envEvalContext builds an hcl.EvalContext with all current environment
// variables available as top-level HCL variables. This allows HCL template
// interpolation like ${MY_VAR} to resolve to the corresponding env value.
func envEvalContext() *hcl.EvalContext {
	vars := make(map[string]cty.Value)
	for _, pair := range os.Environ() {
		k, v, ok := strings.Cut(pair, "=")
		if ok {
			vars[k] = cty.StringVal(v)
		}
	}
	return &hcl.EvalContext{Variables: vars}
}

func LoadHCLConfig(filePath string) (*HCLResult, error) {
	src, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading HCL config: %w", err)
	}

	parser := hclparse.NewParser()
	file, diags := parser.ParseHCL(src, filePath)
	if diags.HasErrors() {
		return nil, fmt.Errorf("parsing HCL %s: %s", filePath, diags.Error())
	}

	ctx := envEvalContext()

	var hclFile HCLFile
	diags = gohcl.DecodeBody(file.Body, ctx, &hclFile)
	if diags.HasErrors() {
		return nil, fmt.Errorf("decoding HCL %s: %s", filePath, diags.Error())
	}

	return convertHCL(&hclFile, file, ctx)
}

func convertHCL(hclFile *HCLFile, file *hcl.File, ctx *hcl.EvalContext) (*HCLResult, error) {
	connMap := make(map[string]*HCLConnection, len(hclFile.Connections))
	for i := range hclFile.Connections {
		connMap[hclFile.Connections[i].Name] = &hclFile.Connections[i]
	}

	result := &HCLResult{
		RateLimiters: make(map[string][]RateLimiterDef),
	}

	for _, p := range hclFile.Plugins {
		for _, lim := range p.Limiters {
			result.RateLimiters[p.Name] = append(result.RateLimiters[p.Name], RateLimiterDef{
				Name:           lim.Name,
				BucketSize:     lim.BucketSize,
				FillRate:       lim.FillRate,
				MaxConcurrency: lim.MaxConcurrency,
				Scope:          lim.Scope,
				Where:          lim.Where,
			})
		}
	}

	for _, dp := range hclFile.Drainpipes {
		cfg := &DrainpipeConfig{}

		if dp.Connection != "" {
			conn, ok := connMap[dp.Connection]
			if !ok {
				return nil, fmt.Errorf("drainpipe %q references undefined connection %q", dp.Name, dp.Connection)
			}
			cfg.Plugin = conn.Plugin
			cfg.Provider = inferProvider(conn.Plugin)

			connExtra, err := decodeConnectionRemain(conn.Remain, file, ctx)
			if err != nil {
				return nil, fmt.Errorf("connection %q: %w", conn.Name, err)
			}
			cfg.Connection.Extra = connExtra

			if profile, ok := connExtra["profile"]; ok {
				cfg.Connection.Profile = fmt.Sprint(profile)
				delete(cfg.Connection.Extra, "profile")
			}
			if regions, ok := connExtra["regions"]; ok {
				if rList, ok := regions.([]string); ok {
					cfg.Connection.Regions = rList
					delete(cfg.Connection.Extra, "regions")
				}
			}
		} else if dp.Provider != "" {
			cfg.Provider = dp.Provider
		}

		cfg.PluginPath = dp.PluginPath
		cfg.IdentityTable = dp.IdentityTable
		cfg.IdentityColumn = dp.IdentityColumn
		cfg.NaturalKey = dp.NaturalKey
		cfg.Connection.AccountID = dp.AccountID
		cfg.Concurrency = dp.Concurrency
		cfg.Retries = dp.Retries
		cfg.Strict = dp.Strict
		cfg.DeepHydration = dp.DeepHydration

		if dp.RetryDelay != "" {
			d, err := time.ParseDuration(dp.RetryDelay)
			if err != nil {
				return nil, fmt.Errorf("drainpipe %q: invalid retry_delay %q: %w", dp.Name, dp.RetryDelay, err)
			}
			cfg.RetryDelay = d
		}
		if dp.TableTimeout != "" {
			d, err := time.ParseDuration(dp.TableTimeout)
			if err != nil {
				return nil, fmt.Errorf("drainpipe %q: invalid table_timeout %q: %w", dp.Name, dp.TableTimeout, err)
			}
			cfg.TableTimeout = d
		}

		for _, t := range dp.Tables {
			cfg.Tables = append(cfg.Tables, TableEntry{Name: t})
		}
		for _, tb := range dp.TableBlocks {
			entry := TableEntry{Name: tb.Name, Where: tb.Where, Columns: tb.Columns}
			if tb.FilterQuery != nil {
				entry.FilterQuery = &FilterQuery{
					Column: tb.FilterQuery.Column,
					Query:  tb.FilterQuery.Query,
				}
			}
			cfg.Tables = append(cfg.Tables, entry)
		}

		for _, a := range dp.AccountBlock {
			cfg.Connection.Accounts = append(cfg.Connection.Accounts, AccountEntry{
				Name:    a.Name,
				Profile: a.Profile,
				Regions: a.Regions,
			})
		}

		if dp.Org != nil {
			org := &OrgConfig{
				RoleName:       dp.Org.RoleName,
				AssumeRoleName: dp.Org.AssumeRoleName,
				AdminAccountID: dp.Org.AdminAccountID,
				Organizations:  dp.Org.Organizations,
			}
			for _, ov := range dp.Org.Overrides {
				override := OrgOverride{
					Match: OverrideMatch{
						AccountNames: ov.MatchAccountNames,
						AccountIDs:   ov.MatchAccountIDs,
					},
					Skip: ov.Skip,
				}
				for _, t := range ov.Tables {
					override.Tables = append(override.Tables, TableEntry{Name: t})
				}
				org.Overrides = append(org.Overrides, override)
			}
			cfg.Connection.Org = org
		}

		result.Configs = append(result.Configs, cfg)
	}

	return result, nil
}

// inferProvider reverse-looks up KnownProviders to find the short provider
// name (e.g. "aws") from a plugin spec (e.g. "turbot/aws@latest"). This
// enables HCL configs that use connection blocks to inherit provider defaults
// for identity resolution, natural keys, etc.
func inferProvider(pluginSpec string) string {
	for name, defaults := range KnownProviders {
		if defaults.Plugin == pluginSpec {
			return name
		}
	}
	return ""
}

// connectionTypedFields are attributes consumed by HCLConnection struct
// tags, so they must be excluded from the pass-through Extra map.
var connectionTypedFields = map[string]bool{
	"plugin": true,
}

// decodeConnectionRemain extracts arbitrary key=value attributes from
// the connection block's HCL body remainder (everything except typed fields).
// The hclsyntax Remain body shares the parent's attribute map, so we
// must explicitly skip fields already decoded into HCLConnection.
func decodeConnectionRemain(body hcl.Body, file *hcl.File, ctx *hcl.EvalContext) (map[string]interface{}, error) {
	if body == nil {
		return nil, nil
	}

	syntaxBody, ok := body.(*hclsyntax.Body)
	if !ok {
		return nil, nil
	}

	result := make(map[string]interface{})
	for name, attr := range syntaxBody.Attributes {
		if connectionTypedFields[name] {
			continue
		}

		val, diags := attr.Expr.Value(ctx)
		if diags.HasErrors() {
			return nil, fmt.Errorf("evaluating %q: %s", name, diags.Error())
		}

		result[name] = ctyToGo(val)
	}

	return result, nil
}

// ctyToGo converts a cty.Value to a native Go type for HCL value serialization.
func ctyToGo(val cty.Value) interface{} {
	ty := val.Type()

	switch {
	case ty == cty.String:
		return val.AsString()
	case ty == cty.Number:
		bf := val.AsBigFloat()
		if bf.IsInt() {
			i, _ := bf.Int64()
			return i
		}
		f, _ := bf.Float64()
		return f
	case ty == cty.Bool:
		return val.True()
	case ty.IsListType() || ty.IsTupleType() || ty.IsSetType():
		var items []string
		for it := val.ElementIterator(); it.Next(); {
			_, v := it.Element()
			if v.Type() == cty.String {
				items = append(items, v.AsString())
			}
		}
		return items
	default:
		return val.GoString()
	}
}

// ── Format-aware config loading ─────────────────────────────────────

func isHCLFile(path string) bool {
	return strings.HasSuffix(path, ".hcl")
}

// LoadAllConfigs loads config files, dispatching to HCL or YAML based
// on file extension. Returns configs and any rate limiter definitions
// from HCL plugin blocks.
func LoadAllConfigs(filePaths []string) ([]*DrainpipeConfig, map[string][]RateLimiterDef, error) {
	var allConfigs []*DrainpipeConfig
	allLimiters := make(map[string][]RateLimiterDef)

	for _, fp := range filePaths {
		fp = strings.TrimSpace(fp)
		if fp == "" {
			continue
		}

		if isHCLFile(fp) {
			result, err := LoadHCLConfig(fp)
			if err != nil {
				return nil, nil, fmt.Errorf("loading %s: %w", fp, err)
			}
			if result != nil {
				allConfigs = append(allConfigs, result.Configs...)
				for k, v := range result.RateLimiters {
					allLimiters[k] = append(allLimiters[k], v...)
				}
			}
		} else {
			configs, err := LoadDrainpipeConfig(fp)
			if err != nil {
				return nil, nil, fmt.Errorf("loading %s: %w", fp, err)
			}
			allConfigs = append(allConfigs, configs...)
		}
	}

	if len(allConfigs) == 0 {
		return nil, nil, nil
	}
	return allConfigs, allLimiters, nil
}

package provider

import (
	"context"
	"fmt"
	"os"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/organizations"
	orgtypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	ststypes "github.com/aws/aws-sdk-go-v2/service/sts/types"
	"github.com/turbot/steampipe-plugin-aws/aws"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

func init() {
	Register(&AWSProvider{})
}

// AWSProvider implements the Provider interface for AWS.
// Fields can be set from a config file; unset fields fall back to env vars.
type AWSProvider struct {
	Profile           string   // AWS named profile (fallback: AWS_PROFILE)
	OrgRoleName       string   // IAM role name for org mode (fallback: AWS_ORG_ROLE_NAME)
	AssumeRoleName    string   // IAM role name to assume in each account (alias for OrgRoleName)
	OrgAdminAccountID string   // Admin account to skip (fallback: AWS_ORG_ADMIN_ACCOUNT_ID)
	Regions           []string // Regions to collect (fallback: AWS_REGIONS)
	Organizations     []string // OU IDs to discover accounts from (e.g., ou-xxxx-xxxxxxxx)

	// Lazily initialized AWS clients (cached for reuse)
	orgClient *organizations.Client
	stsClient *sts.Client
}

func (p *AWSProvider) Name() string { return "aws" }

func (p *AWSProvider) PluginFunc() plugin.PluginFunc { return aws.Plugin }

func (p *AWSProvider) DefaultConnectionConfig() string {
	var parts []string

	if profile := p.resolveProfile(); profile != "" {
		parts = append(parts, fmt.Sprintf(`  profile = %q`, profile))
	}
	parts = append(parts, regionsHCL(p.resolveRegions())...)

	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "\n")
}

// ResolveAccount queries aws_sts_caller_identity via the plugin to get the
// AWS account ID for the current credentials.
func (p *AWSProvider) ResolveAccount(ctx context.Context, queryFunc QueryFunc) (string, error) {
	row, err := queryFunc(ctx, "aws_sts_caller_identity")
	if err != nil {
		return "", fmt.Errorf("querying STS caller identity: %w", err)
	}
	if row == nil {
		return "", fmt.Errorf("aws_sts_caller_identity returned no data")
	}

	accountID, ok := row["account_id"]
	if !ok || accountID == nil {
		return "", fmt.Errorf("account_id not found in STS caller identity")
	}

	return fmt.Sprintf("%v", accountID), nil
}

// NaturalKeyColumns returns the natural key for an AWS table.
// Prefers "arn" when available — ARNs are globally unique (account+region+resource),
// while GetCallKeyColumnList keys (e.g., "name") may only be unique within a region,
// causing duplicate-row errors during multi-region upserts.
// Falls back to GetCallKeyColumnList keys for tables without an arn column.
func (p *AWSProvider) NaturalKeyColumns(tableName string, schema *proto.TableSchema) []string {
	// Prefer arn — globally unique across accounts and regions
	if schema != nil {
		for _, col := range schema.Columns {
			if col.Name == "arn" {
				return []string{"arn"}
			}
		}
	}
	// Fall back to GetCallKeyColumnList for tables without arn
	return DefaultNaturalKeyColumns(schema)
}

// ---------------------------------------------------------------------------
// MultiAccountProvider — AWS Organizations support
// ---------------------------------------------------------------------------

// DiscoverAccounts lists active accounts in an AWS Organization.
// When Organizations (OU IDs) are configured, it discovers accounts per-OU
// using ListAccountsForParent. Otherwise, it falls back to listing all accounts.
// Returns nil (single-account fallback) when org mode is not configured.
// Does NOT assume any roles — credentials are obtained lazily via AssumeAccountRole.
func (p *AWSProvider) DiscoverAccounts(ctx context.Context) ([]AccountInfo, error) {
	roleName := p.resolveAssumeRoleName()
	if roleName == "" {
		return nil, nil // single-account mode
	}

	if err := p.ensureClients(ctx); err != nil {
		return nil, err
	}

	skipAccountID := p.OrgAdminAccountID
	if skipAccountID == "" {
		skipAccountID = os.Getenv("AWS_ORG_ADMIN_ACCOUNT_ID")
	}

	var activeAccounts []orgtypes.Account
	var err error

	if len(p.Organizations) > 0 {
		// Discover accounts per-OU
		for _, ouID := range p.Organizations {
			ouAccounts, ouErr := listActiveAccountsForParent(ctx, p.orgClient, ouID)
			if ouErr != nil {
				return nil, fmt.Errorf("listing accounts for OU %s: %w", ouID, ouErr)
			}
			activeAccounts = append(activeAccounts, ouAccounts...)
		}
	} else {
		// Fall back to listing all accounts in the organization
		activeAccounts, err = listActiveAccounts(ctx, p.orgClient)
		if err != nil {
			return nil, fmt.Errorf("listing organization accounts: %w", err)
		}
	}

	// Deduplicate accounts (an account could appear in multiple OUs)
	seen := make(map[string]bool)
	var accounts []AccountInfo
	for _, acct := range activeAccounts {
		acctID := stringVal(acct.Id)
		if acctID == skipAccountID || seen[acctID] {
			continue
		}
		seen[acctID] = true
		accounts = append(accounts, AccountInfo{
			AccountID:   acctID,
			AccountName: stringVal(acct.Name),
		})
	}

	if len(accounts) == 0 {
		return nil, fmt.Errorf("no accounts available after org discovery (found %d active, skipped admin %q)", len(activeAccounts), skipAccountID)
	}

	return accounts, nil
}

// AssumeAccountRole obtains temporary credentials for a specific member account
// by assuming the configured IAM role via STS. Called just-in-time by workers
// so credentials are always fresh (1-hour TTL).
// Leverages sts:TagSession to pass session tags for downstream policy evaluation.
func (p *AWSProvider) AssumeAccountRole(ctx context.Context, account AccountInfo) (*AccountConfig, error) {
	roleName := p.resolveAssumeRoleName()

	if err := p.ensureClients(ctx); err != nil {
		return nil, err
	}

	roleARN := fmt.Sprintf("arn:aws:iam::%s:role/%s", account.AccountID, roleName)

	creds, err := p.stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         &roleARN,
		RoleSessionName: strPtr("drainpipe-" + account.AccountID),
		Tags: []ststypes.Tag{
			{
				Key:   strPtr("DrainpipeAccountId"),
				Value: strPtr(account.AccountID),
			},
			{
				Key:   strPtr("DrainpipeAccountName"),
				Value: strPtr(account.AccountName),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("assuming role %s: %w", roleARN, err)
	}

	// Build HCL connection config with temporary credentials
	regions := regionsHCL(p.resolveRegions())
	var configParts []string
	configParts = append(configParts, fmt.Sprintf("  access_key = %q", *creds.Credentials.AccessKeyId))
	configParts = append(configParts, fmt.Sprintf("  secret_key = %q", *creds.Credentials.SecretAccessKey))
	configParts = append(configParts, fmt.Sprintf("  session_token = %q", *creds.Credentials.SessionToken))
	configParts = append(configParts, regions...)

	return &AccountConfig{
		AccountID:        account.AccountID,
		AccountName:      account.AccountName,
		ConnectionConfig: strings.Join(configParts, "\n"),
	}, nil
}

// ensureClients lazily initializes the AWS Organizations and STS clients.
func (p *AWSProvider) ensureClients(ctx context.Context) error {
	if p.stsClient != nil {
		return nil
	}

	var opts []func(*awsconfig.LoadOptions) error
	if profile := p.resolveProfile(); profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(profile))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}

	p.orgClient = organizations.NewFromConfig(cfg)
	p.stsClient = sts.NewFromConfig(cfg)
	return nil
}

// listActiveAccounts paginates through Organizations ListAccounts and returns
// only accounts with ACTIVE status.
func listActiveAccounts(ctx context.Context, client *organizations.Client) ([]orgtypes.Account, error) {
	var accounts []orgtypes.Account
	paginator := organizations.NewListAccountsPaginator(client, &organizations.ListAccountsInput{})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, acct := range page.Accounts {
			if acct.Status == orgtypes.AccountStatusActive {
				accounts = append(accounts, acct)
			}
		}
	}

	return accounts, nil
}

// listActiveAccountsForParent recursively discovers all ACTIVE accounts under
// a parent OU, including accounts nested in child OUs at any depth.
func listActiveAccountsForParent(ctx context.Context, client *organizations.Client, parentID string) ([]orgtypes.Account, error) {
	var accounts []orgtypes.Account

	// 1. Collect direct child accounts of this parent
	acctPaginator := organizations.NewListAccountsForParentPaginator(client, &organizations.ListAccountsForParentInput{
		ParentId: &parentID,
	})
	for acctPaginator.HasMorePages() {
		page, err := acctPaginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing accounts for %s: %w", parentID, err)
		}
		for _, acct := range page.Accounts {
			if acct.Status == orgtypes.AccountStatusActive {
				accounts = append(accounts, acct)
			}
		}
	}

	// 2. Recurse into child OUs
	childOUs, err := listChildOUs(ctx, client, parentID)
	if err != nil {
		return nil, err
	}
	for _, child := range childOUs {
		childAccounts, err := listActiveAccountsForParent(ctx, client, stringVal(child.Id))
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, childAccounts...)
	}

	return accounts, nil
}

// listChildOUs returns the immediate child OUs of a parent (OU or root).
func listChildOUs(ctx context.Context, client *organizations.Client, parentID string) ([]orgtypes.OrganizationalUnit, error) {
	var ous []orgtypes.OrganizationalUnit
	paginator := organizations.NewListOrganizationalUnitsForParentPaginator(client, &organizations.ListOrganizationalUnitsForParentInput{
		ParentId: &parentID,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("listing child OUs for %s: %w", parentID, err)
		}
		ous = append(ous, page.OrganizationalUnits...)
	}
	return ous, nil
}

// resolveProfile returns the AWS profile to use: struct field → env var → empty.
func (p *AWSProvider) resolveProfile() string {
	if p.Profile != "" {
		return p.Profile
	}
	return os.Getenv("AWS_PROFILE")
}

// resolveRegions returns the regions to use: struct field → env var → nil.
func (p *AWSProvider) resolveRegions() []string {
	if len(p.Regions) > 0 {
		return p.Regions
	}
	if regionsStr := os.Getenv("AWS_REGIONS"); regionsStr != "" {
		var regions []string
		for _, r := range strings.Split(regionsStr, ",") {
			regions = append(regions, strings.TrimSpace(r))
		}
		return regions
	}
	return nil
}

// regionsHCL formats a region list into HCL config lines.
func regionsHCL(regions []string) []string {
	if len(regions) == 0 {
		return nil
	}
	quoted := make([]string, len(regions))
	for i, r := range regions {
		quoted[i] = fmt.Sprintf("%q", r)
	}
	return []string{fmt.Sprintf("  regions = [%s]", strings.Join(quoted, ", "))}
}

// resolveAssumeRoleName returns the IAM role name to assume: AssumeRoleName → OrgRoleName → env var → empty.
func (p *AWSProvider) resolveAssumeRoleName() string {
	if p.AssumeRoleName != "" {
		return p.AssumeRoleName
	}
	if p.OrgRoleName != "" {
		return p.OrgRoleName
	}
	return os.Getenv("AWS_ORG_ROLE_NAME")
}

func stringVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func strPtr(s string) *string {
	return &s
}

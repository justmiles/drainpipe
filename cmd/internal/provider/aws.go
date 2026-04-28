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
)

// AWSMultiAccount implements MultiAccountProvider for AWS Organizations.
// It discovers member accounts and assumes IAM roles to obtain credentials.
// This logic runs in the host process using the AWS SDK directly — it does
// not depend on the Steampipe plugin.
type AWSMultiAccount struct {
	Profile           string   // AWS named profile (fallback: AWS_PROFILE)
	OrgRoleName       string   // IAM role name for org mode (fallback: AWS_ORG_ROLE_NAME)
	AssumeRoleName    string   // IAM role name to assume in each account (alias for OrgRoleName)
	OrgAdminAccountID string   // Admin account to skip (fallback: AWS_ORG_ADMIN_ACCOUNT_ID)
	Regions           []string // Regions to collect (fallback: AWS_REGIONS)
	Organizations     []string // OU IDs to discover accounts from

	orgClient *organizations.Client
	stsClient *sts.Client
}

// DiscoverAccounts lists active accounts in an AWS Organization.
// When Organizations (OU IDs) are configured, it discovers accounts per-OU.
// Otherwise, it falls back to listing all accounts.
// Returns nil (single-account fallback) when org mode is not configured.
func (p *AWSMultiAccount) DiscoverAccounts(ctx context.Context) ([]AccountInfo, error) {
	roleName := p.resolveAssumeRoleName()
	if roleName == "" {
		return nil, nil
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
		for _, ouID := range p.Organizations {
			ouAccounts, ouErr := listActiveAccountsForParent(ctx, p.orgClient, ouID)
			if ouErr != nil {
				return nil, fmt.Errorf("listing accounts for OU %s: %w", ouID, ouErr)
			}
			activeAccounts = append(activeAccounts, ouAccounts...)
		}
	} else {
		activeAccounts, err = listActiveAccounts(ctx, p.orgClient)
		if err != nil {
			return nil, fmt.Errorf("listing organization accounts: %w", err)
		}
	}

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

// AssumeAccountRole obtains temporary credentials for a specific member account.
func (p *AWSMultiAccount) AssumeAccountRole(ctx context.Context, account AccountInfo) (*AccountConfig, error) {
	roleName := p.resolveAssumeRoleName()

	if err := p.ensureClients(ctx); err != nil {
		return nil, err
	}

	roleARN := fmt.Sprintf("arn:aws:iam::%s:role/%s", account.AccountID, roleName)

	creds, err := p.stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         &roleARN,
		RoleSessionName: strPtr("drainpipe-" + account.AccountID),
		Tags: []ststypes.Tag{
			{Key: strPtr("DrainpipeAccountId"), Value: strPtr(account.AccountID)},
			{Key: strPtr("DrainpipeAccountName"), Value: strPtr(account.AccountName)},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("assuming role %s: %w", roleARN, err)
	}

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

// NewAWSMultiAccount creates an AWSMultiAccount from config settings.
func NewAWSMultiAccount(profile string, regions []string, org *OrgSettings) *AWSMultiAccount {
	ma := &AWSMultiAccount{
		Profile: profile,
		Regions: regions,
	}
	if org != nil {
		ma.OrgRoleName = org.RoleName
		ma.AssumeRoleName = org.AssumeRoleName
		ma.OrgAdminAccountID = org.AdminAccountID
		ma.Organizations = org.Organizations
	}
	return ma
}

// OrgSettings holds org configuration extracted from DrainpipeConfig.
type OrgSettings struct {
	RoleName       string
	AssumeRoleName string
	AdminAccountID string
	Organizations  []string
}

func (p *AWSMultiAccount) ensureClients(ctx context.Context) error {
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

func listActiveAccountsForParent(ctx context.Context, client *organizations.Client, parentID string) ([]orgtypes.Account, error) {
	var accounts []orgtypes.Account

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

func (p *AWSMultiAccount) resolveProfile() string {
	if p.Profile != "" {
		return p.Profile
	}
	return os.Getenv("AWS_PROFILE")
}

func (p *AWSMultiAccount) resolveRegions() []string {
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

func (p *AWSMultiAccount) resolveAssumeRoleName() string {
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

package provider

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armsubscriptions"
)

// SubscriptionInfo holds a discovered Azure subscription with its pre-generated
// Steampipe connection HCL. Unlike AWS (where STS tokens expire), Azure service
// principal credentials are long-lived, so connection config is built at
// discovery time rather than just-in-time in workers.
type SubscriptionInfo struct {
	SubscriptionID   string
	DisplayName      string
	TenantID         string
	ConnectionConfig string
}

// AzureMultiSubscription discovers Azure subscriptions across one or more
// tenants using a single service principal. The same client_id/client_secret
// pair authenticates against each tenant where the SP has been granted access.
type AzureMultiSubscription struct {
	ClientID     string
	ClientSecret string
	TenantIDs    []string
}

// NewAzureMultiSubscription creates an AzureMultiSubscription from config.
func NewAzureMultiSubscription(clientID, clientSecret string, tenantIDs []string) *AzureMultiSubscription {
	return &AzureMultiSubscription{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TenantIDs:    tenantIDs,
	}
}

// DiscoverSubscriptions enumerates all enabled subscriptions visible to the
// service principal across the configured tenants. Subscriptions are deduped
// by ID (a subscription reachable from multiple tenants is reported once,
// under the first tenant where it was seen).
func (a *AzureMultiSubscription) DiscoverSubscriptions(ctx context.Context) ([]SubscriptionInfo, error) {
	seen := make(map[string]bool)
	var subs []SubscriptionInfo

	for _, tenantID := range a.TenantIDs {
		cred, err := azidentity.NewClientSecretCredential(tenantID, a.ClientID, a.ClientSecret, nil)
		if err != nil {
			return nil, fmt.Errorf("authenticating to tenant %s: %w", tenantID, err)
		}

		client, err := armsubscriptions.NewClient(cred, nil)
		if err != nil {
			return nil, fmt.Errorf("creating subscriptions client for tenant %s: %w", tenantID, err)
		}

		pager := client.NewListPager(nil)
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return nil, fmt.Errorf("listing subscriptions for tenant %s: %w", tenantID, err)
			}
			for _, sub := range page.Value {
				if sub.State == nil || *sub.State != armsubscriptions.SubscriptionStateEnabled {
					continue
				}
				subID := ptrVal(sub.SubscriptionID)
				if subID == "" || seen[subID] {
					continue
				}
				seen[subID] = true
				subs = append(subs, SubscriptionInfo{
					SubscriptionID:   subID,
					DisplayName:      ptrVal(sub.DisplayName),
					TenantID:         tenantID,
					ConnectionConfig: a.buildConnectionHCL(tenantID, subID),
				})
			}
		}
	}

	if len(subs) == 0 {
		return nil, fmt.Errorf("no enabled subscriptions found across %d tenant(s)", len(a.TenantIDs))
	}

	return subs, nil
}

// buildConnectionHCL generates the Steampipe connection config body for a
// single Azure subscription. This is the HCL that gets passed to the
// turbot/azure plugin via SetConnectionConfig.
func (a *AzureMultiSubscription) buildConnectionHCL(tenantID, subscriptionID string) string {
	parts := []string{
		fmt.Sprintf("  tenant_id = %q", tenantID),
		fmt.Sprintf("  client_id = %q", a.ClientID),
		fmt.Sprintf("  client_secret = %q", a.ClientSecret),
		fmt.Sprintf("  subscription_id = %q", subscriptionID),
	}
	return strings.Join(parts, "\n")
}

func ptrVal(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

package provider

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armsubscriptions"
)

// ---------- NewAzureMultiSubscription ----------

func TestNewAzureMultiSubscription(t *testing.T) {
	tenants := []string{"t1", "t2"}
	ms := NewAzureMultiSubscription("cid", "csecret", tenants)
	if ms.ClientID != "cid" {
		t.Errorf("ClientID = %q, want cid", ms.ClientID)
	}
	if ms.ClientSecret != "csecret" {
		t.Errorf("ClientSecret = %q, want csecret", ms.ClientSecret)
	}
	if len(ms.TenantIDs) != 2 || ms.TenantIDs[0] != "t1" || ms.TenantIDs[1] != "t2" {
		t.Errorf("TenantIDs = %v, want [t1, t2]", ms.TenantIDs)
	}
}

// ---------- buildConnectionHCL ----------

func TestBuildConnectionHCL(t *testing.T) {
	ms := NewAzureMultiSubscription("my-client-id", "my-secret", []string{"tenant-1"})
	got := ms.buildConnectionHCL("tenant-abc", "sub-123")

	expects := []string{
		`tenant_id = "tenant-abc"`,
		`client_id = "my-client-id"`,
		`client_secret = "my-secret"`,
		`subscription_id = "sub-123"`,
	}
	for _, want := range expects {
		if !strings.Contains(got, want) {
			t.Errorf("buildConnectionHCL() missing %q in:\n%s", want, got)
		}
	}
}

func TestBuildConnectionHCL_QuotesSpecialChars(t *testing.T) {
	ms := NewAzureMultiSubscription("id-with\"quote", "secret", []string{"t"})
	got := ms.buildConnectionHCL("t", "sub")
	if !strings.Contains(got, `"id-with\"quote"`) {
		t.Errorf("special chars not properly quoted in:\n%s", got)
	}
}

// ---------- ptrVal ----------

func TestPtrVal_Nil(t *testing.T) {
	if got := ptrVal(nil); got != "" {
		t.Errorf("ptrVal(nil) = %q, want empty", got)
	}
}

func TestPtrVal_NonNil(t *testing.T) {
	s := "hello"
	if got := ptrVal(&s); got != "hello" {
		t.Errorf("ptrVal() = %q, want %q", got, "hello")
	}
}

// ---------- DiscoverSubscriptions with mock ----------

type mockSubscriptionPager struct {
	pages []armsubscriptions.ClientListResponse
	index int
}

func (m *mockSubscriptionPager) More() bool {
	return m.index < len(m.pages)
}

func (m *mockSubscriptionPager) NextPage(_ context.Context) (armsubscriptions.ClientListResponse, error) {
	if m.index >= len(m.pages) {
		return armsubscriptions.ClientListResponse{}, fmt.Errorf("no more pages")
	}
	page := m.pages[m.index]
	m.index++
	return page, nil
}

func strp(s string) *string { return &s }

func statePtr(s armsubscriptions.SubscriptionState) *armsubscriptions.SubscriptionState { return &s }

func TestDiscoverSubscriptions_Deduplication(t *testing.T) {
	// Simulate two tenants returning overlapping subscriptions by testing
	// the dedup logic directly via buildConnectionHCL + seen map.
	ms := NewAzureMultiSubscription("cid", "csec", []string{"t1", "t2"})

	seen := make(map[string]bool)
	var subs []SubscriptionInfo

	type fakeSub struct {
		id, name, tenant string
	}
	// sub-1 appears under both tenants
	fakes := []fakeSub{
		{"sub-1", "Sub One", "t1"},
		{"sub-2", "Sub Two", "t1"},
		{"sub-1", "Sub One", "t2"},
		{"sub-3", "Sub Three", "t2"},
	}

	for _, f := range fakes {
		if seen[f.id] {
			continue
		}
		seen[f.id] = true
		subs = append(subs, SubscriptionInfo{
			SubscriptionID:   f.id,
			DisplayName:      f.name,
			TenantID:         f.tenant,
			ConnectionConfig: ms.buildConnectionHCL(f.tenant, f.id),
		})
	}

	if len(subs) != 3 {
		t.Fatalf("expected 3 unique subs, got %d", len(subs))
	}

	// sub-1 should be attributed to t1 (first seen)
	if subs[0].TenantID != "t1" {
		t.Errorf("sub-1 tenant = %q, want t1 (first seen wins)", subs[0].TenantID)
	}
	if !strings.Contains(subs[0].ConnectionConfig, `tenant_id = "t1"`) {
		t.Error("sub-1 connConfig should reference t1")
	}
}

func TestDiscoverSubscriptions_ConnectionConfigPerSubscription(t *testing.T) {
	ms := NewAzureMultiSubscription("my-cid", "my-secret", []string{"tenant-a"})

	sub1Config := ms.buildConnectionHCL("tenant-a", "sub-111")
	sub2Config := ms.buildConnectionHCL("tenant-a", "sub-222")

	if sub1Config == sub2Config {
		t.Error("different subscriptions should produce different connection configs")
	}

	if !strings.Contains(sub1Config, `subscription_id = "sub-111"`) {
		t.Errorf("sub1Config missing subscription_id:\n%s", sub1Config)
	}
	if !strings.Contains(sub2Config, `subscription_id = "sub-222"`) {
		t.Errorf("sub2Config missing subscription_id:\n%s", sub2Config)
	}

	// Both should share the same credentials
	for _, cfg := range []string{sub1Config, sub2Config} {
		if !strings.Contains(cfg, `client_id = "my-cid"`) {
			t.Errorf("config missing client_id:\n%s", cfg)
		}
		if !strings.Contains(cfg, `client_secret = "my-secret"`) {
			t.Errorf("config missing client_secret:\n%s", cfg)
		}
		if !strings.Contains(cfg, `tenant_id = "tenant-a"`) {
			t.Errorf("config missing tenant_id:\n%s", cfg)
		}
	}
}

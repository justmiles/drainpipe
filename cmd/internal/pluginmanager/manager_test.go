package pluginmanager

import (
	"testing"
)

func TestParsePluginRef_FullSpec(t *testing.T) {
	ref, err := ParsePluginRef("turbot/aws@1.30.0")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Org != "turbot" {
		t.Errorf("Org = %q, want turbot", ref.Org)
	}
	if ref.Name != "aws" {
		t.Errorf("Name = %q, want aws", ref.Name)
	}
	if ref.Version != "1.30.0" {
		t.Errorf("Version = %q, want 1.30.0", ref.Version)
	}
}

func TestParsePluginRef_WithVPrefix(t *testing.T) {
	ref, err := ParsePluginRef("turbot/aws@v1.30.0")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Version != "1.30.0" {
		t.Errorf("Version = %q, want 1.30.0 (v prefix stripped)", ref.Version)
	}
}

func TestParsePluginRef_NoVersion(t *testing.T) {
	ref, err := ParsePluginRef("turbot/cloudflare")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Org != "turbot" || ref.Name != "cloudflare" {
		t.Errorf("got %+v, want turbot/cloudflare", ref)
	}
	if ref.Version != "latest" {
		t.Errorf("Version = %q, want latest", ref.Version)
	}
}

func TestParsePluginRef_ShortName(t *testing.T) {
	ref, err := ParsePluginRef("aws@1.30.0")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Org != "turbot" {
		t.Errorf("Org = %q, want turbot (default)", ref.Org)
	}
	if ref.Name != "aws" || ref.Version != "1.30.0" {
		t.Errorf("got %+v", ref)
	}
}

func TestParsePluginRef_NameOnly(t *testing.T) {
	ref, err := ParsePluginRef("azure")
	if err != nil {
		t.Fatal(err)
	}
	if ref.Org != "turbot" || ref.Name != "azure" || ref.Version != "latest" {
		t.Errorf("got %+v", ref)
	}
}

func TestParsePluginRef_Empty(t *testing.T) {
	_, err := ParsePluginRef("")
	if err == nil {
		t.Error("expected error for empty spec")
	}
}

func TestPluginRef_PluginName(t *testing.T) {
	ref := PluginRef{Name: "aws"}
	if got := ref.PluginName(); got != "steampipe-plugin-aws" {
		t.Errorf("PluginName() = %q, want steampipe-plugin-aws", got)
	}
}

func TestPluginRef_BinaryName(t *testing.T) {
	ref := PluginRef{Name: "cloudflare"}
	if got := ref.BinaryName(); got != "steampipe-plugin-cloudflare.plugin" {
		t.Errorf("BinaryName() = %q, want steampipe-plugin-cloudflare.plugin", got)
	}
}

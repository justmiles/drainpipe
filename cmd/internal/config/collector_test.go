package config

import (
	"testing"
)

func te(name string) TableEntry { return TableEntry{Name: name} }

// ---------- TablesForAccount ----------

func TestTablesForAccount_NoOverrides(t *testing.T) {
	cfg := &DrainpipeConfig{
		Tables: []TableEntry{te("aws_s3_bucket")},
	}
	entries, skip := cfg.TablesForAccount("123", "myaccount")
	if skip {
		t.Error("skip = true, want false")
	}
	if len(entries) != 1 || entries[0].Name != "aws_s3_bucket" {
		t.Errorf("entries = %v, want [{aws_s3_bucket}]", entries)
	}
}

func TestTablesForAccount_NilConfig(t *testing.T) {
	var cfg *DrainpipeConfig
	entries, skip := cfg.TablesForAccount("123", "test")
	if skip {
		t.Error("skip = true, want false for nil config")
	}
	if entries != nil {
		t.Errorf("entries = %v, want nil for nil config", entries)
	}
}

func TestTablesForAccount_MatchByAccountID(t *testing.T) {
	cfg := &DrainpipeConfig{
		Tables: []TableEntry{te("aws_s3_bucket")},
		Connection: ConnectionConfig{
			Org: &OrgConfig{
				Overrides: []OrgOverride{
					{
						Match:  OverrideMatch{AccountIDs: []string{"222222222222"}},
						Tables: []TableEntry{te("aws_ec2_instance")},
					},
				},
			},
		},
	}

	entries, skip := cfg.TablesForAccount("222222222222", "prod")
	if skip {
		t.Error("skip = true, want false")
	}
	if len(entries) != 1 || entries[0].Name != "aws_ec2_instance" {
		t.Errorf("entries = %v, want [{aws_ec2_instance}]", entries)
	}
}

func TestTablesForAccount_MatchByNameGlob(t *testing.T) {
	cfg := &DrainpipeConfig{
		Tables: []TableEntry{te("aws_s3_bucket")},
		Connection: ConnectionConfig{
			Org: &OrgConfig{
				Overrides: []OrgOverride{
					{
						Match:  OverrideMatch{AccountNames: []string{"sandbox-*"}},
						Tables: []TableEntry{te("aws_vpc")},
					},
				},
			},
		},
	}

	entries, skip := cfg.TablesForAccount("999", "sandbox-dev")
	if skip {
		t.Error("skip = true, want false")
	}
	if len(entries) != 1 || entries[0].Name != "aws_vpc" {
		t.Errorf("entries = %v, want [{aws_vpc}]", entries)
	}
}

func TestTablesForAccount_Skip(t *testing.T) {
	cfg := &DrainpipeConfig{
		Connection: ConnectionConfig{
			Org: &OrgConfig{
				Overrides: []OrgOverride{
					{
						Match: OverrideMatch{AccountIDs: []string{"333"}},
						Skip:  true,
					},
				},
			},
		},
	}

	_, skip := cfg.TablesForAccount("333", "skipme")
	if !skip {
		t.Error("skip = false, want true")
	}
}

func TestTablesForAccount_NoMatch_ReturnsDefault(t *testing.T) {
	cfg := &DrainpipeConfig{
		Tables: []TableEntry{te("aws_default")},
		Connection: ConnectionConfig{
			Org: &OrgConfig{
				Overrides: []OrgOverride{
					{
						Match:  OverrideMatch{AccountIDs: []string{"999"}},
						Tables: []TableEntry{te("aws_special")},
					},
				},
			},
		},
	}

	entries, skip := cfg.TablesForAccount("111", "other")
	if skip {
		t.Error("skip = true, want false")
	}
	if len(entries) != 1 || entries[0].Name != "aws_default" {
		t.Errorf("entries = %v, want [{aws_default}]", entries)
	}
}

func TestTablesForAccount_NoDefaultTables_ReturnsNil(t *testing.T) {
	cfg := &DrainpipeConfig{
		Connection: ConnectionConfig{
			Org: &OrgConfig{
				Overrides: []OrgOverride{
					{
						Match:  OverrideMatch{AccountIDs: []string{"999"}},
						Tables: []TableEntry{te("aws_special")},
					},
				},
			},
		},
	}

	entries, skip := cfg.TablesForAccount("111", "other")
	if skip {
		t.Error("skip = true, want false")
	}
	if entries != nil {
		t.Errorf("entries = %v, want nil", entries)
	}
}

// ---------- matchesAccount ----------

func TestMatchesAccount_ByID(t *testing.T) {
	m := OverrideMatch{AccountIDs: []string{"111", "222"}}
	if !matchesAccount(m, "222", "") {
		t.Error("expected match by ID")
	}
}

func TestMatchesAccount_ByNameGlob(t *testing.T) {
	m := OverrideMatch{AccountNames: []string{"prod-*"}}
	if !matchesAccount(m, "", "prod-east") {
		t.Error("expected match by name glob")
	}
}

func TestMatchesAccount_NoMatch(t *testing.T) {
	m := OverrideMatch{
		AccountIDs:   []string{"999"},
		AccountNames: []string{"staging-*"},
	}
	if matchesAccount(m, "111", "production") {
		t.Error("expected no match")
	}
}

func TestMatchesAccount_EmptyMatch(t *testing.T) {
	m := OverrideMatch{}
	if matchesAccount(m, "111", "any") {
		t.Error("empty match should not match anything")
	}
}

func TestMatchesAccount_ExactName(t *testing.T) {
	m := OverrideMatch{AccountNames: []string{"production"}}
	if !matchesAccount(m, "", "production") {
		t.Error("expected exact name match")
	}
	if matchesAccount(m, "", "production-east") {
		t.Error("exact name should not match prefix")
	}
}

// ---------- defaultTables ----------

func TestDefaultTables_Nil(t *testing.T) {
	var cfg *DrainpipeConfig
	if got := cfg.defaultTables(); got != nil {
		t.Errorf("defaultTables() on nil = %v, want nil", got)
	}
}

func TestDefaultTables_Empty(t *testing.T) {
	cfg := &DrainpipeConfig{}
	if got := cfg.defaultTables(); got != nil {
		t.Errorf("defaultTables() on empty = %v, want nil", got)
	}
}

func TestDefaultTables_Present(t *testing.T) {
	cfg := &DrainpipeConfig{Tables: []TableEntry{te("aws_s3_bucket")}}
	got := cfg.defaultTables()
	if len(got) != 1 || got[0].Name != "aws_s3_bucket" {
		t.Errorf("defaultTables() = %v, want [{aws_s3_bucket}]", got)
	}
}

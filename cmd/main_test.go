package main

import (
	"reflect"
	"testing"
)

// ---------- parseFlags ----------

func TestParseFlags_LongFlags(t *testing.T) {
	got := parseFlags([]string{"--provider", "aws", "--tables", "aws_s3_*"})
	if got["provider"] != "aws" {
		t.Errorf("provider = %q, want aws", got["provider"])
	}
	if got["tables"] != "aws_s3_*" {
		t.Errorf("tables = %q, want aws_s3_*", got["tables"])
	}
}

func TestParseFlags_ShortFlags(t *testing.T) {
	got := parseFlags([]string{"-p", "azure", "-t", "azure_*", "-c", "my.yaml"})
	if got["provider"] != "azure" {
		t.Errorf("provider = %q, want azure", got["provider"])
	}
	if got["tables"] != "azure_*" {
		t.Errorf("tables = %q, want azure_*", got["tables"])
	}
	if got["config"] != "my.yaml" {
		t.Errorf("config = %q, want my.yaml", got["config"])
	}
}

func TestParseFlags_EqualsStyle(t *testing.T) {
	got := parseFlags([]string{"--provider=aws", "--tables=aws_s3_bucket"})
	if got["provider"] != "aws" {
		t.Errorf("provider = %q, want aws", got["provider"])
	}
	if got["tables"] != "aws_s3_bucket" {
		t.Errorf("tables = %q, want aws_s3_bucket", got["tables"])
	}
}

func TestParseFlags_BooleanFlag(t *testing.T) {
	got := parseFlags([]string{"--unsupported"})
	if _, ok := got["unsupported"]; !ok {
		t.Error("expected 'unsupported' flag to be present")
	}
}

func TestParseFlags_RepeatedFlags(t *testing.T) {
	got := parseFlags([]string{"--tables", "aws_s3_*", "--tables", "aws_ec2_*"})
	if got["tables"] != "aws_s3_*,aws_ec2_*" {
		t.Errorf("tables = %q, want comma-joined", got["tables"])
	}
}

func TestParseFlags_NonFlagArgsIgnored(t *testing.T) {
	got := parseFlags([]string{"drain", "--provider", "aws", "extra"})
	if got["provider"] != "aws" {
		t.Errorf("provider = %q, want aws", got["provider"])
	}
	if _, ok := got["drain"]; ok {
		t.Error("non-flag 'drain' should not be in flags")
	}
}

func TestParseFlags_Empty(t *testing.T) {
	got := parseFlags(nil)
	if !reflect.DeepEqual(got, map[string]string{}) {
		t.Errorf("parseFlags(nil) = %v, want empty map", got)
	}
}

func TestParseFlags_MixedStyles(t *testing.T) {
	got := parseFlags([]string{"-p", "aws", "--tables=s3", "--unsupported"})
	if got["provider"] != "aws" {
		t.Errorf("provider = %q", got["provider"])
	}
	if got["tables"] != "s3" {
		t.Errorf("tables = %q", got["tables"])
	}
	if _, ok := got["unsupported"]; !ok {
		t.Error("missing unsupported flag")
	}
}

// ---------- flagOrDefault ----------

func TestFlagOrDefault_Present(t *testing.T) {
	flags := map[string]string{"provider": "aws"}
	if got := flagOrDefault(flags, "provider", "azure"); got != "aws" {
		t.Errorf("got %q, want aws", got)
	}
}

func TestFlagOrDefault_Missing(t *testing.T) {
	flags := map[string]string{}
	if got := flagOrDefault(flags, "provider", "aws"); got != "aws" {
		t.Errorf("got %q, want aws (default)", got)
	}
}

func TestFlagOrDefault_EmptyValue(t *testing.T) {
	flags := map[string]string{"provider": ""}
	if got := flagOrDefault(flags, "provider", "aws"); got != "aws" {
		t.Errorf("got %q, want aws (empty treated as missing)", got)
	}
}

// ---------- flagHas ----------

func TestFlagHas_Present(t *testing.T) {
	flags := map[string]string{"unsupported": ""}
	if !flagHas(flags, "unsupported") {
		t.Error("flagHas = false, want true")
	}
}

func TestFlagHas_Absent(t *testing.T) {
	flags := map[string]string{}
	if flagHas(flags, "unsupported") {
		t.Error("flagHas = true, want false")
	}
}

func TestFlagHas_WithValue(t *testing.T) {
	flags := map[string]string{"config": "file.yaml"}
	if !flagHas(flags, "config") {
		t.Error("flagHas = false, want true for key with value")
	}
}

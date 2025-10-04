package match

import (
	"reflect"
	"testing"
)

// ---------- Tables ----------

func TestTables_ExactMatch(t *testing.T) {
	tables := []string{"aws_s3_bucket", "aws_ec2_instance", "aws_vpc"}
	got := Tables(tables, []string{"aws_s3_bucket"})
	want := []string{"aws_s3_bucket"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Tables() = %v, want %v", got, want)
	}
}

func TestTables_GlobStar(t *testing.T) {
	tables := []string{"aws_ec2_instance", "aws_ec2_vpc", "aws_s3_bucket"}
	got := Tables(tables, []string{"aws_ec2_*"})
	want := []string{"aws_ec2_instance", "aws_ec2_vpc"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Tables() = %v, want %v", got, want)
	}
}

func TestTables_GlobQuestion(t *testing.T) {
	tables := []string{"aws_s3_bucket", "aws_s4_bucket"}
	got := Tables(tables, []string{"aws_s?_bucket"})
	want := []string{"aws_s3_bucket", "aws_s4_bucket"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Tables() = %v, want %v", got, want)
	}
}

func TestTables_NoMatch(t *testing.T) {
	tables := []string{"aws_s3_bucket", "aws_ec2_instance"}
	got := Tables(tables, []string{"azure_*"})
	if len(got) != 0 {
		t.Errorf("Tables() = %v, want empty", got)
	}
}

func TestTables_EmptyPatterns_ReturnsAll(t *testing.T) {
	tables := []string{"a", "b", "c"}
	got := Tables(tables, nil)
	if !reflect.DeepEqual(got, tables) {
		t.Errorf("Tables(nil patterns) = %v, want %v", got, tables)
	}

	got2 := Tables(tables, []string{})
	if !reflect.DeepEqual(got2, tables) {
		t.Errorf("Tables(empty patterns) = %v, want %v", got2, tables)
	}
}

func TestTables_Deduplication(t *testing.T) {
	tables := []string{"aws_s3_bucket", "aws_ec2_instance"}
	// Both patterns match aws_s3_bucket
	got := Tables(tables, []string{"aws_s3_bucket", "aws_s3_*"})
	want := []string{"aws_s3_bucket"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Tables() = %v, want %v (no duplicates)", got, want)
	}
}

func TestTables_MultiplePatterns(t *testing.T) {
	tables := []string{"aws_ec2_instance", "aws_s3_bucket", "aws_vpc"}
	got := Tables(tables, []string{"aws_s3_*", "aws_vpc"})
	want := []string{"aws_s3_bucket", "aws_vpc"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Tables() = %v, want %v", got, want)
	}
}

func TestTables_EmptyTables(t *testing.T) {
	got := Tables(nil, []string{"aws_*"})
	if len(got) != 0 {
		t.Errorf("Tables() with nil tables = %v, want empty", got)
	}
}

func TestTables_WildcardMatchesAll(t *testing.T) {
	tables := []string{"a", "b", "c"}
	got := Tables(tables, []string{"*"})
	if !reflect.DeepEqual(got, tables) {
		t.Errorf("Tables(*) = %v, want %v", got, tables)
	}
}

// ---------- Suggest ----------

func TestSuggest_SubstringMatch(t *testing.T) {
	tables := []string{"aws_s3_bucket", "aws_s3_bucket_acl", "aws_ec2_instance"}
	got := Suggest(tables, []string{"s3_bucket"}, 5)
	// Both s3 tables should match
	if len(got) < 2 {
		t.Errorf("Suggest() = %v, want at least 2 results", got)
	}
	// aws_ec2_instance should not be in results
	for _, name := range got {
		if name == "aws_ec2_instance" {
			t.Error("Suggest() should not include aws_ec2_instance for pattern s3_bucket")
		}
	}
}

func TestSuggest_StripWildcards(t *testing.T) {
	tables := []string{"aws_s3_bucket", "aws_ec2_instance"}
	got := Suggest(tables, []string{"aws_s3*"}, 5)
	if len(got) != 1 || got[0] != "aws_s3_bucket" {
		t.Errorf("Suggest() = %v, want [aws_s3_bucket]", got)
	}
}

func TestSuggest_MaxResults(t *testing.T) {
	tables := []string{"aws_a", "aws_b", "aws_c", "aws_d", "aws_e"}
	got := Suggest(tables, []string{"aws"}, 2)
	if len(got) != 2 {
		t.Errorf("Suggest() returned %d results, want 2", len(got))
	}
}

func TestSuggest_NoCandidates(t *testing.T) {
	tables := []string{"aws_s3_bucket"}
	got := Suggest(tables, []string{"azure"}, 3)
	if len(got) != 0 {
		t.Errorf("Suggest() = %v, want empty", got)
	}
}

func TestSuggest_EmptyPattern(t *testing.T) {
	tables := []string{"aws_s3_bucket"}
	// Pattern that strips to empty string after removing wildcards
	got := Suggest(tables, []string{"***"}, 3)
	if len(got) != 0 {
		t.Errorf("Suggest(***) = %v, want empty (clean string is empty)", got)
	}
}

func TestSuggest_Deduplication(t *testing.T) {
	tables := []string{"aws_s3_bucket"}
	// Two patterns both match the same table
	got := Suggest(tables, []string{"s3", "bucket"}, 5)
	count := 0
	for _, name := range got {
		if name == "aws_s3_bucket" {
			count++
		}
	}
	if count > 1 {
		t.Errorf("Suggest() returned duplicate entries: %v", got)
	}
}

func TestSuggest_OrderByMatchQuality(t *testing.T) {
	tables := []string{"aws_s3_bucket", "aws_s3_bucket_logging_configuration"}
	// "s3_bucket_logging_configuration" is a longer substring match
	got := Suggest(tables, []string{"s3_bucket_logging_configuration"}, 5)
	if len(got) == 0 {
		t.Fatal("Suggest() returned empty")
	}
	// The longer match should be first
	if got[0] != "aws_s3_bucket_logging_configuration" {
		t.Errorf("Suggest()[0] = %q, want aws_s3_bucket_logging_configuration (longer match first)", got[0])
	}
}

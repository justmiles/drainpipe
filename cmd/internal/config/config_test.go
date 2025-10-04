package config

import (
	"os"
	"testing"
)

// ---------- getEnv ----------

func TestGetEnv_Set(t *testing.T) {
	t.Setenv("TEST_GETENV_KEY", "custom_value")
	if got := getEnv("TEST_GETENV_KEY", "fallback"); got != "custom_value" {
		t.Errorf("getEnv() = %q, want %q", got, "custom_value")
	}
}

func TestGetEnv_Unset(t *testing.T) {
	os.Unsetenv("TEST_GETENV_MISSING")
	if got := getEnv("TEST_GETENV_MISSING", "fallback"); got != "fallback" {
		t.Errorf("getEnv() = %q, want %q", got, "fallback")
	}
}

func TestGetEnv_EmptyString(t *testing.T) {
	t.Setenv("TEST_GETENV_EMPTY", "")
	// An explicitly set empty string should be returned, not the fallback.
	if got := getEnv("TEST_GETENV_EMPTY", "fallback"); got != "" {
		t.Errorf("getEnv() = %q, want empty string", got)
	}
}

// ---------- Load ----------

func TestLoad_Defaults(t *testing.T) {
	// Unset all DB_* vars so Load() returns defaults.
	for _, k := range []string{"DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"} {
		t.Setenv(k, "")
		os.Unsetenv(k)
	}

	cfg := Load()

	checks := map[string]struct{ got, want string }{
		"DBHost":     {cfg.DBHost, "localhost"},
		"DBPort":     {cfg.DBPort, "5432"},
		"DBName":     {cfg.DBName, "cmdb"},
		"DBUser":     {cfg.DBUser, "cmdb"},
		"DBPassword": {cfg.DBPassword, "cmdb_dev"},
	}
	for field, c := range checks {
		if c.got != c.want {
			t.Errorf("Load().%s = %q, want %q", field, c.got, c.want)
		}
	}
}

func TestLoad_FromEnv(t *testing.T) {
	t.Setenv("DB_HOST", "db.example.com")
	t.Setenv("DB_PORT", "5433")
	t.Setenv("DB_NAME", "inventory")
	t.Setenv("DB_USER", "admin")
	t.Setenv("DB_PASSWORD", "s3cret")

	cfg := Load()

	if cfg.DBHost != "db.example.com" {
		t.Errorf("DBHost = %q, want %q", cfg.DBHost, "db.example.com")
	}
	if cfg.DBPort != "5433" {
		t.Errorf("DBPort = %q, want %q", cfg.DBPort, "5433")
	}
	if cfg.DBName != "inventory" {
		t.Errorf("DBName = %q, want %q", cfg.DBName, "inventory")
	}
}

// ---------- DSN ----------

func TestDSN_Default(t *testing.T) {
	os.Unsetenv("DB_SSLMODE")
	cfg := &Config{
		DBHost:     "localhost",
		DBPort:     "5432",
		DBName:     "cmdb",
		DBUser:     "cmdb",
		DBPassword: "cmdb_dev",
	}

	want := "postgres://cmdb:cmdb_dev@localhost:5432/cmdb?sslmode=disable"
	if got := cfg.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}

func TestDSN_CustomSSLMode(t *testing.T) {
	t.Setenv("DB_SSLMODE", "require")
	cfg := &Config{
		DBHost:     "prod.db",
		DBPort:     "5432",
		DBName:     "inventory",
		DBUser:     "app",
		DBPassword: "pw",
	}

	want := "postgres://app:pw@prod.db:5432/inventory?sslmode=require"
	if got := cfg.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}

func TestDSN_SpecialCharsInPassword(t *testing.T) {
	os.Unsetenv("DB_SSLMODE")
	cfg := &Config{
		DBHost:     "localhost",
		DBPort:     "5432",
		DBName:     "db",
		DBUser:     "user",
		DBPassword: "p@ss:w0rd",
	}

	// DSN currently does not URL-encode; verify the raw string is placed as-is.
	want := "postgres://user:p@ss:w0rd@localhost:5432/db?sslmode=disable"
	if got := cfg.DSN(); got != want {
		t.Errorf("DSN() = %q, want %q", got, want)
	}
}

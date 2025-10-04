package config

import (
	"fmt"
	"os"
)

// Config holds database connection settings.
// Provider-specific config is handled by each Provider implementation.
type Config struct {
	DBHost     string
	DBPort     string
	DBName     string
	DBUser     string
	DBPassword string
}

// DSN returns the PostgreSQL connection string.
func (c *Config) DSN() string {
	sslmode := getEnv("DB_SSLMODE", "disable")
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.DBUser, c.DBPassword, c.DBHost, c.DBPort, c.DBName, sslmode,
	)
}

// Load reads database configuration from environment variables.
func Load() *Config {
	return &Config{
		DBHost:     getEnv("DB_HOST", "localhost"),
		DBPort:     getEnv("DB_PORT", "5432"),
		DBName:     getEnv("DB_NAME", "cmdb"),
		DBUser:     getEnv("DB_USER", "cmdb"),
		DBPassword: getEnv("DB_PASSWORD", "cmdb_dev"),
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}

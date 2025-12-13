package config

import (
	"os"
	"strings"
	"sync"

	"github.com/joho/godotenv"
)

var (
	instance *Config
	once     sync.Once
)

// Config holds the application configuration
type Config struct {
	// Environment
	Env string

	// Server
	Port string

	// Supabase
	SupabaseURL        string
	SupabaseServiceKey string
	SupabaseAnonKey    string

	// Database (optional direct connection)
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string
	DBCACert   string

	// Data transformation
	JoinMappings  []JoinMapping
	HiddenColumns []HiddenColumn
	FlattenJSONB  []FlattenJSONBConfig
}

// JoinMapping defines how to replace a foreign key with a display value
type JoinMapping struct {
	Schema        string // Source table schema (e.g., "analytics")
	Table         string // Source table name (e.g., "realtime_metrics")
	Column        string // Source column with FK (e.g., "machine_id")
	ForeignSchema string // Foreign table schema (e.g., "production")
	ForeignTable  string // Foreign table name (e.g., "machine_list")
	ForeignKey    string // Foreign table key column (e.g., "id") - defaults to "id"
	DisplayColumn string // Column to display instead (e.g., "name")
	OutputName    string // Output column name (e.g., "machine_name")
}

// HiddenColumn defines a column to hide from output
type HiddenColumn struct {
	Schema string
	Table  string
	Column string
}

// FlattenJSONBConfig defines a JSONB column to flatten
type FlattenJSONBConfig struct {
	Schema string
	Table  string
	Column string
}

// Get returns the singleton config instance
func Get() *Config {
	once.Do(func() {
		instance = load()
	})
	return instance
}

// MustLoad loads config and panics on error
func MustLoad() *Config {
	return Get()
}

func load() *Config {
	// Load .env file if it exists
	_ = godotenv.Load()

	cfg := &Config{
		Env:                getEnv("ENV", "development"),
		Port:               getEnv("PORT", "8080"),
		SupabaseURL:        getEnv("SUPABASE_URL", ""),
		SupabaseServiceKey: getEnv("SUPABASE_SERVICE_KEY", ""),
		SupabaseAnonKey:    getEnv("SUPABASE_ANON_KEY", ""),
		DBHost:             getEnv("SUPABASE_DB_HOST", ""),
		DBPort:             getEnv("SUPABASE_DB_PORT", "5432"),
		DBUser:             getEnv("SUPABASE_DB_USER", ""),
		DBPassword:         getEnv("SUPABASE_DB_PASSWORD", ""),
		DBName:             getEnv("SUPABASE_DB_NAME", "postgres"),
		DBCACert:           getEnv("SUPABASE_CA_CERT", ""),
	}

	// Parse JOIN_MAPPINGS
	cfg.JoinMappings = parseJoinMappings(getEnv("JOIN_MAPPINGS", ""))

	// Parse HIDDEN_COLUMNS
	cfg.HiddenColumns = parseHiddenColumns(getEnv("HIDDEN_COLUMNS", ""))

	// Parse FLATTEN_JSONB
	cfg.FlattenJSONB = parseFlattenJSONB(getEnv("FLATTEN_JSONB", ""))

	return cfg
}

// parseJoinMappings parses the JOIN_MAPPINGS environment variable
// Format: "schema.table.column:foreign_schema.foreign_table.foreign_key.display_column:output_name,..."
// Example: "analytics.realtime_metrics.machine_id:production.machine_list.id.name:machine_name"
func parseJoinMappings(value string) []JoinMapping {
	if value == "" {
		return nil
	}

	var mappings []JoinMapping
	entries := strings.Split(value, ",")

	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		// Split by colon: source:foreign:output
		parts := strings.Split(entry, ":")
		if len(parts) != 3 {
			continue
		}

		// Parse source: schema.table.column
		sourceParts := strings.Split(parts[0], ".")
		if len(sourceParts) != 3 {
			continue
		}

		// Parse foreign: schema.table.foreign_key.display_column (4 parts)
		// OR: schema.table.display_column (3 parts, foreign_key defaults to "id")
		foreignParts := strings.Split(parts[1], ".")

		var foreignSchema, foreignTable, foreignKey, displayColumn string

		if len(foreignParts) == 4 {
			// Full format: schema.table.foreign_key.display_column
			foreignSchema = foreignParts[0]
			foreignTable = foreignParts[1]
			foreignKey = foreignParts[2]
			displayColumn = foreignParts[3]
		} else if len(foreignParts) == 3 {
			// Short format: schema.table.display_column (foreign_key defaults to "id")
			foreignSchema = foreignParts[0]
			foreignTable = foreignParts[1]
			foreignKey = "id"
			displayColumn = foreignParts[2]
		} else {
			continue
		}

		// Parse output name
		outputName := strings.TrimSpace(parts[2])
		if outputName == "" {
			outputName = displayColumn
		}

		mappings = append(mappings, JoinMapping{
			Schema:        sourceParts[0],
			Table:         sourceParts[1],
			Column:        sourceParts[2],
			ForeignSchema: foreignSchema,
			ForeignTable:  foreignTable,
			ForeignKey:    foreignKey,
			DisplayColumn: displayColumn,
			OutputName:    outputName,
		})
	}

	return mappings
}

// parseHiddenColumns parses the HIDDEN_COLUMNS environment variable
// Format: "schema.table.column,schema.table.column,..."
func parseHiddenColumns(value string) []HiddenColumn {
	if value == "" {
		return nil
	}

	var columns []HiddenColumn
	entries := strings.Split(value, ",")

	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.Split(entry, ".")
		if len(parts) != 3 {
			continue
		}

		columns = append(columns, HiddenColumn{
			Schema: parts[0],
			Table:  parts[1],
			Column: parts[2],
		})
	}

	return columns
}

// parseFlattenJSONB parses the FLATTEN_JSONB environment variable
// Format: "schema.table.column,schema.table.column,..."
func parseFlattenJSONB(value string) []FlattenJSONBConfig {
	if value == "" {
		return nil
	}

	var configs []FlattenJSONBConfig
	entries := strings.Split(value, ",")

	for _, entry := range entries {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		parts := strings.Split(entry, ".")
		if len(parts) != 3 {
			continue
		}

		configs = append(configs, FlattenJSONBConfig{
			Schema: parts[0],
			Table:  parts[1],
			Column: parts[2],
		})
	}

	return configs
}

// ShouldFlattenJSONB checks if a column should be flattened
func (c *Config) ShouldFlattenJSONB(schema, table, column string) bool {
	for _, cfg := range c.FlattenJSONB {
		if cfg.Schema == schema && cfg.Table == table && cfg.Column == column {
			return true
		}
	}
	return false
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

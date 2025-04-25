package config

import (
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// Config holds all configuration for the application
type Config struct {
	AppPort           int
	AppEnv            string
	MongoURI          string
	MongoDatabase     string
	JWTSecret         string
	JWTExpiry         time.Duration
	AllowOrigins      string
	OpenRouterAPIKey  string
	OpenRouterModel   string
	OpenRouterBaseURL string
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	// Load .env file if it exists
	godotenv.Load()

	// Set default values
	config := &Config{
		AppPort:       8080,
		AppEnv:        "development",
		MongoURI:      "mongodb://localhost:27017",
		MongoDatabase: "goquery",
		JWTSecret:     "your-secret-key",
		JWTExpiry:     time.Hour * 24 * 7, // 7 days
		AllowOrigins:  "*",
	}

	// Override with environment variables if they exist
	if port := os.Getenv("APP_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			config.AppPort = p
		}
	}

	if env := os.Getenv("APP_ENV"); env != "" {
		config.AppEnv = env
	}

	if uri := os.Getenv("MONGO_URI"); uri != "" {
		config.MongoURI = uri
	}

	if db := os.Getenv("MONGO_DATABASE"); db != "" {
		config.MongoDatabase = db
	}

	if secret := os.Getenv("JWT_SECRET"); secret != "" {
		config.JWTSecret = secret
	}

	if expiry := os.Getenv("JWT_EXPIRY"); expiry != "" {
		if exp, err := time.ParseDuration(expiry); err == nil {
			config.JWTExpiry = exp
		}
	}

	if origins := os.Getenv("ALLOW_ORIGINS"); origins != "" {
		config.AllowOrigins = origins
	}

	if apiKey := os.Getenv("OPENROUTER_API_KEY"); apiKey != "" {
		config.OpenRouterAPIKey = apiKey
	}

	if model := os.Getenv("OPENROUTER_MODEL"); model != "" {
		config.OpenRouterModel = model
	} else {
		// Default model if not specified
		config.OpenRouterModel = "deepseek-chat"
	}

	if baseURL := os.Getenv("OPENROUTER_BASE_URL"); baseURL != "" {
		config.OpenRouterBaseURL = baseURL
	} else {
		// Default base URL if not specified
		config.OpenRouterBaseURL = "https://api.deepseek.com/chat/completions"
	}

	return config, nil
}

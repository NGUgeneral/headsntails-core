package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	AppEnv       string
	AppHost      string
	JwtSecretKey string
	JwtAlgorithm string

	// Redis Parameters
	RedisAddr     string
	RedisPassword string
	RedisURL      string
	RedisHashKey  string

	// PostgreSQL Parameters
	DBHost string
	DBPort string
	DBUser string
	DBPass string
	DBName string

	// External Services
	RateLimiterURL      string
	RateLimiterGRPCURL  string
	RateLimiterGRPCCall bool
}

func LoadConfig() *Config {
	appEnv := getEnv("APP_ENV", "local")
	if appEnv != "production" {
		if err := godotenv.Load(); err != nil {
			log.Println("⚠️ No .env file discovered; falling back to native environment variables.")
		}
	}

	cfg := &Config{
		AppEnv:       appEnv,
		AppHost:      getEnv("APP_HOST", "localhost:8080"),
		JwtAlgorithm: getEnv("JWT_ALGORITHM", "HS256"),

		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		RedisURL:      os.Getenv("REDIS_URL"),
		RedisHashKey:  getEnv("REDIS_HASH_KEY", "headsntails:v1:flags"),

		DBHost: getEnv("DB_HOST", "localhost"),
		DBPort: getEnv("DB_PORT", "5432"),
		DBUser: getEnv("DB_USER", "postgres"),
		DBPass: getEnv("DB_PASS", "postgres"),
		DBName: getEnv("DB_NAME", "headsntails"),

		RateLimiterURL:      os.Getenv("RATE_LIMITER_URL"),
		RateLimiterGRPCURL:  os.Getenv("RATE_LIMITER_GRPC_URL"),
		RateLimiterGRPCCall: getEnv("RATE_LIMITER_GRPC_CALL", "false") == "true",
		JwtSecretKey:        os.Getenv("JWT_SECRET_KEY"),
	}

	cfg.validateRequiredFields()

	return cfg
}

func (c *Config) GetPostgresConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.DBUser, c.DBPass, c.DBHost, c.DBPort, c.DBName,
	)
}

func (c *Config) validateRequiredFields() {
	if c.RateLimiterURL == "" {
		log.Fatal("CRITICAL: RATE_LIMITER_URL environment variable is required but not set!")
	}
	if c.JwtSecretKey == "" {
		log.Fatal("CRITICAL: JWT_SECRET_KEY environment variable is required but not set!")
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

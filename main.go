package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"headsntails-core/config"
	"headsntails-core/docs"
	"headsntails-core/middleware"

	_ "headsntails-core/docs"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	httpSwagger "github.com/swaggo/http-swagger/v2"
)

var ctx = context.Background()

type PgxPoolIface interface {
	Ping(ctx context.Context) error
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	Close()
}

type Engine struct {
	mu           sync.RWMutex
	flags        map[string]bool
	rdb          *redis.Client
	dbPool       PgxPoolIface
	redisHashKey string
}

func NewEngine(cfg *config.Config) *Engine {
	dbCtx, dbCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dbCancel()

	connString := cfg.GetPostgresConnectionString()
	dbPool, err := pgxpool.New(dbCtx, connString)
	if err != nil {
		log.Fatalf("CRITICAL: Failed to create PostgreSQL connection pool allocation footprint: %v", err)
	}

	if err := dbPool.Ping(dbCtx); err != nil {
		log.Fatalf("CRITICAL: PostgreSQL source of truth is unreachable: %v", err)
	}
	log.Println("PostgreSQL client pool initialized and verified successfully.")

	var opt *redis.Options
	if cfg.RedisURL != "" {
		opt, err = redis.ParseURL(cfg.RedisURL)
		if err != nil {
			log.Fatalf("CRITICAL: Failed to parse secure Redis connection URL: %v", err)
		}
		log.Println("Redis client initialized securely via connection URL string (TLS Enabled).")
	} else {
		opt = &redis.Options{
			Addr:     cfg.RedisAddr,
			Password: cfg.RedisPassword,
			DB:       0,
		}
		log.Printf("Redis client initialized via unencrypted parameters. Target: %s", cfg.RedisAddr)
	}

	rdb := redis.NewClient(opt)

	redisCtx, redisCancel := context.WithTimeout(ctx, 2*time.Second)
	defer redisCancel()
	if err := rdb.Ping(redisCtx).Err(); err != nil {
		log.Fatalf("CRITICAL: Redis caching boundary is unreachable: %v", err)
	}

	engine := &Engine{
		flags:        make(map[string]bool),
		rdb:          rdb,
		dbPool:       dbPool,
		redisHashKey: cfg.RedisHashKey,
	}

	engine.hydrateAndSyncDatabases()

	return engine
}

func (e *Engine) hydrateAndSyncDatabases() {
	e.mu.Lock()
	defer e.mu.Unlock()

	log.Println("Initializing critical source of truth state synchronization flow...")

	rows, err := e.dbPool.Query(ctx, "SELECT service, key, value FROM public.feature_flags")
	if err != nil {
		log.Fatalf("CRITICAL: Base system hydration failed during PostgreSQL stream extraction: %v", err)
	}
	defer rows.Close()

	pipe := e.rdb.Pipeline()
	pipe.Del(ctx, e.redisHashKey)

	dbCount := 0
	for rows.Next() {
		var service, key string
		var value bool
		if err := rows.Scan(&service, &key, &value); err != nil {
			log.Fatalf("CRITICAL: Row scans corrupted during schema extraction mapping loop: %v", err)
		}

		compositeKey := fmt.Sprintf("%s:%s", service, key)

		e.flags[compositeKey] = value

		valStr := "false"
		if value {
			valStr = "true"
		}
		pipe.HSet(ctx, e.redisHashKey, compositeKey, valStr)
		dbCount++
	}

	if dbCount > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("CRITICAL: Failed to propagate primary database state down to Redis layer cache: %v", err)
		}
	}

	log.Printf("PARITY COMPLETE: Hydrated %d flags from PostgreSQL directly into Redis and Local Memory Cache.", dbCount)
}

func (e *Engine) CheckRedisConnectivity() error {
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return e.rdb.Ping(pingCtx).Err()
}

func (e *Engine) GetFlag(service, key string) bool {
	compositeKey := fmt.Sprintf("%s:%s", service, key)

	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.flags[compositeKey]
}

func (e *Engine) SetFlag(service, key string, value bool) error {
	compositeKey := fmt.Sprintf("%s:%s", service, key)

	tx, err := e.dbPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize db transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	query := `
		INSERT INTO public.feature_flags (service, key, value, updated_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (service, key)
		DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
	`
	_, err = tx.Exec(ctx, query, service, key, value)
	if err != nil {
		return fmt.Errorf("postgresql source of truth write failure: %w", err)
	}

	valStr := "false"
	if value {
		valStr = "true"
	}

	if err := e.rdb.HSet(ctx, e.redisHashKey, compositeKey, valStr).Err(); err != nil {
		return fmt.Errorf("redis cache write failure (postgres transaction aborted): %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit postgres transaction: %w", err)
	}

	e.mu.Lock()
	e.flags[compositeKey] = value
	e.mu.Unlock()

	return nil
}

func (e *Engine) GetFlagsByService(service string) map[string]bool {
	prefix := fmt.Sprintf("%s:", service)
	prefixLen := len(prefix)

	e.mu.RLock()
	defer e.mu.RUnlock()

	serviceFlags := make(map[string]bool)

	for compositeKey, value := range e.flags {
		if len(compositeKey) >= prefixLen && compositeKey[:prefixLen] == prefix {
			rawKey := compositeKey[prefixLen:]
			serviceFlags[rawKey] = value
		}
	}

	return serviceFlags
}

type FlagPayload struct {
	Service string `json:"service" example:"billing-service"`
	Key     string `json:"key" example:"enable-stripe-v2"`
	Value   bool   `json:"value" example:"true"`
}

type HealthResponse struct {
	Status string `json:"status" example:"healthy"`
	Redis  string `json:"redis" example:"connected"`
}

type ErrorResponse struct {
	Error string `json:"error" example:"Missing required parameters"`
}

// Global runtime metadata configuration block
// @title                      headsntails Feature Engine API
// @version                    1.0
// @description                High-performance inline feature flagging control plane.
// @host                       localhost:8080
// @BasePath                   /
// @securityDefinitions.apiKey  BearerAuth
// @in                         header
// @name                       Authorization
// @description                Type 'Bearer <your_jwt_token>' to access protected routes.
func main() {
	cfg := config.LoadConfig()
	engine := NewEngine(cfg)

	secretBytes := []byte(cfg.JwtSecretKey)
	authGuard := middleware.AuthMiddleware(secretBytes, cfg.JwtAlgorithm)

	const RateLimitTimeout = 40 * time.Millisecond
	var limiterStrategy middleware.RateLimiterStrategy

	if cfg.RateLimiterGRPCCall {
		conn, err := grpc.NewClient(cfg.RateLimiterGRPCURL, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("[MAIN] Critical failure initializing Rate Limiter gRPC channel: %v", err)
		}
		limiterStrategy = middleware.NewGRPCRateLimiter(conn)
		log.Printf("[MAIN] Rate Limiter initialized in high-performance gRPC mode targeting: %s", cfg.RateLimiterGRPCURL)
	} else {
		// Fall back to the default HTTP JSON endpoint path
		limiterStrategy = middleware.NewHTTPRateLimiter(cfg.RateLimiterURL, RateLimitTimeout)
		log.Printf("[MAIN] Rate Limiter initialized in standard HTTP mode targeting: %s", cfg.RateLimiterURL)
	}

	rateGuard := middleware.RateLimitGuard(limiterStrategy, RateLimitTimeout)

	// --- PUBLIC ROUTING ---
	http.HandleFunc("/health", handleHealth(engine))

	// --- AUTOMATED INTERACTIVE DOCUMENTATION TESTBENCH ---
	docs.SwaggerInfo.Host = cfg.AppHost
	http.Handle("/docs/", httpSwagger.Handler(httpSwagger.URL("/docs/doc.json")))
	http.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/docs/", http.StatusMovedPermanently)
	})

	// --- PROTECTED ROUTING WITH INLINE DEFENSIVE RATE LIMITING ---
	// Execution order: Auth verification -> Rate validation checks -> Flag computation logic
	http.Handle("/api/v1/get", authGuard(rateGuard(http.HandlerFunc(handleGetFlag(engine)))))
	http.Handle("/api/v1/set", authGuard(rateGuard(http.HandlerFunc(handleSetFlag(engine)))))
	http.Handle("/api/v1/get_flags", authGuard(rateGuard(http.HandlerFunc(handleGetFlagsByService(engine)))))

	// --- GLOBAL INTERNET INGRESS WRAPPER ---
	// Passing http.DefaultServeMux wrapped by CORSEnforcer ensures that /health, /docs,
	// and all mutation endpoints catch the browser handshake automatically.
	globalHandler := middleware.CORSEnforcer(http.DefaultServeMux)

	log.Printf("headsntails Core online [%s mode]. Control port listening on :8080...", cfg.AppEnv)
	if err := http.ListenAndServe(":8080", globalHandler); err != nil {
		log.Fatalf("Server panic: %v", err)
	}
}

// --- EXTRACTED HANDLER LAYER CONTEXTS WITH SWAGGER TAGS ---

// handleHealth godoc
// @Summary      Engine Health Check
// @Description  Verifies running web engine operations and synchronous underlying upstash storage ping telemetry.
// @Tags         System
// @Produce      json
// @Success      200  {object}  HealthResponse
// @Failure      503  {object}  HealthResponse  "Service Unavailable - Storage cluster connection broken"
// @Failure      429      {object}  middleware.RateCheck429Response "Too Many Requests - Rate limit exceeded or quota exhausted"
// @Router       /health [get]
func handleHealth(engine *Engine) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if err := engine.dbPool.Ping(ctx); err != nil {
			log.Printf("Health check failure: PostgreSQL unreachable: %v", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy", "database": "disconnected"})
			return
		}

		if err := engine.CheckRedisConnectivity(); err != nil {
			log.Printf("Health check failure: Redis unreachable: %v", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "unhealthy", "redis": "disconnected"})
			return
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(HealthResponse{Status: "healthy", Redis: "connected"})
	}
}

// handleGetFlag godoc
// @Summary      Get Individual Feature Flag Evaluation
// @Description  Evaluates a single state status targeting a specific composite namespace binding key.
// @Tags         Evaluation
// @Produce      plain
// @Param        service  query     string  true  "Target identifying domain service space"
// @Param        key      query     string  true  "The distinct targeting feature gate identifier identifier"
// @Security     BearerAuth
// @Success      200      {string}  string  "Service [billing] Flag [enable-v2]: true"
// @Failure      400      {string}  string  "Missing required parameters"
// @Failure      401      {string}  string  "Unauthorized missing payload token signature"
// @Failure      429      {object}  middleware.RateCheck429Response "Too Many Requests - Rate limit exceeded or quota exhausted"
// @Router       /get [get]
func handleGetFlag(engine *Engine) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		service := r.URL.Query().Get("service")
		key := r.URL.Query().Get("key")

		if service == "" || key == "" {
			http.Error(w, "Missing required 'service' or 'key' parameter", http.StatusBadRequest)
			return
		}

		enabled := engine.GetFlag(service, key)
		fmt.Fprintf(w, "Service [%s] Flag [%s]: %t\n", service, key, enabled)
	}
}

// handleSetFlag godoc
// @Summary      Mutate/Set Target Feature State
// @Description  Commits an administrative state change parameter downward to secure cluster storage and instantly syncs internal tracking memory cache state.
// @Tags         Administration
// @Accept       json
// @Produce      plain
// @Param        payload  body      FlagPayload  true  "Target state runtime mutation specification matrix description block"
// @Security     BearerAuth
// @Success      200      {string}  string       "Successfully updated flag [billing:enable-v2] to true"
// @Failure      400      {string}  string       "Invalid configuration format parameters"
// @Failure      401      {string}  string       "Unauthorized"
// @Failure      429      {object}  middleware.RateCheck429Response "Too Many Requests - Rate limit exceeded or quota exhausted"
// @Failure      500      {string}  string       "Internal persistence storage communication error context"
// @Router       /set [post]
func handleSetFlag(engine *Engine) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var payload FlagPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "Invalid JSON body", http.StatusBadRequest)
			return
		}

		if payload.Service == "" || payload.Key == "" {
			http.Error(w, "Fields 'service' and 'key' cannot be empty strings", http.StatusBadRequest)
			return
		}

		if err := engine.SetFlag(payload.Service, payload.Key, payload.Value); err != nil {
			http.Error(w, "Internal persistence error", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Successfully updated flag [%s:%s] to %t\n", payload.Service, payload.Key, payload.Value)
	}
}

// handleGetFlagsByService godoc
// @Summary      Get All Component Configuration Flags for a Service Namespace
// @Description  Extracts the complete underlying operational state dataset block bounded to a single contextual microservice domain.
// @Tags         Evaluation
// @Produce      json
// @Param        service  query     string         true  "Target identifying domain service space context parameter mapping string"
// @Security     BearerAuth
// @Success      200      {object}  map[string]bool  "Example output dictionary block payload"
// @Failure      400      {object}  ErrorResponse
// @Failure      401      {string}  string           "Unauthorized"
// @Failure      429      {object}  middleware.RateCheck429Response "Too Many Requests - Rate limit exceeded or quota exhausted"
// @Router       /get_flags [get]
func handleGetFlagsByService(engine *Engine) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		service := r.URL.Query().Get("service")
		if service == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing required 'service' parameter"})
			return
		}

		flagsMatrix := engine.GetFlagsByService(service)

		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(flagsMatrix); err != nil {
			log.Printf("Failed to encode flags matrix payload: %v", err)
		}
	}
}

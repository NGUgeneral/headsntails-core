package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"headsntails-core/config"

	"github.com/alicebob/miniredis/v2"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/redis/go-redis/v9"
)

func setupTestEngine(t *testing.T) (*Engine, *miniredis.Miniredis, pgxmock.PgxPoolIface) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to initialize local miniredis: %v", err)
	}

	dbMock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("Failed to initialize pgx pool mock: %v", err)
	}

	cfg := &config.Config{
		RedisHashKey: "test:feature:flags",
	}

	rows := pgxmock.NewRows([]string{"service", "key", "value"})
	dbMock.ExpectQuery("SELECT service, key, value FROM public.feature_flags").WillReturnRows(rows)

	engine := &Engine{
		flags:        make(map[string]bool),
		rdb:          redis.NewClient(&redis.Options{Addr: s.Addr()}),
		dbPool:       dbMock,
		redisHashKey: cfg.RedisHashKey,
	}

	engine.hydrateAndSyncDatabases()

	return engine, s, dbMock
}

func TestEngineMutationsAndHydration(t *testing.T) {
	engine, mr, dbMock := setupTestEngine(t)
	defer mr.Close()
	defer dbMock.Close()

	dbMock.ExpectBegin()
	dbMock.ExpectExec("INSERT INTO public.feature_flags").
		WithArgs("billing", "enable-stripe-v2", true).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	dbMock.ExpectCommit()

	err := engine.SetFlag("billing", "enable-stripe-v2", true)
	if err != nil {
		t.Fatalf("Unexpected write failure: %v", err)
	}

	if !engine.GetFlag("billing", "enable-stripe-v2") {
		t.Error("Expected billing:enable-stripe-v2 to evaluate to true")
	}

	val := mr.HGet("test:feature:flags", "billing:enable-stripe-v2")
	if val != "true" {
		t.Errorf("Persistence tracking layout mismatch in storage: expected 'true', got %q", val)
	}

	if err := dbMock.ExpectationsWereMet(); err != nil {
		t.Errorf("Unfulfilled database transaction expectations: %v", err)
	}
}

func TestGetFlagFallbackBehaviors(t *testing.T) {
	engine, mr, dbMock := setupTestEngine(t)
	defer mr.Close()
	defer dbMock.Close()

	if engine.GetFlag("ghost-service", "any-key") {
		t.Error("Engine target lookup logic evaluated missing keys to true instead of false default")
	}
}

func TestHandleHealthSuccess(t *testing.T) {
	engine, mr, dbMock := setupTestEngine(t)
	defer mr.Close()
	defer dbMock.Close()

	dbMock.ExpectPing()

	req, _ := http.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	handler := handleHealth(engine)

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", rr.Code)
	}
}

func TestHandleHealthStorageFailure(t *testing.T) {
	engine, _, dbMock := setupTestEngine(t)
	defer dbMock.Close()

	dbMock.ExpectPing().WillReturnError(fmt.Errorf("database connectivity lost"))

	req, _ := http.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	handler := handleHealth(engine)

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status code 503 for database disconnection, got %d", rr.Code)
	}
}

func TestHandleSetFlagPayloadVerification(t *testing.T) {
	engine, mr, dbMock := setupTestEngine(t)
	defer mr.Close()
	defer dbMock.Close()

	dbMock.ExpectBegin()
	dbMock.ExpectExec("INSERT INTO public.feature_flags").
		WithArgs("inventory", "realtime-sync", true).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	dbMock.ExpectCommit()

	payload := FlagPayload{
		Service: "inventory",
		Key:     "realtime-sync",
		Value:   true,
	}
	body, _ := json.Marshal(payload)

	req, _ := http.NewRequest("POST", "/api/v1/set", bytes.NewBuffer(body))
	req.Method = http.MethodPost
	rr := httptest.NewRecorder()

	handleSetFlag(engine)(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d. Body: %s", rr.Code, rr.Body.String())
	}

	if !engine.GetFlag("inventory", "realtime-sync") {
		t.Error("Engine failed to synchronize volatile memory space following a mutation push")
	}
}

func TestHandleGetFlagsByService(t *testing.T) {
	engine, mr, dbMock := setupTestEngine(t)
	defer mr.Close()
	defer dbMock.Close()

	dbMock.ExpectBegin()
	dbMock.ExpectExec("INSERT INTO public.feature_flags").WithArgs("telemetry", "metrics-v2", true).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	dbMock.ExpectCommit()

	dbMock.ExpectBegin()
	dbMock.ExpectExec("INSERT INTO public.feature_flags").WithArgs("telemetry", "tracing-v1", false).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	dbMock.ExpectCommit()

	engine.SetFlag("telemetry", "metrics-v2", true)
	engine.SetFlag("telemetry", "tracing-v1", false)

	req, _ := http.NewRequest("GET", "/api/v1/get_flags?service=telemetry", nil)
	rr := httptest.NewRecorder()

	handleGetFlagsByService(engine)(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", rr.Code)
	}

	var response map[string]bool
	json.NewDecoder(rr.Body).Decode(&response)

	if len(response) != 2 {
		t.Errorf("Expected 2 payload items inside map layout footprint, got %d", len(response))
	}
	if !response["metrics-v2"] {
		t.Error("Expected key 'metrics-v2' to pass true evaluation mapping target state")
	}
	if response["tracing-v1"] {
		t.Error("Expected key 'tracing-v1' to return false evaluation mapping target state")
	}
}

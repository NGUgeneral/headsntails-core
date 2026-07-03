package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"headsntails-core/config"

	"github.com/alicebob/miniredis/v2"
)

// Helper function to spin up an isolated engine linked to a fresh miniredis instance
func setupTestEngine(t *testing.T) (*Engine, *miniredis.Miniredis) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("Failed to initialize local miniredis: %v", err)
	}

	cfg := &config.Config{
		RedisAddr:    s.Addr(),
		RedisHashKey: "test:feature:flags",
		AppEnv:       "local",
	}

	return NewEngine(cfg), s
}

func TestEngineMutationsAndHydration(t *testing.T) {
	engine, mr := setupTestEngine(t)
	defer mr.Close()

	// 1. Verify Set and Get behaviors through the execution engine
	err := engine.SetFlag("billing", "enable-stripe-v2", true)
	if err != nil {
		t.Fatalf("Unexpected write failure: %v", err)
	}

	if !engine.GetFlag("billing", "enable-stripe-v2") {
		t.Error("Expected billing:enable-stripe-v2 to evaluate to true")
	}

	// 2. Verify state was physically pushed downward into miniredis hash structures
	val := mr.HGet("test:feature:flags", "billing:enable-stripe-v2")
	if val != "true" {
		t.Errorf("Persistence tracking layout mismatch in storage: expected 'true', got %q", val)
	}

	// 3. Verify prefix grouping evaluations
	engine.SetFlag("billing", "discount-v1", false)
	engine.SetFlag("auth", "oauth-enabled", true)

	billingFlags := engine.GetFlagsByService("billing")
	if len(billingFlags) != 2 {
		t.Errorf("Expected 2 billing flags, found %d", len(billingFlags))
	}
	if billingFlags["enable-stripe-v2"] != true || billingFlags["discount-v1"] != false {
		t.Error("Matrix payload mapping corrupted across namespaces")
	}
}

func TestHandleHealthSuccess(t *testing.T) {
	engine, mr := setupTestEngine(t)
	defer mr.Close()

	req, _ := http.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	handler := handleHealth(engine)

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected status code 200, got %d", rr.Code)
	}

	var resp HealthResponse
	json.Unmarshal(rr.Body.Bytes(), &resp)
	if resp.Status != "healthy" || resp.Redis != "connected" {
		t.Errorf("Unexpected payload response: %+v", resp)
	}
}

func TestHandleHealthStorageFailure(t *testing.T) {
	engine, mr := setupTestEngine(t)
	// Force close miniredis immediately to simulate a cluster network partition drop
	mr.Close()

	req, _ := http.NewRequest("GET", "/health", nil)
	rr := httptest.NewRecorder()
	handler := handleHealth(engine)

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("Expected status code 503, got %d", rr.Code)
	}
}

func TestHandleGetFlagValidation(t *testing.T) {
	engine, mr := setupTestEngine(t)
	defer mr.Close()

	engine.SetFlag("shipping", "dhl-tracking", true)

	// Case A: Missing parameters should throw a 400 Bad Request boundary check
	req, _ := http.NewRequest("GET", "/api/v1/get?service=shipping", nil)
	rr := httptest.NewRecorder()
	handleGetFlag(engine)(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for incomplete queries, got %d", rr.Code)
	}

	// Case B: Full parameter mapping verification
	req, _ = http.NewRequest("GET", "/api/v1/get?service=shipping&key=dhl-tracking", nil)
	rr = httptest.NewRecorder()
	handleGetFlag(engine)(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rr.Code)
	}
	expectedOutput := "Service [shipping] Flag [dhl-tracking]: true\n"
	if rr.Body.String() != expectedOutput {
		t.Errorf("Output mismatch: got %q, expected %q", rr.Body.String(), expectedOutput)
	}
}

func TestHandleSetFlagPayloadVerification(t *testing.T) {
	engine, mr := setupTestEngine(t)
	defer mr.Close()

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
	engine, mr := setupTestEngine(t)
	defer mr.Close()

	engine.SetFlag("telemetry", "metrics-v2", true)
	engine.SetFlag("telemetry", "tracing-v1", false)

	req, _ := http.NewRequest("GET", "/api/v1/get_flags?service=telemetry", nil)
	rr := httptest.NewRecorder()

	handleGetFlagsByService(engine)(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rr.Code)
	}

	var res map[string]bool
	json.Unmarshal(rr.Body.Bytes(), &res)

	if len(res) != 2 {
		t.Errorf("Expected 2 flags returned, got %d", len(res))
	}
	if res["metrics-v2"] != true || res["tracing-v1"] != false {
		t.Error("Payload response data matrices are misaligned")
	}
}

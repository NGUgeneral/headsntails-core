package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Dummy ultimate destination handler to verify cascading execution flow
func nextHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	})
}

// ============================================================================
// 1. CORS Middleware Test Suite
// ============================================================================

func TestCORSEnforcer_OptionsPreflight(t *testing.T) {
	req, _ := http.NewRequest(http.MethodOptions, "/any-route", nil)
	rr := httptest.NewRecorder()

	handler := CORSEnforcer(nextHandler())
	handler.ServeHTTP(rr, req)

	// Preflight handshakes must intercept and return 200 OK immediately without calling downstream
	if rr.Code != http.StatusOK {
		t.Errorf("Expected CORS preflight status 200, got %d", rr.Code)
	}
	if rr.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("Missing Access-Control-Allow-Origin header matching wildcard '*'")
	}
}

func TestCORSEnforcer_StandardPassThrough(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/get", nil)
	rr := httptest.NewRecorder()

	handler := CORSEnforcer(nextHandler())
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK || rr.Body.String() != "success" {
		t.Error("CORS failed to forward execution control down the handler stack")
	}
}

// ============================================================================
// 2. Auth Middleware Test Suite
// ============================================================================

func TestAuthMiddleware_MissingHeader(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "/secure", nil)
	rr := httptest.NewRecorder()

	handler := AuthMiddleware([]byte("secret"), "HS256")(nextHandler())
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("Expected 401 for missing authorization string, got %d", rr.Code)
	}
}

func TestAuthMiddleware_ValidTokenContextInjection(t *testing.T) {
	secret := []byte("test_secret_key_abc_123")
	alg := "HS256"

	// Create a valid signed JWT matching the expected CustomClaims model structure
	claims := CustomClaims{
		Type: "access",
		RegisteredClaims: jwt.RegisteredClaims{
			Audience:  jwt.ClaimStrings{"headsntails-platform"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, _ := token.SignedString(secret)

	req, _ := http.NewRequest(http.MethodGet, "/secure", nil)
	req.Header.Set("Authorization", "Bearer "+tokenString)
	rr := httptest.NewRecorder()

	// Verify that the context variable is passed downward to the underlying logic layer
	verifyContextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		aud, ok := r.Context().Value(ContextAudienceKey).(string)
		if !ok || aud != "headsntails-platform" {
			t.Errorf("Context validation token token_audience missing or drifted: %s", aud)
		}
		w.WriteHeader(http.StatusOK)
	})

	handler := AuthMiddleware(secret, alg)(verifyContextHandler)
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected valid authorization token loop to pass, got status %d", rr.Code)
	}
}

// ============================================================================
// 3. Rate Limit Middleware Boundary Test Suite
// ============================================================================

func TestResolveClientIP_ProxyHeaderMatching(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", " 192.168.1.50, 10.0.0.1 ")

	ip := ResolveClientIP(req)
	if ip != "192.168.1.50" {
		t.Errorf("Failed to strip and isolate leftmost edge proxy router IP: expected '192.168.1.50', got %q", ip)
	}
}

func TestRateLimitGuard_MockHTTPClientPassThrough(t *testing.T) {
	// Spin up a fast mock HTTP server that simulates your AWS Lambda Rate Limiter cluster returning an 'allowed' state
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { // <-- FIXED HERE
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":        "allowed",
			"current_count": 1,
			"limit":         10,
			"remaining":     9,
		})
	}))
	defer mockServer.Close()

	// Initialize the custom client client mapped directly to our local mock instance
	client := &RateLimiterClient{
		httpClient:  &http.Client{Timeout: time.Second},
		endpointURL: mockServer.URL,
	}

	req, _ := http.NewRequest(http.MethodGet, "/resource", nil)
	// Inject a dummy token string context so it hits the token limiting path condition block
	ctx := context.WithValue(req.Context(), ContextAudienceKey, "test-client")
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	handler := RateLimitGuard(client, time.Second)(nextHandler())
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected request to pass cleanly, got fallback error code %d", rr.Code)
	}
}

package middleware

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	ratelimitv1 "headsntails-core/proto/ratelimit/v1"
)

// Dummy ultimate destination handler to verify cascading execution flow
func nextHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	})
}

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

func TestResolveClientIP_ProxyHeaderMatching(t *testing.T) {
	req, _ := http.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Forwarded-For", " 192.168.1.50, 10.0.0.1 ")

	ip := ResolveClientIP(req)
	if ip != "192.168.1.50" {
		t.Errorf("Failed to strip and isolate leftmost edge proxy router IP: expected '192.168.1.50', got %q", ip)
	}
}

func TestRateLimitGuard_MockHTTPClientPassThrough(t *testing.T) {
	// Spin up a fast mock HTTP server that simulates your Rate Limiter service returning an 'allowed' state
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	// ─── UPDATED: Instantiate the new HTTP strategy implementation interface wrapper ───
	strategy := NewHTTPRateLimiter(mockServer.URL, time.Second)

	req, _ := http.NewRequest(http.MethodGet, "/resource", nil)
	// Inject a dummy token string context so it hits the token limiting path condition block
	ctx := context.WithValue(req.Context(), ContextAudienceKey, "test-client")
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	// Pass the configured strategy container directly into the unified guard interceptor
	handler := RateLimitGuard(strategy, time.Second)(nextHandler())
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected request to pass cleanly, got fallback error code %d", rr.Code)
	}
}

// MockRateLimiterServer implements the generated gRPC server stub interface
type MockRateLimiterServer struct {
	ratelimitv1.UnimplementedRateLimiterServiceServer
	MockStatus string // "allowed" or "blocked"
}

func (m *MockRateLimiterServer) IsAllowed(ctx context.Context, req *ratelimitv1.IsAllowedRequest) (*ratelimitv1.IsAllowedResponse, error) {
	return &ratelimitv1.IsAllowedResponse{
		Status:       m.MockStatus,
		CurrentCount: 1,
		Limit:        10,
		Remaining:    9,
		Message:      nil,
	}, nil
}

func TestRateLimitGuard_MockGRPCClientPassThrough(t *testing.T) {
	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)
	s := grpc.NewServer()

	// 1. Instantiate our mock server state to return "allowed"
	mockServer := &MockRateLimiterServer{MockStatus: "allowed"}
	ratelimitv1.RegisterRateLimiterServiceServer(s, mockServer)

	// Spin up the listener loop inside a background goroutine
	go func() {
		if err := s.Serve(lis); err != nil && err.Error() != "closed" {
			log.Fatalf("Server exited with error: %v", err)
		}
	}()
	defer s.GracefulStop()

	// 2. Establish a non-blocking modern Client connection over the in-memory buffer channel
	conn, err := grpc.NewClient("passthrough://bufconn",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
	)
	if err != nil {
		t.Fatalf("Failed to dial bufconn: %v", err)
	}
	defer conn.Close()

	// 3. Bind the active channel straight into your GRPCRateLimiter strategy container
	strategy := NewGRPCRateLimiter(conn)

	req, _ := http.NewRequest(http.MethodGet, "/resource", nil)
	// Inject a dummy token string context so it evaluates the token limiting flow paths
	reqCtx := context.WithValue(req.Context(), ContextAudienceKey, "test-grpc-client")
	req = req.WithContext(reqCtx)

	rr := httptest.NewRecorder()

	// Execute the HTTP interceptor using the underlying gRPC strategy backend
	handler := RateLimitGuard(strategy, time.Second)(nextHandler())
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("Expected request to pass cleanly through gRPC validation strategy, got %d", rr.Code)
	}
}

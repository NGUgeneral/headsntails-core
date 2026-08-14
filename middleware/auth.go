package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type contextKey string

const ContextAudienceKey contextKey = "token_audience"

type CustomClaims struct {
	Type string `json:"type"`
	jwt.RegisteredClaims
}

// ────────────────────────────────────────────────────────
// 1. CORE DOMAIN VALIDATOR (Transport-Agnostic)
// ────────────────────────────────────────────────────────

// ValidateAccessToken parses, verifies the signature/algorithm, and ensures
// the provided token is a valid "access" token. Returns the primary audience claim.
func ValidateAccessToken(tokenString string, jwtSecret []byte, expectedAlg string) (string, error) {
	claims := &CustomClaims{}

	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method family: %v", t.Header["alg"])
		}

		if t.Method.Alg() != expectedAlg {
			return nil, fmt.Errorf("unexpected signing method: expected %s, got %s", expectedAlg, t.Method.Alg())
		}

		return jwtSecret, nil
	})

	if err != nil || !token.Valid {
		return "", fmt.Errorf("invalid or expired access token")
	}

	if claims.Type != "access" {
		return "", fmt.Errorf("invalid token context scope")
	}

	audience, err := claims.GetAudience()
	if err != nil || len(audience) == 0 {
		return "", fmt.Errorf("missing identity profile claims")
	}

	return audience[0], nil
}

// ────────────────────────────────────────────────────────
// 2. INGRESS ADAPTERS (HTTP Middleware & gRPC Interceptor)
// ────────────────────────────────────────────────────────

// AuthMiddleware creates the HTTP middleware adapter
func AuthMiddleware(jwtSecret []byte, expectedAlg string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, "Unauthorized: Missing Authorization header", http.StatusUnauthorized)
				return
			}

			parts := strings.Split(authHeader, " ")
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				http.Error(w, "Unauthorized: Malformed Authorization header scheme", http.StatusUnauthorized)
				return
			}

			tokenAudience, err := ValidateAccessToken(parts[1], jwtSecret, expectedAlg)
			if err != nil {
				http.Error(w, fmt.Sprintf("Unauthorized: %s", err.Error()), http.StatusUnauthorized)
				return
			}

			ctx := context.WithValue(r.Context(), ContextAudienceKey, tokenAudience)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// AuthUnaryInterceptor creates the gRPC unary server interceptor adapter
func AuthUnaryInterceptor(jwtSecret []byte, expectedAlg string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "Unauthorized: Missing request metadata")
		}

		authHeaders := md.Get("authorization")
		if len(authHeaders) == 0 || authHeaders[0] == "" {
			return nil, status.Error(codes.Unauthenticated, "Unauthorized: Missing authorization header")
		}

		parts := strings.Split(authHeaders[0], " ")
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			return nil, status.Error(codes.Unauthenticated, "Unauthorized: Malformed authorization header scheme")
		}

		tokenAudience, err := ValidateAccessToken(parts[1], jwtSecret, expectedAlg)
		if err != nil {
			return nil, status.Error(codes.Unauthenticated, fmt.Sprintf("Unauthorized: %s", err.Error()))
		}

		ctxWithAudience := context.WithValue(ctx, ContextAudienceKey, tokenAudience)
		return handler(ctxWithAudience, req)
	}
}

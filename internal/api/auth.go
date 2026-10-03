package api

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/apikey"
	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
)

type contextKey string

const clientNameKey contextKey = "client_name"

// APIKeyStore is the subset of repository needed for authentication.
// Using an interface avoids coupling auth to JobRepository.
type APIKeyStore interface {
	GetAPIKeyByHash(ctx context.Context, hashedKey string) (*repository.APIKey, error)
}

func APIKeyAuth(repo APIKeyStore) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")

			if authHeader == "" {
				WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED", "missing authorization header")
				return
			}

			const prefix = "Bearer "

			if !strings.HasPrefix(authHeader, prefix) {
				WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED",
					"invalid authorization header")
				return
			}
			rawkey := strings.TrimSpace(strings.TrimPrefix(authHeader, prefix))

			if rawkey == "" {
				WriteError(
					w,
					http.StatusUnauthorized,
					"UNAUTHORIZED",
					"invalid authorization header",
				)
				return
			}

			sum := apikey.Hash(rawkey)

			hashKey := sum

			key, err := repo.GetAPIKeyByHash(r.Context(), hashKey)

			if err != nil {
				if errors.Is(err, repository.ErrNotFound) {
					WriteError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid API key")
					return
				}

				WriteError(
					w,
					http.StatusInternalServerError,
					"INTERNAL_SERVER_ERROR",
					"failed to authenticate request",
				)
				return
			}

			if key.RevokedAt != nil {
				WriteError(
					w,
					http.StatusUnauthorized,
					"UNAUTHORIZED",
					"API key has been revoked",
				)
				return
			}

			ctx := context.WithValue(r.Context(), clientNameKey, key.ClientName)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func ClientNameFromContext(ctx context.Context) (string, bool) {
	clientName, ok := ctx.Value(clientNameKey).(string)
	return clientName, ok
}

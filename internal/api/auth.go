package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/BurhaanAshraf/job-scheduler-platform/internal/repository"
)

type contextKey string

const clientNameKey contextKey = "client_name"

func APIKeyAuth(repo *repository.JobRepository) func(http.Handler) http.Handler {
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

			sum := sha256.Sum256([]byte(rawkey))

			hashKey := hex.EncodeToString(sum[:])

			key, err := repo.GetAPIKeyByHash(r.Context(), hashKey)

			if err != nil {
				if errors.Is(err, repository.ErrNotFound) {
					WriteError(w, http.StatusUnauthorized, "UNATHORIZED", "invalid API key")
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

package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

func WriteError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	response := ErrorResponse{
		Error: ErrorDetail{
			Code:    code,
			Message: message,
		},
	}

	err := json.NewEncoder(w).Encode(response)
	if err != nil {
		slog.Error("failed to encode error response", "error", err)
	}
}

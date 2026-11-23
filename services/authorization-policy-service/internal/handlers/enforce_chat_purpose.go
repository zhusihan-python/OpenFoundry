package handlers

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"slices"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
)

// EnforceChatPurpose is a service-to-service operation. Only the dedicated
// scope delegates authority to identify the actor and selected network.
func (h *Handlers) EnforceChatPurpose(w http.ResponseWriter, r *http.Request) {
	caller, ok := authmw.FromContext(r.Context())
	if !ok {
		writeJSONErr(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if caller.Sub == uuid.Nil || caller.IsGuestSession() || !slices.Contains(caller.AuthMethods, "service") || !slices.Contains(caller.Permissions, "purpose-checkpoint:enforce") {
		writeJSONErr(w, http.StatusForbidden, "purpose checkpoint service permission required")
		return
	}
	var req authmw.PurposeCheckpointRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10))
	if err := decoder.Decode(&req); err != nil || decoder.Decode(&json.RawMessage{}) != io.EOF || req.ActorID == nil || *req.ActorID == uuid.Nil || req.InteractionType != "ai_chat_completion" {
		writeJSONErr(w, http.StatusBadRequest, "a chat interaction and authenticated actor are required")
		return
	}
	if h.Repo == nil || h.Repo.Pool == nil {
		writeJSONErr(w, http.StatusServiceUnavailable, "purpose checkpoint unavailable")
		return
	}
	result, err := h.Repo.EnforceChatPurpose(r.Context(), caller.Sub, req)
	if err != nil {
		slog.ErrorContext(r.Context(), "purpose checkpoint unavailable", slog.String("error", err.Error()))
		writeJSONErr(w, http.StatusServiceUnavailable, "purpose checkpoint unavailable")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

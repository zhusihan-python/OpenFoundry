package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/stretchr/testify/require"
)

func TestEnforcePurposeRequiresScopedService(t *testing.T) {
	sub := uuid.New()
	for _, tc := range []struct {
		name   string
		claims *authmw.Claims
		status int
	}{
		{"unauthenticated", nil, 401},
		{"ordinary user with permission", &authmw.Claims{Sub: sub, Permissions: []string{"purpose-checkpoint:enforce"}}, 403},
		{"service without permission", &authmw.Claims{Sub: sub, AuthMethods: []string{"service"}}, 403},
		{"service admin without explicit scope", &authmw.Claims{Sub: sub, Roles: []string{"admin"}, AuthMethods: []string{"service"}}, 403},
		{"zero subject", &authmw.Claims{Permissions: []string{"purpose-checkpoint:enforce"}, AuthMethods: []string{"service"}}, 403},
		{"missing authenticated actor", &authmw.Claims{Sub: sub, Permissions: []string{"purpose-checkpoint:enforce"}, AuthMethods: []string{"service"}}, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, authmw.PurposeCheckpointEnforcePath, strings.NewReader(`{"interaction_type":"ai_chat_completion"}`))
			if tc.claims != nil {
				req = req.WithContext(authmw.ContextWithClaims(req.Context(), tc.claims))
			}
			w := httptest.NewRecorder()
			(&Handlers{}).EnforceChatPurpose(w, req)
			require.Equal(t, tc.status, w.Code)
		})
	}
}

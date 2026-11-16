package handler

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
)

// TenantScope is the trusted tenant projection derived exclusively from the
// validated JWT claims attached by the auth middleware. It never consults
// request headers or bodies, so a caller cannot widen its own visibility.
//
// Semantics (P1.1 tenant isolation):
//
//   - TenantID != nil  → organization domain: only rows with the same
//     tenant_id are visible. Admins get no cross-tenant pass.
//   - TenantID == nil  → personal domain: only rows with tenant_id NULL
//     *and* owned by Subject are visible (legacy pre-tenant rows).
type TenantScope struct {
	TenantID *uuid.UUID
	Subject  uuid.UUID
}

// TenantScopeFromClaims projects validated claims into a TenantScope.
// A nil scope is returned when claims are absent so internal/background
// callers can opt in explicitly via ContextWithTenantScope instead.
func TenantScopeFromClaims(claims *authmw.Claims) *TenantScope {
	if claims == nil {
		return nil
	}
	return &TenantScope{TenantID: claims.OrgID, Subject: claims.Sub}
}

type tenantScopeKey struct{}

// ContextWithTenantScope attaches an explicit tenant scope to ctx. It takes
// precedence over claims-derived scopes when repository code resolves the
// effective scope (used by background workers that act per tenant without a
// user JWT).
func ContextWithTenantScope(ctx context.Context, scope *TenantScope) context.Context {
	if scope == nil {
		return ctx
	}
	return context.WithValue(ctx, tenantScopeKey{}, scope)
}

// TenantScopeFromContext returns the explicit scope attached by
// ContextWithTenantScope, falling back to a claims-derived scope.
func TenantScopeFromContext(ctx context.Context) *TenantScope {
	if scope, ok := ctx.Value(tenantScopeKey{}).(*TenantScope); ok && scope != nil {
		return scope
	}
	if claims, ok := authmw.FromContext(ctx); ok {
		return TenantScopeFromClaims(claims)
	}
	return nil
}

// TenantScopeMiddleware derives the caller's tenant scope from the validated
// JWT claims and attaches it to the request context. Mounted once per
// authenticated route group so every downstream repository call sees the
// same boundary. Unauthenticated requests pass through unchanged (the auth
// middleware already rejects them on protected groups).
func TenantScopeMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if claims, ok := authmw.FromContext(r.Context()); ok {
				r = r.WithContext(ContextWithTenantScope(r.Context(), TenantScopeFromClaims(claims)))
			}
			next.ServeHTTP(w, r)
		})
	}
}

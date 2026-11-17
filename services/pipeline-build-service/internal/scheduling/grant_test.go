package scheduling

import (
	"testing"
	"time"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
)

func TestTokenCipherRoundTrip(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}
	c, err := NewTokenCipher(key)
	if err != nil {
		t.Fatalf("NewTokenCipher: %v", err)
	}
	sealed, err := c.Encrypt([]byte("bearer-token"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	opened, err := c.Decrypt(sealed)
	if err != nil {
		t.Fatalf("Decrypt: %v", err)
	}
	if string(opened) != "bearer-token" {
		t.Fatalf("round trip mismatch: %q", opened)
	}
	// Tampered ciphertext fails closed without leaking material.
	if _, err := c.Decrypt(append(sealed[:0:0], append(sealed, 'x')...)); err == nil {
		t.Fatal("tampered ciphertext must fail")
	}
}

func TestTokenCipherKeyShape(t *testing.T) {
	if _, err := NewTokenCipher(make([]byte, 16)); err == nil {
		t.Fatal("16-byte key must be rejected")
	}
	if _, err := NewTokenCipher(nil); err == nil {
		t.Fatal("nil key must be rejected")
	}
}

func mintToken(t *testing.T, claims *authmw.Claims) (string, *authmw.JWTConfig) {
	t.Helper()
	cfg := authmw.NewJWTConfig("test-secret-material-for-grants-only")
	signed, err := authmw.EncodeToken(cfg, claims)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signed, cfg
}

func TestValidateGrantTokenOrgDomain(t *testing.T) {
	org := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	sub := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	token, cfg := mintToken(t, &authmw.Claims{Sub: sub, OrgID: &org, EXP: time.Now().Add(time.Hour).Unix()})
	id, err := ValidateGrantToken(cfg, token, time.Now())
	if err != nil {
		t.Fatalf("ValidateGrantToken: %v", err)
	}
	if id.Tenant.Kind != scheduleevents.TenantKindOrg || id.Tenant.ID != org {
		t.Fatalf("org domain derivation: %+v", id.Tenant)
	}
	if id.OrgID == nil || *id.OrgID != org {
		t.Fatal("org identity must carry org id")
	}
	sc := id.ScopeForExecution()
	if sc.Tenant.Kind != scheduleevents.TenantKindOrg || sc.Subject != sub {
		t.Fatalf("execution scope: %+v", sc)
	}
}

func TestValidateGrantTokenPersonalDomain(t *testing.T) {
	sub := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	token, cfg := mintToken(t, &authmw.Claims{Sub: sub, EXP: time.Now().Add(time.Hour).Unix()})
	id, err := ValidateGrantToken(cfg, token, time.Now())
	if err != nil {
		t.Fatalf("ValidateGrantToken: %v", err)
	}
	if id.Tenant.Kind != scheduleevents.TenantKindPersonal || id.Tenant.ID != sub {
		t.Fatalf("personal domain derivation: %+v", id.Tenant)
	}
	if id.OrgID != nil {
		t.Fatal("personal identity must carry no org id")
	}
}

func TestValidateGrantTokenRejections(t *testing.T) {
	sub := uuid.MustParse("44444444-4444-4444-4444-444444444444")
	org := uuid.MustParse("33333333-3333-3333-3333-333333333333")

	// Expired token.
	token, cfg := mintToken(t, &authmw.Claims{Sub: sub, EXP: time.Now().Add(time.Hour).Unix()})
	if _, err := ValidateGrantToken(cfg, token, time.Now().Add(2*time.Hour)); err == nil {
		t.Fatal("expired grant token must be rejected")
	}
	// Wrong signature.
	if _, err := ValidateGrantToken(authmw.NewJWTConfig("another-secret"), token, time.Now()); err == nil {
		t.Fatal("bad signature must be rejected")
	}
	// Nil sub.
	badSub, cfg2 := mintToken(t, &authmw.Claims{Sub: uuid.Nil, OrgID: &org, EXP: time.Now().Add(time.Hour).Unix()})
	if _, err := ValidateGrantToken(cfg2, badSub, time.Now()); err == nil {
		t.Fatal("nil sub must be rejected")
	}
	// exp == 0 (epoch): structurally present but meaningless — the
	// explicit claims.EXP==0 guard must reject it even though the JWT
	// library itself only rejects past dates.
	epoch := &authmw.Claims{Sub: sub, OrgID: &org, EXP: 0}
	noExp, cfg3 := mintToken(t, epoch)
	if _, err := ValidateGrantToken(cfg3, noExp, time.Now()); err == nil {
		t.Fatal("exp==0 token must be rejected")
	}

	// OrgID present but the NIL uuid: rejected (zero-org guard).
	zeroOrg := uuid.Nil
	zeroToken, cfg4 := mintToken(t, &authmw.Claims{Sub: sub, OrgID: &zeroOrg, EXP: time.Now().Add(time.Hour).Unix()})
	if _, err := ValidateGrantToken(cfg4, zeroToken, time.Now()); err == nil {
		t.Fatal("nil-uuid org ticket must be rejected")
	}
}

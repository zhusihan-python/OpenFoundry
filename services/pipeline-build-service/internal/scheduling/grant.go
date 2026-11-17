// Package scheduling implements the T8 pipeline-scheduling consumer
// and its bounded, revocable grant store (T8 decision §4):
// deterministic envelope admission, fenced execution, and AES-256-GCM
// encrypted caller tokens that never enter Kafka, the outbox, logs, or
// public JSON.
package scheduling

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
)

// TokenCipher encrypts/decrypts grant caller tokens with AES-256-GCM
// under the dedicated SCHEDULER_CREDENTIAL_ENCRYPTION_KEY. Errors are
// sanitized: no key or plaintext material ever reaches an error string.
type TokenCipher struct {
	aead cipher.AEAD
}

// NewTokenCipher builds a TokenCipher from the raw 32-byte key.
func NewTokenCipher(key []byte) (*TokenCipher, error) {
	if len(key) != 32 {
		return nil, errors.New("scheduling: encryption key must be exactly 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, errors.New("scheduling: init cipher")
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, errors.New("scheduling: init gcm")
	}
	return &TokenCipher{aead: aead}, nil
}

// Encrypt seals plaintext; output is nonce||ciphertext.
func (c *TokenCipher) Encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, errors.New("scheduling: nonce")
	}
	return c.aead.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt opens nonce||ciphertext.
func (c *TokenCipher) Decrypt(sealed []byte) ([]byte, error) {
	ns := c.aead.NonceSize()
	if len(sealed) < ns {
		return nil, errors.New("scheduling: ciphertext too short")
	}
	out, err := c.aead.Open(nil, sealed[:ns], sealed[ns:], nil)
	if err != nil {
		return nil, errors.New("scheduling: decrypt failed")
	}
	return out, nil
}

// GrantIdentity is the derived tenant + subject of a stored grant
// token. Domain is derived from VALIDATED claims only — callers never
// accept a domain from the request body.
type GrantIdentity struct {
	Tenant  scheduleevents.TenantRef
	Subject uuid.UUID
	OrgID   *uuid.UUID // org domain: same as Tenant.ID; personal: nil
	EXP     time.Time
}

// ValidateGrantToken verifies the caller JWT with the SAME verifier the
// HTTP auth middleware uses, then derives the tenant domain and
// enforces the ingest rules (T8 decision §3): non-nil sub, exp present
// and in the future, and — for org tokens — OrgID present. Personal
// tokens MUST carry no OrgID (a subject switching orgs can never reuse
// a personal grant).
func ValidateGrantToken(cfg *authmw.JWTConfig, token string, now time.Time) (*GrantIdentity, error) {
	claims, err := authmw.DecodeToken(cfg, token)
	if err != nil {
		return nil, fmt.Errorf("scheduling: grant token invalid: %w", err)
	}
	if claims.Sub == uuid.Nil {
		return nil, errors.New("scheduling: grant token has nil sub")
	}
	if claims.EXP == 0 {
		return nil, errors.New("scheduling: grant token has no exp")
	}
	exp := time.Unix(claims.EXP, 0).UTC()
	if !exp.After(now) {
		return nil, errors.New("scheduling: grant token expired")
	}
	if claims.OrgID != nil {
		id := *claims.OrgID
		if id == uuid.Nil {
			return nil, errors.New("scheduling: grant token org id is nil uuid")
		}
		return &GrantIdentity{
			Tenant:  scheduleevents.TenantRef{Kind: scheduleevents.TenantKindOrg, ID: id},
			Subject: claims.Sub, OrgID: &id, EXP: exp,
		}, nil
	}
	return &GrantIdentity{
		Tenant:  scheduleevents.TenantRef{Kind: scheduleevents.TenantKindPersonal, ID: claims.Sub},
		Subject: claims.Sub, OrgID: nil, EXP: exp,
	}, nil
}

// ScopeForExecution maps a grant identity onto the repository tenant
// scope per tenancy.go's rules: org -> TenantID=&org; personal ->
// TenantID=nil (the subject must NEVER be written into the TenantID
// pointer — it would impersonate an org).
func (g *GrantIdentity) ScopeForExecution() *ExecutionScope {
	return &ExecutionScope{
		Tenant:  g.Tenant,
		Subject: g.Subject,
	}
}

// ExecutionScope is the tenant/subject pair the consumer executes a
// dispatched pipeline under.
type ExecutionScope struct {
	Tenant  scheduleevents.TenantRef
	Subject uuid.UUID
}

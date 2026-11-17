package config

import (
	"encoding/base64"
	"os"
	"strings"
	"testing"
)

func b64key() string { return base64.StdEncoding.EncodeToString(make([]byte, 32)) }

func TestSchedulerDisabledByDefault(t *testing.T) {
	os.Unsetenv("SCHEDULER_CONSUMER_ENABLED")
	c, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv: %v", err)
	}
	if c.Scheduler.Enabled {
		t.Fatal("scheduler must default to off")
	}
}

func TestSchedulerEnabledKeyDiscipline(t *testing.T) {
	os.Setenv("SCHEDULER_CONSUMER_ENABLED", "true")
	os.Setenv("SCHEDULER_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
	os.Setenv("SCHEDULER_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
	t.Cleanup(func() {
		os.Unsetenv("SCHEDULER_CONSUMER_ENABLED")
		os.Unsetenv("SCHEDULER_KAFKA_BOOTSTRAP_SERVERS")
		os.Unsetenv("SCHEDULER_KAFKA_SECURITY_PROTOCOL")
		os.Unsetenv("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY")
		os.Unsetenv("JWT_SECRET")
	})

	// Missing key: rejected, no fallback of any kind.
	os.Unsetenv("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY")
	os.Unsetenv("JWT_SECRET")
	if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), "must decode to 32 bytes") {
		t.Fatalf("missing key must be rejected: %v", err)
	}

	// Valid base64 32-byte key accepted (independent of JWT secret).
	jwtSecret := strings.Repeat("k", 32) // exactly 32 bytes
	os.Setenv("JWT_SECRET", jwtSecret)
	os.Setenv("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY", b64key())
	c, err := FromEnv()
	if err != nil {
		t.Fatalf("valid key: %v", err)
	}
	if len(c.Scheduler.CredentialEncryptionKey) != 32 {
		t.Fatal("decoded key must be 32 bytes")
	}

	// Key equal to the raw JWT secret rejected.
	os.Setenv("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString([]byte(jwtSecret)))
	if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), "independent of JWT_SECRET") {
		t.Fatalf("raw-secret key must be rejected: %v", err)
	}

	// Wrong length rejected.
	os.Setenv("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY", base64.StdEncoding.EncodeToString(make([]byte, 16)))
	if _, err := FromEnv(); err == nil {
		t.Fatal("16-byte key must be rejected")
	}
}

func TestSchedulerAttemptsRangeAndStrictParse(t *testing.T) {
	os.Setenv("SCHEDULER_CONSUMER_ENABLED", "true")
	os.Setenv("SCHEDULER_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
	os.Setenv("SCHEDULER_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
	os.Setenv("JWT_SECRET", "s")
	os.Setenv("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY", b64key())
	t.Cleanup(func() {
		os.Unsetenv("SCHEDULER_CONSUMER_ENABLED")
		os.Unsetenv("SCHEDULER_KAFKA_BOOTSTRAP_SERVERS")
		os.Unsetenv("SCHEDULER_KAFKA_SECURITY_PROTOCOL")
		os.Unsetenv("SCHEDULER_CREDENTIAL_ENCRYPTION_KEY")
		os.Unsetenv("SCHEDULER_LEASE_TTL")
		os.Unsetenv("SCHEDULER_MAX_EVENT_ATTEMPTS")
		os.Unsetenv("JWT_SECRET")
	})

	os.Setenv("SCHEDULER_MAX_EVENT_ATTEMPTS", "6")
	if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), "between 1 and 5") {
		t.Fatalf("attempts>5 must be rejected: %v", err)
	}
	os.Setenv("SCHEDULER_MAX_EVENT_ATTEMPTS", "5")
	if _, err := FromEnv(); err != nil {
		t.Fatalf("attempts=5 must pass: %v", err)
	}

	// Typo'd numeric value fails CLOSED when enabled.
	os.Setenv("SCHEDULER_LEASE_TTL", "12O")
	if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), "SCHEDULER_LEASE_TTL") {
		t.Fatalf("malformed number must fail closed: %v", err)
	}
	// Same typo with the consumer disabled falls back silently.
	os.Unsetenv("SCHEDULER_CONSUMER_ENABLED")
	if _, err := FromEnv(); err != nil {
		t.Fatalf("disabled consumer must tolerate malformed numbers: %v", err)
	}
}

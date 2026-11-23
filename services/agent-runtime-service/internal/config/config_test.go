package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFromEnvAllowFakeLLMProviderDefaultsFalse(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("JWT_SECRET", "secret")

	cfg, err := FromEnv()

	require.NoError(t, err)
	assert.False(t, cfg.AllowFakeLLMProvider)
}

func TestFromEnvAllowFakeLLMProviderCanBeEnabled(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("JWT_SECRET", "secret")
	t.Setenv("ALLOW_FAKE_LLM_PROVIDER", "true")

	cfg, err := FromEnv()

	require.NoError(t, err)
	assert.True(t, cfg.AllowFakeLLMProvider)
}

func TestConfiguredChatProviderRequiresEndpointAndModel(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("JWT_SECRET", "secret")
	t.Setenv("LLM_PROVIDER_MODEL", "sample")
	t.Setenv("LLM_PROVIDER_BASE_URL", "")
	_, err := FromEnv()
	require.ErrorContains(t, err, "LLM_PROVIDER_BASE_URL")
	t.Setenv("LLM_PROVIDER_BASE_URL", "not-an-endpoint")
	_, err = FromEnv()
	require.ErrorContains(t, err, "absolute HTTP")
	t.Setenv("LLM_PROVIDER_BASE_URL", "http://inference:8000/v1")
	t.Setenv("AUTHORIZATION_POLICY_SERVICE_URL", "")
	t.Setenv("PURPOSE_CHECKPOINT_URL", "")
	_, err = FromEnv()
	require.ErrorContains(t, err, "AUTHORIZATION_POLICY_SERVICE_URL")
	t.Setenv("AUTHORIZATION_POLICY_SERVICE_URL", "http://policy")
	t.Setenv("PURPOSE_CHECKPOINT_BEARER", "")
	_, err = FromEnv()
	require.ErrorContains(t, err, "PURPOSE_CHECKPOINT_BEARER")
	t.Setenv("PURPOSE_CHECKPOINT_BEARER", "service-token")
	t.Setenv("LLM_PROVIDER_NETWORK_SCOPE", "invalid-scope")
	_, err = FromEnv()
	require.ErrorContains(t, err, "LLM_PROVIDER_NETWORK_SCOPE")
	t.Setenv("LLM_PROVIDER_NETWORK_SCOPE", "local")
	cfg, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, "local", cfg.ChatProvider.RouteRules.NetworkScope)
	require.Equal(t, "sample", cfg.ChatProvider.ModelName)
	require.Equal(t, "http://inference:8000/v1", cfg.ChatProvider.EndpointURL)
	require.Equal(t, "chat_completions", cfg.ChatProvider.APIMode)
	t.Setenv("LLM_PROVIDER_MODEL", "")
	_, err = FromEnv()
	require.ErrorContains(t, err, "LLM_PROVIDER_MODEL")
}

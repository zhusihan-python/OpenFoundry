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
	cfg, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, "sample", cfg.ChatProvider.ModelName)
	require.Equal(t, "http://inference:8000/v1", cfg.ChatProvider.EndpointURL)
	require.Equal(t, "chat_completions", cfg.ChatProvider.APIMode)
	t.Setenv("LLM_PROVIDER_MODEL", "")
	_, err = FromEnv()
	require.ErrorContains(t, err, "LLM_PROVIDER_MODEL")
}

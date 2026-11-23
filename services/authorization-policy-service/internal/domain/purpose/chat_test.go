package purpose

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/authorization-policy-service/internal/models"
)

func TestEvaluateChatPurpose(t *testing.T) {
	policy := models.CheckpointPolicy{Slug: "ai-private-network", InteractionType: "ai_chat_completion", EnforcementMode: "require_justification", Rules: json.RawMessage(`[{"key":"require_private_network","expected":"true"},{"key":"minimum_justification_length","expected":"20"}]`), Prompts: json.RawMessage(`["why"]`)}
	for _, tc := range []struct {
		name, network, purpose, status string
		requestedPrivate               bool
	}{
		{"trusted route and independent purpose", "local", strings.Repeat("订", 20), "approved", false},
		{"caller flag insufficient", "public", strings.Repeat("订", 20), "denied", true},
		{"unicode codepoints", "hybrid", strings.Repeat("订", 19), "pending_justification", true},
		{"whitespace", "private", "     ", "pending_justification", true},
		{"bounded justification", "private", strings.Repeat("x", 1025), "denied", true},
		{"unknown network", "anything", strings.Repeat("x", 20), "denied", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev, _ := json.Marshal(map[string]string{"network_scope": tc.network, "operation": "chat/completions", "raw_prompt": "DO_NOT_RETAIN"})
			request := authmw.PurposeCheckpointRequest{InteractionType: "ai_chat_completion", PurposeJustification: &tc.purpose, RequestedPrivateNetwork: tc.requestedPrivate, Evidence: ev}
			got, evidence, err := EvaluateChatPurpose(policy, request)
			require.NoError(t, err)
			require.Equal(t, tc.status, got.Status)
			require.Equal(t, tc.status == "approved", got.Approved)
			b, err := json.Marshal(evidence)
			require.NoError(t, err)
			require.NotContains(t, string(b), "DO_NOT_RETAIN")
		})
	}
	for _, rules := range []string{`[{"key":"ignore_me","expected":"true"}]`, `[]`, `[{"key":"require_private_network","expected":"false"}]`, `null`, `[{"key":"minimum_justification_length","expected":"1"},{"key":"minimum_justification_length","expected":"2"}]`} {
		bad := policy
		bad.Rules = json.RawMessage(rules)
		_, _, err := EvaluateChatPurpose(bad, authmw.PurposeCheckpointRequest{})
		require.Error(t, err)
	}
}

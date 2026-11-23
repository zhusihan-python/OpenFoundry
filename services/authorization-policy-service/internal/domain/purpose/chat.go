// Package purpose evaluates the policy catalog used by chat purpose checkpoints.
package purpose

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/authorization-policy-service/internal/models"
)

const maxPurposeLength = 1024

// Evidence contains only selected routing facts and derived values. In
// particular request tags, provider URLs, prompts and arbitrary metadata
// must never be copied into an audit event.
type Evidence struct {
	Operation               string `json:"operation"`
	NetworkScope            string `json:"network_scope"`
	RequestedPrivateNetwork bool   `json:"requested_private_network"`
	JustificationLength     int    `json:"justification_length"`
	JustificationSHA256     string `json:"justification_sha256,omitempty"`
	Reason                  string `json:"reason"`
	// The justification is saved only in the access-controlled purpose ledger.
	Justification *string `json:"-"`
}

func EvaluateChatPurpose(policy models.CheckpointPolicy, req authmw.PurposeCheckpointRequest) (authmw.PurposeCheckpointEvaluation, Evidence, error) {
	var result authmw.PurposeCheckpointEvaluation
	var facts Evidence
	if policy.InteractionType != "ai_chat_completion" || policy.EnforcementMode != "require_justification" {
		return result, facts, errors.New("unsupported chat purpose policy")
	}
	minimum, err := justificationMinimum(policy.Rules)
	if err != nil {
		return result, facts, err
	}
	if err := json.Unmarshal(policy.Prompts, &result.RequiredPrompts); err != nil || len(result.RequiredPrompts) == 0 {
		return result, facts, errors.New("invalid chat purpose prompts")
	}

	// These two fields originate from the authenticated runtime's operation
	// and selected provider. All other unstructured evidence is discarded.
	var routing struct {
		Operation    string `json:"operation"`
		NetworkScope string `json:"network_scope"`
	}
	if len(req.Evidence) > 0 {
		if err := json.Unmarshal(req.Evidence, &routing); err != nil {
			routing.Operation = ""
			routing.NetworkScope = ""
		}
	}
	switch routing.Operation {
	case "chat/completions", "copilot/ask":
		facts.Operation = routing.Operation
	default:
		facts.Operation = "unknown"
	}
	switch scope := strings.ToLower(strings.TrimSpace(routing.NetworkScope)); scope {
	case "private", "hybrid", "local", "public":
		facts.NetworkScope = scope
	default:
		facts.NetworkScope = "unknown"
	}
	facts.RequestedPrivateNetwork = req.RequestedPrivateNetwork
	if req.PurposeJustification != nil {
		value := strings.TrimSpace(*req.PurposeJustification)
		facts.JustificationLength = utf8.RuneCountInString(value)
		if value != "" {
			digest := sha256.Sum256([]byte(value))
			facts.JustificationSHA256 = hex.EncodeToString(digest[:])
			if facts.JustificationLength <= maxPurposeLength {
				facts.Justification = &value
			}
		}
	}
	result.PolicySlug = &policy.Slug
	result.Status = "denied"
	switch {
	case facts.Operation == "unknown":
		facts.Reason = "unsupported runtime operation"
	case req.RequiresApproval:
		facts.Reason = "a separate approval is required"
	case facts.NetworkScope == "public" || facts.NetworkScope == "unknown":
		facts.Reason = "the policy requires a private provider route"
	case facts.JustificationLength > maxPurposeLength:
		facts.Reason = fmt.Sprintf("purpose justification exceeds %d characters", maxPurposeLength)
	case facts.JustificationLength < minimum:
		result.Status = "pending_justification"
		facts.Reason = fmt.Sprintf("purpose justification must contain at least %d characters", minimum)
	default:
		result.Approved = true
		result.Status = "approved"
		result.RequiredPrompts = []string{}
		facts.Reason = "private provider route and purpose justification verified"
	}
	result.Reason = &facts.Reason
	return result, facts, nil
}

// The shipped policy contains both constraints. A new rule, weakened rule
// shape or duplicate must never accidentally create an allow policy.
func justificationMinimum(raw json.RawMessage) (int, error) {
	var rules []struct {
		Key      string `json:"key"`
		Expected string `json:"expected"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&rules); err != nil || len(rules) != 2 {
		return 0, errors.New("invalid chat purpose rules")
	}
	private := false
	minimum := 0
	for _, rule := range rules {
		switch rule.Key {
		case "require_private_network":
			private = rule.Expected == "true"
		case "minimum_justification_length":
			n, err := strconv.Atoi(rule.Expected)
			if err != nil || n <= 0 || n > maxPurposeLength {
				return 0, errors.New("invalid purpose length rule")
			}
			minimum = n
		default:
			return 0, errors.New("unsupported chat purpose rule")
		}
	}
	if !private || minimum == 0 {
		return 0, errors.New("incomplete chat purpose rules")
	}
	return minimum, nil
}

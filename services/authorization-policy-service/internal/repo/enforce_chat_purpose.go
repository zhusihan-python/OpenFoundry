package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	audittrail "github.com/openfoundry/openfoundry-go/libs/audit-trail"
	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/libs/outbox"
	"github.com/openfoundry/openfoundry-go/services/authorization-policy-service/internal/domain/purpose"
	"github.com/openfoundry/openfoundry-go/services/authorization-policy-service/internal/models"
)

// EnforceChatPurpose locks the active catalog rows through the commit of the
// ledger receipt and audit event. No approval is visible on a failed commit.
func (r *Repo) EnforceChatPurpose(ctx context.Context, caller uuid.UUID, req authmw.PurposeCheckpointRequest) (authmw.PurposeCheckpointEvaluation, error) {
	var result authmw.PurposeCheckpointEvaluation
	err := pgx.BeginFunc(ctx, r.Pool, func(tx pgx.Tx) error {
		var policy models.CheckpointPolicy
		err := tx.QueryRow(ctx, `
			SELECT p.slug,p.interaction_type,p.enforcement_mode,p.prompts,p.rules
			FROM sensitive_interaction_configs c
			JOIN checkpoint_policies p ON p.slug=c.linked_policy_slug
			    AND p.interaction_type=c.interaction_type AND p.sensitivity=c.sensitivity
			WHERE c.interaction_type=$1 AND c.require_purpose_justification AND c.require_auditable_record
			FOR SHARE OF c,p`, req.InteractionType).Scan(&policy.Slug, &policy.InteractionType, &policy.EnforcementMode, &policy.Prompts, &policy.Rules)
		if err != nil {
			return fmt.Errorf("load chat purpose policy: %w", err)
		}
		evaluation, evidence, err := purpose.EvaluateChatPurpose(policy, req)
		if err != nil {
			return err
		}
		recordID := uuid.New()
		evidenceJSON, err := json.Marshal(evidence)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `INSERT INTO purpose_records
			(id,interaction_type,actor_id,enforced_by,purpose_justification,status,policy_slug,evidence)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, recordID, req.InteractionType, req.ActorID, caller, evidence.Justification, evaluation.Status, policy.Slug, evidenceJSON)
		if err != nil {
			return fmt.Errorf("persist chat purpose receipt: %w", err)
		}
		evaluation.RecordID = recordID
		if err := emitPurposeDecision(ctx, tx, evaluation, caller, *req.ActorID, evidence); err != nil {
			return fmt.Errorf("emit chat purpose decision: %w", err)
		}
		result = evaluation
		return nil
	})
	if err != nil {
		return authmw.PurposeCheckpointEvaluation{}, err
	}
	return result, nil
}

func emitPurposeDecision(ctx context.Context, tx pgx.Tx, decision authmw.PurposeCheckpointEvaluation, caller, actor uuid.UUID, evidence purpose.Evidence) error {
	const kind = "purpose_checkpoint.evaluated"
	rid := decision.RecordID.String()
	at := time.Now().UTC()
	payload, err := json.Marshal(map[string]any{
		"kind": kind, "record_id": rid, "enforced_by": caller, "actor_id": actor, "policy_slug": decision.PolicySlug,
		"status": decision.Status, "evidence": evidence,
	})
	if err != nil {
		return err
	}
	env := audittrail.AuditEnvelope{
		EventID: audittrail.DeriveEventID(kind, rid, rid), Kind: kind, ResourceRID: rid, ActorID: actor.String(),
		SourceService: "authorization-policy-service", At: at.UnixMicro(), OccurredAt: at,
		Categories: []string{string(audittrail.CategoryDataCreate)}, MarkingsAtEvent: []string{}, Payload: payload,
	}
	event, err := audittrail.ToOutboxEvent(env)
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, tx, event)
}

package scheduling

import (
	"context"
	"errors"
	"github.com/google/uuid"
	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	scheduleevents "github.com/openfoundry/openfoundry-go/libs/schedule-events"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/postgres"
	"testing"
	"time"
)

type contextRunner struct {
	calls int
	value any
}

func (r *contextRunner) Run(c context.Context, n executor.NodeContext) (executor.NodeResult, error) {
	r.calls++
	r.value = c.Value("bearer")
	return executor.NodeResult{}, nil
}
func TestNodeGateRefusesBeforeExecution(t *testing.T) {
	r := &contextRunner{}
	denied := errors.New("revoked")
	_, err := (gatedNodeRunner{runner: r, gate: func(context.Context) error { return denied }}).Run(context.Background(), executor.NodeContext{})
	if !errors.Is(err, denied) || r.calls != 0 {
		t.Fatal("authorization gate allowed execution")
	}
}
func TestNodeGatePreservesContextAndChecksResult(t *testing.T) {
	r := &contextRunner{}
	calls := 0
	denied := errors.New("lost lease")
	_, err := (gatedNodeRunner{runner: r, gate: func(context.Context) error {
		calls++
		if calls == 2 {
			return denied
		}
		return nil
	}}).Run(context.WithValue(context.Background(), "bearer", "token"), executor.NodeContext{})
	if !errors.Is(err, denied) || r.calls != 1 || r.value != "token" {
		t.Fatal("result fence or caller context lost")
	}
}

type recoveringGrantStore struct {
	RunnerStore
	grant   *postgres.SchedulingGrant
	reads   int
	subject uuid.UUID
}

func (s *recoveringGrantStore) GetGrantByRef(context.Context, uuid.UUID) (*postgres.SchedulingGrant, error) {
	s.reads++
	if s.reads == 1 {
		return nil, errors.New("transient database read")
	}
	return s.grant, nil
}
func (s *recoveringGrantStore) LoadDefinitionBinding(context.Context, scheduleevents.TenantRef, string) (postgres.DefinitionBinding, error) {
	return postgres.DefinitionBinding{Found: true, Enabled: true, DefinitionRevision: 1, PipelineRID: "pipeline", BuildBranch: "main", GrantRef: s.grant.ID}, nil
}
func (s *recoveringGrantStore) LoadDispatchExecutionState(context.Context, uuid.UUID) (*postgres.DispatchExecutionSnapshot, error) {
	return &postgres.DispatchExecutionSnapshot{}, nil
}
func (s *recoveringGrantStore) ListPipelineOutputDatasets(ctx context.Context, _ string, _ string, _ []string) ([]string, error) {
	s.subject = handler.TenantScopeFromContext(ctx).Subject
	return nil, errors.New("stop after caller context")
}
func (s *recoveringGrantStore) FinishDispatch(context.Context, uuid.UUID, string, int64, string, string, postgres.ExtraTx) error {
	return nil
}
func TestRunRecoversGrantReadUsingFreshIdentity(t *testing.T) {
	sub := uuid.New()
	token, jwt := mintToken(t, &authmw.Claims{Sub: sub, EXP: time.Now().Add(time.Hour).Unix()})
	cipher, _ := NewTokenCipher(make([]byte, 32))
	sealed, _ := cipher.Encrypt([]byte(token))
	id := uuid.New()
	store := &recoveringGrantStore{grant: &postgres.SchedulingGrant{ID: id, Generation: 1, State: "active", ExpiresAt: time.Now().Add(time.Hour), PipelineRID: "pipeline", TenantKind: "personal", TenantID: sub, Subject: sub, TokenCiphertext: sealed}}
	runner := &canonicalRunner{deps: CanonicalDeps{Store: store, JWT: jwt, Cipher: cipher}}
	d := postgres.SchedulingDispatch{GrantRef: id, GrantGeneration: 1, TenantKind: "personal", TenantID: sub, PipelineRID: "pipeline", BuildBranch: "main", DefinitionRevision: 1}
	if err := runner.Run(context.Background(), d, nil, nil); err == nil {
		t.Fatal("transient read ignored")
	}
	runner.Run(context.Background(), d, nil, nil)
	if store.subject != sub {
		t.Fatal("fresh identity not propagated")
	}
}

package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

type ManualBuildOutput struct {
	JobID          uuid.UUID
	DatasetRID     string
	Branch         string
	TransactionRID string
}

type manualBuildStore interface {
	BuildPlanRepository
	GetBuild(context.Context, string) (*models.BuildEnvelope, error)
	ListManualBuildOutputs(context.Context, uuid.UUID) ([]ManualBuildOutput, error)
	MoveManualBuild(context.Context, uuid.UUID, models.BuildState, models.BuildState) (bool, error)
	SettleManualBuildOutput(context.Context, uuid.UUID, ManualBuildOutput, string) error
	SettleManualBuildOutputs(context.Context, uuid.UUID, []ManualBuildPublication) error
	FinishManualBuild(context.Context, uuid.UUID, models.BuildState, executor.Outcome) (bool, error)
	RecoverManualBuildCompletion(context.Context, uuid.UUID) (bool, error)
}

func runManualBuild(w http.ResponseWriter, r *http.Request, ports ExecutionPorts, buildID uuid.UUID) {
	store, ok := ports.Plans.(manualBuildStore)
	if !ok {
		writeExecutionPortsUnavailable(w, "manual build persistence unavailable")
		return
	}
	ctx := withCallerBearer(r)
	build, err := store.GetBuild(ctx, buildID.String())
	if err != nil {
		writeManualBuildError(w, http.StatusInternalServerError, "manual_build_load_failed", err)
		return
	}
	if build == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if !ordinaryBuild(build) {
		writeManualBuildError(w, http.StatusConflict, "manual_build_trigger_unsupported", nil)
		return
	}
	subject, ok := requireManualBuildCreator(w, ctx, build)
	if !ok {
		return
	}
	state := models.BuildState(build.State)
	if state.IsTerminal() {
		writeManualBuildOutcome(w, buildID, persistedManualBuildOutcome(build))
		return
	}
	run := manualBuildExecution{base: ctx, store: store, dataset: *ports.ManualDataset, buildID: buildID, subject: subject}
	if state == models.BuildRunning || state == models.BuildAborting {
		if err := run.reconcileReceipts(ctx); err != nil {
			writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_recovery_pending", err)
			return
		}
		writeCurrentManualBuild(w, ctx, store, buildID)
		return
	}
	if state != models.BuildResolution {
		writeManualBuildError(w, http.StatusConflict, "manual_build_not_resolved", nil)
		return
	}
	plan, err := store.LoadPlan(ctx, buildID)
	if err != nil {
		writeManualBuildError(w, http.StatusInternalServerError, "manual_build_plan_failed", err)
		return
	}
	outputs, err := store.ListManualBuildOutputs(ctx, buildID)
	if err != nil {
		writeManualBuildError(w, http.StatusInternalServerError, "manual_build_outputs_failed", err)
		return
	}
	run.outputs, err = bindManualBuildOutputs(&plan, outputs)
	if err != nil {
		writeManualBuildError(w, http.StatusConflict, "manual_build_outputs_not_ready", err)
		return
	}
	if len(outputs) > maxManualDatasetGroup {
		writeManualBuildError(w, http.StatusConflict, "manual_build_output_limit_exceeded", nil)
		return
	}
	started, err := store.MoveManualBuild(ctx, buildID, models.BuildResolution, models.BuildRunning)
	if err != nil {
		writeManualBuildError(w, http.StatusInternalServerError, "manual_build_start_failed", err)
		return
	}
	if !started {
		writeCurrentManualBuild(w, ctx, store, buildID)
		return
	}
	run.fingerprints = &sync.Map{}
	runner := ports.NodeRunner
	if runner == nil {
		runner = newRuntimeNodeRunner(ports)
	}
	execCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	unregister := registerExecutionCancel(buildID, cancel)
	defer unregister()
	plan = normalizePlan(plan, ports)
	if len(outputs) > 1 {
		run.executeGroup(w, execCtx, build, plan, runner, ports.Audit)
		return
	}
	outcome, err := executor.Execute(execCtx, plan, runner, run, run, ports.Audit)
	finishCtx, cancelFinish := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancelFinish()
	if err != nil {
		// Executor validation can fail before it has entered any node.
		// Release the same owned reservations through the normal cleanup.
		outcome = manualStoppedOutcome(build, models.BuildFailed, err.Error())
		for _, output := range outputs {
			if _, cleanupErr := run.settle(finishCtx, output); cleanupErr != nil {
				writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_recovery_pending", cleanupErr)
				return
			}
		}
	}
	if outcome.FinalState != models.BuildCompleted {
		// A failed PBS write can be retried by the executor's abort
		// callback after DVS has already committed. Every output having
		// a durable commit proves its job succeeded in the runner.
		completed, err := store.RecoverManualBuildCompletion(finishCtx, buildID)
		if err != nil {
			writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_recovery_pending", err)
			return
		}
		if completed {
			writeCurrentManualBuild(w, finishCtx, store, buildID)
			return
		}
	}
	finished, err := store.FinishManualBuild(finishCtx, buildID, models.BuildRunning, outcome)
	if err != nil {
		writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_finalize_failed", err)
		return
	}
	if !finished {
		writeCurrentManualBuild(w, finishCtx, store, buildID)
		return
	}
	writeManualBuildOutcome(w, buildID, outcome)
}

func abortManualBuildIfConfigured(w http.ResponseWriter, r *http.Request, id string) bool {
	ports, ok := currentExecutionPorts()
	if !ok || ports.ManualDataset == nil {
		return false
	}
	store, ok := ports.Plans.(manualBuildStore)
	if !ok {
		return false
	}
	ctx := withCallerBearer(r)
	build, err := store.GetBuild(ctx, id)
	if err != nil {
		writeManualBuildError(w, http.StatusInternalServerError, "manual_build_load_failed", err)
		return true
	}
	if build == nil || !ordinaryBuild(build) || len(build.Jobs) == 0 {
		return false
	}
	subject, ok := requireManualBuildCreator(w, ctx, build)
	if !ok {
		return true
	}
	state := models.BuildState(build.State)
	if state == models.BuildFailed {
		return false
	}
	if state == models.BuildCompleted {
		w.WriteHeader(http.StatusConflict)
		return true
	}
	response := abortBuildResponse{RID: build.RID, BuildID: build.ID, State: string(models.BuildAborted)}
	if state == models.BuildAborted {
		writeAbortBuildResponse(w, response)
		return true
	}
	if state != models.BuildAborting {
		moved, err := store.MoveManualBuild(ctx, build.ID, state, models.BuildAborting)
		if err != nil || !moved {
			writeManualBuildError(w, http.StatusConflict, "manual_build_state_changed", err)
			return true
		}
	}
	response.CancelledExecution = cancelExecution(build.ID)
	cleanupCtx, cancelCleanup := context.WithTimeout(context.WithoutCancel(ctx), 45*time.Second)
	defer cancelCleanup()
	run := manualBuildExecution{base: ctx, store: store, dataset: *ports.ManualDataset, buildID: build.ID, subject: subject}
	outputs, err := store.ListManualBuildOutputs(cleanupCtx, build.ID)
	if err != nil {
		writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_recovery_pending", err)
		return true
	}
	for _, output := range outputs {
		published, err := run.settle(cleanupCtx, output)
		if err != nil {
			response.TransactionErrors = append(response.TransactionErrors, fmt.Sprintf("%s: %v", output.TransactionRID, err))
		} else if published == "" {
			response.AbortedTransactions++
		}
	}
	if len(response.TransactionErrors) > 0 {
		response.State = string(models.BuildAborting)
		writeAbortBuildResponse(w, response)
		return true
	}
	completed, err := store.RecoverManualBuildCompletion(cleanupCtx, build.ID)
	if err != nil {
		writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_recovery_pending", err)
		return true
	}
	if completed {
		w.WriteHeader(http.StatusConflict)
		return true
	}
	build, err = store.GetBuild(cleanupCtx, build.ID.String())
	if err != nil || build == nil {
		writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_reload_failed", err)
		return true
	}
	if build.State != string(models.BuildAborted) {
		outcome := manualStoppedOutcome(build, models.BuildAborted, "aborted by user")
		moved, err := store.FinishManualBuild(cleanupCtx, build.ID, models.BuildAborting, outcome)
		if err != nil || !moved {
			writeManualBuildError(w, http.StatusConflict, "manual_build_in_progress_or_recovery_pending", err)
			return true
		}
	}
	writeAbortBuildResponse(w, response)
	return true
}

func requireManualBuildCreator(w http.ResponseWriter, ctx context.Context, build *models.BuildEnvelope) (uuid.UUID, bool) {
	claims, ok := authmw.FromContext(ctx)
	if !ok || claims.Sub == uuid.Nil || callerBearerFromContext(ctx) == "" {
		writeManualBuildError(w, http.StatusUnauthorized, "manual_build_caller_required", nil)
		return uuid.Nil, false
	}
	if build.RequestedBy != claims.Sub.String() ||
		(!claims.HasRole("admin") && !claims.HasPermissionKey("dataset.write") && !claims.HasPermission("dataset", "write")) {
		writeManualBuildError(w, http.StatusForbidden, "manual_build_creator_with_dataset_write_required", nil)
		return uuid.Nil, false
	}
	return claims.Sub, true
}

func ordinaryBuild(build *models.BuildEnvelope) bool {
	return build.TriggerKind == "MANUAL" || build.TriggerKind == "FORCE"
}

func bindManualBuildOutputs(plan *executor.Plan, outputs []ManualBuildOutput) (map[string]ManualBuildOutput, error) {
	byReservation := make(map[string]ManualBuildOutput, len(outputs))
	for _, output := range outputs {
		if _, exists := byReservation[output.TransactionRID]; exists {
			return nil, errors.New("manual build output reservation belongs to multiple jobs")
		}
		byReservation[output.TransactionRID] = output
	}
	count := 0
	for i := range plan.Nodes {
		node := &plan.Nodes[i]
		for j := range node.Outputs {
			output := &node.Outputs[j]
			owned, ok := byReservation[output.TransactionRID]
			if !ok || owned.JobID != node.JobID || owned.DatasetRID != output.DatasetRID || owned.Branch != plan.BuildBranch {
				return nil, fmt.Errorf("output %s does not have a matching build reservation", output.DatasetRID)
			}
			output.Branch = owned.Branch
			count++
		}
	}
	if count == 0 || count != len(outputs) {
		return nil, errors.New("manual build does not have a complete set of owned output reservations")
	}
	return byReservation, nil
}

type manualBuildExecution struct {
	base         context.Context
	store        manualBuildStore
	dataset      DatasetHTTPVersioning
	buildID      uuid.UUID
	subject      uuid.UUID
	outputs      map[string]ManualBuildOutput
	fingerprints *sync.Map
}

func (m manualBuildExecution) Commit(ctx context.Context, output executor.OutputTransaction, result executor.NodeResult) error {
	owned, ok := m.outputs[output.TransactionRID]
	if !ok {
		return errors.New("unbound manual output")
	}
	remote, err := newManualDatasetOutput(m.dataset, m.subject, m.buildID, output)
	if err != nil {
		return err
	}
	payload, fingerprint, err := remote.commitPayload(output, result)
	if err != nil {
		return err
	}
	m.fingerprints.Store(output.TransactionRID, fingerprint)
	published, publishErr := remote.commit(ctx, payload, fingerprint)
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(m.base), 30*time.Second)
	defer cancel()
	if publishErr != nil {
		// Transport errors include a lost acknowledgement after DVS
		// committed. Check its durable receipt before returning failure.
		published, err = remote.receipt(persistCtx, fingerprint)
		if err != nil || published == "" {
			return fmt.Errorf("dataset_output_commit_failed: %w", errors.Join(publishErr, err))
		}
	}
	return m.store.SettleManualBuildOutput(persistCtx, m.buildID, owned, published)
}

func (m manualBuildExecution) Abort(_ context.Context, output executor.OutputTransaction) error {
	owned, ok := m.outputs[output.TransactionRID]
	if !ok {
		return errors.New("unbound manual output")
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(m.base), 45*time.Second)
	defer cancel()
	_, err := m.settle(ctx, owned)
	return err
}

func (m manualBuildExecution) settle(ctx context.Context, output ManualBuildOutput) (string, error) {
	remote, err := newManualDatasetOutput(m.dataset, m.subject, m.buildID, executor.OutputTransaction{
		DatasetRID: output.DatasetRID, TransactionRID: output.TransactionRID, Branch: output.Branch,
	})
	if err != nil {
		return "", err
	}
	var fingerprint string
	if m.fingerprints != nil {
		if expected, ok := m.fingerprints.Load(output.TransactionRID); ok {
			fingerprint, _ = expected.(string)
		}
	}
	published, err := remote.receipt(ctx, fingerprint)
	if err != nil {
		return "", err
	}
	if published == "" {
		// The original reservation row serializes abort against DVS's
		// atomic exchange. An ABORTED reservation is ambiguous until we
		// look for a receipt again after the abort has returned.
		if err := m.dataset.AbortTransaction(ctx, output.DatasetRID, output.Branch, output.TransactionRID); err != nil {
			return "", err
		}
		published, err = remote.receipt(ctx, fingerprint)
		if err != nil {
			return "", err
		}
	}
	return published, m.store.SettleManualBuildOutput(ctx, m.buildID, output, published)
}

func (m manualBuildExecution) reconcileReceipts(ctx context.Context) error {
	outputs, err := m.store.ListManualBuildOutputs(ctx, m.buildID)
	if err != nil {
		return err
	}
	var publications []ManualBuildPublication
	for _, output := range outputs {
		remote, err := newManualDatasetOutput(m.dataset, m.subject, m.buildID, executor.OutputTransaction{
			DatasetRID: output.DatasetRID, TransactionRID: output.TransactionRID, Branch: output.Branch,
		})
		if err != nil {
			return err
		}
		published, err := remote.receipt(ctx, "")
		if err != nil {
			return err
		}
		if published != "" {
			publications = append(publications, ManualBuildPublication{output, published})
		}
	}
	if len(publications) > 0 {
		if err := m.store.SettleManualBuildOutputs(ctx, m.buildID, publications); err != nil {
			return err
		}
	}
	_, err = m.store.RecoverManualBuildCompletion(ctx, m.buildID)
	return err
}

func writeCurrentManualBuild(w http.ResponseWriter, ctx context.Context, store manualBuildStore, buildID uuid.UUID) {
	build, err := store.GetBuild(ctx, buildID.String())
	if err != nil || build == nil {
		writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_reload_failed", err)
		return
	}
	if !models.BuildState(build.State).IsTerminal() {
		writeManualBuildError(w, http.StatusConflict, "manual_build_in_progress_or_recovery_pending", nil)
		return
	}
	writeManualBuildOutcome(w, buildID, persistedManualBuildOutcome(build))
}

func persistedManualBuildOutcome(build *models.BuildEnvelope) executor.Outcome {
	out := executor.Outcome{
		FinalState: models.BuildState(build.State), Nodes: map[string]executor.NodeState{},
		Attempts: map[string]int{}, Reasons: map[string]string{},
	}
	for _, job := range build.Jobs {
		state := executor.NodeState(job.State)
		out.Nodes[job.JobSpecRID] = state
		out.Attempts[job.JobSpecRID] = int(job.Attempt)
		if job.FailureReason != nil {
			out.Reasons[job.JobSpecRID] = *job.FailureReason
		}
		switch state {
		case executor.NodeCompleted:
			out.Completed++
		case executor.NodeFailed:
			out.Failed++
		case executor.NodeAborted:
			out.Aborted++
		}
	}
	return out
}

func manualStoppedOutcome(build *models.BuildEnvelope, state models.BuildState, reason string) executor.Outcome {
	stopped := *build
	stopped.State = string(state)
	stopped.Jobs = append([]models.Job(nil), build.Jobs...)
	for i := range stopped.Jobs {
		job := &stopped.Jobs[i]
		switch executor.NodeState(job.State) {
		case executor.NodeFailed, executor.NodeCompleted, executor.NodeAborted:
			continue
		}
		job.State = string(executor.NodeAborted)
		if state == models.BuildFailed {
			job.State = string(executor.NodeFailed)
		}
		job.FailureReason = &reason
	}
	return persistedManualBuildOutcome(&stopped)
}

func writeManualBuildOutcome(w http.ResponseWriter, buildID uuid.UUID, out executor.Outcome) {
	writeJSON(w, http.StatusOK, executePipelineResponse{BuildID: buildID, State: string(out.FinalState), Completed: out.Completed, Failed: out.Failed, Aborted: out.Aborted, Attempts: out.Attempts, Nodes: out.Nodes, Reasons: out.Reasons})
}

func writeManualBuildError(w http.ResponseWriter, status int, code string, err error) {
	body := map[string]string{"error": code}
	if err != nil {
		body["detail"] = strings.TrimSpace(err.Error())
	}
	writeJSON(w, status, body)
}

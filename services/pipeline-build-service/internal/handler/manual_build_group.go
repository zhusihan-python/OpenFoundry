package handler

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

const maxManualDatasetGroup = 16

var errManualGroupReceiptsPending = errors.New("complete manual group receipts are not available")

type ManualBuildPublication struct {
	Output         ManualBuildOutput
	TransactionRID string
}

type manualGroupOutput struct {
	owned  ManualBuildOutput
	remote manualDatasetOutput
	body   datasetOutputCommitRequest
}

type manualGroupAudit struct {
	executor.AuditSink
	outputJobs map[uuid.UUID]bool
}

func (a manualGroupAudit) Record(ctx context.Context, event executor.AuditEvent) error {
	// The executor marks computation done so dependents can run. Output
	// jobs are completed in PBS only after their publication is durable.
	if a.AuditSink == nil || event.DatasetRID != "" || event.JobID == uuid.Nil ||
		(a.outputJobs[event.JobID] && event.To == executor.NodeCompleted) {
		return nil
	}
	return a.AuditSink.Record(ctx, event)
}

type manualGroupedDatasetProducersKey struct{}

func (m manualBuildExecution) executeGroup(w http.ResponseWriter, ctx context.Context, build *models.BuildEnvelope, plan executor.Plan, runner executor.NodeRunner, audit executor.AuditSink) {
	producers := make(map[string]uuid.UUID, len(m.outputs))
	outputJobs := make(map[uuid.UUID]bool, len(m.outputs))
	for _, output := range m.outputs {
		if id, err := canonicalDatasetRIDForComparison(output.DatasetRID); err == nil {
			producers[id] = output.JobID
		}
		outputJobs[output.JobID] = true
	}
	runCtx := context.WithValue(ctx, manualGroupedDatasetProducersKey{}, producers)
	// The executor handles dependencies and retries. Its external commit
	// and abort ports stay idle until every output has been prepared.
	if m.staged != nil && m.staged.Excel != nil {
		runner = manualExcelGroupRunner{dataset: m.dataset, config: m.staged}
	}
	outcome, executionErr := executor.Execute(runCtx, plan, runner, nil, nil, manualGroupAudit{audit, outputJobs})
	var group []manualGroupOutput
	if executionErr == nil && outcome.FinalState == models.BuildCompleted {
		group, executionErr = m.prepareGroup(runCtx, plan, outcome)
	}
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(m.base), 45*time.Second)
	defer cancel()
	if len(group) > 0 {
		publications, rejected, err := m.publishGroup(ctx, persistCtx, group)
		if err != nil && !rejected {
			// An unavailable or incomplete receipt read cannot establish
			// rollback. Keep reservations and let execute recover later.
			writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_recovery_pending", err)
			return
		}
		executionErr = err
		if err == nil {
			if err := m.store.SettleManualBuildOutputs(persistCtx, m.buildID, publications); err != nil {
				writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_recovery_pending", err)
				return
			}
		}
	}
	if executionErr != nil {
		state := models.BuildFailed
		if ctx.Err() != nil {
			state = models.BuildAborted
		}
		outcome = manualStoppedOutcome(build, state, executionErr.Error())
	}
	if outcome.FinalState != models.BuildCompleted {
		for _, node := range plan.Nodes {
			if len(node.Outputs) > 0 && outcome.Nodes[node.ID] == executor.NodeCompleted {
				outcome.Nodes[node.ID] = executor.NodeAborted
				outcome.Reasons[node.ID] = "output group was not published"
				outcome.Completed--
				outcome.Aborted++
			}
		}
		for _, output := range m.outputs {
			if _, err := m.settle(persistCtx, output); err != nil {
				writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_recovery_pending", err)
				return
			}
		}
	}
	finished, err := m.store.FinishManualBuild(persistCtx, m.buildID, models.BuildRunning, outcome)
	if err != nil {
		writeManualBuildError(w, http.StatusServiceUnavailable, "manual_build_finalize_failed", err)
		return
	}
	if !finished {
		writeCurrentManualBuild(w, persistCtx, m.store, m.buildID)
		return
	}
	writeManualBuildOutcome(w, m.buildID, outcome)
}

func (m manualBuildExecution) prepareGroup(ctx context.Context, plan executor.Plan, outcome executor.Outcome) ([]manualGroupOutput, error) {
	group := make([]manualGroupOutput, 0, len(m.outputs))
	for _, node := range plan.Nodes {
		for _, output := range node.Outputs {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			owned, ok := m.outputs[output.TransactionRID]
			if !ok {
				return nil, errors.New("unbound manual output")
			}
			remote, err := newManualDatasetOutput(m.dataset, m.subject, m.buildID, output)
			if err != nil {
				return nil, err
			}
			result := outcome.Results[node.ID]
			var artifact excelGroupArtifact
			if m.staged != nil && m.staged.Excel != nil {
				artifacts, _ := result.Metadata["excel_artifacts"].(map[string]excelGroupArtifact)
				var found bool
				if artifact, found = artifacts[remote.dataset]; !found {
					return nil, errors.New("missing prepared Excel group output")
				}
				result = artifact.result
			}
			body, err := remote.commitBody(ctx, output, result)
			if err != nil {
				return nil, err
			}
			if artifact.schema != nil {
				body.Schema, body.ArtifactManifest = artifact.schema, &artifact.manifest
			}
			group = append(group, manualGroupOutput{owned, remote, body})
		}
	}
	sort.Slice(group, func(i, j int) bool { return group[i].remote.dataset < group[j].remote.dataset })
	if m.staged != nil {
		if err := m.stageGroup(ctx, group, outcome); err != nil {
			return nil, err
		}
	}
	return group, nil
}

func (m manualBuildExecution) publishGroup(ctx, recoveryCtx context.Context, group []manualGroupOutput) ([]ManualBuildPublication, bool, error) {
	payload, fingerprint, err := encodeManualOutputGroup(group)
	if err != nil {
		return nil, true, err
	}
	// Persist the exact ordered request fingerprint before contacting DVS.
	// A concurrent abort that changes the build state wins before this
	// compare-and-set and prevents publication.
	path := "/v1/datasets/outputs:commit-group"
	var durableBody []byte
	if m.staged != nil {
		path, durableBody = stagedManualGroupPath, payload
	}
	saved, err := m.store.SetManualGroupPublishFingerprint(recoveryCtx, m.buildID, "", fingerprint, durableBody)
	if err != nil {
		return nil, false, err
	}
	if !saved {
		return nil, false, errManualGroupReceiptsPending
	}
	status, raw, err := m.dataset.do(ctx, http.MethodPost, path, payload)
	if err == nil && (status == http.StatusCreated || status == http.StatusOK) {
		publications, decodeErr := decodeManualGroupReceipts(group, raw, fingerprint)
		if decodeErr == nil {
			return publications, false, nil
		}
		err = decodeErr
	} else if err == nil {
		err = datasetErrorFromResponse(status, raw)
	}
	// Check the same durable keys for every member after a lost response.
	// A partial read can straddle COMMIT, so it is never evidence of rejection.
	publications := make([]ManualBuildPublication, 0, len(group))
	for _, member := range group {
		published, receiptErr := member.remote.receipt(recoveryCtx, fingerprint)
		if receiptErr != nil {
			return nil, false, errors.Join(err, receiptErr)
		}
		if published != "" {
			publications = append(publications, ManualBuildPublication{member.owned, published})
		}
	}
	if len(publications) == len(group) {
		return publications, false, nil
	}
	rejected := len(publications) == 0 && status >= 400 && status < 500 && status != http.StatusRequestTimeout
	if rejected {
		cleared, clearErr := m.store.SetManualGroupPublishFingerprint(recoveryCtx, m.buildID, fingerprint, "", nil)
		if clearErr != nil || !cleared {
			return nil, false, errors.Join(errManualGroupReceiptsPending, err, clearErr)
		}
	}
	return nil, rejected, err
}

func encodeManualOutputGroup(group []manualGroupOutput) ([]byte, string, error) {
	type memberBody struct {
		DatasetRID string                     `json:"dataset_rid"`
		Output     datasetOutputCommitRequest `json:"output"`
	}
	type memberKey struct {
		DatasetID string `json:"dataset_id"`
		Key       string `json:"key"`
	}
	members := make([]memberBody, 0, len(group))
	keys := make([]memberKey, 0, len(group))
	for _, member := range group {
		members = append(members, memberBody{member.remote.dataset, member.body})
		keys = append(keys, memberKey{strings.TrimPrefix(member.remote.dataset, datasetRIDPrefix), member.remote.key})
	}
	payload, err := json.Marshal(struct {
		Members []memberBody `json:"members"`
	}{members})
	if err != nil {
		return nil, "", err
	}
	// DVS binds receipts to the ordered group and exact request bytes.
	// Match the output-commit-group-v1 receipt fingerprint contract.
	requestHash := sha256.Sum256(payload)
	binding, err := json.Marshal(struct {
		Kind    string      `json:"kind"`
		Request string      `json:"request"`
		Members []memberKey `json:"members"`
	}{"output-commit-group-v1", hex.EncodeToString(requestHash[:]), keys})
	if err != nil {
		return nil, "", err
	}
	hash := sha256.Sum256(binding)
	return payload, hex.EncodeToString(hash[:]), nil
}

func decodeManualGroupReceipts(group []manualGroupOutput, raw []byte, fingerprint string) ([]ManualBuildPublication, error) {
	var response struct {
		Receipts []json.RawMessage `json:"receipts"`
	}
	if err := json.Unmarshal(raw, &response); err != nil {
		return nil, err
	}
	if len(response.Receipts) != len(group) {
		return nil, fmt.Errorf("manual group returned %d receipts for %d outputs", len(response.Receipts), len(group))
	}
	publications := make([]ManualBuildPublication, 0, len(group))
	for i, member := range group {
		published, err := member.remote.decodeReceipt(response.Receipts[i], fingerprint)
		if err != nil {
			return nil, err
		}
		publications = append(publications, ManualBuildPublication{member.owned, published})
	}
	return publications, nil
}

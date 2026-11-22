package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/domain/executor"
	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/models"
)

type targetBuildRepo struct {
	recordingBuildRepo
	parent  *models.Pipeline
	readErr error
}

func (r *targetBuildRepo) GetPipeline(context.Context, uuid.UUID) (*models.Pipeline, error) {
	return r.parent, r.readErr
}

func TestManualResolutionChecksSelectedOutputsBeforeReservation(t *testing.T) {
	pipelineID, dataset, object := uuid.New(), uuid.New(), uuid.New()
	link := uuid.NewSHA1(uuid.NameSpaceOID, []byte(pipelineID.String()+":link:dataset-output"))
	nodes := []models.PipelineNode{
		{ID: "plain", TransformType: "output_dataset", OutputDatasetID: &dataset},
		{ID: "object", TransformType: "output_dataset", OutputDatasetID: &object, Config: json.RawMessage(`{"_output":{"kind":"ontology_object"}}`)},
		{ID: "link", TransformType: "output_link_type"},
	}
	raw, err := json.Marshal(nodes)
	require.NoError(t, err)
	for _, tc := range []struct {
		name    string
		request uuid.UUID
		outputs []string
		payload string
		status  int
		code    string
	}{
		{"legacy non-object recipe", dataset, []string{dataset.String()}, `"select x"`, 202, ""},
		{"ordinary sibling", dataset, []string{dataset.String()}, "{}", 202, ""},
		{"object without JobSpec", object, nil, "", 409, "manual_build_ontology_output_unsupported"},
		{"implicit link output", link, nil, "", 409, "manual_build_ontology_output_unsupported"},
		{"companion JobSpec output", dataset, []string{dataset.String(), object.String()}, "{}", 409, "manual_build_ontology_output_unsupported"},
		{"JobSpec declares ontology", dataset, []string{dataset.String()}, `{"_output":{"kind":"link_type"}}`, 409, "manual_build_ontology_output_unsupported"},
		{"empty nested declaration", dataset, []string{dataset.String()}, `{"kind":"ontology","_output":{}}`, 409, "manual_build_ontology_output_unsupported"},
		{"link alias", dataset, []string{dataset.String()}, `{"_output":{"kind":"link"}}`, 409, "manual_build_ontology_output_unsupported"},
		{"malformed declaration", dataset, []string{dataset.String()}, `{"_output":{"kind":[]}}`, 409, "manual_build_target_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &targetBuildRepo{parent: &models.Pipeline{ID: pipelineID, DAG: raw}}
			specs, dvs := newHandlerJobSpecRepo(), newHandlerDatasetRepo()
			var outputs []string
			for _, id := range tc.outputs {
				outputs = append(outputs, datasetRIDPrefix+id)
				dvs.addBranch(datasetRIDPrefix+id, "main")
			}
			if len(outputs) > 0 {
				spec := handlerJobSpec("spec", nil, outputs)
				spec.LogicPayload = json.RawMessage(tc.payload)
				specs.add(spec)
			}
			t.Cleanup(SetBuildLifecyclePorts(BuildLifecyclePorts{Builds: store, JobSpecs: specs, Versioning: dvs}))
			body, err := json.Marshal(models.CreateBuildRequest{PipelineRID: pipelineID.String(), BuildBranch: "main", OutputDatasetRIDs: []string{datasetRIDPrefix + tc.request.String()}})
			require.NoError(t, err)
			call := func() *httptest.ResponseRecorder {
				rr := httptest.NewRecorder()
				CreateBuild(rr, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
				return rr
			}
			store.readErr = context.DeadlineExceeded
			require.Equal(t, 503, call().Code)
			require.Zero(t, store.opened)
			require.Zero(t, dvs.openedTransactions)
			store.readErr = nil
			store.parent.DAG = json.RawMessage(`{"nodes":1}`)
			require.Contains(t, call().Body.String(), "manual_build_target_invalid")
			require.Zero(t, dvs.openedTransactions)
			store.parent.DAG = raw
			rr := call()
			require.Equal(t, tc.status, rr.Code, rr.Body.String())
			if tc.code != "" {
				require.Contains(t, rr.Body.String(), tc.code)
				require.Zero(t, store.opened)
				require.Zero(t, dvs.openedTransactions)
			} else {
				require.Equal(t, 1, store.opened)
				require.Equal(t, 1, dvs.openedTransactions)
			}
		})
	}
}

type replacedJobSpec struct {
	first, second models.JobSpec
	reads         int
}

func (r *replacedJobSpec) Lookup(context.Context, string, string, string, []string) (*models.JobSpec, error) {
	r.reads++
	if r.reads == 1 {
		return &r.first, nil
	}
	return &r.second, nil
}

func TestManualResolutionRechecksJobSpecBeforeReservation(t *testing.T) {
	store := &recordingBuildRepo{}
	dvs := newHandlerDatasetRepo()
	dvs.addBranch("orders", "main")
	first := handlerJobSpec("ordinary", nil, []string{"orders"})
	second := handlerJobSpec("ontology", nil, []string{"orders"})
	canonical := json.RawMessage(`{"transform_type":"output_object_type"}`)
	second.PublishedLogicPayload = &canonical
	specs := &replacedJobSpec{first: first, second: second}
	t.Cleanup(SetBuildLifecyclePorts(BuildLifecyclePorts{Builds: store, JobSpecs: specs, Versioning: dvs}))
	body := fmt.Sprintf(`{"pipeline_rid":"%s","build_branch":"main","output_dataset_rids":["orders"]}`, uuid.New())
	rr := httptest.NewRecorder()
	CreateBuild(rr, httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body)))
	require.Equal(t, 409, rr.Code, rr.Body.String())
	require.Equal(t, 2, specs.reads)
	require.Zero(t, dvs.openedTransactions)
	require.Zero(t, store.persisted)
}

type historicalManualStore struct {
	manualBuildStore
	build       models.BuildEnvelope
	plan        executor.Plan
	output      ManualBuildOutput
	readErr     error
	publication string
}

func (s *historicalManualStore) GetBuild(context.Context, string) (*models.BuildEnvelope, error) {
	return &s.build, nil
}
func (s *historicalManualStore) LoadPlan(context.Context, uuid.UUID) (executor.Plan, error) {
	return s.plan, s.readErr
}
func (s *historicalManualStore) ListManualBuildOutputs(context.Context, uuid.UUID) ([]ManualBuildOutput, error) {
	return []ManualBuildOutput{s.output}, nil
}
func (s *historicalManualStore) ManualStagedGroupConfig(context.Context, uuid.UUID) (json.RawMessage, error) {
	return nil, nil
}
func (s *historicalManualStore) MoveManualBuild(_ context.Context, _ uuid.UUID, from, to models.BuildState) (bool, error) {
	if s.build.State != string(from) {
		return false, nil
	}
	s.build.State = string(to)
	return true, nil
}
func (s *historicalManualStore) SettleManualBuildOutput(_ context.Context, _ uuid.UUID, _ ManualBuildOutput, published string) error {
	s.publication = published
	return nil
}
func (s *historicalManualStore) FinishManualBuild(_ context.Context, _ uuid.UUID, _ models.BuildState, out executor.Outcome) (bool, error) {
	s.build.State = string(out.FinalState)
	return true, nil
}

// This store contains the records available before target validation shipped.
// It deliberately exposes no mutable parent graph. Only the resolved recipe and
// the existing reservation can determine what is published.
func TestManualExecuteUsesHistoricalJobSpecAndKeepsCompletedReceipt(t *testing.T) {
	subject, build, job, dataset, reservation, published := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	out := executor.OutputTransaction{DatasetRID: datasetRIDPrefix + dataset.String(), TransactionRID: transactionRIDPrefix + reservation.String(), Branch: "main"}
	node := executor.Node{ID: "pinned-spec", JobID: job, Outputs: []executor.OutputTransaction{out}, Metadata: map[string]any{"logic_kind": "TRANSFORM"}}
	store := &historicalManualStore{
		build:  models.BuildEnvelope{Build: models.Build{ID: build, State: string(models.BuildResolution), TriggerKind: "MANUAL", RequestedBy: subject.String()}},
		plan:   executor.Plan{BuildID: build, BuildBranch: "main", Nodes: []executor.Node{node}},
		output: ManualBuildOutput{JobID: job, DatasetRID: out.DatasetRID, TransactionRID: out.TransactionRID, Branch: "main"},
	}
	var calls atomic.Int32
	dvs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var commit datasetOutputCommitRequest
		if err := json.Unmarshal(body, &commit); err != nil {
			t.Error(err)
			return
		}
		writeJSON(w, 201, map[string]any{
			"dataset_rid": out.DatasetRID, "branch": "main", "idempotency_key": commit.IdempotencyKey, "request_fingerprint": fmt.Sprintf("%x", sha256.Sum256(body)),
			"transaction": map[string]any{"id": published, "dataset_id": dataset, "branch_name": "main", "status": "COMMITTED", "tx_type": "SNAPSHOT", "started_by": subject},
		})
	}))
	t.Cleanup(dvs.Close)
	ports := ExecutionPorts{Plans: store, ManualDataset: &DatasetHTTPVersioning{BaseURL: dvs.URL, Client: dvs.Client()}, NodeRunner: &recordingNodeRunner{}}
	call := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Authorization", "Bearer actor")
		req = req.WithContext(authmw.ContextWithClaims(req.Context(), &authmw.Claims{Sub: subject, Roles: []string{"admin"}}))
		rr := httptest.NewRecorder()
		runManualBuild(rr, req, ports, build)
		return rr
	}
	store.readErr = context.DeadlineExceeded
	require.Equal(t, 503, call().Code)
	store.readErr = nil
	delete(node.Metadata, "logic_kind")
	require.Equal(t, 503, call().Code)
	node.Metadata["logic_kind"] = "TRANSFORM"
	node.Metadata["logic_payload"] = json.RawMessage(`{"transform_type":"output_object_type"}`)
	require.Equal(t, 409, call().Code)
	require.Zero(t, calls.Load())
	require.Equal(t, string(models.BuildResolution), store.build.State)
	node.Metadata["logic_payload"] = json.RawMessage(`{"transform_type":"dataset_input"}`)
	rr := call()
	require.Equal(t, 200, rr.Code, rr.Body.String())
	require.Equal(t, string(models.BuildCompleted), store.build.State)
	require.Equal(t, transactionRIDPrefix+published.String(), store.publication)
	store.readErr = context.DeadlineExceeded
	require.Equal(t, 200, call().Code)
	require.EqualValues(t, 1, calls.Load())
}

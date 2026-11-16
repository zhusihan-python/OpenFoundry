#!/usr/bin/env python3
"""P2.5 HTTP acceptance engine — smoke_p25.py (Primary-owned, v3).

Sole author: Primary GLM (root handoff 2026-11-14; Tools confirmed no
further edits). v3 folds the FULL review-smoke-round1 ten-group wire
corrections (design frozen in HANDOFF-PRIMARY.md "v3 修法"): the v2 engine
orchestrated the right scenarios against WRONG wire contracts; v3 pins
every dataset read to the P1-verified surface and every fixture/proxy
fact to the Tools r3 layout.

Basis:
  * implementation-contract.md v1.2.1 (business code FROZEN — this engine
    never edits business source or non-PG tests to fit itself);
  * fixtures/FIXTURE-PROXY-API.md r2 semantics + Tools r3 layout
    (p25h4 carries a second NON-EMPTY B window; proxy ledger keys are
    request_target / ack_dataset_id / ack_txn_id / ack_txn_status /
    verified_committed);
  * dataset-http-read-facts.md (P1-verified dataset read surface):
      - files listing  = GET /v1/datasets/{id}/files  -> envelope
        {branch,total,files:[{id,transaction_id,...}]}; the run's files
        are the entries with file.transaction_id == run.dataset_version_id
        (run.dataset_version_id is the commit transaction UUID — NEVER an
        integer version; the versions route requires a positive integer
        and returns no files, so it is NOT used at all);
      - artifact       = GET /v1/datasets/{id}/files/{file_id}/download;
      - transaction    = GET /v1/datasets/{id}/branches/main/
        transactions/{txnUUID} -> FLAT RuntimeTransaction {id,dataset_id,
        branch_id,branch_name,tx_type,status,metadata,committed_at};
      - latest view    = GET /v1/datasets/{id}/views/at?branch=main ->
        ViewOut {dataset_id,requested_branch,resolved_branch,
        head_transaction_id,files[{logical_path,physical_path,size_bytes,
        introduced_by}]}; the effective set is matched by introduced_by ==
        known file transaction and ONLY view-selected fileIDs are
        downloaded (the files-envelope total is never an effective count);
      - the preview endpoint returns metadata/fallback logical_path, and
        the rows endpoint force-parses NDJSON objects (this fixture's
        records are bare strings) — BOTH are forbidden here;
  * records are bare strings transmitted verbatim: expected raw bytes are
    exactly b"".join(json.dumps(value).encode()+b"\\n" for each expected
    string) (all ASCII; an empty batch is b"").

Review-smoke-round1 ten groups -> code map:
  1  per-source egress override  -> incremental_source_config() sets
     config.egress.allow_insecure_http=true (rest_snapshot.go policy():
     service posture is a ceiling; only an explicit source override
     enables plain HTTP).
  2  dataset chain               -> files_envelope()/bind_run_files()/
     download_file()/txn_get()/view_at() + verify_run_artifact()/
     verify_latest_view(); the versions, preview and rows routes are
     deleted outright.
  3  exact values                -> SRC_WINDOWS/_ndjson/verify_run_artifact
     (values+raw+len+sha, run/txn/download three-way, per-run ledger
     slices: fixed input token per page, ordered cursor chain, exact page
     count, window index).
  4  h4 multi-window             -> native p25h4 (A 12+12 then NON-EMPTY
     B(8)); six candidate faults on B's non-empty terminal page with
     reached-proof (the fault run's ledger slice IS window 1).
  5  h6/h7/h15                   -> h6 overlapping sibling defs (two live
     owners at once); h7 retarget to a REAL second dataset + file_glob
     fence + allowed name/cron/enable branches + before/after checkpoint
     snapshots + org-2 tenant 404s; h15 manual busy = REAL sync POST.
  6  h13 scheduling              -> make_sync_def() honours extra_cfg
     (schedule_cron/time_zone/enabled at creation); arm via the
     Authorization header; every enqueued tick.run_id must reference a
     REAL succeeded run; busy due tick = skipped_incremental_busy with NO
     new run row, data checkpoint untouched, cron cursor advanced;
     revoke+disable then quiet.
  7  h14 remote evidence         -> ledger key ack_txn_id (never the old
     upstream key); filter request_target==this dataset, exactly one
     incremental dispatch; run.dataset_version_id MUST be empty (lost
     ACK); remote proof = flat txn GET + full-value file download bound
     to ack_txn_id.
  8  h10 two-stage driver iface  -> --h10-stage pre|post + --h10-defs +
     --h10-prior: pre snapshots live state, post (driver already
     restarted the connector) asserts byte-stable success states, owner
     unchanged, no adoption (no new run ids), business blocked_reason;
     BOTH rosters are built from ACTUAL rows only; --tail-declaration is
     consumed (observed retained owners must be inside the declared set,
     else recorded as undeclared); h14 no longer fabricates driver
     placeholder rows — only scenarios actually performed are declared.
  9  final quiet strict          -> per source HTTP 200 with
     inventory_started/inventory_finished/active present as non-bool
     ints, active==0, started==finished; proxy stats 200 with
     commit_inflight==0 and commit_started==commit_finished; ANY quiet
     failure joins the failed list and forces a non-zero exit.
 10  h12 + evidence              -> h12 binds the PG track's real
     pg-candidate-bindings-r17.json (fixed rollback name pinned via
     --pg-proof-name; required names come from the referenced frozen
     file, never self-selected; raw/rc/meta artifacts re-hashed here);
     HttpApi records the FULL safe JSON response body (downloads record
     sha256+bytes) so CP/txn/ledger assertions are replayable;
     AttemptRecorder.persist_handle() writes handles.json IMMEDIATELY
     for every created source/def/dataset/run — including failed
     attempts.

EXECUTION AUTHORITY (strict):
  * Pure local modes ONLY are runnable today: --plan / --contract-probe /
    --list-gates / --ast-check / --selftest-mocked (in-process fakes, no
    sockets, no listeners; h12 additionally READS the PG track's frozen
    evidence artifacts — read-only file consumption, no DB, no docker).
  * Real HTTP modes (--phase healthy|tail / --only hN) are fully
    implemented but double-gated: BOTH --released AND env P25_CONFIRM=yes,
    else exit rc=3 without any network call. Root releases the runtime.

Standing rules: zero-dispatch proofs use the proxy forward ledger
filtered by request_target; no "remote has no new version" claims for
non-forwarded faults (connector-local outcome only); every failed
attempt keeps its own raw record (-rN, never overwritten); no
Authorization/JWT/DSN values are ever written to evidence.
"""

import argparse
import copy
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request

P25_SINCE_PARAM = "since_token"
P25_COMPLETED_PATH = "meta.completed_token"

RELEASE_ENV = "P25_CONFIRM"
RELEASE_ENV_VALUE = "yes"

# The PG track's frozen h12 binding (root-released three-stage execution).
# Consumed read-only; the engine never fabricates PG coverage.
DEFAULT_PG_PROOF_FILE = "pg-candidate-bindings-r17.json"
# The single pinned rollback case (p25-required-names.txt member; the
# name list itself is NOT redefined here).
PINNED_ROLLBACK_NAME = "TestPGCheckpointAdvanceAllOrNothingRollback"
# The frozen three-stage shape the PG binding must show EXACTLY
# (round-2 #6: a missing stage can never be green).
PG_STAGE_NAMES = ("new-normal", "new-race", "unified")

# ---------------------------------------------------------------------------
# Route registry (pinned against Connector server.go / dataset server.go).
# Connector routes are under /api/v1; the DATASET surface is under /v1
# (dataset server.go mounts BOTH prefixes on the same handlers; the P1
# helper uses /v1 exclusively and so does this engine).
# -------------------------------------------------------------------------

ROUTES = {
    "sync_create": "/api/v1/data-connection/syncs",                        # POST
    "sync_patch": "/api/v1/data-connection/syncs/{id}",                    # PATCH
    "sync_get": "/api/v1/data-connection/syncs/{id}",                      # GET
    "sync_runs": "/api/v1/data-connection/syncs/{id}/runs",                # GET
    "sync_run": "/api/v1/data-connection/syncs/{id}/run",                  # POST
    "run_get": "/api/v1/data-connection/syncs/{id}/runs/{run_id}",         # GET
    "schedule_arm": "/api/v1/data-connection/syncs/{id}/schedule:arm",     # POST
    "schedule_revoke": "/api/v1/data-connection/syncs/{id}/schedule:arm",  # DELETE
    "schedule_ticks": "/api/v1/data-connection/syncs/{id}/schedule/ticks", # GET
    "checkpoint_view": "/api/v1/data-connection/syncs/{id}/checkpoint",    # GET (P25)
    "run_cancel": "/api/v1/data-connection/syncs/{id}/runs/{run_id}/cancel",  # POST
    "source_create": "/api/v1/connections",                                # POST
    "connection_patch": "/api/v1/connections/{id}",                        # PATCH
    "credentials_post": "/api/v1/data-connection/sources/{id}/credentials",  # POST
}

# Dataset-versioning read surface (P1 smoke.py + dataset server.go pins).
# The versions route (integer-only, no files), the preview endpoint
# (metadata/fallback only) and the rows endpoint (forces NDJSON objects,
# rejecting this fixture's bare-string records) are deliberately ABSENT.
# So is the file-INDEX route: its entry UUIDs are DatasetFileIndexEntry
# ids rebuilt from scratch on every publish — independent of the real
# DatasetFile ids and useless as historical-file evidence (round-2 #1).
DATASET_ROUTES = {
    "dataset_create": "/v1/datasets",                                  # POST
    "files_list": "/v1/datasets/{id}/files",                           # GET envelope
    "file_download": "/v1/datasets/{id}/files/{file_id}/download",     # GET bytes
    "txn_get": "/v1/datasets/{id}/branches/{branch}/transactions/{txn}",  # GET flat
    "view_at": "/v1/datasets/{id}/views/at",                           # GET ViewOut
}

FIELDS = {
    "incremental": "sync.incremental",
    "enabled": "incremental.enabled",
    "request_param": "incremental.request_param",
    "completed_path": "incremental.completed_checkpoint_path",
    "initial_value": "incremental.initial_value",
    "egress_flag": "egress.allow_insecure_http",
    "run_incremental_view": "incremental",
    "cp_busy_code": "sync_incremental_busy",
    "cp_stale_code": "sync_incremental_binding_stale",
    "cp_owner_unresolved_code": "sync_incremental_owner_unresolved",
    "blocked_reason_business": "requires_reconciliation",
    "tick_busy_outcome": "skipped_incremental_busy",
    "tick_stale_outcome": "error_binding_stale",
}

PROXY_MODES = ("passthrough", "drop_after_verify", "no_txn_200",
               "not_committed", "wrong_target", "http_500")

GATES = ("G_ACK_STRICT", "G_CAS_ATOMIC", "G_OWNER_DURABLE", "G_RETAIN_UNKNOWN",
         "G_NO_REPLAY", "G_EMPTY_BATCH", "G_CANDIDATE", "G_SNAPSHOT_ONLY",
         "G_COUNTERS", "G_LEDGER")

# ---------------------------------------------------------------------------
# Exact fixture content (mirrors fixtures/rest_source_p25.py BATCHES —
# Tools r3 layout). Records are BARE STRINGS "%s-%03d|name-%s%d|qty-%d";
# the connector appends each RawMessage unchanged plus a newline, so a
# downloaded line json.loads to a str and the expected raw bytes are the
# exact NDJSON of the expected strings.
# ---------------------------------------------------------------------------


def _letter_rows(letter, lo, hi):
    return ["%s-%03d|name-%s%d|qty-%d" % (letter, i, letter, i, i)
            for i in range(lo, hi + 1)]


_A_PAGES = [_letter_rows("a", 1, 12), _letter_rows("a", 13, 24)]
_B_PAGES = [_letter_rows("b", 1, 8), _letter_rows("b", 9, 16),
            _letter_rows("b", 17, 20)]
_C_PAGES = [_letter_rows("c", 1, 30)]

SRC_WINDOWS = {
    "p25h1": [_A_PAGES, _B_PAGES, _C_PAGES],      # A(12+12) B(8+8+4) C(30)
    "p25h2": [_A_PAGES, [_B_PAGES[2]]],           # A(12+12) then b17..20
    "p25h3": [_A_PAGES],                          # A only (then exhausted)
    "p25h4": [_A_PAGES, [_B_PAGES[0]]],           # A(12+12) then B(8) r3
    "p25h6": [[_A_PAGES[0]], [_B_PAGES[0]], [_C_PAGES[0]]],
    "p25h13": [[_A_PAGES[0]], [_B_PAGES[0]]],
    "p25empty": [[[]]],                           # ONE window, ONE empty
                                                  # terminal page (mirrors
                                                  # EMPTY_BATCH=[[[]]])
}


def src_token(src, window):
    """The terminal candidate token the fixture derives from window W."""
    return "%s-tok-%d" % (src, window)


def window_values(src, window):
    return [r for page in SRC_WINDOWS[src][window] for r in page]


def _ndjson(values):
    """Exact expected raw bytes for a batch of bare-string records."""
    return b"".join(json.dumps(v).encode("utf-8") + b"\n" for v in values)


# ---------------------------------------------------------------------------
# Scenario registry
# ---------------------------------------------------------------------------

SCENARIOS = [
    {"id": "h1", "phase": "healthy", "name": "baseline A->B->C cross-run incremental",
     "paths": ["sync"], "src": "p25h1", "proxy": "passthrough",
     "gates": ["G_ACK_STRICT", "G_CAS_ATOMIC", "G_LEDGER", "G_SNAPSHOT_ONLY"],
     "notes": "3 runs. Full three-way value proof per run (run facts / flat "
              "txn / per-fileID download: exact values, raw bytes, len, "
              "sha); base_epoch 0/1/2; checkpoint epoch 1/2/3 with the "
              "exact window token (len+sha); per-run ledger slices: fixed "
              "input token on EVERY page, ordered cursor chain, exact page "
              "count (2/3/1), window index 0/1/2; latest effective view "
              "after run3 == exactly the C file set."},
    {"id": "h2", "phase": "healthy", "name": "per-batch independent SNAPSHOT",
     "paths": ["sync"], "src": "p25h2", "proxy": "passthrough",
     "gates": ["G_SNAPSHOT_ONLY", "G_ACK_STRICT"],
     "notes": "V1(A 24 rows) then V2(b17..20). Both txns COMMITTED via the "
              "flat transaction GET; after V2 the views/at effective set "
              "matches the B file set as an EXACT multiset over "
              "(logical_path, normalized physical path, size, "
              "introduced_by) with unique per-file mapping and full-value "
              "downloads of only the view-selected fileIDs; V1's fileID "
              "remains historically downloadable with its exact content. "
              "The file-INDEX route is NOT used: its entry UUIDs are "
              "rebuilt per publish and are not DatasetFile ids."},
    {"id": "h3", "phase": "healthy", "name": "empty batch (exhausted position + initial empty)",
     "paths": ["sync"], "src": "p25h3", "proxy": "passthrough",
     "gates": ["G_EMPTY_BATCH", "G_CAS_ATOMIC"],
     "notes": "p25h3: run1 consumes A (token tok-0); run2 at the exhausted "
              "position: 0 rows, the SAME token re-issued -> succeeded, "
              "epoch+1, checkpoint len/sha unchanged, owner released, the "
              "run's txn-bound file downloads as exactly b''. p25empty: "
              "first request terminates with 0 rows + tok-0, same value "
              "assertions (no empty_batch knob exists)."},
    {"id": "h4", "phase": "healthy", "name": "invalid terminal candidates on a NON-EMPTY window",
     "paths": ["sync"], "src": "p25h4", "proxy": "passthrough",
     "gates": ["G_CANDIDATE"],
     "notes": "Native p25h4 (r3): seed run consumes A (tok-0, epoch 1); "
              "the fault run inputs tok-0 and REACHES the non-empty B(8) "
              "terminal page where candidate_mode in {missing, non_string, "
              "empty, oversize, ctrl_chars, echo_request_token} is applied. "
              "Each mode: run failed PRE-publish + owner released + epoch "
              "still 1 + checkpoint value unchanged + reached-proof (the "
              "fault run's ledger slice IS window 1 with token tok-0) + "
              "error must NOT contain the candidate raw value."},
    {"id": "h5", "phase": "healthy", "name": "legacy snapshot compatibility",
     "paths": ["sync"], "src": None, "proxy": None,
     "gates": [],
     "notes": "carried by the frozen predecessor regression suites; this "
              "engine registers it for attribution only (no new "
              "assertions; selftest lists it under skipped_by_design)."},
    {"id": "h6", "phase": "healthy", "name": "overlapping sibling defs + same-def busy rejection",
     "paths": ["sync", "async"], "src": "p25h6", "proxy": "passthrough",
     "gates": ["G_OWNER_DURABLE", "G_COUNTERS", "G_ACK_STRICT"],
     "notes": "def1 and def2 (same source, different datasets) hold their "
              "runs OVERLAPPING (both checkpoint owners visible "
              "simultaneously); a second manual trigger on def1 during "
              "the window -> 409 busy; zero additional dispatch for def1's "
              "target during the busy window (proxy ledger filtered by "
              "request_target); BOTH runs then succeed with full-value "
              "proofs (independent Def = independent position)."},
    {"id": "h7", "phase": "healthy", "name": "config/target/credential fences + tenant visibility",
     "paths": ["sync"], "src": "p25h4", "proxy": "passthrough",
     "gates": ["G_OWNER_DURABLE"],
     "notes": "while a run holds the owner (slow terminal): PATCH sync "
              "output_dataset_id (to a REAL second dataset) / file_glob, "
              "PATCH connection config, POST credentials -> 409 "
              "owner_unresolved; pure name / cron / enable PATCHes -> 200 "
              "with before/after checkpoint snapshots EQUAL; org-2 token "
              "reads checkpoint + run -> 404 (cfg.jwt_org2 REQUIRED — a "
              "missing org-2 JWT fails the gate, it is never skipped); "
              "after release a REAL config change makes the next trigger "
              "409 binding_stale."},
    {"id": "h8", "phase": "healthy", "name": "pull/paging failures; limits; retry integrity",
     "paths": ["sync"], "src": "p25h1", "proxy": "passthrough",
     "gates": ["G_CANDIDATE", "G_LEDGER"],
     "notes": "fail_mid_pages and fail_terminal_after_rows -> failed + "
              "owner released + position unchanged (epoch 0) + retry "
              "re-pulls the WHOLE batch (ledger slice of the retry run is "
              "the full window again, same input token); max_pages=1 on "
              "the 2-page window -> explicit limit error naming max_pages, "
              "no truncation, position unchanged."},
    {"id": "h9", "phase": "healthy", "name": "pre-publish cancel",
     "paths": ["async"], "src": "p25h6", "proxy": "passthrough",
     "gates": ["G_RETAIN_UNKNOWN"],
     "notes": "cancel while pulling: run aborted, publish_entered_at NULL "
              "(absent in the run view), ZERO commit dispatch for this "
              "target in the proxy ledger, owner released, position "
              "unchanged; the next trigger re-runs from the same token and "
              "succeeds with full-value proof."},
    {"id": "h10", "phase": "healthy", "name": "driver-coordinated CRASH probes (two-stage)",
     "paths": ["sync"], "src": "p25h3", "proxy": "passthrough",
     "gates": ["G_RETAIN_UNKNOWN"],
     "notes": "EXCLUDED from the default healthy phase; the driver "
              "invokes --only h10 explicitly. pre does NOT require "
              "global quiet: the --h10-held-def source is expected "
              "genuinely IN FLIGHT (real owner-held run via "
              "request_timeout_ms=30000 + 15s terminal hold) while the "
              "other observed defs are quiet-succeeded. The crash is a "
              "deliberate SIGKILL of the disposable p25connector + start "
              "of the SAME accepted image (a graceful restart would "
              "correctly cancel/release and break the immutability "
              "expectation); the STRICTLY-validated crash record "
              "(--h10-restart-record) carries concrete identities, raw "
              "kill/start cmd rc + artifact SHAs, StartedAt "
              "before!=after, ordered timestamps, held identities and "
              "the prior snapshot sha — restarted:true alone never "
              "passes. post requires REAL quiet and NO adoption/advance "
              "(identical run ids AND per-run protected state; the held "
              "owner row unchanged, blocked_reason may legitimately flip "
              "to the business word after lease expiry)."},
    {"id": "h11", "phase": "tail", "name": "strict-ACK single-predicate negatives",
     "paths": ["sync"], "src": "p25h1",
     "proxy": "no_txn_200|not_committed|wrong_target|http_500",
     "gates": ["G_ACK_STRICT", "G_RETAIN_UNKNOWN"],
     "notes": "each fabricated mode breaks EXACTLY ONE ACK predicate; the "
              "engine filters the proxy ledger by request_target == THIS "
              "dataset and asserts an exact one-entry delta for the "
              "attempt (forwarded=false; fabricated_status 200 for the "
              "three body faults, 502 for http_500) plus the connector's "
              "LOCAL outcome (failed + owner retained + epoch 0). No "
              "'remote has no new version' claim is ever made."},
    {"id": "h12", "phase": "healthy", "name": "same-tx advance+terminal rollback (isolated PG)",
     "paths": ["pg"], "src": None, "proxy": None,
     "gates": ["G_CAS_ATOMIC"],
     "notes": "owned by the PG track. This engine binds the FROZEN "
              "pg-candidate-bindings-r17.json: required names come from "
              "the referenced required-names file (never self-selected), "
              "the pinned rollback name must be among them, each stage's "
              "raw/rc/meta artifacts must exist with rc==0, source "
              "content sha before==after==candidate_source_sha256, and "
              "every required name must PASS in the new-normal raw. The "
              "engine re-hashes the consumed artifacts and records the "
              "hashes; it never fabricates PG coverage."},
    {"id": "h13", "phase": "healthy", "name": "scheduled batches; REAL busy tick with SQL facts; quiet",
     "paths": ["scheduled"], "src": "p25h13", "proxy": "passthrough",
     "gates": ["G_OWNER_DURABLE", "G_CAS_ATOMIC", "G_LEDGER"],
     "notes": "round-3 #1: ticks sorted scheduled_for ASC. Phase S: >=2 "
              "enqueued ticks, every tick.run_id a REAL succeeded run "
              "(full-value). Phase D: DEDICATED FRESH scheduled def; "
              "source request_timeout_ms=30000 + terminal hold 15000, "
              "manual hold run started 3s BEFORE the actual next due; "
              "SQL baseline AFTER manual admission and BEFORE the busy "
              "tick (manual admission legitimately advances the stamp), "
              "post snapshot after the observed busy tick while the "
              "owner is still held — actual SQL proves stamp unchanged, "
              "cron cursor advanced, NO new run row, DATA checkpoint "
              "unchanged (readonly reader via --sql-env-dir; a missed "
              "busy window is reported as a miss, never claimed). "
              "Revoke+disable before the following due, then quiet."},
    {"id": "h14", "phase": "tail", "name": "lost-ACK unknown (controlled tail)",
     "paths": ["sync"], "src": "p25h1", "proxy": "drop_after_verify",
     "gates": ["G_RETAIN_UNKNOWN", "G_NO_REPLAY", "G_ACK_STRICT"],
     "notes": "drop_after_verify: filter the proxy ledger by "
              "request_target == this dataset, exactly ONE incremental "
              "dispatch, real key ack_txn_id; run.dataset_version_id MUST "
              "be EMPTY (that is the lost-ACK definition). Remote proof = "
              "INDEPENDENT flat txn GET of ack_txn_id (COMMITTED, exact "
              "dataset, main, SNAPSHOT, lineage.sync_run_id == run.id) + "
              "full-value download of the file bound to that txn (values/"
              "raw/len/sha). Local: failed + owner retained + epoch 0 + "
              "re-trigger 409. ONLY the scenario actually performed is "
              "declared (no driver placeholder rows)."},
    {"id": "h15", "phase": "healthy", "name": "busy manual trigger + ONE real async success",
     "paths": ["async"], "src": "p25h6", "proxy": "passthrough",
     "gates": ["G_OWNER_DURABLE"],
     "notes": "manual busy = REAL sync POST (no async parameter) during a "
              "slow async run -> 409 busy + ZERO new run rows + zero "
              "dispatch for this target + data position untouched; the "
              "async run then completes ONE real successful batch "
              "(full-value proof)."},
]

# ---------------------------------------------------------------------------
# Evidence discipline
# ---------------------------------------------------------------------------


class AttemptRecorder:
    """Every HTTP call (or probe) becomes one immutable raw record file:
    scenario/attempt-NNNN.json with -rN collision suffixes. Records never
    contain Authorization/JWT/DSN values. Failures are kept, never
    overwritten, and every record carries its attribution.

    v3: persist_handle() writes handles.json IMMEDIATELY (rewritten
    atomically on every handle) so quiesce owns ALL created objects —
    sources/defs/datasets/runs, including FAILED creation attempts.
    """

    def __init__(self, evidence_dir, phase):
        self.dir = os.path.join(evidence_dir, "p25-%s-%s" % (phase, _ts()))
        os.makedirs(self.dir, exist_ok=True)
        self.seq = 0
        self.handles = []
        self.handles_path = os.path.join(self.dir, "handles.json")
        self._flush_handles()

    def record(self, scenario, kind, detail):
        self.seq += 1
        safe = json.dumps(detail, ensure_ascii=False, sort_keys=True,
                          default=str)
        name = "%s-attempt-%04d" % (scenario, self.seq)
        path = os.path.join(self.dir, name + ".json")
        n = 0
        while os.path.exists(path):
            n += 1
            path = os.path.join(self.dir, "%s-r%d.json" % (name, n))
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(safe + "\n")
        return path

    def persist_handle(self, kind, ref, info=None):
        """Register a created object (or a failed creation attempt with
        ok=False) and flush handles.json right away — never batched."""
        row = {"kind": kind, "ref": ref, "ok": True, "at": _ts()}
        if info:
            row.update(info)
        self.handles.append(row)
        self._flush_handles()
        return row

    def persist_failed_handle(self, kind, ref, info=None):
        row = {"kind": kind, "ref": ref, "ok": False, "at": _ts()}
        if info:
            row.update(info)
        self.handles.append(row)
        self._flush_handles()
        return row

    def _flush_handles(self):
        tmp = self.handles_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump({"handles": self.handles,
                       "count": len(self.handles)}, fh, indent=1,
                      ensure_ascii=False, default=str)
        os.replace(tmp, self.handles_path)

    def summary(self):
        return {"evidence_dir": self.dir, "records": self.seq,
                "handles": len(self.handles),
                "handles_file": self.handles_path}


def _ts():
    return time.strftime("%Y%m%dT%H%M%S", time.gmtime())


def _sha256(data):
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _new_uuid():
    import uuid
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# HTTP layer (REAL; only reachable in released mode)
# ---------------------------------------------------------------------------

_MAX_RECORDED_BODY = 65536


class HttpApi:
    """Thin stdlib HTTP client with per-call evidence recording. `bases`
    maps: connector / dataset / fixture / proxy_admin -> base URL strings.
    The jwt is used for the Authorization header and is NEVER recorded.

    v3: JSON responses record their FULL parsed body (safely capped) so
    CP/txn/ledger assertions are replayable from evidence alone; raw
    downloads record sha256+bytes only.
    """

    def __init__(self, bases, jwt, recorder, scenario):
        self.bases = bases
        self.jwt = jwt
        self.rec = recorder
        self.scenario = scenario

    def call(self, base_key, method, path, body=None, timeout=30,
             raw=False, auth=True):
        url = self.bases[base_key].rstrip("/") + path
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if auth and self.jwt:
            headers["Authorization"] = (
                self.jwt if self.jwt.lower().startswith("bearer ")
                else "Bearer " + self.jwt)
        req = urllib.request.Request(url, data=data, headers=headers,
                                     method=method)
        status, payload, err = None, None, None
        started = time.time()
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                status = resp.status
                content = resp.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
            content = exc.read()
        except Exception as exc:            # transport-level failure
            err = "%s: %s" % (type(exc).__name__, exc)
            content = b""
        elapsed_ms = int((time.time() - started) * 1000)
        text = content.decode("utf-8", "replace")
        body_rec = None
        if not raw:
            try:
                payload = json.loads(text) if text else None
            except ValueError:
                payload = None
            if text:
                body_rec = text if len(text) <= _MAX_RECORDED_BODY else \
                    text[:_MAX_RECORDED_BODY] + "...<truncated>"
        else:
            payload = content
        detail = {
            "base": base_key, "method": method,
            "path": _strip_query(url), "query": _query_of(url),
            "status": status, "elapsed_ms": elapsed_ms, "error": err,
            "resp_sha256": _sha256(content) if content else None,
            "resp_bytes": len(content),
            "request_body_sha256": _sha256(data) if data else None,
        }
        if body_rec is not None:
            detail["resp_body"] = body_rec      # full safe JSON body
        self.rec.record(self.scenario, "http", detail)
        return {"status": status, "json": payload, "raw": content,
                "error": err, "text": text}


def _strip_query(url):
    return url.split("?", 1)[0]


def _query_of(url):
    q = url.split("?", 1)
    return q[1] if len(q) == 2 else ""


def route(name, **kw):
    path = ROUTES[name]
    for k, v in kw.items():
        path = path.replace("{%s}" % k, str(v))
    return path


def dataset_route(name, **kw):
    path = DATASET_ROUTES[name]
    for k, v in kw.items():
        path = path.replace("{%s}" % k, str(v))
    return path


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

class GateResult:
    def __init__(self, name, ok, detail, evidence=None):
        self.name, self.ok, self.detail = name, ok, detail
        self.evidence = evidence or []

    def to_json(self):
        return {"gate": self.name, "ok": self.ok, "detail": self.detail,
                "evidence": self.evidence}


class ScenarioResult:
    def __init__(self, sid, phase):
        self.id, self.phase = sid, phase
        self.gates = []
        self.owner_roster = []   # ACTUAL retained checkpoint owners
        self.run_roster = []     # ACTUAL nonterminal run rows
        self.failed = False

    def gate(self, name, ok, detail, evidence=None):
        self.gates.append(GateResult(name, bool(ok), detail, evidence))
        if not ok:
            self.failed = True
        return bool(ok)

    def to_json(self):
        return {"id": self.id, "phase": self.phase,
                "gates": [g.to_json() for g in self.gates],
                "owner_roster": self.owner_roster,
                "run_roster": self.run_roster, "failed": self.failed}


def incremental_source_config(base_url, src, extra=None):
    """Source config for the owned HTTP fixture. v3: the per-source egress
    override config.egress.allow_insecure_http=true is REQUIRED — the
    fixture is plain http:// and the aux-host allowlist alone does not
    enable insecure transport (rest_snapshot.go policy(): AllowInsecureHTTP
    defaults false; only an explicit source override can relax it)."""
    cfg = {
        "base_url": base_url,
        "permissions": {"discoverable": True, "syncable": True,
                        "invokable": False, "usable_in_code": False},
        "sync": {
            "resource_path": "/data",
            "records_path": "records",
            "pagination": {"type": "cursor", "cursor_param": "cursor",
                           "next_cursor_path": "next_cursor"},
            "limits": {"max_pages": 20, "max_records": 500,
                       "max_response_bytes": 8388608,
                       "max_total_bytes": 33554432,
                       "request_timeout_ms": 15000},
            "query_params": {"src": src},
            "incremental": {"enabled": True,
                            "request_param": P25_SINCE_PARAM,
                            "completed_checkpoint_path": P25_COMPLETED_PATH},
        },
        "egress": {"allow_insecure_http": True},
    }
    if extra:
        cfg = _deep_merge(cfg, extra)
    return cfg


def _deep_merge(a, b):
    out = copy.deepcopy(a)
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def expect_run_terminal(api, sync_id, run_id, timeout_s=60):
    """Poll the run until terminal or timeout; returns (run_json, ok)."""
    deadline = time.time() + timeout_s
    run = None
    while time.time() < deadline:
        resp = api.call("connector", "GET", route("run_get", id=sync_id,
                                                  run_id=run_id))
        run = resp["json"]
        if run and run.get("status") in ("succeeded", "failed", "aborted"):
            return run, True
        time.sleep(0.5)
    return run, False


def checkpoint_of(api, sync_id):
    resp = api.call("connector", "GET", route("checkpoint_view", id=sync_id))
    return resp["json"] if resp["status"] == 200 else None


def runs_list(api, sync_id):
    resp = api.call("connector", "GET", route("sync_runs", id=sync_id))
    if resp["status"] != 200 or resp["json"] is None:
        return None
    body = resp["json"]
    if isinstance(body, dict) and isinstance(body.get("runs"), list):
        return body["runs"]
    return body if isinstance(body, list) else None


def def_view(api, sync_id):
    """GET the def itself (source join for parent-chain facts)."""
    resp = api.call("connector", "GET", route("sync_get", id=sync_id))
    if resp["status"] != 200 or not isinstance(resp["json"], dict):
        return None
    return resp["json"]


# ---- dataset read helpers (P1-verified surface; /v1 prefix) --------------


def dataset_create(api, name):
    resp = api.call("dataset", "POST", dataset_route("dataset_create"),
                    body={"name": name})
    return (resp["json"] if resp["status"] in (200, 201) else None), resp


def files_envelope(api, dataset_id):
    """GET /v1/datasets/{id}/files -> {branch,total,files:[...]} (the
    HISTORICAL file index, not the effective set)."""
    resp = api.call("dataset", "GET", dataset_route("files_list",
                                                    id=dataset_id))
    if resp["status"] != 200 or not isinstance(resp["json"], dict):
        return None
    return resp["json"]


def bind_run_files(envelope, txn_id):
    """The run's files = envelope entries with transaction_id == the run's
    commit transaction UUID (run.dataset_version_id)."""
    if not envelope:
        return []
    return [f for f in (envelope.get("files") or [])
            if f.get("transaction_id") == txn_id]


def download_file(api, dataset_id, file_id):
    resp = api.call("dataset", "GET", dataset_route(
        "file_download", id=dataset_id, file_id=file_id), raw=True)
    return resp


def txn_get(api, dataset_id, txn, branch="main"):
    """GET .../branches/main/transactions/{txnUUID} -> FLAT
    RuntimeTransaction (no nested transaction, no files)."""
    resp = api.call("dataset", "GET", dataset_route(
        "txn_get", id=dataset_id, branch=branch, txn=txn))
    if resp["status"] != 200 or not isinstance(resp["json"], dict):
        return None
    return resp["json"]


def view_at(api, dataset_id, branch="main"):
    """GET .../views/at?branch=main -> ViewOut with the EFFECTIVE file set
    (files[].introduced_by = the introducing transaction UUID)."""
    resp = api.call("dataset", "GET", dataset_route("view_at",
                                                    id=dataset_id) +
                    "?branch=" + branch)
    if resp["status"] != 200 or not isinstance(resp["json"], dict):
        return None
    return resp["json"]


def ndjson_values(raw_bytes):
    """Parse downloaded NDJSON; every line must load (bare strings here)."""
    rows = []
    for line in raw_bytes.decode("utf-8", "strict").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


# ---- fixture / proxy admin ------------------------------------------------


def proxy_stats(api):
    resp = api.call("proxy_admin", "GET", "/admin/stats")
    return resp["json"] if resp["status"] == 200 else None


def proxy_log(api):
    resp = api.call("proxy_admin", "GET", "/admin/log")
    return resp["json"] if resp["status"] == 200 else []


def proxy_set_mode(api, mode):
    resp = api.call("proxy_admin", "POST", "/admin/mode", body={"mode": mode})
    return resp["status"] == 200


def fixture_ledger(api, src):
    resp = api.call("fixture", "GET", "/admin/log/%s" % src)
    return resp["json"] if resp["status"] == 200 else []


def fixture_snapshot(api, src):
    resp = api.call("fixture", "GET", "/admin/src/%s" % src)
    return resp["json"] if resp["status"] == 200 else None


def fixture_set_knob(api, src, knob):
    resp = api.call("fixture", "POST", "/admin/knob/%s" % src, body=knob)
    return resp["status"] == 200


def _is_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def _owner_fence_message(def_id, run_id):
    return ("sync has an unresolved incremental checkpoint owner: "
            "sync def has an unresolved incremental checkpoint owner: def=%s run=%s" % (def_id, run_id))


def _mutation_fence_error(response, branch, def_id, run_id):
    body = response.get("json")
    if response.get("status") != 409 or not isinstance(body, dict):
        return None
    expected = ("source config cannot change while pending or running sync runs exist"
                if branch == "config" else _owner_fence_message(def_id, run_id))
    return body.get("error") if body.get("error") == expected else None


def _h15_data_position(response):
    body = response.get("json")
    if (response.get("status") == 404 and isinstance(body, dict)
        and body.get("error") == "incremental checkpoint not initialized for this sync definition"):
        return {"mode": "cursor", "initialized": False, "confirmed_epoch": 0,
                "checkpoint_len": None, "checkpoint_sha256": None, "last_confirmed": None}
    if response.get("status") != 200 or not isinstance(body, dict) or not _is_int(body.get("confirmed_epoch")):
        return None
    return {key: body.get(key) for key in ("mode", "initialized", "confirmed_epoch",
                    "checkpoint_len", "checkpoint_sha256", "last_confirmed")}


def _vf_path(p):
    """Normalize a physical/logical path for multiset comparison:
    strip a local:/// scheme prefix and outer slashes (dataset-http-read
    -facts #7: explicit local:/// normalization where applicable)."""
    if not isinstance(p, str):
        return p
    prefix = "local:///"
    if p.startswith(prefix):
        p = p[len(prefix):]
    return p.strip("/")


def _rfc3339(v):
    """Parse an RFC3339 timestamp to a float epoch (0.0 when absent or
    malformed so sorting never throws)."""
    if not isinstance(v, str) or not v:
        return 0.0
    try:
        import datetime
        text = v.replace("Z", "+00:00")
        return datetime.datetime.fromisoformat(text).timestamp()
    except ValueError:
        return 0.0


def _parent_verdict_count(lines, prefix, name):
    """Count exact PARENT verdict lines for `name` — subtest verdicts
    ("--- PASS: Name/sub") never match because of the boundary char."""
    n = 0
    for line in lines:
        if not line.startswith(prefix + name):
            continue
        rest = line[len(prefix) + len(name):]
        if rest == "" or rest.startswith(" ("):
            n += 1
    return n


def _parent_pass_count(lines, name):
    return _parent_verdict_count(lines, "--- PASS: ", name)


def _arm_ready(view):
    """POST schedule:arm readiness (schedule_auth.go:666-674): the wire
    body carries {armed: true, grant_id, actor_subject,
    schedule_revision, authorization_expires_at,
    scheduler_loop_running: bool, note} — there is NO 'live'/'running'
    field (a GET-auth live view is a different contract, never conflated
    here). Both flags must be exactly True."""
    return (isinstance(view, dict)
            and view.get("armed") is True
            and view.get("scheduler_loop_running") is True)


# ---------------------------------------------------------------------------
# round-3 #1: injectable READONLY SQL fact reader (owned by THIS engine,
# not by caller JSON or a hidden Runtime handshake). Actual IO happens
# ONLY behind the existing --released + P25_CONFIRM gate: a module
# tripwire refuses otherwise, so pure/mock runs can never touch a DB.
# ---------------------------------------------------------------------------

_SQL_RELEASE_ARMED = {"armed": False}


class SqlFactRefused(Exception):
    """Raised when SQL fact IO is attempted while not released."""


def _sql_fact_selects(def_id, source_id):
    """EXACT constant SELECTs (labels make -At output self-delimiting);
    UUIDs are the engine's OWN created Def/Source ids only."""
    return [
        "SELECT 'FACT_STAMP|' || coalesce(sync_last_admitted_at::text,"
        " 'NULL') FROM connections WHERE id = '%s'" % source_id,
        "SELECT 'FACT_NEXTFIRE|' || coalesce(schedule_next_fire_at::text,"
        " 'NULL') FROM batch_sync_defs WHERE id = '%s'" % def_id,
        "SELECT 'FACT_CP|' || confirmed_epoch::text || '|' ||"
        " coalesce(octet_length(checkpoint_value)::text,'NULL') || '|' ||"
        " coalesce(encode(sha256(checkpoint_value::bytea),'hex'),'NULL')"
        " || '|' || coalesce(active_run_id::text,'NULL')"
        " FROM sync_def_checkpoints WHERE sync_def_id = '%s'" % def_id,
        "SELECT 'FACT_TICK|' || scheduled_for::text || '|' ||"
        " coalesce(outcome,'NULL') || '|' ||"
        " coalesce(run_id::text,'NULL') FROM sync_schedule_ticks"
        " WHERE sync_def_id = '%s' ORDER BY scheduled_for" % def_id,
        "SELECT 'FACT_RUN|' || id::text || '|' || status FROM sync_runs"
        " WHERE sync_def_id = '%s' ORDER BY started_at" % def_id,
    ]


def parse_sql_facts(stdout_text):
    """Parse labelled psql -At output into the fact dict (pure)."""
    facts = {"stamp": None, "next_fire": None,
             "cp": {"epoch": None, "len": None, "sha": None,
                    "active_run_id": None},
             "ticks": [], "runs": []}
    for line in (stdout_text or "").splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("FACT_STAMP|"):
            facts["stamp"] = line.split("|", 1)[1]
        elif line.startswith("FACT_NEXTFIRE|"):
            facts["next_fire"] = line.split("|", 1)[1]
        elif line.startswith("FACT_CP|"):
            parts = line.split("|")
            if len(parts) == 5:
                facts["cp"] = {"epoch": parts[1], "len": parts[2],
                               "sha": parts[3], "active_run_id": parts[4]}
        elif line.startswith("FACT_TICK|"):
            parts = line.split("|")
            if len(parts) == 4:
                facts["ticks"].append({"scheduled_for": parts[1],
                                       "outcome": parts[2],
                                       "run_id": parts[3]})
        elif line.startswith("FACT_RUN|"):
            parts = line.split("|")
            if len(parts) == 3:
                facts["runs"].append({"id": parts[1], "status": parts[2]})
    return facts


def _sql_facts_complete(facts):
    if not isinstance(facts, dict) or not _rfc3339(facts.get("stamp")) or not _rfc3339(facts.get("next_fire")):
        return False
    cp = facts.get("cp") or {}
    try:
        if int(cp.get("epoch")) < 0:
            return False
        if cp.get("len") == "NULL":
            if cp.get("sha") != "NULL": return False
        elif int(cp.get("len")) < 0 or not _is_sha256hex(cp.get("sha")):
            return False
    except (TypeError, ValueError):
        return False
    if cp.get("active_run_id") != "NULL" and not _is_uuidish(cp.get("active_run_id")):
        return False
    return all(_is_uuidish(r.get("id")) and r.get("status") in
               ("pending", "running", "succeeded", "failed", "aborted")
               for r in facts.get("runs", []))


class PsqlFactReader:
    """Readonly q3 SQL facts via fixed docker compose exec postgres psql.
    The q3 env directory supplies compose.yaml; no host PG port or DSN. The cmd is written to evidence BEFORE execution and
    the DSN is NEVER recorded; raw stdout/stderr, the ORIGINAL rc and
    their SHAs land in the same attempt record. `snapshot` may only be
    called for the engine's OWN created Def/Source UUIDs."""

    def __init__(self, env_dir, recorder, scenario="sql-facts"):
        self.env_dir = env_dir
        self.rec = recorder
        self.scenario = scenario
        self.env = dict(os.environ)
        self.compose_file = os.path.abspath(os.path.join(env_dir, "compose.yaml"))
        if not os.path.isfile(self.compose_file):
            raise ValueError("q3 env dir must contain compose.yaml")

    def _own_uuid(self, value, kinds):
        if not _is_uuidish(value):
            raise ValueError("refusing non-UUID fact target %r" % (value,))
        for h in self.rec.handles:
            if h.get("kind") in kinds and h.get("ref") == value:
                return
        raise ValueError("fact target %r is not one of this engine's own "
                         "created %s handles" % (value, kinds))

    def snapshot(self, tag, def_id, source_id):
        self._own_uuid(def_id, ("def",))
        self._own_uuid(source_id, ("source",))
        if not _SQL_RELEASE_ARMED["armed"]:
            raise SqlFactRefused(
                "SQL fact IO is not released (--released + %s=%s)"
                % (RELEASE_ENV, RELEASE_ENV_VALUE))
        sql = "BEGIN READ ONLY;\n" + ";\n".join(
            _sql_fact_selects(def_id, source_id)) + ";\nROLLBACK;"
        argv = ["docker", "compose", "-f", self.compose_file,
                "--project-name", "openfoundry-platform-p1-q3v9", "exec", "-T",
                "postgres", "psql", "-U", "openfoundry", "-d", "connectors",
                "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", sql]
        cmd_file = os.path.join(
            self.rec.dir, "%s-sqlcmd-%04d.cmd" % (tag, self.rec.seq + 1))
        with open(cmd_file, "w", encoding="utf-8") as fh:   # BEFORE exec
            fh.write(" ".join(
                str(a)
                for i, a in enumerate(argv)) + "\n")
        import subprocess
        captured_at = time.time()
        try:
            proc = subprocess.run(argv, capture_output=True, timeout=30,
                                  env=self.env)
            rc, out, err = proc.returncode, proc.stdout, proc.stderr
        except Exception as exc:               # honest failure, recorded
            rc, out, err = -1, b"", type(exc).__name__.encode()
        out_file = os.path.join(self.rec.dir, "%s.stdout" % os.path.basename(cmd_file))
        err_file = os.path.join(self.rec.dir, "%s.stderr" % os.path.basename(cmd_file))
        for path, raw in ((out_file, out), (err_file, err)):
            with open(path, "wb") as fh:
                fh.write(raw)
        out_text = out.decode("utf-8", "replace")
        err_text = err.decode("utf-8", "replace")
        facts = parse_sql_facts(out_text) if rc == 0 else None
        self.rec.record(self.scenario, "sql-facts", {
            "tag": tag, "def_id": def_id, "source_id": source_id,
            "cmd_file": os.path.basename(cmd_file),
            "cmd_sha256": _file_sha256(cmd_file), "captured_at": captured_at,
            "stdout_file": out_file, "stderr_file": err_file,
            "rc": rc,
            "stdout": out_text[:16384], "stderr": err_text[:4096],
            "stdout_sha256": _sha256(out), "stderr_sha256": _sha256(err),
            "parsed_ok": _sql_facts_complete(facts),
        })
        return {"ok": rc == 0 and _sql_facts_complete(facts),
                "rc": rc, "facts": facts, "captured_at": captured_at}


class FakeSqlFactReader:
    """Pure stand-in with the same snapshot() contract, deriving facts
    from the fake connector state (stamp advances on every ADMISSION —
    manual admission legitimately advances it; the busy tick does not)."""

    def __init__(self, connector, recorder):
        self.connector = connector
        self.rec = recorder

    def snapshot(self, tag, def_id, source_id):
        c = self.connector
        d = c.defs[def_id]
        val = d["value"]
        runs = [_public_run(r) for r in c.runs.values()
                if r.get("sync_def_id") == def_id]
        facts = {"stamp": c.sources[source_id].get("last_admitted"),
                 "next_fire": d.get("schedule_next_fire_at"),
                 "cp": {"epoch": str(d["epoch"]),
                        "len": str(len(val)) if val is not None else "NULL",
                        "sha": _sha256(val) if val is not None else "NULL",
                        "active_run_id": d["owner"] or "NULL"},
                 "ticks": [dict(t) for t in c.tick_history.get(def_id, [])],
                 "runs": [{"id": r["id"], "status": r["status"]}
                          for r in runs]}
        self.rec.record(tag, "mocked-sql-facts", {
            "def_id": def_id, "source_id": source_id, "facts": facts})
        return {"ok": True, "rc": 0, "facts": facts}


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class P25Engine:
    """Scenario orchestration. `api_factory(scenario_id, auth='main')`
    yields the Api (real HttpApi or FakeApi in selftest); `cfg` carries
    ids/ds/jwts/h10 stage parameters."""

    def __init__(self, api_factory, cfg, recorder):
        self.api_factory = api_factory
        self.cfg = cfg
        self.rec = recorder

    # -- creation helpers (every attempt persists a handle immediately) ---

    def create_source(self, api, name, cfg, scenario=None):
        body = {"name": name, "connector_type": "rest_api", "config": cfg}
        resp = api.call("connector", "POST", route("source_create"),
                        body=body)
        ok = resp["status"] in (200, 201) and resp["json"]
        if ok:
            self.rec.persist_handle("source", resp["json"].get("id"),
                                    {"name": name, "scenario": scenario,
                                     "config": cfg})
            return resp["json"]
        self.rec.persist_failed_handle(
            "source", None,
            {"name": name, "scenario": scenario, "status": resp["status"],
             "error": resp["error"]})
        return None

    def create_dataset(self, api, name, scenario=None):
        ds, resp = dataset_create(api, name)
        if ds:
            self.rec.persist_handle("dataset", ds.get("id"),
                                    {"name": name, "scenario": scenario})
            return ds
        self.rec.persist_failed_handle(
            "dataset", None,
            {"name": name, "scenario": scenario, "status": resp["status"],
             "error": resp["error"]})
        return None

    def make_sync_def(self, api, name, source_id, output_dataset_id,
                      extra_cfg=None, scenario=None):
        """v3: extra_cfg is honoured — schedule_cron / schedule_time_zone /
        schedule_enabled / file_glob enter the creation body (h13 arms a
        REAL schedule instead of patching later)."""
        body = {"source_id": source_id, "output_dataset_id": output_dataset_id}
        for key in ("schedule_cron", "schedule_time_zone", "schedule_enabled",
                    "file_glob"):
            if extra_cfg and key in extra_cfg:
                body[key] = extra_cfg[key]
        resp = api.call("connector", "POST", route("sync_create"), body=body)
        ok = resp["status"] in (200, 201) and resp["json"]
        if ok:
            self.rec.persist_handle(
                "def", resp["json"].get("id"),
                {"name": name, "scenario": scenario,
                 "schedule_cron": body.get("schedule_cron"),
                 "file_glob": body.get("file_glob")})
            return resp["json"], resp
        self.rec.persist_failed_handle(
            "def", None,
            {"name": name, "scenario": scenario, "status": resp["status"],
             "error": resp["error"]})
        return None, resp

    def trigger_run(self, api, sync_id, async_mode=False, wait=True,
                    scenario=None):
        """POST the run trigger; the attempt (even a 409 rejection) is a
        persisted handle immediately."""
        path = route("sync_run", id=sync_id) + \
            ("?async=true" if async_mode else "")
        resp = api.call("connector", "POST", path, body={})
        run = resp["json"]
        if resp["status"] in (200, 201, 202) and run and run.get("id"):
            self.rec.persist_handle(
                "run", run["id"],
                {"def": sync_id, "scenario": scenario, "async": async_mode,
                 "initial_status": run.get("status")})
        else:
            self.rec.persist_failed_handle(
                "run", None,
                {"def": sync_id, "scenario": scenario, "async": async_mode,
                 "status": resp["status"],
                 "code": (run or {}).get("code") if isinstance(run, dict)
                 else None})
        # POST terminal SyncRun omits the GET-only incremental projection.
        # Every successful wait=True trigger must read the canonical GET.
        if not wait or not run or not run.get("id") or resp["status"] not in (200, 201, 202):
            return run, resp
        final, ok = expect_run_terminal(api, sync_id, run["id"])
        return (final if ok else run), resp

    # -- shared verification blocks ---------------------------------------

    def verify_run_artifact(self, api, res, ds_id, run, values, gate,
                            tag):
        """Full three-way exact-value proof for ONE succeeded run:
        run facts / flat transaction / per-fileID download. Returns the
        bound file_meta (or None)."""
        want_raw = _ndjson(values)
        want_sha = _sha256(want_raw)
        if not res.gate(gate, run and run.get("status") == "succeeded",
                        "%s: run status=%r" % (tag, run and run.get("status"))):
            return None
        txn_id = run.get("dataset_version_id")
        facts_ok = (isinstance(txn_id, str) and txn_id
                    and run.get("content_hash") == want_sha
                    and run.get("rows_written") == len(values)
                    and run.get("bytes_written") == len(want_raw))
        res.gate(gate, facts_ok,
                 "%s: run facts txn=%r rows=%r/%d bytes=%r/%d sha=%s" %
                 (tag, txn_id, run.get("rows_written"), len(values),
                  run.get("bytes_written"), len(want_raw),
                  (run.get("content_hash") or "")[:12]))
        env = files_envelope(api, ds_id)
        bound = bind_run_files(env, txn_id) if env is not None else []
        if not res.gate(gate, bool(bound),
                        "%s: files envelope has txn-bound file (%d of %d)" %
                        (tag, len(bound),
                         len((env or {}).get("files") or []))):
            return None
        fm = bound[0]
        dl = download_file(api, ds_id, fm["id"])
        raw = dl["raw"] or b""
        rows = ndjson_values(raw) if dl["status"] == 200 else None
        res.gate(gate, rows == values and raw == want_raw,
                 "%s: download exact values/raw (%d rows, %d bytes, sha=%s)"
                 % (tag, len(rows or []), len(raw), _sha256(raw)[:12]))
        txn = txn_get(api, ds_id, txn_id)
        meta = (txn or {}).get("metadata") or {}
        lineage = meta.get("lineage") or {}
        txn_ok = (txn is not None
                  and txn.get("id") == txn_id
                  and txn.get("dataset_id") == ds_id
                  and txn.get("branch_name") == "main"
                  and txn.get("tx_type") == "SNAPSHOT"
                  and txn.get("status") == "COMMITTED"
                  and lineage.get("sync_run_id") == run.get("id")
                  and meta.get("content_hash") == want_sha
                  and meta.get("rows") == len(values)
                  and meta.get("bytes") == len(want_raw))
        res.gate(gate, txn_ok,
                 "%s: flat txn COMMITTED/SNAPSHOT/main/lineage/hash "
                 "(status=%r tx_type=%r)" %
                 (tag, (txn or {}).get("status"), (txn or {}).get("tx_type")))
        return fm

    def verify_checkpoint_value(self, api, res, sync_id, src, epoch,
                                gate, tag):
        """Checkpoint after a successful window run: exact epoch + the
        exact window token (len + sha256), owner released."""
        cp = checkpoint_of(api, sync_id) or {}
        tok = src_token(src, epoch - 1)
        ok = (cp.get("confirmed_epoch") == epoch
              and cp.get("initialized") is True
              and cp.get("checkpoint_len") == len(tok)
              and cp.get("checkpoint_sha256") == _sha256(tok)
              and not (cp.get("owner") or {}).get("run_id"))
        res.gate(gate, ok, "%s: cp epoch=%r len=%r sha=%r owner=%r" %
                 (tag, cp.get("confirmed_epoch"), cp.get("checkpoint_len"),
                  (cp.get("checkpoint_sha256") or "")[:12],
                  (cp.get("owner") or {}).get("run_id")))
        return cp

    def verify_ledger_slice(self, api, res, src, start_idx, input_token,
                            window, pages, gate, tag):
        """Per-run fixture ledger slice: exact page count, fixed input
        token on EVERY page, ordered cursor chain (first empty then
        non-empty), correct window index."""
        led = fixture_ledger(api, src)
        sl = led[start_idx:] if isinstance(led, list) else []
        toks_ok = all(e.get("token") == input_token for e in sl)
        cursor_ok = bool(sl) and sl[0].get("cursor") in ("", None) and \
            all(e.get("cursor") for e in sl[1:])
        win_ok = all(e.get("window") == window for e in sl)
        ok = (len(sl) == pages and toks_ok and cursor_ok and win_ok)
        res.gate(gate, ok,
                 "%s: ledger slice pages=%d/%d tokens_ok=%s cursor_ok=%s "
                 "window_ok=%s" % (tag, len(sl), pages, toks_ok, cursor_ok,
                                   win_ok))
        return len(led) if isinstance(led, list) else start_idx

    def verify_latest_view(self, api, res, ds_id, head_txn, known_files,
                           values, gate, tag):
        """Effective-set proof via views/at (round-2 #4): EXACT multiset
        and cardinality over (logical_path, normalized physical path,
        size_bytes, introduced_by) — an introduced_by-only mapping could
        double-map different/duplicate view files onto the same known
        fileID. Each actual view file must match a known file UNIQUELY;
        only then are the matched fileIDs downloaded in full."""
        view = view_at(api, ds_id)
        if not res.gate(gate, view is not None,
                        "%s: views/at readable" % tag):
            return
        vfiles = view.get("files") or []
        head_ok = (view.get("dataset_id") == ds_id
                   and view.get("requested_branch") == "main"
                   and view.get("resolved_branch") == "main"
                   and view.get("head_transaction_id") == head_txn)
        res.gate(gate, head_ok,
                 "%s: view head=%r branches=%r/%r" %
                 (tag, view.get("head_transaction_id"),
                  view.get("requested_branch"), view.get("resolved_branch")))
        expected = sorted((_vf_path(f.get("logical_path")),
                           _vf_path(f.get("physical_uri")),
                           f.get("size_bytes"), f.get("transaction_id"))
                          for f in known_files)
        actual = sorted((_vf_path(vf.get("logical_path")),
                         _vf_path(vf.get("physical_path")),
                         vf.get("size_bytes"), vf.get("introduced_by"))
                        for vf in vfiles)
        multiset_ok = (actual == expected and bool(expected)
                       and len(set(expected)) == len(expected))
        # unique tuple -> known file (bijection; no double mapping)
        by_tuple = {(_vf_path(f.get("logical_path")),
                     _vf_path(f.get("physical_uri")),
                     f.get("size_bytes"), f.get("transaction_id")): f
                    for f in known_files}
        matched = [by_tuple[(_vf_path(vf.get("logical_path")),
                             _vf_path(vf.get("physical_path")),
                             vf.get("size_bytes"),
                             vf.get("introduced_by"))]
                   for vf in vfiles] if multiset_ok else []
        want_raw = _ndjson(values)
        dl_ok = bool(matched)
        for fm in matched:
            dl = download_file(api, ds_id, fm["id"])
            raw = dl["raw"] or b""
            if dl["status"] != 200 or raw != want_raw:
                dl_ok = False
        res.gate(gate, multiset_ok and dl_ok,
                 "%s: view multiset EXACT match on (logical_path, "
                 "normalized physical path, size, introduced_by); "
                 "%d==%d files uniquely matched, downloads exact %d bytes"
                 % (tag, len(vfiles), len(known_files), len(want_raw)))

    # -- scenarios ---------------------------------------------------------

    def h1(self, api, res):
        cfg = self.cfg
        src = "p25h1"
        ds = self.create_dataset(api, "p25-h1", scenario="h1")
        if not ds:
            res.gate("G_CAS_ATOMIC", False, "dataset create failed")
            return res
        source = self.create_source(
            api, "p25-h1-src",
            incremental_source_config(cfg["fixture_src_url"], src), "h1")
        sync, _ = self.make_sync_def(api, "p25-h1", source["id"], ds["id"],
                                     scenario="h1")
        # round-2 #3: the ledger baseline is the length captured BEFORE
        # this attempt's first fetch — the fixture retains history from
        # earlier scenarios/retries and it is never cleared or treated as
        # a CP position.
        led_idx = len(fixture_ledger(api, src))
        known_files = []
        for i, prefix in enumerate(("a", "b", "c")):
            run, _ = self.trigger_run(api, sync["id"], scenario="h1")
            if not run:
                res.gate("G_CAS_ATOMIC", False, "run %d missing" % (i + 1))
                return res
            inc = run.get("incremental") or {}
            res.gate("G_CAS_ATOMIC", inc.get("base_epoch") == i,
                     "run %d base_epoch=%r want %d" %
                     (i + 1, inc.get("base_epoch"), i))
            led_idx = self.verify_ledger_slice(
                api, res, src, led_idx,
                None if i == 0 else src_token(src, i - 1), i,
                len(SRC_WINDOWS[src][i]), "G_LEDGER", "h1 run%d" % (i + 1))
            fm = self.verify_run_artifact(
                api, res, ds["id"], run, window_values(src, i),
                "G_ACK_STRICT", "h1 run%d(%s)" % (i + 1, prefix))
            if fm:
                known_files.append(fm)
            self.verify_checkpoint_value(api, res, sync["id"], src, i + 1,
                                         "G_CAS_ATOMIC", "h1 run%d" % (i + 1))
        run3_txns = [f.get("transaction_id") for f in known_files]
        # latest effective view == exactly the C file set
        last_run_txn = run3_txns[-1] if run3_txns else None
        if last_run_txn:
            self.verify_latest_view(
                api, res, ds["id"], last_run_txn,
                [known_files[-1]], window_values(src, 2),
                "G_SNAPSHOT_ONLY", "h1 latest")
        return res

    def h2(self, api, res):
        cfg = self.cfg
        src = "p25h2"
        ds = self.create_dataset(api, "p25-h2", scenario="h2")
        source = self.create_source(
            api, "p25-h2-src",
            incremental_source_config(cfg["fixture_src_url"], src), "h2")
        sync, _ = self.make_sync_def(api, "p25-h2", source["id"], ds["id"],
                                     scenario="h2")
        fms = []
        for i in (0, 1):
            run, _ = self.trigger_run(api, sync["id"], scenario="h2")
            fm = self.verify_run_artifact(
                api, res, ds["id"], run, window_values(src, i),
                "G_SNAPSHOT_ONLY", "h2 run%d" % (i + 1))
            if fm:
                fms.append(fm)
        if len(fms) != 2:
            return res
        # After the B SNAPSHOT the effective set is EXACTLY the B files
        # (introduced_by == run2 txn); only view-selected fileIDs are
        # downloaded and compared in full.
        self.verify_latest_view(api, res, ds["id"],
                                fms[1].get("transaction_id"),
                                [fms[1]], window_values(src, 1),
                                "G_SNAPSHOT_ONLY", "h2 latest(B)")
        # V1's fileID remains HISTORICALLY downloadable, unchanged.
        dl = download_file(api, ds["id"], fms[0]["id"])
        raw = dl["raw"] or b""
        res.gate("G_SNAPSHOT_ONLY",
                 dl["status"] == 200 and raw == _ndjson(window_values(src, 0)),
                 "h2: A fileID historically downloadable unchanged "
                 "(%d bytes, sha=%s)" % (len(raw), _sha256(raw)[:12]))
        return res

    def h3(self, api, res):
        cfg = self.cfg
        for src, runs in (("p25h3", 2), ("p25empty", 1)):
            ds = self.create_dataset(api, "p25-h3-" + src, scenario="h3")
            source = self.create_source(
                api, "p25-h3-src-" + src,
                incremental_source_config(cfg["fixture_src_url"], src), "h3")
            sync, _ = self.make_sync_def(api, "p25-h3-" + src, source["id"],
                                         ds["id"], scenario="h3")
            first_cp = None
            run = None
            for i in range(runs):
                led_before = len(fixture_ledger(api, src))
                run, _ = self.trigger_run(api, sync["id"], scenario="h3")
                if not run or run.get("status") != "succeeded":
                    break
                # exhausted/empty run: raw b'', zero rows, empty ledger
                # page with the SAME token re-issued.
                want_vals = window_values(src, i) if i < runs - 1 or \
                    src == "p25empty" else []
                self.verify_run_artifact(
                    api, res, ds["id"], run, want_vals, "G_EMPTY_BATCH",
                    "h3 %s run%d" % (src, i + 1))
                led = fixture_ledger(api, src)
                sl = led[led_before:]
                exp_tok = None if i == 0 else src_token(src, i - 1)
                if src == "p25empty":
                    exp_tok = None
                    want_pages, want_window = 1, 0
                elif i == 0:
                    want_pages, want_window = len(SRC_WINDOWS[src][0]), 0
                else:
                    want_pages, want_window = 1, len(SRC_WINDOWS[src])
                res.gate("G_LEDGER", len(sl) == want_pages and
                         all(e.get("token") == exp_tok for e in sl) and
                         all(e.get("window") == want_window for e in sl),
                         "h3 %s run%d ledger slice=%d want=%d" %
                         (src, i + 1, len(sl), want_pages))
                if i == 0:
                    first_cp = checkpoint_of(api, sync["id"]) or {}
            cp = checkpoint_of(api, sync["id"]) or {}
            tok = src_token(src, 0)
            ok = (run and run.get("status") == "succeeded"
                  and cp.get("confirmed_epoch") == runs
                  and cp.get("checkpoint_len") == len(tok)
                  and cp.get("checkpoint_sha256") == _sha256(tok)
                  and cp.get("checkpoint_len") == first_cp.get("checkpoint_len")
                  and cp.get("checkpoint_sha256") ==
                  first_cp.get("checkpoint_sha256")
                  and not (cp.get("owner") or {}).get("run_id"))
            res.gate("G_EMPTY_BATCH", ok,
                     "%s: epoch=%r len/sha stable=%s owner released" %
                     (src, cp.get("confirmed_epoch"),
                      cp.get("checkpoint_sha256") ==
                      first_cp.get("checkpoint_sha256")))
        return res

    def h4(self, api, res):
        cfg = self.cfg
        src = "p25h4"
        for mode in ("missing", "non_string", "empty", "oversize",
                     "ctrl_chars", "echo_request_token"):
            ds = self.create_dataset(api, "p25-h4-" + mode, scenario="h4")
            source = self.create_source(
                api, "p25-h4-src-" + mode,
                incremental_source_config(cfg["fixture_src_url"], src), "h4")
            sync, _ = self.make_sync_def(api, "p25-h4-" + mode,
                                         source["id"], ds["id"], scenario="h4")
            seed, _ = self.trigger_run(api, sync["id"], scenario="h4")
            if not seed or seed.get("status") != "succeeded":
                res.gate("G_CANDIDATE", False,
                         "%s: seed run %r" % (mode, seed and seed.get("status")))
                continue
            tok0 = src_token(src, 0)
            fixture_set_knob(api, src, {"candidate_mode": mode})
            led_before = len(fixture_ledger(api, src))
            run, _ = self.trigger_run(api, sync["id"], scenario="h4")
            cp = checkpoint_of(api, sync["id"]) or {}
            owner = cp.get("owner") or {}
            # REACHED-PROOF: the fault run's ledger slice IS the non-empty
            # window-1 B page, input token tok-0.
            sl = fixture_ledger(api, src)[led_before:]
            reached = (len(sl) == 1 and sl[0].get("token") == tok0
                       and sl[0].get("window") == 1)
            ok = (run and run.get("status") == "failed"
                  and not owner.get("run_id")
                  and cp.get("confirmed_epoch") == 1
                  and cp.get("checkpoint_len") == len(tok0)
                  and cp.get("checkpoint_sha256") == _sha256(tok0))
            res.gate("G_CANDIDATE", ok and reached,
                     "%s: status=%s owner=%r epoch=%r reached=%s" %
                     (mode, run and run.get("status"), owner.get("run_id"),
                      cp.get("confirmed_epoch"), reached))
            err_txt = json.dumps(run.get("error")) if run else ""
            leak = (("x" * 513 in err_txt) or ("bad\x01" in err_txt)
                    or (mode == "echo_request_token" and tok0 in err_txt))
            res.gate("G_CANDIDATE", not leak,
                     "%s: no raw candidate leak in error" % mode)
            fixture_set_knob(api, src, {"candidate_mode": None})
        return res

    def h6(self, api, res):
        cfg = self.cfg
        src = "p25h6"
        ds1 = self.create_dataset(api, "p25-h6-d1", scenario="h6")
        ds2 = self.create_dataset(api, "p25-h6-d2", scenario="h6")
        source = self.create_source(
            api, "p25-h6-src",
            incremental_source_config(cfg["fixture_src_url"], src), "h6")
        sync1, _ = self.make_sync_def(api, "p25-h6-1", source["id"],
                                      ds1["id"], scenario="h6")
        sync2, _ = self.make_sync_def(api, "p25-h6-2", source["id"],
                                      ds2["id"], scenario="h6")
        fixture_set_knob(api, src, {"slow_terminal_ms": 4000})
        target1_before = [e for e in proxy_log(api)
                          if e.get("request_target") == ds1["id"]]
        run1, _ = self.trigger_run(api, sync1["id"], async_mode=True,
                                   wait=False, scenario="h6")
        run2, _ = self.trigger_run(api, sync2["id"], async_mode=True,
                                   wait=False, scenario="h6")
        # OVERLAP: both owners live at the same time (independent Defs).
        cp1 = checkpoint_of(api, sync1["id"]) or {}
        cp2 = checkpoint_of(api, sync2["id"]) or {}
        res.gate("G_OWNER_DURABLE",
                 (cp1.get("owner") or {}).get("run_id") == run1["id"] and
                 (cp2.get("owner") or {}).get("run_id") == run2["id"],
                 "overlapping owners def1=%r def2=%r" %
                 ((cp1.get("owner") or {}).get("run_id"),
                  (cp2.get("owner") or {}).get("run_id")))
        # same-def second trigger during the window -> 409 busy
        busy = api.call("connector", "POST", route("sync_run",
                                                   id=sync1["id"]), body={})
        code = (busy["json"] or {}).get("code") if isinstance(busy["json"],
                                                              dict) else None
        res.gate("G_OWNER_DURABLE", busy["status"] == 409 and
                 code == FIELDS["cp_busy_code"],
                 "busy trigger status=%s code=%r" % (busy["status"], code))
        target1_during = [e for e in proxy_log(api)
                          if e.get("request_target") == ds1["id"]]
        res.gate("G_OWNER_DURABLE", len(target1_during) == len(target1_before),
                 "zero additional dispatch for target during busy window "
                 "(%d->%d, request_target filter)" %
                 (len(target1_before), len(target1_during)))
        fixture_set_knob(api, src, {"slow_terminal_ms": 0})
        final1, _ = expect_run_terminal(api, sync1["id"], run1["id"], 60)
        final2, _ = expect_run_terminal(api, sync2["id"], run2["id"], 60)
        self.verify_run_artifact(api, res, ds1["id"], final1,
                                 window_values(src, 0), "G_ACK_STRICT",
                                 "h6 def1")
        self.verify_run_artifact(api, res, ds2["id"], final2,
                                 window_values(src, 0), "G_ACK_STRICT",
                                 "h6 def2 (independent position)")
        snap = fixture_snapshot(api, src) or {}
        res.gate("G_COUNTERS",
                 snap.get("name") == src and
                 snap.get("active") == 0 and
                 snap.get("inventory_started") == snap.get("inventory_finished")
                 and _is_int(snap.get("active"))
                 and _is_int(snap.get("inventory_started")),
                 "cohort counters quiet: %r" % snap)
        return res

    def h7(self, api, res):
        cfg = self.cfg
        src = "p25h4"
        ds = self.create_dataset(api, "p25-h7", scenario="h7")
        ds2 = self.create_dataset(api, "p25-h7-retarget", scenario="h7")
        if not ds2:
            res.gate("G_OWNER_DURABLE", False,
                     "real second dataset create failed (retarget needs a "
                     "valid same-domain dataset, not a random UUID)")
            return res
        source = self.create_source(
            api, "p25-h7-src",
            incremental_source_config(cfg["fixture_src_url"], src), "h7")
        sync, _ = self.make_sync_def(api, "p25-h7", source["id"], ds["id"],
                                     scenario="h7")
        fixture_set_knob(api, src, {"slow_terminal_ms": 4000})
        run1, _ = self.trigger_run(api, sync["id"], async_mode=True,
                                   wait=False, scenario="h7")
        fenced = []
        r = api.call("connector", "PATCH", route("sync_patch", id=sync["id"]),
                     body={"output_dataset_id": ds2["id"]})
        fenced.append(("target->real-ds2", r["status"],
                       _mutation_fence_error(r, "target", sync["id"], run1["id"])))
        r = api.call("connector", "PATCH", route("sync_patch", id=sync["id"]),
                     body={"file_glob": "*.ndjson"})
        fenced.append(("file_glob", r["status"], _mutation_fence_error(r, "file_glob", sync["id"], run1["id"])))
        r = api.call("connector", "PATCH",
                     route("connection_patch", id=source["id"]),
                     body={"config": incremental_source_config(
                         cfg["fixture_src_url"], src,
                         {"sync": {"limits": {"max_records": 400}}})})
        fenced.append(("config", r["status"], _mutation_fence_error(r, "config", sync["id"], run1["id"])))
        r = api.call("connector", "POST",
                     route("credentials_post", id=source["id"]),
                     body={"kind": "oauth_token", "value": "test-only-secret"})
        fenced.append(("credential", r["status"], _mutation_fence_error(r, "credential", sync["id"], run1["id"])))
        # allowed branches: name-only / cron-only / enable-only, with
        # before/after checkpoint snapshots EQUAL.
        cp_before = checkpoint_of(api, sync["id"]) or {}
        allowed = []
        r = api.call("connector", "PATCH",
                     route("connection_patch", id=source["id"]),
                     body={"name": "p25-h7-src-renamed"})
        allowed.append(("name", r["status"]))
        r = api.call("connector", "PATCH", route("sync_patch", id=sync["id"]),
                     body={"schedule_cron": "*/7 * * * *"})
        allowed.append(("cron", r["status"]))
        r = api.call("connector", "PATCH", route("sync_patch", id=sync["id"]),
                     body={"schedule_enabled": False})
        allowed.append(("enable-off", r["status"]))
        r = api.call("connector", "PATCH", route("sync_patch", id=sync["id"]),
                     body={"schedule_enabled": True})
        allowed.append(("enable-on", r["status"]))
        cp_after = checkpoint_of(api, sync["id"]) or {}
        # tenant visibility: org-2 token cannot see this def's CP or runs.
        org2 = self.api_factory("h7", auth="org2")
        tenant = []
        if not cfg.get("jwt_org2"):
            tenant.append(("org2-missing", None, None))
        else:
            r = org2.call("connector", "GET",
                          route("checkpoint_view", id=sync["id"]))
            tenant.append(("org2-checkpoint", r["status"], None))
            r = org2.call("connector", "GET",
                          route("run_get", id=sync["id"], run_id=run1["id"]))
            tenant.append(("org2-run", r["status"], None))
        fixture_set_knob(api, src, {"slow_terminal_ms": 0})
        expect_run_terminal(api, sync["id"], run1["id"], 60)
        fence_ok = all(s == 409 and bool(c)
                       for _, s, c in fenced)
        res.gate("G_OWNER_DURABLE", fence_ok, "fences=%r" % (fenced,))
        res.gate("G_OWNER_DURABLE",
                 all(s == 200 for _, s in allowed) and
                 cp_after.get("confirmed_epoch") ==
                 cp_before.get("confirmed_epoch") and
                 cp_after.get("checkpoint_sha256") ==
                 cp_before.get("checkpoint_sha256"),
                 "allowed name/cron/enable branches ok and checkpoint "
                 "snapshots equal (%r)" % (allowed,))
        res.gate("G_OWNER_DURABLE",
                 all(s == 404 for _, s, _ in tenant),
                 "org-2 tenant reads are 404 (jwt_org2 required): %r" %
                 (tenant,))
        # post-release REAL config change -> next trigger binding_stale.
        r = api.call("connector", "PATCH",
                     route("connection_patch", id=source["id"]),
                     body={"config": incremental_source_config(
                         cfg["fixture_src_url"], src,
                         {"sync": {"query_params": {"src": src, "x": "1"}}})})
        stale = api.call("connector", "POST", route("sync_run",
                                                    id=sync["id"]), body={})
        stale_code = (stale["json"] or {}).get("code") if isinstance(
            stale["json"], dict) else None
        res.gate("G_OWNER_DURABLE",
                 r["status"] == 200 and stale["status"] == 409 and
                 stale_code == FIELDS["cp_stale_code"],
                 "real config change -> binding_stale (%s/%s/%r)" %
                 (r["status"], stale["status"], stale_code))
        return res

    def h8(self, api, res):
        cfg = self.cfg
        src = "p25h1"
        for knob, mode in (({"fail_mid_pages": True}, "mid"),
                           ({"fail_terminal_after_rows": True}, "terminal")):
            ds = self.create_dataset(api, "p25-h8-" + mode, scenario="h8")
            source = self.create_source(
                api, "p25-h8-src-" + mode,
                incremental_source_config(cfg["fixture_src_url"], src), "h8")
            sync, _ = self.make_sync_def(api, "p25-h8-" + mode,
                                         source["id"], ds["id"], scenario="h8")
            fixture_set_knob(api, src, dict(knob))
            led_before = len(fixture_ledger(api, src))
            run, _ = self.trigger_run(api, sync["id"], scenario="h8")
            cp = checkpoint_of(api, sync["id"]) or {}
            sl = fixture_ledger(api, src)[led_before:]
            ok = (run and run.get("status") == "failed"
                  and not (cp.get("owner") or {}).get("run_id")
                  and cp.get("confirmed_epoch") in (0, None))
            res.gate("G_CANDIDATE", ok,
                     "%s: %r owner=%r epoch=%r fault-pages=%d" %
                     (mode, run and run.get("status"),
                      (cp.get("owner") or {}).get("run_id"),
                      cp.get("confirmed_epoch"), len(sl)))
            fixture_set_knob(api, src,
                             {"fail_mid_pages": False,
                              "fail_terminal_after_rows": False})
            led_retry_before = len(fixture_ledger(api, src))
            retry, _ = self.trigger_run(api, sync["id"], scenario="h8")
            retry_ok = retry and retry.get("status") == "succeeded"
            res.gate("G_CANDIDATE", retry_ok,
                     "%s retry re-pulls the WHOLE batch: %r" %
                     (mode, retry and retry.get("status")))
            rsl = fixture_ledger(api, src)[led_retry_before:]
            res.gate("G_LEDGER",
                     len(rsl) == len(SRC_WINDOWS[src][0]) and
                     all(e.get("token") is None for e in rsl) and
                     all(e.get("window") == 0 for e in rsl),
                     "%s retry ledger slice=%d (full window A again, "
                     "NULL token — position was not consumed)" %
                     (mode, len(rsl)))
            if retry_ok:
                self.verify_run_artifact(api, res, ds["id"], retry,
                                         window_values(src, 0),
                                         "G_CANDIDATE", "h8 %s retry" % mode)
        ds = self.create_dataset(api, "p25-h8-limits", scenario="h8")
        source = self.create_source(
            api, "p25-h8-limits-src",
            incremental_source_config(
                cfg["fixture_src_url"], src,
                {"sync": {"limits": {"max_pages": 1}}}), "h8")
        sync, _ = self.make_sync_def(api, "p25-h8-limits", source["id"],
                                     ds["id"], scenario="h8")
        run, _ = self.trigger_run(api, sync["id"], scenario="h8")
        cp = checkpoint_of(api, sync["id"]) or {}
        res.gate("G_CANDIDATE",
                 run and run.get("status") == "failed" and
                 "max_pages" in json.dumps(run.get("error")) and
                 cp.get("confirmed_epoch") in (0, None) and
                 not (cp.get("owner") or {}).get("run_id"),
                 "limits refused truncation: %r" % (run and run.get("error")))
        return res

    def h9(self, api, res):
        cfg = self.cfg
        src = "p25h6"
        ds = self.create_dataset(api, "p25-h9", scenario="h9")
        source = self.create_source(
            api, "p25-h9-src",
            incremental_source_config(cfg["fixture_src_url"], src), "h9")
        sync, _ = self.make_sync_def(api, "p25-h9", source["id"], ds["id"],
                                     scenario="h9")
        fixture_set_knob(api, src, {"slow_terminal_ms": 6000})
        target_before = [e for e in proxy_log(api)
                         if e.get("request_target") == ds["id"]]
        run, _ = self.trigger_run(api, sync["id"], async_mode=True,
                                  wait=False, scenario="h9")
        time.sleep(1.0)
        api.call("connector", "POST",
                 route("run_cancel", id=sync["id"], run_id=run["id"]),
                 body={})
        final, _ = expect_run_terminal(api, sync["id"], run["id"], 30)
        fixture_set_knob(api, src, {"slow_terminal_ms": 0})
        target_after = [e for e in proxy_log(api)
                        if e.get("request_target") == ds["id"]]
        cp = checkpoint_of(api, sync["id"]) or {}
        res.gate("G_RETAIN_UNKNOWN",
                 final and final.get("status") == "aborted" and
                 final.get("publish_entered_at") is None,
                 "aborted pre-gate: %r publish_entered_at=%r" %
                 (final and final.get("status"),
                  final and final.get("publish_entered_at")))
        res.gate("G_RETAIN_UNKNOWN",
                 len(target_after) == len(target_before),
                 "zero commit dispatch across cancel (request_target "
                 "filter %d->%d)" % (len(target_before), len(target_after)))
        res.gate("G_RETAIN_UNKNOWN",
                 not (cp.get("owner") or {}).get("run_id") and
                 cp.get("confirmed_epoch") in (0, None),
                 "owner released, position unchanged: epoch=%r" %
                 cp.get("confirmed_epoch"))
        retry, _ = self.trigger_run(api, sync["id"], scenario="h9")
        if retry and retry.get("status") == "succeeded":
            self.verify_run_artifact(api, res, ds["id"], retry,
                                     window_values(src, 0),
                                     "G_RETAIN_UNKNOWN", "h9 retry")
        else:
            res.gate("G_RETAIN_UNKNOWN", False,
                     "re-trigger from the same position: %r" %
                     (retry and retry.get("status")))
        return res

    # -- h10: two-stage driver-coordinated restart probes -------------------

    def _h10_snapshot(self, api, defs):
        snap = {}
        ok = True
        for did in defs:
            cp = checkpoint_of(api, did)
            runs = runs_list(api, did)
            dv = def_view(api, did)
            if cp is None or runs is None or dv is None:
                ok = False
                continue
            snap[did] = {
                "source_id": dv.get("source_id"),
                "checkpoint": cp,
                "runs": [{"id": r.get("id"), "sync_def_id": r.get("sync_def_id"), "status": r.get("status"),
                          "generation": r.get("generation"),
                          "attempt": r.get("attempt"),
                          "control_mode": r.get("control_mode"),
                          "publish_entered_at": r.get("publish_entered_at")}
                         for r in runs],
            }
        return snap, ok

    def h10(self, api, res):
        """round-3 #2/#3. h10 is EXCLUDED from the default healthy phase;
        the driver invokes --only h10 as pre / REAL CRASH / post. The
        crash is a deliberate SIGKILL of the disposable p25connector
        followed by starting the SAME accepted image — a graceful docker
        restart would correctly cancel pre-gate work and release the
        owner, making the immutability expectation wrong. PRE does NOT
        require global quiet: the held def's source is expected to be
        genuinely ACTIVE (a real non-terminal owner-held run prepared via
        request_timeout_ms=30000 + a 15s terminal hold) while every other
        observed def is quiet-succeeded; POST requires REAL quiet and
        NO adoption/advance (identical run ids AND per-run protected
        state; the held owner row unchanged — blocked_reason may
        legitimately flip to the business word after lease expiry). The
        crash record is validated STRICTLY (no restarted:true-only)."""
        cfg = self.cfg
        live = bool(cfg.get("live"))
        stage = cfg.get("h10_stage") or "post"
        defs = [d for d in (cfg.get("h10_defs") or []) if d]
        held_def = cfg.get("h10_held_def") or ""
        if not defs:
            res.gate("G_RETAIN_UNKNOWN", False,
                     "--h10-defs empty (driver must supply real def ids)")
            return res
        if len(defs) < 2:
            res.gate("G_RETAIN_UNKNOWN", False,
                     "observation set needs at least one QUIET-SUCCEEDED "
                     "def AND the held def (%d given)" % len(defs))
            return res
        if held_def not in defs:
            res.gate("G_RETAIN_UNKNOWN", False,
                     "--h10-held-def %r must be one of --h10-defs" %
                     held_def)
            return res
        for did in defs:
            self.rec.persist_handle("h10-def", did,
                                    {"stage": stage, "scenario": "h10"})
        snap, ok = self._h10_snapshot(api, defs)
        res.gate("G_RETAIN_UNKNOWN", ok,
                 "%s-stage: all defs readable (def+checkpoint+runs)" % stage)
        if not ok:
            return res
        if stage == "pre":
            # held def: owner genuinely set by a NON-terminal run (live:
            # the source is expected IN FLIGHT at the fixture)
            hs = snap.get(held_def) or {}
            held_cp = (hs.get("checkpoint") or {})
            held_owner = (held_cp.get("owner") or {}).get("run_id")
            held_run = next((r for r in hs.get("runs", [])
                             if r.get("id") == held_owner), None)
            deadline = time.time() + 20
            while (not held_owner or not held_run or
                   held_run.get("status") not in ("pending", "running")) \
                    and time.time() < deadline:
                snap, _ = self._h10_snapshot(api, [held_def])
                hs = snap.get(held_def) or {}
                held_cp = (hs.get("checkpoint") or {})
                held_owner = (held_cp.get("owner") or {}).get("run_id")
                held_run = next((r for r in hs.get("runs", [])
                                 if r.get("id") == held_owner), None)
                time.sleep(0.5)
            res.gate("G_RETAIN_UNKNOWN",
                     bool(held_owner) and held_run is not None and
                     held_run.get("status") in ("pending", "running"),
                     "held def %s: genuine owner-held NON-terminal run "
                     "captured (owner=%r status=%r)" %
                     (held_def, held_owner,
                      held_run and held_run.get("status")))
            snap, _ = self._h10_snapshot(api, defs)
            if live:
                src_of = {}
                for did in defs:
                    src_of[did] = (snap.get(did) or {}).get("source_id")
                # fixture src of the held def: expected ACTIVE; the other
                # observed defs' srcs: expected quiet (round-3 #2 — pre
                # is NEVER a claim of global quiet)
                held_active = 0
                deadline = time.time() + 10
                fxs = fixture_snapshot(api, self._src_name_of(
                    snap, held_def)) or {}
                while fxs.get("active", 0) < 1 and time.time() < deadline:
                    fxs = fixture_snapshot(api, self._src_name_of(
                        snap, held_def)) or {}
                    time.sleep(0.25)
                res.gate("G_RETAIN_UNKNOWN", fxs.get("active", 0) >= 1,
                         "pre: held source genuinely in flight "
                         "(fixture active=%r) — never claimed quiet" %
                         fxs.get("active"))
            for did in defs:
                if did == held_def:
                    continue
                s = snap.get(did) or {}
                cpw = s.get("checkpoint") or {}
                if live:
                    qsrc = self._src_name_of(snap, did)
                    qs = fixture_snapshot(api, qsrc) if qsrc else None
                    res.gate("G_RETAIN_UNKNOWN", bool(qs) and qs.get("active") == 0,
                             "pre: other source fixture is quiet")
                runs_ok = bool(s.get("runs")) and \
                    all(r.get("status") == "succeeded"
                        for r in s["runs"])
                res.gate("G_RETAIN_UNKNOWN",
                         not (cpw.get("owner") or {}).get("run_id") and
                         runs_ok,
                         "pre: quiet-succeeded def %s (owner none, %d/%d "
                         "runs succeeded)" %
                         (did, sum(1 for r in (s.get("runs") or [])
                                   if r.get("status") == "succeeded"),
                          len(s.get("runs") or [])))
            prior_path = cfg.get("h10_prior") or os.path.join(
                self.rec.dir, "h10-prior-%s.json" % _ts())
            with open(prior_path, "w", encoding="utf-8") as fh:
                json.dump({"defs": defs, "held_def": held_def,
                           "snapshot": snap, "taken_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())},
                          fh, indent=1, default=str)
            self.rec.record("h10", "h10-prior-snapshot",
                            {"path": prior_path, "defs": defs,
                             "held_def": held_def,
                             "prior_sha256": _file_sha256(prior_path)})
            res.gate("G_RETAIN_UNKNOWN", True,
                     "pre-stage snapshot written to %s (sha=%s)" %
                     (prior_path, _file_sha256(prior_path)[:12]))
            return res
        # ---------------- post ----------------
        prior_path = cfg.get("h10_prior")
        if not prior_path or not os.path.exists(prior_path):
            res.gate("G_RETAIN_UNKNOWN", False,
                     "--h10-prior missing for post stage")
            return res
        try:
            with open(prior_path, "r", encoding="utf-8") as fh:
                prior_doc = json.load(fh)
            prior = prior_doc.get("snapshot") or {}
            if prior_doc.get("held_def") != held_def or prior_doc.get("defs") != defs:
                raise ValueError("prior def set / held def differs from CLI")
        except Exception as exc:
            res.gate("G_RETAIN_UNKNOWN", False,
                     "unreadable prior snapshot: %s" % exc)
            return res
        crash_path = cfg.get("h10_restart_record")
        crash_rec = None
        if crash_path and os.path.exists(crash_path):
            try:
                with open(crash_path, "r", encoding="utf-8") as fh:
                    crash_rec = json.load(fh)
            except Exception as exc:
                res.gate("G_RETAIN_UNKNOWN", False,
                         "unreadable crash record: %s" % exc)
                return res
        problems = _validate_crash_record(crash_rec, defs, held_def,
                                          prior, prior_path, cfg.get("h10_expected_image"))
        res.gate("G_RETAIN_UNKNOWN", not problems,
                 "crash record STRICTLY validated (SIGKILL + same-image "
                 "start; cmd/rc/artifact SHA; StartedAt before!=after; "
                 "ordered timestamps; held identities; prior sha)%s" %
                 ("" if not problems else ": " + "; ".join(problems[:4])))
        if problems:
            return res
        # POST requires REAL quiet: the killed process left no in-flight
        # request anywhere (fixture counters finish on disconnect).
        quiet_ok = False
        deadline = time.time() + 30
        while time.time() < deadline:
            quiet_ok = True
            for did in defs:
                src = self._src_name_of(snap, did)
                fxs = fixture_snapshot(api, src) if src else None
                if not fxs or not _is_int(fxs.get("active")) or fxs["active"] != 0:
                    quiet_ok = False
            if quiet_ok:
                break
            time.sleep(.25)
        res.gate("G_RETAIN_UNKNOWN", quiet_ok,
                 "post: REAL quiet on every observed source (crash killed "
                 "the in-flight holder)")
        for did in defs:
            p, n = prior.get(did), snap.get(did)
            if not p or not n:
                res.gate("G_RETAIN_UNKNOWN", False,
                         "%s missing from prior/current snapshot" % did)
                continue
            pc, nc = p["checkpoint"], n["checkpoint"]
            stable = (n.get("source_id") == p.get("source_id")
                      and nc.get("mode") == pc.get("mode")
                      and nc.get("initialized") == pc.get("initialized")
                      and nc.get("last_confirmed") == pc.get("last_confirmed")
                      and nc.get("confirmed_epoch") == pc.get("confirmed_epoch")
                      and nc.get("checkpoint_len") == pc.get("checkpoint_len")
                      and nc.get("checkpoint_sha256") ==
                      pc.get("checkpoint_sha256"))
            po = (pc.get("owner") or {}).get("run_id")
            no = (nc.get("owner") or {}).get("run_id")
            owner_stable = all((nc.get("owner") or {}).get(k) ==
                               (pc.get("owner") or {}).get(k) for k in ("run_id", "run_status", "since"))
            res.gate("G_RETAIN_UNKNOWN", stable and po == no and owner_stable,
                     "%s: checkpoint state unchanged and owner row "
                     "UNCHANGED across the crash (%r -> %r — no release, "
                     "no adoption)" % (did, po, no))
            if no:
                blocked = (nc.get("owner") or {}).get("blocked_reason")
                # after lease expiry the business word is EXPECTED; while
                # the lease reads live it stays "" — both legitimate
                res.gate("G_RETAIN_UNKNOWN",
                         blocked in ("", FIELDS["blocked_reason_business"]),
                         "%s: blocked_reason=%r (may legitimately flip to "
                         "the business word after lease expiry)" %
                         (did, blocked))
                res.owner_roster.append({
                    "def_id": did, "source_id": n.get("source_id"),
                    "run_id": no,
                    "run_status": (nc.get("owner") or {}).get("run_status"),
                    "blocked_reason": blocked,
                    "source": "h10-post-actual"})
            prow = {r["id"]: r for r in p["runs"]}
            nrow = {r["id"]: r for r in n["runs"]}
            res.gate("G_RETAIN_UNKNOWN", set(nrow) == set(prow),
                     "%s: no adoption/replay — run ids identical (%d)" %
                     (did, len(nrow)))
            for rid, r in nrow.items():
                pr = prow.get(rid)
                if pr is None:
                    continue
                same_safe = all(r.get(k) == pr.get(k) for k in
                                ("status", "generation", "attempt",
                                 "control_mode", "publish_entered_at"))
                res.gate("G_RETAIN_UNKNOWN", same_safe,
                         "%s/%s: protected state identical across the "
                         "crash (status=%r gen=%r->%r attempt=%r->%r "
                         "control=%r)" %
                         (did, rid[:8], r.get("status"),
                          pr.get("generation"), r.get("generation"),
                          pr.get("attempt"), r.get("attempt"),
                          r.get("control_mode")))
                if r.get("status") in ("pending", "running"):
                    res.run_roster.append({
                        "def_id": did, "source_id": n.get("source_id"),
                        "run_id": rid, "status": r["status"],
                        "source": "h10-post-actual"})
                if pr.get("status") in ("pending", "running") and \
                        r.get("status") not in ("pending", "running"):
                    res.gate("G_RETAIN_UNKNOWN", False,
                             "%s: nonterminal run %s became %s after the "
                             "crash (adoption/cancel?)" %
                             (did, rid, r.get("status")))
        return res

    def _src_name_of(self, snap, did):
        """The fixture src name for a def, recovered from the engine's
        own source config (query_params.src) via the source handle."""
        sid = (snap.get(did) or {}).get("source_id")
        for h in self.rec.handles:
            if h.get("kind") == "source" and h.get("ref") == sid:
                cfgx = h.get("config")
                if isinstance(cfgx, dict):
                    return (cfgx.get("sync", {}).get("query_params", {})
                            .get("src"))
        return None

    def h11(self, api, res):
        cfg = self.cfg
        src = "p25h1"
        for mode in ("no_txn_200", "not_committed", "wrong_target",
                     "http_500"):
            ds = self.create_dataset(api, "p25-h11-" + mode, scenario="h11")
            source = self.create_source(
                api, "p25-h11-src-" + mode,
                incremental_source_config(cfg["fixture_src_url"], src), "h11")
            sync, _ = self.make_sync_def(api, "p25-h11-" + mode,
                                         source["id"], ds["id"],
                                         scenario="h11")
            proxy_set_mode(api, mode)
            before = [e for e in proxy_log(api)
                      if e.get("request_target") == ds["id"]]
            run, _ = self.trigger_run(api, sync["id"], scenario="h11")
            after = [e for e in proxy_log(api)
                     if e.get("request_target") == ds["id"]]
            delta = after[len(before):]
            cp = checkpoint_of(api, sync["id"]) or {}
            owner = cp.get("owner") or {}
            want_status = 502 if mode == "http_500" else 200
            # exactly ONE attempt in the delta, not forwarded, fabricated
            # with the documented status — i.e. exactly the intended ACK
            # predicate is broken (proxy design isolates one predicate
            # per mode) and the connector REJECTED it.
            entry_ok = (len(delta) == 1 and delta[0].get("mode") == mode
                        and delta[0].get("forwarded") is False
                        and delta[0].get("fabricated_status") == want_status)
            local_ok = (run and run.get("status") == "failed"
                        and owner.get("run_id")
                        and cp.get("confirmed_epoch") in (0, None))
            res.gate("G_ACK_STRICT", entry_ok and local_ok,
                     "%s: delta=%d forwarded=%r fab_status=%r run=%r "
                     "owner=%r (LOCAL outcome only)" %
                     (mode, len(delta),
                      delta[0].get("forwarded") if delta else None,
                      delta[0].get("fabricated_status") if delta else None,
                      run and run.get("status"), owner.get("run_id")))
            if owner.get("run_id"):
                res.owner_roster.append({
                    "def_id": sync["id"], "run_id": owner.get("run_id"),
                    "run_status": owner.get("run_status"),
                    "blocked_reason": owner.get("blocked_reason"),
                    "mode": mode, "source": "h11-actual"})
            proxy_set_mode(api, "passthrough")
        return res

    def h12(self, api, res, pg_proof_file):
        """Bind the PG track's FROZEN r17 evidence (round-2 #6 strict):
        EXACTLY the three named stages, each original_rc==0, every
        artifact re-hashed, meta source sha before==after==candidate sha,
        and EVERY required name showing exactly 1 parent RUN + 1 parent
        PASS + 0 SKIP + 0 FAIL in EVERY stage's raw (exact-line matching
        — substring PASS never counts; subtest lines are not parents).
        Missing stages can never be green."""
        path = pg_proof_file
        if not path:
            res.gate("G_CAS_ATOMIC", False,
                     "PG proof file not supplied (PG track owns the run)")
            return res
        try:
            with open(path, "r", encoding="utf-8") as fh:
                proof = json.load(fh)
        except Exception as exc:
            res.gate("G_CAS_ATOMIC", False, "unreadable proof: %s" % exc)
            return res
        problems = []
        consumed = []
        stages = proof.get("stages") or []
        got_names = [s.get("stage") for s in stages]
        if sorted(g for g in got_names if g) != \
                sorted(PG_STAGE_NAMES) or len(stages) != len(PG_STAGE_NAMES):
            problems.append("stages %r != exactly %r" %
                            (got_names, list(PG_STAGE_NAMES)))
        for s in stages:
            if s.get("original_rc") != 0:
                problems.append("stage %r original_rc=%r want 0" %
                                (s.get("stage"), s.get("original_rc")))
        # required names from the referenced frozen file (repo-relative).
        names_file = proof.get("required_names_file") or ""
        base = os.path.dirname(os.path.abspath(path))
        # same dir first, then repo-root-relative (this file sits THREE
        # levels below the repo root)
        roots = [os.path.join(base, os.path.basename(names_file)),
                 os.path.abspath(os.path.join(base, "..", "..", "..",
                                              names_file))]
        names_path = next((p for p in roots if os.path.exists(p)), None)
        if not names_path:
            res.gate("G_CAS_ATOMIC", False,
                     "required-names file %r not found (referenced by the "
                     "binding; never self-selected)" % names_file)
            return res
        with open(names_path, "r", encoding="utf-8") as fh:
            names = [ln.strip() for ln in fh.read().splitlines()
                     if ln.strip() and not ln.startswith("#")]
        if len(names) != proof.get("required_names_count"):
            problems.append("required names count %d != binding %r" %
                            (len(names), proof.get("required_names_count")))
        pinned = self.cfg.get("pg_proof_name") or PINNED_ROLLBACK_NAME
        if pinned not in names:
            problems.append("pinned rollback name %s not in required names"
                            % pinned)
        consumed.append({"file": names_path,
                         "sha256": _file_sha256(names_path)})
        want_sha = proof.get("candidate_source_sha256")
        for stage in stages:
            base_path = stage.get("attempt_base") or ""
            for suffix in stage.get("artifacts") or []:
                artifact = base_path + suffix
                if not os.path.exists(artifact):
                    problems.append("%s missing" % artifact)
                    continue
                consumed.append({"file": artifact,
                                 "sha256": _file_sha256(artifact)})
            rc_file = base_path + ".rc"
            if not os.path.exists(rc_file):
                problems.append("%s missing" % rc_file)
            else:
                with open(rc_file, "r", encoding="utf-8") as fh:
                    rc_text = fh.read().strip()
                if rc_text != "0":
                    problems.append("%s rc=%r want 0" % (rc_file, rc_text))
            meta_file = base_path + ".meta"
            if not os.path.exists(meta_file):
                problems.append("%s missing" % meta_file)
            else:
                meta = {}
                with open(meta_file, "r", encoding="utf-8") as fh:
                    for ln in fh.read().splitlines():
                        if "=" in ln:
                            k, v = ln.split("=", 1)
                            meta[k.strip()] = v.strip()
                before = meta.get("source_content_sha256_before")
                after = meta.get("source_content_sha256_after")
                if before != want_sha or after != want_sha:
                    problems.append(
                        "%s source sha before/after != candidate sha" %
                        meta_file)
            raw_file = base_path + ".raw"
            if not os.path.exists(raw_file):
                problems.append("%s missing" % raw_file)
                continue
            with open(raw_file, "r", encoding="utf-8",
                      errors="replace") as fh:
                raw_lines = fh.read().splitlines()
            for n in names:
                run_n = sum(1 for l in raw_lines if l == "=== RUN   " + n)
                pass_n = _parent_pass_count(raw_lines, n)
                skip_n = _parent_verdict_count(raw_lines, "--- SKIP: ", n)
                fail_n = _parent_verdict_count(raw_lines, "--- FAIL: ", n)
                if (run_n, pass_n, skip_n, fail_n) != (1, 1, 0, 0):
                    problems.append(
                        "%s: %s RUN/PASS/SKIP/FAIL=%d/%d/%d/%d want "
                        "1/1/0/0" % (stage.get("stage"), n, run_n, pass_n,
                                     skip_n, fail_n))
        ok = not problems
        res.gate("G_CAS_ATOMIC", ok,
                 "bound to frozen PG evidence %s: stages=%s names=%d "
                 "(rollback=%s pinned; every name 1RUN/1PASS/0SKIP/0FAIL "
                 "in every stage) %s" %
                 (os.path.basename(path), got_names, len(names), pinned,
                  "" if ok else "| " + "; ".join(problems[:6])),
                 evidence=consumed)
        return res

    def h13(self, api, res):
        """round-2 #2 redesign. Real ticks arrive created_at DESC: sort by
        scheduled_for ASC and only then associate runs. The busy case uses
        a DEDICATED FRESH scheduled def whose next window is still
        NON-EMPTY (the original def is exhausted after 2 batches and a
        manual run there returns an empty confirmation BEFORE any slow
        knob can hold the owner). The manual owner-holding run is started
        deterministically BEFORE the actual next due derived from the last
        tick's scheduled_for — a fixed 4s delay can never be assumed to
        span an arbitrary 60s cron. Fixture activity is proven live (the
        stronger API-level owner/non-terminal facts are asserted in both
        modes). The no-success-admission-stamp fact is SQL-hidden and is
        supplied by the driver (HTTP-INTERFACE.md)."""
        cfg = self.cfg
        src = "p25h13"
        live = bool(cfg.get("live"))
        sql_reader = cfg.get("sql_fact_reader")
        ds = self.create_dataset(api, "p25-h13", scenario="h13")
        # round-3 #1: explicit request_timeout_ms=30000 (limits default
        # 10000 / max 120000) so the terminal hold comfortably covers the
        # SQL + poll window around the due tick.
        source = self.create_source(
            api, "p25-h13-src",
            incremental_source_config(
                cfg["fixture_src_url"], src,
                {"sync": {"limits": {"request_timeout_ms": 30000}}}),
            "h13")

        def arm(def_id):
            arm_api = self.api_factory("h13", auth="arm")
            r = arm_api.call("connector", "POST",
                             route("schedule_arm", id=def_id), body={})
            view = r["json"] if isinstance(r["json"], dict) else {}
            ok = r["status"] == 200 and _arm_ready(view)
            res.gate("G_OWNER_DURABLE", ok,
                     "arm via Authorization header: status=%s armed=%r "
                     "scheduler_loop_running=%r (schedule_auth.go POST arm "
                     "returns exactly these flags; the loop must be "
                     "RUNNING — deployment sets "
                     "OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=1)" %
                     (r["status"], view.get("armed"),
                      view.get("scheduler_loop_running")))
            return ok

        def ticks_of(def_id):
            resp = api.call("connector", "GET",
                            route("schedule_ticks", id=def_id))
            body = (resp["json"] or []) if resp["status"] == 200 else []
            # created_at DESC on the wire -> chronological order
            return sorted([t for t in body if isinstance(t, dict)],
                          key=lambda t: _rfc3339(t.get("scheduled_for")))

        def wait_ticks(def_id, predicate, want, timeout_s):
            deadline = time.time() + timeout_s
            out = []
            while time.time() < deadline:
                out = [t for t in ticks_of(def_id) if predicate(t)]
                if len(out) >= want:
                    return out
                time.sleep(2)
            return out

        # ---- phase S: two successful scheduled batches ------------------
        sync_s, _ = self.make_sync_def(
            api, "p25-h13", source["id"], ds["id"],
            extra_cfg={"schedule_cron": "*/1 * * * *",
                       "schedule_time_zone": "UTC",
                       "schedule_enabled": True},
            scenario="h13")
        if not arm(sync_s["id"]):
            return res
        enq = wait_ticks(sync_s["id"],
                         lambda t: t.get("outcome") == "enqueued", 2, 130)
        res.gate("G_OWNER_DURABLE", len(enq) >= 2,
                 "scheduled enqueued ticks=%d (scheduled_for ASC)" %
                 len(enq))
        for i, t in enumerate(enq[:2]):
            rid = t.get("run_id")
            run, terminal = (expect_run_terminal(api, sync_s["id"], rid, 60)
                             if _is_uuidish(rid) else (None, False))
            ok = (terminal and run and run.get("id") == rid
                  and run.get("sync_def_id") == sync_s["id"]
                  and run.get("status") == "succeeded")
            res.gate("G_OWNER_DURABLE", ok,
                     "tick%d (ASC).run_id=%r -> real run %r" %
                     (i + 1, t.get("run_id"), run and run.get("status")))
            if ok:
                self.verify_run_artifact(
                    api, res, ds["id"], run, window_values(src, i),
                    "G_CAS_ATOMIC", "h13 tick%d" % (i + 1))
        cp = checkpoint_of(api, sync_s["id"]) or {}
        res.gate("G_CAS_ATOMIC", (cp.get("confirmed_epoch") or 0) >= 2,
                 "checkpoint epoch advanced by the ticks: %r" %
                 cp.get("confirmed_epoch"))
        api.call("connector", "DELETE", route("schedule_revoke",
                                              id=sync_s["id"]))
        api.call("connector", "PATCH", route("sync_patch", id=sync_s["id"]), body={"schedule_enabled": False})
        # ---- phase D: REAL busy on a fresh non-empty scheduled def ------
        ds_d = self.create_dataset(api, "p25-h13-busy", scenario="h13")
        sync_d, _ = self.make_sync_def(
            api, "p25-h13-busy", source["id"], ds_d["id"],
            extra_cfg={"schedule_cron": "*/1 * * * *",
                       "schedule_time_zone": "UTC",
                       "schedule_enabled": True},
            scenario="h13")
        if not arm(sync_d["id"]):
            return res
        d_enq = wait_ticks(sync_d["id"],
                           lambda t: t.get("outcome") == "enqueued", 1, 130)
        res.gate("G_OWNER_DURABLE", len(d_enq) >= 1,
                 "fresh busy def: first scheduled tick=%d (window A "
                 "consumed; window B remains NON-EMPTY for the hold)" %
                 len(d_enq))
        if not d_enq:
            return res
        d_tick1 = d_enq[0]
        rid = d_tick1.get("run_id")
        run, terminal = (expect_run_terminal(api, sync_d["id"], rid, 60)
                         if _is_uuidish(rid) else (None, False))
        first_ok = (terminal and run and run.get("id") == rid
                    and run.get("sync_def_id") == sync_d["id"]
                    and run.get("status") == "succeeded")
        res.gate("G_CAS_ATOMIC", first_ok,
                 "busy-def first scheduled tick exact run becomes succeeded")
        if not first_ok:
            return res
        self.verify_run_artifact(api, res, ds_d["id"], run,
                                 window_values(src, 0),
                                 "G_CAS_ATOMIC", "h13 busy-def tick1")
        # deterministic hold: start the manual run 3s BEFORE the actual
        # next due (scheduled_for + cron period). Terminal hold is 15s
        # (source request_timeout_ms=30000 covers it), so the SQL/poll
        # window comfortably spans the due tick.
        due = _rfc3339(d_tick1.get("scheduled_for")) + 60.0
        lead = 3.0
        wait_s = due - lead - time.time()
        if live and wait_s > 0:
            time.sleep(wait_s)
        fixture_set_knob(api, src, {"slow_terminal_ms": 15000})
        manual, _ = self.trigger_run(api, sync_d["id"], async_mode=True,
                                     wait=False, scenario="h13")
        if not manual or not manual.get("id"):
            res.gate("G_OWNER_DURABLE", False,
                     "manual hold run not admitted")
            fixture_set_knob(api, src, {"slow_terminal_ms": 0})
            return res
        # wait until the manual run is ADMITTED and holds the owner (the
        # SQL baseline MUST come after this: manual admission legitimately
        # advances the admission stamp, the busy tick must not).
        admitted = False
        deadline = time.time() + 10
        while time.time() < deadline:
            cpw = checkpoint_of(api, sync_d["id"]) or {}
            if (cpw.get("owner") or {}).get("run_id") == manual["id"]:
                admitted = True
                break
            time.sleep(0.5)
        res.gate("G_OWNER_DURABLE", admitted,
                 "manual hold run admitted and owns the checkpoint before "
                 "the busy window")
        if not admitted:
            fixture_set_knob(api, src, {"slow_terminal_ms": 0})
            return res
        max_active = 0
        if live:
            deadline = time.time() + 8
            while time.time() < deadline:
                s = fixture_snapshot(api, src) or {}
                max_active = max(max_active, s.get("active") or 0)
                if max_active >= 1:
                    break
                time.sleep(0.25)
            res.gate("G_OWNER_DURABLE", max_active >= 1,
                     "hold run genuinely in flight at the fixture "
                     "(active=%r) — never claim busy from an idle source"
                     % max_active)
        # SQL BASELINE (after manual admission, BEFORE the busy due tick)
        if sql_reader is None:
            res.gate("G_OWNER_DURABLE", False,
                     "--sql-env-dir required for the h13 busy SQL facts "
                     "(injectable readonly reader; no caller-JSON proof)")
            fixture_set_knob(api, src, {"slow_terminal_ms": 0})
            return res
        baseline = sql_reader.snapshot("h13-busy-baseline", sync_d["id"],
                                       source["id"])
        res.gate("G_OWNER_DURABLE", bool(baseline.get("ok")),
                 "SQL baseline captured (after manual admission, before "
                 "busy tick): rc=%r" % baseline.get("rc"))
        if live:
            res.gate("G_OWNER_DURABLE", baseline.get("captured_at", time.time()) < due
                     and time.time() < due,
                     "SQL baseline completed BEFORE the exact due window")
        # busy tick — only accepted while the owner is STILL held by the
        # manual run (if the hold is lost first, the busy claim is
        # honestly missed, never asserted).
        busy_tick = None
        deadline = time.time() + 45
        while time.time() < deadline:
            for t in ticks_of(sync_d["id"]):
                if t.get("outcome") == FIELDS["tick_busy_outcome"] and abs(_rfc3339(t.get("scheduled_for")) - due) < 0.01:
                    busy_tick = t
                    break
            if busy_tick:
                break
            cpw = checkpoint_of(api, sync_d["id"]) or {}
            if (cpw.get("owner") or {}).get("run_id") != manual["id"]:
                break                     # hold lost before due — miss
            time.sleep(0.5)
        still_held = (checkpoint_of(api, sync_d["id"]) or {}).get(
            "owner", {}).get("run_id") == manual["id"]
        res.gate("G_OWNER_DURABLE",
                 busy_tick is not None and still_held,
                 "busy due tick OBSERVED while owner still held (%s); a "
                 "missed window is reported as a miss, never claimed" %
                 ("tick seen" if busy_tick else
                  ("hold lost before due" if not still_held else "no tick")))
        # SQL POST snapshot (owner still held; before the manual run ends)
        post = sql_reader.snapshot("h13-busy-post", sync_d["id"],
                                   source["id"]) if still_held else None
        res.gate("G_OWNER_DURABLE", bool(post and post.get("ok")),
                 "SQL post snapshot captured after the busy tick "
                 "(owner still held)")
        if baseline.get("ok") and post and post.get("ok"):
            b, a = baseline["facts"], post["facts"]
            new_ticks = [t for t in a["ticks"] if t not in b["ticks"]]
            res.gate("G_OWNER_DURABLE", len(new_ticks) == 1 and
                     new_ticks[0].get("outcome") == FIELDS["tick_busy_outcome"] and
                     abs(_rfc3339(new_ticks[0].get("scheduled_for")) - due) < .01 and
                     new_ticks[0].get("run_id") in (None, "NULL"),
                     "SQL exact new busy tick matches due and has no admitted run")
            res.gate("G_OWNER_DURABLE", a["stamp"] == b["stamp"],
                     "admission stamp UNCHANGED by the busy tick "
                     "(baseline==post: %r)" % (a["stamp"] == b["stamp"]))
            res.gate("G_OWNER_DURABLE",
                     _rfc3339(a["next_fire"]) > _rfc3339(b["next_fire"]),
                     "cron cursor ADVANCED across the busy tick "
                     "(%s -> %s)" % (b["next_fire"], a["next_fire"]))
            res.gate("G_OWNER_DURABLE",
                     {r["id"] for r in a["runs"]} ==
                     {r["id"] for r in b["runs"]},
                     "NO new Run row from the busy tick (SQL run set "
                     "identical, %d rows)" % len(a["runs"]))
            res.gate("G_CAS_ATOMIC", a["cp"] == b["cp"],
                     "DATA checkpoint UNCHANGED across the busy tick "
                     "(epoch/len/sha/owner identical)")
        # stop the schedule BEFORE the following due tick, then let the
        # manual hold run finish legitimately.
        api.call("connector", "DELETE", route("schedule_revoke",
                                              id=sync_d["id"]))
        api.call("connector", "PATCH", route("sync_patch",
                                             id=sync_d["id"]),
                 body={"schedule_enabled": False})
        fixture_set_knob(api, src, {"slow_terminal_ms": 0})
        final, _ = expect_run_terminal(api, sync_d["id"], manual["id"], 90)
        if final and final.get("status") == "succeeded":
            self.verify_run_artifact(api, res, ds_d["id"], final,
                                     window_values(src, 1),
                                     "G_CAS_ATOMIC", "h13 hold run")
        else:
            res.gate("G_CAS_ATOMIC", False,
                     "manual hold run completes after the busy tick: %r" %
                     (final and final.get("status")))
        # schedule already revoked+disabled BEFORE the following due tick;
        # prove quiet now
        time.sleep(3)
        quiet_a = len(ticks_of(sync_d["id"]))
        time.sleep(3)
        quiet_b = len(ticks_of(sync_d["id"]))
        res.gate("G_OWNER_DURABLE", quiet_a == quiet_b,
                 "no further ticks after revoke+disable (%d==%d)" %
                 (quiet_a, quiet_b))
        return res

    def h14(self, api, res):
        cfg = self.cfg
        src = "p25h1"
        ds = self.create_dataset(api, "p25-h14", scenario="h14")
        source = self.create_source(
            api, "p25-h14-src",
            incremental_source_config(cfg["fixture_src_url"], src), "h14")
        sync, _ = self.make_sync_def(api, "p25-h14", source["id"], ds["id"],
                                     scenario="h14")
        proxy_set_mode(api, "drop_after_verify")
        before = [e for e in proxy_log(api)
                  if e.get("request_target") == ds["id"]]
        run, _ = self.trigger_run(api, sync["id"], scenario="h14")
        after = [e for e in proxy_log(api)
                 if e.get("request_target") == ds["id"]]
        delta = after[len(before):]
        incremental = [e for e in delta if e.get("mode") ==
                       "drop_after_verify" and e.get("forwarded") is True]
        entry = incremental[-1] if incremental else None
        res.gate("G_ACK_STRICT",
                 len(incremental) == 1 and entry is not None and
                 entry.get("verified_committed") is True,
                 "exactly ONE forwarded+verified drop entry for THIS "
                 "target (request_target filter; delta=%d incr=%d)" %
                 (len(delta), len(incremental)))
        # lost-ACK definition: the local run carries NO transaction.
        res.gate("G_RETAIN_UNKNOWN",
                 run and run.get("status") == "failed" and
                 not run.get("dataset_version_id"),
                 "run failed with EMPTY dataset_version_id (lost ACK): %r" %
                 (run and run.get("dataset_version_id")))
        remote_ok = False
        detail = "no ack_txn_id"
        if entry and entry.get("ack_txn_id"):
            ack_txn = entry["ack_txn_id"]
            txn = txn_get(api, ds["id"], ack_txn)
            meta = (txn or {}).get("metadata") or {}
            lineage = meta.get("lineage") or {}
            # bind the file by THIS transaction and download in full
            env = files_envelope(api, ds["id"])
            bound = bind_run_files(env, ack_txn) if env is not None else []
            want_raw = _ndjson(window_values(src, 0))
            raw = None
            if bound:
                dl = download_file(api, ds["id"], bound[0]["id"])
                raw = dl["raw"] or b""
            remote_ok = (txn is not None
                         and txn.get("status") == "COMMITTED"
                         and txn.get("dataset_id") == ds["id"]
                         and txn.get("branch_name") == "main"
                         and txn.get("tx_type") == "SNAPSHOT"
                         and lineage.get("sync_run_id") == (run or {}).get("id")
                         and raw == want_raw
                         and meta.get("content_hash") == _sha256(want_raw))
            detail = ("ack_txn=%s COMMITTED=%r target_ok=%s lineage_run=%s "
                      "download=%d bytes" %
                      (ack_txn[:8], (txn or {}).get("status"),
                       (txn or {}).get("dataset_id") == ds["id"],
                       lineage.get("sync_run_id") == (run or {}).get("id"),
                       len(raw or b"")))
        res.gate("G_ACK_STRICT", remote_ok,
                 "remote COMMITTED proven by INDEPENDENT direct reads "
                 "(flat txn GET + full file download bound to ack_txn_id; "
                 "the proxy ledger is not the commit proof): %s" % detail)
        cp = checkpoint_of(api, sync["id"]) or {}
        owner = cp.get("owner") or {}
        res.gate("G_RETAIN_UNKNOWN",
                 owner.get("run_id") and cp.get("confirmed_epoch") in (0, None),
                 "local owner retained + position unchanged (epoch=%r)" %
                 cp.get("confirmed_epoch"))
        blocked = api.call("connector", "POST", route("sync_run",
                                                      id=sync["id"]),
                           body={})
        res.gate("G_NO_REPLAY", blocked["status"] == 409,
                 "re-trigger blocked while owner unresolved (%s)" %
                 blocked["status"])
        if owner.get("run_id"):
            res.owner_roster.append({
                "def_id": sync["id"], "run_id": owner.get("run_id"),
                "run_status": owner.get("run_status"),
                "blocked_reason": owner.get("blocked_reason"),
                "mode": "drop_after_verify", "source": "h14-actual"})
        proxy_set_mode(api, "passthrough")
        # ONLY the scenario actually performed is declared here. v2's
        # predeclared driver placeholder rows are gone: lease-loss and
        # local-commit-unknown are performed (or not) by the driver/PG
        # tracks and enter the tail declaration through THEIR artifacts.
        return res

    def h15(self, api, res):
        cfg = self.cfg
        src = "p25h6"
        ds = self.create_dataset(api, "p25-h15", scenario="h15")
        source = self.create_source(
            api, "p25-h15-src",
            incremental_source_config(cfg["fixture_src_url"], src), "h15")
        sync, _ = self.make_sync_def(api, "p25-h15", source["id"], ds["id"],
                                     scenario="h15")
        fixture_set_knob(api, src, {"slow_terminal_ms": 4000})
        runs_before = runs_list(api, sync["id"]) or []
        cp_before = _h15_data_position(api.call("connector", "GET", route("checkpoint_view", id=sync["id"])))
        target_before = [e for e in proxy_log(api)
                         if e.get("request_target") == ds["id"]]
        run1, _ = self.trigger_run(api, sync["id"], async_mode=True,
                                   wait=False, scenario="h15")
        # manual busy = REAL sync POST (no async parameter)
        busy = api.call("connector", "POST", route("sync_run",
                                                   id=sync["id"]), body={})
        runs_during = runs_list(api, sync["id"]) or []
        cp_during = _h15_data_position(api.call("connector", "GET", route("checkpoint_view", id=sync["id"])))
        target_during = [e for e in proxy_log(api)
                         if e.get("request_target") == ds["id"]]
        code = (busy["json"] or {}).get("code") if isinstance(busy["json"],
                                                              dict) else None
        res.gate("G_OWNER_DURABLE",
                 busy["status"] == 409 and code == FIELDS["cp_busy_code"] and
                 len(runs_during) == len(runs_before) + 1,
                 "manual busy: 409/%r + exactly one new run row (the async "
                 "one), no extra run from busy" % code)
        res.gate("G_OWNER_DURABLE",
                 len(target_during) == len(target_before) and
                 cp_before is not None and cp_during is not None and cp_during == cp_before,
                 "zero dispatch for this target and data position "
                 "untouched during busy (epoch %r)" %
                 (cp_during or {}).get("confirmed_epoch"))
        fixture_set_knob(api, src, {"slow_terminal_ms": 0})
        final, _ = expect_run_terminal(api, sync["id"], run1["id"], 60)
        if final and final.get("status") == "succeeded":
            self.verify_run_artifact(api, res, ds["id"], final,
                                     window_values(src, 0),
                                     "G_OWNER_DURABLE", "h15 async")
        else:
            res.gate("G_OWNER_DURABLE", False,
                     "ONE real async success: %r" %
                     (final and final.get("status")))
        return res

    # -- registry dispatch -------------------------------------------------

    def run_scenario(self, sid, pg_proof_file=None):
        api = self.api_factory(sid)
        spec = next(s for s in SCENARIOS if s["id"] == sid)
        res = ScenarioResult(sid, spec["phase"])
        if sid == "h5":
            # attribution-only: the frozen predecessor suites carry legacy
            # compatibility; this engine adds no assertions and no calls.
            return res
        fn = getattr(self, sid, None)
        if sid == "h12":
            return self.h12(api, res, pg_proof_file)
        if fn is None:
            res.gate("G_OWNER_DURABLE", False, "scenario not implemented")
            return res
        try:
            fn(api, res)
        except Exception as exc:  # keep the failure as an honest gate
            res.gate("G_OWNER_DURABLE", False,
                     "engine exception: %s: %s" % (type(exc).__name__, exc))
        finally:
            if sid == "h13":
                for h in self.rec.handles:
                    if h.get("kind") == "def" and h.get("scenario") == "h13" and h.get("ok"):
                        try:
                            revoked = api.call("connector", "DELETE", route("schedule_revoke", id=h["ref"]))
                            disabled = api.call("connector", "PATCH", route("sync_patch", id=h["ref"]), body={"schedule_enabled": False})
                            res.gate("G_OWNER_DURABLE", revoked["status"] in (200, 204) and disabled["status"] in (200, 204),
                                     "finally revoke+disable owned schedule %s" % h["ref"])
                        except Exception as exc:
                            res.gate("G_OWNER_DURABLE", False, "finally schedule disable: %s" % type(exc).__name__)
        return res

    # -- strict final quiet -------------------------------------------------

    def final_quiet(self, api, srcs):
        """v3 strict: per source HTTP 200 with inventory_started/
        inventory_finished/active present as NON-BOOL ints, active==0,
        started==finished; proxy stats 200 with commit_inflight==0 (int)
        and commit_started==commit_finished. ANY collection error or
        missing field is a FAILURE (never silently swallowed)."""
        out = {"fixture": {}, "proxy": proxy_stats(api), "problems": []}
        if out["proxy"] is None:
            out["problems"].append("proxy stats unreadable")
        else:
            p = out["proxy"]
            for k in ("commit_inflight", "commit_started",
                      "commit_finished"):
                if not _is_int(p.get(k)):
                    out["problems"].append("proxy stats field %r missing "
                                           "or non-int" % k)
            if out["proxy"].get("commit_inflight") != 0:
                out["problems"].append("proxy commit_inflight != 0")
            if _is_int(p.get("commit_started")) and \
                    _is_int(p.get("commit_finished")) and \
                    p.get("commit_started") != p.get("commit_finished"):
                out["problems"].append("proxy started != finished")
        for src in srcs:
            snap = fixture_snapshot(api, src)
            if snap is None:
                out["fixture"][src] = None
                out["problems"].append("fixture src %s snapshot unreadable"
                                       % src)
                continue
            out["fixture"][src] = snap
            for k in ("inventory_started", "inventory_finished", "active"):
                if not _is_int(snap.get(k)):
                    out["problems"].append(
                        "%s field %r missing or non-int (got %r)" %
                        (src, k, snap.get(k)))
            if snap.get("active") != 0:
                out["problems"].append("%s active != 0" % src)
            if _is_int(snap.get("inventory_started")) and \
                    _is_int(snap.get("inventory_finished")) and \
                    snap.get("inventory_started") != \
                    snap.get("inventory_finished"):
                out["problems"].append("%s started != finished" % src)
        out["ok"] = not out["problems"]
        return out

    def final_rosters(self, api):
        """round-2 #7 + round-3 #5: AFTER the scenarios — on failures too
        — enumerate EVERY persisted def handle (a transport 503 may have
        created a Run whose id the caller never saw) and rebuild BOTH
        actual rosters from live rows with REAL parent joins (def ->
        source via the def GET; run -> def via the run row's own
        sync_def_id). Owners (terminal-failed included, with
        blocked_reason) and nonterminal runs stay distinct rosters."""
        owners, runs, problems = [], [], []
        def_ids, seen = [], set()
        for h in self.rec.handles:
            if h.get("kind") in ("def", "h10-def") and h.get("ok") and \
                    isinstance(h.get("ref"), str) and h["ref"] not in seen:
                seen.add(h["ref"])
                def_ids.append(h["ref"])
        for did in def_ids:
            dv = def_view(api, did)
            if dv is None:
                problems.append("final roster: def %s unreadable" % did)
                continue
            source_id = dv.get("source_id")
            cp = checkpoint_of(api, did)
            if cp is None:
                problems.append("final roster: def %s checkpoint unreadable"
                                % did)
            elif (cp.get("owner") or {}).get("run_id"):
                o = cp["owner"]
                owners.append({"def_id": did, "source_id": source_id,
                               "run_id": o.get("run_id"),
                               "run_status": o.get("run_status"),
                               "blocked_reason": o.get("blocked_reason"),
                               "source": "finally-actual"})
            rl = runs_list(api, did)
            if rl is None:
                problems.append("final roster: def %s runs unreadable" % did)
                continue
            if not _is_uuidish(source_id):
                problems.append("def %s lacks UUID source" % did)
            run_ids = [r.get("id") for r in rl if isinstance(r, dict)]
            if len(run_ids) != len(set(run_ids)):
                problems.append("duplicate run UUID under %s" % did)
            for r in rl:
                if not isinstance(r, dict) or r.get("sync_def_id") != did:
                    problems.append("run parent mismatch under %s" % did)
            for owner in [o for o in owners if o["def_id"] == did]:
                match = [r for r in rl if isinstance(r, dict) and r.get("id") == owner["run_id"]]
                if len(match) != 1 or match[0].get("sync_def_id") != did or match[0].get("status") != owner["run_status"]:
                    problems.append("owner run missing or parent/status mismatch under %s" % did)
            for r in rl:
                if isinstance(r, dict) and r.get("status") in \
                        ("pending", "running"):
                    runs.append({"def_id": did, "source_id": source_id,
                                 "run_id": r.get("id"),
                                 "sync_def_id": r.get("sync_def_id"),
                                 "status": r.get("status"),
                                 "generation": r.get("generation"),
                                 "attempt": r.get("attempt"),
                                 "control_mode": r.get("control_mode"),
                                 "publish_entered_at": r.get("publish_entered_at"),
                                 "source": "finally-actual"})
        return {"owner_roster": owners, "run_roster": runs,
                "defs": def_ids, "problems": problems}


# ---------------------------------------------------------------------------
# Fakes — mocked, PURE (no sockets) mirror of the REAL wire contracts:
#   * FakeIncrementalFixture mirrors fixtures/rest_source_p25.py exactly
#     (Tools r3 layout incl. p25h4's second NON-EMPTY window, page keys
#     pg-<sha12>, request-lifecycle counters inventory_started/
#     inventory_finished/active, ledger {token,cursor,page_size,window});
#   * FakeProxyDataset mirrors fixtures/dataset_fault_proxy_p25.py r3
#     (ledger keys request_target/ack_dataset_id/ack_txn_id/
#     ack_txn_status/verified_committed; fabrication entries carry
#     forwarded=false + fabricated_status) and hosts a REAL-shaped
#     dataset store (/v1 surface: files envelope, files index, flat
#     transaction GET, views/at ComputeView replay where the last SNAPSHOT
#     clears the prior effective set);
#   * FakeConnector mirrors contract v1.2.1 (single-flight owner,
#     gate-entry boundary, FULL usable-ACK validation, pre-gate release vs
#     post-dispatch retention, fences, schedule gating incl. the busy tick
#     outcome, org-2 tenant invisibility).
# Mock green is ENGINE logic evidence only — never connector evidence.
# ---------------------------------------------------------------------------


def _page_key(page):
    h = hashlib.sha256("\n".join(page).encode()).hexdigest()[:12]
    return "pg-%s" % h


class FakeIncrementalFixture:
    """In-memory mirror of the r2-corrected fixture core with the r3
    window layout. Position derives from the REQUEST token only."""

    def __init__(self):
        self.windows = {src: [list(p) for p in w] for src, w in
                        SRC_WINDOWS.items()}
        self.knobs = {src: {} for src in self.windows}
        self.ledger = {k: [] for k in self.windows}
        self.counters = {k: {"request_count": 0, "inventory_started": 0,
                             "inventory_finished": 0, "active": 0,
                             "total": sum(len(p) for p in w),
                             "mode": "ok", "name": k}
                         for k, w in self.windows.items()}

    def token_for(self, src, window):
        return src_token(src, window)

    def window_for(self, src, token):
        st = self.windows[src]
        if token is None:
            return 0                          # NULL -> FIRST window
        if token == self.token_for(src, len(st) - 1):
            return len(st)                    # exhausted position
        prefix = "%s-tok-" % src
        if not token.startswith(prefix):
            return None
        try:
            i = int(token[len(prefix):])
        except ValueError:
            return None
        if i >= len(st) - 1:
            return None                       # only the LAST token maps on
        return i + 1

    def data(self, src, params):
        if src not in self.windows:
            return 404, None
        st = self.windows[src]
        c = self.counters[src]
        c["request_count"] += 1
        c["inventory_started"] += 1
        c["active"] += 1
        try:
            since = params.get(P25_SINCE_PARAM)
            cursor = params.get("cursor", "")
            window = self.window_for(src, since)
            self.ledger[src].append({
                "since_param": P25_SINCE_PARAM, "token": since,
                "cursor": cursor, "page_size": 50,
                "ts": round(time.time(), 6),
                "window": window if window is not None else -1})
            if window is None:
                return 400, {"error": "unknown since token"}
            if window >= len(st):
                # exhausted: zero rows; re-issue the REQUEST token
                body = {"records": [], "next_cursor": ""}
                _set_path(body, P25_COMPLETED_PATH, since)
                return 200, body
            pages = st[window]
            keys = [_page_key(p) for p in pages]
            idx = keys.index(cursor) if cursor else 0
            knobs = self.knobs.get(src, {})
            if knobs.get("fail_mid_pages") and idx == 1 and cursor:
                return 500, {"error": "injected mid-page failure"}
            rows = pages[idx]
            nxt = keys[idx + 1] if idx + 1 < len(pages) else ""
            body = {"records": list(rows), "next_cursor": nxt}
            if nxt == "":
                if knobs.get("fail_terminal_after_rows"):
                    return 500, {"error":
                                 "injected terminal failure after rows"}
                _apply_candidate(self, src, body, window,
                                 knobs.get("candidate_mode"), since)
            return 200, body
        finally:
            c["inventory_finished"] += 1
            c["active"] = max(0, c["active"] - 1)

    def set_knob(self, src, knob):
        if src not in self.windows:
            return False
        self.knobs.setdefault(src, {}).update(knob or {})
        return True

    def snapshot(self, src):
        snap = dict(self.counters.get(src, {}))
        snap["name"] = src
        return snap


def _set_path(body, path, value):
    cur = body
    segs = path.split(".")
    for s in segs[:-1]:
        cur = cur.setdefault(s, {})
    cur[segs[-1]] = value


def _apply_candidate(fx, src, body, window, mode, request_token):
    good = fx.token_for(src, window)
    if mode == "missing":
        return
    if mode == "non_string":
        _set_path(body, P25_COMPLETED_PATH, {"not": "a string"})
        return
    if mode == "empty":
        _set_path(body, P25_COMPLETED_PATH, "")
        return
    if mode == "oversize":
        _set_path(body, P25_COMPLETED_PATH, "x" * 513)
        return
    if mode == "ctrl_chars":
        _set_path(body, P25_COMPLETED_PATH, "bad\x01token")
        return
    if mode == "echo_request_token" and request_token is not None:
        _set_path(body, P25_COMPLETED_PATH, request_token)
        return
    _set_path(body, P25_COMPLETED_PATH, good)


class FakeProxyDataset:
    """Fake dataset store + fault proxy mirroring the r3 ledger keys."""

    def __init__(self):
        self.mode = "passthrough"
        self.ledger = []
        self.stats = {"commit_started": 0, "commit_finished": 0,
                      "commit_inflight": 0, "modes": {}}
        self.datasets = {}
        self.seq = 0

    # ---- dataset store ---------------------------------------------------

    def create_dataset(self, name):
        self.seq += 1
        did = str(_new_uuid())
        self.datasets[did] = {"name": name, "txns": [],
                              "branch_id": str(_new_uuid())}
        return {"id": did, "name": name}

    def _real_commit(self, dataset_id, values, lineage):
        txn = _new_uuid()
        raw = _ndjson(values)
        fid = "file-%s" % txn[:8]
        file = {"id": fid, "dataset_id": dataset_id,
                "transaction_id": txn,
                "logical_path": "sync-%d.ndjson" % (len(
                    self.datasets[dataset_id]["txns"]) + 1),
                "physical_uri": "local:///%s" % fid,
                "size_bytes": len(raw)}
        self.datasets[dataset_id]["txns"].append({
            "id": txn, "file": file, "values": values, "raw": raw,
            "metadata": {"content_hash": _sha256(raw),
                         "rows": len(values), "bytes": len(raw),
                         "lineage": dict(lineage)},
            "committed_at": "2026-11-14T00:00:00Z"})
        return txn

    def files_envelope(self, dataset_id):
        ds = self.datasets.get(dataset_id)
        if ds is None:
            return None
        files = [dict(t["file"]) for t in ds["txns"]]
        return {"branch": "main", "total": len(files), "files": files}

    def txn_get(self, dataset_id, txn):
        ds = self.datasets.get(dataset_id)
        if ds is None:
            return None
        for t in ds["txns"]:
            if t["id"] == txn:
                return {"id": t["id"], "dataset_id": dataset_id,
                        "branch_id": ds["branch_id"], "branch_name": "main",
                        "tx_type": "SNAPSHOT", "status": "COMMITTED",
                        "summary": "fake", "metadata": t["metadata"],
                        "committed_at": t["committed_at"]}
        return None

    def download(self, dataset_id, file_id):
        ds = self.datasets.get(dataset_id)
        if ds is None:
            return None
        for t in ds["txns"]:
            if t["file"]["id"] == file_id:
                return t["raw"]
        return None

    def view_at(self, dataset_id, branch="main"):
        ds = self.datasets.get(dataset_id)
        if ds is None:
            return None
        # ComputeView replay: the LAST SNAPSHOT clears the prior
        # effective set — the effective files are the head txn's files.
        head = ds["txns"][-1] if ds["txns"] else None
        files = ([{"logical_path": head["file"]["logical_path"],
                   "physical_path": head["file"]["physical_uri"],
                   "size_bytes": head["file"]["size_bytes"],
                   "introduced_by": head["id"]}] if head else [])
        return {"id": "view-%s" % dataset_id[:11], "dataset_id": dataset_id,
                "branch_id": ds["branch_id"],
                "head_transaction_id": head["id"] if head else _new_uuid(),
                "requested_branch": branch, "resolved_branch": branch,
                "fallback_chain": [], "computed_at": "2026-11-14T00:00:00Z",
                "file_count": len(files),
                "size_bytes": sum(f["size_bytes"] for f in files),
                "files": files}

    # ---- proxy -----------------------------------------------------------

    def set_mode(self, mode):
        if mode not in PROXY_MODES:
            return False
        self.mode = mode
        return True

    def _record(self, **kv):
        self.ledger.append(kv)
        m = kv.get("mode")
        self.stats["modes"][m] = self.stats["modes"].get(m, 0) + 1

    def commit(self, dataset_id, values, lineage):
        """Returns an ack dict: {status, body} — body None means the
        response was dropped (connector observes a timeout)."""
        self.stats["commit_started"] += 1
        self.stats["commit_inflight"] += 1
        try:
            if self.mode == "passthrough":
                txn = self._real_commit(dataset_id, values, lineage)
                body = {"dataset_id": dataset_id,
                        "transaction": {"id": txn, "status": "COMMITTED"}}
                self._record(mode="passthrough", method="POST",
                             path="/api/v1/datasets/%s/outputs:commit"
                                  % dataset_id,
                             forwarded=True, request_target=dataset_id,
                             upstream_status=200,
                             ack_dataset_id=dataset_id, ack_txn_id=txn,
                             ack_txn_status="COMMITTED")
                return {"status": 200, "body": body}
            if self.mode == "drop_after_verify":
                txn = self._real_commit(dataset_id, values, lineage)
                body = {"dataset_id": dataset_id,
                        "transaction": {"id": txn, "status": "COMMITTED"}}
                self._record(mode="drop_after_verify", method="POST",
                             path="/api/v1/datasets/%s/outputs:commit"
                                  % dataset_id,
                             forwarded=True, request_target=dataset_id,
                             upstream_status=200,
                             ack_dataset_id=dataset_id, ack_txn_id=txn,
                             ack_txn_status="COMMITTED",
                             verified_committed=True,
                             upstream_ack_sha256=_sha256(
                                 json.dumps(body).encode()))
                return {"status": None, "body": None}   # dropped
            fabricated_status = 502 if self.mode == "http_500" else 200
            self._record(mode=self.mode, method="POST",
                         path="/api/v1/datasets/%s/outputs:commit"
                              % dataset_id,
                         forwarded=False, request_target=dataset_id,
                         fabricated_status=fabricated_status)
            if self.mode == "http_500":
                return {"status": 502, "body": {"error": "injected"}}
            if self.mode == "no_txn_200":
                return {"status": 200, "body": {
                    "dataset_id": dataset_id,
                    "transaction": {"status": "COMMITTED"}}}
            if self.mode == "not_committed":
                return {"status": 200, "body": {
                    "dataset_id": dataset_id,
                    "transaction": {"id": _new_uuid(),
                                    "status": "OPEN"}}}
            if self.mode == "wrong_target":
                return {"status": 200, "body": {
                    "dataset_id": _new_uuid(),
                    "transaction": {"id": _new_uuid(),
                                    "status": "COMMITTED"}}}
            return {"status": 500, "body": {"error": "bad mode"}}
        finally:
            self.stats["commit_finished"] += 1
            self.stats["commit_inflight"] = max(
                0, self.stats["commit_inflight"] - 1)


class FakeConnector:
    """In-memory connector honouring contract v1.2.1."""

    def __init__(self, fixture, proxy):
        self.fixture = fixture
        self.proxy = proxy
        self.sources = {}
        self.defs = {}
        self.runs = {}
        self.schedule = {}      # did -> {"armed": bool}
        self.busy_ticks = {}    # did -> appended busy tick count
        self.tick_history = {}  # did -> [tick, ...] (GET /schedule/ticks)
        self.scheduler_loop_running = True   # ScheduleRunner.Enabled()
        self.seq = 0

    # ---- objects ---------------------------------------------------------

    def create_source(self, sid, cfg):
        self.sources[sid] = {"id": sid, "config": cfg, "name": sid,
                             "last_admitted": 0}
        return 201, {"id": sid}

    def create_def(self, did, source_id, output_dataset_id, extra=None):
        d = {"id": did, "source_id": source_id,
             "output_dataset_id": output_dataset_id, "file_glob": None,
             "schedule_cron": None, "schedule_enabled": False,
             "schedule_next_fire_at": None,
             "owner": None, "epoch": 0, "value": None, "stale": False,
             "fingerprint": _sha256(json.dumps(
                 self.sources[source_id]["config"], sort_keys=True,
                 default=str))}
        for k in ("schedule_cron", "schedule_time_zone", "schedule_enabled",
                  "file_glob"):
            if extra and k in extra:
                d[k] = extra[k]
        self.defs[did] = d
        self.schedule[did] = {"armed": False}
        return d

    def def_view(self, did):
        d = self.defs.get(did)
        if d is None:
            return None
        return {"id": d["id"], "source_id": d["source_id"],
                "output_dataset_id": d["output_dataset_id"],
                "file_glob": d["file_glob"],
                "schedule_cron": d["schedule_cron"],
                "schedule_enabled": d["schedule_enabled"],
                "schedule_next_fire_at": d["schedule_next_fire_at"]}

    # ---- runs ------------------------------------------------------------

    def trigger(self, did, async_mode=False):
        d = self.defs[did]
        if d["owner"] is not None:
            return 409, {"code": FIELDS["cp_busy_code"],
                         "active_run_id": d["owner"],
                         "active_since": "2026-11-14T00:00:00Z",
                         "retry_hint": "unconfirmed outcome; reconcile"}
        if d["stale"]:
            return 409, {"code": FIELDS["cp_stale_code"],
                         "recovery": "new sync def required"}
        self.seq += 1
        run_id = str(_new_uuid())
        run = {"id": run_id, "sync_def_id": did, "status": "pending",
               "started_at": "2026-11-14T00:00:00Z", "finished_at": None,
               "rows_written": 0, "bytes_written": 0,
               "publish_entered_at": None,
               "incremental": {"mode": "cursor", "base_epoch": d["epoch"]},
               "dataset_version_id": None, "content_hash": None,
               "error": None, "control_mode": "controlled",
               "generation": 1, "attempt": 1}
        self.runs[run_id] = run
        d["owner"] = run_id
        # admission stamp mirror: EVERY admitted trigger advances it (the
        # busy tick admits nothing, so its stamp stays put)
        srcrow = self.sources.get(d["source_id"])
        if srcrow is not None:
            srcrow["last_admitted"] = srcrow.get("last_admitted", 0) + 1
        if async_mode:
            run["_pending"] = True        # deferred worker stand-in
            run["_def"] = did
            return 202, dict(run)
        self._execute(d, run)
        return 200, dict(run)

    def poll_run(self, run_id):
        run = self.runs.get(run_id)
        if run and run.get("_pending"):
            src = _src_of(self, run["_def"])
            if not self.fixture.knobs.get(src, {}).get("slow_terminal_ms"):
                del run["_pending"]
                self._execute(self.defs[run["_def"]], run)
        return run

    def cancel_run(self, run_id):
        run = self.runs.get(run_id)
        if not run:
            return 404, {"error": "not found"}
        if run.get("_pending"):
            del run["_pending"]            # provably pre-gate
            run["status"] = "aborted"
            self.defs[run["_def"]]["owner"] = None
            return 200, dict(run)
        return 202, dict(run)

    def _execute(self, d, run):
        cfg = self.sources[d["source_id"]]["config"]
        src = cfg["sync"]["query_params"]["src"]
        knobs = self.fixture.knobs.get(src, {})
        since = d["value"]
        params = {}
        if since is not None:
            params[P25_SINCE_PARAM] = since
        rows, candidate, failed, cursor, pages = [], None, None, "", 0
        max_pages = cfg["sync"]["limits"]["max_pages"]
        while True:
            params["cursor"] = cursor
            status, body = self.fixture.data(src, params)
            pages += 1
            if status != 200:
                failed = "upstream %d" % status
                break
            rows.extend(body["records"])
            nxt = body.get("next_cursor", "")
            if nxt == "":
                cand = (body.get("meta") or {}).get("completed_token")
                candidate = cand
                break
            if pages >= max_pages:
                failed = "exceeded max_pages limit (%d)" % max_pages
                break
            cursor = nxt
        if failed is None:
            problem = None
            if not isinstance(candidate, str) or candidate == "":
                problem = ("rest sync: completed checkpoint candidate is "
                           "missing or invalid")
            elif len(candidate.encode("utf-8")) > 512 or \
                    any(c < 0x20 or c == 0x7F for c in
                        candidate.encode("utf-8")):
                problem = ("rest sync: completed checkpoint candidate is "
                           "missing or invalid")
            elif rows and since is not None and candidate == since:
                problem = ("rest sync: records returned but completed "
                           "checkpoint did not advance")
            if problem:
                failed = problem
        if failed is not None:
            # PRE-GATE failure (pull/candidate/limits): release the owner.
            run["status"], run["error"] = "failed", failed
            d["owner"] = None
            return
        run["publish_entered_at"] = "2026-11-14T00:00:05Z"  # gate ENTRY
        ack = self.proxy.commit(d["output_dataset_id"], rows,
                                {"sync_run_id": run["id"]})
        body = (ack or {}).get("body") or {}
        txn = ((body.get("transaction") or {}).get("id"))
        usable = ((ack or {}).get("status") == 200 and txn
                  and (body.get("transaction") or {}).get("status") ==
                  "COMMITTED"
                  and body.get("dataset_id") == d["output_dataset_id"])
        if not usable:
            # post-dispatch unknown: honest failed, owner RETAINED
            run["status"], run["error"] = "failed", "ack unusable"
            return
        run["status"] = "succeeded"
        run["dataset_version_id"] = txn
        raw = _ndjson(rows)
        run["content_hash"] = _sha256(raw)
        run["rows_written"] = len(rows)
        run["bytes_written"] = len(raw)
        d["epoch"] += 1
        d["value"] = candidate
        d["owner"] = None

    # ---- patches / fences --------------------------------------------------

    def patch_def(self, did, body):
        d = self.defs[did]
        protected = [k for k in ("output_dataset_id", "file_glob") if k in body]
        if protected and d["owner"]:
            return 409, {"error": _owner_fence_message(did, d["owner"])}
        for k in ("output_dataset_id", "file_glob", "schedule_cron",
                  "schedule_time_zone", "schedule_enabled"):
            if k in body:
                d[k] = body[k]
        return 200, {"id": did}

    def patch_connection(self, source_id, body):
        # only an effective CONFIG change is fenced; name/status-only
        # patches succeed even while an owner is held.
        if "config" in body:
            for d in self.defs.values():
                if d["source_id"] == source_id and d["owner"]:
                    return 409, {"error": "source config cannot change while pending or running sync runs exist"}
        if "config" in body:
            self.sources[source_id]["config"] = body["config"]
            new_fp = _sha256(json.dumps(body["config"], sort_keys=True,
                                        default=str))
            for d in self.defs.values():
                if d["source_id"] == source_id:
                    if d["fingerprint"] != new_fp and d["value"] is not None:
                        d["stale"] = True
        if "name" in body:
            self.sources[source_id]["name"] = body["name"]
        return 200, {"id": source_id}

    def set_credential(self, source_id):
        for d in self.defs.values():
            if d["source_id"] == source_id and d["owner"]:
                return 409, {"error": _owner_fence_message(d["id"], d["owner"])}
        return 200, {"ok": True}

    def checkpoint_view(self, did):
        d = self.defs[did]
        owner = None
        if d["owner"]:
            run = self.runs.get(d["owner"], {})
            blocked = ""
            if run.get("status") == "failed":
                blocked = FIELDS["blocked_reason_business"]
            owner = {"run_id": d["owner"],
                     "run_status": run.get("status"),
                     "since": "2026-11-14T00:00:00Z",
                     "blocked_reason": blocked}
        val = d["value"]
        return {"mode": "cursor", "initialized": val is not None,
                "checkpoint_len": len(val) if val else None,
                "checkpoint_sha256": _sha256(val) if val else None,
                "confirmed_epoch": d["epoch"], "owner": owner}

    # ---- scheduling --------------------------------------------------------

    def arm(self, did, on):
        """Mirrors the REAL POST arm body (schedule_auth.go:666-674):
        {armed, grant_id, actor_subject, schedule_revision,
        authorization_expires_at, scheduler_loop_running, note} — no
        'live'/'running' field. scheduler_loop_running reflects the
        (fake) ScheduleRunner.Enabled() and can be flipped for negative
        controls."""
        self.schedule.setdefault(did, {})["armed"] = on
        return 200, {"armed": bool(on),
                     "grant_id": "grant-%s" % did,
                     "actor_subject": "00000000-0000-4000-8000-%012d" % (
                         abs(hash(did)) % 10**12),
                     "schedule_revision": 1,
                     "authorization_expires_at": "2026-11-14T01:00:00Z",
                     "scheduler_loop_running": self.scheduler_loop_running,
                     "note": "per-instance authorization (mocked shape)"}

    def _tick_ts(self, did):
        """Synthetic monotonically-advancing scheduled_for (per def, one
        minute apart, in the past so mocked timing math stays instant)."""
        n = len(self.tick_history.get(did, []))
        return "2026-11-14T%02d:%02d:00Z" % (n // 60, n % 60)

    def due_tick(self, did):
        """One due-tick decision for GET /schedule/ticks (real shape:
        {outcome, run_id?, scheduled_for, claimed_at}): enqueued with a
        REAL run when free; skipped_incremental_busy when the owner is
        held (NO run row, cron cursor still advances); nothing when the
        schedule is off or the stream is exhausted."""
        d = self.defs[did]
        if not (self.schedule.get(did, {}).get("armed")
                and d.get("schedule_enabled")
                and self.scheduler_loop_running):
            return None
        ts = self._tick_ts(did)
        # every recorded tick advances the cron cursor (next fire)
        d["schedule_next_fire_at"] = "2026-11-14T%02d:%02d:00Z" % (
            (len(self.tick_history.get(did, [])) + 1) // 60,
            (len(self.tick_history.get(did, [])) + 1) % 60)
        if d["owner"] is not None:
            n = self.busy_ticks.get(did, 0)
            self.busy_ticks[did] = n + 1
            if n == 0:
                return {"outcome": FIELDS["tick_busy_outcome"],
                        "run_id": None, "scheduled_for": ts,
                        "claimed_at": ts, "created_at": ts}
            return None
        src = _src_of(self, did)
        if d["epoch"] >= len(SRC_WINDOWS[src]):
            return None                        # nothing left to enqueue
        status, run = self.trigger(did)
        if status != 200:
            return None
        return {"outcome": "enqueued", "run_id": run["id"],
                "scheduled_for": ts, "claimed_at": ts, "created_at": ts}


class FakeApi:
    """Same call() surface as HttpApi, backed by the fakes (pure; no
    sockets). auth='org2' mirrors tenant invisibility (404)."""

    def __init__(self, connector, fixture, proxy, recorder, scenario,
                 auth="main"):
        self.connector, self.fixture, self.proxy = connector, fixture, proxy
        self.rec, self.scenario, self.auth = recorder, scenario, auth

    def call(self, base, method, path, body=None, **_):
        status, payload, raw = self._route(base, method, path, body)
        rec_detail = {"base": base, "method": method, "path": path,
                      "status": status, "auth": self.auth}
        if raw is None and payload is not None:
            rec_detail["resp"] = _truncate(payload)
        self.rec.record(self.scenario, "mocked-http", rec_detail)
        return {"status": status, "json": payload, "raw": raw,
                "error": None,
                "text": "" if raw is not None else json.dumps(
                    payload, default=str)}

    # -- routing -----------------------------------------------------------

    def _route(self, base, method, path, body):
        c, fx, px = self.connector, self.fixture, self.proxy
        if base == "fixture":
            if path.startswith("/admin/knob/"):
                src = path.rsplit("/", 1)[-1]
                ok = fx.set_knob(src, body or {})
                return (200 if ok else 404), {"ok": ok}, None
            if path.startswith("/admin/log/"):
                src = path.rsplit("/", 1)[-1]
                return 200, list(fx.ledger.get(src, [])), None
            if path.startswith("/admin/src/"):
                src = path.rsplit("/", 1)[-1]
                snap = fx.snapshot(src)
                return (200 if snap else 404), snap, None
            return 404, {"error": "unknown"}, None
        if base == "proxy_admin":
            if path == "/admin/mode" and method == "POST":
                ok = px.set_mode((body or {}).get("mode", ""))
                return (200 if ok else 400), {"ok": ok}, None
            if path == "/admin/mode":
                return 200, {"mode": px.mode}, None
            if path == "/admin/log":
                return 200, list(px.ledger), None
            if path == "/admin/stats":
                return 200, dict(px.stats), None
            return 404, {"error": "unknown"}, None
        if base == "dataset":
            dpath = path.split("?", 1)[0]      # views/at carries ?branch=
            if method == "POST" and dpath == "/v1/datasets":
                return 201, px.create_dataset(body["name"]), None
            did = self._dsid(dpath)
            if dpath == "/v1/datasets/%s/files" % did:
                env = px.files_envelope(did)
                return (200 if env is not None else 404), env, None
            if dpath.endswith("/download"):
                fid = dpath.split("/files/", 1)[1].rsplit("/download", 1)[0]
                data = px.download(did, fid)
                if data is None:
                    return 404, {"error": "no such file"}, None
                return 200, None, data
            if "/branches/" in dpath and "/transactions/" in dpath:
                txn = dpath.rsplit("/", 1)[-1]
                t = px.txn_get(did, txn)
                return (200 if t is not None else 404), t, None
            if dpath.endswith("/views/at"):
                view = px.view_at(did, "main")
                return (200 if view is not None else 404), view, None
            return 404, {"error": "unknown dataset route"}, None
        if base == "connector":
            return self._connector(method, path, body)
        return 404, {"error": "unknown"}, None

    def _dsid(self, path):
        return path.split("/datasets/", 1)[1].split("/", 1)[0]

    def _connector(self, method, path, body):
        c = self.connector
        if self.auth == "org2" and method == "GET" and (
                "/checkpoint" in path or "/runs" in path):
            # tenant fence: another org cannot see this def's CP or runs
            return 404, {"error": "not found"}, None
        if method == "POST" and path == "/api/v1/connections":
            sid = str(_new_uuid())
            return self._pack(c.create_source(sid, body["config"]))
        if method == "PATCH" and path.startswith("/api/v1/connections/"):
            return self._pack(c.patch_connection(
                path.rsplit("/", 1)[-1], body or {}))
        if method == "POST" and path.endswith("/credentials"):
            sid = path.split("/sources/", 1)[1].split("/credentials")[0]
            return self._pack(c.set_credential(sid))
        if method == "POST" and path == "/api/v1/data-connection/syncs":
            extra = {k: v for k, v in (body or {}).items()
                     if k in ("schedule_cron", "schedule_time_zone",
                              "schedule_enabled", "file_glob")}
            did = str(_new_uuid())
            d = c.create_def(did, body["source_id"],
                             body["output_dataset_id"], extra)
            return 201, {"id": d["id"]}, None
        if method == "PATCH" and path.startswith(
                "/api/v1/data-connection/syncs/"):
            return self._pack(c.patch_def(path.rsplit("/", 1)[-1],
                                          body or {}))
        if method == "POST" and "/run?async" in path:
            did = path.split("/syncs/", 1)[1].split("/run", 1)[0]
            status, payload = c.trigger(did, async_mode=True)
            return self._pack((status, _public_run(payload)))
        if method == "POST" and path.endswith("/run"):
            did = path.split("/syncs/", 1)[1].rsplit("/run", 1)[0]
            status, payload = c.trigger(did)
            return self._pack((status, _public_run(payload)))
        if method == "POST" and path.endswith("/cancel"):
            rid = path.rsplit("/runs/", 1)[1].rsplit("/cancel", 1)[0]
            return self._pack(c.cancel_run(rid))
        if method == "GET" and \
                path.startswith("/api/v1/data-connection/syncs/"):
            tail = path[len("/api/v1/data-connection/syncs/"):].split("?")[0]
            if "/" not in tail:               # bare def GET (sync_get)
                dv = c.def_view(tail)
                return self._pack(dv if dv else {"error": "not found"})
        if method == "GET" and "/checkpoint" in path:
            did = path.split("/syncs/", 1)[1].split("/checkpoint", 1)[0]
            return 200, c.checkpoint_view(did), None
        if method == "GET" and "/runs/" in path:
            rid = path.rsplit("/", 1)[-1]
            run = c.poll_run(rid)
            return self._pack(_public_run(run, incremental=True) if run
                              else {"error": "not found"})
        if method == "GET" and path.endswith("/runs"):
            did = path.split("/syncs/", 1)[1].rsplit("/runs", 1)[0]
            return 200, [_public_run(r) for r in c.runs.values()
                         if r.get("sync_def_id") == did], None
        if path.endswith("/schedule:arm") and method == "POST":
            return self._pack(c.arm(path.split("/syncs/", 1)[1]
                                    .rsplit("/schedule", 1)[0], True))
        if path.endswith("/schedule:arm") and method == "DELETE":
            return self._pack(c.arm(path.split("/syncs/", 1)[1]
                                    .rsplit("/schedule", 1)[0], False))
        if path.endswith("/schedule/ticks") and method == "GET":
            did = path.split("/syncs/", 1)[1].rsplit("/schedule/ticks",
                                                     1)[0]
            tick = c.due_tick(did)
            if tick is not None:
                c.tick_history.setdefault(did, []).append(tick)
            return 200, list(c.tick_history.get(did, [])), None
        return 404, {"error": "unknown connector route"}, None

    @staticmethod
    def _pack(pair):
        if isinstance(pair, tuple):
            status, payload = pair
        else:                      # bare payload -> implicit 200
            status, payload = 200, pair
        return status, payload, None


def _public_run(run, incremental=False):
    return {k: v for k, v in run.items() if not k.startswith("_")
            and (incremental or k != "incremental")}


def _src_of(connector, did):
    d = connector.defs[did]
    return connector.sources[d["source_id"]]["config"]["sync"][
        "query_params"]["src"]


def _truncate(payload):
    try:
        return json.loads(json.dumps(payload, ensure_ascii=False,
                                     default=str))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# round-3 #4/#5: TWO-STEP tail contract.
#   (a) BEFORE the tail run: a hash-frozen SEMANTIC declaration (tags,
#       modes, expected Run/Owner states and counts — p25-tail-roster-v1
#       spec; optionally already-known h10 bindings under known_bindings,
#       which live OUTSIDE the sha-frozen spec). NO invented future UUIDs.
#   (b) AFTER the actual cases: the engine joins its real parents and
#       outputs tag -> actual {def_id, source_id, run_id}; the SAME
#       immutable declaration then adjudicates EXACTLY BOTH rosters
#       (owners AND nonterminal runs) — missing/extras/duplicates/orphan
#       runs hard-fail, and UUID shapes alone are insufficient (the run's
#       own sync_def_id and the def's source_id must join).
# ---------------------------------------------------------------------------

_UUID_CHARS = set("0123456789abcdefABCDEF-")


def _is_uuidish(v):
    import uuid
    try:
        return isinstance(v, str) and str(uuid.UUID(v)) == v.lower()
    except (ValueError, TypeError, AttributeError):
        return False


def _is_sha256hex(v):
    return (isinstance(v, str) and len(v) == 64
            and all(c in "0123456789abcdef" for c in v))


_CRASH_TS_KEYS = ("pre_at", "kill_at", "started_after_at", "post_at")


def load_semantic_declaration(path):
    """Load + verify the hash-frozen semantic declaration (schema/ok and
    declaration_sha256 recomputed over spec; known_bindings optional and
    outside the frozen sha)."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            decl = json.load(fh)
    except (OSError, ValueError):
        return None, "declaration file unreadable or invalid JSON"
    if not isinstance(decl, dict):
        return None, "declaration must be an object"
    if decl.get("schema") != "p25-tail-roster-v1" or decl.get("ok") is \
            not True:
        return None, "declaration schema/ok invalid (schema=%r ok=%r)" % \
            (decl.get("schema"), decl.get("ok"))
    spec = decl.get("spec")
    if not isinstance(spec, dict):
        return None, "declaration spec missing"
    if _sha256(json.dumps(spec, sort_keys=True)) != \
            decl.get("declaration_sha256"):
        return None, "declaration_sha256 mismatch (spec edited after " \
                     "freeze?)"
    for key in ("owner_rows", "run_rows"):
        if not isinstance(spec.get(key), list):
            return None, "%s must be an array" % key
        tags = []
        for row in spec[key]:
            if not isinstance(row, dict) or not isinstance(row.get("def_tag"), str) or not row["def_tag"]:
                return None, "each semantic row needs a def_tag"
            if any(k in row for k in ("def_id", "source_id", "run_id")):
                return None, "semantic rows must not contain future UUID bindings"
            tags.append(row["def_tag"])
            if key == "owner_rows":
                if row.get("run_ref") not in ("terminal-failed", "same") or row.get("blocked_reason") not in ("", "requires_reconciliation"):
                    return None, "invalid owner state declaration"
            elif type(row.get("count")) is not int or row["count"] < 1 or row.get("semantics") != "p25-crash-held-prepublish":
                return None, "run semantics/count must describe actual held prepublish crash"
        if len(tags) != len(set(tags)):
            return None, "duplicate declaration tag"
    known = decl.get("known_bindings")
    if known is not None and not isinstance(known, dict):
        return None, "known_bindings must be an object of tag -> ids"
    for tag, b in (known or {}).items():
        if not isinstance(b, dict) or any(not _is_uuidish(b.get(k)) for k in ("def_id", "source_id", "run_id")):
            return None, "known binding requires actual UUID parents"
    return decl, None


def _consume_owner(expected_terminal, actual):
    status = actual.get("run_status")
    return status in ("failed", "aborted") if expected_terminal \
        else status in ("pending", "running")


def adjudicate_tail(decl, owners_actual, runs_actual):
    """Exact two-roster adjudication against the immutable semantic
    declaration. Returns a verdict dict with the actual tag bindings
    (round-3 #4b output shape) and hard failures."""
    out = {"ok": False, "problems": [], "bindings": {}}
    spec = decl.get("spec") or {}
    known = decl.get("known_bindings") or {}
    owner_rows = spec.get("owner_rows") or []
    run_rows = spec.get("run_rows") or []
    problems = out["problems"]

    def dup_check(tag, ids):
        if tag in out["bindings"]:
            problems.append("duplicate tag %r" % tag)
            return True
        out["bindings"][tag] = ids
        return False

    # ---- owners: known bindings consume their exact actual rows first
    used = set()

    def owner_key(o):
        return (o.get("def_id"), o.get("run_id"))

    for row in owner_rows:
        tag = row.get("def_tag")
        terminal = row.get("run_ref") == "terminal-failed"
        kb = known.get(tag)
        if isinstance(kb, dict) and _is_uuidish(kb.get("def_id")) and \
                _is_uuidish(kb.get("run_id")):
            hit = next((o for o in owners_actual
                        if owner_key(o) == (kb.get("def_id"),
                                            kb.get("run_id"))
                        and owner_key(o) not in used), None)
            if hit is None:
                problems.append("known-bound owner %r not observed" % tag)
                continue
            if kb.get("source_id") != hit.get("source_id"):
                problems.append("known-bound owner %r source mismatch" %
                                tag)
            if not _consume_owner(terminal, hit):
                problems.append("owner %r run_ref=%r but run_status=%r" %
                                (tag, row.get("run_ref"),
                                 hit.get("run_status")))
            used.add(owner_key(hit))
            dup_check(tag, {"def_id": hit.get("def_id"),
                            "source_id": hit.get("source_id"),
                            "run_id": hit.get("run_id"),
                            "kind": "owner", "mode": row.get("mode")})
            continue
        if not row.get("mode"):
            problems.append("non-mode owner %r requires a known actual binding" % tag)
            continue
        # unbound tag: match by mode first (engine-assigned), else by
        # run_ref semantics, deterministically ordered
        pool = [o for o in owners_actual if owner_key(o) not in used and
                _consume_owner(terminal, o)]
        if row.get("mode"):
            mpool = [o for o in pool if o.get("mode") == row.get("mode")]
            pool = mpool
        if not pool:
            problems.append("declared owner %r has no actual row (missing "
                            "expected owner FAILS)" % tag)
            continue
        pool.sort(key=lambda o: str(o.get("run_id")))
        hit = pool[0]
        used.add(owner_key(hit))
        dup_check(tag, {"def_id": hit.get("def_id"),
                        "source_id": hit.get("source_id"),
                        "run_id": hit.get("run_id"), "kind": "owner",
                        "mode": row.get("mode")})
    extras = [o for o in owners_actual if owner_key(o) not in used]
    for o in extras:
        problems.append("observed owner def=%s run=%s is NOT declared "
                        "(extra owner FAILS)" %
                        (o.get("def_id"), o.get("run_id")))

    # ---- runs: every declared run row expects count NONTERMINAL runs
    rused = set()

    def run_key(r):
        return (r.get("def_id"), r.get("run_id"))

    for row in run_rows:
        tag = row.get("def_tag")
        count = row.get("count") if isinstance(row.get("count"), int) \
            and row.get("count") >= 1 else 1
        for i in range(count):
            sub = tag if count == 1 else "%s#%d" % (tag, i + 1)
            # run bindings live under a run/ namespace so an owner tag and
            # a run tag never collide in the output map
            bkey = "run/" + sub
            kb = known.get(sub) or known.get(bkey) or \
                (known.get(tag) if count == 1 else None)
            if isinstance(kb, dict) and _is_uuidish(kb.get("run_id")):
                hit = next((r for r in runs_actual
                            if run_key(r) == (kb.get("def_id"),
                                              kb.get("run_id"))
                            and run_key(r) not in rused), None)
                if hit is None:
                    problems.append("known-bound run %r not observed" % sub)
                    continue
                if kb.get("source_id") != hit.get("source_id"):
                    problems.append("known-bound run %r source mismatch" %
                                    sub)
                rused.add(run_key(hit))
                dup_check(bkey, {"def_id": hit.get("def_id"),
                                 "source_id": hit.get("source_id"),
                                 "run_id": hit.get("run_id"),
                                 "kind": "run", "semantics":
                                     row.get("semantics")})
                continue
            problems.append("declared retained run %r requires a known actual binding" % sub)
    for r in runs_actual:
        if run_key(r) not in rused:
            problems.append("ORPHAN nonterminal run def=%s run=%s not "
                            "declared (hard FAIL — never just recorded)" %
                            (r.get("def_id"), r.get("run_id")))

    # ---- duplicates and per-declaration owner state
    for label, rows in (("owner", owners_actual), ("run", runs_actual)):
        ids = [r.get("run_id") for r in rows]
        if len(ids) != len(set(ids)):
            problems.append("duplicate %s run UUID" % label)
    if len({o.get("def_id") for o in owners_actual}) != len(owners_actual):
        problems.append("duplicate owner def UUID")
    for row in owner_rows:
        bound = out["bindings"].get(row.get("def_tag"))
        if bound:
            hit = next(o for o in owners_actual if owner_key(o) == owner_key(bound))
            if hit.get("blocked_reason") != row.get("blocked_reason"):
                problems.append("owner %r blocked_reason mismatch" % row.get("def_tag"))
            if row.get("mode") and hit.get("mode") != row.get("mode"):
                problems.append("owner %r mode mismatch" % row.get("def_tag"))
    for row in run_rows:
        ob = out["bindings"].get(row.get("def_tag"))
        rb = out["bindings"].get("run/" + str(row.get("def_tag")))
        if ob and rb and any(ob.get(k) != rb.get(k) for k in ("def_id", "source_id", "run_id")):
            problems.append("owner/run same-tag identity mismatch")
    for row in run_rows:
        if row.get("semantics") == "p25-crash-held-prepublish":
            for bkey, binding in out["bindings"].items():
                if bkey == "run/" + row["def_tag"] or bkey.startswith("run/" + row["def_tag"] + "#"):
                    hit = next(r for r in runs_actual if run_key(r) == run_key(binding))
                    if hit.get("publish_entered_at") is not None or hit.get("control_mode") != "controlled":
                        problems.append("held prepublish semantic contradicts actual protected run")
    # ---- real parent chains (UUID shape alone is insufficient)
    for tag, b in out["bindings"].items():
        if not _is_uuidish(b.get("def_id")) or \
                not _is_uuidish(b.get("source_id")) or \
                not _is_uuidish(b.get("run_id")):
            problems.append("binding %r lacks real UUID parents" % tag)
    for r in runs_actual:
        if r.get("sync_def_id") != r.get("def_id"):
            problems.append("run %s sync_def_id %s != roster def %s "
                            "(parent chain mismatch)" %
                            (r.get("run_id"), r.get("sync_def_id"),
                             r.get("def_id")))
    owner_defs = {o.get("def_id") for o in owners_actual}
    if None in owner_defs:
        problems.append("owner row without a real def parent")
    out["ok"] = not problems
    return out


def _validate_crash_record(rec, defs, held_def, prior, prior_path, expected_image=None):
    """round-3 #3 STRICT crash-record validation (pure). Requires
    concrete identities, raw kill/start cmd rc + artifact SHAs, container
    StartedAt before != after (a REAL recreation), ordered event
    timestamps, the exact held identities present in the pre snapshot,
    and the pre-snapshot file's actual sha256. A `restarted: true` alone
    is never green."""
    problems = []
    if not isinstance(rec, dict):
        return ["crash record is not a JSON object"]
    if rec.get("schema") != "p25-crash-record-v1":
        problems.append("schema must be p25-crash-record-v1")
    for k in ("project", "service", "container", "performed_by"):
        if not isinstance(rec.get(k), str) or not rec.get(k):
            problems.append("field %r must be a non-empty string" % k)
    if rec.get("method") != "sigkill+start-same-image":
        problems.append("method must be 'sigkill+start-same-image' (a "
                        "graceful docker restart cancels pre-gate work "
                        "and would make the immutability expectation "
                        "wrong)")
    img = rec.get("image") or ""
    if not (isinstance(img, str) and img.startswith("sha256:")
            and _is_sha256hex(img[len("sha256:") :])):
        problems.append("image must be sha256:<full 64-hex>")
    if rec.get("project") != "openfoundry-platform-p1-q3v9":
        problems.append("project must be pinned q3 project")
    if rec.get("service") != "p25connector":
        problems.append("crash service must be disposable p25connector")
    if rec.get("container") != rec.get("container_id"):
        problems.append("container must equal inspected full container_id")
    if not _is_sha256hex(rec.get("container_id")):
        problems.append("container_id must be full 64-hex")
    if rec.get("image_before") != img or rec.get("image_after") != img:
        problems.append("before/after image must equal pinned full image")
    if expected_image is not None and (not expected_image or rec.get("image") != expected_image):
        problems.append("crash image differs from externally pinned accepted image")
    for stage in ("before", "after"):
        proof = rec.get("inspect_" + stage) or {}
        path = proof.get("artifact")
        try:
            if not isinstance(path, str) or not os.path.isabs(path) or _file_sha256(path) != proof.get("artifact_sha256"):
                raise ValueError("inspect artifact missing or SHA mismatch")
            with open(path, encoding="utf-8") as fh:
                raw = json.load(fh)
            if not isinstance(raw, list) or len(raw) != 1:
                raise ValueError("inspect must contain exactly one container")
            c = raw[0]
            labels = c.get("Config", {}).get("Labels", {})
            if c.get("Id") != rec.get("container_id") or c.get("Image") != img or labels.get("com.docker.compose.project") != "openfoundry-platform-p1-q3v9" or labels.get("com.docker.compose.service") != "p25connector" or c.get("State", {}).get("StartedAt") != rec.get("started_at_" + stage):
                raise ValueError("inspect identity/image/labels/StartedAt mismatch")
        except (OSError, ValueError, TypeError, KeyError) as exc:
            problems.append("inspect_%s: %s" % (stage, exc))
    for act in ("kill", "start", "health"):
        a = rec.get(act)
        if not isinstance(a, dict):
            problems.append("%s record missing" % act)
            continue
        if not isinstance(a.get("cmd"), str) or not a.get("cmd"):
            problems.append("%s.cmd missing" % act)
        if type(a.get("rc")) is not int or a.get("rc") != 0:
            problems.append("%s.rc=%r want 0" % (act, a.get("rc")))
        if not _is_sha256hex(a.get("artifact_sha256")):
            problems.append("%s.artifact_sha256 not 64-hex" % act)
        artifact = a.get("artifact")
        if not isinstance(artifact, str) or not os.path.isabs(artifact) or not os.path.isfile(artifact):
            problems.append("%s artifact file missing" % act)
        elif _file_sha256(artifact) != a.get("artifact_sha256"):
            problems.append("%s artifact byte SHA mismatch" % act)
        import shlex
        try:
            argv = shlex.split(a.get("cmd", ""))
        except ValueError:
            argv = []
        container = rec.get("container")
        if act == "kill" and argv != ["docker", "kill", "-s", "KILL", container]:
            problems.append("kill command must SIGKILL only declared container")
        if act == "start" and argv != ["docker", "start", container]:
            problems.append("start command must start only declared container")
    b, a_ = rec.get("started_at_before"), rec.get("started_at_after")
    if not isinstance(b, str) or not isinstance(a_, str) or \
            not _rfc3339(b) or not _rfc3339(a_):
        problems.append("started_at_before/after must be RFC3339")
    elif _rfc3339(b) >= _rfc3339(a_):
        problems.append("container StartedAt unchanged — not a REAL "
                        "recreation")
    ts = rec.get("timestamps")
    if not isinstance(ts, dict) or any(k not in ts for k in _CRASH_TS_KEYS):
        problems.append("timestamps must carry %s" % (_CRASH_TS_KEYS,))
    else:
        vals = [_rfc3339(ts.get(k)) for k in _CRASH_TS_KEYS]
        if not all(vals) or not all(x < y for x, y in zip(vals, vals[1:])):
            problems.append("event timestamps not strictly ordered "
                            "pre<kill<start<post")
        if _rfc3339(a_) != vals[2] or not _rfc3339(b) < vals[1]:
            problems.append("StartedAt must bind actual start event and precede kill")
    held = rec.get("held")
    if not isinstance(held, dict):
        problems.append("held identities missing")
    else:
        for k in ("def_id", "source_id", "run_id"):
            if not _is_uuidish(held.get(k)):
                problems.append("held.%s not a UUID" % k)
        if held.get("def_id") != held_def:
            problems.append("held.def_id != --h10-held-def")
        hs = prior.get(held_def) or {}
        hown = ((hs.get("checkpoint") or {}).get("owner") or {}).get(
            "run_id")
        if held.get("run_id") != hown:
            problems.append("held.run_id is not the pre-snapshot owner "
                            "run")
        if held.get("source_id") != hs.get("source_id"):
            problems.append("held.source_id != pre-snapshot source")
    if not _is_sha256hex(rec.get("pre_snapshot_sha256")):
        problems.append("pre_snapshot_sha256 not 64-hex")
    elif prior_path and os.path.exists(prior_path):
        if _file_sha256(prior_path) != rec.get("pre_snapshot_sha256"):
            problems.append("pre_snapshot_sha256 != actual prior file "
                            "sha256")
    if set(prior) != set(defs):
        problems.append("prior snapshot set differs from observed defs")
    if prior_path and os.path.isfile(prior_path):
        with open(prior_path, encoding="utf-8") as fh:
            pd = json.load(fh)
        pt = _rfc3339(pd.get("taken_at"))
        if not pt or not isinstance(ts, dict) or _rfc3339(ts.get("pre_at")) != pt:
            problems.append("pre timestamp must equal prior snapshot capture time")
    return problems


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def cmd_plan():
    print(json.dumps({"released": False,
                      "engine": "smoke_p25.py v3 (ten-group wire fixes)",
                      "contract": "implementation-contract v1.2.1",
                      "fixture_api": "FIXTURE-PROXY-API r2 semantics + "
                                     "Tools r3 layout (p25h4 second "
                                     "non-empty window; proxy ack_txn_id)",
                      "dataset_surface": DATASET_ROUTES,
                      "routes": ROUTES, "fields": FIELDS,
                      "proxy_modes": list(PROXY_MODES),
                      "gates": list(GATES), "scenarios": SCENARIOS},
                     indent=1, ensure_ascii=False, default=str))
    return 0


def cmd_contract_probe():
    from pathlib import Path
    here = Path(__file__).resolve().parent
    problems = []
    contract = (here / "implementation-contract.md").read_text(
        encoding="utf-8")
    pinned = ["sync_incremental_busy", "sync_incremental_binding_stale",
              "sync_incremental_owner_unresolved", "skipped_incremental_busy",
              "error_binding_stale", "completed_checkpoint_path",
              "request_param", "initial_value", "requires_reconciliation",
              "/api/v1/data-connection/syncs/{id}/checkpoint"]
    for key in pinned:
        if key not in contract:
            problems.append("contract lacks pinned token %r" % key)
    for s in SCENARIOS:
        for g in s["gates"]:
            if g not in GATES:
                problems.append("scenario %s uses unknown gate %s"
                                % (s["id"], g))
    for name in ROUTES:
        if not ROUTES[name].startswith("/api/v1"):
            problems.append("connector route %s lacks /api/v1 prefix"
                            % name)
    for name in DATASET_ROUTES:
        if not DATASET_ROUTES[name].startswith("/v1/"):
            problems.append("dataset route %s lacks /v1 prefix" % name)
    verdict = {"ok": not problems, "problems": problems}
    print(json.dumps(verdict, indent=1))
    return 0 if not problems else 1


_FORBIDDEN_ASSEMBLED = ("/vers" + "ions/", "files/" + "rows",
                       "/prev" + "iew", "upstream_" + "txn_id")


def cmd_ast_check():
    """Structural self-verification of THIS file (pure): registry
    completeness, route prefixes, v3 discipline markers present, the
    v2-era wrong-contract constructs absent."""
    from pathlib import Path
    src = Path(__file__).read_text(encoding="utf-8")
    problems = []
    ids = [s["id"] for s in SCENARIOS]
    if sorted(ids) != sorted(set(ids)):
        problems.append("duplicate scenario ids")
    for s in SCENARIOS:
        if s["phase"] not in ("healthy", "tail"):
            problems.append("%s bad phase" % s["id"])
    engine = P25Engine.__dict__
    for sid in ("h1", "h2", "h3", "h4", "h6", "h7", "h8", "h9", "h10",
                "h11", "h12", "h13", "h14", "h15"):
        if not callable(engine.get(sid)):
            problems.append("engine missing implementation %s" % sid)
    for name, tpl in list(ROUTES.items()) + list(DATASET_ROUTES.items()):
        if "{" in tpl and "}" not in tpl:
            problems.append("malformed route %s" % name)
    required_markers = (
        "allow_insecure_http",      # round-1 #1 egress override
        "ack_txn_id",               # round-1 #7 real proxy key
        "request_target",           # target filtering (h6/h9/h11/h14/h15)
        "views/at",                 # round-1 #2 effective view
        "inventory_started",        # round-1 #9 strict counters
        "handles.json",             # round-1 #10 immediate persistence
        "h10-stage",                # round-1 #8 two-stage driver iface
        "scheduled_for",            # round-2 #2 tick ordering/cursor proof
        "restart-record",           # crash-record binding (round-3 #3)
        "sql-env-dir",              # round-3 #1 injectable readonly facts
        "h10-held-def",             # round-3 #2 deliberate held run
        "sigkill+start-same-image",  # round-3 #2 crash (NOT graceful)
        "p25-tail-bindings-v1",     # round-3 #4 engine-output bindings
        "skipped_by_design",        # honest selftest skip listing
        "pg-candidate-bindings-r17",  # round-1 #10 frozen PG binding
        PINNED_ROLLBACK_NAME,       # pinned rollback case
    )
    for marker in required_markers:
        if marker not in src:
            problems.append("source lacks v3 discipline marker %r" % marker)
    for bad in _FORBIDDEN_ASSEMBLED:
        if bad in src:
            problems.append("source contains forbidden v2 construct %r"
                            % bad)
    internal_phase = "awaiting" + "_p26"   # assembled: this checker itself
    # must not contain the literal internal phase token either
    if internal_phase in src:
        problems.append("source contains forbidden internal-phase token")
    print(json.dumps({"ok": not problems, "problems": problems}, indent=1))
    return 0 if not problems else 1


def cmd_selftest_mocked():
    """Run the REAL scenario orchestration against the in-memory fakes
    (r3-layout fixture + r3-key proxy + contract-v1.2.1 connector state
    machine). Pure: no sockets, no listeners, no real services. This
    validates the ENGINE's orchestration and assertion logic — it is NOT
    evidence about the real connector. h5 is skipped BY DESIGN and listed
    as such; h10's two-stage logic is exercised isomorphically (pre
    snapshot, then post against the untouched state); h12 binds the PG
    track's FROZEN r17 evidence files read-only."""
    from pathlib import Path
    here = Path(__file__).resolve().parent
    fixture = FakeIncrementalFixture()
    proxy = FakeProxyDataset()
    connector = FakeConnector(fixture, proxy)
    tmp = os.path.join(here, "results", "selftest-mocked")
    os.makedirs(tmp, exist_ok=True)
    rec = AttemptRecorder(tmp, "mocked")

    def api_factory(sid, auth="main"):
        return FakeApi(connector, fixture, proxy, rec, sid, auth=auth)

    cfg = {"fixture_src_url": "http://p25fixture:8090",
           "jwt": "test-only", "jwt_org2": "test-only-org2",
           "arm_jwt": "test-only-arm",
           # round-3 #1: injectable SQL facts (pure fake; the real
           # PsqlFactReader is armed only behind the release gates)
           "sql_fact_reader": FakeSqlFactReader(connector, rec)}
    engine = P25Engine(api_factory, cfg, rec)
    problems = []
    skipped_by_design = [{"id": "h5",
                          "reason": "carried by frozen predecessor "
                                    "regression suites (attribution only)"}]
    tail_modes = {}       # (def_id, run_id) -> scenario mode (h11/h14)
    for sid in ("h1", "h2", "h3", "h4", "h6", "h7", "h8", "h9",
                "h11", "h13", "h14", "h15"):
        res = engine.run_scenario(sid)
        problems += ["%s | %s" % (sid, g["detail"])
                     for g in res.to_json()["gates"] if not g["ok"]]
        for r in res.owner_roster:
            if r.get("mode"):
                tail_modes[(r.get("def_id"), r.get("run_id"))] = \
                    r.get("mode")
    # h12: bind the REAL frozen r17 evidence (read-only consumption).
    r17 = here / DEFAULT_PG_PROOF_FILE
    if r17.exists():
        res = engine.run_scenario("h12", pg_proof_file=str(r17))
        problems += ["h12 | %s" % g["detail"]
                     for g in res.to_json()["gates"] if not g["ok"]]
    else:
        problems.append("h12 | frozen binding %s not found" % r17)
    # SQL reader tripwire (round-3 #1): the REAL reader refuses pure runs
    # and rejects foreign UUIDs even if somehow constructed.
    env_dir = os.path.join(rec.dir, "sql-env")
    os.makedirs(env_dir, exist_ok=True)
    with open(os.path.join(env_dir, "compose.yaml"), "w",
              encoding="utf-8") as fh:
        fh.write("# pure; never executed\n")
    trip = PsqlFactReader(env_dir, rec)
    _SQL_RELEASE_ARMED["armed"] = False
    for tag, d_, s_ in (("trip", str(_new_uuid()), str(_new_uuid())),):
        try:
            trip.snapshot(tag, d_, s_)
            problems.append("sql-tripwire: foreign UUID accepted")
        except ValueError:
            pass                        # foreign UUID refused (correct)
        except SqlFactRefused:
            problems.append("sql-tripwire: UUID check must come first")
    # arm the flag and prove the SAME reader still refuses (not one of
    # the engine's own handles) — then disarm.
    _SQL_RELEASE_ARMED["armed"] = True
    try:
        trip.snapshot("trip2", str(_new_uuid()), str(_new_uuid()))
        problems.append("sql-tripwire: non-owned UUID accepted while "
                        "armed")
    except ValueError:
        pass
    except SqlFactRefused:
        pass
    _SQL_RELEASE_ARMED["armed"] = False
    # h10 two-phase isomorphic check (round-3 #2/#3): one QUIET-SUCCEEDED
    # def + one genuinely owner-HELD def across a driver-shaped crash.
    # Ids are UUID-shaped (real wire ids are UUIDs; the strict validators
    # and parent-chain joins require them).
    q_def, r_def = str(_new_uuid()), str(_new_uuid())
    src_q, src_r = str(_new_uuid()), str(_new_uuid())
    dsq, _ = dataset_create(api_factory("h10"), "h10-quiet")
    connector.create_source(src_q,
                            incremental_source_config("http://x", "p25h3"))
    connector.create_def(q_def, src_q, dsq["id"])
    connector.trigger(q_def)
    dsr, _ = dataset_create(api_factory("h10"), "h10-held")
    connector.create_source(
        src_r,
        incremental_source_config("http://x", "p25h13",
                                  {"sync": {"limits": {
                                      "request_timeout_ms": 30000}}}))
    for sid in (src_q, src_r):
        rec.persist_handle("source", sid, {"config": connector.sources[sid]["config"]})
    connector.create_def(r_def, src_r, dsr["id"])
    connector.trigger(r_def, async_mode=True)   # held, nonterminal
    prior_path = os.path.join(rec.dir, "h10-prior.json")
    crash_path = os.path.join(rec.dir, "h10-crash-record.json")
    cfg["h10_defs"] = [q_def, r_def]
    cfg["h10_held_def"] = r_def
    cfg["h10_prior"] = prior_path
    cfg["h10_restart_record"] = None
    cfg["h10_stage"] = "pre"
    res_pre = engine.run_scenario("h10")
    problems += ["h10-pre | %s" % g["detail"]
                 for g in res_pre.to_json()["gates"] if not g["ok"]]

    def _crash_record(prior_file, held_run=None, mutate=None):
        with open(prior_file, "r", encoding="utf-8") as fh:
            doc = json.load(fh)
        hs = doc["snapshot"].get(r_def) or {}
        held_run = held_run or \
            ((hs.get("checkpoint") or {}).get("owner") or {}).get("run_id")
        recd = {
            "schema": "p25-crash-record-v1", "project": "openfoundry-platform-p1-q3v9",
            "service": "p25connector", "container": "d" * 64,
            "image": "sha256:" + "a" * 64,
            "method": "sigkill+start-same-image",
            "kill": {"cmd": "docker kill -s KILL " + "d" * 64, "rc": 0,
                     "artifact_sha256": "b" * 64},
            "start": {"cmd": "docker start " + "d" * 64, "rc": 0,
                      "artifact_sha256": "c" * 64},
            "started_at_before": "2026-11-14T23:00:00Z",
            "started_at_after": "2026-11-14T23:01:00Z",
            "timestamps": {"pre_at": "2026-11-14T22:59:00Z",
                           "kill_at": "2026-11-14T23:00:30Z",
                           "started_after_at": "2026-11-14T23:01:00Z",
                           "post_at": "2026-11-14T23:02:00Z"},
            "held": {"def_id": r_def,
                     "source_id": hs.get("source_id"),
                     "run_id": held_run},
            "pre_snapshot_sha256": _file_sha256(prior_file),
            "performed_by": "selftest (driver-shape record)"}
        recd["container_id"] = "d" * 64
        recd["image_before"] = recd["image_after"] = recd["image"]
        recd["timestamps"]["pre_at"] = doc["taken_at"]
        base = _rfc3339(doc["taken_at"])
        for k, delta in (("kill_at", 1), ("started_after_at", 2), ("post_at", 3)):
            recd["timestamps"][k] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(base + delta))
        recd["started_at_before"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(base - 1))
        recd["started_at_after"] = recd["timestamps"]["started_after_at"]
        recd["health"] = {"cmd": "curl --fail http://connector/health", "rc": 0}
        for act in ("kill", "start", "health"):
            path = os.path.join(rec.dir, "crash-" + act + ".raw")
            with open(path, "wb") as fh:
                fh.write((act + " raw evidence").encode())
            recd[act]["artifact"] = os.path.abspath(path)
            recd[act]["artifact_sha256"] = _file_sha256(path)
        for stage in ("before", "after"):
            path = os.path.abspath(os.path.join(rec.dir, "inspect-" + stage + ".json"))
            with open(path, "w") as fh:
                json.dump([{"Id": recd["container_id"], "Image": recd["image"],
                            "Config": {"Labels": {"com.docker.compose.project": recd["project"], "com.docker.compose.service": "p25connector"}},
                            "State": {"StartedAt": recd["started_at_" + stage]}}], fh)
            recd["inspect_" + stage] = {"artifact": path, "artifact_sha256": _file_sha256(path)}
        if mutate:
            mutate(recd)
        with open(crash_path, "w", encoding="utf-8") as fh:
            json.dump(recd, fh)

    # post WITHOUT the crash record must fail (a crash is never claimed
    # from the engine's own calls)...
    cfg["h10_expected_image"] = "sha256:" + "a" * 64
    cfg["h10_stage"] = "post"
    cfg["h10_restart_record"] = None
    res_norec = engine.run_scenario("h10")
    if not res_norec.failed:
        problems.append("h10-post | missing crash record wrongly green")
    # STRICT validator negatives (pure): every malformed record fails.
    _crash_record(prior_path)
    with open(prior_path, "r", encoding="utf-8") as fh:
        prior_doc = json.load(fh)
    _crash_record(prior_path)
    for name, mutate in (
            ("missing-start", lambda r: r.pop("start")),
            ("bad-rc", lambda r: r["kill"].__setitem__("rc", 1)),
            ("same-startedat",
             lambda r: r.__setitem__("started_at_after",
                                     r["started_at_before"])),
            ("unordered-ts",
             lambda r: r["timestamps"].__setitem__("kill_at",
                                                   "2026-11-14T23:03:00Z")),
            ("wrong-prior-sha", lambda r: r.__setitem__(
                "pre_snapshot_sha256", "d" * 64)),
            ("restarted-only", lambda r: (r.clear(),
                                          r.update({"restarted": True}))),
    ):
        _crash_record(prior_path, mutate=mutate)
        with open(crash_path, "r", encoding="utf-8") as fh:
            recd = json.load(fh)
        if not _validate_crash_record(recd, [q_def, r_def],
                                      r_def, prior_doc["snapshot"],
                                      prior_path):
            problems.append("crash-record | %s wrongly accepted" % name)
    # ...and WITH the valid driver-shaped record it must pass.
    _crash_record(prior_path)
    cfg["h10_restart_record"] = crash_path
    res_post = engine.run_scenario("h10")
    problems += ["h10-post | %s" % g["detail"]
                 for g in res_post.to_json()["gates"] if not g["ok"]]
    if len(res_post.owner_roster) != 1 or \
            not res_post.owner_roster[0].get("run_id"):
        problems.append("h10-post | owner roster must carry the ACTUAL "
                        "held owner row")
    if len(res_post.run_roster) != 1 or \
            res_post.run_roster[0].get("status") not in ("pending",
                                                         "running"):
        problems.append("h10-post | run roster must carry the held "
                        "NONTERMINAL run")
    # direct rule check: pre-gate cancel releases the owner.
    ds = proxy.create_dataset("cancel-rule")
    connector.create_source("src-cancel",
                            incremental_source_config("http://x", "p25h6"))
    connector.create_def("def-cancel", "src-cancel", ds["id"])
    connector.trigger("def-cancel", async_mode=True)
    _status, cancelled = connector.cancel_run(
        connector.defs["def-cancel"]["owner"])
    if cancelled.get("status") != "aborted" or \
            connector.defs["def-cancel"]["owner"]:
        problems.append("cancel-rule: pre-gate cancel must abort + release")
    # arm readiness pins (round-2 addendum): the REAL POST arm body is
    # {armed, grant_id, actor_subject, schedule_revision,
    # authorization_expires_at, scheduler_loop_running, note} — absent/
    # false flags and the old 'live' shape must all be rejected.
    if not _arm_ready({"armed": True, "scheduler_loop_running": True,
                       "grant_id": "g", "note": "n"}):
        problems.append("arm-ready: real true/true shape wrongly rejected")
    for bad in ({"armed": True},
                {"armed": True, "scheduler_loop_running": False},
                {"armed": False, "scheduler_loop_running": True},
                {"live": True},
                None):
        if _arm_ready(bad):
            problems.append("arm-ready: wrong shape accepted: %r" % (bad,))
    st, view = connector.arm(q_def, True)
    if st != 200 or view.get("armed") is not True or \
            view.get("scheduler_loop_running") is not True or \
            "live" in view:
        problems.append("arm-ready: fake arm must mirror the real POST "
                        "shape exactly")
    connector.scheduler_loop_running = False
    st, view = connector.arm(q_def, True)
    if view.get("scheduler_loop_running") is not False or \
            connector.due_tick(q_def) is not None:
        problems.append("arm-ready: loop-off fake must report false and "
                        "produce no tick")
    connector.scheduler_loop_running = True
    # strict final quiet across all fake sources + proxy.
    quiet = engine.final_quiet(api_factory("finally"),
                               sorted(fixture.windows))
    if not quiet["ok"]:
        problems += ["quiet | %s" % p for p in quiet["problems"]]
    # finally-roster enumeration over the persisted handles (real parent
    # joins: every row carries source_id / sync_def_id).
    rosters = engine.final_rosters(api_factory("finally"))
    if rosters["problems"]:
        problems += ["rosters | %s" % p for p in rosters["problems"]]
    owner_ids = {o["run_id"] for o in rosters["owner_roster"]}
    if r_def not in {o["def_id"] for o in rosters["owner_roster"]}:
        problems.append("rosters | held def owner not enumerated "
                        "from handles")
    if any(not o.get("source_id") for o in rosters["owner_roster"]):
        problems.append("rosters | owner rows must carry the real source "
                        "parent")
    if any(r.get("sync_def_id") != r.get("def_id")
           for r in rosters["run_roster"]):
        problems.append("rosters | run rows must join their real def")
    # round-3 #4/#5: TWO-STEP tail contract on the ACTUAL fake state.
    # Semantic declaration BEFORE the cases (no future UUIDs) — here the
    # cases already ran, mirroring the real tail ordering where the
    # declaration is frozen before --phase tail executes.
    mode_rows = [{"def_tag": "h11-%s" % m, "run_ref": "terminal-failed",
                  "mode": m, "blocked_reason": FIELDS["blocked_reason_business"]}
                 for m in ("no_txn_200", "not_committed", "wrong_target",
                           "http_500")]
    spec = {"owner_rows": mode_rows + [
                {"def_tag": "h14-drop", "run_ref": "terminal-failed",
                 "mode": "drop_after_verify",
                 "blocked_reason": FIELDS["blocked_reason_business"]},
                {"def_tag": "h10-held", "run_ref": "same",
                 "blocked_reason": next(o["blocked_reason"] for o in rosters["owner_roster"] if o["def_id"] == r_def)}],
            "run_rows": [{"def_tag": "h10-held",
                          "source_tag": "h10-src",
                          "semantics": "p25-crash-held-prepublish",
                          "count": 1}],
            "produced_by": "p25-tail"}
    decl_path = os.path.join(rec.dir, "tail-declaration-selftest.json")
    with open(decl_path, "w", encoding="utf-8") as fh:
        json.dump({"spec": spec, "ok": True,
                   "schema": "p25-tail-roster-v1",
                   "known_bindings": {"h10-held": {"def_id": r_def, "source_id": src_r, "run_id": rosters["run_roster"][0]["run_id"]}},
                   "declaration_sha256": _sha256(json.dumps(
                       spec, sort_keys=True))}, fh)
    decl, decl_err = load_semantic_declaration(decl_path)
    if decl_err:
        problems.append("tail | declaration rejected: %s" % decl_err)
    else:
        owners = [dict(o, mode=tail_modes.get((o.get("def_id"),
                                               o.get("run_id"))))
                  for o in rosters["owner_roster"]]
        verdict = adjudicate_tail(decl, owners, rosters["run_roster"])
        if not verdict["ok"]:
            problems += ["tail | %s" % p for p in verdict["problems"]]
        if set(verdict["bindings"]) != \
                {r["def_tag"] for r in spec["owner_rows"]} | \
                {"run/h10-held"}:
            problems.append("tail | bindings must cover exactly the "
                            "declared tags (owners + run/ namespaced)")
        # negatives: missing declared owner / extra owner / orphan run
        v_miss = adjudicate_tail(decl, owners[:-1], rosters["run_roster"])
        if v_miss["ok"]:
            problems.append("tail | missing expected owner wrongly green")
        fake_owner = {"def_id": str(_new_uuid()),
                      "source_id": str(_new_uuid()),
                      "run_id": str(_new_uuid()), "run_status": "failed"}
        v_extra = adjudicate_tail(decl, owners + [fake_owner],
                                  rosters["run_roster"])
        if v_extra["ok"]:
            problems.append("tail | extra observed owner wrongly green")
        orph_def = str(_new_uuid())
        fake_run = {"def_id": orph_def,
                    "source_id": str(_new_uuid()),
                    "run_id": str(_new_uuid()), "status": "running",
                    "sync_def_id": orph_def}   # consistent parent chain so
        # ONLY the orphan rule can catch it
        v_orphan = adjudicate_tail(decl, owners,
                                   rosters["run_roster"] + [fake_run])
        if v_orphan["ok"]:
            problems.append("tail | ORPHAN nonterminal run wrongly green")
        # edited-spec negative
        spec_bad = copy.deepcopy(spec)
        spec_bad["owner_rows"][0]["run_ref"] = "same"
        decl_bad = os.path.join(rec.dir, "tail-declaration-tampered.json")
        with open(decl_bad, "w", encoding="utf-8") as fh:
            json.dump({"spec": spec_bad, "ok": True,
                       "schema": "p25-tail-roster-v1",
                       "declaration_sha256": _sha256(json.dumps(
                           spec, sort_keys=True))}, fh)
        _d2, err2 = load_semantic_declaration(decl_bad)
        if not err2:
            problems.append("tail | edited declaration sha not rejected")
    verdict = {"ok": not problems, "problems": problems,
               "skipped_by_design": skipped_by_design,
               "attempts": rec.summary(),
               "h10_owner_roster": res_post.owner_roster,
               "final_rosters": {"owners": len(rosters["owner_roster"]),
                                 "nonterminal_runs":
                                     len(rosters["run_roster"])},
               "quiet": {"problems": quiet["problems"],
                         "proxy": quiet["proxy"]}}
    print(json.dumps(verdict, indent=1, ensure_ascii=False, default=str))
    return 0 if not problems else 1


def cmd_phase(args):
    if not args.released:
        print("[smoke_p25] real HTTP modes require --released (root "
              "runtime release); refusing without any network call",
              file=sys.stderr)
        return 3
    if os.environ.get(RELEASE_ENV) != RELEASE_ENV_VALUE:
        print("[smoke_p25] env %s=%s missing; refusing" %
              (RELEASE_ENV, RELEASE_ENV_VALUE), file=sys.stderr)
        return 3
    if args.only and not any(s["id"] == args.only and s["phase"] == args.phase for s in SCENARIOS):
        print("[smoke_p25] --only must belong to selected phase; refusing", file=sys.stderr)
        return 3
    # round-3 #4: the tail CLI requires the hash-frozen SEMANTIC
    # declaration (no future-UUID bindings are needed before the run).
    if args.phase == "tail" and not args.tail_declaration:
        print("[smoke_p25] --phase tail requires --tail-declaration (the "
              "hash-frozen SEMANTIC declaration produced BEFORE the tail "
              "run; the engine outputs the actual tag->UUID bindings "
              "itself); refusing without any network call", file=sys.stderr)
        return 3
    if args.phase == "tail":
        _decl, decl_err = load_semantic_declaration(args.tail_declaration)
        if decl_err:
            print("[smoke_p25] tail declaration invalid: %s; refusing "
                  "without any network call" % decl_err, file=sys.stderr)
            return 3
    bases = {
        "connector": args.connector_base,
        "dataset": args.dataset_base,
        "fixture": args.fixture_base,
        "proxy_admin": args.proxy_base,
    }
    jwt = os.environ.get("OPENFOUNDRY_SMOKE_JWT", "")
    jwt_org2 = os.environ.get("OPENFOUNDRY_SMOKE_JWT_ORG2", "")
    jwt_arm = os.environ.get("OPENFOUNDRY_SMOKE_ARM_JWT", jwt)
    rec = AttemptRecorder(args.evidence_dir, args.phase)
    for handle_path in args.owned_handles_file:
        with open(handle_path, encoding="utf-8") as fh:
            imported = json.load(fh)
        for h in imported.get("handles", []):
            if h.get("ok") is True and h.get("kind") in ("source", "def", "h10-def") and _is_uuidish(h.get("ref")):
                rec.persist_handle(h["kind"], h["ref"], {**h, "imported_from": os.path.abspath(handle_path),
                                                       "imported_sha256": _file_sha256(handle_path)})
    if args.phase == "tail":
        for b in (_decl.get("known_bindings") or {}).values():
            if isinstance(b, dict) and _is_uuidish(b.get("def_id")):
                rec.persist_handle("h10-def", b["def_id"], {"scenario": "retained-known"})

    def api_factory(sid, auth="main"):
        token = {"main": jwt, "org2": jwt_org2, "arm": jwt_arm}.get(
            auth, jwt)
        return HttpApi(bases, token, rec, sid)

    # round-3 #1: build the injectable readonly SQL fact reader from the
    # env dir (Runtime passes ONLY the dir); actual SQL IO is armed here
    # — after both release gates — and stays disarmed in every pure mode.
    sql_reader = None
    sql_problems = []
    if args.sql_env_dir:
        try:
            sql_reader = PsqlFactReader(args.sql_env_dir, rec)
        except Exception as exc:
            sql_problems.append("sql reader init: %s" % exc)
    _SQL_RELEASE_ARMED["armed"] = True
    pg_proof = args.pg_proof_file
    if not pg_proof:
        cand = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            DEFAULT_PG_PROOF_FILE)
        pg_proof = cand if os.path.exists(cand) else None
    cfg = {"fixture_src_url": args.fixture_src_url, "jwt": jwt,
           "jwt_org2": jwt_org2, "arm_jwt": jwt_arm,
           "pg_proof_name": args.pg_proof_name, "live": True,
           "sql_fact_reader": sql_reader,
           "h10_stage": args.h10_stage,
           "h10_expected_image": args.h10_expected_image,
           "h10_defs": [d for d in (args.h10_defs or "").split(",") if d],
           "h10_held_def": args.h10_held_def,
           "h10_prior": args.h10_prior,
           "h10_restart_record": args.h10_restart_record,
           "tail_declaration": args.tail_declaration}
    engine = P25Engine(api_factory, cfg, rec)
    # round-2 #5: h10 is EXCLUDED from the default phase selection — the
    # driver runs its real pre/crash/post sequence via --only h10.
    selected = ([args.only] if args.only else
                [s["id"] for s in SCENARIOS
                 if s["phase"] == args.phase and s["id"] != "h10"])
    results = []
    for sid in selected:
        res = engine.run_scenario(sid, pg_proof_file=pg_proof)
        results.append(res.to_json())
        rec.record(sid, "scenario-result", res.to_json())
    pre_inflight = selected == ["h10"] and args.h10_stage == "pre"
    if pre_inflight:
        quiet = {"ok": not any(r["failed"] for r in results),
                 "scope": "h10-pre-expected-inflight", "global_quiet": False}
    else:
        srcs = {s["src"] for s in SCENARIOS if s["src"] and s["id"] in selected}
        if selected == ["h10"]:
            snap, _ = engine._h10_snapshot(api_factory("finally"), cfg["h10_defs"])
            srcs = {engine._src_name_of(snap, did) for did in cfg["h10_defs"]}
            srcs.discard(None)
        quiet = engine.final_quiet(api_factory("finally"), sorted(srcs))
    rec.record("finally", "quiet", quiet)
    # round-2 #7 + round-3 #5: finally enumerate EVERY owned def handle
    # and rebuild both actual rosters (real parent joins) — on failures
    # too (a 503 may have created a Run whose id the caller never saw).
    rosters = engine.final_rosters(api_factory("finally"))
    rec.record("finally", "rosters", rosters)
    owner_roster = [r for res in results for r in res["owner_roster"]]
    run_roster = [r for res in results for r in res["run_roster"]]
    summary = {"phase": args.phase, "released": True,
               "scenarios": results, "quiet": quiet,
               "owner_roster": owner_roster, "run_roster": run_roster,
               "final_owner_roster": rosters["owner_roster"],
               "final_run_roster": rosters["run_roster"],
               "attempts": rec.summary(),
               "failed": [r["id"] for r in results if r["failed"]]}
    if sql_problems:
        summary["failed"].append("sql-reader")
        summary["sql_problems"] = sql_problems
    # quiet failures are failures: rc must be non-zero (review #9).
    if not quiet["ok"]:
        summary["failed"].append("finally-quiet")
    if rosters["problems"]:
        summary["failed"].append("finally-rosters")
        summary["roster_problems"] = rosters["problems"]
    out = args.result_file or os.path.join(rec.dir, "summary.json")
    # round-3 #4b: tail adjudication against the SAME immutable semantic
    # declaration; the engine OUTPUTS the actual tag->UUID bindings (the
    # Runtime/Archive consume this shape — never a summary.bound they
    # must guess). Scenario modes join the finally owners by (def, run).
    if args.phase == "tail":
        current_decl, _decl_err = load_semantic_declaration(args.tail_declaration)
        decl = _decl
        if _decl_err or current_decl != decl:
            summary["failed"].append("tail-declaration-mutated")
        mode_by_key = {(r.get("def_id"), r.get("run_id")): r.get("mode")
                       for r in owner_roster}
        owners = [dict(o, mode=mode_by_key.get((o.get("def_id"),
                                                o.get("run_id"))))
                  for o in rosters["owner_roster"]]
        verdict = adjudicate_tail(decl, owners, rosters["run_roster"])
        bindings_doc = {
            "schema": "p25-tail-bindings-v1",
            "declaration": os.path.abspath(args.tail_declaration),
            "declaration_sha256": decl.get("declaration_sha256"),
            "bindings": verdict["bindings"],
            "bindings_sha256": _sha256(json.dumps(
                verdict["bindings"], sort_keys=True)),
            "problems": verdict["problems"]}
        summary["tail_adjudication"] = {
            "ok": verdict["ok"], "problems": verdict["problems"],
            "declaration_sha256": decl.get("declaration_sha256")}
        summary["tail_bindings"] = verdict["bindings"]
        bind_path = os.path.join(rec.dir, "tail-bindings-%s.json" % _ts())
        summary["tail_bindings_file"] = bind_path
        if not verdict["ok"]:
            summary["failed"].append("tail-adjudication")
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=1, ensure_ascii=False,
                      default=str)
        bindings_doc["summary_sha256"] = _file_sha256(out)
        with open(bind_path, "w", encoding="utf-8") as fh:
            json.dump(bindings_doc, fh, indent=1, ensure_ascii=False, default=str)
        rec.record("finally", "tail-adjudication", verdict)
    else:
        with open(out, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=1, ensure_ascii=False,
                      default=str)
    _SQL_RELEASE_ARMED["armed"] = False
    print(json.dumps({"result_file": out,
                      "failed": summary["failed"],
                      "owner_roster_rows": len(owner_roster),
                      "run_roster_rows": len(run_roster),
                      "final_owner_roster_rows":
                          len(rosters["owner_roster"]),
                      "quiet_ok": quiet["ok"]}, indent=1))
    return 1 if summary["failed"] else 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", action="store_true")
    ap.add_argument("--contract-probe", action="store_true")
    ap.add_argument("--list-gates", action="store_true")
    ap.add_argument("--ast-check", action="store_true")
    ap.add_argument("--selftest-mocked", action="store_true")
    ap.add_argument("--phase", choices=("healthy", "tail"))
    ap.add_argument("--only")
    ap.add_argument("--released", action="store_true")
    ap.add_argument("--evidence-dir", default="results/smoke")
    ap.add_argument("--result-file")
    ap.add_argument("--pg-proof-file",
                    help="PG track binding JSON (default: the frozen %s "
                         "next to this script)" % DEFAULT_PG_PROOF_FILE)
    ap.add_argument("--pg-proof-name", default=PINNED_ROLLBACK_NAME,
                    help="pinned rollback case name inside the required "
                         "names (default: %s)" % PINNED_ROLLBACK_NAME)
    ap.add_argument("--connector-base")
    ap.add_argument("--dataset-base")
    ap.add_argument("--fixture-base")
    ap.add_argument("--proxy-base")
    ap.add_argument("--fixture-src-url", default="http://p25fixture:8090")
    ap.add_argument("--owned-handles-file", action="append", default=[],
                    help="prior real handles.json; repeat to retain source configs and enumerate earlier defs")
    ap.add_argument("--h10-expected-image", default="", help="externally accepted full image ID; mandatory h10 post")
    ap.add_argument("--h10-stage", choices=("pre", "post"), default="post",
                    help="h10 two-stage driver interface (review #8)")
    ap.add_argument("--h10-defs", default="",
                    help="comma-separated REAL sync def ids to observe")
    ap.add_argument("--h10-prior", default="",
                    help="pre-stage snapshot path (pre writes it, post "
                         "consumes it)")
    ap.add_argument("--h10-held-def", default="",
                    help="the def id deliberately holding a real owner "
                         "run across the crash (one of --h10-defs)")
    ap.add_argument("--h10-restart-record", default="",
                    help="driver-written STRICT crash record (SIGKILL + "
                         "same-image start) for the post stage")
    ap.add_argument("--sql-env-dir", default="",
                    help="q3 env dir containing compose.yaml for fixed exec postgres psql READONLY SQL "
                         "fact reader (h13 busy proof; armed only after "
                         "the release gates)")
    ap.add_argument("--tail-declaration", default="",
                    help="hash-frozen SEMANTIC tail declaration produced "
                         "BEFORE the tail run (REQUIRED for --phase tail; "
                         "no future UUIDs — the engine outputs the actual "
                         "bindings itself)")
    args = ap.parse_args()
    if args.plan:
        return cmd_plan()
    if args.contract_probe:
        return cmd_contract_probe()
    if args.list_gates:
        for s in SCENARIOS:
            print("%s: %s  phase=%s gates=%s paths=%s proxy=%s" %
                  (s["id"], s["name"], s["phase"], s["gates"], s["paths"],
                   s["proxy"]))
        return 0
    if args.ast_check:
        return cmd_ast_check()
    if args.selftest_mocked:
        return cmd_selftest_mocked()
    if args.phase:
        return cmd_phase(args)
    print("[smoke_p25] no mode selected (see --help); real HTTP is "
          "double-gated: --released AND env %s=%s" %
          (RELEASE_ENV, RELEASE_ENV_VALUE), file=sys.stderr)
    return 3


if __name__ == "__main__":
    sys.exit(main())

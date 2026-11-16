# RUNTIME-INTERFACE.md — Runtime-track data schemas (for independent adjudication)

Owner: Runtime GLM (driver + the 7 runtime files + this file). The
Archive group consumes these schemas READ-ONLY and adjudicates the
SAME shapes independently — no cross-imports of WIP modules, no
mutual edits (write boundary per root redistribution 2026-11-14).
Values below are schema contracts, not acceptance claims.

## 1. Semantic declaration (BEFORE tail; HTTP v1)

`p25_tail_roster.py declare --h10-binding <actual h10 known file>
--failed-healthy-summary EV/window-result.json --out EV/tail-declaration.json`
produces schema `p25-tail-roster-v1`. `spec.owner_rows` contain only
`def_tag`, `run_ref`, expected `blocked_reason`, optional `mode`, and
origin metadata. Future h11/h14 Def/Source/Run UUIDs are forbidden in
semantic rows. `spec.run_rows` describes one `h10-held` run with
`semantics=p25-crash-held-prepublish`, count=1. The spec SHA uses
`sha256(json.dumps(spec, sort_keys=True))`. `declared_at` is UTC RFC3339.

Known bindings are actual already-existing triples outside spec: h10 held
plus failedhealthy h13 owners, using the exact h10 producer tags
`failedhealthy-h13-<actual def UUID>`. Default generation independently
binds these origins to original failed summary/handles and raw GET Run
postpublish-403 evidence. Missing/changed/extra known owners are refused.
No P26 cleanup/adoption is performed. The actual h10 lease observation
must show `requires_reconciliation` before freezing that expectation.

Runtime pins the reviewed HTTP engine SHA de6eb2...59a1 and reuses its
pure semantic loader/adjudicator. Archive independently verifies the
whole global full-27 projection, original six rows, and parents.

## 2. Actual bindings (AFTER tail only)

HTTP emits `summary.tail_bindings_file` and `summary.tail_bindings`:
```json
{"schema":"p25-tail-bindings-v1","declaration":"<absolute path>",
 "declaration_sha256":"<spec SHA>","bindings":{
  "<owner-tag>":{"def_id":"<uuid>","source_id":"<uuid>","run_id":"<uuid>","kind":"owner","mode":null},
  "run/h10-held":{"def_id":"<uuid>","source_id":"<uuid>","run_id":"<uuid>","kind":"run","semantics":"p25-crash-held-prepublish"}},
 "bindings_sha256":"<sorted JSON SHA>","summary_sha256":"<final summary bytes SHA>","problems":[]}
```

`p25_tail_roster.py verify --declaration ... --summary ... --out ...`
replays exact actual owner + nonterminal-run sets, states, real parents,
and immutable declaration/binding/summary hash chains. `owner-roster`
with the same inputs emits the bare actual `def_id/source_id/run_id/
run_ref/blocked_reason/origin` list for the quiesce checker. No old v2
prebinding or `summary.bound` consumer remains in the tail driver.

## 3. Deployment record (`deploy_record_helper_p25.py` output)

```json
{"schema": "p25-deployment-record-v1",
 "cause": "<legitimate candidate/old-suite/base3 restart tag>",
 "recorded_at_utc": "RFC3339", "connector_boot": "RFC3339",
 "connector_image": "sha256:<hex64>",
 "source_sha256": "<candidate a1bff07f…ffd27>",
 "selected_build_sha256": "<hex64 of EV selected-build.json>",
 "selected_build_path": "<abs path>"}
```
Written after every LEGITIMATE (re)start; pointer at
`EV/deployment-record-latest.txt`; history never overwritten (new
records get -rN suffixes). Steady guard targets = the LATEST record's
boot/image.

## 4. Quiesce state + blocked proof (`check_owned_quiesced_p25.py`)

State (readonly psql collect, projections pinned to the real DDL):
```json
{"schema": "p25-quiesce-state-v1", "declared_at": "<str>",
 "defs": [{"id": "<uuid>", "schedule_enabled": false,
           "cursor_null": true}],
 "runs_terminal": [{"sync_def_id": "<uuid>", "id": "<uuid>",
                    "status": "succeeded|failed|aborted",
                    "started_at": "<str>"}],
 "owner_rows": [{"def_id": "<uuid>", "source_id":"<uuid>", "run_def_id":"<same def UUID>", "run_id": "<uuid>",
                 "run_status": "<str>",
                 "blocked_reason": "requires_reconciliation|"}],
 "new_runs_since_decl": [{"id": "<uuid>", "sync_def_id": "<uuid>"}]}
```
Pre-ABC blocked-admission proof (SAVED artifact; a POST after ABC is
a forbidden write):
```json
{"ok": true, "phase": "pre-abc",
 "entries": {"<owner-def-uuid>": {"http_status": <4xx int>,
   "rc": <int>, "body_sha256": "<hex64>",
   "run_id": "<uuid == retained owner run, optional>"}}}
```
Every owner def UUID needs its OWN entry; unknown owner rows and
missing entries fail closed (retained truthfully, never cleaned).

## 5. Driver-side canonical artifacts (paths under $EV_ROOT)

- `selected-build.json` — schema `p25-selected-build-v1` (images map
  with FULL sha256 ids incl. python-3.12-alpine; components with
  per-artifact shas; source_sha256 == the candidate).
- `swap-build-binding.json` — schema `p25-swap-build-binding-v2`
  (r18-exact group/input verification; produced BEFORE any stop).
- `selected-index.json` — schema `p25-selected-index-v1`, exactly the
  22 canonical stage ids → latest `steps/<stage>-attempt<N>` dirs
  (ARCHIVE-CONTRACT §1.1; finalize validates cmd/stdout/stderr/rc +
  explicit TRUE verdict per stage).
- `aux-down-attemptN/proxy-quiet-proof.stdout.log` /
  `fixture-quiet-proof.stdout.log` — RAW quiet proofs (proxy stats
  JSON object with strict non-bool ints; per-src fixture counter JSON
  list) — consumed by the Archive postfacts collector via
  `--aux-quiet-dir`.
- `abc-result.json` — THIS round's raw `result_p21.json`, byte-exact
  (written once by p21abc; overwrite refused).

## 6. Actual engine handoff

Tail argv uses actual connector17094/dataset17078/fixture17092/proxy17093
bases, fixture source URL `http://p25fixture:8090`, `--tail-declaration`,
and repeatable `--owned-handles-file` for actual window union + h10 merged
handles. There is NO `--tail-bindings` input. All tenant JWTs stay in env
memory; ARM reuses the main publish-capable actor JWT. Healthy SQL uses
`--sql-env-dir <fixed q3 dir>` and the engine's fixed compose/psql read-only
transport, never a host DSN. Arm readiness is `armed:true` and
`scheduler_loop_running:true`.

Base3 restart evidence: `restart-before-running.json` and
`restart-running.json` with raw per-service ps/inspect; three base boots
advance and six aux boots remain unchanged. Four CP snapshots (h1 trial,
full healthy h1, two failedhealthy owners) are byte-semantically unchanged.
The canonical restart step also contains byte-exact `h10-report.json`
pointing to the actual helper attempt's crash/prior/post/baseline raws.

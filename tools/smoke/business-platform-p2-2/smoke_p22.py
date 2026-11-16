#!/usr/bin/env python3
"""OpenFoundry P2.2 interruptible-worker HTTP acceptance (SKELETON v2).

Status: TOOL SKELETON, 2026-11-14, aligned to the root's supplemental
authorization. The P2.2 service candidate is still being implemented —
NOTHING here has run against a stack. Every P2.2-specific route spelling
/ JSON field / DB column / status vocabulary is a CANDIDATE from the
finalized contract briefing and is re-verified against the root's actual
candidate at run time (preflight probe + fail-closed pins — README §契约
核定). No fault-entry or test-only endpoint is invented on the services:
the only mutated service routes are the real contract ones (async
trigger, single-run GET, cancel) plus the frozen P1 surface. Injection
lives ONLY in the isolation stack's own tooling: the fixture barrier
(root-authorized extension of the shared rest_source.py) and the
controlled dataset proxy (dataset_proxy.py, tool-side).

Scenario matrix — maps 1:1 to the root's 8-item minimal HTTP list:
  D (1) async_admission_and_completion — 202 while the source barrier
        holds page 1; the POST has fully returned; the parked pull stays
        CONTINUOUSLY in flight >=35s after admission WHILE heartbeat_at
        advances in PG (BOTH conditions measured and asserted
        numerically — a 20s advance followed by an early release proves
        nothing); release; the SAME run completes with real content
        (151 rows value-by-value, sha256 == txn metadata.content_hash,
        txn COMMITTED, lineage bound).
  E (3) running_cancel_cross_process — slow page parked on the barrier,
        executed by instance A (`connector`); cancel arrives via HTTP on
        instance B (connector2, same DB) => DB observation, not a local
        map; believed only from the PERSISTED terminal row; after the
        release NO further pages and NO publish; duplicate cancel is
        idempotent (no state flip, no resurrect, no new pages).
  F (2) queue_pending_cancel_and_queuefull — one worker occupied by a
        barrier-parked run; a second run stays pending and is cancelled
        with ZERO source requests for its src; duplicate idempotent;
        queue-full admission is an EXPLICIT rejection leaving no orphan
        run row. Requires the root-pinned QUEUE_CONTRACT; unpinned FAILs.
  G (5) gate_wins_too_late_via_proxy — deterministic gate-window: the
        other replicas are stopped so connector3 (whose
        DATASET_SERVICE_URL is the controlled proxy) executes the run;
        the proxy holds the REAL outputs:commit RESPONSE after the REAL
        Dataset has processed it; gate-entry is observed via the PG phase
        column; cancel => 409 too_late (pinned marker); release => the
        connector receives the genuine committed response and the run
        SUCCEEDS; nothing is relabelled aborted.
  H (4) tenant_and_binding_denials — same-sub cross-org, cross-org
        admin, personal domain, same-org NON-WRITER, wrong-parent sync,
        malformed run id: every denial leaves the runs/target snapshots
        unchanged; the attacked run stays cancellable by its owner.
  I (6a) sigterm_shutdown_no_redispatch — SIGTERM (docker stop -t 120,
        far above the ~75s stop budget) with the run parked pre-gate:
        graceful exit verified via WHITELISTED inspect fields only
        (Running/ExitCode/OOMKilled/FinishedAt); the blocked run is
        GENUINELY terminalized with zero new pages/publish; the STABLE
        baseline is taken after the stop and before the restart; restart
        => the SAME full safe projection and counts (zero re-dispatch);
        P2.1 legacy rows untouched; the fixture is never restarted.
  J (6b) deadline_bounds — DISCRIMINATED from a plain source timeout: a
        dedicated aux rebuild of connector2 ONLY (--no-deps, interlocked,
        live-task-guarded) runs TASK_TTL=7s with the source HTTP timeout
        at 60s; terminal at TTL scale + deadline-ish failure reason +
        zero publish + no zombie pulls; the aux is restored afterwards.
  K (6c) sync_disconnect_compat — DEFAULT (synchronous) POST with the
        first page parked; the CLIENT disconnects (NOT the cancel API):
        the run must reach a genuine terminal (aborted, or failed WITH a
        real non-empty failure reason — no arbitrary non-success passes),
        zero publish, no further pages after release.
  L (7) jwt_expiry_no_fallback — a short-TTL caller JWT expires WHILE
        the fetch is parked; the proof requires the run to have ENTERED
        the publish gate, an AUTH-rejection marker in the real failure
        detail (exact wording pinned with root), and ZERO txns/files —
        a source timeout or a never-attempted commit never passes.
  M (8) restart_state_preserved — a FIXED cohort (completed/cancelled/
        interrupted + P2.1 legacy rows), each bound to its run/source/
        target: full safe projection, source request_count, file id-set
        and txn id->status map compared before/after the connector-only
        restart; the completed member's bound txn stays COMMITTED and
        its artifact re-downloads 200-then-hash-identical.
  N (cancel convention) legacy_cancel_unsupported — cancelling a P2.1
        LEGACY run answers 409 + unsupported (ROOT-PINNED); the archived
        committed-unconfirmed C row is never aborted and never
        re-dispatched. The owner identity is resolved at runtime from
        PUBLIC columns only (the archived evidence stores none).

ROOT-PINNED (2026-11-14) and HANDLER-VERIFIED against
services/connector-management-service/internal/handlers/handlers.go
(GetSyncRun / CancelSyncRun / RunSyncJob) plus models.SyncRun and
domain/sync/status.go:
  * models.SyncRun PUBLIC control fields control_mode (legacy|controlled)
    / cancel_requested_at / cancelled_by / publish_entered_at (last three
    omitted when empty) — these NEVER trip the forbidden-key walk;
    PRIVATE stays lease material only (lease_holder/lease_token/
    lease_expires_at/heartbeat_at + variants).
  * GET run -> 200 direct SyncRun; async trigger -> 202 direct SyncRun
    (no wrapper); malformed uuid -> 400; not visible -> 404 {"error"}.
  * cancel -> {"error"?, "code"?, "run"?}: pending-aborted & existing-
    terminal 200 {run}; running persisted 202 {run} still running; gate
    won 409 code=cancel_too_late {run}; legacy 409
    code=cancel_unsupported_legacy {run}; queue full / shutting down ->
    503 {"error"} BEFORE any durable row.
  * run status vocabulary: pending/running/succeeded/failed/aborted
    (no queued/cancelled/error values).
Defaults concurrency=1 / queue=64 / overall TTL=300s remain root-pinned;
their env NAMES and the required PG test list are still pending.

Evidence layering (README §证据分层): the narrow cancel-vs-BeginPublish
ordering, PID lock waiting, queue internals, and shutdown tails are
proven by the root's real-PG suites + handler unit tests (names go into
p22-required-names.txt); HTTP scenarios above prove only the externally
observable behavior. No random sleeps gate any ordering.

Usage: python3 -I smoke_p22.py --env-dir <tmp> [--only D,E,...]
       python3 -I smoke_p22.py --env-dir <tmp> --contract-probe
"""

import argparse
import hashlib
import http.client
import json
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.error
from urllib.parse import quote
import uuid

P1_SMOKE = Path(__file__).resolve().parent.parent / "business-platform-p1" / "smoke" / "smoke.py"

# ------------------------------------------------------------------ contract
# CANDIDATE constants — the root session's final candidate overrides any
# of these before execution (README §契约核定 lists what must be pinned).
CONTRACT_P22 = {
    "sync_run_async": "/api/v1/data-connection/syncs/{id}/run?async=true",  # CANDIDATE spelling
    "sync_run_sync": "/api/v1/data-connection/syncs/{id}/run",              # frozen P1 surface
    "sync_run_get": "/api/v1/data-connection/syncs/{id}/runs/{run_id}",     # already in P1 CONTRACT
    "sync_run_cancel": "/api/v1/data-connection/syncs/{id}/runs/{run_id}/cancel",  # CANDIDATE
}
ADMIT_STATUS = 202                      # async admission (handler-verified)
# --- cancel response conventions: HANDLER-VERIFIED 2026-11-14 ----------
#   read from handlers.go CancelSyncRun / repo cancel.go:
#   * unified envelope {"error"?, "code"?, "run"?: SyncRun} — the post-
#     cancel run state is ALWAYS body.run.status, never the outer body
#     (the outer body has NO status key; missing run = FAIL, not pass).
#   * pending (already aborted) / existing terminal -> 200 {run}
#   * running, gate open                          -> 202 {run} (still running)
#   * gate entered first                          -> 409 code=cancel_too_late {run}
#   * legacy row                                  -> 409 code=cancel_unsupported_legacy {run}
#   * not visible (wrong parent/tenant/unknown)   -> 404 {"error": ...}
#   * queue full / shutting down (admission)      -> 503 {"error": ...}
#     answered BEFORE any durable row exists.
CANCEL_PENDING_STATUS = 200             # handler-verified
CANCEL_RUNNING_STATUS = 202             # handler-verified
CANCEL_TERMINAL_STATUS = 200            # handler-verified (idempotent no-op)
TOO_LATE_STATUS = 409                   # handler-verified
TOO_LATE_CODE = "cancel_too_late"       # handler-verified (writeJSON code)
UNSUPPORTED_STATUS = 409                # handler-verified (legacy rows)
UNSUPPORTED_CODE = "cancel_unsupported_legacy"  # handler-verified
QUEUEFULL_STATUS = 503                  # handler-verified (pre-durable-row)
# -------------------------------------------------------------------------
DENY_STATUSES = (403, 404)              # 403 canWriteSource / 404 not visible
MALFORMED_ID_STATUS = 400               # handler-verified (routeUUIDParam)
# run status vocabulary read from domain/sync/status.go: pending/running/
# succeeded/failed/aborted — there is NO queued/cancelled/error value.
CANCEL_TERMINAL_CANDIDATES = ("aborted",)               # runtime-pinned in E
RUNNING_STATES = ("pending", "running")                 # pending == queued
TERMINAL_STATES = ("succeeded", "failed", "aborted")

# --- models.SyncRun P22 PUBLIC fields: ROOT-APPROVED 2026-11-14 ---------
# control_mode (legacy|controlled), cancel_requested_at, cancelled_by
# (UUID), publish_entered_at — the last three OMITTED when empty. These
# are PUBLIC control fields, NOT lease secrets: they must never trip the
# forbidden-key walk. owner_id/requested_by/attempt/generation stay
# public from P2.1.
PUBLIC_CONTROL_KEYS = ("control_mode", "cancel_requested_at",
                       "cancelled_by", "publish_entered_at")
PUBLIC_KEYS_P21 = ("requested_by", "owner_id", "attempt", "generation")

# --- PRIVATE keys: ROOT-PINNED — lease material only, plus variants ------
PRIVATE_KEYS = ("lease_holder", "lease_token", "lease_expires",
                "lease_expires_at", "heartbeat", "heartbeat_at")

# --- DB columns for the whitelist projection: ROOT-APPROVED names -------
PINNED_CANCEL_COLUMNS = ("cancel_requested_at", "cancelled_by")
PINNED_CONTROL_COLUMNS = ("control_mode", "publish_entered_at")
# full-projection extras (I/M compare these across stop/restart): the
# P2.1 columns plus the terminal-time/artifact-binding columns. None of
# them is secret; lease token VALUES are never selected.
PINNED_PROJECTION_COLUMNS = ("attempt", "generation", "content_hash",
                             "dataset_version_id", "finished_at")

# --- defaults ROOT-PINNED 2026-11-14; env names VERIFIED from the config
# candidate (root: key names will not change; explicit-0 validation is
# being fixed separately). NOTE: CONNECTOR_SYNC_ASYNC_QUEUE is the TOTAL
# admission capacity (queued + running, sync + async combined) — NOT an
# extra waiting-slot budget on top of the running task. ------
QUEUE_CONTRACT = {
    "env": "CONNECTOR_SYNC_ASYNC_QUEUE",     # total admission capacity
    "depth": 64,
    "concurrency_env": "CONNECTOR_SYNC_ASYNC_WORKERS",
    "concurrency": 1,
}
DEADLINE_CONTRACT = {
    "env": "CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS",   # task TTL, fixed at
    "seconds": 300,                                 # Reserve admission
    "auth_env": "CONNECTOR_SYNC_ASYNC_AUTH_TTL_SECS",  # in-process auth
    "auth_seconds": 300,                            # reference TTL
}

# P2.1 archived legacy runs (acceptance run b6ec49ab): must stay
# running/untouched across every P2.2 restart check; cancelling them is
# 409 unsupported (N).
P21_LEGACY_RUNS = {
    "B_lease_lost_unpublished": "6d6dc56f-1498-4b13-871e-bab489e8b631",
    "C_committed_unconfirmed": "4ff175b3-65d7-47b6-9a83-d46e03bbfe23",
}

ARGS = None
M = None      # loaded P1 module
PINS = {}     # runtime-resolved contract pins
SESSION = {}  # cross-scenario handles (cancelled run ids, artifacts, ...)


# ------------------------------------------------------------------ plumbing
def load_p1(env_dir):
    global M
    import importlib.util
    spec = importlib.util.spec_from_file_location("p1smoke", P1_SMOKE)
    M = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(M)
    M.ARGS = type("A", (), {"env_dir": str(env_dir)})()
    M.load_env(env_dir)
    # Optional P2.2 replicas register in BASES ONLY when they answer
    # healthz — M.wait_health() iterates every BASES entry, so an
    # unconditional registration would hang runs without the replicas;
    # the scenarios that need them fail closed instead.
    import urllib.request as _u
    for key, url in (("connector2", ARGS.connector2_base),
                     ("connector3", ARGS.connector3_base),
                     ("dataset_proxy", ARGS.dataset_proxy_base)):
        path = "/healthz" if not key.startswith("dataset") else "/p22proxy/healthz"
        # brief retry: aux containers may still be binding their ports
        # right after `p22_connector2.sh up` returns
        for _attempt in range(6):
            try:
                with _u.urlopen(url + path, timeout=3) as r:
                    if r.status == 200:
                        M.BASES[key] = url
                        break
            except Exception:
                time.sleep(1.0)
        else:
            M.BASES.pop(key, None)


def http_long(base_key, path, *, token, method="POST", payload=None, timeout):
    """Request with an EXPLICIT timeout (M.http pins 30s)."""
    base = M.BASES[base_key]
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Authorization": "Bearer " + token}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return resp.status, (json.loads(body) if body else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read(4000)
        try:
            return exc.code, json.loads(raw) if raw else None
        except ValueError:
            return exc.code, {"_raw": raw.decode(errors="replace")}


def assert_no_private_keys(body, where):
    """Recursive key walk (as accepted in P2.1). Leak evidence carries the
    KEY + JSON path ONLY — never body values."""
    stack = [(body, "$")]
    while stack:
        node, path = stack.pop()
        if isinstance(node, dict):
            for key, value in node.items():
                M.expect(key not in PRIVATE_KEYS,
                         f"private key present in public JSON ({where})",
                         {"key": key, "path": f"{path}.{key}"})
                stack.append((value, f"{path}.{key}"))
        elif isinstance(node, list):
            stack.extend((item, f"{path}[{i}]") for i, item in enumerate(node))


def psql_connectors(sql):
    return M.psql(sql, database="connectors")


def compose(*args):
    """docker compose against the P1 project; the P2.2 override file is
    included whenever it has been rendered so stop/start can address the
    optional replicas too."""
    env_dir = Path(ARGS.env_dir)
    files = ["-f", str(env_dir / "compose.yaml")]
    override = env_dir / "compose-p22-connector2.yaml"
    if override.exists():
        files += ["-f", str(override)]
    cmd = ["docker", "compose", *files,
           "--project-name", "openfoundry-platform-p1-q3v9", *args]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300)


def connector_replicas():
    """Every connector replica present in this run (base + optional)."""
    return ["connector"] + [r for r in ("connector2", "connector3") if r in M.BASES]


_RFC3339_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def inspect_started_at(cid, label):
    """Whitelisted container StartedAt with FAIL-CLOSED plumbing (final
    review D): the command must exit 0 AND the value must be a non-empty
    RFC3339 timestamp BEFORE any caller compares it — an empty string
    must never silently differ from (and thus 'differ-pass' against) a
    real one."""
    proc = subprocess.run(["docker", "inspect", "--format",
                           "{{.State.StartedAt}}", cid],
                          capture_output=True, text=True, timeout=60)
    M.expect(proc.returncode == 0, f"{label}: docker inspect failed",
             M.scrub(proc.stderr)[:200])
    value = proc.stdout.strip()
    M.expect(_RFC3339_RE.match(value),
             f"{label}: StartedAt is not a valid non-empty RFC3339 value",
             {"value": value[:50]})
    return value


# ------------------------------------------------- DB whitelist projection
_SYNC_RUN_COLUMNS = None


def sync_run_columns():
    global _SYNC_RUN_COLUMNS
    if _SYNC_RUN_COLUMNS is None:
        out = psql_connectors(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'sync_runs' ORDER BY column_name;")
        _SYNC_RUN_COLUMNS = {line.strip() for line in out.splitlines() if line.strip()}
    return _SYNC_RUN_COLUMNS


def require_columns(candidates, what):
    """Fail closed when NONE of the candidate columns has landed yet."""
    present = [c for c in candidates if c in sync_run_columns()]
    M.expect(present, f"{what}: no candidate column has landed "
             f"(tried {candidates}) — root contract not final; refusing to guess",
             {"present": present})
    return present[0]


def run_row(run_id):
    """Whitelisted, non-secret projection (ROOT-APPROVED column names):
    status/control/cancel/gate + lease booleans/timestamps + the P2.1
    attempt/generation + artifact-binding/terminal-time columns for the
    I/M full across-restart comparisons. NEVER selects lease token
    values, credentials, or config bodies."""
    for col in (PINNED_CANCEL_COLUMNS + PINNED_CONTROL_COLUMNS
                + PINNED_PROJECTION_COLUMNS):
        require_columns((col,), col)
    out = psql_connectors(
        f"SELECT status, (lease_holder IS NOT NULL), "
        f"COALESCE(heartbeat_at::text, ''), COALESCE(lease_expires_at::text, ''), "
        f"(lease_token IS NOT NULL), (cancel_requested_at IS NOT NULL), "
        f"(publish_entered_at IS NOT NULL), COALESCE(control_mode, ''), "
        f"COALESCE(attempt::text, ''), COALESCE(generation::text, ''), "
        f"(content_hash IS NOT NULL), (dataset_version_id IS NOT NULL), "
        f"COALESCE(finished_at::text, '') "
        f"FROM sync_runs WHERE id = '{run_id}'::uuid;")
    M.expect(out and out.count("|") == 12, f"sync_runs row unreadable for {run_id}",
             {"got": out[:100]})
    (status, has_holder, hb, exp, has_token, cancel_seen, gate, mode,
     attempt, generation, has_hash, has_version, finished) = out.split("|")
    return {"status": status, "has_holder": has_holder == "t",
            "heartbeat": hb, "expires": exp, "has_token_col": has_token == "t",
            "cancel_requested": cancel_seen == "t", "gate_entered": gate == "t",
            "control_mode": mode, "attempt": attempt, "generation": generation,
            "has_content_hash": has_hash == "t", "has_version": has_version == "t",
            "finished_at": finished}


def pg_run_ids(sync_id):
    """Whitelist run-ID set for a sync (all connectors may be DOWN — this
    is the post-stop baseline source, HTTP list is not)."""
    out = psql_connectors(
        f"SELECT COALESCE(string_agg(id::text, ',' ORDER BY id::text), '') "
        f"FROM sync_runs WHERE sync_def_id = '{sync_id}'::uuid;")
    return sorted(x for x in out.strip().split(",") if x)


def wait_run_row(run_id, timeout=30):
    """Retry run_row until the PG row exists (admitted-but-not-yet-visible)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return run_row(run_id)
        except AssertionError:
            time.sleep(0.5)
    raise AssertionError(f"run row never appeared in PG: {run_id}")


# ------------------------------------------------------------- HTTP helpers
def trigger_async(token, sync_id, *, timeout=60, via="connector"):
    path = CONTRACT_P22["sync_run_async"].format(id=sync_id)
    return http_long(via, path, token=token, payload={}, timeout=timeout)


def get_run(token, sync_id, run_id, *, via="connector"):
    path = CONTRACT_P22["sync_run_get"].format(id=sync_id, run_id=run_id)
    return M.http(via, path, token=token)


def cancel_run(token, sync_id, run_id, *, via="connector"):
    path = CONTRACT_P22["sync_run_cancel"].format(id=sync_id, run_id=run_id)
    return M.http(via, path, payload={}, token=token)


def cancel_body_run(body, where):
    """The post-cancel run ALWAYS lives at body.run (handler-verified
    cancelResponse). The outer envelope has no status key — a missing run
    object is a FAIL, never an accidental pass."""
    M.expect(isinstance(body, dict) and isinstance(body.get("run"), dict),
             f"{where}: cancel body is missing the run object",
             {"keys": sorted(body.keys()) if isinstance(body, dict) else None})
    return (body or {}).get("run") or {}


def poll_run(token, sync_id, run_id, *, until, timeout, interval=1.0, via="connector"):
    """Poll the single-run GET until predicate(json) holds. Never treats
    an error body as terminal. Returns (final_body_or_None, last_status)."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        res = get_run(token, sync_id, run_id, via=via)
        if res.status == 200 and res.body is not None:
            assert_no_private_keys(res.body, "poll_run body")
            last = res.body
            if until(res.body):
                return res.body, res.status
        time.sleep(interval)
    return last, 0


def snapshot_runs(token, sync_id):
    res = M.api("connector", "sync_runs", path_args={"id": sync_id}, token=token)
    M.expect(res.status == 200, "sync runs list failed", {"status": res.status})
    items = M.extract_items(res.body) or []
    return sorted(str(r.get("id")) for r in items if r.get("id"))


# -------------------------------------------------- fixture barrier control
def fixture_snap(src):
    return M.http("fixture", f"/admin/src/{quote(src)}").body or {}


def barrier_set(src, pages, wait_s=180):
    """pages: list[int] or 'all'. Installing RESETS release marks."""
    state = M.fixture_admin(src, {"barrier_pages": pages, "barrier_wait_s": wait_s})
    return state


def barrier_release(src, pages="all"):
    state = M.fixture_admin(src, {"barrier_release": pages})
    return state


def wait_barrier_parked(src, *, min_waiters=1, timeout=30):
    """Wait until >=min_waiters requests are parked on src's barrier
    (request_count already incremented at entry — the in-flight gate)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = fixture_snap(src)
        if snap.get("barrier_waiters", 0) >= min_waiters \
                and snap.get("active", 0) >= min_waiters:
            return snap
        time.sleep(0.2)
    raise AssertionError(f"{src}: barrier parking (waiters>={min_waiters}) never "
                         f"observed: {fixture_snap(src)}")


def fixture_count(src):
    return fixture_snap(src).get("request_count", 0)


# ------------------------------------------------------- proxy admin plane
def proxy_hold(*, hid, method, path_suffix, dataset_id=None, max_hold_s=300):
    res = M.http("dataset_proxy", "/p22proxy/hold",
                 payload={"id": hid, "method": method, "path_suffix": path_suffix,
                          "dataset_id": dataset_id, "max_hold_s": max_hold_s})
    M.expect(res.status == 200, "proxy hold install failed",
             {"status": res.status, "body": M.scrub(str(res.body))[:200]})
    return res.body


def proxy_release(*, hid=None, all_=False):
    payload = {"all": True} if all_ else {"id": hid}
    res = M.http("dataset_proxy", "/p22proxy/release", payload=payload)
    M.expect(res.status == 200, "proxy release failed", {"status": res.status})
    return res.body


def proxy_snapshot():
    res = M.http("dataset_proxy", "/p22proxy/snapshot")
    M.expect(res.status == 200, "proxy snapshot failed", {"status": res.status})
    return res.body or {"holds": []}


def wait_proxy_held(hid, *, timeout=60):
    """Wait until hold hid has matched>=1 and is currently withholding a
    response — i.e. the proxy has ALREADY received the upstream's answer
    to the real request (the proxy calls upstream BEFORE parking).

    CAVEAT (review 2026-11-14): held_now proves the upstream RESPONSE was
    received, NOT that the Dataset business outcome is COMMITTED. The
    commit proof for too_late/lost-response scenarios stays with the real
    transaction/file/hash verification (prove_committed_artifact after
    release) plus the publish_entered_at gate observation — never with
    the proxy counters alone."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        hold = next((h for h in proxy_snapshot().get("holds", [])
                     if h.get("id") == hid), None)
        if hold and hold.get("matched", 0) >= 1 and hold.get("held_now", 0) >= 1:
            return hold
        time.sleep(0.2)
    raise AssertionError(f"proxy hold {hid} never withheld a matched response")


# ------------------------------------------------------------- stack setup
def create_source_raw(token, payload):
    res = M.api("connector", "src_create", token=token, payload=payload)
    M.expect(res.status in (200, 201) and res.body.get("id"),
             "source create failed", {"status": res.status,
                                      "body": M.scrub(str(res.body))[:400]})
    return res.body


def owned_org_sync(src_name, *, total=151, page_size=50, timeout_ms=40000,
                   owner=None, org=None):
    """Owner identity + fixture (no barrier/delay — scenarios park pages
    explicitly via barrier_set) + Source + Dataset + Sync.

    Review fix 1: pass `owner` (and optionally `org`) to REUSE an existing
    identity/tenant instead of minting a fresh one — F's queue fills and
    H's wrong-parent sync must live in ONE tenant the owner can
    positively access (triggering into a foreign tenant's sync would 404
    before any queue/binding semantics could be exercised)."""
    if owner is not None:
        org = org or owner.org
        M.expect(org == owner.org,
                 f"{src_name}: reused owner's org ({owner.org}) != requested org",
                 {"requested": org})
    else:
        org = org or str(uuid.uuid4())
        owner = M.Identity(f"p22-{src_name}-owner", org=org)
    M.fixture_admin(src_name, {"mode": "ok", "content": 1, "total": total,
                               "page_size": page_size})
    payload = M.source_payload(f"p22-{src_name}", src_name)
    payload["config"]["sync"]["limits"]["request_timeout_ms"] = timeout_ms
    payload["config"]["sync"]["pagination"]["page_size"] = page_size
    source = create_source_raw(owner.token, payload)
    target = M.create_dataset(owner.token, f"p22-{src_name}-out", "json")
    sync = M.create_sync(owner.token, source["id"], target["id"])
    M.expect(sync.status in (200, 201), f"{src_name}: sync create failed",
             {"status": sync.status})
    return {"org": org, "owner": owner, "source": source, "target": target,
            "sync_id": sync.body["id"], "src": src_name}


def extract_run_id(admission_body):
    """Resolve the run id from the admission response (CANDIDATE 'id')."""
    M.expect(isinstance(admission_body, dict), "admission body is not an object")
    run_id = admission_body.get("id")
    M.expect(run_id, "admission body carries no run id (candidate key 'id' — "
             "root pin needed)", sorted(admission_body.keys()))
    return run_id


def short_ttl_identity(label, org, *, ttl_s, roles=None):
    """Identity whose JWT expires after ttl_s — L proves the Dataset
    rejects the EXPIRED caller identity with no fallback principal."""
    sub = str(uuid.uuid4())
    token = M.mint_token(sub=sub, org=org, roles=roles, ttl=ttl_s)
    return type("I", (), {"sub": sub, "org": org, "token": token})()


def prove_committed_artifact(token, target, run_json, *, total, ev, label):
    """THE anti-false-positive proof block (as accepted in P2.1 C)."""
    files = M.list_files(token, target["id"])
    M.expect(len(files) == 1, f"{label}: expected exactly ONE committed file",
             {"files": len(files)})
    file_row = files[0]
    run_id = run_json.get("id")
    M.expect(str(file_row.get("logical_path", "")).find(str(run_id)) >= 0,
             f"{label}: logical_path not bound to this run",
             {"logical_path": file_row.get("logical_path")})
    txn_id = file_row.get("transaction_id")
    M.expect(txn_id, f"{label}: published file has no transaction binding")
    txn = M.api("dataset", "ds_txn_get",
                path_args={"id": target["id"], "branch": "main", "txn": txn_id},
                token=token)
    M.expect(txn.status == 200, f"{label}: transaction GET failed",
             {"status": txn.status})
    txn_body = txn.body or {}
    ev["txn_status"] = str(txn_body.get("status", "")).upper()
    M.expect(ev["txn_status"] == "COMMITTED", f"{label}: txn not COMMITTED",
             ev["txn_status"])
    lineage = (txn_body.get("metadata") or {}).get("lineage") or {}
    ev["lineage_run_match"] = lineage.get("sync_run_id") == run_id
    M.expect(ev["lineage_run_match"], f"{label}: lineage not bound to this run",
             {"lineage": lineage.get("sync_run_id")})
    dl = M.download_file(token, target["id"], file_row["id"])
    M.expect(dl.status == 200, f"{label}: artifact download failed",
             {"status": dl.status})
    digest = hashlib.sha256(dl.body).hexdigest()
    ev["artifact_sha256_matches_txn"] = digest == (txn_body.get("metadata") or {}).get("content_hash")
    M.expect(ev["artifact_sha256_matches_txn"],
             f"{label}: artifact sha256 != txn metadata.content_hash")
    rows = [json.loads(line) for line in dl.body.splitlines() if line.strip()]
    M.expect(rows == M.expected_records(total, 1),
             f"{label}: artifact rows do not match the source value-by-value",
             {"rows": len(rows), "want": total})
    return file_row


def dataset_txn_ids(token, dataset_id):
    """Dataset ListTransactions via the P1 route — HANDLER-VERIFIED
    envelope (transactions.go:135-174, models.Page): {"data": [...]|null,
    "next_cursor"?(omitempty), "has_more": bool}. Fails CLOSED on any
    unknown envelope (never defaults an unreadable structure to zero);
    treats a JSON-null data as the legal empty nil slice; refuses to
    silently truncate by requiring has_more=false in this caller.
    Returns {txn_id: status}."""
    res = M.api("dataset", "ds_transactions_list",
                path_args={"id": dataset_id}, token=token)
    M.expect(res.status == 200, "transactions list failed",
             {"status": res.status})
    body = res.body
    M.expect(isinstance(body, dict) and "data" in body and "has_more" in body,
             "transactions envelope is not models.Page {data,has_more} — "
             "refusing to default an unknown envelope to zero",
             {"keys": sorted(body.keys()) if isinstance(body, dict)
              else type(body).__name__})
    data = body.get("data")
    if data is None:
        data = []  # legal nil slice (data has no omitempty -> null)
    M.expect(isinstance(data, list), "transactions data is not an array",
             {"type": type(data).__name__})
    M.expect(body.get("has_more") is False,
             "transactions has_more=true — follow the cursor or shrink "
             "the case; never silently truncate",
             {"has_more": body.get("has_more")})
    out = {}
    for item in data:
        M.expect(isinstance(item, dict) and item.get("id"),
                 "transaction item missing id")
        out[item["id"]] = str(item.get("status", "")).upper()
    return out


# ------------------------------------------------------------- preflight ----
def contract_probe(ev):
    """Route/schema presence probes BEFORE any scenario mutates state."""
    ctx = owned_org_sync(f"p22probe-{uuid.uuid4().hex[:6]}", total=0)
    owner = ctx["owner"]
    # (a) cancel route exists: unknown run under an OWNED sync (authorized
    # principal — a stranger's 403 would conflate tenant denial with route
    # absence) must give a defined 404; 405/501 means the route spelling
    # has not landed.
    ghost = str(uuid.uuid4())
    res = cancel_run(owner.token, ctx["sync_id"], ghost)
    ev["cancel_route_status"] = res.status
    M.expect(res.status == 404,
             "cancel route not reachable (405/501/other): CANDIDATE spelling "
             f"{CONTRACT_P22['sync_run_cancel']} not landed — pin with root",
             {"status": res.status})
    # (b) schema columns landed under the ROOT-APPROVED names (fail closed)
    ev["cancel_columns"] = [require_columns((c,), c) for c in PINNED_CANCEL_COLUMNS]
    ev["control_columns"] = [require_columns((c,), c) for c in PINNED_CONTROL_COLUMNS]
    # (c) run GET route already part of the P1 contract — sanity only.
    res = get_run(owner.token, ctx["sync_id"], ghost)
    M.expect(res.status in DENY_STATUSES, "run GET for unknown run must 403/404",
             {"status": res.status})
    # (d) fixture barrier extension actually deployed in THIS stack.
    snap = fixture_snap(f"p22probe-{uuid.uuid4().hex[:6]}")
    for key in ("barrier_waiters", "active", "inventory_started"):
        M.expect(key in snap, f"fixture barrier extension missing key {key} — "
                 "is the stack running the extended rest_source.py?")
    # (e) optional replicas: record availability; scenarios fail closed.
    ev["connector2_up"] = "connector2" in M.BASES
    ev["connector3_up"] = "connector3" in M.BASES
    ev["dataset_proxy_up"] = "dataset_proxy" in M.BASES


def pin_cancelled_vocabulary(observed_status):
    M.expect(observed_status in CANCEL_TERMINAL_CANDIDATES,
             f"observed cancel terminal '{observed_status}' outside candidate "
             f"vocabulary {CANCEL_TERMINAL_CANDIDATES} — root pin needed")
    PINS["cancelled_status"] = observed_status


# ------------------------------------------------------------------ D -------
def scenario_d(suffix, ev):
    """(root 1) 202 while page 1 is barrier-parked; the parked pull stays
    CONTINUOUSLY in flight >=35s after admission WHILE heartbeat_at
    advances (both conditions measured, both asserted numerically —
    review fix 3: observing a 20s advance and releasing early proves
    nothing about a 30s window); release; same-run real completion.
    Source request_timeout_ms=60000 legally exceeds the window; default
    task/Auth TTLs (300s) exceed it too."""
    ctx = owned_org_sync(f"p22d-{suffix}", timeout_ms=60000)
    owner = ctx["owner"]
    barrier_set(ctx["src"], [1], wait_s=180)

    t_admit = time.monotonic()
    status, body = trigger_async(owner.token, ctx["sync_id"], timeout=30)
    admission_elapsed = round(time.monotonic() - t_admit, 1)
    ev["admission_status"], ev["admission_elapsed_s"] = status, admission_elapsed
    assert_no_private_keys(body, "D admission response")
    M.expect(status == ADMIT_STATUS, "D: async trigger != 202", {"status": status})
    run_id = extract_run_id(body)
    # early-return proof: the POST has fully returned WHILE page 1 is
    # parked (waiters>=1) — the run exists but no data page has returned.
    snap = wait_barrier_parked(ctx["src"])
    ev["parked_at_admission"] = {"waiters": snap["barrier_waiters"],
                                 "active": snap["active"],
                                 "pages": snap["request_count"]}
    M.expect(admission_elapsed < 30, "D: admission not early (barrier held)",
             {"elapsed_s": admission_elapsed})

    # baseline row once running; heartbeat must advance (compared by
    # Postgres, baseline non-empty — the accepted P2.1 A gate).
    row0 = wait_run_row(run_id, timeout=30)
    M.expect(row0["status"] in RUNNING_STATES, "D: run not running while parked",
             row0["status"])
    M.expect(row0["heartbeat"], "D: heartbeat baseline never initialized",
             {"row": row0})
    # the window: heartbeat advanced AND the pull continuously in flight
    # for >= INFLIGHT_MIN_S after admission — only then may we release.
    INFLIGHT_MIN_S = 35
    advanced_at = None
    continuously_inflight = True
    while True:
        snap = fixture_snap(ctx["src"])
        if snap.get("active", 0) < 1 or snap.get("barrier_waiters", 0) < 1:
            continuously_inflight = False
            break
        if advanced_at is None:
            old = row0["heartbeat"].replace("'", "''")
            got = psql_connectors(
                f"SELECT (heartbeat_at > '{old}'::timestamptz) "
                f"FROM sync_runs WHERE id = '{run_id}'::uuid;")
            if got.strip() == "t":
                advanced_at = time.monotonic()
        if advanced_at is not None \
                and time.monotonic() - t_admit >= INFLIGHT_MIN_S:
            break
        if time.monotonic() - t_admit > 150:
            break  # bounded: loop below fails with the real numbers
        time.sleep(1.0)
    inflight_s = round(time.monotonic() - t_admit, 1)
    ev["heartbeat_advanced_at_s"] = round(advanced_at - t_admit, 1) \
        if advanced_at else None
    ev["inflight_s"] = inflight_s
    ev["continuously_inflight"] = continuously_inflight
    M.expect(advanced_at is not None and continuously_inflight
             and inflight_s >= INFLIGHT_MIN_S,
             "D: window proof failed — need heartbeat advance AND "
             f"continuous inflight >= {INFLIGHT_MIN_S}s after admission",
             {"heartbeat_advanced_at_s": ev["heartbeat_advanced_at_s"],
              "inflight_s": inflight_s,
              "continuously_inflight": continuously_inflight})
    ev["still_running_at_release"] = run_row(run_id)["status"]
    M.expect(ev["still_running_at_release"] in RUNNING_STATES,
             "D: run left running during the window",
             ev["still_running_at_release"])

    # release; the SAME run completes with real content.
    barrier_release(ctx["src"])
    final, st = poll_run(owner.token, ctx["sync_id"], run_id,
                         until=lambda r: r.get("status") == "succeeded",
                         timeout=120)
    M.expect(final is not None and final.get("status") == "succeeded",
             "D: run never reached succeeded after release", {"last_status": st})
    assert_no_private_keys(final, "D final run GET")
    # new runs are CONTROLLED (public field, ROOT-APPROVED vocabulary)
    ev["control_mode"] = final.get("control_mode")
    M.expect(final.get("control_mode") == "controlled",
             "D: fresh async run is not control_mode=controlled",
             {"control_mode": final.get("control_mode")})
    row = run_row(run_id)
    ev["db_status"] = row["status"]
    M.expect(row["status"] == "succeeded", "D: DB not succeeded", row["status"])
    file_row = prove_committed_artifact(owner.token, ctx["target"], final,
                                        total=151, ev=ev, label="D")
    SESSION["completed_run"] = {"run_id": run_id, "ctx": ctx,
                                "txn_id": file_row.get("transaction_id")}


# ------------------------------------------------------------------ E -------
def scenario_e(suffix, ev):
    """(root 3) cancel a run executed by A via HTTP on B (connector2):
    DB observation, not a local map. Persistent terminal + frozen pages
    + zero publish + idempotent duplicate."""
    if "connector2" not in M.BASES:
        raise AssertionError("E: connector2 not available — cross-process "
                             "proof needs the replica (p22_connector2.sh)")
    ctx = owned_org_sync(f"p22e-{suffix}")
    owner = ctx["owner"]
    barrier_set(ctx["src"], [2], wait_s=180)  # page 1 fast, page 2 parks

    status, body = trigger_async(owner.token, ctx["sync_id"], timeout=30)
    M.expect(status == ADMIT_STATUS, "E: async trigger != 202", {"status": status})
    run_id = extract_run_id(body)
    wait_barrier_parked(ctx["src"])  # page 2 provably parked, page 1 served
    pages_at_cancel = fixture_count(ctx["src"])
    M.expect(pages_at_cancel == 2, "E: expected exactly 2 pages before cancel",
             {"pages": pages_at_cancel})

    # cancel through the SECOND process, owner principal: the cancel and
    # the executing worker live in different processes. HANDLER-VERIFIED:
    # a running cancel answers 202 {run} with the REAL run still running.
    res = cancel_run(owner.token, ctx["sync_id"], run_id, via="connector2")
    ev["cancel_status"], ev["cancel_via"] = res.status, "connector2"
    ev["cancel_code"] = (res.body or {}).get("code") if isinstance(res.body, dict) else None
    assert_no_private_keys(res.body, "E cancel response")
    M.expect(res.status == CANCEL_RUNNING_STATUS,
             "E: running cancel via connector2 != 202", {"status": res.status})
    run_in_body = cancel_body_run(res.body, "E 202")
    M.expect(run_in_body.get("id") == run_id,
             "E: 202 body.run is not the real run", {"id": run_in_body.get("id")})
    M.expect(run_in_body.get("status") in RUNNING_STATES,
             "E: 202 body.run claims the run already left running",
             {"status": run_in_body.get("status")})

    # believed ONLY from the persisted row.
    final, st = poll_run(owner.token, ctx["sync_id"], run_id,
                         until=lambda r: r.get("status") in CANCEL_TERMINAL_CANDIDATES,
                         timeout=90)
    M.expect(final is not None and final.get("status") in CANCEL_TERMINAL_CANDIDATES,
             "E: run never reached a persisted cancel-terminal state",
             {"last_status": st})
    pin_cancelled_vocabulary(final.get("status"))
    ev["cancelled_status"] = final.get("status")
    row = run_row(run_id)
    ev["db_status"], ev["db_cancel_requested"] = row["status"], row["cancel_requested"]
    M.expect(row["status"] == final.get("status"),
             "E: DB status != public status",
             {"db": row["status"], "public": final.get("status")})
    M.expect(row["cancel_requested"], "E: cancel_requested_at/by never persisted")

    # release the parked page; the aborted worker must NOT fetch page 3
    # and must NOT publish (deterministic — no sleep-based guessing).
    barrier_release(ctx["src"])
    settle, _ = poll_run(owner.token, ctx["sync_id"], run_id,
                         until=lambda r: True, timeout=5)  # settle beat
    ev["pages_after_release"] = fixture_count(ctx["src"])
    M.expect(ev["pages_after_release"] == pages_at_cancel,
             "E: pages were fetched after the cancel",
             {"at_cancel": pages_at_cancel, "after": ev["pages_after_release"]})
    files = M.list_files(owner.token, ctx["target"]["id"])
    ev["target_files"] = len(files)
    M.expect(not files, "E: target received published files after the cancel", files)

    # duplicate cancel on a TERMINAL run: HANDLER-VERIFIED 200 {run}
    # (idempotent no-op), and the terminal state must not flip,
    # resurrect, or pull again.
    res2 = cancel_run(owner.token, ctx["sync_id"], run_id)
    ev["duplicate_cancel_status"] = res2.status
    assert_no_private_keys(res2.body, "E duplicate cancel response")
    M.expect(res2.status == CANCEL_TERMINAL_STATUS,
             "E: duplicate cancel on terminal run != 200",
             {"status": res2.status})
    dup_run = cancel_body_run(res2.body, "E duplicate")
    M.expect(dup_run.get("id") == run_id
             and dup_run.get("status") == final.get("status"),
             "E: duplicate cancel body.run disagrees with the terminal run",
             {"body_status": dup_run.get("status"),
              "run_status": final.get("status")})
    row2 = run_row(run_id)
    M.expect(row2["status"] == row["status"],
             "E: duplicate cancel changed the terminal state",
             {"first": row["status"], "after_duplicate": row2["status"]})
    M.expect(fixture_count(ctx["src"]) == pages_at_cancel,
             "E: duplicate cancel caused further pulls")
    M.expect(not M.list_files(owner.token, ctx["target"]["id"]),
             "E: duplicate cancel resurrected a publish")
    SESSION["cancelled_run"] = {"run_id": run_id, "ctx": ctx}


# ------------------------------------------------------------------ F -------
def scenario_f(suffix, ev):
    """(root 2) pending cancel with zero source pulls + queuefull reject
    without orphans. Requires the root-pinned QUEUE_CONTRACT."""
    if QUEUE_CONTRACT is None:
        raise AssertionError("F: QUEUE_CONTRACT not pinned by root — refusing "
                             "to guess the bounded-queue branch (see README)")
    if QUEUE_CONTRACT == "reject":
        ctx = owned_org_sync(f"p22f-{suffix}")
        owner = ctx["owner"]
        barrier_set(ctx["src"], [1], wait_s=180)
        status1, body1 = trigger_async(owner.token, ctx["sync_id"], timeout=30)
        M.expect(status1 == ADMIT_STATUS, "F: first trigger != 202",
                 {"status": status1})
        run1 = extract_run_id(body1)
        wait_barrier_parked(ctx["src"])  # worker provably occupied
        runs_before = snapshot_runs(owner.token, ctx["sync_id"])
        status2, body2 = trigger_async(owner.token, ctx["sync_id"], timeout=30)
        ev["overflow_status"] = status2
        assert_no_private_keys(body2, "F overflow response")
        M.expect(status2 == QUEUEFULL_STATUS,
                 "F: queue-full overflow was admitted (silent pending) or "
                 "answered an unexpected status",
                 {"status": status2, "want": QUEUEFULL_STATUS})
        M.expect(snapshot_runs(owner.token, ctx["sync_id"]) == runs_before,
                 "F: overflow left an orphan run row")
        barrier_release(ctx["src"])
        poll_run(owner.token, ctx["sync_id"], run1,
                 until=lambda r: r.get("status") in TERMINAL_STATES, timeout=120)
        return

    depth = QUEUE_CONTRACT.get("depth")
    M.expect(isinstance(depth, int) and depth >= 1,
             "F: QUEUE_CONTRACT depth must be >= 1 in this branch")
    ctx = owned_org_sync(f"p22f-{suffix}")            # occupies the worker
    owner = ctx["owner"]
    barrier_set(ctx["src"], [1], wait_s=180)
    status1, body1 = trigger_async(owner.token, ctx["sync_id"], timeout=30)
    M.expect(status1 == ADMIT_STATUS, "F: first trigger != 202", {"status": status1})
    run1 = extract_run_id(body1)
    wait_barrier_parked(ctx["src"])

    ctx2 = owned_org_sync(f"p22f2-{suffix}", owner=owner)  # same tenant
    status2, body2 = trigger_async(owner.token, ctx2["sync_id"], timeout=30)
    ev["second_admission"] = status2
    M.expect(status2 == ADMIT_STATUS, "F: queued trigger != 202", {"status": status2})
    run2 = extract_run_id(body2)
    pending, _ = poll_run(owner.token, ctx2["sync_id"], run2,
                          until=lambda r: r.get("status") in RUNNING_STATES,
                          timeout=20)
    ev["pending_status"] = (pending or {}).get("status")
    M.expect(ev["pending_status"] in RUNNING_STATES,
             "F: second run never observed pending/running",
             {"status": ev["pending_status"]})
    M.expect(fixture_count(ctx2["src"]) == 0,
             "F: pending task fetched pages before any worker started it")

    # pending cancel -> HANDLER-VERIFIED 200 {run} ALREADY aborted; the
    # task never started: ZERO pages EVER for its src, no publish.
    res = cancel_run(owner.token, ctx2["sync_id"], run2)
    ev["pending_cancel_status"] = res.status
    assert_no_private_keys(res.body, "F pending cancel response")
    M.expect(res.status == CANCEL_PENDING_STATUS,
             "F: pending cancel != 200+run", {"status": res.status})
    pend_run = cancel_body_run(res.body, "F pending cancel")
    M.expect(pend_run.get("id") == run2
             and pend_run.get("status") in CANCEL_TERMINAL_CANDIDATES,
             "F: pending cancel body.run is not the aborted run",
             {"body_status": pend_run.get("status")})
    pin_cancelled_vocabulary(pend_run.get("status"))
    M.expect(fixture_count(ctx2["src"]) == 0,
             "F: pending-cancelled task fetched pages at any point")
    M.expect(not M.list_files(owner.token, ctx2["target"]["id"]),
             "F: pending-cancelled task published files")
    # duplicate cancel idempotent (terminal -> 200 {run}).
    res2 = cancel_run(owner.token, ctx2["sync_id"], run2)
    M.expect(res2.status == CANCEL_TERMINAL_STATUS
             and cancel_body_run(res2.body, "F duplicate").get("status")
             == pend_run.get("status")
             and run_row(run2)["status"] == pend_run.get("status"),
             "F: duplicate pending cancel flipped the terminal state")

    # queuefull without orphans: CONNECTOR_SYNC_ASYNC_QUEUE is the TOTAL
    # admission capacity (queued + running, sync + async combined) — so
    # with one run parked on the barrier, fills continue until the TOTAL
    # is reached and the next admission is an explicit 503. The dynamic
    # admit-until-reject loop is robust to any off-by-one in how the
    # total is counted; every rejection must leave its fresh sync with
    # ZERO run rows.
    fills = []
    reject_seen = None
    cap = depth + 3
    for i in range(cap):
        fctx = owned_org_sync(f"p22f-fill{i}-{suffix}", owner=owner)
        st_i, body_i = trigger_async(owner.token, fctx["sync_id"], timeout=30)
        ev.setdefault("fill_statuses", []).append(st_i)
        if st_i >= 400:
            assert_no_private_keys(body_i, "F queuefull response")
            M.expect(st_i == QUEUEFULL_STATUS,
                     "F: overflow answered an unexpected status "
                     "(handler contract: 503 before any durable row)",
                     {"status": st_i})
            M.expect(isinstance(body_i, dict) and body_i.get("error"),
                     "F: queuefull 503 carries no error message")
            M.expect(snapshot_runs(owner.token, fctx["sync_id"]) == [],
                     "F: rejected overflow left an orphan run row")
            reject_seen = st_i
            break
        M.expect(st_i == ADMIT_STATUS, f"F: fill trigger {i} neither admitted "
                 "nor explicitly rejected", {"status": st_i})
        fills.append((fctx, extract_run_id(body_i)))
    ev["queuefull_status"] = reject_seen
    ev["fills_admitted"] = len(fills)
    M.expect(reject_seen is not None,
             "F: no explicit queue-full rejection within capacity+2 triggers")

    # cleanup: cancel everything so later scenarios meet a free worker;
    # a single global deadline bounds the wait over all handles.
    barrier_release(ctx["src"])
    handles = [(ctx, run1)] + fills
    for fctx, rid in handles:
        cancel_run(owner.token, fctx["sync_id"], rid)
    deadline = time.monotonic() + 240
    remaining = {rid for _f, rid in handles}
    while remaining and time.monotonic() < deadline:
        for fctx, rid in handles:
            if rid in remaining:
                try:
                    body, _ = poll_run(owner.token, fctx["sync_id"], rid,
                                       until=lambda r: r.get("status") in TERMINAL_STATES,
                                       timeout=2)
                except Exception:
                    body = None
                if body is not None:
                    remaining.discard(rid)
        time.sleep(1.0)
    ev["cleanup_left_running"] = len(remaining)


# ------------------------------------------------------------------ G -------
def scenario_g(suffix, ev):
    """(root 5) gate-wins too_late with a REAL Dataset commit and a held
    RESPONSE. The P2.2 executor has NO DB scanning: an async admission on
    connector3 belongs to connector3's own queue, so the run executes
    there WITHOUT stopping any other replica (review fix 2 — stopping
    base/connector2 would only break the default-via get/cancel/poll
    paths). The cancel is issued through the BASE connector instance,
    which doubles as the cross-instance cancel proof."""
    missing = [k for k in ("connector3", "dataset_proxy") if k not in M.BASES]
    if missing:
        raise AssertionError("G: needs connector3 + dataset-proxy replicas "
                             f"(missing {missing}); start them with "
                             "p22_connector2.sh (aux-only, --no-deps)")
    ctx = owned_org_sync(f"p22g-{suffix}", total=1, page_size=1)
    owner = ctx["owner"]
    try:
        proxy_hold(hid=f"p22g-{suffix}", method="POST",
                   path_suffix="/outputs:commit",
                   dataset_id=ctx["target"]["id"], max_hold_s=180)
        # admission VIA connector3 => the task lives in connector3's own
        # bounded queue (no DB scan for other instances to claim it)
        status, body = trigger_async(owner.token, ctx["sync_id"],
                                     timeout=30, via="connector3")
        M.expect(status == ADMIT_STATUS, "G: async trigger != 202",
                 {"status": status})
        run_id = extract_run_id(body)
        # the REAL Dataset has processed the commit while the response is
        # being withheld from the connector.
        hold = wait_proxy_held(f"p22g-{suffix}", timeout=90)
        ev["proxy_held"] = {"matched": hold["matched"], "held_now": hold["held_now"]}
        # gate-entry observed via the ROOT-APPROVED publish_entered_at
        # (public field; DB whitelist projection checks NOT NULL).
        row = run_row(run_id)
        ev["gate_entered_at_hold"] = row["gate_entered"]
        M.expect(row["gate_entered"],
                 "G: publish_entered_at not set while the commit response "
                 "is held — the gate proof fails")
        held_run = get_run(owner.token, ctx["sync_id"], run_id)
        ev["public_publish_entered_at"] = (held_run.body or {}).get("publish_entered_at") \
            if held_run.status == 200 else None
        # cancel inside the held window => HANDLER-VERIFIED 409 with
        # code=cancel_too_late and the REAL run in body.run.
        res = cancel_run(owner.token, ctx["sync_id"], run_id)
        ev["cancel_status"] = res.status
        ev["cancel_code"] = (res.body or {}).get("code") \
            if isinstance(res.body, dict) else None
        assert_no_private_keys(res.body, "G cancel response")
        M.expect(res.status == TOO_LATE_STATUS
                 and ev["cancel_code"] == TOO_LATE_CODE,
                 "G: cancel during held commit did not answer 409 "
                 "code=cancel_too_late",
                 {"status": res.status, "code": ev["cancel_code"]})
        M.expect(cancel_body_run(res.body, "G 409").get("id") == run_id,
                 "G: 409 body.run is not the real run",
                 {"id": cancel_body_run(res.body, "G 409").get("id")})
        # release: the connector receives the REAL committed response and
        # the run SUCCEEDS — nothing is relabelled aborted.
        proxy_release(hid=f"p22g-{suffix}")
        final, st = poll_run(owner.token, ctx["sync_id"], run_id,
                             until=lambda r: r.get("status") == "succeeded",
                             timeout=90)
        M.expect(final is not None, "G: run never succeeded after release",
                 {"last_status": st})
        row = run_row(run_id)
        M.expect(row["status"] == "succeeded",
                 "G: DB not succeeded after released commit", row["status"])
        file_row = prove_committed_artifact(owner.token, ctx["target"], final,
                                            total=1, ev=ev, label="G")
        SESSION["gate_run"] = {"run_id": run_id, "ctx": ctx,
                               "txn_id": file_row.get("transaction_id")}
    finally:
        proxy_release(all_=True)


# ------------------------------------------------------------------ H -------
def scenario_h(suffix, ev):
    """(root 4) denial matrix + snapshot invariance after every attack."""
    ctx = owned_org_sync(f"p22h-{suffix}")
    owner = ctx["owner"]
    barrier_set(ctx["src"], [1], wait_s=180)
    status, body = trigger_async(owner.token, ctx["sync_id"], timeout=30)
    M.expect(status == ADMIT_STATUS, "H: async trigger != 202", {"status": status})
    run_id = extract_run_id(body)
    wait_barrier_parked(ctx["src"])

    ctx2 = owned_org_sync(f"p22h2-{suffix}", owner=owner)  # same tenant
    status2, body2 = trigger_async(owner.token, ctx2["sync_id"], timeout=30)
    M.expect(status2 == ADMIT_STATUS, "H: second trigger != 202", {"status": status2})
    run2_id = extract_run_id(body2)

    other_org = str(uuid.uuid4())
    # Review fix 1: the SAME SUBJECT (owner.sub) holding a token for a
    # DIFFERENT org — a random sub would prove nothing about dual-org
    # semantics.
    same_sub_other_org = M.Identity("p22h-dual", sub=owner.sub, org=other_org)
    cross_org_admin = M.Identity("p22h-xadmin", org=other_org, roles=["admin"])
    personal = M.Identity("p22h-personal", org=None)
    # same ORG, different member, WITHOUT dataset.write: the Source-write
    # permission requirement must deny this principal.
    nowrite_sub = str(uuid.uuid4())
    same_org_no_write = type("I", (), {
        "sub": nowrite_sub, "org": ctx["org"],
        "token": M.mint_token(sub=nowrite_sub, org=ctx["org"],
                              permissions=[], ttl=3600)})()

    runs_before = snapshot_runs(owner.token, ctx["sync_id"])
    files_before = sorted(f.get("id")
                          for f in M.list_files(owner.token, ctx["target"]["id"]))

    def deny(label, fn):
        res = fn()
        ev[label] = res.status
        assert_no_private_keys(res.body, f"H {label}")
        M.expect(res.status in DENY_STATUSES,
                 f"H: {label} was not denied", {"status": res.status})
        M.expect(snapshot_runs(owner.token, ctx["sync_id"]) == runs_before,
                 f"H: runs list changed after {label}")
        now_files = sorted(f.get("id")
                           for f in M.list_files(owner.token, ctx["target"]["id"]))
        M.expect(now_files == files_before, f"H: target files changed after {label}")

    deny("same_sub_cross_org_cancel",
         lambda: cancel_run(same_sub_other_org.token, ctx["sync_id"], run_id))
    deny("same_sub_cross_org_run_get",
         lambda: get_run(same_sub_other_org.token, ctx["sync_id"], run_id))
    deny("cross_org_admin_cancel",
         lambda: cancel_run(cross_org_admin.token, ctx["sync_id"], run_id))
    deny("personal_cancel",
         lambda: cancel_run(personal.token, ctx["sync_id"], run_id))
    deny("same_org_no_write_cancel",
         lambda: cancel_run(same_org_no_write.token, ctx["sync_id"], run_id))
    deny("wrong_parent_cancel",
         lambda: cancel_run(owner.token, ctx2["sync_id"], run_id))
    deny("wrong_parent_run_get",
         lambda: get_run(owner.token, ctx2["sync_id"], run_id))
    res = cancel_run(owner.token, ctx["sync_id"], "not-a-uuid")
    ev["malformed_run_id"] = res.status
    M.expect(res.status == MALFORMED_ID_STATUS or res.status in DENY_STATUSES,
             "H: malformed run id not rejected", {"status": res.status})

    # liveness: the attacked (parked, running) run is still cancellable
    # by its owner — HANDLER-VERIFIED 202 {run}, run still running.
    res = cancel_run(owner.token, ctx["sync_id"], run_id)
    M.expect(res.status == CANCEL_RUNNING_STATUS,
             "H: post-attack legit cancel != 202+run", {"status": res.status})
    M.expect(cancel_body_run(res.body, "H liveness").get("status")
             in RUNNING_STATES,
             "H: liveness cancel body.run not running")
    final, _ = poll_run(owner.token, ctx["sync_id"], run_id,
                        until=lambda r: r.get("status") in CANCEL_TERMINAL_CANDIDATES,
                        timeout=90)
    M.expect(final is not None, "H: attacked run never cancelled")
    pin_cancelled_vocabulary(final.get("status"))
    barrier_release(ctx["src"])
    # cleanup ctx2 (its worker slot frees when the pending task cancels)
    cancel_run(owner.token, ctx2["sync_id"], run2_id)
    poll_run(owner.token, ctx2["sync_id"], run2_id,
             until=lambda r: r.get("status") in TERMINAL_STATES, timeout=90)


# ------------------------------------------------------------------ I -------
def scenario_i(suffix, ev):
    """(root 6a) SIGTERM with the run parked on the barrier (pre-gate):
    the controlled stop uses a GENEROUS grace (120s >> the service's
    ~75s stop budget) so a plain rc=0 can never masquerade as a join;
    exit is verified via WHITELISTED inspect fields only (Running/
    ExitCode/OOMKilled/FinishedAt — never a full inspect with Env). The
    blocked run must be GENUINELY terminalized with zero new pages/
    artifacts; the STABLE baseline is taken after the stop settles and
    BEFORE the restart; after restart the SAME full safe projection
    (status/control/attempt/generation/cancel/stage/hash/version/time)
    and all counts must be unchanged. Only the NAMED connector replicas
    are stopped — the fixture (its counters are evidence) never is."""
    ctx = owned_org_sync(f"p22i-{suffix}", timeout_ms=60000)
    owner = ctx["owner"]
    barrier_set(ctx["src"], [1], wait_s=180)
    status, body = trigger_async(owner.token, ctx["sync_id"], timeout=30)
    M.expect(status == ADMIT_STATUS, "I: async trigger != 202", {"status": status})
    run_id = extract_run_id(body)
    wait_barrier_parked(ctx["src"])

    replicas = connector_replicas()
    # cache the real container ids BEFORE the stop (review fix B): a
    # stopped container is not listed by a default `ps -q`, and the aux
    # names only exist in the override file — the module compose() already
    # merges the override whenever it is rendered.
    ev["replica_cids"] = {}
    for svc in replicas:
        cid = compose("ps", "-q", svc).stdout.strip().splitlines()
        M.expect(cid, f"I: no container id resolved for {svc} before stop")
        ev["replica_cids"][svc] = cid[0]
    stop = compose("stop", "-t", "120", *replicas)
    ev["stop_rc"] = stop.returncode
    M.expect(stop.returncode == 0, "I: docker stop failed",
             M.scrub(stop.stderr)[:200])
    # whitelisted exit verification per cached container (no full Env)
    ev["replica_exits"] = {}
    for svc in replicas:
        ins = subprocess.run(["docker", "inspect", "--format",
                              "{{.State.Running}}|{{.State.ExitCode}}|"
                              "{{.State.OOMKilled}}|{{.State.FinishedAt}}",
                              ev["replica_cids"][svc]],
                             capture_output=True, text=True,
                             timeout=60).stdout.strip()
        running, exit_code, oom, finished_at = ins.split("|")
        ev["replica_exits"][svc] = {"exit_code": exit_code, "oom": oom,
                                    "finished_at": finished_at}
        M.expect(running == "false" and exit_code == "0" and oom == "false",
                 f"I: {svc} did not shut down gracefully (whitelisted inspect)",
                 ev["replica_exits"][svc])

    # processes are confirmed out: release the parked page now (its
    # waiter belongs to a dead client) and WAIT for the fixture to drain
    # so the release-caused counter change settles BEFORE the baseline —
    # it must never pollute the restart comparison.
    barrier_release(ctx["src"])
    drain_deadline = time.monotonic() + 15
    while time.monotonic() < drain_deadline:
        snap = fixture_snap(ctx["src"])
        if snap.get("active", 0) == 0 and snap.get("barrier_waiters", 0) == 0:
            break
        time.sleep(0.5)
    snap = fixture_snap(ctx["src"])
    M.expect(snap.get("active", 0) == 0 and snap.get("barrier_waiters", 0) == 0,
             "I: fixture did not drain after the release (post-stop)",
             {"active": snap.get("active"), "waiters": snap.get("barrier_waiters")})
    row_after_stop = run_row(run_id)
    ev["row_after_stop"] = {k: row_after_stop[k] for k in
                            ("status", "control_mode", "gate_entered")}
    M.expect(row_after_stop["status"] in ("aborted", "failed"),
             "I: the pre-gate blocked run was not genuinely terminalized "
             "by the graceful shutdown", row_after_stop["status"])
    ev["pages_after_stop"] = fixture_count(ctx["src"])
    M.expect(ev["pages_after_stop"] == 1,
             "I: pages were fetched after SIGTERM",
             {"after": ev["pages_after_stop"]})
    M.expect(not M.list_files(owner.token, ctx["target"]["id"]),
             "I: target received files after SIGTERM")

    # STABLE baseline after the stop settled and the fixture drained,
    # BEFORE the restart — runs come from PG (all connectors are down;
    # dataset/fixture remain reachable).
    baseline = {
        "pages": ev["pages_after_stop"],
        "files": sorted(f.get("id") for f in
                        M.list_files(owner.token, ctx["target"]["id"])),
        "runs": pg_run_ids(ctx["sync_id"]),
        "projection": run_row(run_id),
        "legacy": {k: run_row(v) for k, v in P21_LEGACY_RUNS.items()},
    }

    restart = compose("start", *replicas)
    ev["restart_rc"] = restart.returncode
    M.expect(restart.returncode == 0, "I: restart failed",
             M.scrub(restart.stderr)[:200])
    M.wait_health()
    time.sleep(45)  # >= 2 heartbeat periods of observation
    ev["pages_after_restart"] = fixture_count(ctx["src"])
    M.expect(ev["pages_after_restart"] == baseline["pages"],
             "I: restart re-dispatched the interrupted run",
             {"baseline": baseline["pages"], "after": ev["pages_after_restart"]})
    M.expect(pg_run_ids(ctx["sync_id"]) == baseline["runs"],
             "I: new runs appeared after restart")
    now = run_row(run_id)
    ev["row_after_restart"] = {k: now[k] for k in
                               ("status", "control_mode", "gate_entered",
                                "attempt", "generation")}
    M.expect(now == baseline["projection"],
             "I: interrupted run's full safe projection changed across restart",
             {"before": baseline["projection"], "after": now})
    for key, legacy_id in P21_LEGACY_RUNS.items():
        M.expect(run_row(legacy_id) == baseline["legacy"][key],
                 f"I: P2.1 legacy run {key} was touched by the P2.2 restart",
                 {"before": baseline["legacy"][key],
                  "after": run_row(legacy_id)})
    SESSION["interrupted_run"] = {"run_id": run_id, "ctx": ctx}


# ------------------------------------------------------------------ J -------
def scenario_j(suffix, ev):
    """(root 6b) task deadline, DISCRIMINATED from a plain source timeout
    (review fix 4): a dedicated aux rebuild of connector2 ONLY (--no-deps,
    interlocked, live-task-guarded via the driver) runs with
    CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS=7 while the source HTTP timeout
    (60s) and the auth TTL (300s) stay far longer. A terminal arriving at
    TTL scale (<50s, far under the 60s source timeout) WITH a deadline-
    ish failure reason is then provably the task deadline. Config is read
    back from the non-sensitive override file (whitelist keys only); the
    aux is restored in finally. No prod/test endpoint is involved."""
    if DEADLINE_CONTRACT is None:
        raise AssertionError("J: DEADLINE_CONTRACT not pinned by root — "
                             "refusing to guess deadline semantics (see README)")
    DEADLINE_AUX_TTL_S = 7
    driver = Path(__file__).resolve().parent / "p22_connector2.sh"

    def drv(*args):
        return subprocess.run(["sh", str(driver), *args],
                              capture_output=True, text=True, timeout=300)

    up = drv("up-deadline", str(DEADLINE_AUX_TTL_S))
    ev["deadline_up_rc"] = up.returncode
    M.expect(up.returncode == 0, "J: deadline aux up failed",
             M.scrub((up.stderr or up.stdout))[:300])
    override_file = Path(ARGS.env_dir) / "compose-p22-deadline.yaml"
    ev["deadline_env"] = {}
    for line in override_file.read_text().splitlines():
        if "CONNECTOR_SYNC_ASYNC" in line and ":" in line:
            key, value = line.split(":", 1)
            ev["deadline_env"][key.strip()] = value.strip().strip('"')
    M.expect(ev["deadline_env"].get("CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS")
             == str(DEADLINE_AUX_TTL_S),
             "J: deadline override does not carry the expected TTL",
             ev["deadline_env"])
    # capture the deadline container's boot so the restore can be proven
    # to have ACTUALLY recreated it (review fix D; fail-closed capture)
    cid2 = compose("ps", "-q", "connector2").stdout.strip().splitlines()
    M.expect(cid2, "J: no connector2 container after up-deadline")
    deadline_boot = inspect_started_at(cid2[0], "J deadline aux")
    ev["deadline_boot_started_at"] = deadline_boot
    try:
        ctx = owned_org_sync(f"p22j-{suffix}", timeout_ms=60000)
        owner = ctx["owner"]
        barrier_set(ctx["src"], [1], wait_s=90)
        status, body = trigger_async(owner.token, ctx["sync_id"],
                                     timeout=30, via="connector2")
        M.expect(status == ADMIT_STATUS, "J: async trigger != 202",
                 {"status": status})
        run_id = extract_run_id(body)
        wait_barrier_parked(ctx["src"])
        t0 = time.monotonic()
        final, st = poll_run(owner.token, ctx["sync_id"], run_id,
                             until=lambda r: r.get("status") in TERMINAL_STATES,
                             timeout=DEADLINE_AUX_TTL_S * 4)
        elapsed = round(time.monotonic() - t0, 1)
        ev["terminal_elapsed_s"] = elapsed
        ev["deadline_terminal"] = (final or {}).get("status")
        M.expect(final is not None, "J: run never terminalized near the task TTL",
                 {"last_status": st})
        M.expect(elapsed < 50,
                 "J: terminalized at source-timeout scale (>=50s vs 60s source "
                 "HTTP timeout), not task-TTL scale",
                 {"elapsed_s": elapsed})
        M.expect(elapsed >= DEADLINE_AUX_TTL_S - 2,
                 "J: terminalized before the TTL could possibly fire",
                 {"elapsed_s": elapsed, "ttl": DEADLINE_AUX_TTL_S})
        M.expect(final.get("status") != "succeeded",
                 "J: deadline-exceeded run must not succeed",
                 final.get("status"))
        row = run_row(run_id)
        M.expect(row["status"] == final.get("status"), "J: DB != public terminal",
                 {"db": row["status"], "public": final.get("status")})
        detail = M.sync_failure_detail(final)
        ev["failure_detail"] = detail[:200]
        # ROOT-PINNED wording (2026-11-14): rest_snapshot.go's
        # DeadlineExceeded branch renders the FIXED format
        #   "rest sync: upstream request to host %s timed out"
        # — for this scenario the host is always fixture:8090. The full
        # known fragment is asserted (never a bare any-timeout match),
        # alongside the numeric discriminators (elapsed >= TTL-2, < 50s
        # vs the 60s source budget, TTL env verified at 7s/300s) and the
        # restore strong-verification in finally.
        ev["deadline_fragment"] = (
            "rest sync: upstream request to host fixture:8090 timed out")
        M.expect(ev["deadline_fragment"] in detail,
                 "J: failure detail lacks the ROOT-PINNED deadline fragment "
                 "(rest_snapshot.go DeadlineExceeded fmt)",
                 {"detail": detail[:200]})
        # release: no zombie pulls after terminalization
        barrier_release(ctx["src"])
        time.sleep(3)
        ev["pages_after_release"] = fixture_count(ctx["src"])
        M.expect(ev["pages_after_release"] == 1,
                 "J: pages were fetched after deadline terminalization",
                 {"after": ev["pages_after_release"]})
        M.expect(not M.list_files(owner.token, ctx["target"]["id"]),
                 "J: deadline-exceeded run published files")
    finally:
        # review fix D: the restore is itself VERIFIED — rc==0, healthz
        # green, the container actually recreated (new StartedAt), and
        # the effective (non-sensitive) env whitelist shows the deadline
        # override GONE from the merged config. A stale 7s container that
        # merely answers healthz must not pass.
        restore = drv("restore")
        ev["restore_rc"] = restore.returncode
        M.expect(restore.returncode == 0, "J: aux restore command failed",
                 M.scrub((restore.stderr or restore.stdout))[:300])
        import urllib.request as _u
        healthy = False
        deadline_wait = time.monotonic() + 60
        while time.monotonic() < deadline_wait:
            try:
                with _u.urlopen(ARGS.connector2_base + "/healthz", timeout=3) as r:
                    if r.status == 200:
                        healthy = True
                        break
            except Exception:
                time.sleep(1.0)
        ev["aux_restored_healthy"] = healthy
        M.expect(healthy, "J: connector2 not healthy after restore")
        cid2b = compose("ps", "-q", "connector2").stdout.strip().splitlines()
        M.expect(cid2b, "J: no connector2 container after restore")
        restored_boot = inspect_started_at(cid2b[0], "J restored aux")
        ev["restored_boot_started_at"] = restored_boot
        M.expect(restored_boot != deadline_boot,
                 "J: connector2 was NOT recreated by the restore (same boot "
                 "timestamp — the 7s-TTL container may still be running)",
                 {"deadline_boot": deadline_boot, "restored_boot": restored_boot})
        # effective config: rc must be 0 AND the merged output must really
        # describe the aux services — a failed `config` with empty stdout
        # must never reduce the TTL check to a vacuous pass.
        cfg = compose("config")
        M.expect(cfg.returncode == 0, "J: compose config failed after restore",
                 M.scrub(cfg.stderr)[:200])
        cfg_lines = cfg.stdout.splitlines()
        M.expect(any(line.strip() == "connector2:" for line in cfg_lines),
                 "J: restored merged config lacks the connector2 service")
        ev["restored_env_lines"] = [line.strip() for line in cfg_lines
                                    if "CONNECTOR_SYNC_ASYNC" in line]
        # whitelist TTL parse: absent => the code default (300) applies;
        # present => it must be the DEFAULT (300), never the 7s aux value
        ttl_value = None
        for line in ev["restored_env_lines"]:
            if "TASK_TTL_SECS" in line:
                match = re.search(
                    r'CONNECTOR_SYNC_ASYNC_TASK_TTL_SECS:?\s*"?(\d+)"?', line)
                if match:
                    ttl_value = int(match.group(1))
        ev["restored_task_ttl"] = ttl_value
        ev["restored_task_ttl_interp"] = (ttl_value if ttl_value is not None
                                          else "default-300 (no explicit env)")
        M.expect(ttl_value != DEADLINE_AUX_TTL_S,
                 "J: effective config still carries the deadline TTL — "
                 "the default aux configuration was not restored",
                 {"ttl": ttl_value})
        M.expect(ttl_value is None or ttl_value == 300,
                 "J: restored TASK_TTL is neither absent (default 300) "
                 "nor the default value", {"ttl": ttl_value})


# ------------------------------------------------------------------ K -------
def scenario_k(suffix, ev):
    """(root 6c) DEFAULT synchronous POST with page 1 parked; the CLIENT
    disconnects. A disconnect is NOT the cancel API, and a run that
    simply IGNORED the disconnect would still fail at the 60s source
    timeout — so the proof is TIMED and REASON-PRECISE (review fix C):
    the terminal must arrive clearly under the routing/source budgets
    (<=15s after the disconnect), and when the state is failed the
    reason must be a real request-context cancellation ("context
    canceled"/"request canceled" family), never an arbitrary non-empty
    string. The POST keeps a real body ({}); a bodyless variant that
    dodges the unread-body cancel path is a separate root-side TCP
    verification, not this scenario. If K fails it is reported as-is —
    never loosened to the source-timeout outcome."""
    CONTEXT_CANCEL_MARKERS = ("context canceled", "request canceled",
                              "client disconnected")
    ctx = owned_org_sync(f"p22k-{suffix}", timeout_ms=60000)
    owner = ctx["owner"]
    barrier_set(ctx["src"], [1], wait_s=180)

    # fire the default sync POST (real body={}) and ABRUPTLY close the
    # client side while the server is parked inside the request
    base = M.BASES["connector"].replace("http://", "")
    host, port = (base.split(":") + ["80"])[:2]
    conn = http.client.HTTPConnection(host, int(port), timeout=10)
    conn.request("POST", CONTRACT_P22["sync_run_sync"].format(id=ctx["sync_id"]),
                 body=json.dumps({}),
                 headers={"Authorization": "Bearer " + owner.token,
                           "Content-Type": "application/json"})
    wait_barrier_parked(ctx["src"])  # server is inside the parked request
    t_disconnect = time.monotonic()
    conn.sock.close()                # abrupt client-side disconnect
    ev["disconnected"] = True
    runs = snapshot_runs(owner.token, ctx["sync_id"])
    M.expect(len(runs) == 1, "K: exactly one run row after disconnect",
             {"runs": len(runs)})

    final, st = poll_run(owner.token, ctx["sync_id"], runs[0],
                         until=lambda r: r.get("status") in TERMINAL_STATES,
                         timeout=45)
    elapsed = round(time.monotonic() - t_disconnect, 1)
    ev["terminal_after_disconnect"] = (final or {}).get("status")
    ev["seconds_from_disconnect"] = elapsed
    M.expect(final is not None and final.get("status") in TERMINAL_STATES,
             "K: disconnected default run never reached a terminal state",
             {"last_status": st})
    # the DISCRIMINATOR: clearly before the 30s routing / 60s source
    # budgets — a disconnect-ignoring run would only fail at ~60s
    M.expect(elapsed <= 15,
             "K: terminal arrived at budget scale, not disconnect scale — "
             "the run likely ignored the disconnect until the source "
             "timeout", {"seconds_from_disconnect": elapsed})
    M.expect(final.get("status") in ("aborted", "failed"),
             "K: unexpected terminal after disconnect (contract: aborted "
             "or request-scoped failed)", {"status": final.get("status")})
    if final.get("status") == "aborted":
        pin_cancelled_vocabulary(final.get("status"))
    detail = M.scrub(str(M.sync_failure_detail(final)))
    ev["failure_detail"] = detail[:300]
    if final.get("status") == "failed":
        ev["context_marker"] = next((m for m in CONTEXT_CANCEL_MARKERS
                                     if m in detail.lower()), None)
        M.expect(ev["context_marker"],
                 "K: 'failed' reason is not a request-context cancellation — "
                 "an arbitrary non-success must not pass",
                 {"detail": detail[:200]})
    barrier_release(ctx["src"])
    time.sleep(3)
    ev["pages_after_release"] = fixture_count(ctx["src"])
    M.expect(ev["pages_after_release"] == 1,
             "K: pages were fetched after the disconnect terminal",
             {"after": ev["pages_after_release"]})
    M.expect(not M.list_files(owner.token, ctx["target"]["id"]),
             "K: target received files after the disconnect terminal")


# ------------------------------------------------------------------ L -------
def scenario_l(suffix, ev):
    """(root 7) the caller JWT expires WHILE the fetch is parked (the
    barrier guarantees the precheck already passed with the fresh
    identity). The proof is EXACT (review fix E): runtime/sync.go's
    HTTPDatasetVersioningClient wraps non-2xx as DependencyError
    {Op:"commit_output", Status}, whose Error() renders
    "dataset-versioning commit_output responded with status 401" (403
    alike) and Sanitize does not touch it — so the run's failure detail
    must contain that FULL fragment. Together with the publish gate
    having been entered and ZERO transactions/files, that is the only
    accepted Dataset-rejected-the-expired-identity proof; any "contains
    token/jwt" shortcut is gone. The worker never swaps the short
    identity; queries use the owner's fresh token. (The main
    implementation is frozen — only the separate Go HTTP unread-body
    TCP concern is still being verified root-side, and it does not
    affect this scenario's assertions.)"""
    DATASET_AUTH_FRAGMENTS = (
        "dataset-versioning commit_output responded with status 401",
        "dataset-versioning commit_output responded with status 403",
    )
    ctx = owned_org_sync(f"p22l-{suffix}", timeout_ms=60000)
    owner = ctx["owner"]
    # Root fix: the SHORT identity is the REAL SOURCE OWNER (same sub and
    # org as the resource owner, ONLY the TTL differs). A random sub only
    # earned a correct permission 403 and proved nothing about expiry.
    short = type("I", (), {"sub": owner.sub, "org": ctx["org"],
                           "token": M.mint_token(sub=owner.sub,
                                                 org=ctx["org"], ttl=5)})()
    barrier_set(ctx["src"], [1], wait_s=180)

    # precheck while the short token is still FRESH: same-tenant read
    # visibility holds, so the later assertions really test EXPIRY
    res = M.api("connector", "sync_runs", path_args={"id": ctx["sync_id"]},
                token=short.token)
    ev["precheck_short_valid_list"] = res.status
    M.expect(res.status == 200,
             "L: fresh short OWNER token cannot read its own sync — that "
             "would be a permission problem, not expiry",
             {"status": res.status})

    status, body = trigger_async(short.token, ctx["sync_id"], timeout=30)
    ev["admission_status"] = status
    M.expect(status == ADMIT_STATUS, "L: async trigger with short JWT != 202",
             {"status": status})
    run_id = extract_run_id(body)
    wait_barrier_parked(ctx["src"])
    time.sleep(8)  # the JWT is now expired while the fetch stays parked
    barrier_release(ctx["src"])

    final, st = poll_run(owner.token, ctx["sync_id"], run_id,
                         until=lambda r: r.get("status") in TERMINAL_STATES,
                         timeout=90)
    ev["terminal"] = (final or {}).get("status")
    M.expect(final is not None and final.get("status") in TERMINAL_STATES,
             "L: run never terminalized after JWT expiry", {"last_status": st})
    M.expect(final.get("status") != "succeeded",
             "L: run SUCCEEDED with an expired caller identity — a fallback "
             "principal must never publish", final.get("status"))
    # (a) the gate was actually entered — the commit was attempted
    row = run_row(run_id)
    ev["gate_entered"] = row["gate_entered"]
    M.expect(row["gate_entered"],
             "L: run never entered the publish gate — a source timeout or "
             "a never-attempted commit must not pass as a JWT-expiry proof",
             {"row": {k: row[k] for k in ("status", "gate_entered",
                                          "has_content_hash")}})
    # (b) the EXACT Dataset auth-rejection fragment (DependencyError)
    detail = M.scrub(str(M.sync_failure_detail(final)))
    ev["failure_detail"] = detail[:300]
    ev["auth_fragment"] = next((f for f in DATASET_AUTH_FRAGMENTS
                                if f in detail), None)
    M.expect(ev["auth_fragment"],
             "L: failure detail lacks the exact 'dataset-versioning "
             "commit_output responded with status 401/403' fragment — "
             "a non-auth failure must not pass as a JWT-expiry proof",
             {"detail": detail[:200]})
    # (c) zero transactions AND zero files on the target
    ev["target_txns"] = dataset_txn_ids(owner.token, ctx["target"]["id"])
    M.expect(not ev["target_txns"],
             "L: target received transactions despite the expired identity")
    M.expect(not M.list_files(owner.token, ctx["target"]["id"]),
             "L: target received files despite the expired identity")


# ------------------------------------------------------------------ M -------
def _legacy_chain(ev, key):
    """Public-chain handle for ONE archived P2.1 legacy run (root-pinned
    JOIN; ownership columns only). Returns (ident, run_id, sync_def,
    target_dataset_id, fixture_src)."""
    legacy_id = P21_LEGACY_RUNS[key]
    out = psql_connectors(
        "SELECT COALESCE(r.owner_id::text, ''), COALESCE(r.org_id::text, ''), "
        "COALESCE(r.sync_def_id::text, ''), COALESCE(c.owner_id::text, ''), "
        "COALESCE(c.org_id::text, ''), COALESCE(d.output_dataset_id::text, '') "
        "FROM sync_runs r "
        "JOIN batch_sync_defs d ON d.id = r.sync_def_id "
        "JOIN connections c ON c.id = d.source_id "
        f"WHERE r.id = '{legacy_id}'::uuid;")
    M.expect(out and out.count("|") == 5,
             f"legacy chain unreadable for {key}", {"got": out[:120] if out else None})
    (run_owner, run_org, sync_def, src_owner, src_org, target) = \
        [x.strip() for x in out.split("|")]
    for label, value in (("r.owner_id", run_owner), ("r.org_id", run_org),
                         ("r.sync_def_id", sync_def), ("c.owner_id", src_owner),
                         ("c.org_id", src_org), ("d.output_dataset_id", target)):
        M.expect(len(value) == 36, f"legacy {label} not uuid-shaped ({key})",
                 {"shape": len(value)})
    M.expect(run_org == src_org, f"legacy run org != source org ({key})")
    ident = M.Identity(f"p22-legacy-{key}", sub=src_owner, org=src_org)
    src = {"B_lease_lost_unpublished": "p21b-b6ec49ab",
           "C_committed_unconfirmed": "p21c-b6ec49ab"}[key]
    ev.setdefault("legacy_chains", {})[key] = {
        "run_owner_is_source_owner": run_owner == src_owner}
    return ident, legacy_id, sync_def, target, src


def _legacy_member(ev, key, label):
    ident, legacy_id, sync_def, target, src = _legacy_chain(ev, key)
    return {"label": label, "run_id": legacy_id,
            "ctx": {"owner": ident, "target": {"id": target},
                    "sync_id": sync_def, "src": src}}


def scenario_m(suffix, ev):
    """(root 8) restart preserves state across a FIXED 5-MEMBER cohort
    (review fix 6 + root 2026-11-14 obligation closure): completed /
    cancelled / interrupted from SESSION (ALL THREE REQUIRED — a missing
    dependency is a recorded FAIL, never a quiet 2-member PASS) plus the
    two archived P2.1 legacy runs B/C as first-class members with REAL
    run/source/target bindings (public chain + archived fixture src +
    real source-owner identity — the same proven handle method as the
    readonly collector). Every member compares the FULL safe projection,
    its source's fixture request_count, its target's file-id set, txn
    id->status map, and its runs set before/after; the completed member
    keeps the bound-txn COMMITTED check and the HTTP-200-then-hash
    artifact proof. Only connector replicas restart; fixture never."""
    completed = SESSION.get("completed_run") or SESSION.get("gate_run")
    cancelled = SESSION.get("cancelled_run")
    interrupted = SESSION.get("interrupted_run")
    M.expect(completed, "M: dependency unmet — completed run (D/G) missing")
    M.expect(cancelled, "M: dependency unmet — cancelled run (E) missing")
    M.expect(interrupted, "M: dependency unmet — interrupted run (I) missing; "
             "a partial cohort is recorded, never a full PASS")

    cohort = [{"label": "completed", **completed},
              {"label": "cancelled", **cancelled},
              {"label": "interrupted", **interrupted},
              _legacy_member(ev, "B_lease_lost_unpublished", "p21b"),
              _legacy_member(ev, "C_committed_unconfirmed", "p21c")]
    ev["cohort"] = [m["label"] for m in cohort]
    M.expect(len(cohort) == 5, "M: cohort must be exactly 5 members",
             ev["cohort"])

    def member_snapshot(member):
        ctx, rid = member["ctx"], member["run_id"]
        owner = ctx["owner"]
        snap = {"projection": run_row(rid),
                "pages": fixture_count(ctx["src"]),
                "file_ids": sorted(str(f.get("id")) for f in
                                   M.list_files(owner.token, ctx["target"]["id"])),
                "txns": dataset_txn_ids(owner.token, ctx["target"]["id"])}
        if member.get("txn_id"):
            snap["bound_txn_status"] = snap["txns"].get(member["txn_id"])
        return snap

    def artifact_sha256(member):
        ctx = member["ctx"]
        files = M.list_files(ctx["owner"].token, ctx["target"]["id"])
        M.expect(len(files) == 1, f"M/{member['label']}: expected the artifact")
        dl = M.download_file(ctx["owner"].token, ctx["target"]["id"],
                             files[0]["id"])
        M.expect(dl.status == 200,
                 f"M/{member['label']}: artifact download != 200 "
                 "(hash would be meaningless)", {"status": dl.status})
        return hashlib.sha256(dl.body).hexdigest()

    completed_member = cohort[0]
    before = {m["label"]: member_snapshot(m) for m in cohort}
    ev["completed_txns_before"] = len(before["completed"]["txns"])
    # the completed target must expose >=1 REAL txn bound to THIS run's
    # artifact — a 0==0 comparison must never fake "no re-dispatch".
    M.expect(len(before["completed"]["txns"]) >= 1,
             "M: committed target exposes no transactions at all")
    bound_txn = completed_member.get("txn_id")
    M.expect(bound_txn and bound_txn in before["completed"]["txns"],
             "M: the completed run's artifact txn is not listed on its target",
             {"txn": bound_txn,
              "listed": len(before["completed"]["txns"])})
    M.expect(before["completed"]["txns"].get(bound_txn) == "COMMITTED",
             "M: bound txn is not COMMITTED",
             {"status": before["completed"]["txns"].get(bound_txn)})
    sha_before = artifact_sha256(completed_member)
    runs_before = {m["label"]: snapshot_runs(m["ctx"]["owner"].token,
                                             m["ctx"]["sync_id"])
                   for m in cohort}

    replicas = connector_replicas()
    restart = compose("restart", *replicas)
    ev["restart_rc"] = restart.returncode
    M.expect(restart.returncode == 0, "M: restart failed",
             M.scrub(restart.stderr)[:200])
    M.wait_health()
    time.sleep(45)  # observation window >= 2 heartbeat periods

    for member in cohort:
        label = member["label"]
        now = member_snapshot(member)
        was = before[label]
        M.expect(now["projection"] == was["projection"],
                 f"M/{label}: full safe projection changed across restart",
                 {"before": was["projection"], "after": now["projection"]})
        M.expect(now["pages"] == was["pages"],
                 f"M/{label}: source request_count changed across restart",
                 {"before": was["pages"], "after": now["pages"]})
        M.expect(now["file_ids"] == was["file_ids"],
                 f"M/{label}: target file set changed across restart")
        M.expect(now["txns"] == was["txns"],
                 f"M/{label}: target txn id->status map changed across restart",
                 {"before": len(was["txns"]), "after": len(now["txns"])})
        if "bound_txn_status" in was:
            M.expect(now.get("bound_txn_status") == was["bound_txn_status"],
                     f"M/{label}: bound txn status changed across restart")
        M.expect(snapshot_runs(member["ctx"]["owner"].token,
                               member["ctx"]["sync_id"]) == runs_before[label],
                 f"M/{label}: runs list changed across restart")
    ev["completed_txns_after"] = len(dataset_txn_ids(
        completed_member["ctx"]["owner"].token,
        completed_member["ctx"]["target"]["id"]))
    sha_after = artifact_sha256(completed_member)
    M.expect(sha_after == sha_before,
             "M: artifact bytes changed across restart")


# ------------------------------------------------------------------ N -------
def _legacy_owner_identity(ev):
    """Resolve the archived C run's REAL Source owner via the ROOT-PINNED
    parent chain (no table discovery):

        sync_runs r -> batch_sync_defs d ON d.id = r.sync_def_id
                    -> connections c     ON c.id = d.source_id

    ONLY public ownership columns are selected (org/owner/requested_by/
    source ids) — never credentials, config bodies, or token material.
    The JWT is minted in-memory exactly like every other identity here
    (M.Identity with the REAL owner sub/org read from the row — never a
    label-generated UUID). Any shape mismatch FAILS closed."""
    legacy_id = P21_LEGACY_RUNS["C_committed_unconfirmed"]
    out = psql_connectors(
        "SELECT COALESCE(r.owner_id::text, ''), COALESCE(r.org_id::text, ''), "
        "COALESCE(r.requested_by::text, ''), COALESCE(r.sync_def_id::text, ''), "
        "COALESCE(d.source_id::text, ''), COALESCE(c.owner_id::text, ''), "
        "COALESCE(c.org_id::text, '') "
        "FROM sync_runs r "
        "JOIN batch_sync_defs d ON d.id = r.sync_def_id "
        "JOIN connections c ON c.id = d.source_id "
        f"WHERE r.id = '{legacy_id}'::uuid;")
    M.expect(out and out.count("|") == 6,
             "N: legacy parent chain unreadable (root-pinned join)",
             {"got": out[:120] if out else None})
    (run_owner, run_org, requested_by, sync_def,
     source_id, src_owner, src_org) = [x.strip() for x in out.split("|")]
    for label, value in (("r.owner_id", run_owner), ("r.org_id", run_org),
                         ("r.sync_def_id", sync_def), ("d.source_id", source_id),
                         ("c.owner_id", src_owner), ("c.org_id", src_org)):
        M.expect(len(value) == 36, f"N: {label} not uuid-shaped",
                 {"shape": len(value)})
    M.expect(run_org == src_org,
             "N: run org != source org — refusing to guess the tenant",
             {"run_org": run_org, "source_org": src_org})
    ev["legacy_owner_resolved"] = {
        "run_owner_is_source_owner": run_owner == src_owner,
        "requested_by_shape": len(requested_by) in (0, 36)}
    # the cancel permission binds to the SOURCE: use its real owner.
    ident = M.Identity("p22-legacy-owner", sub=src_owner,
                       org=src_org if len(src_org) == 36 else None)
    return ident, legacy_id, sync_def


def scenario_n(suffix, ev):
    """(cancel convention pin) cancelling a P2.1 LEGACY run is
    409 unsupported: the run is never aborted and never re-dispatched —
    exactly the archived committed-unconfirmed C row that must not be
    re-pulled."""
    ident, legacy_id, sync_def = _legacy_owner_identity(ev)
    before = run_row(legacy_id)
    runs_before = snapshot_runs(ident.token, sync_def)
    res = cancel_run(ident.token, sync_def, legacy_id)
    ev["cancel_status"] = res.status
    ev["cancel_code"] = (res.body or {}).get("code") \
        if isinstance(res.body, dict) else None
    assert_no_private_keys(res.body, "N cancel response")
    M.expect(res.status == UNSUPPORTED_STATUS
             and ev["cancel_code"] == UNSUPPORTED_CODE,
             "N: legacy cancel did not answer 409 "
             "code=cancel_unsupported_legacy",
             {"status": res.status, "code": ev["cancel_code"]})
    M.expect(before["status"] == "running"
             and run_row(legacy_id)["status"] == "running",
             "N: legacy run state changed by the cancel attempt",
             {"before": before["status"], "after": run_row(legacy_id)["status"]})
    M.expect(snapshot_runs(ident.token, sync_def) == runs_before,
             "N: legacy cancel created or removed runs")
    time.sleep(3)  # any re-dispatch would surface as a state flip
    M.expect(run_row(legacy_id)["status"] == "running",
             "N: legacy run was re-dispatched after the cancel attempt")


# ---------------------------------------------------------------- main ----
SCENARIOS = {"D": ("async_admission_and_completion", scenario_d),
             "E": ("running_cancel_cross_process", scenario_e),
             "F": ("queue_pending_cancel_and_queuefull", scenario_f),
             "G": ("gate_wins_too_late_via_proxy", scenario_g),
             "H": ("tenant_and_binding_denials", scenario_h),
             "I": ("sigterm_shutdown_no_redispatch", scenario_i),
             "J": ("deadline_bounds", scenario_j),
             "K": ("sync_disconnect_compat", scenario_k),
             "L": ("jwt_expiry_no_fallback", scenario_l),
             "M": ("restart_state_preserved", scenario_m),
             "N": ("legacy_cancel_unsupported", scenario_n)}


def main():
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-dir", required=True)
    parser.add_argument("--only", default="D,E,F,G,H,I,J,K,L,M,N")
    parser.add_argument("--connector2-base", default="http://127.0.0.1:17089")
    parser.add_argument("--connector3-base", default="http://127.0.0.1:17087")
    parser.add_argument("--dataset-proxy-base", default="http://127.0.0.1:17091")
    parser.add_argument("--contract-probe", action="store_true",
                        help="run ONLY the read-mostly preflight probes")
    ARGS = parser.parse_args()
    env_dir = Path(ARGS.env_dir)
    load_p1(env_dir)
    M.wait_health()

    suffix = uuid.uuid4().hex[:8]
    if ARGS.contract_probe:
        ev = {}
        try:
            contract_probe(ev)
            print("[p22] contract-probe OK:", json.dumps(ev), flush=True)
            return 0
        except Exception as exc:
            print(f"[p22] contract-probe FAIL: {M.scrub(str(exc))}", flush=True)
            return 1

    wanted = [x.strip().upper() for x in ARGS.only.split(",") if x.strip()]
    unknown = [k for k in wanted if k not in SCENARIOS]
    M.expect(wanted, "--only must name at least one scenario (empty subset rejected)")
    M.expect(not unknown, "--only contains unknown scenarios", unknown)
    M.expect(len(wanted) == len(set(wanted)),
             "--only contains duplicate scenarios", wanted)
    full_run = set(wanted) == set(SCENARIOS)

    contract_probe({})  # fail closed BEFORE any scenario mutates state

    results, evidence = {}, {}
    for key in wanted:
        name, fn = SCENARIOS[key]
        started = time.time()
        ev = {}
        try:
            fn(suffix, ev)
            results[name] = {"status": "pass", "seconds": round(time.time() - started, 1)}
            print(f"[p22] PASS {name} ({results[name]['seconds']}s)", flush=True)
        except Exception as exc:
            results[name] = {"status": "fail", "seconds": round(time.time() - started, 1),
                             "error": M.scrub(str(exc))}
            print(f"[p22] FAIL {name}: {M.scrub(str(exc))}", flush=True)
        evidence[name] = ev

    failed = [k for k, v in results.items() if v["status"] == "fail"]
    summary = {"suffix": suffix, "results": results, "evidence": evidence,
               "pins": dict(PINS),
               "partial": not full_run,
               "official": full_run and not failed,
               "counts": {"pass": len(results) - len(failed), "fail": len(failed)}}
    out = env_dir / "result_p22.json"
    out.write_text(json.dumps(summary, indent=2) + "\n")
    tag = "OFFICIAL" if summary["official"] else ("PARTIAL" if not failed else "FAILED")
    print(f"[p22] {tag} pass={summary['counts']['pass']} fail={summary['counts']['fail']} "
          f"(official success requires D..N each once) -> {out}", flush=True)
    return 1 if (failed or not full_run) else 0


if __name__ == "__main__":
    sys.exit(main())

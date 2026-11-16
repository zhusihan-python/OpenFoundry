#!/usr/bin/env python3
"""OpenFoundry P2.3 source-admission HTTP acceptance (tool skeleton,
2026-11-14; review-hardened same day — root batches 1-3 + P convergence.
NOTHING here has run against a stack; formal execution waits for the
root release, and every mutation of the q3v9 stack is forbidden until
then).

Reuse model (frozen sources are imported/invoked, never edited):
  * P1 helpers via importlib of ../business-platform-p1/smoke/smoke.py
    (identity minting, HTTP, psql, fixture admin, dataset/sync creation,
    artifact proof, scrub) — the P2.2 pattern.
  * P2.2 helpers via importlib of ../business-platform-p2-2/smoke_p22.py
    (trigger/cancel/get/poll, barrier, dataset-proxy, run_row whitelist
    projection, pg_run_ids, compose) — module level is constants only,
    so importing is side-effect free.
  * The frozen P22 scenarios (incl. J deadline bounds and K sync
    disconnect) are NOT re-run by this tool; the formal flow's
    10-p22-full step runs the complete frozen eleven. Scenario U is the
    P23-specific public/private-field sweep instead (root batch-F).

ROOT-PINNED CONTRACT (2026-11-14) — fail-closed constants:
  * config: optional typed object config.sync.admission with
      max_inflight_runs  — omissible/null = dimension OFF; explicit
                           integer 1..4096; 0 (and any other illegal
                           value) is a 400 on create/update
      min_interval_ms    — omissible/null/0 = OFF; positive integer
                           <= 86400000
    default/null/empty-object -> NO dimension -> old behavior.
  * occupancy: pending+running over ALL syncs of the SAME source and ALL
    actors/instances; released only by a genuine legal terminal
    (succeeded/failed/aborted); a running row with cancel_requested, an
    expired lease, or a legacy-unknown row STILL OCCUPIES; cancelling
    does not refund the interval.
  * throttling: HTTP 429 with body {error, code="source_sync_throttled",
    reason in (capacity|min_interval|contention), retry_after_seconds
    (positive int)} and header Retry-After carrying the SAME value.
    interval -> ceil of the post-lock DB-clock remaining seconds;
    capacity/contention -> advisory 1s (not guaranteed, only >=1 is
    asserted). A typed rejection causes ZERO runs, ZERO stamp advance,
    ZERO extra source pulls, ZERO publish, and the local reservation is
    released.
  * persistence: connections.sync_last_admitted_at is PRIVATE (never in
    any public JSON; tests read it ONLY as a safe PG text column — NULL
    is legal before the first admission). It advances with the DB clock
    in the same tx as every successful run insert and survives restarts.
  * 55P03 (lock_not_available on the source-row NOWAIT lock) maps to
    typed reason=contention; other DB errors keep their existing
    semantics; local queue-full/shutdown stays 503 {"error"}.

EVIDENCE LAYERING (root P-convergence 2026-11-14): the HTTP scenarios
prove ONLY the externally observable behavior — pending genuine-cancel
release, real running cancel-request -> genuine terminal, manual retry
after terminal, interval persistence, typed rejections with zero side
effects, tenant denials before throttling, config validation. The
expired-lease / legacy-unknown / publish-gate / fenced-completion full
release matrix is NOT fabricated on the q3v9 stack (no synthetic
forever-running rows, swap_guard's controlled==0 requirement stays
valid) — it is proven by the root-pinned 7 real private PG top-level
tests (p23-required-names.txt). Narrow windows use the fixture barrier
or a REAL interactively-held DB row lock (active ROLLBACK, exact 55P03
evidence, holder backend pid) — never random sleeps.

Value-level evidence: key transitions archive snapshot pairs under
snapshots_p23/<suffix>/ named before-<base>.json / after-<base>.json /
compare-<base>.json (admission_snapshots.py; sanitized handle echo —
ids/bindings/counts only, never tokens; strict compare with explicit
per-field allows).

Usage:
  python3 -I smoke_p23.py --env-dir <tmp> [--only O,P,Q,R,S,T,U,V]
      [--allow-restart]   # required by Q's restart sub-case (formal only)
  python3 -I smoke_p23.py --env-dir <tmp> --contract-probe
"""

import argparse
import importlib.util
import json
import os
import re
import select
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote

HERE = Path(__file__).resolve().parent
P1_SMOKE = HERE.parent / "business-platform-p1" / "smoke" / "smoke.py"
P22_SMOKE = HERE.parent / "business-platform-p2-2" / "smoke_p22.py"
SNAP_PY = HERE / "admission_snapshots.py"
PROJECT = "openfoundry-platform-p1-q3v9"

# ---------------------------------------------------------------- contract
THROTTLE_STATUS = 429
THROTTLE_CODE = "source_sync_throttled"
THROTTLE_REASONS = ("capacity", "min_interval", "contention")
RETRY_AFTER_KEY = "retry_after_seconds"
RETRY_AFTER_HEADER = "retry-after"          # headers lower-cased
STAMP_COLUMN = "sync_last_admitted_at"      # connections (PRIVATE -> PG only)
ADMIT_KEY = "admission"                     # config.sync.admission
MAX_INFLIGHT_MAX = 4096
MIN_INTERVAL_MAX_MS = 86_400_000
INVALID_CONFIG_STATUS = 400                 # typed config validation refusal

# private keys: the frozen P22 set PLUS the P2.3 source stamp — none of
# them may ever appear in a public JSON body
PRIVATE_KEYS_P23 = ("lease_holder", "lease_token", "lease_expires",
                    "lease_expires_at", "heartbeat", "heartbeat_at",
                    STAMP_COLUMN)

# statuses reused from the frozen, handler-verified P22 contract
ADMIT_STATUS = 202
CANCEL_PENDING_STATUS = 200
CANCEL_RUNNING_STATUS = 202
DENY_STATUSES = (403, 404)
MALFORMED_ID_STATUS = 400
RUNNING_STATES = ("pending", "running")
TERMINAL_STATES = ("succeeded", "failed", "aborted")
CANCEL_TERMINAL_CANDIDATES = ("aborted",)

ARGS = None
M = None    # loaded P1 helper module
P = None    # loaded P2.2 helper module
SNAP = None  # loaded admission_snapshots module
SESSION = {}


# ------------------------------------------------------------------ loading
def load_helpers(env_dir):
    global M, P
    spec = importlib.util.spec_from_file_location("p1smoke", P1_SMOKE)
    M = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(M)
    M.ARGS = type("A", (), {"env_dir": str(env_dir)})()
    M.load_env(env_dir)

    spec2 = importlib.util.spec_from_file_location("p22smoke", P22_SMOKE)
    P = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(P)
    # P.load_p1 re-loads the P1 module into P.M and registers the aux
    # BASES entries that answer healthz; it needs P.ARGS to carry the
    # replica base URLs (same attribute names as our argparse). Note
    # load_env needs a Path — main() passes one.
    P.ARGS = ARGS
    P.load_p1(env_dir)
    M = P.M                    # one shared helper module with aux BASES


def snap_module():
    global SNAP
    if SNAP is None:
        spec = importlib.util.spec_from_file_location("p23snap", SNAP_PY)
        SNAP = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(SNAP)
    return SNAP


# ------------------------------------------------------------- HTTP helpers
def http_full(base_key, path, *, token, method="POST", payload=None,
              timeout=30):
    """Request that also returns response HEADERS (Retry-After proof)."""
    base = M.BASES[base_key]
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Authorization": "Bearer " + token}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers,
                                 method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            try:
                body = json.loads(raw) if raw else None
            except ValueError:
                body = {"_raw": raw.decode(errors="replace")}
            return resp.status, body, hdrs
    except urllib.error.HTTPError as exc:
        raw = exc.read(4000)
        hdrs = {k.lower(): v for k, v in (exc.headers or {}).items()}
        try:
            body = json.loads(raw) if raw else None
        except ValueError:
            body = {"_raw": raw.decode(errors="replace")}
        return exc.code, body, hdrs


def trigger(token, sync_id, *, via="connector", mode="async", timeout=60):
    """mode=async -> ?async=true 202 early; mode=sync -> the frozen P1
    synchronous POST. Both go through the SAME source admission gate."""
    key = "sync_run_async" if mode == "async" else "sync_run_sync"
    path = P.CONTRACT_P22[key].format(id=sync_id)
    return http_full(via, path, token=token, payload={}, timeout=timeout)


def expect_throttle(res, where, *, reason=None):
    """Full typed-429 validation. Returns (reason, retry_after_seconds)."""
    status, body, headers = res
    M.expect(status == THROTTLE_STATUS, f"{where}: expected typed 429",
             {"status": status})
    M.expect(isinstance(body, dict), f"{where}: 429 body not an object",
             {"body_type": type(body).__name__})
    assert_no_private_keys(body, f"{where} 429 body")
    M.expect(isinstance(body.get("error"), str) and body.get("error"),
             f"{where}: 429 body missing a non-empty error message",
             {"keys": sorted(body.keys())})
    M.expect(body.get("code") == THROTTLE_CODE,
             f"{where}: 429 code != {THROTTLE_CODE}",
             {"code": body.get("code")})
    got_reason = body.get("reason")
    M.expect(got_reason in THROTTLE_REASONS,
             f"{where}: 429 reason outside {THROTTLE_REASONS}",
             {"reason": got_reason})
    if reason is not None:
        M.expect(got_reason == reason,
                 f"{where}: 429 reason != pinned {reason}",
                 {"reason": got_reason})
    ra = body.get(RETRY_AFTER_KEY)
    M.expect(isinstance(ra, int) and not isinstance(ra, bool) and ra >= 1,
             f"{where}: retry_after_seconds must be a positive integer",
             {"value": ra})
    hdr = headers.get(RETRY_AFTER_HEADER)
    M.expect(hdr is not None and hdr.strip() == str(ra),
             f"{where}: Retry-After header must equal retry_after_seconds",
             {"header": hdr, "body": ra})
    return got_reason, ra


def assert_no_private_keys(body, where):
    """Recursive key walk with the P2.3 private set (frozen P22 set plus
    connections.sync_last_admitted_at). Evidence carries KEY + path only."""
    stack = [(body, "$")]
    while stack:
        node, path = stack.pop()
        if isinstance(node, dict):
            for key, value in node.items():
                M.expect(key not in PRIVATE_KEYS_P23,
                         f"private key present in public JSON ({where})",
                         {"key": key, "path": f"{path}.{key}"})
                stack.append((value, f"{path}.{key}"))
        elif isinstance(node, list):
            stack.extend((item, f"{path}[{i}]") for i, item in enumerate(node))


# ------------------------------------------------------------ PG whitelist
_CONNECTIONS_COLUMNS = None


def connections_columns():
    global _CONNECTIONS_COLUMNS
    if _CONNECTIONS_COLUMNS is None:
        out = P.psql_connectors(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'connections' ORDER BY column_name;")
        _CONNECTIONS_COLUMNS = {ln.strip() for ln in out.splitlines()
                                if ln.strip()}
    return _CONNECTIONS_COLUMNS


def require_stamp_column():
    M.expect(STAMP_COLUMN in connections_columns(),
             f"connections.{STAMP_COLUMN} has not landed — the P2.3 "
             "migration is not in this build; refusing to guess",
             {"present": sorted(connections_columns())[:40]})


def stamp_of(source_id):
    """STRICT stamp read for sources that HAVE admitted (non-null)."""
    out = P.psql_connectors(
        f"SELECT COALESCE({STAMP_COLUMN}::text, '') FROM connections "
        f"WHERE id = '{source_id}'::uuid;")
    value = out.strip()
    M.expect(value and "\n" not in value,
             f"stamp read failed for source {source_id}",
             {"got": out[:80]})
    return value


def stamp_nullable(source_id):
    """Stamp read for not-yet-admitted sources: NULL is legal and is
    returned as None (root batch-3: pre-admission NULL stamp is valid)."""
    out = P.psql_connectors(
        f"SELECT ({STAMP_COLUMN} IS NULL), COALESCE({STAMP_COLUMN}::text, '') "
        f"FROM connections WHERE id = '{source_id}'::uuid;")
    flag, _, value = out.strip().partition("|")
    M.expect(flag in ("t", "f"),
             f"nullable stamp read failed for source {source_id}",
             {"got": out[:80]})
    return None if flag == "t" else value


def admission_of(source_id):
    """PG-safe projection of ONLY the admission block (typed numbers —
    never the full config body with its credentials). Used by S to
    verify an illegal PATCH left nothing behind, WITHOUT masking via a
    subsequent legal write."""
    out = P.psql_connectors(
        f"SELECT COALESCE((config->'sync'->'{ADMIT_KEY}')::text, '') "
        f"FROM connections WHERE id = '{source_id}'::uuid;")
    value = out.strip()
    M.expect("\n" not in value, f"admission projection failed for {source_id}",
             {"got": out[:80]})
    return value


_INFLIGHT_SQL = ("SELECT count(*) FROM sync_runs r "
                 "JOIN batch_sync_defs d ON d.id = r.sync_def_id "
                 "WHERE d.source_id = '{sid}'::uuid "
                 "AND r.status IN ('pending','running');")
_RUNIDS_SQL = ("SELECT COALESCE(string_agg(r.id::text, ',' "
               "ORDER BY r.id::text), '') FROM sync_runs r "
               "JOIN batch_sync_defs d ON d.id = r.sync_def_id "
               "WHERE d.source_id = '{sid}'::uuid;")


def inflight_of(source_id):
    raw = P.psql_connectors(_INFLIGHT_SQL.format(sid=source_id)).strip()
    M.expect(raw.isdigit(), "inflight count not strictly numeric",
             {"got": raw[:40]})
    return int(raw)


def source_run_ids(source_id):
    agg = P.psql_connectors(_RUNIDS_SQL.format(sid=source_id)).strip()
    return sorted(x for x in agg.split(",") if x)


def psql_rc(sql, timeout=90):
    """psql probe with a REAL return code. Root-verified (PG16 docs +
    psql startup.c): default stderr carries NO SQLSTATE — probes run
    with -X -v ON_ERROR_STOP=1 -v VERBOSITY=sqlstate so an error prints
    an independent `ERROR:  <SQLSTATE>` line; with `-c` (single-query
    action) a failed query exits EXACTLY 1 (rc 3 is the script-loop
    code and does not apply). An arbitrary nonzero rc is never accepted
    as a lock-conflict proof."""
    env_dir = Path(ARGS.env_dir)
    files = ["-f", str(env_dir / "compose.yaml")]
    override = env_dir / "compose-p22-connector2.yaml"
    if override.exists():
        files += ["-f", str(override)]
    cmd = ["docker", "compose", *files, "--project-name", PROJECT,
           "exec", "-T", "postgres", "psql", "-X",
           "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=sqlstate",
           "-U", "openfoundry", "-d", "connectors", "-t", "-A", "-c", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True,
                          timeout=timeout)
    return proc.returncode, proc.stdout, proc.stderr


# ------------------------------------------------------- stack setup helpers
def admission_sync(src_name, *, admission=None, owner=None, total=151,
                   page_size=50, timeout_ms=40000):
    """Fixture src + (optional) admission-configured rest_api Source +
    Dataset + Sync in ONE tenant. admission=None leaves the block out
    entirely (old behavior); admission={} is the explicit empty object
    (also old behavior per contract)."""
    if owner is None:
        owner = M.Identity(f"p23-{src_name}-owner", org=str(uuid.uuid4()))
    M.fixture_admin(src_name, {"mode": "ok", "content": 1, "total": total,
                               "page_size": page_size})
    payload = M.source_payload(f"p23-{src_name}", src_name)
    payload["config"]["sync"]["limits"]["request_timeout_ms"] = timeout_ms
    payload["config"]["sync"]["pagination"]["page_size"] = page_size
    if admission is not None:
        payload["config"]["sync"][ADMIT_KEY] = admission
    source = P.create_source_raw(owner.token, payload)
    assert_no_private_keys(source, f"{src_name} source echo")
    target = M.create_dataset(owner.token, f"p23-{src_name}-out", "json")
    sync = M.create_sync(owner.token, source["id"], target["id"])
    M.expect(sync.status in (200, 201), f"{src_name}: sync create failed",
             {"status": sync.status})
    return {"org": owner.org, "owner": owner, "source": source,
            "target": target, "sync_id": sync.body["id"], "src": src_name}


def extra_sync(ctx, i):
    """One more Sync under the SAME source (multi-Sync shared quota).
    Carries its own target + the owning token so snapshot/zero-side-
    effect checks can cover EVERY actually-addressed target (root
    batch-B: checking only the primary target would miss artifacts/txns
    published through the extra syncs)."""
    target = M.create_dataset(ctx["owner"].token,
                              f"p23-{ctx['src']}-out{i}", "json")
    sync = M.create_sync(ctx["owner"].token, ctx["source"]["id"],
                         target["id"])
    M.expect(sync.status in (200, 201), f"{ctx['src']}: extra sync failed",
             {"status": sync.status})
    return {"sync_id": sync.body["id"], "target": target,
            "source_id": ctx["source"]["id"],
            "owner_token": ctx["owner"].token}


# ------------------------------------------------- zero-side-effect checking
def side_effect_state(ctx, extra_targets=()):
    """Everything a typed rejection must NOT change: the source's full
    run-ID set, its PRIVATE stamp (PG-only read), upstream pulls, and
    published files across EVERY actually-addressed target (root
    batch-B: extra_targets = iterable of (token, dataset_id))."""
    files = []
    for f in M.list_files(ctx["owner"].token, ctx["target"]["id"]):
        files.append(str(f.get("id")))
    for token, dataset_id in extra_targets:
        for f in M.list_files(token, dataset_id):
            files.append(str(f.get("id")))
    return {"run_ids": source_run_ids(ctx["source"]["id"]),
            "stamp": stamp_of(ctx["source"]["id"]),
            "pages": P.fixture_count(ctx["src"]),
            "files": sorted(files)}


def assert_no_side_effects(before, where):
    """Re-read the zero-side-effect state against the ctx that produced
    the baseline (the caller stashes ctx AND extra_targets in
    SESSION[where] BEFORE acting)."""
    entry = SESSION[where]
    after = side_effect_state(entry["ctx"],
                              extra_targets=entry.get("extra_targets", ()))
    for key in ("run_ids", "stamp", "pages", "files"):
        M.expect(after[key] == before[key],
                 f"{where}: typed rejection changed {key} (must be "
                 "zero-run / zero-stamp-advance / zero-pull / zero-publish)",
                 {"key": key, "before": before[key], "after": after[key]})
    return after


# ------------------------------------------------------------- snapshots ---
def snap_handles(ctxs, runs=(), extra_syncs=()):
    """Full universe (root batch-B): every sync of the source INCLUDING
    the extra syncs, and every actually-addressed target WITH its owner
    token (tokens stay in memory; the snapshot echo strips them)."""
    syncs = [{"id": c["sync_id"], "source_id": c["source"]["id"],
              "target_id": c["target"]["id"]} for c in ctxs]
    targets = [{"dataset_id": c["target"]["id"],
                "token": c["owner"].token} for c in ctxs]
    for s in extra_syncs:
        syncs.append({"id": s["sync_id"], "source_id": s["source_id"],
                      "target_id": s["target"]["id"]})
        targets.append({"dataset_id": s["target"]["id"],
                        "token": s["owner_token"]})
    return {"sources": [{"id": c["source"]["id"], "src": c["src"]}
                        for c in ctxs],
            "syncs": syncs,
            "runs": list(runs),
            "targets": targets}


def write_snapshot(tag, snap):
    out_dir = Path(ARGS.env_dir) / "snapshots_p23" / SESSION["suffix"]
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{tag}.json"
    path.write_text(json.dumps(snap, indent=2) + "\n")
    return path


def snapshot_now(tag, ctxs, runs=(), extra_syncs=()):
    snap = snap_module().collect(M, snap_handles(ctxs, runs, extra_syncs))
    write_snapshot(tag, snap)
    M.expect(not snap["collection_failures"],
             f"snapshot {tag}: collection failures",
             {"failures": snap["collection_failures"][:3]})
    return snap


def compare_snapshots(base, before, after, allowed=()):
    """Naming contract: before-<base> / after-<base> / compare-<base>."""
    result = snap_module().compare(before, after, allowed=allowed)
    write_snapshot(f"compare-{base}", result)
    M.expect(result["ok"], f"snapshot compare {base}: violations",
             {"violations": result["violations"][:5]})
    return result


def _txn_fetch(token, dataset_id, cursor):
    url = f"{M.BASES['dataset']}/v1/datasets/{dataset_id}/transactions"
    if cursor:
        url += f"?cursor={quote(cursor)}"
    req = urllib.request.Request(url, headers={
        "Authorization": "Bearer " + token})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as exc:
        raw = exc.read(4000)
        try:
            return exc.code, json.loads(raw) if raw else None
        except ValueError:
            return exc.code, {"_raw": raw.decode(errors="replace")}


def expect_committed_txn_nonempty(ctx, ev, where):
    """Positive-path proof (root batch-1): the FULL transaction set of
    the completed run's target carries at least one COMMITTED entry."""
    txns = snap_module().enumerate_target_txns(
        lambda cursor: _txn_fetch(ctx["owner"].token, ctx["target"]["id"],
                                  cursor))
    committed = sorted(k for k, v in txns.items() if v == "COMMITTED")
    ev[f"{where}_txn_count"] = len(txns)
    ev[f"{where}_committed_txns"] = committed
    M.expect(bool(committed),
             f"{where}: full txn set carries no COMMITTED entry",
             {"txn_count": len(txns)})


# ---------------------------------------------------------------- probe ----
def contract_probe(ev):
    """Read-mostly preflight BEFORE any scenario mutates state (creates a
    fresh probe tenant/objects like the frozen P22 probe does)."""
    # (a) frozen P22 surface still green (cancel route, P22 columns,
    # barrier extension, aux availability) — its own probe context.
    ev22 = {}
    P.contract_probe(ev22)
    ev["p22_surface"] = "ok"
    # (b) the P2.3 migration landed under the pinned private column name
    require_stamp_column()
    ev["stamp_column"] = True
    # (c) admission config accepted; deterministic typed-429 shape via a
    # max-length interval (first trigger admits and completes fast; the
    # second MUST be throttled regardless of timing).
    ctx = admission_sync(f"p23probe-{uuid.uuid4().hex[:6]}",
                         admission={"min_interval_ms": MIN_INTERVAL_MAX_MS},
                         total=1, page_size=1)
    st, body, _ = trigger(ctx["owner"].token, ctx["sync_id"])
    M.expect(st == ADMIT_STATUS, "probe: first trigger != 202", {"st": st})
    run_id = P.extract_run_id(body)
    final, lst = P.poll_run(ctx["owner"].token, ctx["sync_id"], run_id,
                            until=lambda r: r.get("status") in TERMINAL_STATES,
                            timeout=60)
    M.expect(final is not None, "probe: probe run never finished",
             {"last_status": lst})
    ev["probe_stamp_present"] = bool(stamp_of(ctx["source"]["id"]))
    st, body, headers = trigger(ctx["owner"].token, ctx["sync_id"])
    reason, ra = expect_throttle((st, body, headers), "probe typed-429",
                                 reason="min_interval")
    ev["throttle_shape"] = {"reason": reason, "retry_after_seconds": ra}
    # (d) invalid admission create refused with the pinned status
    probe_src = f"p23probe-inv-{uuid.uuid4().hex[:6]}"
    payload = M.source_payload(f"p23-{probe_src}", probe_src)
    payload["config"]["sync"][ADMIT_KEY] = {"max_inflight_runs": 0}
    res = M.api("connector", "src_create", token=ctx["owner"].token,
                payload=payload)
    ev["invalid_create_status"] = res.status
    M.expect(res.status == INVALID_CONFIG_STATUS,
             "probe: max_inflight_runs=0 create not refused with 400",
             {"status": res.status})


# ------------------------------------------------------------------ O -------
def scenario_o(suffix, ev):
    """Capacity is shared across ALL syncs of one source, all legal
    actors, and both replicas. Root batch-2 structure: BOTH filling runs
    go through the SAME instance (one running parked on the barrier +
    one pending behind its single worker) so the cap is genuinely filled
    without depending on cross-replica worker counts; rejections are
    then issued from BOTH replicas (cross-instance gate); a DIFFERENT
    source completes for real on the IDLE replica. The snapshot pair
    genuinely spans before -> rejections -> after, with ONLY the parked
    run's heartbeat/expires explicitly allowed to advance."""
    ctx = admission_sync(f"p23o-a-{suffix}",
                         admission={"max_inflight_runs": 2},
                         timeout_ms=60000)
    owner = ctx["owner"]
    ex1, ex2 = extra_sync(ctx, 1), extra_sync(ctx, 2)
    # root batch-B: a DEFAULT identity carries no connections:write — a
    # legal DIFFERENT actor of the same org must be an admin (permission
    # logic itself is never changed by the tool)
    actor2 = M.Identity(f"p23o-actor2-{suffix}", org=ctx["org"],
                        roles=["admin"])
    extra_targets = ((owner.token, ex1["target"]["id"]),
                     (owner.token, ex2["target"]["id"]))
    try:
        P.barrier_set(ctx["src"], [1], wait_s=180)

        # sync1 via connector (owner): running, parked on page 1
        st, body, _ = trigger(owner.token, ctx["sync_id"], via="connector")
        M.expect(st == ADMIT_STATUS, "O: first trigger != 202", {"st": st})
        run1 = P.extract_run_id(body)
        P.wait_barrier_parked(ctx["src"])
        row1 = P.wait_run_row(run1, timeout=30)
        M.expect(row1["status"] == "running",
                 "O: run1 not running while parked", row1["status"])

        # sync2 via the SAME instance (different legal actor, same org):
        # pending behind connector's busy worker — no second puller
        st, body, _ = trigger(actor2.token, ex1["sync_id"], via="connector")
        M.expect(st == ADMIT_STATUS,
                 "O: second trigger (actor2, same instance) != 202",
                 {"st": st})
        run2 = P.extract_run_id(body)
        pend, _ = P.poll_run(actor2.token, ex1["sync_id"], run2,
                             until=lambda r: r.get("status") == "pending",
                             timeout=20)
        ev["second_run_status"] = (pend or {}).get("status")
        M.expect(ev["second_run_status"] == "pending",
                 "O: second run never observed as pending (it must not "
                 "start pulling while the worker is parked)",
                 {"status": ev["second_run_status"]})
        ev["pages_at_saturation"] = P.fixture_count(ctx["src"])
        M.expect(ev["pages_at_saturation"] == 1,
                 "O: pending run pulled pages before its worker slot")
        ev["inflight_at_saturation"] = inflight_of(ctx["source"]["id"])
        M.expect(ev["inflight_at_saturation"] == 2,
                 "O: shared pending+running count != max_inflight_runs")

        before = side_effect_state(ctx, extra_targets=extra_targets)
        SESSION["O-reject"] = {"ctx": ctx, "extra_targets": extra_targets}
        before_snap = snapshot_now("before-o-reject", [ctx],
                                   runs=[run1, run2],
                                   extra_syncs=[ex1, ex2])

        # third trigger rejected from BOTH replicas + the sync-mode POST
        reasons = []
        for via in ("connector", "connector2"):
            res = trigger(owner.token, ex2["sync_id"], via=via)
            reason, ra = expect_throttle(res, f"O rejection via {via}",
                                         reason="capacity")
            reasons.append({"via": via, "reason": reason, "retry_after": ra})
            assert_no_side_effects(before, "O-reject")
        res = trigger(owner.token, ex2["sync_id"], via="connector",
                      mode="sync", timeout=90)
        reason, ra = expect_throttle(res, "O sync-mode rejection",
                                     reason="capacity")
        reasons.append({"via": "connector(sync-mode)", "reason": reason,
                        "retry_after": ra})
        assert_no_side_effects(before, "O-reject")
        ev["rejections"] = reasons

        after_snap = snapshot_now("after-o-reject", [ctx],
                                  runs=[run1, run2],
                                  extra_syncs=[ex1, ex2])
        # heartbeat/expires legitimately advance on the RUNNING row while
        # parked; everything else (status/IDs/parents/quota/stamp/pulls/
        # txns/artifacts) must be unchanged — explicit allows only.
        compare_snapshots("o-reject", before_snap, after_snap, allowed=[
            f"runs.{run1}.heartbeat", f"runs.{run1}.expires"])

        # DIFFERENT source on the IDLE replica: still admits and REALLY
        # completes (no reservation leak from the typed rejections).
        ctx_b = admission_sync(f"p23o-b-{suffix}")
        evb = {}
        st, body, _ = trigger(ctx_b["owner"].token, ctx_b["sync_id"],
                              via="connector2")
        M.expect(st == ADMIT_STATUS, "O: other-source trigger != 202",
                 {"st": st})
        run_b = P.extract_run_id(body)
        final_b, lst = P.poll_run(ctx_b["owner"].token, ctx_b["sync_id"],
                                  run_b,
                                  until=lambda r: r.get("status") == "succeeded",
                                  timeout=120)
        M.expect(final_b is not None,
                 "O: other-source run never succeeded on the idle replica",
                 {"last_status": lst})
        P.prove_committed_artifact(ctx_b["owner"].token, ctx_b["target"],
                                   final_b, total=151, ev=evb, label="O-B")
        expect_committed_txn_nonempty(ctx_b, evb, "o_b")
        ev["other_source"] = {"run": run_b, **evb}
    finally:
        P.barrier_release(ctx["src"])

    # cleanup: cancel the pending run, let both reach genuine terminals
    P.cancel_run(owner.token, ex1["sync_id"], run2)
    P.poll_run(owner.token, ex1["sync_id"], run2,
               until=lambda r: r.get("status") in TERMINAL_STATES,
               timeout=120)
    P.poll_run(owner.token, ctx["sync_id"], run1,
               until=lambda r: r.get("status") in TERMINAL_STATES,
               timeout=120)
    ev["inflight_after_cleanup"] = inflight_of(ctx["source"]["id"])
    M.expect(ev["inflight_after_cleanup"] == 0,
             "O: source still occupied after cleanup")


# ------------------------------------------------------------------ P -------
def scenario_p(suffix, ev):
    """Release semantics (root P-convergence): the HTTP face proves ONLY
    (PA) a pending run cancelled with its genuine aborted terminal
    releases the slot, and (PB) a real running row with only a persisted
    cancel REQUEST still occupies (pinned by the barrier) until its
    genuine terminal, after which the manual retry admits. The
    expired-lease / legacy-unknown / publish-gate / fenced-completion
    release matrix is NOT fabricated here — it belongs to the pinned 7
    real private PG tests (see module docstring evidence layering)."""
    # ---- PA (cap 2): pending genuine-cancel releases ------------------
    pa = admission_sync(f"p23p-a-{suffix}",
                        admission={"max_inflight_runs": 2},
                        timeout_ms=60000)
    owner = pa["owner"]
    ex = extra_sync(pa, 1)
    try:
        P.barrier_set(pa["src"], [1], wait_s=180)
        run1 = P.extract_run_id(
            trigger(owner.token, pa["sync_id"], via="connector")[1])
        P.wait_barrier_parked(pa["src"])
        run2 = P.extract_run_id(
            trigger(owner.token, ex["sync_id"], via="connector")[1])
        pend, _ = P.poll_run(owner.token, ex["sync_id"], run2,
                             until=lambda r: r.get("status") == "pending",
                             timeout=20)
        M.expect((pend or {}).get("status") == "pending",
                 "PA: filler run never observed pending")
        M.expect(inflight_of(pa["source"]["id"]) == 2, "PA: not saturated")
        expect_throttle(trigger(owner.token, ex["sync_id"],
                                via="connector2"),
                        "PA saturated rejection", reason="capacity")
        # pending cancel -> genuine aborted terminal -> slot released
        res = P.cancel_run(owner.token, ex["sync_id"], run2)
        M.expect(res.status == CANCEL_PENDING_STATUS,
                 "PA: pending cancel != 200", {"status": res.status})
        M.expect(P.cancel_body_run(res.body, "PA pending").get("status")
                 in CANCEL_TERMINAL_CANDIDATES,
                 "PA: pending cancel not aborted")
        M.expect(inflight_of(pa["source"]["id"]) == 1,
                 "PA: pending-cancel did not release the slot")
        run3 = P.extract_run_id(
            trigger(owner.token, ex["sync_id"], via="connector")[1])
        P.cancel_run(owner.token, ex["sync_id"], run3)
        P.poll_run(owner.token, ex["sync_id"], run3,
                   until=lambda r: r.get("status") in TERMINAL_STATES,
                   timeout=60)
        # real running row: cancel is only REQUESTED while the barrier
        # pins it (a genuine DB row, no fabricated state)
        res = P.cancel_run(owner.token, pa["sync_id"], run1)
        M.expect(res.status == CANCEL_RUNNING_STATUS,
                 "PA: running cancel != 202", {"status": res.status})
        M.expect(P.cancel_body_run(res.body, "PA running").get("status")
                 == "running",
                 "PA: 202 body.run claims already terminal")
        row = P.run_row(run1)
        ev["pa_cancel_requested_persisted"] = row["cancel_requested"]
        M.expect(row["cancel_requested"] and row["status"] == "running",
                 "PA: cancel_requested not persisted on the running row")
    finally:
        P.barrier_release(pa["src"])
    final1, _ = P.poll_run(owner.token, pa["sync_id"], run1,
                           until=lambda r: r.get("status")
                           in CANCEL_TERMINAL_CANDIDATES, timeout=120)
    M.expect(final1 is not None, "PA: parked run never reached its terminal")
    ev["pa_inflight_final"] = inflight_of(pa["source"]["id"])
    M.expect(ev["pa_inflight_final"] == 0,
             "PA: source still occupied after genuine terminal")

    # ---- PB (cap 1): running request-only hold + manual retry ----------
    pb = admission_sync(f"p23p-b-{suffix}",
                        admission={"max_inflight_runs": 1},
                        total=1, page_size=1, timeout_ms=60000)
    owner_b = pb["owner"]
    try:
        P.barrier_set(pb["src"], [1], wait_s=180)
        run = P.extract_run_id(
            trigger(owner_b.token, pb["sync_id"], via="connector")[1])
        P.wait_barrier_parked(pb["src"])
        M.expect(P.run_row(run)["status"] == "running", "PB: not running")
        expect_throttle(trigger(owner_b.token, pb["sync_id"],
                                via="connector2"),
                        "PB saturated rejection", reason="capacity")
        res = P.cancel_run(owner_b.token, pb["sync_id"], run)
        M.expect(res.status == CANCEL_RUNNING_STATUS,
                 "PB: running cancel != 202", {"status": res.status})
        # root batch-C: NO narrow-window still-429 assertion here — the
        # worker may abort immediately after the persisted request. The
        # HTTP proof is: 202 body.run still running + the request row +
        # release below reaching the genuine terminal; the
        # cancelRequested-occupancy matrix belongs to the pinned PG suite.
        M.expect(P.cancel_body_run(res.body, "PB running").get("status")
                 == "running",
                 "PB: 202 body.run claims already terminal")
        row = P.run_row(run)
        ev["pb_cancel_requested_persisted"] = row["cancel_requested"]
        M.expect(row["cancel_requested"],
                 "PB: cancel_requested not persisted on the run row")
    finally:
        P.barrier_release(pb["src"])
    final, _ = P.poll_run(owner_b.token, pb["sync_id"], run,
                          until=lambda r: r.get("status")
                          in CANCEL_TERMINAL_CANDIDATES, timeout=120)
    ev["pb_terminal"] = (final or {}).get("status")
    M.expect(final is not None, "PB: run never reached its terminal")
    M.expect(inflight_of(pb["source"]["id"]) == 0,
             "PB: slot not released after the genuine terminal")
    # manual retry AFTER the genuine terminal admits and completes
    run2 = P.extract_run_id(
        trigger(owner_b.token, pb["sync_id"], via="connector")[1])
    final2, _ = P.poll_run(owner_b.token, pb["sync_id"], run2,
                           until=lambda r: r.get("status") == "succeeded",
                           timeout=60)
    M.expect(final2 is not None,
             "PB: manual retry after terminal never succeeded")


# ------------------------------------------------------------------ Q -------
def wait_interval_elapsed(source_id, interval_ms, timeout=60):
    """Ground the interval in the DB clock (single clock), not client
    sleeps: poll until PG says now() >= stamp + interval."""
    sql = (f"SELECT ({STAMP_COLUMN} + ('{interval_ms} milliseconds')::interval)"
           f" <= now() FROM connections WHERE id = '{source_id}'::uuid;")
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if P.psql_connectors(sql).strip() == "t":
            return True
        time.sleep(0.5)
    raise AssertionError("interval never elapsed per the DB clock "
                         f"(source {source_id}, {interval_ms}ms)")


def remaining_interval_seconds(source_id, interval_ms):
    sql = (f"SELECT GREATEST(0, CEIL(EXTRACT(EPOCH FROM (({STAMP_COLUMN} "
           f"+ ('{interval_ms} milliseconds')::interval) - now()))))::int "
           f"FROM connections WHERE id = '{source_id}'::uuid;")
    raw = P.psql_connectors(sql).strip()
    M.expect(raw.isdigit(), "remaining-seconds read not numeric",
             {"got": raw[:40]})
    return int(raw)


def expect_retry_after_consistent(source_id, interval_ms, header_value,
                                  ev, where):
    """Cross-check the header against the POST-LOCK DB-clock remaining,
    recomputed independently from the private stamp (PG-only read). The
    server computed its value slightly EARLIER, so header >= remaining
    now; allow a small slack for the read latency."""
    expected_now = remaining_interval_seconds(source_id, interval_ms)
    ev.setdefault("retry_after_checks", []).append(
        {"where": where, "expected_now": expected_now,
         "header": header_value})
    M.expect(expected_now <= header_value <= expected_now + 3,
             f"{where}: Retry-After not consistent with the DB-clock "
             "remaining time",
             {"expected_now": expected_now, "header": header_value})


def scenario_q(suffix, ev):
    """min_interval: fast-success then immediate typed rejection whose
    Retry-After matches the DB-clock remaining; cancelling does not
    refund; the anchor survives a connector restart on a DEDICATED
    long-interval source (DB-clock precondition guarantees the interval
    cannot naturally expire during the restart health wait); expiry
    admits on manual retry."""
    INTERVAL_MS = 5000
    ctx = admission_sync(f"p23q-{suffix}",
                         admission={"min_interval_ms": INTERVAL_MS},
                         total=1, page_size=1)
    owner = ctx["owner"]
    sid = ctx["source"]["id"]

    run1 = P.extract_run_id(trigger(owner.token, ctx["sync_id"])[1])
    final1, _ = P.poll_run(owner.token, ctx["sync_id"], run1,
                           until=lambda r: r.get("status") == "succeeded",
                           timeout=60)
    M.expect(final1 is not None, "Q: fast run never succeeded")
    stamp1 = stamp_of(sid)
    ev["stamp_after_first_success"] = stamp1

    before = side_effect_state(ctx)
    SESSION["Q-reject"] = {"ctx": ctx}
    res = trigger(owner.token, ctx["sync_id"])
    reason, ra = expect_throttle(res, "Q fast-success rejection",
                                 reason="min_interval")
    ev["first_rejection"] = {"reason": reason, "retry_after": ra}
    expect_retry_after_consistent(sid, INTERVAL_MS, ra, ev, "Q-first")
    after = assert_no_side_effects(before, "Q-reject")
    M.expect(after["stamp"] == stamp1,
             "Q: rejected request advanced the admission stamp")

    # cancel does not refund: admit run2 (parked), cancel quickly, the
    # very next trigger is still throttled against run2's admission
    wait_interval_elapsed(sid, INTERVAL_MS)
    try:
        P.barrier_set(ctx["src"], [1], wait_s=180)
        run2 = P.extract_run_id(trigger(owner.token, ctx["sync_id"])[1])
        P.wait_barrier_parked(ctx["src"])
        stamp2 = stamp_of(sid)
        M.expect(stamp2 != stamp1, "Q: stamp did not advance on admission")
        P.cancel_run(owner.token, ctx["sync_id"], run2)
        P.poll_run(owner.token, ctx["sync_id"], run2,
                   until=lambda r: r.get("status") in TERMINAL_STATES,
                   timeout=120)
        res = trigger(owner.token, ctx["sync_id"])
        reason, ra = expect_throttle(res, "Q no-refund after cancel",
                                     reason="min_interval")
        expect_retry_after_consistent(sid, INTERVAL_MS, ra, ev, "Q-no-refund")
        ev["no_refund"] = True

        # expiry -> manual retry succeeds (page released for this run)
        wait_interval_elapsed(sid, INTERVAL_MS)
        P.barrier_release(ctx["src"])
        run3 = P.extract_run_id(trigger(owner.token, ctx["sync_id"])[1])
        final3, _ = P.poll_run(owner.token, ctx["sync_id"], run3,
                               until=lambda r: r.get("status") == "succeeded",
                               timeout=60)
        M.expect(final3 is not None, "Q: post-interval run never succeeded")
        stamp3 = stamp_of(sid)

        # batch-3 fix: wait for the DB interval AFTER run3 before run4
        wait_interval_elapsed(sid, INTERVAL_MS)
        run4 = P.extract_run_id(trigger(owner.token, ctx["sync_id"])[1])
        final4, _ = P.poll_run(owner.token, ctx["sync_id"], run4,
                               until=lambda r: r.get("status") == "succeeded",
                               timeout=60)
        M.expect(final4 is not None, "Q: run4 never admitted/succeeded")
        stamp4 = stamp_of(sid)
        M.expect(stamp4 != stamp3, "Q: stamp did not advance for run4")
    finally:
        P.barrier_release(ctx["src"])

    # ---- restart persistence on a DEDICATED long-interval source ------
    # Fail closed without the formal flag — an official run may never
    # skip this sub-case. root batch-D: LONG_MS (120s) strictly exceeds
    # the RESTART_HEADROOM_S (60s) precondition, so the interval cannot
    # naturally expire during the restart health wait AND the remaining
    # check is self-consistent (remaining <= LONG_MS/1000 = 120 >= 60).
    if not getattr(ARGS, "allow_restart", False):
        raise AssertionError("Q: restart sub-case requires --allow-restart "
                             "(formal runs only; refusing to produce an "
                             "official result without it)")
    LONG_MS = 120000
    RESTART_HEADROOM_S = 60
    ctx_r = admission_sync(f"p23q-r-{suffix}",
                           admission={"min_interval_ms": LONG_MS},
                           total=1, page_size=1)
    owner_r = ctx_r["owner"]
    sid_r = ctx_r["source"]["id"]
    run_r1 = P.extract_run_id(trigger(owner_r.token, ctx_r["sync_id"])[1])
    P.poll_run(owner_r.token, ctx_r["sync_id"], run_r1,
               until=lambda r: r.get("status") == "succeeded", timeout=60)
    stamp_r = stamp_of(sid_r)
    remaining = remaining_interval_seconds(sid_r, LONG_MS)
    ev["restart_precondition_remaining_s"] = remaining
    M.expect(remaining >= RESTART_HEADROOM_S,
             "Q-restart: remaining interval shorter than the restart "
             "headroom — precondition failed honestly",
             {"remaining": remaining, "headroom_s": RESTART_HEADROOM_S})
    res = trigger(owner_r.token, ctx_r["sync_id"])
    reason, ra = expect_throttle(res, "Q pre-restart rejection",
                                 reason="min_interval")
    expect_retry_after_consistent(sid_r, LONG_MS, ra, ev, "Q-pre-restart")

    # no live controlled task anywhere before the restart — same
    # fixed-exception guard discipline as the formal swap (only the two
    # precisely-provenanced P21 B/C rows may exist, undrifted, pre-boot;
    # everything else incl. started_at NULL blocks). The frozen
    # swap_guard.py itself stays untouched.
    guard = subprocess.run(
        ["python3", "-I", str(HERE / "p23_live_guard.py"),
         str(ARGS.env_dir), "connector"],
        capture_output=True, text=True, timeout=120)
    ev["restart_guard_rc"] = guard.returncode
    M.expect(guard.returncode == 0,
             "Q: controlled live-task guard refused the restart",
             {"stdout": guard.stdout[-200:]})

    restart = P.compose("restart", "connector")
    ev["restart_rc"] = restart.returncode
    M.expect(restart.returncode == 0, "Q: connector restart failed",
             {"stderr": restart.stderr[-200:]})
    health = subprocess.run(
        ["python3", "-I", str(P1_SMOKE), "--env-dir", str(ARGS.env_dir),
         "--wait-only"], capture_output=True, text=True, timeout=300)
    ev["health_rc"] = health.returncode
    M.expect(health.returncode == 0, "Q: post-restart health wait failed")

    stamp_r2 = stamp_of(sid_r)
    M.expect(stamp_r2 == stamp_r,
             "Q: restart changed the admission anchor (must persist)")
    res = trigger(owner_r.token, ctx_r["sync_id"])
    reason, ra = expect_throttle(res, "Q post-restart rejection",
                                 reason="min_interval")
    expect_retry_after_consistent(sid_r, LONG_MS, ra, ev, "Q-post-restart")
    ev["restart_persisted"] = True
    wait_interval_elapsed(sid_r, LONG_MS, timeout=150)
    run_r2 = P.extract_run_id(trigger(owner_r.token, ctx_r["sync_id"])[1])
    final_r2, _ = P.poll_run(owner_r.token, ctx_r["sync_id"], run_r2,
                             until=lambda r: r.get("status") == "succeeded",
                             timeout=60)
    M.expect(final_r2 is not None,
             "Q: post-restart-expiry run never admitted")


# ------------------------------------------------------------------ R -------
def scenario_r(suffix, ev):
    """Authorization denials come FIRST: even on a saturated source an
    unauthorized principal gets 403/404 (never 429 — that would leak
    throttle state), every denial leaves the world unchanged, and the
    owner keeps liveness. Wording stays neutral about WHICH internal
    check fired: the Source/Def parent-chain internals are proven by
    the pinned PG suite, not claimed here."""
    ctx = admission_sync(f"p23r-{suffix}",
                         admission={"max_inflight_runs": 1},
                         timeout_ms=60000)
    owner = ctx["owner"]
    ctx2 = admission_sync(f"p23r2-{suffix}")      # another tenant (no gate)
    try:
        P.barrier_set(ctx["src"], [1], wait_s=180)
        run1 = P.extract_run_id(trigger(owner.token, ctx["sync_id"])[1])
        P.wait_barrier_parked(ctx["src"])
        M.expect(inflight_of(ctx["source"]["id"]) == 1,
                 "R: source not saturated")

        other_org = str(uuid.uuid4())
        same_sub_other_org = M.Identity(f"p23r-dual-{suffix}", sub=owner.sub,
                                        org=other_org)
        cross_org_admin = M.Identity(f"p23r-xadmin-{suffix}", org=other_org,
                                     roles=["admin"])
        personal = M.Identity(f"p23r-personal-{suffix}", org=None)

        before = side_effect_state(ctx)
        SESSION["R-deny"] = {"ctx": ctx}

        def deny_trigger(label, token, sync_id):
            st, body, headers = trigger(token, sync_id, via="connector")
            ev[label] = st
            assert_no_private_keys(body, f"R {label}")
            M.expect(st in DENY_STATUSES,
                     f"R: {label} not denied (403/404) — a 429 here would "
                     "leak throttle state to an unauthorized principal",
                     {"status": st})
            assert_no_side_effects(before, "R-deny")

        deny_trigger("same_sub_cross_org_trigger",
                     same_sub_other_org.token, ctx["sync_id"])
        deny_trigger("cross_org_admin_trigger",
                     cross_org_admin.token, ctx["sync_id"])
        deny_trigger("personal_trigger", personal.token, ctx["sync_id"])
        deny_trigger("foreign_tenant_sync_trigger",
                     owner.token, ctx2["sync_id"])
        st, body, _ = trigger(owner.token, "not-a-uuid")
        ev["malformed_sync_id"] = st
        M.expect(st == MALFORMED_ID_STATUS or st in DENY_STATUSES,
                 "R: malformed sync id not rejected", {"status": st})

        # liveness: the saturated run is still cancellable by its owner
        res = P.cancel_run(owner.token, ctx["sync_id"], run1)
        M.expect(res.status == CANCEL_RUNNING_STATUS,
                 "R: post-attack owner cancel != 202", {"status": res.status})
    finally:
        P.barrier_release(ctx["src"])
    P.poll_run(owner.token, ctx["sync_id"], run1,
               until=lambda r: r.get("status") in TERMINAL_STATES,
               timeout=120)


# ------------------------------------------------------------------ S -------
def scenario_s(suffix, ev):
    """Typed config validation: illegal values refused with 400 and —
    verified via the PG-safe admission projection BEFORE any subsequent
    legal write — nothing persisted; a legal PATCH carrying an explicit
    legal admission round-trips; PATCH-with-active-runs keeps the frozen
    409; omitted AND empty admission both keep the old two-concurrent-
    runs behavior. (/api/v1/connections is the legal legacy alias for
    the PATCH route.)"""
    owner = M.Identity(f"p23s-owner-{suffix}", org=str(uuid.uuid4()))
    ev["invalid_statuses"] = {}

    def try_invalid(adm, label):
        name = f"p23s-inv-{label}-{uuid.uuid4().hex[:6]}"
        M.fixture_admin(name, {"mode": "ok", "content": 1, "total": 1,
                               "page_size": 1})
        payload = M.source_payload(f"p23-{name}", name)
        payload["config"]["sync"][ADMIT_KEY] = adm
        res = M.api("connector", "src_create", token=owner.token,
                    payload=payload)
        ev["invalid_statuses"][label] = res.status
        M.expect(res.status == INVALID_CONFIG_STATUS,
                 f"S: admission {adm!r} not refused with 400",
                 {"status": res.status})
        M.expect(isinstance(res.body, dict) and res.body.get("error"),
                 f"S: 400 for {label} carries no error message")

    for label, adm in (
            ("max_zero", {"max_inflight_runs": 0}),
            ("max_negative", {"max_inflight_runs": -1}),
            ("max_over", {"max_inflight_runs": MAX_INFLIGHT_MAX + 1}),
            ("max_float", {"max_inflight_runs": 1.5}),
            ("max_string", {"max_inflight_runs": "2"}),
            ("max_bool", {"max_inflight_runs": True}),
            ("interval_negative", {"min_interval_ms": -1}),
            ("interval_over", {"min_interval_ms": MIN_INTERVAL_MAX_MS + 1}),
            ("interval_float", {"min_interval_ms": 1.5}),
            ("interval_string", {"min_interval_ms": "1000"}),
            ("interval_bool", {"min_interval_ms": True})):
        try_invalid(adm, label)

    # valid create round-trips through the stored-row echo (normalized)
    provided = {"max_inflight_runs": 3, "min_interval_ms": 1500}
    ctx = admission_sync(f"p23s-rt-{suffix}", admission=provided,
                         owner=owner, total=1)
    cfg0 = ctx["source"].get("config") if isinstance(
        ctx["source"].get("config"), dict) else {}
    sync0 = cfg0.get("sync") if isinstance(cfg0.get("sync"), dict) else {}
    echoed = sync0.get(ADMIT_KEY)
    ev["roundtrip_echo"] = echoed
    M.expect(isinstance(echoed, dict),
             "S: normalized echo missing the admission block")
    for key, value in provided.items():
        M.expect(echoed.get(key) == value,
                 f"S: roundtrip changed provided key {key}",
                 {"echoed": echoed.get(key), "provided": value})
    ev["normalize_extra_keys"] = sorted(
        set(echoed) - set(provided)) if isinstance(echoed, dict) else None

    # PATCH: illegal change refused AND the stored projection proves no
    # side effect BEFORE any legal write could mask it (batch-3 rule);
    # then a LEGAL PATCH with an explicit legal admission round-trips.
    src_id = ctx["source"]["id"]
    base_config = M.source_payload(f"p23-p23s-rt-{suffix}", ctx["src"])[
        "config"]

    def patch(config):
        return M.http("connector", f"/api/v1/connections/{src_id}",
                      payload={"config": config}, method="PATCH",
                      token=owner.token)

    adm0 = admission_of(src_id)
    ev["admission_projection_before"] = adm0
    bad = json.loads(json.dumps(base_config))
    bad["sync"][ADMIT_KEY] = {"max_inflight_runs": 0}
    res = patch(bad)
    ev["patch_invalid_status"] = res.status
    M.expect(res.status == INVALID_CONFIG_STATUS,
             "S: PATCH with max_inflight_runs=0 not refused",
             {"status": res.status})
    adm_after_bad = admission_of(src_id)
    ev["admission_projection_after_bad"] = adm_after_bad
    M.expect(adm_after_bad == adm0,
             "S: illegal PATCH changed the stored admission (must be "
             "nothing persisted)", {"before": adm0, "after": adm_after_bad})

    legal = json.loads(json.dumps(base_config))
    legal_admission = {"max_inflight_runs": 5, "min_interval_ms": 2000}
    legal["sync"][ADMIT_KEY] = legal_admission
    res = patch(legal)
    ev["patch_legal_status"] = res.status
    M.expect(res.status in (200, 201), "S: legal PATCH failed",
             {"status": res.status})
    body_legal = res.body if isinstance(res.body, dict) else {}
    cfg_legal = body_legal.get("config") if isinstance(
        body_legal.get("config"), dict) else {}
    sync_legal = cfg_legal.get("sync") if isinstance(
        cfg_legal.get("sync"), dict) else {}
    echoed_legal = sync_legal.get(ADMIT_KEY)
    ev["patch_legal_echo"] = echoed_legal
    M.expect(isinstance(echoed_legal, dict),
             "S: legal PATCH echo missing the admission block")
    for key, value in legal_admission.items():
        M.expect(echoed_legal.get(key) == value,
                 f"S: legal PATCH roundtrip changed key {key}",
                 {"echoed": echoed_legal.get(key), "provided": value})
    adm_after_legal = admission_of(src_id)
    ev["admission_projection_after_legal"] = adm_after_legal
    M.expect(adm_after_legal != adm0 and "5" in adm_after_legal,
             "S: legal PATCH did not persist the new admission",
             {"after": adm_after_legal})

    # PATCH with an ACTIVE run keeps the frozen ErrActiveRuns 409
    ctx2 = admission_sync(f"p23s-active-{suffix}",
                          admission={"max_inflight_runs": 1},
                          timeout_ms=60000)
    try:
        P.barrier_set(ctx2["src"], [1], wait_s=180)
        run = P.extract_run_id(trigger(ctx2["owner"].token,
                                       ctx2["sync_id"])[1])
        P.wait_barrier_parked(ctx2["src"])
        changed = M.source_payload(f"p23-p23s-active-{suffix}", ctx2["src"])[
            "config"]
        changed["sync"][ADMIT_KEY] = {"max_inflight_runs": 4}
        res = M.http("connector",
                     f"/api/v1/connections/{ctx2['source']['id']}",
                     payload={"config": changed}, method="PATCH",
                     token=ctx2["owner"].token)
        ev["patch_active_status"] = res.status
        ev["patch_active_error"] = (res.body or {}).get("error") \
            if isinstance(res.body, dict) else None
        M.expect(res.status == 409,
                 "S: admission change under an active run != 409",
                 {"status": res.status})
        P.cancel_run(ctx2["owner"].token, ctx2["sync_id"], run)
    finally:
        P.barrier_release(ctx2["src"])
    P.poll_run(ctx2["owner"].token, ctx2["sync_id"], run,
               until=lambda r: r.get("status") in TERMINAL_STATES,
               timeout=120)

    # differential: omitted AND {} admission both admit TWO concurrent
    # runs (1 running parked + 1 pending) — the old default behavior
    for label, adm in (("omitted", None), ("empty", {})):
        dctx = admission_sync(f"p23s-def-{label}-{suffix}", admission=adm,
                              timeout_ms=60000)
        dex = extra_sync(dctx, 1)
        try:
            P.barrier_set(dctx["src"], [1], wait_s=180)
            r1 = P.extract_run_id(trigger(dctx["owner"].token,
                                          dctx["sync_id"],
                                          via="connector")[1])
            P.wait_barrier_parked(dctx["src"])
            st2, body2, _ = trigger(dctx["owner"].token, dex["sync_id"],
                                    via="connector")
            ev[f"default_{label}_second"] = st2
            M.expect(st2 == ADMIT_STATUS,
                     f"S: default ({label}) admission behaved like a gate "
                     "— omitted/empty must keep the old behavior",
                     {"st": st2})
            r2 = P.extract_run_id(body2)
        finally:
            P.barrier_release(dctx["src"])
        P.cancel_run(dctx["owner"].token, dex["sync_id"], r2)
        P.poll_run(dctx["owner"].token, dex["sync_id"], r2,
                   until=lambda r: r.get("status") in TERMINAL_STATES,
                   timeout=120)
        P.poll_run(dctx["owner"].token, dctx["sync_id"], r1,
                   until=lambda r: r.get("status") in TERMINAL_STATES,
                   timeout=120)


# ------------------------------------------------------------------ T -------
class _SourceRowLock:
    """Interactive psql session holding FOR UPDATE on the source row —
    the exact lock the service takes NOWAIT. Released by an ACTIVE
    ROLLBACK in finally (no pg_sleep timing dependency); the holder's
    real backend pid is captured for evidence.

    I/O discipline (root T finding): the child is spawned with
    bufsize=0 and lines are split from a SELF-MANAGED byte buffer fed by
    os.read on the raw fd. A default BufferedReader.readline would
    prefetch the FOLLOWING pid line into the Python-side buffer, the OS
    fd would then look empty to select() and the next wait would spuriously
    time out. A local fake-child selftest (selftest_lock_holder.py)
    exercises both lines arriving in ONE write with stdin held open and
    proves acquire/release terminate."""

    def __init__(self, sid, cmd_factory=None):
        self.sid = sid
        self._cmd_factory = cmd_factory   # testability: local fake child
        self.proc = None
        self.pid = None
        self._buf = b""

    def _cmd(self):
        if self._cmd_factory is not None:
            return self._cmd_factory()
        env_dir = Path(ARGS.env_dir)
        files = ["-f", str(env_dir / "compose.yaml")]
        override = env_dir / "compose-p22-connector2.yaml"
        if override.exists():
            files += ["-f", str(override)]
        return ["docker", "compose", *files, "--project-name", PROJECT,
                "exec", "-T", "postgres", "psql", "-X",
                "-v", "ON_ERROR_STOP=1", "-v", "VERBOSITY=sqlstate",
                "-U", "openfoundry", "-d", "connectors", "-t", "-A"]

    def _readline_raw(self, timeout):
        """One full line from the unbuffered child stdout within the
        timeout, or None. Bytes leftover stay in self._buf for the next
        call (never stranded in a Python BufferedReader)."""
        deadline = time.monotonic() + timeout
        while b"\n" not in self._buf:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            ready, _, _ = select.select([self.proc.stdout], [], [],
                                        min(0.5, remaining))
            if not ready:
                if self.proc.poll() is not None:
                    break      # child gone; drain whatever arrived
                continue
            chunk = os.read(self.proc.stdout.fileno(), 4096)
            if not chunk:
                break          # EOF
            self._buf += chunk
        if b"\n" not in self._buf:
            return None
        line, self._buf = self._buf.split(b"\n", 1)
        return line + b"\n"

    def acquire(self, timeout=15):
        self.proc = subprocess.Popen(self._cmd(), stdin=subprocess.PIPE,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, bufsize=0)
        script = ("BEGIN;\n"
                  f"SELECT id FROM connections WHERE id = '{self.sid}'::uuid"
                  " FOR UPDATE;\n"
                  "SELECT pg_backend_pid();\n").encode()
        self.proc.stdin.write(script)
        self.proc.stdin.flush()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            line = self._readline_raw(0.5)
            if line is None:
                if self.proc.poll() is not None and b"\n" not in self._buf:
                    break
                continue
            text = line.decode(errors="replace").strip()
            if text.isdigit():
                self.pid = int(text)
                return self.pid
            # the FOR UPDATE result row (uuid) is expected first — skip
        raise AssertionError(
            "holder session never reported its backend pid "
            f"(rc={self.proc.poll()}, buffered={self._buf[:60]!r})")

    def release(self, timeout=15):
        """Root batch-E: NEVER stdin.close() then communicate() (closed
        pipe -> ValueError, lost process) — feed the ROLLBACK through
        communicate(input=...); on timeout kill + reap."""
        if self.proc is None:
            return None
        rc = None
        try:
            try:
                self.proc.communicate(input=b"ROLLBACK;\n", timeout=timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.communicate(timeout=10)
            rc = self.proc.returncode
        except Exception:  # noqa: BLE001 — release is best-effort in finally
            rc = None
        self.proc = None
        return rc


def scenario_t(suffix, ev):
    """contention: a REAL DB row lock held by an interactive session on
    the source row forces the typed contention answer. Held-proof is
    lock-state based: our own NOWAIT probe fails with the EXACT 55P03
    AND the holder's real backend pid sits 'idle in transaction'. After
    an ACTIVE ROLLBACK (bounded, in finally) the NOWAIT probe succeeds
    and admission works again. Deterministic window — no sleeps."""
    ctx = admission_sync(f"p23t-{suffix}",
                         admission={"max_inflight_runs": 1},
                         total=1, page_size=1)
    owner = ctx["owner"]
    sid = ctx["source"]["id"]

    # baseline: admits and completes when the lock is free
    run0 = P.extract_run_id(trigger(owner.token, ctx["sync_id"])[1])
    final0, _ = P.poll_run(owner.token, ctx["sync_id"], run0,
                           until=lambda r: r.get("status") == "succeeded",
                           timeout=60)
    M.expect(final0 is not None, "T: baseline run never succeeded")

    nowait_sql = (f"SELECT id FROM connections WHERE id = '{sid}'::uuid "
                  "FOR UPDATE NOWAIT;")
    holder = _SourceRowLock(sid)
    holder_rc = None
    try:
        pid = holder.acquire()
        ev["holder_pid"] = pid
        # held-proof 1 (root T pin, corrected 2026-11-14 against PG16.15
        # psql startup.c:350-361): `psql -c` is the ACT_SINGLE_QUERY
        # branch — a failed SendQuery exits EXIT_FAILURE=1 (rc 3 belongs
        # to the script main loop and does NOT apply to -c). The proof
        # is therefore EXACTLY rc 1 AND an independent SQLSTATE error
        # line `ERROR:  55P03` (VERBOSITY=sqlstate guarantees the line
        # format). An arbitrary nonzero rc proves nothing.
        rc, out, err = psql_rc(nowait_sql, timeout=30)
        sqlstate_line = any(re.fullmatch(r"ERROR:\s+55P03", ln.strip())
                            for ln in err.splitlines())
        ev["probe_rc"] = rc
        ev["probe_55p03_line"] = sqlstate_line
        ev["probe_err_tail"] = M.scrub(err[-160:])
        M.expect(rc == 1 and sqlstate_line,
                 "T: NOWAIT probe did not fail with EXACTLY rc=1 plus an "
                 "independent `ERROR: 55P03` SQLSTATE line while the "
                 "holder owns the row lock",
                 {"rc": rc, "err_tail": ev["probe_err_tail"]})
        # held-proof 2: the holder's real backend sits idle in the open tx
        activity = P.psql_connectors(
            f"SELECT count(*) FROM pg_stat_activity "
            f"WHERE pid = {pid} AND state = 'idle in transaction';").strip()
        ev["holder_idle_in_transaction"] = activity
        M.expect(activity == "1",
                 "T: holder backend not idle-in-transaction — the lock "
                 "proof is not established", {"pid": pid, "count": activity})

        before = side_effect_state(ctx)
        SESSION["T-contend"] = {"ctx": ctx}
        res = trigger(owner.token, ctx["sync_id"])
        reason, ra = expect_throttle(res, "T contention", reason="contention")
        ev["contention_retry_after"] = ra
        M.expect(ra >= 1, "T: advisory retry_after_seconds < 1", {"ra": ra})
        assert_no_side_effects(before, "T-contend")
    finally:
        holder_rc = holder.release()
        ev["holder_release_rc"] = holder_rc
    M.expect(holder_rc == 0, "T: holder ROLLBACK exited non-zero",
             {"rc": holder_rc})
    # deterministic free-proof: our NOWAIT probe succeeds again
    rc, out, err = psql_rc(nowait_sql, timeout=30)
    M.expect(rc == 0, "T: NOWAIT probe still failing after ROLLBACK — "
             "the lock is NOT free", {"stderr": M.scrub(err[-200:])})
    run1 = P.extract_run_id(trigger(owner.token, ctx["sync_id"])[1])
    final1, _ = P.poll_run(owner.token, ctx["sync_id"], run1,
                           until=lambda r: r.get("status") == "succeeded",
                           timeout=60)
    M.expect(final1 is not None, "T: post-lock run never succeeded")


# ------------------------------------------------------------------ U -------
def scenario_u(suffix, ev):
    """P23 public-response private-field sweep (root closeout-5). Routes
    are the REAL server.go:104-126 surfaces — no guessing, no skip
    fallback: GET /api/v1/data-connection/sources/{id} detail,
    /sources list, /sources/{id}/syncs list, /syncs/{id} detail,
    /syncs/{id}/runs list, /syncs/{id}/runs/{run_id} detail. Every one
    MUST answer 200, carry the target id / a non-empty actual item set,
    and pass the recursive private-key sweep (new stamp key AND frozen
    lease keys). Precondition: a REAL succeeded run with the PG stamp
    non-NULL. A fresh typed-429 body is swept too. The frozen P22
    regressions are NOT run here — the formal 10-p22-full step still
    runs the complete eleven (J/K included)."""
    ctx = admission_sync(f"p23u-{suffix}",
                         admission={"min_interval_ms": MIN_INTERVAL_MAX_MS},
                         total=1, page_size=1)
    owner = ctx["owner"]
    sid, sync_id = ctx["source"]["id"], ctx["sync_id"]

    run1 = P.extract_run_id(trigger(owner.token, sync_id)[1])
    final1, _ = P.poll_run(owner.token, sync_id, run1,
                           until=lambda r: r.get("status") == "succeeded",
                           timeout=60)
    M.expect(final1 is not None, "U: real run never succeeded")
    # precondition: the run's admission really advanced the stamp (PG)
    ev["stamp_nonnull_precondition"] = bool(stamp_of(sid))

    def get(path):
        st, body, _ = http_full("connector", path, token=owner.token,
                                method="GET")
        M.expect(st == 200, f"U: {path.rsplit('/', 1)[-1] or path} "
                 f"route != 200 (real server.go surface must answer)",
                 {"status": st, "path": path})
        assert_no_private_keys(body, f"U GET {path}")
        return body

    def ids_of(body):
        items = M.extract_items(body)
        M.expect(isinstance(items, list) and bool(items),
                 "U: list route returned no extractable items",
                 {"body_keys": sorted(body.keys())
                  if isinstance(body, dict) else type(body).__name__})
        return [str(i.get("id")) for i in items if isinstance(i, dict)]

    surfaces = []

    # 1. source detail — id verified against the target
    body = get(f"/api/v1/data-connection/sources/{sid}")
    M.expect(body.get("id") == sid, "U: source detail id mismatch",
             {"got": body.get("id")})
    surfaces.append("source_detail")
    # 2. sources list — non-empty and contains OUR source
    ids = ids_of(get("/api/v1/data-connection/sources"))
    M.expect(sid in ids, "U: sources list missing our source",
             {"count": len(ids)})
    surfaces.append("sources_list")
    # 3. sync detail
    body = get(f"/api/v1/data-connection/syncs/{sync_id}")
    M.expect(body.get("id") == sync_id, "U: sync detail id mismatch",
             {"got": body.get("id")})
    surfaces.append("sync_detail")
    # 4. per-source syncs list
    ids = ids_of(get(f"/api/v1/data-connection/sources/{sid}/syncs"))
    M.expect(sync_id in ids, "U: source syncs list missing our sync",
             {"count": len(ids)})
    surfaces.append("source_syncs_list")
    # 5. runs list
    ids = ids_of(get(f"/api/v1/data-connection/syncs/{sync_id}/runs"))
    M.expect(run1 in ids, "U: runs list missing the real run",
             {"count": len(ids), "want": run1})
    surfaces.append("runs_list")
    # 6. run detail
    body = get(f"/api/v1/data-connection/syncs/{sync_id}/runs/{run1}")
    M.expect(body.get("id") == run1, "U: run detail id mismatch",
             {"got": body.get("id")})
    surfaces.append("run_detail")
    # 7. source create echo (extra surface — never a substitute above)
    assert_no_private_keys(ctx["source"], "U source_echo")
    surfaces.append("source_echo")
    # 8. fresh typed 429 body (max-interval source -> deterministic)
    res = trigger(owner.token, sync_id)
    expect_throttle(res, "U typed-429 sweep", reason="min_interval")
    surfaces.append("throttle_body")

    ev["surfaces_checked"] = surfaces
    REQUIRED_SURFACES = {"source_detail", "sources_list", "sync_detail",
                         "source_syncs_list", "runs_list", "run_detail",
                         "throttle_body"}
    M.expect(REQUIRED_SURFACES <= set(surfaces),
             "U: a required public surface was not swept",
             {"missing": sorted(REQUIRED_SURFACES - set(surfaces))})


# ------------------------------------------------------------------ V -------
def scenario_v(suffix, ev):
    """Concurrent fire from BOTH replicas while a sampler watches the PG
    inflight count (any sample above the limit fails immediately), then
    a SERIAL fill to exactly the cap. Under true concurrency 1..cap
    admissions are legal (batch-3: do not force exactly cap concurrent);
    the serial phase proves the cap is reachable and never exceeded.
    The sampler is stopped and joined in finally; the barrier is
    released in finally. A NULL stamp before the first admission is
    legal and handled."""
    LIMIT = 2
    FIRE = 8
    ctx = admission_sync(f"p23v-{suffix}",
                         admission={"max_inflight_runs": LIMIT},
                         timeout_ms=60000)
    owner = ctx["owner"]
    ex = extra_sync(ctx, 1)
    sid = ctx["source"]["id"]
    syncs = [ctx["sync_id"], ex["sync_id"]]

    stamp_before = stamp_nullable(sid)      # NULL before first admission
    sampler = {"max": 0, "stop": False, "error": None}

    def sample():
        while not sampler["stop"]:
            try:
                sampler["max"] = max(sampler["max"], inflight_of(sid))
            except Exception as exc:  # noqa: BLE001 — recorded
                sampler["error"] = M.scrub(str(exc))[:200]
                return
            time.sleep(0.3)

    thread = threading.Thread(target=sample)
    thread.start()
    admitted = []            # (run_id, parent_sync_id)
    rejected_count = 0
    sampler_stopped = False
    try:
        P.barrier_set(ctx["src"], [1], wait_s=180)  # keep runs in-flight

        def fire(i):
            via = "connector" if i % 2 == 0 else "connector2"
            sync_id = syncs[i % len(syncs)]
            st, body, headers = trigger(owner.token, sync_id, via=via)
            # remember the PARENT sync: even same-tenant reads are
            # parent-bound (frozen P22 H) — cleanup polls via the right one
            return via, sync_id, st, body, headers

        with ThreadPoolExecutor(max_workers=FIRE) as pool:
            results = list(pool.map(fire, range(FIRE)))

        conc_admitted = 0
        for via, sync_id, st, body, headers in results:
            if st == ADMIT_STATUS:
                admitted.append((P.extract_run_id(body), sync_id))
                conc_admitted += 1
            else:
                expect_throttle((st, body, headers),
                                f"V concurrent reject via {via}")
                rejected_count += 1
        ev["concurrent_admitted"] = conc_admitted
        ev["concurrent_rejected"] = rejected_count
        M.expect(conc_admitted + rejected_count == FIRE,
                 "V: some concurrent trigger neither admitted nor typed-429")
        M.expect(1 <= conc_admitted <= LIMIT,
                 "V: concurrent admissions outside [1, cap] — under-cap "
                 "admits are legal races; zero or over-cap are not",
                 {"admitted": conc_admitted, "cap": LIMIT})

        # serial fill to exactly the cap (alternating replicas)
        serial_admitted = 0
        serial_reject = None
        for i in range(LIMIT + 2):
            via = "connector2" if i % 2 else "connector"
            st, body, headers = trigger(owner.token,
                                        syncs[i % len(syncs)], via=via)
            if st == ADMIT_STATUS:
                admitted.append((P.extract_run_id(body),
                                 syncs[i % len(syncs)]))
                serial_admitted += 1
            else:
                serial_reject = expect_throttle(
                    (st, body, headers), f"V serial reject via {via}")
                break
        ev["serial_admitted"] = serial_admitted
        ev["serial_reject_seen"] = serial_reject is not None
        M.expect(serial_reject is not None,
                 "V: serial fill never hit the typed rejection within "
                 "cap+2 triggers")
        ev["final_inflight"] = inflight_of(sid)
        M.expect(ev["final_inflight"] == LIMIT,
                 "V: inflight != cap after serial fill",
                 {"inflight": ev["final_inflight"]})
        run_ids = [rid for rid, _ in admitted]
        M.expect(len(set(run_ids)) == len(run_ids),
                 "V: duplicate run ids across 202 bodies")
        # root V pin: the source is FRESH, so the PG run set must EQUAL
        # the admitted set exactly — a typed 429 that secretly created
        # an orphan run row (present in PG, absent from the 202s) fails
        # here just like a vanished admission.
        pg_ids = set(source_run_ids(sid))
        ev["pg_ids_exact_match"] = (pg_ids == set(run_ids))
        M.expect(pg_ids == set(run_ids),
                 "V: PG run set != admitted set (typed 429 left orphan "
                 "run rows, or an admitted run vanished)",
                 {"pg_only": sorted(pg_ids - set(run_ids)),
                  "admitted_only": sorted(set(run_ids) - pg_ids)})
        M.expect(len(run_ids) == LIMIT,
                 "V: total admitted != cap after the serial fill",
                 {"admitted": len(run_ids), "cap": LIMIT})

        # the private anchor advanced across the window (PG clock); a
        # legal NULL before the first admission counts as advanced once set
        if stamp_before is None:
            stamp_after = stamp_nullable(sid)
            ev["stamp_grew"] = "null->set" if stamp_after is not None \
                else "still-null"
            M.expect(stamp_after is not None,
                     "V: admission stamp never advanced from its legal "
                     "pre-admission NULL")
        else:
            esc = stamp_before.replace("'", "''")
            grew = P.psql_connectors(
                f"SELECT ({STAMP_COLUMN} > '{esc}'::timestamptz) "
                f"FROM connections WHERE id = '{sid}'::uuid;").strip()
            ev["stamp_grew"] = grew
            M.expect(grew == "t",
                     "V: admission stamp never advanced across fires")
    finally:
        sampler["stop"] = True
        thread.join(timeout=10)
        sampler_stopped = not thread.is_alive()
        P.barrier_release(ctx["src"])
        deadline = time.monotonic() + 240
        remaining = list(admitted)
        while remaining and time.monotonic() < deadline:
            for rid, parent in list(remaining):
                try:
                    body, _ = P.poll_run(owner.token, parent, rid,
                                         until=lambda r: r.get("status")
                                         in TERMINAL_STATES, timeout=2)
                except Exception:  # noqa: BLE001
                    body = None
                if body is not None:
                    remaining.remove((rid, parent))
            time.sleep(1.0)
        ev["cleanup_left"] = len(remaining)

    ev["sampler_stopped"] = sampler_stopped
    M.expect(sampler_stopped, "V: sampler thread did not stop within bound")
    ev["sampler_max_inflight"] = sampler["max"]
    ev["sampler_error"] = sampler["error"]
    M.expect(sampler["error"] is None, "V: sampler failed",
             {"error": sampler["error"]})
    M.expect(sampler["max"] <= LIMIT,
             "V: PG inflight EXCEEDED the limit — check-then-insert race "
             "observed", {"max": sampler["max"]})
    M.expect(inflight_of(sid) == 0, "V: source still occupied after cleanup")


# ---------------------------------------------------------------- main -----
SCENARIOS = {
    "O": ("admission_capacity_shared", scenario_o),
    "P": ("admission_release_matrix", scenario_p),
    "Q": ("admission_min_interval", scenario_q),
    "R": ("denial_precede_throttle", scenario_r),
    "S": ("admission_config_validation", scenario_s),
    "T": ("contention_real_db_lock", scenario_t),
    "U": ("public_private_field_sweep", scenario_u),
    "V": ("concurrent_inflight_invariant", scenario_v),
}


def main():
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-dir", required=True)
    parser.add_argument("--only", default="O,P,Q,R,S,T,U,V")
    parser.add_argument("--connector2-base", default="http://127.0.0.1:17089")
    parser.add_argument("--connector3-base", default="http://127.0.0.1:17087")
    parser.add_argument("--dataset-proxy-base",
                        default="http://127.0.0.1:17091")
    parser.add_argument("--contract-probe", action="store_true",
                        help="run ONLY the read-mostly preflight probes")
    parser.add_argument("--allow-restart", action="store_true",
                        help="formal runs only: enables Q's connector-"
                             "restart sub-case (guarded, never the fixture)")
    ARGS = parser.parse_args()
    env_dir = Path(ARGS.env_dir)      # load_env needs a Path
    load_helpers(env_dir)
    M.wait_health()

    suffix = uuid.uuid4().hex[:8]
    SESSION["suffix"] = suffix
    if ARGS.contract_probe:
        ev = {}
        try:
            contract_probe(ev)
            print("[p23] contract-probe OK:", json.dumps(ev), flush=True)
            return 0
        except Exception as exc:  # noqa: BLE001 — reported, never hidden
            print(f"[p23] contract-probe FAIL: {M.scrub(str(exc))}",
                  flush=True)
            return 1

    wanted = [x.strip().upper() for x in ARGS.only.split(",") if x.strip()]
    unknown = [k for k in wanted if k not in SCENARIOS]
    M.expect(wanted, "--only must name at least one scenario")
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
            results[name] = {"status": "pass",
                             "seconds": round(time.time() - started, 1)}
            print(f"[p23] PASS {name} ({results[name]['seconds']}s)",
                  flush=True)
        except Exception as exc:  # noqa: BLE001 — recorded honestly
            results[name] = {"status": "fail",
                             "seconds": round(time.time() - started, 1),
                             "error": M.scrub(str(exc))}
            print(f"[p23] FAIL {name}: {M.scrub(str(exc))}", flush=True)
        evidence[name] = ev

    failed = [k for k, v in results.items() if v["status"] == "fail"]
    summary = {"suffix": suffix, "results": results, "evidence": evidence,
               "partial": not full_run,
               "official": full_run and not failed,
               "counts": {"pass": len(results) - len(failed),
                          "fail": len(failed)}}
    out = env_dir / "result_p23.json"
    out.write_text(json.dumps(summary, indent=2) + "\n")
    tag = "OFFICIAL" if summary["official"] else (
        "PARTIAL" if not failed else "FAILED")
    print(f"[p23] {tag} pass={summary['counts']['pass']} "
          f"fail={summary['counts']['fail']} (official success requires "
          f"O..V each once) -> {out}", flush=True)
    return 1 if (failed or not full_run) else 0


if __name__ == "__main__":
    sys.exit(main())

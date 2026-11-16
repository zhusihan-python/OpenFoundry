#!/usr/bin/env python3
"""OpenFoundry P2.1 lease/persistent-state HTTP acceptance.

Implements scenarios A/B/C against the CONFIRMED local contract (re-check
against the final candidate before any execution):

  * HTTP surface unchanged: POST /api/v1/data-connection/syncs/{id}/run,
    GET .../syncs/{id}/runs. NO lease HTTP endpoint (none may be invented);
    lease internals observed ONLY via this isolation stack's connectors PG.
  * Public SyncRun JSON adds requested_by/owner_id/attempt/generation.
    lease_holder/lease_token/lease_expires/heartbeat(_at) are PRIVATE DB
    columns — asserted ABSENT from every public JSON (recursive key walk);
    run_row() whitelists non-secret columns only (never selects the token).
  * Manual runs: TTL 60s, heartbeat 20s.

Scenarios (synthetic targets only; handle snapshots; zero tokens printed):
  A heartbeat_progress   — src total=1 (single record; the 25s delay hits
                           the empty terminating page), delay_from_page=2,
                           source request_timeout_ms legally above the
                           delay. The Source/Sync are created by the OWNER;
                           the run is triggered by a SAME-ORG ADMIN: the
                           final run JSON must carry owner_id == Source
                           owner's sub and requested_by == admin's sub.
                           heartbeat_at must advance in PG while
                           attempt/generation stay fixed; final 202 with DB
                           terminal state + real artifact hash match and no
                           private keys anywhere.
  B lease_loss_fencing   — 151-row src with REAL follow-on pages, delay
                           25s from page 2; the POST thread uses an
                           explicit timeout well above the 20s heartbeat
                           cadence. When fixture request_count shows page 2
                           in flight, ONLY this run's lease_expires_at is
                           moved into the past via psql. Expect HTTP 409
                           with a lease-lost marker, no further fixture
                           pages, no Dataset publish (the in-flight page
                           does not count), DB non-succeeded.
  C terminal_commit_fault — BEFORE UPDATE trigger scoped to this scenario's
                           sync_def id (dropped in finally): REST fetch and
                           Dataset publish succeed, the terminal DB write
                           faults. HTTP must answer a clear error (500-class
                           or explicit error marker — this is an injected
                           terminal-write fault, NOT lease fencing); DB must
                           stay non-terminal and must not self-heal into a
                           terminal state after the trigger is dropped; the
                           remote Dataset MAY already be committed — its
                           state is recorded, never asserted unchanged.

Exit code 0 ONLY if every scenario's assertions pass and the result file
is written with real counts.

Usage: python3 -I smoke_p21.py --env-dir <tmp> [--only A,B,C]
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import threading
import time
import urllib.error
from urllib.parse import quote
from urllib.request import Request, urlopen
import uuid

P1_SMOKE = Path(__file__).resolve().parent.parent / "business-platform-p1" / "smoke" / "smoke.py"

PRIVATE_KEYS = ("lease_holder", "lease_token", "lease_expires", "lease_expires_at",
                "heartbeat", "heartbeat_at")
PUBLIC_NEW_KEYS = ("requested_by", "owner_id", "attempt", "generation")
TERMINAL_DB_STATES = ("succeeded", "failed", "cancelled", "error")

ARGS = None
M = None  # loaded P1 module (http helpers, identities, psql, scrub, ...)


def load_p1(env_dir):
    global M
    import importlib.util
    spec = importlib.util.spec_from_file_location("p1smoke", P1_SMOKE)
    M = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(M)
    M.ARGS = type("A", (), {"env_dir": str(env_dir)})()
    M.load_env(env_dir)


def http_long(path, *, token, method="POST", payload=None, timeout):
    """Request against the connector base with an EXPLICIT timeout (the P1
    helper pins 30s, too short for the 20s-cadence heartbeat scenarios)."""
    base = M.BASES["connector"]
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Authorization": "Bearer " + token}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = Request(base + path, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return resp.status, (json.loads(body) if body else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read(4000)
        try:
            return exc.code, json.loads(raw) if raw else None
        except ValueError:
            return exc.code, {"_raw": raw.decode(errors="replace")}


def assert_no_private_keys(body, where):
    """Recursive key walk: private lease keys must be absent from every
    object level of any public JSON. On a leak the evidence carries the
    KEY and its JSON path ONLY — never body values (scrub knows JWTs, not
    opaque lease tokens, so echoing any part of the body could archive the
    token itself)."""
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


def run_row(run_id):
    """Whitelisted, non-secret projection of the run's lease columns."""
    out = psql_connectors(
        f"SELECT status, "
        f"(lease_holder IS NOT NULL), "
        f"COALESCE(heartbeat_at::text, ''), "
        f"COALESCE(lease_expires_at::text, ''), "
        f"(lease_token IS NOT NULL) "
        f"FROM sync_runs WHERE id = '{run_id}'::uuid;")
    M.expect(out and out.count("|") == 4, f"sync_runs row unreadable for {run_id}", out)
    status, has_holder, heartbeat, expires, has_token = out.split("|")
    return {"status": status, "has_holder": has_holder == "t",
            "heartbeat": heartbeat, "expires": expires, "has_token_col": has_token == "t"}


def wait_run_row(run_id, timeout=30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return run_row(run_id)
        except Exception:
            time.sleep(0.5)
    raise AssertionError(f"run row never appeared in PG: {run_id}")


SAFE_FIELDS = ("id", "status", "requested_by", "owner_id", "attempt", "generation")


def safe(obj):
    """Whitelist projection for evidence: never raw bodies/whole rows."""
    if isinstance(obj, dict):
        return {k: obj.get(k) for k in SAFE_FIELDS if k in obj}
    return {k: getattr(obj, "get", lambda _k=None: None)(k) for k in SAFE_FIELDS}


def runs_list(token, sync_id):
    res = M.api("connector", "sync_runs", path_args={"id": sync_id}, token=token)
    M.expect(res.status == 200, "sync runs list failed", {"status": res.status})
    assert_no_private_keys(res.body, "sync runs list")  # checked BEFORE use
    items = M.extract_items(res.body) or []
    return res.body, items


def find_run_in_list(token, sync_id):
    _, items = runs_list(token, sync_id)
    return items[0] if items else None


def slow_source_config(src, *, timeout_ms, page_size):
    payload = M.source_payload(f"p21-{src}", src)
    payload["config"]["sync"]["limits"]["request_timeout_ms"] = timeout_ms
    payload["config"]["sync"]["pagination"]["page_size"] = page_size
    return payload


def create_source_raw(token, payload):
    res = M.api("connector", "src_create", token=token, payload=payload)
    M.expect(res.status in (200, 201) and res.body.get("id"),
             "source create failed", {"status": res.status,
                                       "body": M.scrub(str(res.body))[:400]})
    return res.body


# ------------------------------------------------------------------ A -----
def scenario_a(suffix, ev):
    org = str(uuid.uuid4())
    owner = M.Identity("p21a-owner", org=org)
    admin = M.Identity("p21a-admin", org=org, roles=["admin"])

    src = f"p21a-{suffix}"
    M.fixture_admin(src, {"mode": "ok", "content": 1, "total": 1, "page_size": 1,
                          "delay_ms": 25000, "delay_from_page": 2})
    source = create_source_raw(owner.token, slow_source_config(src, timeout_ms=40000, page_size=1))
    target = M.create_dataset(owner.token, f"p21a-out-{suffix}", "json")
    sync = M.create_sync(owner.token, source["id"], target["id"])
    M.expect(sync.status in (200, 201), "A: sync create failed", {"status": sync.status})

    result = {}

    def trigger():
        status, body = http_long(f"/api/v1/data-connection/syncs/{sync.body['id']}/run",
                                 token=admin.token, payload={}, timeout=120)
        result["status"], result["body"] = status, body

    th = threading.Thread(target=trigger)
    started = time.monotonic()
    th.start()

    # Gate 1: confirm the delayed page 2 is IN FLIGHT (request_count counts
    # at request entry). The heartbeat proof only starts once the pull is
    # provably parked inside the 25s wait — otherwise a fast pre-commit
    # renew could masquerade as periodic heartbeat.
    inflight = False
    t_inflight = None
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        snap = M.http("fixture", f"/admin/src/{quote(src)}").body
        if snap and snap.get("request_count", 0) >= 2:
            inflight = True
            t_inflight = time.monotonic()
            break
        time.sleep(0.2)
    M.expect(inflight, "A: delayed page-2 in-flight signal never observed")

    # Gate 2: baseline row0/list0 (claimed & running) taken only AFTER the
    # in-flight gate, and REQUIRED to initialize — not just run_id present.
    row0, list0, run_id = None, None, None
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and th.is_alive():
        candidate = find_run_in_list(owner.token, sync.body["id"])
        if candidate and candidate.get("id"):
            run_id = candidate["id"]
            try:
                row = run_row(run_id)  # raises until the PG row exists
            except AssertionError:
                row = None  # row-not-yet-present is the ONLY retriable case
            if row and row["heartbeat"] and row["has_holder"] and row["status"] == "running":
                row0, list0 = row, candidate
                break
        time.sleep(0.5)
    M.expect(row0 is not None and list0 is not None and run_id is not None,
             "A: baseline row0/list0 (claimed & running) never initialized",
             {"run_id_found": run_id is not None})

    # Gate 3: accept a heartbeat advance ONLY while (a) the POST is still
    # in flight, (b) the DB row is still running, and (c) the observation
    # window closes BEFORE the 25s empty page returns (margin 3s) — so a
    # final pre-commit renew after the page returned cannot pass as a
    # periodic heartbeat. The comparison itself is done by Postgres
    # (heartbeat_at > old_value), never by string inequality.
    ADVANCE_WINDOW_S = 25 - 3
    advanced, t_advance = False, None
    deadline = t_inflight + ADVANCE_WINDOW_S
    while time.monotonic() < deadline:
        if not th.is_alive():
            break  # POST finished before any in-window advance: proof fails
        row = run_row(run_id)
        if row["status"] != "running":
            break  # left running before an in-window advance: proof fails
        old = row0["heartbeat"].replace("'", "''")
        got = psql_connectors(
            f"SELECT (heartbeat_at > '{old}'::timestamptz) "
            f"FROM sync_runs WHERE id = '{run_id}'::uuid;")
        if got.strip() == "t":
            advanced, t_advance = True, time.monotonic()
            break
        time.sleep(1.0)
    ev["heartbeat_advanced"] = advanced
    ev["elapsed_at_advance_s"] = (round(t_advance - t_inflight, 1)
                                  if t_advance else None)
    M.expect(advanced,
             "A: no heartbeat advance observed while POST in flight, row "
             "running, and strictly before the delayed page returned "
             "(a pre-commit-only renewer must FAIL here)",
             {"window_s": ADVANCE_WINDOW_S,
              "elapsed_at_advance_s": ev["elapsed_at_advance_s"]})
    candidate = find_run_in_list(owner.token, sync.body["id"])
    assert_no_private_keys(candidate, "A runs-list after heartbeat")
    ev["attempt_generation_stable"] = (
        isinstance(candidate.get("attempt"), int) and isinstance(candidate.get("generation"), int)
        and candidate.get("attempt") == list0.get("attempt")
        and candidate.get("generation") == list0.get("generation"))
    M.expect(ev["attempt_generation_stable"],
             "A: attempt/generation missing or changed during the run",
             {"first": {k: list0.get(k) for k in ("attempt", "generation")},
              "now": {k: candidate.get(k) for k in ("attempt", "generation")}})

    th.join(timeout=130)
    M.expect(not th.is_alive(), "A: trigger POST thread never finished")
    final_json = result.get("body") or {}
    assert_no_private_keys(final_json, "A POST response")  # BEFORE any other use
    M.expect(result.get("status") == 202, "A: POST run != 202",
             {"status": result.get("status")})
    M.expect(final_json.get("requested_by") == admin.sub,
             "A: requested_by != triggering admin sub",
             {"got": final_json.get("requested_by"), "want_admin": "<sub recorded>"})
    M.expect(final_json.get("owner_id") == owner.sub,
             "A: owner_id != Source owner sub", {"got": final_json.get("owner_id")})
    for key in PUBLIC_NEW_KEYS:
        M.expect(key in final_json, f"A: public key {key} missing from final run JSON")
    # DB terminal + real artifact hash三方一致 via the P1 run-bound download
    row = run_row(run_id)
    M.expect(row["status"] == "succeeded", "A: DB not succeeded after 202", row["status"])
    run_json, _ = runs_list(owner.token, sync.body["id"])
    assert_no_private_keys(run_json, "A final runs list")
    rows, _file, _raw = M.download_run_artifact(owner.token, target["id"], final_json)
    M.expect(rows == [M.expected_record(1, 1)], "A: artifact rows mismatch", rows)
    ev["owner_vs_requester"] = {"owner_is_source_owner": True, "requester_is_admin": True}
    ev["elapsed_s"] = round(time.monotonic() - started, 1)


# ------------------------------------------------------------------ B -----
def scenario_b(suffix, ev):
    org = str(uuid.uuid4())
    owner = M.Identity("p21b-owner", org=org)

    src = f"p21b-{suffix}"
    M.fixture_admin(src, {"mode": "ok", "content": 1, "total": 151, "page_size": 50,
                          "delay_ms": 25000, "delay_from_page": 2})
    source = create_source_raw(owner.token, slow_source_config(src, timeout_ms=40000, page_size=50))
    target = M.create_dataset(owner.token, f"p21b-out-{suffix}", "json")
    sync = M.create_sync(owner.token, source["id"], target["id"])
    M.expect(sync.status in (200, 201), "B: sync create failed", {"status": sync.status})

    result = {}

    def trigger():
        status, body = http_long(f"/api/v1/data-connection/syncs/{sync.body['id']}/run",
                                 token=owner.token, payload={}, timeout=120)
        result["status"], result["body"] = status, body

    th = threading.Thread(target=trigger)
    th.start()

    # wait until page 2 is IN FLIGHT (request_count pre-wait increment);
    # a timeout here is a FAIL — injecting expiry without a confirmed
    # in-flight page would prove nothing.
    deadline = time.monotonic() + 30
    inflight = False
    while time.monotonic() < deadline:
        snap = M.http("fixture", f"/admin/src/{quote(src)}").body
        if snap and snap.get("request_count", 0) >= 2:
            inflight = True
            break
        time.sleep(0.3)
    M.expect(inflight, "B: page-2 in-flight signal (request_count>=2) never observed")
    candidate = find_run_in_list(owner.token, sync.body["id"])
    M.expect(candidate and candidate.get("id"), "B: run not listed while in flight")
    run_id = candidate["id"]
    assert_no_private_keys(candidate, "B runs-list in flight")

    # inject expiry INTO THE PAST for exactly this run
    psql_connectors(
        f"UPDATE sync_runs SET lease_expires_at = NOW() - INTERVAL '5 seconds' "
        f"WHERE id = '{run_id}'::uuid;")
    ev["expiry_injected"] = True
    count_at_inject = M.http("fixture", f"/admin/src/{quote(src)}").body["request_count"]

    th.join(timeout=130)
    M.expect(not th.is_alive(), "B: trigger POST thread never finished")
    status, body = result.get("status"), result.get("body") or {}
    assert_no_private_keys(body, "B POST error response")  # BEFORE any other use
    blob = json.dumps(body)
    ev["http_status"] = status
    ev["lease_lost_marker"] = "lease" in blob.lower()
    M.expect(status == 409 and "lease" in blob.lower(),
             "B: expected HTTP 409 with a lease-lost marker",
             {"status": status, "lease_marker": ev["lease_lost_marker"]})

    # no further fixture pages after the injection window (page 2 delay 25s)
    time.sleep(27)
    count_after = M.http("fixture", f"/admin/src/{quote(src)}").body["request_count"]
    ev["pages_before_inject"] = count_at_inject
    ev["pages_after_wait"] = count_after
    # STRICT equality: at_inject already includes the in-flight page 2, so
    # any increase means page 3 was fetched after the lease was lost.
    M.expect(count_after == count_at_inject,
             "B: further fixture pages were fetched after lease loss",
             {"at_inject": count_at_inject, "after": count_after})
    # no Dataset publish (in-flight page does not count as one)
    files = M.list_files(owner.token, target["id"])
    ev["target_files"] = len(files)
    M.expect(not files, "B: target received published files after lease loss", files)
    row = run_row(run_id)
    ev["db_status"] = row["status"]
    M.expect(row["status"] not in ("succeeded",), "B: DB recorded succeeded after lease loss", row["status"])
    assert_no_private_keys(runs_list(owner.token, sync.body["id"])[0], "B final runs list")


# ------------------------------------------------------------------ C -----
P21C_FUNCTION = "p21_c_terminal_fault"
P21C_TRIGGER = "p21_c_terminal_fault_trg"
P21C_FAULT_MARKER = "p21 injected terminal-state fault"


def scenario_c(suffix, ev):
    """Injected terminal-state write fault — the run MUST have actually
    reached the "Dataset committed, Connector unconfirmed" state before
    the fault can count: REST fetched, Dataset publish COMMITTED and
    bound to THIS run, and only the Connector terminal write failed.
    Anything less (no real publish, remote_files_after == 0) is a FAIL,
    never a green result."""
    org = str(uuid.uuid4())
    owner = M.Identity("p21c-owner", org=org)

    src = f"p21c-{suffix}"
    M.fixture_admin(src, {"mode": "ok", "content": 1, "total": 151, "page_size": 50})
    source = create_source_raw(owner.token, M.source_payload(f"p21-c-{suffix}", src))
    target = M.create_dataset(owner.token, f"p21c-out-{suffix}", "json")
    sync = M.create_sync(owner.token, source["id"], target["id"])
    M.expect(sync.status in (200, 201), "C: sync create failed", {"status": sync.status})
    sync_def_id = sync.body["id"]

    target_before = M.snapshot_dataset(owner.token, target["id"])
    ev["files_before"] = len(target_before["ids"])

    psql_connectors(
        f"CREATE OR REPLACE FUNCTION {P21C_FUNCTION}() RETURNS trigger AS $$ "
        f"BEGIN "
        f"  IF NEW.sync_def_id::text = '{sync_def_id}' "
        f"     AND NEW.status IN ('succeeded','failed','cancelled','error') THEN "
        f"    RAISE EXCEPTION '{P21C_FAULT_MARKER}'; "
        f"  END IF; "
        f"  RETURN NEW; "
        f"END $$ LANGUAGE plpgsql;")
    psql_connectors(
        f"CREATE TRIGGER {P21C_TRIGGER} BEFORE UPDATE ON sync_runs "
        f"FOR EACH ROW EXECUTE FUNCTION {P21C_FUNCTION}();")
    try:
        status, body = http_long(f"/api/v1/data-connection/syncs/{sync_def_id}/run",
                                 token=owner.token, payload={}, timeout=90)
        assert_no_private_keys(body, "C POST error response")  # BEFORE any other use
        blob = json.dumps(body or {})
        ev["http_status"] = status
        # (1) explicit 5xx terminal-persist error carrying THIS injection's
        # marker — an unrelated 500 is not accepted as the fault proof.
        ev["fault_marker_present"] = P21C_FAULT_MARKER in blob
        M.expect(status >= 500 and P21C_FAULT_MARKER in blob,
                 "C: HTTP did not surface the injected terminal-persist fault "
                 "(needs 5xx AND this run's injection marker)",
                 {"status": status, "marker": ev["fault_marker_present"]})

        run_id_c = body.get("id") if isinstance(body, dict) else None
        if not run_id_c:
            listed = find_run_in_list(owner.token, sync_def_id)
            run_id_c = listed["id"] if listed else None
        M.expect(run_id_c, "C: could not resolve the run id")
        row = wait_run_row(run_id_c)
        # (2) DB truth: still non-terminal, lease claimed (holder column
        # set), and NO fabricated dataset_version_id in the public JSON.
        ev["db_status"] = row["status"]
        ev["lease_claimed"] = row["has_holder"]
        M.expect(row["status"] == "running",
                 "C: DB status must be exactly 'running' (claimed, not aborted "
                 "or any other non-terminal value)", row["status"])
        M.expect(row["has_holder"] and row["has_token_col"],
                 "C: lease not fully claimed (holder/token column missing)",
                 {"has_holder": row["has_holder"], "has_token_col": row["has_token_col"]})
        public_run = find_run_in_list(owner.token, sync_def_id) or {}
        ev["attempt"] = public_run.get("attempt")
        ev["generation"] = public_run.get("generation")
        M.expect(isinstance(public_run.get("attempt"), int) and public_run.get("attempt") >= 1
                 and isinstance(public_run.get("generation"), int) and public_run.get("generation") >= 1,
                 "C: attempt/generation not fully claimed (need ints >= 1)",
                 safe(public_run))
        M.expect(not public_run.get("dataset_version_id"),
                 "C: public run carries a fabricated dataset_version_id "
                 "although the terminal write faulted",
                 public_run.get("dataset_version_id"))
        assert_no_private_keys(public_run, "C public run row")

        # (3) THE core anti-false-positive proof: the Dataset really holds a
        # COMMITTED artifact bound to THIS run. No publish => FAIL.
        files_after = M.list_files(owner.token, target["id"])
        ev["files_after"] = len(files_after)
        M.expect(len(target_before["ids"]) == 0 and len(files_after) == 1,
                 "C: expected exactly ONE new committed file on the target "
                 "(REST->Dataset publish must have really happened)",
                 {"before": len(target_before["ids"]), "after": len(files_after)})
        file_row = files_after[0]
        ev["logical_path"] = file_row.get("logical_path")
        M.expect(str(file_row.get("logical_path", "")).find(str(run_id_c)) >= 0,
                 "C: file logical_path is not bound to this run id",
                 {"logical_path": file_row.get("logical_path"), "run": run_id_c})
        ev["txn_bound"] = bool(file_row.get("transaction_id"))
        txn_id = file_row.get("transaction_id")
        M.expect(txn_id, "C: published file has no transaction binding")
        txn = M.api("dataset", "ds_txn_get",
                    path_args={"id": target["id"], "branch": "main", "txn": txn_id},
                    token=owner.token)
        M.expect(txn.status == 200, "C: transaction GET failed", {"status": txn.status})
        txn_body = txn.body or {}
        ev["txn_status"] = str(txn_body.get("status", "")).upper()
        M.expect(ev["txn_status"] == "COMMITTED",
                 "C: dataset transaction is not COMMITTED", ev["txn_status"])
        lineage = (txn_body.get("metadata") or {}).get("lineage") or {}
        ev["lineage_run_match"] = lineage.get("sync_run_id") == run_id_c
        M.expect(ev["lineage_run_match"],
                 "C: transaction lineage not bound to this run",
                 {"lineage_run": lineage.get("sync_run_id"), "run": run_id_c})
        dl = M.download_file(owner.token, target["id"], file_row["id"])
        M.expect(dl.status == 200, "C: artifact download failed", {"status": dl.status})
        import hashlib
        digest = hashlib.sha256(dl.body).hexdigest()
        ev["artifact_sha256_matches_txn"] = digest == (txn_body.get("metadata") or {}).get("content_hash")
        M.expect(ev["artifact_sha256_matches_txn"],
                 "C: artifact sha256 != transaction metadata.content_hash")
        rows = [json.loads(line) for line in dl.body.splitlines() if line.strip()]
        M.expect(rows == M.expected_records(151, 1),
                 "C: artifact rows do not match the source value-by-value",
                 {"rows": len(rows)})
        ev["fact"] = ("Dataset COMMITTED and run-bound while the Connector "
                      "terminal write faulted — 'committed-but-unconfirmed'; "
                      "no auto re-run is expected or performed")
    finally:
        psql_connectors(f"DROP TRIGGER IF EXISTS {P21C_TRIGGER} ON sync_runs;")
        psql_connectors(f"DROP FUNCTION IF EXISTS {P21C_FUNCTION}();")

    # after the trigger is gone the run must NOT self-heal into a terminal
    # state on its own (no background recovery is in P2.1 scope)
    time.sleep(5)
    final_row = run_row(run_id_c)
    ev["db_status_after_unblock"] = final_row["status"]
    M.expect(final_row["status"] == ev["db_status"] and final_row["status"] not in TERMINAL_DB_STATES,
             "C: run auto-transitioned after the fault was removed",
             {"during": ev["db_status"], "after": final_row["status"]})


# ---------------------------------------------------------------- main ----
SCENARIOS = {"A": ("heartbeat_progress", scenario_a),
             "B": ("lease_loss_fencing", scenario_b),
             "C": ("terminal_commit_fault", scenario_c)}


def main():
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-dir", required=True)
    parser.add_argument("--only", default="A,B,C")
    ARGS = parser.parse_args()
    env_dir = Path(ARGS.env_dir)
    load_p1(env_dir)
    M.wait_health()

    suffix = uuid.uuid4().hex[:8]
    wanted = [x.strip().upper() for x in ARGS.only.split(",") if x.strip()]
    unknown = [k for k in wanted if k not in SCENARIOS]
    M.expect(wanted, "--only must name at least one scenario (empty subset rejected)")
    M.expect(not unknown, "--only contains unknown scenarios", unknown)
    M.expect(len(wanted) == len(set(wanted)),
             "--only contains duplicate scenarios", wanted)
    full_run = set(wanted) == set(SCENARIOS)
    results, evidence = {}, {}
    for key in wanted:
        name, fn = SCENARIOS[key]
        started = time.time()
        ev = {}
        try:
            fn(suffix, ev)
            results[name] = {"status": "pass", "seconds": round(time.time() - started, 1)}
            print(f"[p21] PASS {name} ({results[name]['seconds']}s)", flush=True)
        except Exception as exc:  # AssertionError / RuntimeError / HTTP issues
            results[name] = {"status": "fail", "seconds": round(time.time() - started, 1),
                             "error": M.scrub(str(exc))}
            print(f"[p21] FAIL {name}: {M.scrub(str(exc))}", flush=True)
        evidence[name] = ev

    failed = [k for k, v in results.items() if v["status"] == "fail"]
    summary = {"suffix": suffix, "results": results, "evidence": evidence,
               "partial": not full_run,
               "official": full_run and not failed,
               "counts": {"pass": len(results) - len(failed), "fail": len(failed)}}
    out = env_dir / "result_p21.json"
    blob = json.dumps(summary)
    for priv in PRIVATE_KEYS:
        # evidence must never carry raw token material; keys are allowed as
        # booleans/notes but raw VALUES never enter evidence by construction
        pass
    out.write_text(json.dumps(summary, indent=2) + "\n")
    tag = "OFFICIAL" if summary["official"] else ("PARTIAL" if not failed else "FAILED")
    print(f"[p21] {tag} pass={summary['counts']['pass']} fail={summary['counts']['fail']} "
          f"(official success requires A+B+C each once) -> {out}", flush=True)
    return 1 if (failed or not full_run) else 0


if __name__ == "__main__":
    sys.exit(main())

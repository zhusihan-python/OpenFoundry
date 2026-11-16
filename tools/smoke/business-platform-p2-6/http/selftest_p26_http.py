#!/usr/bin/env python3
"""P2.6 W7 pure selftests (python3 -I -B; NO sockets, NO docker, NO
services, NO real HTTP).

R2 additions proved here (mechanics only):
  * CLI verdict: rc 0 IFF outcome==pass AND cleanup_verified (pure
    live_exit_code matrix incl. fail-cleanup downgrade);
  * origin binding: resolve_bases enforces Root-approved origins
    (userinfo/query/fragment/old ports rejected; flags must EQUAL);
  * every scenario request — polls included — is its own recorded
    evidence step (no unrecorded requests anywhere);
  * deterministic kill window: hold_drop_once parks the real 201 while
    the owned ConnectorOps stop/restart runs (boot id changes, stale
    auth 401 zero-outward, fresh lookup-only finalize);
  * in-flight receipt 404 (real barrier), two-instance claim exclusivity
    (loser typed busy zero-outward), budget-3 persistence across
    restart with lookup-only convergence, A→B→recover-A head/CP
    non-rewind, refusal group incl. owned tamper_receipt mode;
  * safe digests survive redaction (exact sensitive keys only).

Fake-stack scenario runs are MECHANICS ONLY, never scenario results.
"""

import base64
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import uuid
from pathlib import Path

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
FIXTURE_DIR = HERE.parent / "fixture"
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(FIXTURE_DIR))

import p26_http_lib as lib
from p26_http_lib import (BoundedEvent, BoundedWait, EvidenceDir,
                          HTTPResponse, ScenarioAssertionError, key_hash,
                          opaque_digest, sha256_hex)
import p26_rest_source as fixture_mod
import p26_dataset_fault_proxy as proxy_mod
import p26_scenarios as scen
import p26_owned_ops as owned_ops

RESULTS = HERE.parent / "results" / "w7"

ADMIN_TOKEN = "local-admin-credential-XYZ"     # in-memory only
PLATFORM_JWT = "platform-org-JWT-abc.def.ghi"  # never to fixture/proxy
# the connector runtime's own outbound credential (an org-valid JWT in
# the fake world; distinct from the harness platform JWT so destination
# assertions can tell them apart)
CONNECTOR_AUTH = "Bearer platform-org-JWT-connector-runtime"

CHECKS = []


def check(name):
    def deco(fn):
        CHECKS.append((name, fn))
        return fn
    return deco


class Fail(Exception):
    pass


def expect(cond, what):
    if not cond:
        raise Fail(what)


# ==========================================================================
# in-memory fake stack (transport-level; zero sockets)
# ==========================================================================

class FakeDVS:
    """Keyed W1-semantics dataset service. Cross-scope: receipt GETs
    with a foreign credential are 404 (indistinguishable from unknown);
    commits require a platform/fresh Bearer credential."""

    OWN_PREFIXES = ("Bearer platform-org-JWT",)

    def __init__(self):
        self.datasets = {}
        self.commit_calls = []
        self.headers_seen = []
        self.files_by_id = {}               # fid -> content bytes

    def _scope(self, headers):
        auth = headers.get("Authorization") or ""
        if not auth:
            return "missing"
        if auth in (f"Bearer {PLATFORM_JWT}", CONNECTOR_AUTH):
            return "writer"
        if auth == "Bearer sigvalid-readonly-sameTenant":
            return "readonly"
        if auth == "Bearer sigvalid-foreignTenant":
            return "foreign"
        return "invalid"

    def _own_auth(self, headers):
        return self._scope(headers) in ("writer", "readonly")

    def handle(self, method, path, query, body, headers):
        self.headers_seen.append({"method": method, "path": path,
                                  "authorization":
                                      headers.get("Authorization")})
        if method == "POST" and path == "/api/v1/datasets":
            did = str(uuid.uuid4())
            self.datasets[did] = {"name": _name(body), "versions": [],
                                  "files": [], "receipts": {},
                                  "next_file": 1}
            return 201, {"id": did}
        import re
        branch = re.match(r"^/v1/datasets/([0-9a-f-]{36})/branches/main$", path)
        if method == "GET" and branch:
            ds = self.datasets.get(branch.group(1))
            if ds is None:
                return 404, {"error": "dataset not found"}
            return 200, {"head_transaction_id": (ds["versions"][-1]["transaction_id"] if ds["versions"] else None)}
        mm = re.match(r"^/api/v1/datasets/([0-9a-f-]{36})"
                      r"/outputs:commit$", path)
        if method == "POST" and mm:
            if not self._own_auth(headers):
                return 401, {"error": "authentication required"}
            return self.commit(mm.group(1), body or b"")
        mm = re.match(r"^/api/v1/datasets/([0-9a-f-]{36})"
                      r"/outputs/commits/([A-Za-z0-9._~:=-]+)$", path)
        if method == "GET" and mm:
            scope = self._scope(headers)
            if scope in ("missing", "invalid"):
                return 401, {"error": "authentication required"}
            if scope == "foreign":
                return 404, {"error": "no committed receipt"}
            did, key = mm.group(1), mm.group(2)
            rec = self.datasets.get(did, {}).get("receipts", {}).get(key)
            if rec is None:
                return 404, {"error": "no committed receipt"}
            return 200, json.loads(rec["response_bytes"])
        mm = re.match(r"^/api/v1/datasets/([0-9a-f-]{36})/versions$",
                      path)
        if method == "GET" and mm:
            # ACTUAL shape: models.Page{data,next_cursor,has_more}
            vs = self.datasets.get(mm.group(1), {}).get("versions", [])
            return 200, {"data": vs, "has_more": False, "next_cursor":
                         None}
        mm = re.match(r"^/api/v1/datasets/([0-9a-f-]{36})/files$", path)
        if method == "GET" and mm:
            # ACTUAL envelope: files + branch + total
            fl = [{"id": f["id"], "logical_path": f["logical_path"]}
                  for f in self.datasets.get(mm.group(1), {}).get(
                      "files", [])]
            return 200, {"files": fl, "branch": "main", "total": len(fl)}
        mm = re.match(r"^/api/v1/datasets/([0-9a-f-]{36})"
                      r"/files/([^/]+)/download$", path)
        if method == "GET" and mm:
            # ACTUAL protocol: 302 presigned local-fs Location
            fid = mm.group(2)
            content = self.files_by_id.get(fid)
            if content is None:
                return 404, {"error": "no such file"}
            location = (f"http://{FakeStackTransport.DVS_FILES}"
                        f"/v1/_internal/local-fs/{fid}.bin"
                        f"?sig={FakeStackTransport.PRESIGN_SECRET}"
                        f"&expires=1731619200")
            return 302, b"", {"Location": location}
        return 404, {"error": "not found"}

    def commit(self, did, body):
        self.commit_calls.append({"dataset": did,
                                  "body_sha256": sha256_hex(body)})
        ds = self.datasets.setdefault(
            did, {"versions": [], "files": [], "receipts": {},
                  "next_file": 1})
        doc = json.loads(body)
        key = doc.get("idempotency_key")
        meta = doc.get("metadata") or {}
        ndjson = base64.b64decode(
            (doc.get("files") or [{}])[0].get("content_base64") or "")
        if not key:
            txn = str(uuid.uuid4())
            ds["versions"].append({"transaction_id": txn})
            fid = f"f{ds['next_file']}"
            ds["files"].append({"id": fid,
                                "logical_path":
                                    (doc.get("files") or [{}])[0].get(
                                        "logical_path"),
                                "content": ndjson})
            self.files_by_id[fid] = ndjson
            ds["next_file"] += 1
            return 200, {"dataset_id": did,
                         "transaction": {"id": txn, "status": "COMMITTED",
                                         "metadata": meta}}
        receipt = ds["receipts"].get(key)
        if receipt is not None:
            if receipt["fingerprint"] != sha256_hex(body):
                return 409, {"error": "idempotency key is bound to a "
                                      "different request body",
                             "code": "IDEMPOTENCY_KEY_CONFLICT"}
            return 200, json.loads(receipt["response_bytes"])
        txn = str(uuid.uuid4())
        ds["versions"].append({"transaction_id": txn})
        fid = f"f{ds['next_file']}"
        ds["files"].append({"id": fid,
                            "logical_path":
                                (doc.get("files") or [{}])[0].get(
                                    "logical_path"),
                            "content": ndjson})
        self.files_by_id[fid] = ndjson
        ds["next_file"] += 1
        resp = {"dataset_id": did, "branch": doc.get("branch"),
                "transaction": {"id": txn, "status": "COMMITTED",
                                "metadata": meta},
                "idempotency_key": key,
                "request_fingerprint": sha256_hex(body)}
        resp_bytes = json.dumps(resp, separators=(",", ":"),
                                sort_keys=True).encode()
        ds["receipts"][key] = {"fingerprint": sha256_hex(body),
                               "response_bytes": resp_bytes}
        return 201, json.loads(resp_bytes)


def _name(body):
    try:
        return json.loads(body or b"{}").get("name")
    except ValueError:
        return None


class FakeExecutorStopped(Exception):
    """Pure-only lost boot fence; not a product exception."""


class FakeConnector:
    """Scripted connector mirroring ACTUAL contracts (r3 memo):
      * JWT verification is STATELESS — a valid writer Bearer stays
        valid across restarts; what restart forbids is AUTOMATIC
        adoption (no local arm ⇒ no automatic remote/budget/claim),
        observed as frozen counters WITHOUT any manual call;
      * counters live in run.recovery.{send_attempts,query_attempts};
        the recovery response is flat run fields + recovery with NO outcome field;
        already-resolved = real 200 with the unchanged row and ZERO
        outward requests;
      * recovery claims are exclusive (first live claim wins; the loser
        gets a typed busy with zero outward requests) — observed at the
        OWNED proxy by holding the winner's receipt GET response;
      * receipt-404 + remaining budget ⇒ SAME-key byte-identical
        resend; a crashed executor (lease lost) never finalizes;
      * auth classification: missing/invalid Bearer 401, valid
        read-only same-tenant 403, valid foreign-tenant 404."""

    MAX_SENDS = 3

    def __init__(self, transport, proxy_base, behavior="manual",
                 refetch_on_recover=False):
        self.transport = transport
        self.proxy_base = proxy_base
        self.behavior = behavior
        self.refetch_on_recover = refetch_on_recover
        self.sources = {}
        self.defs = {}
        self.runs = {}
        self.intents = {}                   # run_id -> frozen intent
        self._claims = {}                   # run_id -> holder instance
        self._claims_lock = threading.Lock()
        self.boot_ids = {"a": str(uuid.uuid4()),
                         "b": str(uuid.uuid4())}
        self.boot_epoch = 0
        self.executor_alive = True

    # ---- classification (fake stand-ins for Root-provided signed JWTs)
    def classify(self, auth):
        if not auth:
            return "missing"
        if auth in (f"Bearer {PLATFORM_JWT}", CONNECTOR_AUTH):
            return "writer"
        if auth == "Bearer sigvalid-readonly-sameTenant":
            return "readonly"
        if auth == "Bearer sigvalid-foreignTenant":
            return "foreign"
        return "invalid"

    # ---- ops surface ------------------------------------------------------
    def ops_stop(self, instance="a"):
        self.executor_alive = False                 # SIGKILL semantics

    def ops_restart(self, instance="a"):
        self.boot_ids[instance] = str(uuid.uuid4())  # new Docker boot
        self.boot_epoch += 1
        self.executor_alive = True
        # NOTE: NO credential invalidation — JWTs are stateless.

    # ---- HTTP surface ----------------------------------------------------
    def handle(self, method, path, query, body, headers, instance="a"):
        if method == "GET" and path == "/healthz":
            return (200, {"ok": True}) if self.executor_alive else (503, {"ok": False})
        if method == "POST" and path == "/api/v1/data-connection/sources":
            doc = json.loads(body or b"{}")
            sid = str(uuid.uuid4())
            self.sources[sid] = {"id": sid, "name": doc.get("name"),
                                 "config": doc.get("config") or {}}
            return 201, self.sources[sid]
        if method == "POST" and path == "/api/v1/data-connection/syncs":
            doc = json.loads(body or b"{}")
            did = str(uuid.uuid4())
            self.defs[did] = {"id": did,
                              "source_id": doc.get("source_id"),
                              "dataset": doc.get("output_dataset_id"),
                              "cp_token": None, "epoch": 0,
                              "last_confirmed": None}
            return 201, self.defs[did]
        if method == "POST" and path.endswith("/run"):
            if not self.executor_alive:
                return 503, {"error": "connector stopped"}
            def_id = path.split("/syncs/")[1].split("/run")[0]
            d = self.defs.get(def_id)
            if d is None:
                return 404, {"error": "unknown def"}
            run_id = str(uuid.uuid4())
            cfg = self.sources[d["source_id"]]["config"]
            recovery = (cfg.get("sync") or {}).get("recovery", {}) \
                .get("enabled", False)
            run = {"id": run_id, "sync_def_id": def_id, "status":
                   "pending", "rows_written": 0, "bytes_written": 0,
                   "publish_entered_at": None, "content_hash": None,
                   "dataset_version_id": None, "generation": 1,
                   "recovery": self._new_recovery(recovery)}
            self.runs[run_id] = run
            th = threading.Thread(target=self._execute_async,
                                  args=(def_id, run_id), daemon=True)
            th.start()
            return 202, run
        if method == "POST" and path.endswith("/recover"):
            parts = path.split("/syncs/")[1].split("/")
            return self._recover(parts[0], parts[2], headers, instance)
        if method == "GET" and "/runs/" in path:
            parts = path.split("/syncs/")[1].split("/")
            run = self.runs.get(parts[2])
            return (200, run) if run else (404, {"error": "no run"})
        if method == "GET" and path.endswith("/checkpoint"):
            def_id = path.split("/syncs/")[1].split("/checkpoint")[0]
            d = self.defs.get(def_id)
            if d is None:
                return 404, {"error": "not initialized"}
            active = next((r for r in self.runs.values() if r["sync_def_id"] == def_id
                           and r["status"] in ("pending", "running")), None)
            return 200, {"mode": "cursor", "initialized": d["cp_token"] is not None,
                         **({"owner": {"run_id": active["id"]}} if active else {}),
                         "confirmed_epoch": d["epoch"],
                         "last_confirmed": d["last_confirmed"]}
        return 404, {"error": "not found"}

    def _new_recovery(self, enabled):
        return {"protocol": "p26-v1" if enabled else None,
                "state": "running" if enabled else None,
                "reason": None, "next_action_at": None,
                "body_len": None, "checkpoint_len": None,
                "txn_id": None, "resolved_at": None,
                "send_attempts": 0, "query_attempts": 0}

    # ---- run execution (async worker) ------------------------------------
    def _execute_async(self, def_id, run_id):
        run = self.runs[run_id]
        executor_boot = self.boot_epoch
        try:
            d = self.defs[def_id]
            cfg = self.sources[d["source_id"]]["config"]
            sync = cfg.get("sync") or {}
            incremental = sync.get("incremental", {}).get("enabled",
                                                          False)
            run["status"] = "running"
            records, new_token = self._pull(
                cfg, d["cp_token"] if incremental else None, expected_boot=executor_boot)
            ndjson = b"".join(json.dumps(r).encode() + b"\n"
                              for r in records)
            body_doc = {
                "create_if_missing": False, "format": "TEXT", "branch":
                    "main", "transaction_type": "SNAPSHOT",
                "summary": f"Connector sync {def_id}",
                "files": [{"logical_path": f"sync-{run_id}.ndjson",
                           "size_bytes": len(ndjson), "content_type":
                           "application/x-ndjson", "operation": "ADD",
                           "content_base64":
                               base64.b64encode(ndjson).decode()}],
                "metadata": {"bytes": len(ndjson), "rows": len(records),
                             "content_hash": sha256_hex(ndjson),
                             "lineage": {"source_id": d["source_id"],
                                         "sync_def_id": def_id,
                                         "sync_run_id": run_id}}}
            recovery = run["recovery"]["protocol"] == "p26-v1"
            if recovery:
                body_doc["idempotency_key"] = \
                    scen.derive_publish_key(run_id)
            body_bytes = json.dumps(body_doc, separators=(",", ":"),
                                    sort_keys=True).encode()
            intent = {"run_id": run_id, "body": body_bytes,
                            "rows": len(records), "bytes": len(ndjson),
                            "sha": sha256_hex(ndjson), "token": new_token,
                            "key": body_doc.get("idempotency_key")}
            d["_intent"] = intent
            self.intents[run_id] = intent
            run["recovery"]["body_len"] = len(body_bytes)
            run["recovery"]["checkpoint_len"] = \
                len(new_token) if new_token else 0
            run["publish_entered_at"] = "2026-11-15T00:00:00Z"
            ok = self._send(d, run, executor_boot=executor_boot)
            if not ok:
                if self.behavior == "auto":
                    self._send(d, run)   # one immediate identical retry
                elif self.behavior == "budget3":
                    # timer-driven in production; the proxy's ATOMIC
                    # drop_then_fail_rest transition makes these retries
                    # deterministic here with zero external race
                    while run["recovery"]["send_attempts"] \
                            < self.MAX_SENDS:
                        if self._send(d, run):
                            break
                    # exhausted: unknown stays running (no terminal)
        except FakeExecutorStopped:
            return  # lost original never writes a gate, budget, or terminal
        except Fail as exc:
            run["status"] = "failed"
            run["error"] = str(exc)

    def _pull(self, cfg, since, expected_boot=None):
        sync = (cfg.get("sync") or {})
        src = (sync.get("query_params") or {}).get("src")
        base = cfg["base_url"]
        resource = sync.get("resource_path", "/data")
        records, cursor, token = [], "", since
        while True:
            q = {"src": src, "page_size": "50"}
            if token is not None and sync.get("incremental", {}).get(
                    "enabled"):
                q["since_token"] = token
            if cursor:
                q["cursor"] = cursor
            qs = "&".join(f"{k}={v}" for k, v in q.items())
            resp = self.transport.request(
                "GET", f"{base}{resource}?{qs}", headers={}, body=None,
                timeout_s=10)
            expect(resp.err is None and resp.status == 200,
                   f"fake connector fixture pull failed: {resp.status}")
            if expected_boot is not None and (not self.executor_alive or expected_boot != self.boot_epoch):
                raise FakeExecutorStopped()
            doc = resp.json()
            records.extend(doc.get("records") or [])
            if not doc.get("next_cursor"):
                if sync.get("incremental", {}).get("enabled"):
                    token = (doc.get("meta") or {}).get(
                        "completed_token")
                break
            cursor = doc["next_cursor"]
        return records, token

    def _send(self, d, run, via_recover=False, executor_boot=None):
        intent = self.intents[run["id"]]
        rec = run["recovery"]
        if rec["send_attempts"] >= self.MAX_SENDS:
            run["status"] = "running"       # budget exhausted: no send
            return False
        commit_url = (f"{self.proxy_base}/api/v1/datasets/"
                      f"{d['dataset']}/outputs:commit")
        rec["send_attempts"] += 1
        resp = self.transport.request(
            "POST", commit_url, headers={"Authorization":
                                         CONNECTOR_AUTH,
                                         "Content-Type":
                                         "application/json"},
            body=intent["body"], timeout_s=30)
        if resp.err is not None or not (200 <= (resp.status or 0) < 300):
            run["status"] = "running"        # P26 unknown semantics
            return False
        if not via_recover and (not self.executor_alive or
                (executor_boot is not None and executor_boot != self.boot_epoch)):
            # crashed ORIGINAL executor (lease lost): the late ACK is
            # DISCARDED — never finalizes, never regresses a run that
            # the recovering executor already finalized
            if run["status"] != "succeeded":
                run["status"] = "running"
            return True
        self._finalize(d, run, resp.json() or {})
        return True

    def _finalize(self, d, run, receipt_doc):
        intent = self.intents[run["id"]]
        txn = (receipt_doc.get("transaction") or {})
        run.update({"status": "succeeded",
                    "rows_written": intent["rows"],
                    "bytes_written": intent["bytes"],
                    "content_hash": intent["sha"],
                    "dataset_version_id": txn.get("id")})
        run["recovery"].update({"state": "finalized",
                                "txn_id": txn.get("id"),
                                "resolved_at": "2026-11-15T00:00:00Z"})
        d["cp_token"] = intent["token"]
        d["epoch"] += 1
        d["last_confirmed"] = {"run_id": run["id"], "txn_id":
                               txn.get("id"), "rows": intent["rows"],
                               "bytes": intent["bytes"],
                               "content_hash": intent["sha"]}

    def _recover(self, def_id, run_id, headers, instance):
        scope = self.classify(headers.get("Authorization"))
        if scope in ("missing", "invalid"):
            return 401, {"error": "authentication required"}
        if scope == "readonly":
            return 403, {"error": "connection write permission required"}
        d = self.defs.get(def_id)
        run = self.runs.get(run_id)
        if d is None or run is None or run.get("sync_def_id") != def_id:
            if scope == "foreign":
                return 404, {"error": "sync run not found"}
            return 404, {"error": "unknown run"}
        if scope == "foreign":
            return 404, {"error": "sync run not found"}  # tenant-blind
        intent = self.intents.get(run_id)
        if (run.get("recovery") or {}).get("protocol") != "p26-v1":
            return 409, {"error": "run is not a p26-v1 recovery run",
                         "code": "NOT_ELIGIBLE"}
        if intent is None:
            # Mirrors the real no-intent resumption shape only; no DB proof.
            run["generation"] += 1
            threading.Thread(target=self._execute_async, args=(def_id, run_id), daemon=True).start()
            return 202, dict(run)
        if run["status"] == "succeeded":
            # already-resolved: REAL 200, unchanged row, ZERO outward
            return 200, dict(run)
        with self._claims_lock:
            holder = self._claims.get(run_id)
            if holder is not None and holder != instance:
                return 409, {"error": "busy_live_holder",
                             "code": "BUSY_LIVE_HOLDER"}
            self._claims[run_id] = instance
            run["generation"] += 1
        try:
            if self.refetch_on_recover:
                # INJECTED VIOLATION: pulling the Source again
                cfg = self.sources[d["source_id"]]["config"]
                self._pull(cfg, d["cp_token"])
            receipt_url = (f"{self.proxy_base}/api/v1/datasets/"
                           f"{d['dataset']}/outputs/commits/"
                           f"{intent['key']}")
            run["recovery"]["query_attempts"] += 1
            resp = self.transport.request(
                "GET", receipt_url, headers={"Authorization":
                                             CONNECTOR_AUTH},
                timeout_s=60)
            if resp.err is not None or resp.status == 502:
                # dependency error: run STAYS running (never 'failed')
                run["recovery"]["state"] = "wait"
                run["recovery"]["reason"] = "dependency"
                return 502, {"error": "recovery verification failed; the run stays recoverable"}
            if resp.status != 200:
                # receipt not committed yet + remaining budget ⇒ resend
                if run["recovery"]["send_attempts"] < self.MAX_SENDS:
                    self._send(d, run, via_recover=True)
                run["recovery"]["state"] = "wait"
                return 202, dict(run)
            doc = resp.json() or {}
            txn = doc.get("transaction") or {}
            receipt_ok = (
                doc.get("idempotency_key") == intent["key"]
                and doc.get("request_fingerprint")
                == sha256_hex(intent["body"])
                and txn.get("status") == "COMMITTED"
                and (txn.get("metadata") or {}).get("content_hash")
                == intent["sha"])
            if not receipt_ok:
                run["recovery"]["state"] = "wait"
                run["recovery"]["reason"] = "dependency"
                return 202, dict(run)
            self._finalize(d, run, doc)
            return 200, dict(run)
        finally:
            with self._claims_lock:
                if self._claims.get(run_id) == instance:
                    del self._claims[run_id]


class FakeStackTransport:
    """Routes by URL netloc to the in-memory cores — the same core
    objects the live HTTP Handlers drive. Zero sockets. Enforces the
    same fail-closed admin authentication as the live Handlers."""

    FIXTURE = "p26fixture:8090"
    PROXY = "p26proxy:8091"
    DVS = "p26dvs:50078"
    DVS_FILES = "p26dvsfiles:17078"        # DATASET_FILES_BASE_URL analog
    CONNECTOR = "p26connectora:17097"
    CONNECTOR_B = "p26connectorb:17098"
    PRESIGN_SECRET = "SECRETSIGN123"       # must NEVER reach evidence

    def __init__(self, behavior="manual", refetch_on_recover=False):
        self.fixture = fixture_mod.P26FixtureCore()
        self.fixture.admin_token = ADMIN_TOKEN
        self.dvs = FakeDVS()
        self.proxy = proxy_mod.P26ProxyCore(
            f"http://{self.DVS}", barrier_max_ms=20000,
            admin_token=ADMIN_TOKEN)
        self.connector = FakeConnector(self, f"http://{self.PROXY}",
                                       behavior=behavior,
                                       refetch_on_recover=
                                       refetch_on_recover)
        self.headers_seen = {"fixture": [], "proxy_admin": [],
                             "proxy_traffic": []}
        self.files_headers_seen = []

    def request(self, method, url, headers=None, body=None, timeout_s=30):
        u = urllib.parse.urlparse(url)
        netloc, path = u.netloc, u.path
        headers = headers or {}
        query = {k: v[0] for k, v in urllib.parse.parse_qs(
            u.query, keep_blank_values=True).items()}
        try:
            if netloc == self.FIXTURE:
                if path.startswith("/admin/"):
                    self.headers_seen["fixture"].append(dict(headers))
                return self._fixture(method, path, query, headers, body)
            if netloc == self.PROXY:
                if path.startswith("/admin/"):
                    self.headers_seen["proxy_admin"].append(dict(headers))
                else:
                    self.headers_seen["proxy_traffic"].append(
                        dict(headers))
                return self._proxy(method, path, query, body, headers)
            if netloc == self.DVS:
                result = self.dvs.handle(method, path, query, body,
                                         headers)
                status, payload = result[0], result[1]
                resp_headers = result[2] if len(result) > 2 else None
                return self._resp(status, payload, resp_headers)
            if netloc == self.DVS_FILES:
                return self._files_origin(method, path, query, headers)
            if netloc == self.CONNECTOR:
                status, payload = self.connector.handle(
                    method, path, query, body, headers, instance="a")
                return self._resp(status, payload)
            if netloc == self.CONNECTOR_B:
                status, payload = self.connector.handle(
                    method, path, query, body, headers, instance="b")
                return self._resp(status, payload)
        except Fail as exc:
            return HTTPResponse(err=exc)
        return HTTPResponse(err=ConnectionError(f"no route {netloc}"))

    @staticmethod
    def _resp(status, payload, headers=None):
        if isinstance(payload, bytes):
            return HTTPResponse(status=status, body=payload,
                                headers=headers or {})
        return HTTPResponse(status=status,
                            body=json.dumps(payload).encode(),
                            headers=headers or {})

    def _files_origin(self, method, path, query, headers):
        """Anonymous presigned local-fs endpoint (mirrors
        /v1/_internal/local-fs/): credential headers are REJECTED — the
        validated download hop must carry no JWT/admin material."""
        import re as _re
        self.files_headers_seen.append(
            {"method": method, "path": path,
             "authorization": headers.get("Authorization"),
             "admin_token": headers.get("X-P26-Admin-Token")})
        if method != "GET" \
                or not path.startswith("/v1/_internal/local-fs/"):
            return self._resp(404, {"error": "not found"})
        if headers.get("Authorization") or \
                headers.get("X-P26-Admin-Token"):
            return self._resp(403, {"error": "presign hop must be "
                                             "anonymous"})
        if query.get("sig") != self.PRESIGN_SECRET:
            return self._resp(403, {"error": "bad signature"})
        fid = _re.match(r"^/v1/_internal/local-fs/(.+)\.bin$",
                        path).group(1)
        content = self.dvs.files_by_id.get(fid)
        if content is None:
            return self._resp(404, {"error": "no such object"})
        return self._resp(200, content)

    def _admin_allowed(self, headers, want):
        return want is not None and headers.get("X-P26-Admin-Token") == want

    # ---- fixture routes (mirror the live Handler) -----------------------
    def _fixture(self, method, path, query, headers, body=None):
        c = self.fixture
        if path.startswith("/admin/") \
                and not self._admin_allowed(headers, c.admin_token):
            return self._resp(403, {"error": "admin token required"})
        if method == "POST" and path.startswith("/admin/knob/"):
            ok, why = c.set_knob(path.rsplit("/", 1)[1], json.loads(body or b"{}"))
            return self._resp(200 if ok else 400, {"ok": ok, "message": why})
        if method == "POST" and path.startswith("/admin/release/"):
            return self._resp(200, {"released": c.release_hold(path.rsplit("/", 1)[1])})
        if method == "GET" and path == "/healthz":
            return self._resp(200, {"ok": True})
        if method == "GET" and path == "/admin/boot":
            return self._resp(200, c.boot_info())
        if method == "GET" and path == "/admin/quiet":
            return self._resp(200, {"quiet": c.quiet()})
        if method == "GET" and path.startswith("/admin/src/"):
            snap = c.snapshot(path.rsplit("/", 1)[1])
            return self._resp(200, snap) if snap else self._resp(
                404, {"error": "unknown src"})
        if method == "GET" and path.startswith("/admin/log/"):
            led = c.ledger(path.rsplit("/", 1)[1])
            return self._resp(200, led if led is not None else [])
        if method == "GET" and path.startswith("/admin/expected/"):
            src = path.rsplit("/", 1)[1]
            exp = c.expected(src, query.get("window"))
            return self._resp(200, exp) if exp else self._resp(
                404, {"error": "unknown src/window"})
        if method == "GET" and path == "/data":
            src = query.pop("src", None)
            if not src:
                return self._resp(400, {"error": "src required"})
            finish = c.begin(src)
            if finish is None:
                return self._resp(404, {"error": "unknown src"})
            try:
                status, doc = c.data(src, query)
                return self._resp(status, doc)
            finally:
                finish()
        return self._resp(404, {"error": "not found"})

    # ---- proxy routes (admin direct; traffic via core) -------------------
    def _proxy(self, method, path, query, body, headers):
        core = self.proxy
        if path.startswith("/admin/") \
                and not self._admin_allowed(headers, core.admin_token):
            return self._resp(403, {"error": "admin token required"})
        if path == "/healthz":
            return self._resp(200, {"ok": True})
        if path == "/admin/boot":
            return self._resp(200, core.boot_info())
        if path == "/admin/mode" and method == "GET":
            return self._resp(200, {"mode": core.mode})
        if path == "/admin/mode" and method == "POST":
            doc = json.loads(body or b"{}")
            ok = core.set_mode(doc.get("mode", ""))
            return self._resp(200 if ok else 400,
                              {"ok": ok, "mode": core.mode})
        if path == "/admin/log" and method == "GET":
            with core.lock:
                return self._resp(200, list(core.ledger))
        if path == "/admin/stats" and method == "GET":
            return self._resp(200, core.snapshot_stats())
        if path == "/admin/barrier" and method == "GET":
            return self._resp(200, {"barrier": core.barrier_state()})
        if path == "/admin/barrier/release" and method == "POST":
            return self._resp(200, {"ok": core.release_barrier()})
        if method == "POST" and proxy_mod.COMMIT_PATH_RE.match(path):
            result = core.handle_commit(path, headers, body, self)
            if result["action"] == "drop":
                return HTTPResponse(err=ConnectionResetError(
                    "peer closed connection without response"))
            return self._resp(result["status"], result["body"] or b"")
        if method == "GET" and proxy_mod.RECEIPT_PATH_RE.match(path):
            result = core.handle_receipt_get(path, headers, self)
            return self._resp(result["status"], result["body"] or b"")
        result = core.handle_other(method, path, headers, body, self)
        return self._resp(result["status"], result["body"] or b"")


class FakeConnectorOps(scen.ConnectorOps):
    """Deterministic in-memory owned-process operations (fake stand-in
    for http/p26_owned_ops.py; credentials are FAKE stand-ins for
    Root-provided signed JWTs)."""

    def __init__(self, stack):
        self.stack = stack

    def stop_connector(self, instance="a"):
        self.stack.connector.ops_stop(instance)

    def wait_stopped(self, timeout_s=30):
        return True                              # synchronous fake

    def restart_connector(self, instance="a"):
        self.stack.connector.ops_restart(instance)

    def wait_lease_expired(self, def_id, run_id, timeout_s=60):
        return True               # live: read-only q3 psql observer

    def connector_boot_id(self, instance="a"):
        return self.stack.connector.boot_ids[instance]

    def observe_pregate(self, def_id, run_id):
        r = self.stack.connector.runs[run_id]
        expect(r["sync_def_id"] == def_id and r["status"] == "running"
               and r["publish_entered_at"] is None
               and run_id not in self.stack.connector.intents, "fake pre-gate state")
        return {"db_clock": "2026-11-15T00:00:00Z", "status": "running",
                "protocol": "p26-v1", "generation": r["generation"],
                "gate_not_entered": True, "no_intent": True,
                "send_attempts": 0, "query_attempts": 0, "cp_owned_epoch_zero": True,
                "fake_only": True}

    def credential(self, kind):
        return {
            "writer": f"Bearer {PLATFORM_JWT}",
            "readonly": "Bearer sigvalid-readonly-sameTenant",
            "foreign": "Bearer sigvalid-foreignTenant",
        }[kind]


FAKE_BASES = {"fixture": f"http://{FakeStackTransport.FIXTURE}",
              "proxy_admin": f"http://{FakeStackTransport.PROXY}",
              "dvs": f"http://{FakeStackTransport.DVS}",
              "connector": f"http://{FakeStackTransport.CONNECTOR}",
              "connector_b": f"http://{FakeStackTransport.CONNECTOR_B}"}


def mechanics_ctx(stack, label, ops=True):
    return scen.LiveCtx(
        stack, FAKE_BASES,
        platform_auth=f"Bearer {PLATFORM_JWT}",
        admin_token=ADMIN_TOKEN,
        evidence_root=RESULTS / "mechanics-fake",
        label=label,
        ops=FakeConnectorOps(stack) if ops else None,
        approved_files_origin=f"http://{FakeStackTransport.DVS_FILES}")


# ==========================================================================
# 1. lib checks
# ==========================================================================

@check("lib-redact-credentials-and-opaque")
def _t1():
    s = lib.redact("Authorization: Bearer abc.def.ghi")
    expect("abc.def.ghi" not in s, f"jwt leaked: {s}")
    s = lib.redact("X-P26-Admin-Token: local-admin-secret")
    expect("local-admin-secret" not in s, f"admin token leaked: {s}")
    s = lib.redact("postgres://u:SECRETPW@h/db")
    expect("SECRETPW" not in s, f"dsn pw leaked: {s}")
    s = lib.redact("GET /data?src=x&since_token=p26inc1-tok-0&cursor=pg-1")
    expect("p26inc1-tok-0" not in s, f"raw token in evidence: {s}")
    expect("sha256-" in s, f"digest missing: {s}")
    d = opaque_digest("p26inc1-tok-0")
    expect(d.startswith("sha256-") and ":len=" in d, f"digest form {d}")


@check("lib-redact-json-exact-keys-keep-safe-digests")
def _t1b():
    cleaned = lib.redact_json({
        "authorization": "Bearer x", "token": "zzz", "jwt": "j",
        "safe_note": "sha256-abcdef1234567890:len=13",
        "last_two_tokens": [None, "sha256-0fa75b1b64502cf1:len=13"],
        "nested": {"ok": 1, "secret": "s"},
    })
    expect(cleaned["authorization"] == "<redacted>", "auth exact")
    expect(cleaned["token"] == "<redacted>", "token exact")
    expect(cleaned["jwt"] == "<redacted>", "jwt exact")
    expect(cleaned["nested"]["secret"] == "<redacted>", "secret exact")
    expect(cleaned["safe_note"] == "sha256-abcdef1234567890:len=13",
           f"safe digest preserved: {cleaned['safe_note']!r}")
    expect(cleaned["last_two_tokens"]
           == [None, "sha256-0fa75b1b64502cf1:len=13"],
           f"digest list preserved: {cleaned['last_two_tokens']!r}")
    expect(cleaned["nested"]["ok"] == 1, "non-secret intact")


@check("lib-client-credential-separation")
def _t1c():
    seen = {}

    class T:
        def request(self, method, url, headers=None, body=None,
                    timeout_s=30):
            seen[url] = dict(headers or {})
            return HTTPResponse(status=200, body=b"{}")

    transport = T()
    platform = lib.Client(transport, "http://platform:1",
                          auth="Bearer " + PLATFORM_JWT)
    admin = lib.Client(transport, "http://fixture:2",
                       extra_headers={"X-P26-Admin-Token": ADMIN_TOKEN})
    platform.get("/x")
    admin.get("/admin/boot")
    expect(seen["http://platform:1/x"]["Authorization"]
           == "Bearer " + PLATFORM_JWT, "platform jwt on platform base")
    expect("Authorization" not in seen["http://fixture:2/admin/boot"],
           "platform JWT must NOT ride the admin client")
    expect(seen["http://fixture:2/admin/boot"]["X-P26-Admin-Token"]
           == ADMIN_TOKEN, "admin token header present")


@check("lib-bounded-event-release-and-timeout")
def _t2():
    ev = BoundedEvent(5000)
    threading.Timer(0.05, lambda: ev.release(by="test")).start()
    released, waited = ev.wait()
    expect(released, "early release must win")
    expect(waited < 2000, f"waited too long: {waited}")
    ev2 = BoundedEvent(120)
    released, waited = ev2.wait()
    expect(not released, "timeout path")
    expect(waited >= 100, "bounded timeout honored")


@check("lib-bounded-wait-deadline")
def _t3():
    ok, _ = BoundedWait(0.3, interval_s=0.05).until(lambda: False)
    expect(not ok, "must respect deadline")
    ok, val = BoundedWait(0.5, interval_s=0.05).until(lambda: 42)
    expect(ok and val == 42, "immediate truth returns")


@check("lib-evidence-append-only")
def _t4():
    with tempfile.TemporaryDirectory() as tmp:
        evd = EvidenceDir(Path(tmp), "attempt-x")
        rec = evd.step("01-first")
        rec.cmd(["GET", "http://x/y"], env_keys=["OPENFOUNDRY_JWT"])
        rec.finish(0, b"out", {"k": "v"})
        expect((evd.path / "01-first.rc").read_text() == "0\n", "rc file")
        try:
            evd.step("01-first")
            raise Fail("append-only violation not detected")
        except RuntimeError:
            pass


@check("lib-release-gate-matrix")
def _t5():
    with tempfile.TemporaryDirectory() as tmp:
        rel = Path(tmp) / "rel.json"
        rel.write_text(json.dumps(
            {"released_for": ["http-fixture-serve"],
             "expires_utc": "20990101T000000Z"}))
        ok, why = lib.release_allows("http-fixture-serve", str(rel))
        expect(ok, f"valid release rejected: {why}")
        ok, _ = lib.release_allows("http-proxy-serve", str(rel))
        expect(not ok, "unlisted phase must be refused")
        rel.write_text(json.dumps(
            {"released_for": ["http-fixture-serve"],
             "expires_utc": "20200101T000000Z"}))
        ok, why = lib.release_allows("http-fixture-serve", str(rel))
        expect(not ok and "expired" in why, f"past expiry: {why}")
        rel.write_text("{bad json")
        ok, _ = lib.release_allows("http-fixture-serve", str(rel))
        expect(not ok, "malformed refused")


@check("lib-no-redirect-opener")
def _t5b():
    handler = lib._NoRedirectHandler()
    expect(handler.redirect_request(None, None, 302, "Found", {},
                                    "http://other.origin/x") is None,
           "redirect_request must refuse to build a follower")
    kinds = [type(h).__name__ for h in lib._NO_REDIRECT_OPENER.handlers]
    expect("_NoRedirectHandler" in kinds, f"opener wired: {kinds}")


@check("lib-resolve-bases-origin-binding")
def _t5c():
    rel = {"w7_bases": {
        "fixture": "http://127.0.0.1:17095",
        "proxy_admin": "http://127.0.0.1:17096",
        "dvs": "http://127.0.0.1:50117",
        "connector": "http://127.0.0.1:17097"}}
    bases, err = lib.resolve_bases({}, rel)
    expect(err is None and bases["dvs"] == "http://127.0.0.1:50117",
           f"approved bases resolve: {err}")
    # a flag that disagrees with the approved origin is refused
    bases, err = lib.resolve_bases({"dvs": "http://evil:9"}, rel)
    expect(err is not None and "Root-approved" in err,
           f"flag widening refused: {err}")
    # structural rejections
    bad = ["http://u:p@127.0.0.1:17095", "http://127.0.0.1:17095/x?y=1",
           "http://127.0.0.1:17095#frag", "ftp://127.0.0.1:17095",
           "http://127.0.0.1:17092"]              # old P25 port
    for b in bad:
        ok, why = lib.validate_origin(b)
        expect(not ok, f"{b} must be rejected ({why})")
    # missing key in release
    bases, err = lib.resolve_bases({}, {"w7_bases": {}})
    expect(err is not None and "missing" in err, "missing key refused")
    bases, err = lib.resolve_bases({}, None)
    expect(err is not None, "no w7_bases refused")


@check("lib-presign-location-validation-and-scrub")
def _t5d():
    approved = "http://127.0.0.1:17078"
    good = (f"{approved}/v1/_internal/local-fs/f1.bin"
            f"?sig={FakeStackTransport.PRESIGN_SECRET}&expires=1731619200")
    facts, err = lib.validated_download_location(good, approved)
    expect(err is None and facts is not None, f"good location: {err}")
    expect(facts["path"] == "/v1/_internal/local-fs/f1.bin", "path kept")
    expect(facts["expiry"] == "1731619200", "whitelisted expiry kept")
    expect(FakeStackTransport.PRESIGN_SECRET not in json.dumps(facts),
           "signature value must NEVER be in safe facts")
    expect(facts["query_sha256"] and facts["query_len"] > 0,
           "query digest/length present")
    bad = [
        ("other origin", "http://evil:1/v1/_internal/local-fs/f.bin?s=1"),
        ("wrong port", "http://127.0.0.1:9/v1/_internal/local-fs/f.bin"),
        ("bad path", f"{approved}/v1/other/f.bin"),
        ("userinfo", f"http://u:p@127.0.0.1:17078"
                     f"/v1/_internal/local-fs/f.bin"),
        ("fragment", f"{approved}/v1/_internal/local-fs/f.bin#x"),
    ]
    for name, loc in bad:
        facts, err = lib.validated_download_location(loc, approved)
        expect(facts is None, f"{name} must be rejected ({err})")
    # redaction covers presign query signatures in any evidence string
    s = lib.redact(f"GET {good}")
    expect(FakeStackTransport.PRESIGN_SIGN if False else
           FakeStackTransport.PRESIGN_SECRET not in s,
           f"signature leaked in redacted cmd: {s}")
    expect("expires=1731619200" in s, "expiry survives redaction")


# ==========================================================================
# 2. fixture core checks
# ==========================================================================

@check("fixture-ndjson-expectation-matches-record-encoding")
def _f0():
    c = fixture_mod.P26FixtureCore()
    for src in c.sources:
        for w in range(len(c.sources[src].windows)):
            for page in c.sources[src].windows[w]:
                for rec in page:
                    expect(rec.isascii(), f"non-ascii record in {src}")
    s, page1 = c.data("p26snap1", {"page_size": "50"})
    exp = c.expected("p26snap1", 0)
    from_response = b"".join(json.dumps(r).encode() + b"\n"
                             for r in page1["records"])
    cur = page1["next_cursor"]
    while cur:
        s, pg = c.data("p26snap1", {"cursor": cur, "page_size": "50"})
        from_response += b"".join(json.dumps(r).encode() + b"\n"
                                  for r in pg["records"])
        cur = pg["next_cursor"]
    expect(from_response.startswith(b'"'), "string records keep JSON "
                                          "quotes on the wire")
    expect(len(from_response) == exp["bytes"],
           f"expected bytes {exp['bytes']} != {len(from_response)}")
    expect(sha256_hex(from_response) == exp["ndjson_sha256"],
           "expected sha must be computed over the RESPONSE encoding")


@check("fixture-determinism-two-boots")
def _f1():
    c1, c2 = fixture_mod.P26FixtureCore(), fixture_mod.P26FixtureCore()
    for src in ("p26snap1", "p26inc1", "p26legacy"):
        for w in range(len(c1.sources[src].windows)):
            expect(c1.expected(src, w) == c2.expected(src, w),
                   f"{src} window {w} not deterministic")
    expect(c1.boot_id != c2.boot_id, "boot ids must be fresh per boot")


@check("fixture-position-semantics")
def _f2():
    c = fixture_mod.P26FixtureCore()
    s, b = c.data("p26inc1", {"page_size": "50"})
    expect(s == 200 and len(b["records"]) == 8 and b["next_cursor"],
           f"NULL window0 first page {b}")
    s, _ = c.data("p26inc1", {"cursor": "pg-deadbeef", "page_size": "50"})
    expect(s == 400, "unknown cursor must 400")
    cur, tok = b["next_cursor"], None
    while True:
        s, b = c.data("p26inc1", {"cursor": cur, "page_size": "50"})
        if not b["next_cursor"]:
            tok = b["meta"]["completed_token"]
            break
        cur = b["next_cursor"]
    expect(tok == "p26inc1-tok-0", f"terminal token {tok}")
    s, b = c.data("p26inc1", {"since_token": tok, "page_size": "50"})
    expect(s == 200 and b["records"][0].startswith("i1b-"),
           f"T0 must serve window B {b['records'][:1]}")
    s, b = c.data("p26inc1", {"since_token": "p26inc1-tok-1"})
    expect(s == 200 and b["records"] == [], "exhausted empty")
    expect(b["meta"]["completed_token"] == "p26inc1-tok-1",
           "same token re-issued")
    s, _ = c.data("p26inc1", {"since_token": "foreign-token"})
    expect(s == 400, "foreign token must 400")


@check("fixture-cursors-opaque-and-stable")
def _f3():
    c = fixture_mod.P26FixtureCore()
    s, b = c.data("p26snap1", {"page_size": "50"})
    k1 = b["next_cursor"]
    s, b2 = c.data("p26snap1", {"page_size": "50"})
    expect(b2["next_cursor"] == k1, "cursor stable across requests")
    expect(k1.startswith("pg-") and len(k1) == 15,
           f"opaque content-derived cursor {k1}")
    expect(not k1[3:].isdigit(), "cursor must not be sequential")


@check("fixture-lifecycle-counters-even-on-errors")
def _f4():
    c = fixture_mod.P26FixtureCore()
    before = c.snapshot("p26snap1")
    finish = c.begin("p26snap1")
    mid = c.snapshot("p26snap1")
    expect(mid["started"] == before["started"] + 1, "started++ at begin")
    c.data("p26snap1", {"cursor": "pg-nope", "page_size": "50"})  # 400
    finish()
    after = c.snapshot("p26snap1")
    expect(after["finished"] == before["finished"] + 1,
           "finished++ on error path")
    expect(after["active"] == before["active"], "active back to base")
    expect(c.quiet(), "quiet after finish")


@check("fixture-hold-barrier-bounded-release-and-cleanup")
def _f5():
    c = fixture_mod.P26FixtureCore()
    ok, _ = c.set_knob("p26snap1", {"hold_next_ms": 8000})
    out = {}
    th = threading.Thread(target=lambda: (
        c.data("p26snap1", {"page_size": "10"}),
        out.setdefault("done", 1)))
    th.start()
    okw, _ = lib.BoundedWait(2.0, 0.05).until(
        lambda: c.snapshot("p26snap1")["active_hold"] is not None)
    expect(okw, "active hold observable")
    expect(c.release_hold("p26snap1"), "admin release hits ACTIVE waiter")
    th.join(timeout=3)
    expect(not th.is_alive(), "worker returned after release")
    ok, _ = c.set_knob("p26snap1", {"hold_next_ms": 30000})
    expect(c.release_hold("p26snap1"), "pending hold releasable")
    ok, _ = c.set_knob("p26snap1", {"hold_next_ms": 30000})
    th2 = threading.Thread(target=lambda: c.data(
        "p26snap1", {"page_size": "10"}))
    th2.start()
    okw, _ = lib.BoundedWait(2.0, 0.05).until(
        lambda: c.snapshot("p26snap1")["active_hold"] is not None)
    c.close()
    th2.join(timeout=3)
    expect(not th2.is_alive(), "close() unblocks held request")


@check("fixture-ledger-digests-no-raw-no-secrets")
def _f6():
    c = fixture_mod.P26FixtureCore()
    c.data("p26snap1", {"page_size": "50"})
    c.data("p26snap1", {"since_token": "p26inc1-tok-0",
                        "cursor": "pg-abcdef123456"})
    blob = json.dumps(c.ledger("p26snap1")) + json.dumps(
        c.snapshot("p26snap1"))
    for secret in ("Bearer", "Authorization", "jwt"):
        expect(secret not in blob, f"ledger leaked {secret!r}")
    expect("p26inc1-tok-0" not in blob, "raw token leaked")
    expect("pg-abcdef123456" not in blob, "raw cursor leaked")
    expect(c.snapshot("p26snap1")["tokens_seen"][-1]
           == fixture_mod.token_digest("p26inc1-tok-0"),
           "tokens_seen carries digests")


# ==========================================================================
# 3. proxy core checks
# ==========================================================================

DS1 = "11111111-1111-4111-8111-111111111111"


def _ack_body(target, key="k1", txn="12345678-1234-4123-8123-111111111111",
              status="COMMITTED", fingerprint=None, content_hash="b" * 64,
              lineage_run="<run>", dataset_echo=None, branch="main"):
    return {"dataset_id": dataset_echo or target, "branch": branch,
            "transaction":
                {"id": txn, "status": status,
                 "metadata": {"content_hash": content_hash,
                              "lineage": {"sync_run_id": lineage_run}}},
            "idempotency_key": key,
            "request_fingerprint": fingerprint}


class _ScriptedUpstream:
    def __init__(self, status=201, ack=None, fail=False):
        self.status, self.ack, self.fail = status, ack, fail
        self.calls = []

    def request(self, method, url, headers, body, timeout_s):
        self.calls.append({"method": method, "url": url,
                           "headers": dict(headers), "body": body})
        if self.fail:
            return HTTPResponse(err=ConnectionRefusedError("up"))
        ack = self.ack
        if ack is None:
            ack = _ack_body(DS1)
            ack["request_fingerprint"] = sha256_hex(body or b"")
        elif isinstance(ack, dict):
            ack = dict(ack)
            if ack.get("request_fingerprint") == "AUTO":
                ack["request_fingerprint"] = sha256_hex(body or b"")
        return HTTPResponse(status=self.status,
                            body=json.dumps(ack).encode())


def _commit_body_bytes(key="k1", branch="main",
                       marker="MARKER-CONFIDENTIAL-123"):
    doc = {"branch": branch, "create_if_missing": False,
           "files": [{"logical_path": "sync-x.ndjson",
                      "content_base64": base64.b64encode(
                          marker.encode()).decode()}],
           "metadata": {"content_hash": "b" * 64, "rows": 1, "bytes": 24},
           "idempotency_key": key}
    return json.dumps(doc, separators=(",", ":")).encode()


@check("proxy-passthrough-ledger-safe-and-exact-forward")
def _p1():
    up = _ScriptedUpstream()
    core = proxy_mod.P26ProxyCore("http://up")
    body = _commit_body_bytes()
    result = core.handle_commit(
        f"/api/v1/datasets/{DS1}/outputs:commit",
        {"Authorization": "Bearer SECRET-JWT-9", "Content-Type":
         "application/json"}, body, up)
    expect(result["action"] == "respond" and result["status"] == 201,
           f"passthrough 201 {result}")
    entry = core.ledger[-1]
    blob = json.dumps(core.ledger)
    expect("SECRET-JWT-9" not in blob, "ledger leaked Authorization")
    expect("MARKER-CONFIDENTIAL-123" not in blob,
           "ledger leaked raw body content")
    expect(entry["request_body_sha256"] == sha256_hex(body),
           "body sha recorded")
    expect(entry["request_branch"] == "main", "request branch recorded")
    expect(entry["boot_id"] == core.boot_id, "ledger entry boot-bound")
    expect(up.calls[0]["body"] == body, "EXACT body bytes forwarded")
    expect(up.calls[0]["headers"]["Authorization"] == "Bearer SECRET-JWT-9",
           "Authorization forwarded intact")
    expect(core.snapshot_stats()["inflight_quiet"], "quiet after")


@check("proxy-verify-strict-predicates")
def _p2():
    fp = sha256_hex(b"req-bytes")

    def verdict(ack, req_branch="main"):
        parsed = proxy_mod.parse_ack(json.dumps(ack).encode())
        return proxy_mod.verify_commit_ack(parsed, DS1, "k1", req_branch,
                                           fp)

    expect(verdict(_ack_body(DS1, fingerprint=fp))["verified_committed"],
           "good ack must verify")
    zero_uuid = "00000000-0000-0000-0000-000000000000"
    cases = [
        ("zero txn uuid", _ack_body(DS1, fingerprint=fp, txn=zero_uuid)),
        ("not committed", _ack_body(DS1, fingerprint=fp, status="OPEN")),
        ("wrong dataset", _ack_body(
            DS1, fingerprint=fp,
            dataset_echo="22222222-2222-4222-8222-222222222222")),
        ("key echo mismatch", _ack_body(DS1, fingerprint=fp,
                                        key="other-key")),
        ("fp well-formed but != sha(body)",
         _ack_body(DS1, fingerprint="c" * 64)),
        ("branch echo mismatch", _ack_body(DS1, fingerprint=fp,
                                           branch="dev")),
        ("content hash not hex64", _ack_body(DS1, fingerprint=fp,
                                             content_hash="zz")),
    ]
    for name, ack in cases:
        expect(not verdict(ack)["verified_committed"],
               f"{name} must NOT verify")


@check("proxy-drop-ack-once-oneshot")
def _p3():
    up = _ScriptedUpstream()
    core = proxy_mod.P26ProxyCore("http://up")
    core.set_mode("drop_ack_once")
    body = _commit_body_bytes()
    r1 = core.handle_commit(f"/api/v1/datasets/{DS1}/outputs:commit",
                            {}, body, up)
    expect(r1["action"] == "drop", "first verified 201 must drop")
    expect(core.mode == "passthrough", "one-shot auto-revert")
    up2 = _ScriptedUpstream(status=200)
    r2 = core.handle_commit(f"/api/v1/datasets/{DS1}/outputs:commit",
                            {}, body, up2)
    expect(r2["action"] == "respond" and r2["status"] == 200,
           "replay passthrough")
    core.set_mode("drop_ack_once")
    up4 = _ScriptedUpstream(ack=_ack_body(DS1, fingerprint="d" * 64))
    r4 = core.handle_commit(f"/api/v1/datasets/{DS1}/outputs:commit",
                            {}, body, up4)
    expect(r4["action"] == "respond",
           "201 with WRONG fingerprint must not drop")
    expect(core.mode == "drop_ack_once", "one-shot kept for real 201")


@check("proxy-concurrent-drop-exactly-one")
def _p3b():
    up = _ScriptedUpstream()
    core = proxy_mod.P26ProxyCore("http://up")
    core.set_mode("drop_ack_once")
    body = _commit_body_bytes()
    results = []
    lock = threading.Lock()

    def worker():
        r = core.handle_commit(f"/api/v1/datasets/{DS1}/outputs:commit",
                               {}, body, up)
        with lock:
            results.append(r)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    drops = [r for r in results if r["action"] == "drop"]
    expect(len(drops) == 1, f"exactly ONE drop, got {len(drops)}")
    expect(core.snapshot_stats()["drops"] == 1, "drop counter atomic")


@check("proxy-hold-drop-once-deterministic-kill-window")
def _p3c():
    up = _ScriptedUpstream()
    core = proxy_mod.P26ProxyCore("http://up", barrier_max_ms=8000)
    core.set_mode("hold_drop_once")
    out = {}
    th = threading.Thread(target=lambda: out.update(
        {"r": core.handle_commit(
            f"/api/v1/datasets/{DS1}/outputs:commit", {},
            _commit_body_bytes(), up)}))
    th.start()
    # observe the parked REAL 201 while the response is held
    ok, _ = lib.BoundedWait(2.0, 0.05).until(
        lambda: any(e.get("held_for_stop") for e in core.ledger))
    expect(ok, "held entry observable BEFORE the drop")
    held = [e for e in core.ledger if e.get("held_for_stop")][0]
    expect(held.get("upstream_status") == 201
           and held.get("verified_committed")
           and not held.get("dropped"),
           "parked entry: real 201, verified, not yet dropped")
    # kill-window work happens here (deterministic); then release
    core.release_barrier()
    th.join(timeout=5)
    expect(not th.is_alive(), "drop completed after release")
    expect(out["r"]["action"] == "drop", "released response is dropped")
    final = [e for e in core.ledger if e.get("dropped")][-1]
    expect(final.get("held_for_stop") is True, "final entry kill-window")
    expect(core.mode == "passthrough", "one-shot consumed once")


@check("proxy-fail-before-forward-zero-upstream")
def _p4():
    up = _ScriptedUpstream()
    core = proxy_mod.P26ProxyCore("http://up")
    core.set_mode("fail_before_forward")
    r = core.handle_commit(f"/api/v1/datasets/{DS1}/outputs:commit", {},
                           _commit_body_bytes(), up)
    expect(r["status"] == 502, "fabricated 502")
    expect(up.calls == [], "ZERO upstream calls (provable)")


@check("proxy-hold-request-barrier-bounded-release-cleanup")
def _p5():
    up = _ScriptedUpstream()
    core = proxy_mod.P26ProxyCore("http://up", barrier_max_ms=8000)
    core.set_mode("hold_request_once")
    out = {}
    th = threading.Thread(target=lambda: out.update(
        {"r": core.handle_commit(
            f"/api/v1/datasets/{DS1}/outputs:commit", {},
            _commit_body_bytes(), up)}))
    th.start()
    ok, _ = lib.BoundedWait(2.0, 0.05).until(
        lambda: core.barrier_state() is not None)
    expect(ok, "barrier observable while held")
    core.release_barrier()
    th.join(timeout=3)
    expect(not th.is_alive() and out["r"]["status"] == 201,
           "released request forwards and returns 201")
    core.set_mode("hold_request_once")
    th2 = threading.Thread(target=lambda: out.update(
        {"r2": core.handle_commit(
            f"/api/v1/datasets/{DS1}/outputs:commit", {},
            _commit_body_bytes(), up)}))
    th2.start()
    ok, _ = lib.BoundedWait(2.0, 0.05).until(
        lambda: core.barrier_state() is not None)
    core.close()
    th2.join(timeout=3)
    expect(not th2.is_alive(), "close() unblocks held commit")


@check("proxy-hold-barriers-auto-timeout")
def _p6():
    up = _ScriptedUpstream()
    core = proxy_mod.P26ProxyCore("http://up", barrier_max_ms=150)
    core.set_mode("hold_request_once")
    t0 = time.monotonic()
    r = core.handle_commit(f"/api/v1/datasets/{DS1}/outputs:commit", {},
                           _commit_body_bytes(), up)
    expect(r["status"] == 201, "auto-released request still forwarded")
    expect(0.1 <= time.monotonic() - t0 < 3, "bounded auto-release")


@check("proxy-receipt-modes-hold-and-fail-once")
def _p7():
    # passthrough 404 stays boot-bound ledger evidence
    up = _ScriptedUpstream(status=404, ack={"error": "none"})
    core = proxy_mod.P26ProxyCore("http://up")
    r = core.handle_receipt_get(
        f"/api/v1/datasets/{DS1}/outputs/commits/of-p26-pub-v1:r1", {},
        up)
    expect(r["status"] == 404, "receipt 404 passthrough")
    entry = core.ledger[-1]
    expect(entry["kind"] == "receipt_query", "receipt ledger kind")
    expect(entry["boot_id"] == core.boot_id, "receipt entry boot-bound")
    # fail_receipt_once: actual dependency error 502, zero upstream,
    # one-shot (second query passes)
    core.set_mode("fail_receipt_once")
    r = core.handle_receipt_get(
        f"/api/v1/datasets/{DS1}/outputs/commits/of-p26-pub-v1:r1", {},
        up)
    expect(r["status"] == 502, "receipt dependency error is 502")
    expect(up.calls and up.calls[-1]["method"] == "GET",
           "upstream reached on the passing query only")
    err = [e for e in core.ledger
           if e.get("kind") == "receipt_error"]
    expect(len(err) == 1 and err[0]["fabricated_status"] == 502,
           "dependency failure ledgered once")
    expect(core.mode == "passthrough", "fail_receipt_once disarmed")
    # hold_receipt_once: first response parked (bounded), one-shot
    good = {"dataset_id": DS1, "branch": "main",
            "transaction": {"id": "t1", "status": "COMMITTED",
                            "metadata": {"content_hash": "e" * 64}},
            "idempotency_key": "of-p26-pub-v1:r2",
            "request_fingerprint": "a" * 64}
    up2 = _ScriptedUpstream(status=200, ack=good)
    core.set_mode("hold_receipt_once")
    out = {}
    th = threading.Thread(target=lambda: out.update(
        {"r": core.handle_receipt_get(
            f"/api/v1/datasets/{DS1}/outputs/commits/of-p26-pub-v1:r2",
            {}, up2)}))
    th.start()
    ok, _ = lib.BoundedWait(2.0, 0.05).until(
        lambda: any(e.get("held_response") for e in core.ledger))
    expect(ok, "held receipt entry observable while parked")
    expect(core.mode == "passthrough",
           "hold_receipt_once disarmed after the FIRST query")
    core.release_barrier()
    th.join(timeout=5)
    expect(not th.is_alive() and out["r"]["status"] == 200,
           "released receipt query returns the upstream response")
    r2 = core.handle_receipt_get(
        f"/api/v1/datasets/{DS1}/outputs/commits/of-p26-pub-v1:r2", {},
        up2)
    expect(r2["status"] == 200, "second receipt query NOT held again")


@check("proxy-upstream-validation-refuses-old-substrate")
def _p8():
    for bad in ("http://p25fixture:8090/x", "http://fixture-1:9",
                "http://host:17092", "http://aux17094:1", "ftp://x", ""):
        ok, _ = proxy_mod.validate_upstream(bad)
        expect(not ok, f"{bad!r} must be refused")
    ok, _ = proxy_mod.validate_upstream("http://p26dvs:50117")
    expect(ok, "new owned upstream accepted (DVS actual default 50117)")


# ==========================================================================
# 4. scenario mechanics on the fake stack (MECHANICS ONLY)
# ==========================================================================

def _all_requests_recorded(ctx, min_steps):
    names = [p.name for p in ctx.evidence.path.iterdir()]
    cmds = [n for n in names if n.endswith(".cmd")]
    expect(len(cmds) >= min_steps,
           f"per-request evidence too small: {len(cmds)} < {min_steps}")
    for n in cmds:
        expect((ctx.evidence.path / n).read_text().strip() != "",
               f"cmd file {n} empty")
    return len(cmds)


@check("scenario-mechanics-healthy-snapshot-and-legacy")
def _s1():
    stack = FakeStackTransport(behavior="manual")
    ctx = mechanics_ctx(stack, "mechanics-healthy-snap")
    facts = scen.SCENARIOS["healthy_snapshot_and_legacy"]["fn"](ctx)
    expect(facts["rows"] == 32 and facts["bytes"] > 0, "facts rows/bytes")
    expect(_all_requests_recorded(ctx, 20),
           "every request (polls included) recorded")
    commits = [e for e in stack.proxy.ledger if e.get("kind") == "commit"]
    keyed = [e for e in commits if e.get("request_key_hash")]
    legacy = [e for e in commits if not e.get("request_key_hash")]
    expect(len(keyed) == 1 and keyed[0]["upstream_status"] == 201,
           "one keyed first-commit 201")
    expect(len(legacy) == 1, "one legacy keyless commit")
    for h in stack.headers_seen["fixture"] \
            + stack.headers_seen["proxy_admin"]:
        expect("Authorization" not in h,
               "platform JWT must never reach fixture/proxy admin")
    expect(facts["receipt"]["txn_id"], "receipt facts saved")
    expect(facts["version_count"] == 1, "version facts saved")
    kinds = {e["kind"] for e in facts["owned_roster"]}
    expect({"dataset", "source", "sync_def", "run"} <= kinds,
           f"owned typed roster populated: {kinds}")
    # actual DownloadFile protocol: 302 + ONE validated anonymous hop
    names = [p.name for p in ctx.evidence.path.iterdir()]
    presign = [n for n in names if "file-presign" in n and
               n.endswith(".rc")]
    follow = [n for n in names if "file-bytes" in n and
              n.endswith(".rc")]
    expect(presign and follow, f"two-step download steps present "
                               f"({len(presign)}/{len(follow)})")
    expect((ctx.evidence.path / presign[0]).read_text() == "302\n",
           "presign step records the truthful 302 rc")
    # the presign SIGNATURE never entered any evidence byte
    blob = b"".join(p.read_bytes() for p in ctx.evidence.path.iterdir())
    expect(FakeStackTransport.PRESIGN_SECRET.encode() not in blob,
           "presign signature leaked into evidence")
    expect(b"sha256-" in blob, "location facts carry digests")
    # the anonymous hop really carried no credentials
    follow_hops = [h for h in stack.files_headers_seen
                   if "/v1/_internal/local-fs/" in h["path"]]
    expect(follow_hops and all(h["authorization"] is None
                               and h["admin_token"] is None
                               for h in follow_hops),
           f"presign hop anonymous: {follow_hops}")


@check("scenario-mechanics-incremental-epoch-once")
def _s2():
    stack = FakeStackTransport(behavior="manual")
    ctx = mechanics_ctx(stack, "mechanics-healthy-inc")
    facts = scen.SCENARIOS["healthy_incremental_epoch_once"]["fn"](ctx)
    expect(facts["epoch_after_a"] == 1 and facts["epoch_after_b"] == 2,
           f"epoch once per window {facts}")
    ds = stack.dvs.datasets[facts["dataset_id"]]
    expect(len(ds["files"]) == 2, "two committed files")


@check("scenario-mechanics-dropack-manual-owned-restart-lookup")
def _s3():
    stack = FakeStackTransport(behavior="manual")
    ctx = mechanics_ctx(stack, "mechanics-dropack-mr")
    facts = scen.SCENARIOS[
        "dropack_manual_owned_restart_lookup"]["fn"](ctx)
    run_id = facts["run_id"]
    key = scen.derive_publish_key(run_id)
    kh = key_hash(key)
    expect(facts["connector_boot_before"] != facts["connector_boot_after"],
           "Docker boot identity changed across owned restart")
    commits = [e for e in stack.proxy.ledger
               if e.get("kind") == "commit" and e.get("request_key_hash")
               == kh]
    finals = [e for e in commits if not e.get("held_phase")]
    helds = [e for e in commits if e.get("held_phase")]
    queries = [e for e in stack.proxy.ledger
               if e.get("kind") == "receipt_query"
               and e.get("key_hash") == kh]
    expect(len(finals) == 1,
           f"ZERO extra POST across crash/restart: {len(finals)}")
    expect(finals[0]["dropped"] and finals[0]["upstream_status"] == 201,
           "the one commit: parked real 201, dropped")
    expect(len(helds) == 1, "kill-window observation entry present")
    expect(len(queries) == 1, f"exactly one receipt GET {len(queries)}")
    expect(len(stack.dvs.commit_calls) == 1, "DVS saw exactly ONE POST")
    run = stack.connector.runs[run_id]
    expect(run["recovery"]["send_attempts"] == 1, "one send accounted")
    expect(run["recovery"]["query_attempts"] == 1, "one query accounted")
    # stateless JWT: the SAME platform bearer did the manual recovery
    recover_calls = [h for h in stack.dvs.headers_seen
                     if h["path"].endswith("/outputs:commit")]
    expect(all(h["authorization"] for h in recover_calls),
           "commit always carried the runtime credential")
    names = [p.name for p in ctx.evidence.path.iterdir()]
    expect(any("run-poll" in n for n in names), "run polls recorded")
    expect(any("no-auto-adoption" in n for n in names),
           "no-auto-adoption observation recorded")


@check("scenario-mechanics-pregate-no-intent-repull")
def _pregate_mechanics():
    stack = FakeStackTransport(behavior="manual")
    ctx = mechanics_ctx(stack, "mechanics-pregate")
    facts = scen.run_scenario("pregate_no_intent_owned_restart", ctx)
    expect(facts["outcome"] == "pass" and facts["cleanup_verified"], "pregate mechanics clean")
    expect(facts["pregate_db_proof"]["fake_only"], "fake proof explicitly labelled")
    expect(len(stack.dvs.commit_calls) == 1, "one publish after no-intent re-pull")


@check("scenario-mechanics-dropack-auto-retry-exact-replay")
def _s4():
    stack = FakeStackTransport(behavior="auto")
    ctx = mechanics_ctx(stack, "mechanics-dropack-auto")
    facts = scen.SCENARIOS["dropack_auto_retry_exact_replay"]["fn"](ctx)
    expect(facts["upstream_statuses"] == [201, 200],
           f"statuses {facts['upstream_statuses']}")
    run_id = facts["run_id"]
    ds = stack.dvs.datasets[facts["dataset_id"]]
    expect(len(ds["versions"]) == 1 and len(ds["receipts"]) == 1,
           "one version, one receipt")
    commits = [e for e in stack.proxy.ledger if e.get("kind") == "commit"]
    expect(len(commits) == 2, f"two dispatches {len(commits)}")
    expect(commits[0]["request_body_sha256"]
           == commits[1]["request_body_sha256"], "byte-identical replay")
    shas = [c["body_sha256"] for c in stack.dvs.commit_calls]
    expect(len(shas) == 2 and shas[0] == shas[1], "identical bodies")


@check("scenario-mechanics-inflight-receipt404-samebody-replay")
def _s5():
    stack = FakeStackTransport(behavior="manual")
    ctx = mechanics_ctx(stack, "mechanics-inflight")
    facts = scen.SCENARIOS[
        "inflight_receipt404_samebody_replay"]["fn"](ctx)
    expect(facts["inflight_receipt_404_observed"] is True, "404 observed")
    expect(facts["dispatch_statuses"] == [200, 201],
           f"pair statuses {facts['dispatch_statuses']}")
    run_id = facts["run_id"]
    kh = key_hash(scen.derive_publish_key(run_id))
    finals = [e for e in stack.proxy.ledger
              if e.get("kind") == "commit" and not e.get("held_phase")
              and e.get("request_key_hash") == kh]
    expect(len(finals) == 2, f"two overlapping dispatches {len(finals)}")
    expect(len({e["request_body_sha256"] for e in finals}) == 1,
           "both dispatches SAME body sha")
    ds = stack.dvs.datasets[facts["dataset_id"]]
    expect(len(ds["versions"]) == 1 and len(ds["receipts"]) == 1,
           "ONE version, ONE receipt after dedup")
    expect(len(stack.dvs.commit_calls) == 2,
           "DVS saw both POSTs (first-commit + immutable replay)")
    run = stack.connector.runs[run_id]
    expect(run["recovery"]["send_attempts"] == 2,
           "two sends accounted (original + recovery resend)")


@check("scenario-mechanics-twoinstances-exactly-one-claim")
def _s6():
    stack = FakeStackTransport(behavior="manual")
    ctx = mechanics_ctx(stack, "mechanics-twoinst")
    facts = scen.SCENARIOS["twoinstances_exactly_one_claim"]["fn"](ctx)
    expect(facts["loser_outcome"] == "busy_live_holder", "typed busy")
    expect(facts["commit_dispatches"] == 1, "single body dispatched")
    expect(facts["receipt_queries"] == 1,
           "exactly ONE receipt GET (A's held query; B never flew)")
    run_id = facts["run_id"]
    ds = stack.dvs.datasets[facts["dataset_id"]]
    expect(len(ds["versions"]) == 1, "one version")


@check("scenario-mechanics-budget-exhausted-then-lookup")
def _s7():
    stack = FakeStackTransport(behavior="budget3")
    ctx = mechanics_ctx(stack, "mechanics-budget")
    facts = scen.SCENARIOS[
        "budget_exhausted_then_lookup_finalize"]["fn"](ctx)
    expect(facts["send_attempts_final"] == 3, "budget 3 persistent")
    expect(facts["dispatch_attempts"] == 3, "three dispatch attempts")
    expect(facts["forwarded_dispatches"] == 1, "only send1 forwarded")
    expect(facts["cp_epoch_after"] == 1,
           "CP advanced exactly once on the incremental finalize")
    run_id = facts["run_id"]
    ds = stack.dvs.datasets[facts["dataset_id"]]
    expect(len(stack.dvs.commit_calls) == 1, "DVS saw exactly ONE POST")
    expect(len(ds["versions"]) == 1 and len(ds["receipts"]) == 1,
           "one version, one receipt")
    run = stack.connector.runs[run_id]
    expect(run["recovery"]["send_attempts"] == 3, "no fourth send")
    expect(run["recovery"]["query_attempts"] == 1, "one lookup query")


@check("scenario-mechanics-a-then-b-recover-a-head-unchanged")
def _s8():
    stack = FakeStackTransport(behavior="manual")
    ctx = mechanics_ctx(stack, "mechanics-aba")
    facts = scen.SCENARIOS[
        "a_then_b_recover_a_head_unchanged"]["fn"](ctx)
    expect(facts["version_count"] == 2, "two versions (A then B)")
    expect(facts["already_resolved_zero_outward"] is True,
           "recover A was a real-200 zero-outward already-resolved")
    ds = stack.dvs.datasets[facts["dataset_id"]]
    expect(len(ds["versions"]) == 2, "recover A wrote NOTHING")
    expect(facts["head_txn_b"] in {
        v["transaction_id"] for v in ds["versions"]},
        "head stays bound to B's receipt txn")
    run_a = stack.connector.runs[facts["run_a"]]
    expect(run_a["recovery"]["send_attempts"] == 1,
           "A's budgets unchanged by the already-resolved recovery")


@check("scenario-mechanics-crossscope-refusals")
def _s9():
    stack = FakeStackTransport(behavior="manual")
    ctx = mechanics_ctx(stack, "mechanics-refuse")
    facts = scen.SCENARIOS[
        "crossscope_permission_legacy_refusals"]["fn"](ctx)
    for flag in ("foreign_valid_404", "readonly_valid_403",
                 "missing_bearer_401", "dependency_error_stay_running",
                 "legacy_refused_row_unchanged"):
        expect(facts[flag] is True, f"{flag} missing")
    run = stack.connector.runs[facts["run_id"]]
    expect(run["status"] == "succeeded",
           "refusal-group run converged on the real committed receipt")
    expect(any(e.get("kind") == "receipt_error"
               for e in stack.proxy.ledger),
           "the dependency 502 was ledgered by the owned proxy")


@check("scenario-negative-refetch-after-intent-fires-assertion")
def _sn1():
    stack = FakeStackTransport(behavior="manual", refetch_on_recover=True)
    ctx = mechanics_ctx(stack, "mechanics-dropack-negative")
    try:
        scen.SCENARIOS[
            "dropack_manual_owned_restart_lookup"]["fn"](ctx)
        raise Fail("violation not detected")
    except ScenarioAssertionError as exc:
        expect("fixture /data span" in str(exc)
               or "ZERO fixture" in str(exc),
               f"expected span/refetch assertion, got: {exc}")


@check("scenario-unwired-connector-ops-fail-closed")
def _sn2():
    stack = FakeStackTransport(behavior="manual")
    ctx = mechanics_ctx(stack, "mechanics-ops-unwired", ops=False)
    try:
        scen.SCENARIOS[
            "dropack_manual_owned_restart_lookup"]["fn"](ctx)
        raise Fail("unwired ops must fail closed")
    except ScenarioAssertionError as exc:
        expect("not wired" in str(exc),
               f"expected unwired-ops refusal, got: {exc}")


@check("scenario-run-wrapper-cleanup-verified-even-on-failure")
def _sn3():
    stack = FakeStackTransport(behavior="manual", refetch_on_recover=True)
    ctx = mechanics_ctx(stack, "mechanics-cleanup-check")
    stack.proxy.set_mode("hold_request")
    try:
        scen.run_scenario("dropack_manual_owned_restart_lookup", ctx)
        raise Fail("expected scenario failure")
    except ScenarioAssertionError:
        pass
    expect(ctx.facts.get("outcome") == "fail", "outcome recorded")
    expect(ctx.facts.get("cleanup_verified") is True,
           f"cleanup verified even on failure: {ctx.facts}")
    names = [p.name for p in ctx.evidence.path.iterdir()]
    expect(any("cleanup-mode-reset" in n for n in names),
           "cleanup evidence present")


@check("cli-live-exit-code-matrix")
def _c0():
    sys.path.insert(0, str(HERE))
    import run_p26_http as cli
    expect(cli.live_exit_code({"outcome": "pass",
                               "cleanup_verified": True}) == 0,
           "pass+verified cleanup => 0")
    expect(cli.live_exit_code({"outcome": "pass"}) != 0,
           "pass WITHOUT verified cleanup => nonzero")
    expect(cli.live_exit_code({"outcome": "pass",
                               "cleanup_verified": False}) != 0,
           "cleanup_verified False => nonzero")
    expect(cli.live_exit_code({"outcome": "fail",
                               "cleanup_verified": True}) != 0,
           "fail stays nonzero")
    expect(cli.live_exit_code({"outcome": "fail-cleanup"}) != 0,
           "fail-cleanup stays nonzero")
    expect(cli.live_exit_code(None) != 0, "no facts => nonzero")


# ==========================================================================
# 5. CLI + serve refusal (zero-bind proofs)
# ==========================================================================

@check("owned-ops-adapter-pure")
def _o1():
    full_image = "sha256:" + "c" * 64

    def aux_doc(cid_a="a" * 64, cid_b="b" * 64, name_a="p26connectora",
                role_a="connector_a", image=full_image):
        return {"a": {"cid": cid_a, "name": name_a,
                      "labels": {"owner": "p26-http", "stage": "p2-6",
                                 "role": role_a}, "image": image},
                "b": {"cid": cid_b, "name": "p26connectorb",
                      "labels": {"owner": "p26-http", "stage": "p2-6",
                                 "role": "connector_b"},
                      "image": image}}

    aux, err = owned_ops.validate_release_aux({"w7_aux": aux_doc()})
    expect(err is None and aux is not None,
           f"valid w7_aux rejected: {err}")
    expect(aux["a"]["name"] == "p26connectora", "exact new name parsed")
    for bad, why in (
        ({}, "missing w7_aux"),
        ({"w7_aux": {"a": aux_doc()["a"]}}, "missing instance b"),
        ({"w7_aux": aux_doc(cid_a="short")}, "short cid"),
        ({"w7_aux": aux_doc(name_a="p25connectora")}, "old name"),
        ({"w7_aux": aux_doc(role_a="connector_b")}, "wrong role"),
        ({"w7_aux": aux_doc(cid_a="b" * 64)}, "same A/B cid"),
        ({"w7_aux": aux_doc(image="sha256:short")}, "short image"),
        ({"w7_aux": {"a": {"cid": "a" * 64, "name": "p26connectora",
                           "labels": {"owner": "p25", "stage": "p2-6",
                                      "role": "connector_a"},
                           "image": full_image},
                     "b": aux_doc()["b"]}}, "old owner label"),
    ):
        res, err = owned_ops.validate_release_aux(bad)
        expect(res is None, f"{why} must be refused ({err})")

    plan = owned_ops.plan_owned_ops(aux)
    expect(plan["valid"] is True, "plan generated")
    ops_verbs = {step["argv"][1] for step in plan["argv"]}
    expect(ops_verbs <= {"kill", "inspect", "start"},
           f"only kill/inspect/start on owned CIDs: {ops_verbs}")
    expect(plan["release_phase"] == "http-owned-ops", "own release phase")

    # unwired execution: phase not released => refusal BEFORE any
    # subprocess (the DEFAULT gated runner enforces this)
    ops = owned_ops.OwnedAuxOps(aux, release_file="/nonexistent/p26-owned-pure-release.json")
    try:
        ops.stop_connector("a")
        raise Fail("unreleased stop executed")
    except owned_ops.OwnedOpsError as exc:
        expect("refusing" in str(exc) and "http-owned-ops" in str(exc),
               f"gate refusal message: {exc}")
    # credential port: private env only, memory only
    import os
    os.environ["P26_W7_JWT_FOREIGN"] = "Bearer test-foreign"
    try:
        expect(ops.credential("foreign") == "Bearer test-foreign",
               "credential read from private env")
        try:
            ops.credential("writer")
            raise Fail("missing writer env should fail")
        except owned_ops.OwnedOpsError:
            pass
    finally:
        del os.environ["P26_W7_JWT_FOREIGN"]
    # ConnectorOps default stays fail-closed for scenarios
    try:
        scen.ConnectorOps().credential("writer")
        raise Fail("default credential port must fail closed")
    except scen.ScenarioAssertionError:
        pass


@check("cli-run-live-safe-wiring-refusals")
def _c2c():
    attempts_before = set((RESULTS / "attempts").glob("*"))
    base_rel = {
        "released_for": ["http-live-scenarios"],
        "expires_utc": "20990101T000000Z",
        "w7_bases": {
            "fixture": "http://127.0.0.1:17095",
            "proxy_admin": "http://127.0.0.1:17096",
            "dvs": "http://127.0.0.1:50078",
            "dvs_files": "http://127.0.0.1:17078",
            "connector": "http://127.0.0.1:17097"}}
    with tempfile.TemporaryDirectory() as tmp:
        def run_cli(rel, scenario, env_extra=None):
            rel_path = Path(tmp) / "rel.json"
            rel_path.write_text(json.dumps(rel))
            env = dict(os.environ)
            env["P26_RELEASE_FILE"] = str(rel_path)
            env.setdefault("OPENFOUNDRY_SMOKE_JWT", "Bearer x")
            env.setdefault("P26_HTTP_ADMIN_TOKEN", "t")
            env.update(env_extra or {})
            return subprocess.run(
                [sys.executable, "-I", "-B", str(HERE /
                 "run_p26_http.py"), "--run-live", scenario],
                capture_output=True, text=True, timeout=60, env=env)

        # ops-requiring scenario without w7_aux => refuse, zero attempts
        cp = run_cli(base_rel, "dropack_manual_owned_restart_lookup")
        expect(cp.returncode == 3, f"no w7_aux rc={cp.returncode}")
        expect("w7_aux" in cp.stderr, f"message: {cp.stderr.strip()[:120]}")
        # invalid w7_aux schema => refuse
        bad = dict(base_rel, w7_aux={"a": {"cid": "short"}})
        cp = run_cli(bad, "dropack_manual_owned_restart_lookup")
        expect(cp.returncode == 3 and "cid" in cp.stderr,
               "bad w7_aux schema refused")
        # crossscope without the private credential envs => refuse,
        # naming env NAMES only
        full_image = "sha256:" + "c" * 64
        good_aux = {"a": {"cid": "a" * 64, "name": "p26connectora",
                          "labels": {"owner": "p26-http",
                                     "stage": "p2-6",
                                     "role": "connector_a"},
                          "image": full_image},
                    "b": {"cid": "b" * 64, "name": "p26connectorb",
                          "labels": {"owner": "p26-http",
                                     "stage": "p2-6",
                                     "role": "connector_b"},
                          "image": full_image}}
        cp = run_cli(dict(base_rel, w7_aux=good_aux),
                     "crossscope_permission_legacy_refusals")
        expect(cp.returncode == 3, f"missing cred envs rc={cp.returncode}")
        expect("P26_W7_JWT_READONLY" in cp.stderr
               and "P26_W7_JWT_FOREIGN" in cp.stderr,
               f"env names listed: {cp.stderr.strip()[:140]}")
        expect("Bearer x" not in cp.stderr, "no credential VALUES")
        # twoinstances without an approved DISTINCT connector_b origin
        cp = run_cli(base_rel, "twoinstances_exactly_one_claim")
        expect(cp.returncode == 3 and "connector_b" in cp.stderr,
               "missing B origin refused")
        # same origin as A => refused at resolve time
        same = dict(base_rel, w7_bases=dict(
            base_rel["w7_bases"], connector_b="http://127.0.0.1:17097"))
        cp = run_cli(same, "twoinstances_exactly_one_claim")
        expect(cp.returncode == 3 and "DIFFER" in cp.stderr,
               "A==B origin refused")
        # no attempts directory was ever created by any refusal
        expect(set((RESULTS / "attempts").glob("*")) == attempts_before,
               "zero NEW attempts dirs from refusals (prior live evidence preserved)")


@check("cli-list-and-plan-live")
def _c1():
    cp = subprocess.run([sys.executable, "-I", "-B", str(HERE /
                         "run_p26_http.py"), "--list-scenarios"],
                        capture_output=True, text=True, timeout=60)
    expect(cp.returncode == 0, f"list rc={cp.returncode}")
    for name in ("dropack_manual_owned_restart_lookup",
                 "inflight_receipt404_samebody_replay",
                 "twoinstances_exactly_one_claim",
                 "budget_exhausted_then_lookup_finalize",
                 "a_then_b_recover_a_head_unchanged",
                 "crossscope_permission_legacy_refusals"):
        expect(name in cp.stdout, f"scenario {name} listed")
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "plan.json"
        cp = subprocess.run(
            [sys.executable, "-I", "-B", str(HERE / "run_p26_http.py"),
             "--plan-live", "dropack_manual_owned_restart_lookup",
             "--out", str(out)],
            capture_output=True, text=True, timeout=60)
        expect(cp.returncode == 0, f"plan rc={cp.returncode}")
        plan = json.loads(out.read_text())
        expect(plan["scenario"]
               == "dropack_manual_owned_restart_lookup", "plan name")
        expect(plan["port_facts_r2"]["q3_dataset_port"] == 50078,
               "q3 dataset port recorded (Go default 50117 unused)")
        expect("302" in plan["port_facts_r2"]["download_protocol"],
               "two-step download protocol documented")
        expect("dvs_files" in plan["release_file_schema"]["w7_bases"],
               "optional files origin documented")
        expect("w7_bases" in plan["release_file_schema"],
               "origin binding documented")


@check("cli-run-live-refused-without-release")
def _c2():
    env = dict(os.environ)
    env["P26_RELEASE_FILE"] = "/nonexistent/release.json"
    before = set((RESULTS / "attempts").glob("*")) \
        if (RESULTS / "attempts").exists() else set()
    cp = subprocess.run(
        [sys.executable, "-I", "-B", str(HERE / "run_p26_http.py"),
         "--run-live", "dropack_manual_owned_restart_lookup"],
        capture_output=True, text=True, timeout=60, env=env)
    expect(cp.returncode == 3, f"rc={cp.returncode}")
    expect("REFUSING" in cp.stderr, "refusal message")
    after = set((RESULTS / "attempts").glob("*")) \
        if (RESULTS / "attempts").exists() else set()
    expect(after == before, "zero attempts dir created on refusal")


@check("cli-run-live-rejects-unapproved-origin")
def _c2b():
    with tempfile.TemporaryDirectory() as tmp:
        rel = Path(tmp) / "rel.json"
        rel.write_text(json.dumps({
            "released_for": ["http-live-scenarios"],
            "expires_utc": "20990101T000000Z",
            "w7_bases": {
                "fixture": "http://127.0.0.1:17095",
                "proxy_admin": "http://127.0.0.1:17096",
                "dvs": "http://127.0.0.1:50117",
                "connector": "http://127.0.0.1:17097"}}))
        env = dict(os.environ)
        env["P26_RELEASE_FILE"] = str(rel)
        env["OPENFOUNDRY_SMOKE_JWT"] = "Bearer x"
        env["P26_HTTP_ADMIN_TOKEN"] = "t"
        cp = subprocess.run(
            [sys.executable, "-I", "-B", str(HERE / "run_p26_http.py"),
             "--run-live", "healthy_snapshot_and_legacy",
             "--dvs-base", "http://evil.example:9"],
            capture_output=True, text=True, timeout=60, env=env)
        expect(cp.returncode == 3, f"unapproved origin rc={cp.returncode}")
        expect("Root-approved" in cp.stderr, "origin refusal message")
        # no release w7_bases at all => refuse
        rel.write_text(json.dumps({
            "released_for": ["http-live-scenarios"],
            "expires_utc": "20990101T000000Z"}))
        cp = subprocess.run(
            [sys.executable, "-I", "-B", str(HERE / "run_p26_http.py"),
             "--run-live", "healthy_snapshot_and_legacy"],
            capture_output=True, text=True, timeout=60, env=env)
        expect(cp.returncode == 3, "missing w7_bases refused")


@check("serve-mains-refuse-without-release-zero-bind")
def _c3():
    real_srv = fixture_mod.ThreadingHTTPServer
    real_release_default = lib.DEFAULT_RELEASE_FILE
    trips = []

    def trip(*_a, **_k):
        trips.append(1)
        raise AssertionError("BIND ATTEMPTED")

    fixture_mod.ThreadingHTTPServer = trip
    try:
        lib.DEFAULT_RELEASE_FILE = "/nonexistent/release.json"
        rc = fixture_mod.main(
            ["p26_rest_source.py", "serve", "127.0.0.1", "18095",
             "--admin-token-env", "TOK"])
        expect(rc == 3, f"fixture serve rc={rc}")
        rc = proxy_mod.main(
            ["p26_dataset_fault_proxy.py", "serve", "127.0.0.1",
             "18096", "http://p26dvs:50117", "--admin-token-env", "TOK"])
        expect(rc == 3, f"proxy serve rc={rc}")
        expect(trips == [], "neither main attempted to bind")
        with tempfile.TemporaryDirectory() as tmp:
            rel = Path(tmp) / "rel.json"
            rel.write_text(json.dumps({
                "released_for": ["http-fixture-serve",
                                 "http-proxy-serve"],
                "expires_utc": "20990101T000000Z"}))
            lib.DEFAULT_RELEASE_FILE = str(rel)
            rc = proxy_mod.main(
                ["p26_dataset_fault_proxy.py", "serve", "127.0.0.1",
                 "18096", "http://p25fixture:8090",
                 "--admin-token-env", "TOK"])
            expect(rc == 3, "old-substrate upstream refused")
            rc = proxy_mod.main(
                ["p26_dataset_fault_proxy.py", "serve", "127.0.0.1",
                 "17092", "http://p26dvs:50117",
                 "--admin-token-env", "TOK"])
            expect(rc == 3, "old port refused")
            os.environ["TOK"] = ""
            rc = fixture_mod.main(
                ["p26_rest_source.py", "serve", "127.0.0.1", "18095",
                 "--admin-token-env", "TOK"])
            expect(rc == 3, "fixture refuses empty admin token")
            rc = fixture_mod.main(
                ["p26_rest_source.py", "serve", "127.0.0.1", "18095"])
            expect(rc == 3, "fixture refuses missing admin flag")
            os.environ["TOK"] = "real-admin-token"
            try:
                fixture_mod.main(
                    ["p26_rest_source.py", "serve", "127.0.0.1",
                     "18095", "--admin-token-env", "TOK"])
                raise Fail("bind tripwire did not fire")
            except AssertionError as exc:
                expect("BIND ATTEMPTED" in str(exc), "gated bind only")
            os.environ.pop("TOK", None)
    finally:
        fixture_mod.ThreadingHTTPServer = real_srv
        lib.DEFAULT_RELEASE_FILE = real_release_default


@check("admin-faces-fail-closed")
def _c4():
    stack = FakeStackTransport(behavior="manual")
    stack.fixture.admin_token = None
    resp = stack.request("GET", f"http://{FakeStackTransport.FIXTURE}"
                                "/admin/boot", headers={})
    expect(resp.status == 403, "fixture admin without token => 403")
    stack.fixture.admin_token = ADMIN_TOKEN
    resp = stack.request("GET", f"http://{FakeStackTransport.FIXTURE}"
                                "/admin/boot",
                         headers={"X-P26-Admin-Token": "wrong"})
    expect(resp.status == 403, "fixture admin wrong token => 403")
    resp = stack.request("GET", f"http://{FakeStackTransport.PROXY}"
                                "/admin/log",
                         headers={"X-P26-Admin-Token": "wrong"})
    expect(resp.status == 403, "proxy admin wrong token => 403")
    resp = stack.request("GET", f"http://{FakeStackTransport.FIXTURE}"
                                "/admin/boot",
                         headers={"X-P26-Admin-Token": ADMIN_TOKEN})
    expect(resp.status == 200, "correct token accepted")
    resp = stack.request("GET", f"http://{FakeStackTransport.FIXTURE}"
                                "/data?src=p26snap1&page_size=5",
                         headers={})
    expect(resp.status == 200, "data endpoint stays protocol-open")


# ==========================================================================
# runner
# ==========================================================================

def main():
    results = []
    for name, fn in CHECKS:
        t0 = time.monotonic()
        try:
            fn()
            results.append((name, True, None))
            print(f"PASS {name}")
        except Exception as exc:                    # noqa: BLE001
            results.append((name, False, f"{type(exc).__name__}: {exc}"))
            print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    n_pass = sum(1 for _, ok, _ in results if ok)
    print("-" * 60)
    for name, ok, err in results:
        if not ok:
            print(f"FAILED: {name}: {err}")
    verdict = (f"ALL {len(results)} CHECKS PASS"
               if n_pass == len(results)
               else f"{n_pass}/{len(results)} CHECKS PASS")
    print(verdict)
    return 0 if n_pass == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())

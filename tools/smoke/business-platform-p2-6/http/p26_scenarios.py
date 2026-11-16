#!/usr/bin/env python3
"""P2.6 W7 HTTP acceptance scenarios — executable orchestration + assertions.

R3 rework (2026-11-15, review-w7-round3-actual-contracts.md): every
fake-only contract is removed in favor of ACTUAL interfaces:
  * no W4 claim-split API / hold_after_claim / recovery_holder /
    retry-timer hooks — two-instance claim strength is observed by a
    REAL receipt GET held at the owned proxy (`hold_receipt_once`);
  * JWT verification is STATELESS: restart never invalidates an
    unexpired Bearer — negatives use actual semantics (no/garbage
    Bearer 401, valid read-only same-tenant 403, valid foreign-tenant
    404) with credentials supplied ONLY through the credential port;
  * counters live in run.recovery.send_attempts / query_attempts; the
    recovery response is flat run fields + recovery with NO outcome field;
    already-resolved = real 200 with the unchanged row and ZERO
    outward requests; legacy refusal pins the actual 'not eligible'
    wording;
  * inflight case now exercises the FULL same-body replay (original
    held BEFORE DVS → crash → lease expiry → receipt 404 → same-key
    exact-body POST while still held → release → dedup 200/one
    version), with `hold_request_once` one-shot so the second POST is
    never held by the global mode again;
  * budget uses the atomic `drop_then_fail_rest` proxy combo (the
    fail transition happens under the proxy lock BEFORE W3's retry —
    no external poll window, no timer hook), on an INCREMENTAL source,
    with owned crash/restart, a no-auto-adoption observation, and a
    lookup-only manual convergence (no fourth POST);
  * wrong-receipt starts from an actual unresolved crashed run: the
    owned proxy returns an actual dependency error (502,
    `fail_receipt_once`) and the run STAYS running — 'failed' is never
    an acceptable unknown; release then converges via fresh recovery;
  * A→B→recoverA binds the head to B's receipt Txn (NOT the DESC
    versions-list tail) and re-checks the full safe view unchanged;
  * proxied receipt probes ride the BUSINESS proxy client (platform
    JWT); the admin credential never appears on a business path (the
    proxy also strips it on outward hops as defense).

The FIRST-LIVE pair (healthy_snapshot_and_legacy,
dropack_auto_retry_exact_replay) is byte-identical to the published
checkpoint (results/w7/CHECKPOINT-firstlive-shas.json).

Live only after Root releases `http-live-scenarios` AND w7_bases in
the release file. Fake-stack runs (selftest) prove mechanics ONLY.
"""

import sys
import threading
import time

sys.dont_write_bytecode = True
sys.path.insert(0, str(__file__).rsplit("/", 1)[0])

from p26_http_lib import (BoundedWait, Client, EvidenceDir, HTTPResponse,
                          ScenarioAssertionError, key_hash,
                          opaque_digest, sha256_hex, utcstamp,
                          validated_download_location, safe_http_evidence)

PUBLISH_KEY_PREFIX = "of-p26-pub-v1"
TERMINAL_RUN_STATUSES = {"succeeded", "failed", "aborted"}
ADMIN_HEADER = "X-P26-Admin-Token"
FIXTURE_BASE_DEFAULT = "http://p26fixture:8090"


def derive_publish_key(run_id):
    return f"{PUBLISH_KEY_PREFIX}:{run_id}"


def source_config(src, incremental=False, recovery=True,
                  base_url=FIXTURE_BASE_DEFAULT, page_size=50):
    sync = {
        "resource_path": "/data",
        "records_path": "records",
        "query_params": {"src": src},
        "pagination": {"type": "cursor", "cursor_param": "cursor",
                       "next_cursor_path": "next_cursor",
                       "page_size": page_size,
                       "page_size_param": "page_size"},
    }
    if incremental:
        sync["incremental"] = {
            "enabled": True, "request_param": "since_token",
            "completed_checkpoint_path": "meta.completed_token"}
    if recovery:
        sync["recovery"] = {"enabled": True}
    return {
        "base_url": base_url,
        "auth": {"type": "none"},
        "runtime": {"worker": "foundry"},
        "permissions": {"syncable": True},
        "egress": {"allow_insecure_http": True},
        "sync": sync,
    }


# --------------------------------------------------------------------------
# owned connector process operations (R2 item 2; R3: actual contracts)
# --------------------------------------------------------------------------

class ConnectorOps:
    """Port for OWNED connector process control. The live adapter is
    http/p26_owned_ops.py (docker kill/start/inspect on the Root-
    released owned aux CIDs ONLY; boot identity = Docker StartedAt+PID;
    read-only lease observer via q3 psql; credentials from private env
    — the harness never mints JWTs). The default below is UNWIRED and
    fails closed."""

    def stop_connector(self, instance="a"):
        raise ScenarioAssertionError(
            "owned connector process ops not wired (W6 provides aux "
            "CIDs; http/p26_owned_ops.py is the live adapter)")

    def wait_stopped(self, timeout_s=30):
        raise ScenarioAssertionError("connector ops not wired")

    def restart_connector(self, instance="a"):
        raise ScenarioAssertionError("connector ops not wired")

    def wait_lease_expired(self, def_id, run_id, timeout_s=90):
        raise ScenarioAssertionError("connector ops not wired")

    def connector_boot_id(self, instance="a"):
        raise ScenarioAssertionError("connector ops not wired")

    def observe_pregate(self, def_id, run_id):
        raise ScenarioAssertionError("owned pre-gate DB observer not wired")

    def credential(self, kind):
        """VALID signed JWT VALUE by kind (writer | readonly | foreign),
        provided via the private credential port (live: Root env;
        memory only). NEVER a fabricated literal."""
        raise ScenarioAssertionError(
            f"credential port not wired for kind {kind!r}")


class LiveCtx:
    """Clients + evidence + credentials (ALL in-memory only).

    Credential separation: `platform_auth` rides ONLY the dvs/connector
    business clients; fixture/proxy admin carry `admin_token` in
    X-P26-Admin-Token. Live bases are Root-approved origins."""

    def __init__(self, transport, bases, platform_auth, admin_token,
                 evidence_root, label, ops=None,
                 approved_files_origin=None):
        self.evidence = EvidenceDir(evidence_root, label)
        self._seq = 0
        self.platform_auth = platform_auth
        self.admin_token = admin_token
        self.ops = ops or ConnectorOps()
        # Root-approved Dataset FILES origin for the presigned download
        # two-step (defaults to the dvs origin when release omits it)
        self.approved_files_origin = (approved_files_origin
                                      or bases["dvs"]).rstrip("/")

        def platform(base):
            return Client(transport, base, auth=platform_auth)

        def admin(base):
            return Client(transport, base, auth=None, extra_headers={
                ADMIN_HEADER: admin_token})

        self.fixture_admin = admin(bases["fixture"])
        self.proxy_admin = admin(bases["proxy_admin"])
        self.proxy = platform(bases["proxy_admin"])   # data plane via
        # the proxy (receipt GETs with the platform credential)
        self.dvs = platform(bases["dvs"])
        self.connector = platform(bases["connector"])
        self.connector_b = platform(bases.get("connector_b",
                                              bases["connector"]))
        self.anon = Client(transport, self.approved_files_origin,
                           auth=None)          # presign hop only
        self.transport = transport
        self.facts = {"owned_roster": []}
        if hasattr(self.ops, "bind_context"):
            self.ops.bind_context(self.facts["owned_roster"], self.evidence)

    # ---- evidence -------------------------------------------------------
    def _next_name(self, tag):
        self._seq += 1
        return f"{self._seq:03d}-{tag}"

    def http(self, tag, client, method, path, **kw):
        """One HTTP attempt = ONE append-only evidence step (polls use
        the same path — no unrecorded actual requests anywhere)."""
        rec = self.evidence.step(self._next_name(tag))
        rec.cmd([method, path], note=f"base={client.base}")
        artifact_bytes = kw.pop("evidence_download", False)
        try:
            resp = client.request(method, path, **kw)
        except Exception as exc:
            # Preserve the original exception after recording this actual
            # request attempt; no child process/HTTP status is invented.
            rec.result(HTTPResponse(err=exc))
            raise
        body = kw.get("json_body")
        meta = {"request": safe_http_evidence(body)} if isinstance(body, (dict, list)) else None
        rec.result(resp, meta, artifact_bytes=artifact_bytes)
        return resp

    def note(self, tag, rc, raw=b"", meta=None):
        rec = self.evidence.step(self._next_name(tag))
        rec.cmd(["note", tag])
        rec.finish(rc, raw, meta)
        return rec

    def roster(self, kind, obj_id, extra=None):
        entry = {"kind": kind, "id": obj_id}
        if extra:
            entry.update(extra)
        self.facts["owned_roster"].append(entry)

    def bearer_client(self, bearer_value, key="connector"):
        """A business client carrying an explicitly provided VALID
        credential from the credential port (stateless JWT — valid
        across restarts)."""
        base = {"connector": self.connector.base,
                "connector_b": self.connector_b.base,
                "dvs": self.dvs.base}[key]
        return Client(self.transport, base, auth=bearer_value)


# --------------------------------------------------------------------------
# assertion helpers (secret-free + opaque-free messages only)
# --------------------------------------------------------------------------

def assert_true(cond, what):
    if not cond:
        raise ScenarioAssertionError(f"ASSERT FAIL: {what}")


def assert_eq(actual, expected, what):
    if actual != expected:
        raise ScenarioAssertionError(
            f"ASSERT FAIL: {what}: expected={expected!r} "
            f"actual={actual!r}")


def require_json(resp, what):
    doc = resp.json()
    assert_true(isinstance(doc, dict),
                f"{what}: response is not a JSON object "
                f"({resp.describe()})")
    return doc


def recovery_of(run):
    """Actual public shape: counters/state live under run.recovery."""
    rec = (run or {}).get("recovery")
    return rec if isinstance(rec, dict) else {}


def wait_until(ctx, tag, poll_fn, what, timeout_s=60, interval_s=0.3):
    """Bounded wait where EVERY poll is a recorded ctx.http request."""

    def pred():
        try:
            return poll_fn() or None
        except Exception:                     # noqa: BLE001 — poll errors
            return None                       # are recorded inside poll_fn

    ok, last = BoundedWait(timeout_s, interval_s=interval_s).until(pred)
    if not ok:
        ctx.note(f"{tag}-timeout", 1, b"deadline exceeded",
                 {"what": what, "last": last})
        raise ScenarioAssertionError(
            f"{what}: condition not reached within {timeout_s}s "
            f"(last={last!r})")
    return last


def poll_run_row(ctx, tag, def_id, run_id, client=None):
    """Recorded GetSyncRun poll; returns the run row (or None)."""
    resp = ctx.http(f"{tag}-run-poll", client or ctx.connector, "GET",
                    f"/api/v1/data-connection/syncs/{def_id}/runs/{run_id}")
    doc = resp.json()
    return doc if isinstance(doc, dict) else None


def wait_run_terminal(ctx, tag, def_id, run_id, timeout_s=120,
                      client=None):
    def poll():
        doc = poll_run_row(ctx, tag, def_id, run_id, client)
        if doc and doc.get("status") in TERMINAL_RUN_STATUSES:
            return doc
        return None

    return wait_until(ctx, f"{tag}-wait-terminal", poll,
                      f"run {run_id} reaches a terminal status",
                      timeout_s=timeout_s, interval_s=1.0)


def poll_proxy_log(ctx, tag):
    resp = ctx.http(f"{tag}-proxy-log-poll", ctx.proxy_admin, "GET",
                    "/admin/log")
    return resp.json() or []


def commit_entries_for(ctx, tag, run_id):
    """Recorded poll: FINAL commit entries for THIS run key on THIS
    boot (pre-hold observation records carry held_phase=True and are
    NOT separate dispatches)."""

    def poll():
        found = [e for e in poll_proxy_log(ctx, tag)
                 if e.get("kind") == "commit"
                 and not e.get("held_phase")
                 and e.get("request_key_hash")
                 == key_hash(derive_publish_key(run_id))
                 and (ctx.facts.get("proxy_boot_id") is None
                      or e.get("boot_id") == ctx.facts["proxy_boot_id"])]
        return found or None

    return poll


def receipt_queries_for(ctx, tag, run_id):
    kh = key_hash(derive_publish_key(run_id))
    return [e for e in poll_proxy_log(ctx, tag)
            if e.get("kind") == "receipt_query"
            and e.get("key_hash") == kh
            and (ctx.facts.get("proxy_boot_id") is None
                 or e.get("boot_id") == ctx.facts["proxy_boot_id"])]


def fixture_delta(ctx, tag, src):
    resp = ctx.http(f"{tag}-fixture-src", ctx.fixture_admin, "GET",
                    f"/admin/src/{src}")
    assert_eq(resp.status, 200, f"fixture /admin/src/{src}")
    snapshot = require_json(resp, "fixture src snapshot")
    ctx.facts.setdefault("fixture_snapshots", []).append({
        "tag": tag, "src": src, "snapshot": safe_http_evidence(snapshot)})
    return snapshot


def assert_no_leak_markers(text, what):
    for marker in ("MARKER", "of-p26-pub-v1", "Bearer"):
        assert_true(marker not in (text or ""),
                    f"{what}: error body leaked {marker!r}")


# --------------------------------------------------------------------------
# shared blocks
# --------------------------------------------------------------------------

def preflight(ctx):
    boot = require_json(ctx.http("preflight-fixture-boot",
                                 ctx.fixture_admin, "GET", "/admin/boot"),
                        "fixture boot")
    ctx.facts["fixture_boot_id"] = boot.get("boot_id")
    pboot = require_json(ctx.http("preflight-proxy-boot",
                                  ctx.proxy_admin, "GET", "/admin/boot"),
                         "proxy boot")
    ctx.facts["proxy_boot_id"] = pboot.get("boot_id")
    stats = require_json(ctx.http("preflight-proxy-stats",
                                  ctx.proxy_admin, "GET", "/admin/stats"),
                         "proxy stats")
    ctx.facts["proxy_stats_baseline"] = dict(stats)
    assert_eq(stats.get("mode"), "passthrough", "proxy starts passthrough")
    assert_true(stats.get("inflight_quiet"), "proxy quiet at preflight")
    return boot



def wait_connector_ready(ctx, tag, client=None):
    client = client or ctx.connector
    return wait_until(ctx, tag + "-ready", lambda: (
        lambda r: r if r.status == 200 else None)(
            ctx.http(tag + "-health", client, "GET", "/healthz", timeout_s=3)),
        "restarted owned Connector /healthz returned 200", timeout_s=30,
        interval_s=0.5)


def restart_ready(ctx, tag, instance="a"):
    ctx.ops.restart_connector(instance)
    wait_connector_ready(ctx, tag, ctx.connector_b if instance == "b" else ctx.connector)


def branch_head(ctx, tag, dataset_id):
    response = ctx.http(tag, ctx.dvs, "GET",
                        f"/v1/datasets/{dataset_id}/branches/main")
    assert_eq(response.status, 200, "actual main branch readable")
    doc = require_json(response, "actual branch")
    assert_true(bool(doc.get("head_transaction_id")), "main has a head transaction")
    return doc["head_transaction_id"]

def fixture_expected(ctx, tag, src, window):
    exp = require_json(ctx.http(f"{tag}-fixture-expected",
                                ctx.fixture_admin, "GET",
                                f"/admin/expected/{src}?window={window}"),
                       "fixture expected")
    ctx.note(f"{tag}-expected-facts", 0, b"expected derived facts",
             {"src": exp["src"], "window": exp["window"],
              "rows": exp["rows"], "bytes": exp["bytes"],
              "ndjson_sha256": exp["ndjson_sha256"],
              "pages": exp["pages"],
              "terminal_token_digest": exp["terminal_token_digest"]})
    return exp


def create_dataset_source_def(ctx, tag, src, name_prefix,
                              incremental=False, recovery=True):
    resp = ctx.http(f"{tag}-create-dataset", ctx.dvs, "POST",
                    "/api/v1/datasets",
                    json_body={"name": name_prefix + utcstamp(),
                               "format": "text"})
    assert_eq(resp.status, 201, "dataset create status")
    dataset_id = require_json(resp, "dataset").get("id")
    assert_true(bool(dataset_id), "dataset id present")
    ctx.roster("dataset", dataset_id)
    resp = ctx.http(f"{tag}-create-source", ctx.connector, "POST",
                    "/api/v1/data-connection/sources",
                    json_body={"name": f"{name_prefix}-src",
                               "connector_type": "rest_api",
                               "config": source_config(
                                   src, incremental=incremental,
                                   recovery=recovery)})
    assert_eq(resp.status, 201, "source create status")
    source_id = require_json(resp, "source").get("id")
    ctx.roster("source", source_id, {"src": src})
    resp = ctx.http(f"{tag}-create-def", ctx.connector, "POST",
                    "/api/v1/data-connection/syncs",
                    json_body={"source_id": source_id,
                               "output_dataset_id": dataset_id})
    assert_eq(resp.status, 201, "sync def create status")
    def_id = require_json(resp, "sync").get("id")
    ctx.roster("sync_def", def_id, {"source": source_id})
    return dataset_id, source_id, def_id


def start_run(ctx, tag, def_id):
    resp = ctx.http(f"{tag}-run-async", ctx.connector, "POST",
                    f"/api/v1/data-connection/syncs/{def_id}/run"
                    "?async=true")
    assert_eq(resp.status, 202, "async run accepted (202)")
    run_id = require_json(resp, "run").get("id")
    assert_true(bool(run_id), "run id present")
    ctx.roster("run", run_id, {"def": def_id})
    return run_id


def recover_route(ctx, tag, def_id, run_id, client=None, timeout_s=30):
    return ctx.http(f"{tag}-recover", client or ctx.connector, "POST",
                    f"/api/v1/data-connection/syncs/{def_id}/runs/"
                    f"{run_id}/recover",
                    json_body={}, timeout_s=timeout_s)


def receipt_get(ctx, tag, dataset_id, key, client=None):
    return ctx.http(f"{tag}-receipt-get", client or ctx.dvs, "GET",
                    f"/api/v1/datasets/{dataset_id}/outputs/commits/{key}")


def assert_run_output(run, exp, label):
    assert_eq(run.get("status"), "succeeded", f"{label} run status")
    assert_eq(run.get("rows_written"), exp["rows"],
              f"{label} rows == fixture rows")
    assert_eq(run.get("bytes_written"), exp["bytes"],
              f"{label} bytes == fixture bytes")
    assert_eq(run.get("content_hash"), exp["ndjson_sha256"],
              f"{label} content_hash == fixture NDJSON sha256")


def list_versions_all(ctx, tag, dataset_id, max_pages=8):
    """Decode the ACTUAL ListVersions shape — models.Page
    {data:[...], next_cursor, has_more} (handlers.go:487-512; pagination
    via limit/cursor) — with a bounded walk and a hard has_more==False
    termination assertion. No unsupported fallback shapes."""
    items, cursor, pages = [], None, 0
    while True:
        pages += 1
        assert_true(pages <= max_pages,
                    f"versions pagination exceeded {max_pages} pages")
        path = f"/api/v1/datasets/{dataset_id}/versions?limit=500"
        if cursor:
            path += f"&cursor={cursor}"
        resp = ctx.http(f"{tag}-versions-p{pages}", ctx.dvs, "GET", path)
        assert_eq(resp.status, 200, "versions list status")
        doc = resp.json()
        assert_true(isinstance(doc, dict) and "data" in doc,
                    f"versions response is the actual Page envelope "
                    f"(data/has_more), got {type(doc).__name__}")
        page_items = doc.get("data") or []
        assert_true(isinstance(page_items, list), "data is a list")
        items.extend(page_items)
        if not doc.get("has_more"):
            return items
        cursor = doc.get("next_cursor")
        assert_true(bool(cursor),
                    "has_more without next_cursor (pagination bug)")


def prove_receipt_and_file(ctx, dataset_id, run_id, expected, tag_prefix):
    """Independent DIRECT-Dataset commit proof with whitelisted
    assertion inputs stored into facts for independent recheck."""
    key = derive_publish_key(run_id)
    resp = receipt_get(ctx, f"{tag_prefix}-receipt-proof", dataset_id,
                       key)
    assert_eq(resp.status, 200, f"{tag_prefix} receipt GET status")
    doc = require_json(resp, "receipt")
    txn = doc.get("transaction") or {}
    meta = txn.get("metadata") or {}
    receipt_facts = {
        "idempotency_key": doc.get("idempotency_key"),
        "request_fingerprint": doc.get("request_fingerprint"),
        "branch": doc.get("branch"),
        "txn_id": txn.get("id"),
        "txn_status": txn.get("status"),
        "content_hash": meta.get("content_hash"),
        "lineage_run_id": (meta.get("lineage") or {}).get("sync_run_id"),
    }
    assert_eq(receipt_facts["idempotency_key"], key, "receipt key echo")
    assert_true(len(str(receipt_facts["request_fingerprint"] or "")) == 64,
                "receipt request_fingerprint is 64 hex chars")
    assert_eq(receipt_facts["txn_status"], "COMMITTED",
              "receipt txn status")
    assert_true(bool(receipt_facts["txn_id"]), "receipt txn id present")
    assert_eq(receipt_facts["content_hash"], expected["ndjson_sha256"],
              "receipt metadata.content_hash == fixture NDJSON sha256")
    assert_eq(receipt_facts["lineage_run_id"], run_id,
              "receipt lineage run")

    versions = list_versions_all(ctx, f"{tag_prefix}-version-count",
                                 dataset_id)
    assert_eq(len(versions), expected.get("expected_versions", 1),
              "dataset version count (no replay duplicates)")

    resp = ctx.http(f"{tag_prefix}-file-list", ctx.dvs, "GET",
                    f"/api/v1/datasets/{dataset_id}/files")
    assert_eq(resp.status, 200, "files list status")
    files_doc = resp.json()
    assert_true(isinstance(files_doc, dict) and "files" in files_doc,
                f"files response uses the actual envelope (files/branch/"
                f"total), got {type(files_doc).__name__}")
    files = files_doc.get("files") or []
    assert_eq(len(files), expected.get("expected_files", 1),
              "committed file count")
    wanted = f"sync-{run_id}.ndjson"
    chosen = [f for f in files
              if wanted in str((f or {}).get("logical_path") or "")]
    if not chosen and len(files) == 1:
        chosen = files
    assert_eq(len(chosen), 1, f"file entry for this run ({wanted})")
    fid = (chosen[0] or {}).get("id") or (chosen[0] or {}).get("file_id")
    assert_true(bool(fid), "file id present")

    # ---- actual DownloadFile protocol: 302 presign + ONE validated
    # anonymous hop (handlers.go:748-796; two-step in
    # output_commit_content_test.go:97-114). Global no-redirect stays;
    # the Location is validated against the approved files origin and
    # its signature NEVER enters evidence (digest/length/path/expiry).
    resp = ctx.http(f"{tag_prefix}-file-presign", ctx.dvs, "GET",
                    f"/api/v1/datasets/{dataset_id}/files/{fid}/download")
    assert_eq(resp.status, 302, "download responds 302 (presigned)")
    location = (resp.headers or {}).get("Location") \
        or (resp.headers or {}).get("location")
    assert_true(bool(location), "Location header present")
    loc_facts, loc_err = validated_download_location(
        location, ctx.approved_files_origin)
    assert_true(loc_facts is not None,
                f"presign Location rejected: {loc_err}")
    from urllib.parse import urlsplit
    _u = urlsplit(location)
    follow_path = _u.path + (f"?{_u.query}" if _u.query else "")
    resp = ctx.http(f"{tag_prefix}-file-bytes", ctx.anon, "GET",
                    follow_path, evidence_download=True)
    assert_eq(resp.status, 200,
              "anonymous presign fetch is 200 after ONE hop")
    assert_true(not (300 <= resp.status < 400),
                "no second redirect allowed (max one hop)")
    ctx.note(f"{tag_prefix}-location-facts", 0,
             b"presign location safe facts", loc_facts)
    download_sha = sha256_hex(resp.body)
    assert_eq(download_sha, expected["ndjson_sha256"],
              "downloaded file sha256 == fixture expected NDJSON sha256")
    assert_eq(len(resp.body), expected["bytes"],
              "downloaded file byte count == fixture expected")

    proof = {
        "receipt": receipt_facts,
        "version_ids": [v.get("transaction_id") or v.get("id")
                        for v in versions],
        "version_count": len(versions),
        "file_id": fid,
        "file_download_sha256": download_sha,
        "file_download_bytes": len(resp.body),
        "expected_ndjson_sha256": expected["ndjson_sha256"],
        "expected_rows": expected["rows"],
    }
    ctx.note(f"{tag_prefix}-proof-inputs", 0,
             b"assertion inputs saved to facts", proof)
    return proof


def arm_proxy_mode(ctx, tag, mode):
    resp = ctx.http(f"{tag}-arm-{mode}", ctx.proxy_admin, "POST",
                    "/admin/mode", json_body={"mode": mode})
    assert_eq(resp.status, 200, f"proxy mode {mode} set")


def observe_no_auto_adoption(ctx, tag, run_id, dispatch_count,
                             send_attempts, timeout_s=6):
    """R3: 'no automatic adoption after restart' = with the persisted
    grant but no local arm, NOTHING automatic happens — observed by a
    bounded window of UNCHANGED counters/dispatches WITHOUT any manual
    call (recorded polls only)."""
    baseline_run = poll_run_row(ctx, f"{tag}-noauto-before", ctx.facts["def_id"], run_id)
    baseline_query = recovery_of(baseline_run).get("query_attempts")
    baseline_generation = baseline_run.get("generation")
    deadline = time.monotonic() + timeout_s
    stable = True
    while time.monotonic() < deadline:
        entries = commit_entries_for(ctx, f"{tag}-noauto", run_id)() or []
        run = poll_run_row(ctx, f"{tag}-noauto", ctx.facts["def_id"],
                           run_id)
        if len(entries) != dispatch_count \
                or recovery_of(run).get("send_attempts") != send_attempts \
                or recovery_of(run).get("query_attempts") != baseline_query \
                or run.get("generation") != baseline_generation:
            stable = False
            break
        time.sleep(0.3)
    assert_true(stable,
                "NO automatic adoption after restart: dispatch count "
                "and send budget unchanged without manual recovery")
    run = poll_run_row(ctx, f"{tag}-noauto-final",
                       ctx.facts["def_id"], run_id)
    assert_true(run.get("status") in {"running", "pending"},
                "run still unknown (no automatic terminal either)")
    ctx.note(f"{tag}-no-auto-adoption", 0, b"no automatic adoption "
             b"observed", {"dispatches": dispatch_count,
                           "send_attempts": send_attempts,
                           "query_attempts": baseline_query,
                           "generation": baseline_generation})


def verify_cleanup(ctx):
    """Recorded AND checked cleanup. Also verifies fixture quiet and
    writes the owned typed roster."""
    resp = ctx.http("cleanup-mode-reset", ctx.proxy_admin, "POST",
                    "/admin/mode", json_body={"mode": "passthrough"})
    assert_eq(resp.status, 200, "cleanup: proxy mode reset accepted")
    resp = ctx.http("cleanup-barrier-release", ctx.proxy_admin, "POST",
                    "/admin/barrier/release")
    assert_eq(resp.status, 200, "cleanup: barrier release accepted")
    stats = require_json(ctx.http("cleanup-stats", ctx.proxy_admin, "GET",
                                  "/admin/stats"), "cleanup stats")
    assert_true(stats.get("inflight_quiet"),
                "cleanup: proxy true quiet (inflight 0, started==finished)")
    barrier = require_json(ctx.http("cleanup-barrier-state",
                                    ctx.proxy_admin, "GET",
                                    "/admin/barrier"),
                           "cleanup barrier").get("barrier")
    assert_true(barrier is None or barrier.get("released"),
                "cleanup: no unreleased barrier remains")
    quiet = require_json(ctx.http("cleanup-fixture-quiet",
                                  ctx.fixture_admin, "GET",
                                  "/admin/quiet"), "fixture quiet")
    assert_true(quiet.get("quiet"), "cleanup: fixture true quiet")
    ctx.note("cleanup-owned-roster", 0, b"owned typed roster",
             {"roster": ctx.facts["owned_roster"]})
    ctx.facts["cleanup_verified"] = True


# ==========================================================================
# scenario 1: healthy snapshot + legacy opt-out (FIRST-LIVE #1 — the
# bytes of this function are pinned by CHECKPOINT-firstlive-shas.json)
# ==========================================================================

def scenario_healthy_snapshot_and_legacy(ctx):
    src, src_legacy = "p26snap1", "p26legacy"
    preflight(ctx)
    exp = fixture_expected(ctx, "01", src, 0)
    dataset_id, _s, def_id = create_dataset_source_def(
        ctx, "02", src, "p26-w7-healthy-snap-")

    before = fixture_delta(ctx, "03", src)
    run_id = start_run(ctx, "04", def_id)
    run = wait_run_terminal(ctx, "05", def_id, run_id)
    assert_run_output(run, exp, "healthy snapshot")
    ctx.note("05b-run-facts", 0, b"run facts",
             {k: run.get(k) for k in ("status", "rows_written",
                                      "bytes_written", "content_hash")})

    after = fixture_delta(ctx, "06", src)
    assert_eq(after["request_count"] - before["request_count"],
              exp["pages"], "fixture /data span == exact window pages")
    assert_true(all(t is None for t in
                    after["tokens_seen"][-exp["pages"]:]),
                "snapshot pull sent no since_token (digest seq: all NULL)")

    proof = prove_receipt_and_file(ctx, dataset_id, run_id, exp,
                                   "07-healthy-snap")

    legacy_dataset, _ls, legacy_def = create_dataset_source_def(
        ctx, "08", src_legacy, "p26-w7-legacy-", recovery=False)
    legacy_run_id = start_run(ctx, "09", legacy_def)
    legacy_run = wait_run_terminal(ctx, "10", legacy_def, legacy_run_id)
    assert_eq(legacy_run.get("status"), "succeeded",
              "legacy opt-out run still succeeds on the old path")

    resp = receipt_get(ctx, "11-legacy-no-keyed-receipt", legacy_dataset,
                       derive_publish_key(legacy_run_id))
    assert_eq(resp.status, 404,
              "legacy run has NO keyed publish receipt (old path)")

    ctx.facts.update({
        "scenario": "healthy_snapshot_and_legacy",
        "dataset_id": dataset_id, "run_id": run_id,
        "rows": exp["rows"], "bytes": exp["bytes"],
        "ndjson_sha256": exp["ndjson_sha256"],
        "legacy_run_id": legacy_run_id, **proof})
    return ctx.facts


# ==========================================================================
# scenario 2: healthy incremental, CP epoch exactly once per window
# ==========================================================================

def scenario_healthy_incremental_epoch_once(ctx):
    src = "p26inc1"
    preflight(ctx)
    exp_a = fixture_expected(ctx, "01", src, 0)
    exp_b = fixture_expected(ctx, "02", src, 1)
    dataset_id, _s, def_id = create_dataset_source_def(
        ctx, "03", src, "p26-w7-healthy-inc-", incremental=True)

    def checkpoint_view():
        r = ctx.http("cp-poll", ctx.connector, "GET",
                     f"/api/v1/data-connection/syncs/{def_id}/checkpoint")
        return r.json() if r.status == 200 else None

    cp0 = checkpoint_view()
    epoch0 = 0 if cp0 is None else int(cp0.get("confirmed_epoch") or 0)
    before = fixture_delta(ctx, "04", src)

    def run_once(tag, expected):
        run_id = start_run(ctx, tag, def_id)
        row = wait_run_terminal(ctx, f"{tag}b", def_id, run_id)
        assert_run_output(row, expected, tag)
        return run_id

    run1_id = run_once("05", exp_a)
    cp1 = wait_until(ctx, "06-cp-after-A", checkpoint_view,
                     "checkpoint initialized after window A")
    assert_true(cp1.get("initialized"),
                "checkpoint initialized after first incremental run")
    assert_eq(cp1.get("confirmed_epoch"), epoch0 + 1,
              "CP epoch advanced EXACTLY once for window A")
    assert_eq((cp1.get("last_confirmed") or {}).get("run_id"), run1_id,
              "last_confirmed.run_id == run A")

    run2_id = run_once("07", exp_b)

    def cp_epoch2():
        v = checkpoint_view()
        if v is not None and v.get("confirmed_epoch") \
                == cp1["confirmed_epoch"] + 1:
            return v
        return None

    cp2 = wait_until(ctx, "08-cp-after-B", cp_epoch2,
                     "CP epoch advanced exactly once more for window B")
    assert_eq((cp2.get("last_confirmed") or {}).get("run_id"), run2_id,
              "last_confirmed.run_id == run B")

    after = fixture_delta(ctx, "09", src)
    assert_eq(after["request_count"] - before["request_count"],
              exp_a["pages"] + exp_b["pages"],
              "fixture /data span == pages of both windows")
    assert_eq(after["tokens_seen"][-2:],
              [None, opaque_digest(f"{src}-tok-0")],
              "second run consumed completed token tok-0 (digest seq)")
    ctx.note("10-token-digest-seq", 0, b"safe digest sequence",
             {"last_two_tokens": after["tokens_seen"][-2:],
              "expected_second": opaque_digest(f"{src}-tok-0")})

    resp = ctx.http("11-exhausted-probe", ctx.fixture_admin, "GET",
                    f"/data?src={src}&since_token={src}-tok-1")
    assert_eq(resp.status, 200, "exhausted-position probe status")
    doc = require_json(resp, "exhausted probe")
    assert_eq(doc.get("records"), [], "exhausted position serves 0 rows")
    assert_eq((doc.get("meta") or {}).get("completed_token"),
              f"{src}-tok-1", "exhausted position re-issues same token")

    proof = prove_receipt_and_file(
        ctx, dataset_id, run2_id,
        {**exp_b, "expected_versions": 2, "expected_files": 2},
        "12-inc-final")
    ctx.facts.update({
        "scenario": "healthy_incremental_epoch_once",
        "dataset_id": dataset_id, "run_a": run1_id, "run_b": run2_id,
        "epoch0": epoch0, "epoch_after_a": cp1["confirmed_epoch"],
        "epoch_after_b": cp2["confirmed_epoch"],
        "head_version_ids": proof["version_ids"], **proof})
    return ctx.facts


# ==========================================================================
# scenario 3: dropACK + OWNED crash/restart + stateless-valid manual
#             LOOKUP-FIRST recovery (zero extra POST)
# ==========================================================================

def scenario_dropack_manual_owned_restart_lookup(ctx):
    """Deterministic crash window: `hold_drop_once` parks the REAL
    committed 201 at a bounded barrier (client provably still inside
    its request — no retry can have fired). Owned ops then run in the
    window: SIGKILL-stop → lease expiry (read-only observer) → restart
    (Docker boot identity changes; JWTs are STATELESS so the existing
    valid writer Bearer remains valid for an EXPLICIT manual request —
    what restart forbids is AUTOMATIC adoption, observed as unchanged
    counters without any manual call). Negatives use actual semantics:
    no/garbage Bearer 401, valid read-only same-tenant 403 — all with
    ZERO outward requests. The manual recovery is LOOKUP-FIRST: one
    receipt GET + finalize, ZERO extra commit POST."""
    src = "p26snap1"
    ops = ctx.ops
    preflight(ctx)
    exp = fixture_expected(ctx, "01", src, 0)
    dataset_id, _s, def_id = create_dataset_source_def(
        ctx, "02", src, "p26-w7-dropack-mr-")


    arm_proxy_mode(ctx, "04", "hold_drop_once")
    before = fixture_delta(ctx, "05", src)
    run_id = start_run(ctx, "06", def_id)
    boot_before = ops.connector_boot_id()
    ctx.note("03-boot-before", 0, b"connector boot before",
             {"boot_id": boot_before})

    key = derive_publish_key(run_id)
    ctx.facts.update({"run_id": run_id, "dataset_id": dataset_id,
                      "def_id": def_id})

    def held_entry_poll():
        found = [e for e in poll_proxy_log(ctx, "07h")
                 if e.get("kind") == "commit" and e.get("held_phase")
                 and e.get("request_key_hash") == key_hash(key)
                 and e.get("boot_id") == ctx.facts["proxy_boot_id"]]
        return found or None

    held = wait_until(ctx, "07-held-commit", held_entry_poll,
                      "real 201 parked at the kill-window barrier")
    assert_eq(held[0].get("upstream_status"), 201,
              "parked dispatch saw the REAL first-commit 201")
    assert_true(held[0].get("verified_committed"),
                "parked dispatch ACK passed the strict verdict")
    after_intent = fixture_delta(ctx, "08", src)

    ops.stop_connector()
    ops.wait_stopped()
    ctx.note("09-owned-stop", 0, b"connector SIGKILLed inside window",
             {"held_body_sha256": held[0].get("request_body_sha256")})

    resp = ctx.http("10-release-kill-window", ctx.proxy_admin, "POST",
                    "/admin/barrier/release")
    assert_eq(resp.status, 200, "kill-window barrier released")

    lease_expired = ops.wait_lease_expired(def_id, run_id)
    assert_true(lease_expired, "lease observed expired (read-only)")
    restart_ready(ctx, "12-restart")
    boot_after = ops.connector_boot_id()
    assert_true(boot_after != boot_before,
                "Docker boot identity CHANGED across the owned restart")
    ctx.note("12-owned-restart", 0, b"connector restarted (new boot)",
             {"boot_id": boot_after, "boot_before": boot_before})

    # no AUTOMATIC adoption without a manual call (counters frozen)
    observe_no_auto_adoption(ctx, "13", run_id, 1, 1)

    # negatives with ACTUAL semantics — all zero outward
    dispatch_before = len(commit_entries_for(ctx, "14", run_id)() or [])
    noauth = Client(ctx.transport, ctx.connector.base, auth=None)
    resp = recover_route(ctx, "14-noauth-401", def_id, run_id,
                         client=noauth)
    assert_eq(resp.status, 401, "missing Bearer => 401")
    garbage = ctx.bearer_client("Bearer garbage-not-a-jwt")
    resp = recover_route(ctx, "14b-garbage-401", def_id, run_id,
                         client=garbage)
    assert_eq(resp.status, 401, "invalid Bearer => 401")
    # The live credential port checks same org + DIFFERENT nonowner sub,
    # empty roles and connections:read only (owner always has write).
    readonly_bearer = ops.credential("readonly")
    ro_client = ctx.bearer_client(readonly_bearer)
    resp = recover_route(ctx, "14c-readonly-403", def_id, run_id,
                         client=ro_client)
    assert_eq(resp.status, 403,
              "VALID read-only same-tenant credential => 403")
    assert_no_leak_markers((resp.body or b"").decode("utf-8", "replace"),
                           "readonly 403 body")
    assert_eq(len(commit_entries_for(ctx, "14d", run_id)() or []),
              dispatch_before,
              "all three rejections caused ZERO outward dispatches")

    # manual recover with the STATELESS-valid writer Bearer (explicit
    # request — valid across the restart; never automatic)
    resp = recover_route(ctx, "15-fresh-recover", def_id, run_id)
    assert_true(resp.status in (200, 202),
                f"manual recover accepted (status={resp.status})")

    run = wait_run_terminal(ctx, "16", def_id, run_id, timeout_s=180)
    assert_eq(run.get("status"), "succeeded",
              "run finalizes after receipt-only recovery")
    assert_eq(run.get("content_hash"), exp["ndjson_sha256"],
              "finalized run content_hash == fixture NDJSON sha256")
    assert_eq(recovery_of(run).get("send_attempts"), 1,
              "exactly ONE send consumed (no retry ever fired)")

    entries = commit_entries_for(ctx, "17", run_id)() or []
    assert_eq(len(entries), 1,
              "exactly ONE commit dispatch total (zero extra POST)")
    final_entry = entries[-1]
    assert_true(final_entry.get("dropped"), "the one dispatch was dropped")
    queries = receipt_queries_for(ctx, "18", run_id)
    assert_eq(len(queries), 1,
              "exactly ONE receipt GET by the manual recovery")
    assert_eq(queries[0].get("upstream_status"), 200,
              "receipt GET returned the committed receipt")
    assert_eq(recovery_of(run).get("query_attempts"), 1,
              "query budget consumed exactly once")

    final_fixture = fixture_delta(ctx, "19", src)
    assert_eq(final_fixture["request_count"] - before["request_count"],
              exp["pages"],
              "total fixture /data span == exact window pages")
    assert_eq(final_fixture["request_count"],
              after_intent["request_count"],
              "ZERO fixture /data calls after the publish intent")

    proof = prove_receipt_and_file(ctx, dataset_id, run_id, exp,
                                   "20-dropack-mr")
    stats = require_json(ctx.http("21-proxy-stats", ctx.proxy_admin,
                                  "GET", "/admin/stats"), "proxy stats")
    ctx.facts["proxy_stats_final"] = dict(stats)
    assert_eq(stats.get("drops", 0) -
              ctx.facts["proxy_stats_baseline"].get("drops", 0), 1,
              "exactly one dropped ACK in this scenario baseline delta")

    ctx.facts.update({
        "scenario": "dropack_manual_owned_restart_lookup",
        "connector_boot_before": boot_before,
        "connector_boot_after": boot_after,
        "commit_dispatches": len(entries),
        "receipt_queries": len(queries), **proof})
    return ctx.facts


# ==========================================================================
# scenario 4: dropACK → automatic bounded retry (exact-key replay)
# (FIRST-LIVE #2 — bytes pinned by CHECKPOINT-firstlive-shas.json)
# ==========================================================================

def scenario_dropack_auto_retry_exact_replay(ctx):
    src = "p26snap1"
    preflight(ctx)
    exp = fixture_expected(ctx, "01", src, 0)
    dataset_id, _s, def_id = create_dataset_source_def(
        ctx, "02", src, "p26-w7-dropack-a-")

    arm_proxy_mode(ctx, "03", "drop_ack_once")
    before = fixture_delta(ctx, "04", src)
    run_id = start_run(ctx, "05", def_id)
    ctx.facts.update({"run_id": run_id, "dataset_id": dataset_id,
                      "def_id": def_id})

    poll_entries = commit_entries_for(ctx, "06", run_id)
    entries = wait_until(
        ctx, "06-two-dispatches",
        lambda: (lambda es: es if len(es) >= 2 else None)(
            poll_entries()),
        "two commit dispatches (this run key, this boot)")
    first, second = entries
    assert_true(first.get("dropped") and first.get("forwarded"),
                "first dispatch: forwarded AND dropped (real 201)")
    assert_eq(first.get("upstream_status"), 201,
              "first dispatch saw the REAL first-commit 201")
    assert_true(second.get("forwarded") and not second.get("dropped"),
                "retry dispatch: forwarded, NOT dropped")
    assert_eq(second.get("upstream_status"), 200,
              "retry got the immutable replay 200")
    assert_eq(second.get("request_body_sha256"),
              first.get("request_body_sha256"),
              "retry sent BYTE-IDENTICAL body (same raw sha256)")
    assert_eq(second.get("request_key_hash"),
              first.get("request_key_hash"),
              "retry used the SAME run-scoped key")
    after_intent = fixture_delta(ctx, "07", src)

    run = wait_run_terminal(ctx, "08", def_id, run_id, timeout_s=180)
    assert_eq(run.get("status"), "succeeded",
              "run finalizes after exact replay")
    assert_eq(run.get("content_hash"), exp["ndjson_sha256"],
              "finalized run content_hash == fixture NDJSON sha256")

    entries = poll_entries()
    assert_eq(len(entries), 2,
              "exactly two commit dispatches for this run key")
    final_fixture = fixture_delta(ctx, "09", src)
    assert_eq(final_fixture["request_count"] - before["request_count"],
              exp["pages"],
              "total fixture /data span == exact window pages")
    assert_eq(final_fixture["request_count"],
              after_intent["request_count"],
              "ZERO fixture /data calls after the publish intent")

    proof = prove_receipt_and_file(ctx, dataset_id, run_id, exp,
                                   "10-dropack-auto")
    stats = require_json(ctx.http("11-proxy-stats", ctx.proxy_admin,
                                  "GET", "/admin/stats"), "proxy stats")
    ctx.facts["proxy_stats_final"] = dict(stats)
    assert_eq(stats.get("drops", 0) -
              ctx.facts["proxy_stats_baseline"].get("drops", 0), 1,
              "exactly one dropped ACK in this scenario baseline delta")

    ctx.facts.update({
        "scenario": "dropack_auto_retry_exact_replay",
        "body_sha256": second.get("request_body_sha256"),
        "upstream_statuses": [first.get("upstream_status"),
                              second.get("upstream_status")], **proof})
    return ctx.facts


# ==========================================================================
# scenario 5: REAL in-flight receipt-404 → SAME-BODY replay → dedup
# ==========================================================================

def scenario_inflight_receipt404_samebody_replay(ctx):
    """Original dispatch is held BEFORE the DVS forward (clearly a
    proxy-held-before-DVS window, NOT a DB commit-in-flight — DVS
    serialization is proven by the real PG tests). Sequence: direct AND
    business-proxied receipt GETs are 404 while held; the owned
    original connector crashes and its lease expires; manual recovery
    sees receipt-404, budget remains ⇒ SAME key + BYTE-IDENTICAL body
    POST passes (one-shot hold, never held again) and becomes the REAL
    first commit (201); releasing the original lets it forward and get
    the immutable replay 200 — the crashed executor discards it (lease
    lost): ONE version, ONE receipt, no double finalize, and the owner
    released only on the actual committed receipt."""
    src = "p26snap1"
    ops = ctx.ops
    preflight(ctx)
    exp = fixture_expected(ctx, "01", src, 0)
    dataset_id, _s, def_id = create_dataset_source_def(
        ctx, "02", src, "p26-w7-inflight-")

    source_before = fixture_delta(ctx, "02b-source-before", src)

    arm_proxy_mode(ctx, "03", "hold_request_once")
    run_id = start_run(ctx, "04", def_id)
    key = derive_publish_key(run_id)
    ctx.facts.update({"run_id": run_id, "dataset_id": dataset_id,
                      "def_id": def_id})

    def held_before_dvs():
        found = [e for e in poll_proxy_log(ctx, "05h")
                 if e.get("kind") == "commit"
                 and e.get("held_before_forward")
                 and e.get("request_key_hash") == key_hash(key)
                 and e.get("boot_id") == ctx.facts["proxy_boot_id"]]
        return found or None

    wait_until(ctx, "05-held-before-dvs", held_before_dvs,
               "original dispatch observed held BEFORE the DVS forward")

    source_at_intent = fixture_delta(ctx, "05-source-after-intent", src)
    # receipt queries WHILE genuinely in flight — business client for
    # the proxied probe (platform JWT; the admin credential never rides
    # a business path)
    resp = receipt_get(ctx, "06-inflight-direct-404", dataset_id, key)
    assert_eq(resp.status, 404,
              "direct receipt GET is 404 while the commit is in flight")
    resp = receipt_get(ctx, "07-inflight-proxy-404", dataset_id, key,
                       client=ctx.proxy)
    assert_eq(resp.status, 404,
              "proxied receipt GET is 404 while the commit is in flight")

    # owned crash + lease expiry of the ORIGINAL executor
    ops.stop_connector()
    ops.wait_stopped()
    assert_true(ops.wait_lease_expired(def_id, run_id),
                "original executor lease observed expired")
    restart_ready(ctx, "08-restart")
    run = poll_run_row(ctx, "08", def_id, run_id)
    assert_true(run.get("status") in {"running", "pending"},
                "run is unknown (running) before any committed receipt")

    # manual recovery: receipt 404 + remaining budget ⇒ same-body POST.
    # The ORIGINAL stays parked (one-shot hold) until WE release it.
    resp = recover_route(ctx, "09-recover-resend", def_id, run_id)
    assert_true(resp.status in (200, 202),
                f"recovery accepted for the same-body resend "
                f"({resp.status})")

    def resend_final_poll():
        es = commit_entries_for(ctx, "10", run_id)() or []
        found = [e for e in es if e.get("forwarded")
                 and not e.get("held_released_by")]
        return found or None

    resend = wait_until(ctx, "10-resend-dispatch", resend_final_poll,
                        "recovery resend dispatch observed (passes the "
                        "one-shot hold)")[-1]
    assert_eq(resend.get("upstream_status"), 201,
              "same-body POST became the REAL first commit (201)")
    held_entry = [e for e in poll_proxy_log(ctx, "10b")
                  if e.get("kind") == "commit"
                  and e.get("held_before_forward")
                  and e.get("request_key_hash")
                  == key_hash(key)][-1]
    assert_eq(resend.get("request_body_sha256"),
              held_entry.get("request_body_sha256"),
              "recovery resent the BYTE-IDENTICAL body")
    assert_eq(resend.get("request_key_hash"),
              held_entry.get("request_key_hash"),
              "recovery used the SAME run-scoped key")

    run = wait_run_terminal(ctx, "11", def_id, run_id, timeout_s=120)
    assert_eq(run.get("status"), "succeeded",
              "run finalized on the actual committed receipt")

    # release the ORIGINAL now: it forwards, gets the immutable replay
    # 200, and the crashed executor discards it (no double finalize)
    resp = ctx.http("12-release-original", ctx.proxy_admin, "POST",
                    "/admin/barrier/release")
    assert_eq(resp.status, 200, "original dispatch released")

    def original_final_poll():
        es = commit_entries_for(ctx, "13", run_id)() or []
        found = [e for e in es if e.get("held_released_by")]
        return found or None

    original_final = wait_until(
        ctx, "13-original-completed", original_final_poll,
        "original dispatch completed after release")[-1]
    assert_eq(original_final.get("upstream_status"), 200,
              "released original got the immutable replay 200")
    statuses = sorted([resend.get("upstream_status"),
                       original_final.get("upstream_status")])
    assert_eq(statuses, [200, 201],
              "dispatch pair = [201 first-commit, 200 immutable replay]")
    assert_eq(original_final.get("request_body_sha256"),
              resend.get("request_body_sha256"),
              "both dispatches carried the SAME body sha")
    forwarded = [resend, original_final]

    run_after = poll_run_row(ctx, "14", def_id, run_id)
    assert_eq(run_after.get("status"), "succeeded",
              "no regression after the late replay")
    assert_eq(recovery_of(run_after).get("send_attempts"), 2,
              "exactly two sends accounted (original + recovery)")

    source_final = fixture_delta(ctx, "14b-source-final", src)
    assert_eq(source_final["request_count"], source_at_intent["request_count"],
              "inflight recovery never re-fetched Source after intent")
    assert_eq(source_final["request_count"]-source_before["request_count"], exp["pages"],
              "inflight original consumed exactly expected pages")
    proof = prove_receipt_and_file(ctx, dataset_id, run_id, exp,
                                   "15-inflight")
    assert_eq(proof["version_count"], 1,
              "exactly ONE version despite two overlapping dispatches")
    ds_probe = receipt_get(ctx, "16-receipt-immutable", dataset_id, key)
    assert_eq(ds_probe.status, 200, "receipt readable after convergence")

    ctx.facts.update({
        "scenario": "inflight_receipt404_samebody_replay",
        "inflight_receipt_404_observed": True,
        "dispatch_statuses": statuses,
        "body_sha256": forwarded[0].get("request_body_sha256"),
        **proof})
    return ctx.facts


# ==========================================================================
# scenario 6: two instances recover the same run — exactly one claim
# ==========================================================================

def scenario_twoinstances_exactly_one_claim(ctx):
    """No W4 claim API exists — claim strength is observed at the OWNED
    proxy: instance A's recovery performs a REAL receipt GET whose
    response is parked (`hold_receipt_once`, one-shot); while it is
    held, instance B's recovery is refused with a TYPED busy and ZERO
    outward requests. Releasing completes A's lookup; A alone
    finalizes; exactly one body was ever dispatched."""
    assert_true(ctx.connector_b.base != ctx.connector.base,
                "twoinstances requires a DISTINCT approved connector_b "
                "origin (w7_bases.connector_b) — never a silent A "
                "fallback")
    src = "p26snap1"
    preflight(ctx)
    exp = fixture_expected(ctx, "01", src, 0)
    dataset_id, _s, def_id = create_dataset_source_def(
        ctx, "02", src, "p26-w7-twoinst-")

    source_before = fixture_delta(ctx, "02b-source-before", src)

    arm_proxy_mode(ctx, "03", "hold_drop_once")
    run_id = start_run(ctx, "04", def_id)
    ctx.facts.update({"run_id": run_id, "dataset_id": dataset_id,
                      "def_id": def_id})
    poll_entries = commit_entries_for(ctx, "05", run_id)
    def original_held():
        return [e for e in poll_proxy_log(ctx, "05h")
                if e.get("kind") == "commit" and e.get("held_phase")
                and e.get("request_key_hash") == key_hash(derive_publish_key(run_id))
                and e.get("boot_id") == ctx.facts["proxy_boot_id"]] or None
    held = wait_until(ctx, "05-original-held", original_held,
                      "original verified 201 held before ACK")
    assert_eq(held[0].get("upstream_status"), 201, "original real commit")
    assert_true(held[0].get("verified_committed"), "original commit verified")
    original_row = poll_run_row(ctx, "05-original-row", def_id, run_id)
    source_at_intent = fixture_delta(ctx, "05-source-after-intent", src)
    ctx.ops.stop_connector()
    released = ctx.http("05-release-original", ctx.proxy_admin, "POST", "/admin/barrier/release")
    assert_eq(released.status, 200, "release original after SIGKILL")
    wait_until(ctx, "05-original-finished", poll_entries, "original proxy operation completed")
    assert_true(ctx.ops.wait_lease_expired(def_id, run_id), "original claim DB-expired")
    restart_ready(ctx, "05-restart")
    arm_proxy_mode(ctx, "06", "hold_receipt_once")
    holder = {"done": threading.Event(), "resp": None}
    def recover_a():
        try:
            holder["resp"] = recover_route(ctx, "06a-a-recover", def_id, run_id, timeout_s=60)
        except Exception as exc:
            holder["error"] = type(exc).__name__
        finally:
            holder["done"].set()
    a_thread = threading.Thread(target=recover_a, daemon=True)
    try:
        a_thread.start()

        def held_receipt_poll():
            found = [e for e in poll_proxy_log(ctx, "07h")
                     if e.get("kind") == "receipt_query"
                     and e.get("held_response")
                     and e.get("key_hash")
                     == key_hash(derive_publish_key(run_id))
                     and e.get("boot_id") == ctx.facts["proxy_boot_id"]]
            return found or None

        wait_until(ctx, "07-a-claim-held", held_receipt_poll,
                   "instance A's recovery receipt GET observed HELD at "
                   "the owned proxy (claim strength)")

        row_claimed = poll_run_row(ctx, "08-a-claimed", def_id, run_id)
        assert_eq(row_claimed.get("generation"), original_row.get("generation", 0)+1,
                  "A alone acquired exactly the next fenced generation")
        assert_eq(recovery_of(row_claimed).get("send_attempts"), 1, "A claim sent no second body")
        assert_eq(recovery_of(row_claimed).get("query_attempts"), 1, "A consumed one held query")
        log_before = len(poll_proxy_log(ctx, "08"))
        entries_before = len(poll_entries() or [])
        resp = recover_route(ctx, "08-b-busy", def_id, run_id,
                             client=ctx.connector_b)
        assert_eq(resp.status, 409,
                  "instance B recovery is TYPED busy (409) while A "
                  "holds the claim")
        assert_no_leak_markers(
            (resp.body or b"").decode("utf-8", "replace"),
            "busy error body")
        assert_eq(len(poll_proxy_log(ctx, "08b")), log_before,
                  "loser instance made ZERO outward proxy requests")
        assert_eq(len(poll_entries() or []), entries_before,
                  "loser instance caused ZERO commit dispatches")

        assert_eq(poll_run_row(ctx, "08c-b-no-write", def_id, run_id), row_claimed,
                  "B loser did not change full safe Run or claim generation")
        resp = ctx.http("09-release-a", ctx.proxy_admin, "POST",
                        "/admin/barrier/release")
        assert_eq(resp.status, 200, "A's held receipt released")
        assert_true(holder["done"].wait(timeout=60),
                    "A's recovery call returned")
        a_thread.join(timeout=30)
        assert_true(not a_thread.is_alive(), "A thread joined")
        assert_true(holder["resp"] is not None
                    and holder["resp"].status in (200, 202),
                    "A's recovery accepted")
    finally:
        # Release BEFORE joining; preserve a primary assertion/HTTP failure.
        primary = sys.exc_info()[0] is not None
        release_ok = False
        try:
            released = ctx.http("09-finally-release", ctx.proxy_admin, "POST",
                                "/admin/barrier/release")
            release_ok = released.status == 200
        except Exception as exc:
            ctx.facts["twoinstance_release_error"] = type(exc).__name__
        finally:
            a_thread.join(timeout=65)
            cleanup_ok = release_ok and not a_thread.is_alive()
            ctx.facts["twoinstance_thread_cleanup"] = cleanup_ok
            if not cleanup_ok and not primary:
                raise ScenarioAssertionError("twoinstance barrier/thread cleanup failed")

    run = wait_run_terminal(ctx, "10", def_id, run_id, timeout_s=120)
    assert_eq(run.get("status"), "succeeded", "A alone finalizes the run")

    assert_eq(run.get("generation"), row_claimed.get("generation"), "only A's claim finalized")
    source_final = fixture_delta(ctx, "10b-source-final", src)
    assert_eq(source_final["request_count"], source_at_intent["request_count"],
              "two-instance recovery never re-fetched Source")
    assert_eq(source_final["request_count"]-source_before["request_count"], exp["pages"],
              "original source pull used expected pages only")
    entries = poll_entries() or []
    assert_eq(len(entries), 1,
              "exactly ONE commit dispatch total — no second body ever")
    queries = receipt_queries_for(ctx, "11", run_id)
    assert_eq(len(queries), 1,
              "exactly ONE receipt GET (A's held query; B never flew)")
    proof = prove_receipt_and_file(ctx, dataset_id, run_id, exp,
                                   "12-twoinst")
    assert_eq(proof["version_count"], 1, "exactly one version")

    ctx.facts.update({
        "scenario": "twoinstances_exactly_one_claim",
        "loser_outcome": "busy_live_holder",
        "commit_dispatches": len(entries),
        "receipt_queries": len(queries), **proof})
    return ctx.facts


# ==========================================================================
# scenario 7: budget exhaustion (incremental) → lookup-only finalize
# ==========================================================================

def scenario_budget_exhausted_then_lookup_finalize(ctx):
    """INCREMENTAL source. `drop_then_fail_rest` makes the FIRST
    verified 201 drop and ATOMICALLY (under the proxy lock) refuses
    every further attempt before forward — W3's timer-driven retries
    deterministically exhaust the PERSISTENT budget of 3 with zero
    external race. Unknown stays running with the CP owner held; the
    owned crash/restart shows NO automatic adoption (counters frozen
    without manual); negatives are actual (no Bearer 401); the manual
    recovery performs ONE lookup query and finalizes with NO fourth
    POST."""
    src = "p26inc1"
    ops = ctx.ops
    preflight(ctx)
    exp = fixture_expected(ctx, "01", src, 0)
    dataset_id, _s, def_id = create_dataset_source_def(
        ctx, "02", src, "p26-w7-budget-", incremental=True)

    source_before = fixture_delta(ctx, "02b-source-before", src)

    arm_proxy_mode(ctx, "03", "drop_then_fail_rest")
    run_id = start_run(ctx, "04", def_id)
    ctx.facts.update({"run_id": run_id, "dataset_id": dataset_id,
                      "def_id": def_id})

    poll_entries = commit_entries_for(ctx, "05", run_id)

    def budget_at_three():
        run = poll_run_row(ctx, "05b", def_id, run_id)
        if (recovery_of(run).get("send_attempts") == 3
                and len(poll_entries() or []) == 3):
            return run
        return None

    run = wait_until(ctx, "05-budget-exhausted", budget_at_three,
                     "persistent send budget exhausted at 3 attempts")
    assert_eq(run.get("status"), "running",
              "exhausted unknown run RETAINS running (no terminal)")
    entries = poll_entries() or []
    assert_eq(len(entries), 3,
              "exactly three dispatch attempts (atomic fail-after-drop)")
    forwarded = [e for e in entries if e.get("forwarded")]
    refused = [e for e in entries if not e.get("forwarded")]
    assert_eq(len(forwarded), 1, "only send 1 ever reached the DVS")
    assert_true(forwarded[0].get("dropped")
                and forwarded[0].get("upstream_status") == 201,
                "send 1: real committed 201, ACK dropped")
    assert_eq(len(refused), 2,
              "sends 2–3 refused before forward (zero upstream)")

    source_at_intent = fixture_delta(ctx, "05c-source-after-budget", src)
    # CP not advanced while unknown: owner held
    resp = ctx.http("06-cp-held", ctx.connector, "GET",
                    f"/api/v1/data-connection/syncs/{def_id}/checkpoint")
    assert_eq(resp.status, 200, "owned incremental CP exists while unknown")
    cp_held = require_json(resp, "CP held")
    assert_eq(cp_held.get("confirmed_epoch"), 0, "unknown CP epoch remains zero")
    assert_eq((cp_held.get("owner") or {}).get("run_id"), run_id,
              "unknown CP remains owned by this run")

    # owned crash + restart; budget persists; NO automatic adoption
    ops.stop_connector()
    ops.wait_stopped()
    restart_ready(ctx, "07-restart")
    assert_true(ops.wait_lease_expired(def_id, run_id), "budget lease DB-expired")
    run = poll_run_row(ctx, "07", def_id, run_id)
    assert_eq(recovery_of(run).get("send_attempts"), 3,
              "restart does NOT reset the persistent send budget")
    observe_no_auto_adoption(ctx, "08", run_id, 3, 3)

    noauth = Client(ctx.transport, ctx.connector.base, auth=None)
    resp = recover_route(ctx, "09-noauth-401", def_id, run_id,
                         client=noauth)
    assert_eq(resp.status, 401, "missing Bearer => 401 (zero outward)")

    # manual lookup-only convergence: ONE query, NO fourth POST
    resp = recover_route(ctx, "10-manual-lookup", def_id, run_id)
    assert_true(resp.status in (200, 202),
                f"manual recovery accepted ({resp.status})")
    run = wait_run_terminal(ctx, "11", def_id, run_id, timeout_s=180)
    assert_eq(run.get("status"), "succeeded",
              "manual lookup-only recovery finalizes the truly-"
              "committed run")
    assert_eq(recovery_of(run).get("send_attempts"), 3,
              "finalize consumed NO extra send (budget still 3)")
    assert_eq(recovery_of(run).get("query_attempts"), 1,
              "exactly ONE lookup query consumed")

    entries = poll_entries() or []
    assert_eq(len(entries), 3, "still exactly three dispatch attempts")
    proof = prove_receipt_and_file(ctx, dataset_id, run_id, exp,
                                   "12-budget")
    assert_eq(proof["version_count"], 1,
              "exactly one version despite three sends")

    # CP advanced exactly once by the finalized incremental window
    resp = ctx.http("13-cp-after", ctx.connector, "GET",
                    f"/api/v1/data-connection/syncs/{def_id}/checkpoint")
    cp = resp.json() if resp.status == 200 else None
    assert_true(cp is not None and cp.get("confirmed_epoch") == 1,
                f"CP advanced exactly once on finalize: {cp}")
    assert_eq((cp.get("last_confirmed") or {}).get("run_id"), run_id,
              "last_confirmed points at the recovered run")

    source_final = fixture_delta(ctx, "13b-source-final", src)
    assert_eq(source_final["request_count"], source_at_intent["request_count"],
              "budget recovery never re-fetched Source after intent")
    assert_eq(source_final["request_count"]-source_before["request_count"], exp["pages"],
              "budget original pulled exactly expected pages")
    ctx.facts.update({
        "scenario": "budget_exhausted_then_lookup_finalize",
        "send_attempts_final": 3,
        "dispatch_attempts": 3,
        "forwarded_dispatches": 1,
        "cp_epoch_after": cp.get("confirmed_epoch"), **proof})
    return ctx.facts


# ==========================================================================
# scenario 8: A finalized → B published → recover A (head bound to B)
# ==========================================================================

def scenario_a_then_b_recover_a_head_unchanged(ctx):
    """After A finalizes and B publishes, recovering A returns the
    ACTUAL already-resolved shape: real 200, flat run fields + recovery with NO
    outcome field, the FULL safe row unchanged, and ZERO outward
    requests. The head stays bound to B's receipt Txn (the versions
    list is DESC and is NOT used as the head); CP epoch and
    last_confirmed still point at B."""
    src = "p26inc1"
    preflight(ctx)
    exp_a = fixture_expected(ctx, "01", src, 0)
    exp_b = fixture_expected(ctx, "02", src, 1)
    dataset_id, _s, def_id = create_dataset_source_def(
        ctx, "03", src, "p26-w7-aba-", incremental=True)

    run_a = start_run(ctx, "04", def_id)
    wait_run_terminal(ctx, "04b", def_id, run_a)
    proof_a = prove_receipt_and_file(ctx, dataset_id, run_a, exp_a,
                                     "05-a-proof")

    run_b = start_run(ctx, "06", def_id)
    wait_run_terminal(ctx, "06b", def_id, run_b)
    proof_b = prove_receipt_and_file(
        ctx, dataset_id, run_b,
        {**exp_b, "expected_versions": 2, "expected_files": 2},
        "07-b-proof")
    head_txn_b = proof_b["receipt"]["txn_id"]
    assert_eq(branch_head(ctx, "07b-main-head-before", dataset_id), head_txn_b,
              "actual main head BEFORE recovery is B")
    ctx.note("07b-head-bound-to-b", 0, b"head bound to B's receipt txn",
             {"head_txn": head_txn_b})

    cp_b = require_json(ctx.http("08-cp-after-b", ctx.connector, "GET",
                                 f"/api/v1/data-connection/syncs/"
                                 f"{def_id}/checkpoint"), "cp after B")
    run_a_row_before = poll_run_row(ctx, "08b", def_id, run_a)

    # recover A: already-resolved — real 200, unchanged row, zero outward
    entries_before = len(commit_entries_for(ctx, "09", run_a)() or [])
    log_before = len(poll_proxy_log(ctx, "09b"))
    queries_before = len(receipt_queries_for(ctx, "09c", run_a))
    resp = recover_route(ctx, "09-recover-a", def_id, run_a)
    assert_eq(resp.status, 200,
              "already-resolved recovery is a REAL 200 (not 409)")
    doc = resp.json() or {}
    assert_true("outcome" not in doc,
                f"recovery response has NO outcome field: {sorted(doc)}")
    returned_run = doc  # actual syncRunRecoveryResponse embeds SyncRun
    assert_eq(returned_run.get("status"), "succeeded",
              "returned run row unchanged (succeeded)")
    assert_eq(returned_run, {k: v for k, v in run_a_row_before.items() if k != "incremental"},
              "actual flat recovery response matches complete shared A fields")
    assert_eq(doc.get("recovery"), recovery_of(run_a_row_before),
              "already-resolved full safe recovery unchanged")
    assert_eq(len(commit_entries_for(ctx, "09d", run_a)() or []),
              entries_before, "recover A: ZERO new commit dispatches")
    assert_eq(len(poll_proxy_log(ctx, "09e")), log_before,
              "recover A: ZERO new proxy requests")
    assert_eq(len(receipt_queries_for(ctx, "09f", run_a)),
              queries_before, "recover A: ZERO receipt queries")

    # B's receipt txn unchanged and still the head evidence
    r = receipt_get(ctx, "10-receipt-b-unchanged", dataset_id,
                    derive_publish_key(run_b))
    assert_eq(r.status, 200, "receipt B still readable")
    assert_eq((r.json() or {}).get("transaction", {}).get("id"),
              head_txn_b, "head txn (B's receipt) unchanged")

    vlist = list_versions_all(ctx, "11-versions-after-recover-a",
                              dataset_id)
    assert_eq(len(vlist), 2, "still exactly two versions (no rewind)")
    assert_true(head_txn_b in {v.get("transaction_id")
                               or v.get("id") for v in vlist},
                "B's receipt txn present; head NOT rewound to A")

    cp_after = require_json(ctx.http("12-cp-after-recover-a",
                                     ctx.connector, "GET",
                                     f"/api/v1/data-connection/syncs/"
                                     f"{def_id}/checkpoint"), "cp after")
    assert_eq(branch_head(ctx, "12b-main-head-after", dataset_id), head_txn_b,
              "actual main head AFTER recovery is B")
    assert_eq(poll_run_row(ctx, "12c-a-after", def_id, run_a), run_a_row_before,
              "full safe A row unchanged after recover")
    assert_eq(cp_after, cp_b, "full checkpoint unchanged by recover A")
    assert_eq(cp_after.get("confirmed_epoch"),
              cp_b.get("confirmed_epoch"),
              "CP epoch NOT rewound by recovering A")
    assert_eq((cp_after.get("last_confirmed") or {}).get("run_id"), run_b,
              "last_confirmed still points at B")

    ctx.facts.update({
        "scenario": "a_then_b_recover_a_head_unchanged",
        "dataset_id": dataset_id, "run_a": run_a, "run_b": run_b,
        "head_txn_b": head_txn_b,
        "epoch_after_b": cp_b.get("confirmed_epoch"),
        "already_resolved_zero_outward": True, **proof_b})
    return ctx.facts


# ==========================================================================
# scenario 9: cross-scope / permission / legacy refusals (actual rules)
# ==========================================================================

def scenario_crossscope_permission_legacy_refusals(ctx):
    """Negative group with ACTUAL semantics: valid foreign-tenant
    credential => 404 (never a literal garbage bearer masquerading);
    valid read-only same-tenant DIFFERENT nonowner subject => 403; missing/invalid Bearer => 401.
    Dependency/transport failure: an actual unresolved crashed run first — the owned
    proxy returns an actual dependency error (502) and the run STAYS
    running ('failed' is never acceptable); the mode is one-shot, so a
    subsequent valid recovery converges. Legacy rows: typed 409 protocol refusal with the row unchanged. Error bodies never leak."""
    src, src_legacy = "p26snap1", "p26legacy"
    ops = ctx.ops
    preflight(ctx)
    exp = fixture_expected(ctx, "01", src, 0)
    dataset_id, _s, def_id = create_dataset_source_def(
        ctx, "02", src, "p26-w7-refuse-")

    _other_ds, _other_source, other_def = create_dataset_source_def(
        ctx, "06e-other-parent", src, "p26-w7-other-parent-")
    source_before = fixture_delta(ctx, "02b-source-before", src)

    arm_proxy_mode(ctx, "03", "hold_drop_once")
    run_id = start_run(ctx, "04", def_id)
    ctx.facts.update({"run_id": run_id, "dataset_id": dataset_id,
                      "def_id": def_id})
    poll_entries = commit_entries_for(ctx, "05", run_id)
    def held_committed():
        return [e for e in poll_proxy_log(ctx, "05-held")
                if e.get("held_phase") and e.get("verified_committed")
                and e.get("request_key_hash") == key_hash(derive_publish_key(run_id))
                and e.get("boot_id") == ctx.facts["proxy_boot_id"]] or None
    wait_until(ctx, "05-committed-held", held_committed, "verified committed 201 held")
    source_at_intent = fixture_delta(ctx, "05-source-after-intent", src)
    ops.stop_connector()
    resp = ctx.http("05-release-killed", ctx.proxy_admin, "POST", "/admin/barrier/release")
    assert_eq(resp.status, 200, "held ACK released only after original SIGKILL")
    wait_until(ctx, "05-original-finished", poll_entries, "original proxy operation finished")
    assert_true(ops.wait_lease_expired(def_id, run_id), "crashed run lease DB-expired")
    restart_ready(ctx, "05-restart")
    # (i) VALID foreign-tenant credential: 404 everywhere it matters
    foreign_bearer = ops.credential("foreign")
    f_client = ctx.bearer_client(foreign_bearer)
    resp = receipt_get(ctx, "06-foreign-receipt", dataset_id,
                       derive_publish_key(run_id), client=ctx.bearer_client(foreign_bearer, "dvs"))
    assert_eq(resp.status, 404,
              "VALID foreign-tenant credential sees 404 (indistinguish-"
              "able from unknown)")
    log_before = len(poll_proxy_log(ctx, "06b"))
    resp = recover_route(ctx, "06c-foreign-recover", def_id, run_id,
                         client=f_client)
    assert_eq(resp.status, 404, "foreign-tenant recovery => 404")
    assert_no_leak_markers((resp.body or b"").decode("utf-8", "replace"),
                           "foreign 404 body")
    assert_eq(len(poll_proxy_log(ctx, "06d")), log_before,
              "foreign 404 caused ZERO outward requests")

    log_before = len(poll_proxy_log(ctx, "06e-parent-baseline"))
    before_parent = poll_run_row(ctx, "06f-before-parent", def_id, run_id)
    resp = recover_route(ctx, "06g-wrong-parent", other_def, run_id)
    assert_eq(resp.status, 404, "valid writer wrong owned Def parent => 404")
    assert_eq(poll_run_row(ctx, "06h-after-parent", def_id, run_id), before_parent,
              "wrong parent refusal left full safe Run unchanged")
    assert_eq(len(poll_proxy_log(ctx, "06i")), log_before, "wrong parent zero outward")

    # (ii) missing / invalid / different-nonowner read-only — actual codes, zero outward
    noauth = Client(ctx.transport, ctx.connector.base, auth=None)
    resp = recover_route(ctx, "07-noauth-401", def_id, run_id,
                         client=noauth)
    assert_eq(resp.status, 401, "missing Bearer => 401")
    garbage = ctx.bearer_client("Bearer foreign-org-JWT")
    resp = recover_route(ctx, "07b-garbage-401", def_id, run_id,
                         client=garbage)
    assert_eq(resp.status, 401,
              "a literal garbage bearer is 401 — NOT the cross-tenant "
              "404 shape")
    ro_client = ctx.bearer_client(ops.credential("readonly"))
    resp = recover_route(ctx, "07c-readonly-403", def_id, run_id,
                         client=ro_client)
    assert_eq(resp.status, 403, "valid read-only same-tenant DIFFERENT nonowner subject => 403")
    assert_no_leak_markers((resp.body or b"").decode("utf-8", "replace"),
                           "403 body")
    assert_eq(len(poll_proxy_log(ctx, "07d")), log_before,
              "401/401/403 rejections: ZERO outward requests")

    assert_eq(poll_run_row(ctx, "07e-after-refusals", def_id, run_id), before_parent,
              "all authorization/parent refusals left whole safe Run unchanged")
    # (iii) transport/dependency 502 only; this does NOT mutate receipt fields.
    arm_proxy_mode(ctx, "08", "fail_receipt_once")
    resp = recover_route(ctx, "08b-receipt-dep-error", def_id, run_id)
    assert_eq(resp.status, 502, "recovery surfaced actual receipt dependency failure")
    run = poll_run_row(ctx, "09", def_id, run_id)
    assert_true(run.get("status") == "running",
                "run STAYS running on a receipt dependency error "
                "('failed' is never acceptable)")
    dep_errors = [e for e in poll_proxy_log(ctx, "09b")
                  if e.get("kind") == "receipt_error"]
    assert_true(dep_errors, "the owned proxy ledgered the 502 "
                            "dependency failure")

    # one-shot: a subsequent valid recovery converges (no fake cancel)
    assert_true(ops.wait_lease_expired(def_id, run_id), "dependency claim DB-expired before retry")
    resp = recover_route(ctx, "10-converge", def_id, run_id)
    assert_true(resp.status in (200, 202),
                f"valid recovery converges after the dependency error "
                f"({resp.status})")
    run = wait_run_terminal(ctx, "11", def_id, run_id, timeout_s=120)
    assert_eq(run.get("status"), "succeeded",
              "run converges to succeeded on the real committed receipt")

    source_final = fixture_delta(ctx, "11b-source-final", src)
    assert_eq(source_final["request_count"], source_at_intent["request_count"],
              "negative/dependency recovery never re-fetched Source after intent")
    assert_eq(source_final["request_count"]-source_before["request_count"], exp["pages"],
              "crossscope original pulled exactly expected pages")
    # (iv) legacy (non-P26) run recovery is typed-refused, row unchanged
    legacy_dataset, _ls, legacy_def = create_dataset_source_def(
        ctx, "12", src_legacy, "p26-w7-refuse-legacy-", recovery=False)
    legacy_run = start_run(ctx, "13", legacy_def)
    legacy_row = wait_run_terminal(ctx, "13b", legacy_def, legacy_run)
    resp = recover_route(ctx, "14-legacy-recover-refused", legacy_def,
                         legacy_run)
    assert_eq(resp.status, 409, "legacy run recovery is TYPED refused")
    body = (resp.body or b"").decode("utf-8", "replace").lower()
    assert_true("run is not a p26-v1 recovery run" in body,
                f"legacy refusal pins the actual wording: {body!r}")
    row_after = poll_run_row(ctx, "14b", legacy_def, legacy_run)
    assert_eq(row_after, legacy_row,
              "legacy row UNCHANGED by the refused recovery")

    ctx.facts.update({
        "scenario": "crossscope_permission_legacy_refusals",
        "foreign_valid_404": True, "readonly_valid_403": True,
        "missing_bearer_401": True, "dependency_error_stay_running": True,
        "legacy_refused_row_unchanged": True})
    return ctx.facts


def scenario_pregate_no_intent_owned_restart(ctx):
    """Real incremental crash before gate: source held, no durable intent.

    Same Run may re-pull its frozen baseline. No product hooks or lease
    writes; exact owned read-only DB proof precedes fixture release.
    """
    src = "p26inc1"
    preflight(ctx)
    exp = fixture_expected(ctx, "01", src, 0)
    dataset_id, _source, def_id = create_dataset_source_def(
        ctx, "02", src, "p26-w7-pregate-", incremental=True)
    before = fixture_delta(ctx, "03-before", src)
    resp = ctx.http("04-hold-source", ctx.fixture_admin, "POST",
                    f"/admin/knob/{src}", json_body={"hold_next_ms": 30000})
    assert_eq(resp.status, 200, "new fixture next-request hold armed")
    stopped = False
    try:
        run_id = start_run(ctx, "05", def_id)
        ctx.facts.update({"run_id": run_id, "def_id": def_id, "dataset_id": dataset_id})
        def active_hold():
            snap = fixture_delta(ctx, "06-active-hold", src)
            return snap if snap.get("active_hold") and snap.get("active", 0) > 0 else None
        held = wait_until(ctx, "06-held", active_hold, "real fixture active_hold reached", timeout_s=10)
        assert_eq(held["request_count"] - before["request_count"], 1, "one original held request")
        boot_before = ctx.ops.connector_boot_id()
        ctx.ops.stop_connector()
        stopped = True
        proof = ctx.ops.observe_pregate(def_id, run_id)
        ctx.facts["pregate_db_proof"] = proof
        ctx.note("07-pregate-proof", 0, b"read-only DB proof", proof)
    finally:
        primary = sys.exc_info()[0] is not None
        try:
            released = ctx.http("08-release-fixture", ctx.fixture_admin, "POST", f"/admin/release/{src}")
            assert_eq(released.status, 200, "held source released")
        except Exception as exc:
            ctx.facts["pregate_release_error"] = type(exc).__name__
            if not primary:
                raise
        finally:
            if primary and stopped:
                try:
                    restart_ready(ctx, "08-failure-restart")
                except Exception as exc:
                    ctx.facts["pregate_restart_error"] = type(exc).__name__
    wait_until(ctx, "09-fixture-quiet", lambda: (
        lambda r: r if r.status == 200 and (r.json() or {}).get("quiet") else None)(
            ctx.http("09-quiet", ctx.fixture_admin, "GET", "/admin/quiet")), "fixture drained")
    released_snapshot = fixture_delta(ctx, "09-after-release", src)
    assert_true(ctx.ops.wait_lease_expired(def_id, run_id), "pre-gate lease DB-expired")
    restart_ready(ctx, "10-restart")
    assert_true(ctx.ops.connector_boot_id() != boot_before, "same CID has new real boot")
    observe_no_auto_adoption(ctx, "11-no-auto", run_id, 0, 0)
    noauto_snapshot = fixture_delta(ctx, "11-no-auto-source", src)
    assert_eq(noauto_snapshot["request_count"], released_snapshot["request_count"],
              "restart without local auth made zero source requests")
    resumed = recover_route(ctx, "12-fresh-manual", def_id, run_id)
    assert_true(resumed.status in (200, 202), "fresh manual resumed same no-intent Run")
    run = wait_run_terminal(ctx, "13", def_id, run_id)
    assert_eq(run.get("status"), "succeeded", "same Run succeeded after baseline re-pull")
    assert_true(run.get("generation", 0) > proof["generation"], "recovery has a new fenced generation")
    assert_eq(recovery_of(run).get("send_attempts"), 1, "one prepared publish after restart")
    assert_eq(recovery_of(run).get("query_attempts"), 0, "no-intent resume never queried receipt")
    assert_eq(recovery_of(run).get("state"), "finalized", "new intent finalized")
    final = fixture_delta(ctx, "14-final", src)
    assert_eq(final["request_count"]-before["request_count"], exp["pages"]+1,
              "one lost held request plus exact recovery page count")
    cp_resp = ctx.http("15-final-cp", ctx.connector, "GET",
                       f"/api/v1/data-connection/syncs/{def_id}/checkpoint")
    assert_eq(cp_resp.status, 200, "final CP readable")
    cp = require_json(cp_resp, "pregate CP")
    assert_eq(cp.get("confirmed_epoch"), 1, "same Run advanced CP once")
    assert_true(not cp.get("owner"), "CP owner released after confirmed publish")
    assert_eq((cp.get("last_confirmed") or {}).get("run_id"), run_id, "CP points at recovered same Run")
    entries = commit_entries_for(ctx, "16", run_id)() or []
    assert_eq(len(entries), 1, "one published output")
    proof = prove_receipt_and_file(ctx, dataset_id, run_id, exp, "17-pregate")
    ctx.facts.update({"scenario": "pregate_no_intent_owned_restart", **proof})
    return ctx.facts


SCENARIOS = {
    "pregate_no_intent_owned_restart": {
        "fn": scenario_pregate_no_intent_owned_restart,
        "requires": ["fixture", "proxy_admin", "dvs", "connector", "connector_ops"],
        "summary": "real incremental active source hold -> SIGKILL -> readonly no-intent proof -> lease expiry/restart -> manual same-Run baseline re-pull, one finalize and CP+1",
    },
    "healthy_snapshot_and_legacy": {
        "fn": scenario_healthy_snapshot_and_legacy,
        "requires": ["fixture", "proxy_admin", "dvs", "connector"],
        "summary": "P26 snapshot keyed publish bytes/hash/rows == fixture; "
                   "legacy opt-out old path with no keyed receipt",
    },
    "healthy_incremental_epoch_once": {
        "fn": scenario_healthy_incremental_epoch_once,
        "requires": ["fixture", "proxy_admin", "dvs", "connector"],
        "summary": "incremental windows A→B, CP epoch +1 exactly once per "
                   "window; token digest sequence NULL→tok-0",
    },
    "dropack_manual_owned_restart_lookup": {
        "fn": scenario_dropack_manual_owned_restart_lookup,
        "credential_kinds": ("readonly",),
        "requires": ["fixture", "proxy_admin", "dvs", "connector",
                     "connector_ops"],
        "summary": "hold_drop_once crash window → owned SIGKILL/lease/"
                   "restart (stateless JWT stays valid for EXPLICIT "
                   "manual; no AUTOMATIC adoption observed) → negatives "
                   "401/401/403 zero-outward → lookup-first finalize",
    },
    "dropack_auto_retry_exact_replay": {
        "fn": scenario_dropack_auto_retry_exact_replay,
        "requires": ["fixture", "proxy_admin", "dvs", "connector"],
        "summary": "automatic bounded retry after drop: byte-identical "
                   "same-key resend → immutable 200 replay",
    },
    "inflight_receipt404_samebody_replay": {
        "fn": scenario_inflight_receipt404_samebody_replay,
        "requires": ["fixture", "proxy_admin", "dvs", "connector",
                     "connector_ops"],
        "summary": "held-before-DVS original + 404 probes → owned crash/"
                   "lease → recovery resends SAME body (201) → release "
                   "original (200 replay, discarded) → ONE version",
    },
    "twoinstances_exactly_one_claim": {
        "fn": scenario_twoinstances_exactly_one_claim,
        "requires": ["fixture", "proxy_admin", "dvs", "connector",
                     "connector_b", "connector_ops"],
        "summary": "A's real receipt GET held at the owned proxy "
                   "(hold_receipt_once) while B gets typed busy with "
                   "ZERO outward requests; A alone finalizes",
    },
    "budget_exhausted_then_lookup_finalize": {
        "fn": scenario_budget_exhausted_then_lookup_finalize,
        "requires": ["fixture", "proxy_admin", "dvs", "connector",
                     "connector_ops"],
        "summary": "incremental + atomic drop_then_fail_rest: budget 3 "
                   "persistent, unknown stays running with CP owner "
                   "held; no automatic adoption after restart; manual "
                   "lookup-only finalize with NO fourth POST",
    },
    "a_then_b_recover_a_head_unchanged": {
        "fn": scenario_a_then_b_recover_a_head_unchanged,
        "requires": ["fixture", "proxy_admin", "dvs", "connector"],
        "summary": "A finalized, B published; recover A = real 200 "
                   "already-resolved with unchanged row and ZERO "
                   "outward; head stays bound to B's receipt txn; CP "
                   "not rewound",
    },
    "crossscope_permission_legacy_refusals": {
        "fn": scenario_crossscope_permission_legacy_refusals,
        "requires": ["fixture", "proxy_admin", "dvs", "connector",
                     "connector_ops"],
        "credential_kinds": ("readonly", "foreign"),
        "summary": "valid-foreign 404 / valid-readonly 403 / missing-"
                   "Bearer 401 (all zero-outward); receipt dependency "
                   "502 keeps the run running then converges; legacy "
                   "409 protocol refusal with whole safe row unchanged",
    },
}


def run_scenario(name, ctx):
    """Run one scenario; verified cleanup runs even on failure; the
    outcome recorded truthfully (fail / fail-cleanup downgrade)."""
    entry = SCENARIOS.get(name)
    if entry is None:
        raise KeyError(f"unknown scenario {name!r}")
    started = time.monotonic()
    ctx.facts["scenario"] = name
    failed = None
    try:
        facts = entry["fn"](ctx)
        ctx.facts.update(facts)
        ctx.facts["outcome"] = "pass"
        return ctx.facts
    except ScenarioAssertionError as exc:
        failed = str(exc)
        ctx.facts["outcome"] = "fail"
        ctx.facts["assertion_error"] = failed
        raise
    finally:
        # primary failure is preserved; cleanup runs recorded+checked
        # and downgrades a would-be pass, never masks the original error
        cleanup_ok = True
        try:
            verify_cleanup(ctx)
        except Exception as exc:                     # noqa: BLE001
            cleanup_ok = False
            ctx.facts["cleanup_verified"] = False
            ctx.facts["cleanup_error"] = str(exc)
        if not cleanup_ok and failed is None:
            ctx.facts["outcome"] = "fail-cleanup"
        ctx.facts["duration_ms"] = int(
            (time.monotonic() - started) * 1000)

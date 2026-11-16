#!/usr/bin/env python3
"""OpenFoundry business-platform P1 acceptance smoke (isolated Docker env).

Covers, via plain HTTP against the isolated compose stack:

  1. dataset_isolation   — dual-org same-sub A/B, org admin, forged
                           owner/tenant headers, sub-resource mismatch,
                           no-OrgID legacy personal domain (P1.1)
  2. pipeline_isolation  — same matrix for pipelines, incl. cross-org
                           input/output datasets on run (P1.1)
  3. connector_isolation — Source/Sync/SyncRun list/get/trigger, forged
                           identity, cross-org source & target (P1.2)
  4. sync_real_rest      — paginated fixture (151 records, >=3 pages,
                           SKU leading zeros) -> Dataset NDJSON download
                           -> Pipeline filter qty>3 -> 148 rows (P1.3)
  5. hash_change         — same config, flipped source content -> new
                           content hash, byte-level re-verification
  6. sync_failures       — page-2 500, invalid JSON, pagination loop,
                           page-limit overrun, target publish failure;
                           failed runs leave no partial snapshot and a
                           prior valid version stays downloadable (P1.3)
  7. legacy_regression   — previous file-IO flow under a no-OrgID token

Design rules:
  * stdlib only; runs with `python3 -I`.
  * No fixed resource UUIDs: everything is created per run and tracked
    by the ids returned by the services.
  * Credentials are minted in memory from OF_P1_JWT_SECRET; they are
    never printed and never written to result.json.
  * The CONTRACT block below centralises request shapes that the three
    in-flight implementations own. Before running, reconcile it with
    the dataset-versioning / pipeline-build / connector-management
    READMEs (final contract source). Do not guess elsewhere.

Usage:
  python3 -I smoke.py --env-dir /private/tmp/openfoundry-platform-p1-q3v9 \
                     [--wait-only] [--only scenario ...]

Exit code 0 iff every executed scenario passed.
"""

import argparse
import base64
from datetime import datetime, timezone
import hashlib
import hmac
import json
from pathlib import Path
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, urlopen
import uuid

# ---------------------------------------------------------------- CONTRACT --
# Shapes below were read from the current tree (see README "contract
# reconciliation"); the three implementing sessions own changes to them.
# Re-verify each entry against the services' READMEs before executing.
CONTRACT = {
    # dataset-versioning-service
    "ds_create": ("POST", "/v1/datasets"),                # {"name","format"}
    "ds_list": ("GET", "/v1/datasets"),
    "ds_get": ("GET", "/v1/datasets/{id}"),
    "ds_delete": ("DELETE", "/v1/datasets/{id}"),
    "ds_upload": ("POST", "/v1/datasets/{id}/upload"),    # multipart "file"
    "ds_files": ("GET", "/v1/datasets/{id}/files"),
    "ds_file_download": ("GET", "/v1/datasets/{id}/files/{file_id}/download"),
    "ds_rows": ("GET", "/v1/datasets/{id}/files/rows?branch=main&mode=preview&limit={limit}"),
    # UploadData writes only the file index (no branch/transaction), so
    # pre-commit row reads must use the branch-less form.
    "ds_rows_nobranch": ("GET", "/v1/datasets/{id}/files/rows?mode=preview&limit={limit}"),
    "ds_output_commit": ("POST", "/v1/datasets/{id}/outputs:commit"),
    "ds_lineage": ("GET", "/v1/datasets/{id}/lineage-links"),
    "ds_file_index": ("GET", "/v1/datasets/{id}/files/index"),  # bare array
    "ds_branch_get": ("GET", "/v1/datasets/{id}/branches/{branch}"),
    "ds_txn_get": ("GET", "/v1/datasets/{id}/branches/{branch}/transactions/{txn}"),
    "ds_local_download": ("GET", "/v1/_internal/local-fs/{key}"),  # HMAC presign

    # pipeline-build-service
    "pl_create": ("POST", "/api/v1/pipelines"),
    "pl_list": ("GET", "/api/v1/pipelines"),
    "pl_get": ("GET", "/api/v1/pipelines/{id}"),
    "pl_preview": ("GET", "/api/v1/pipelines/{id}/nodes/{node}/preview?sample_size={n}"),
    "pl_run": ("POST", "/api/v1/pipelines/{id}/runs"),    # {"skip_unchanged": False}
    "pl_runs_list": ("GET", "/api/v1/pipelines/{id}/runs"),
    "pl_run_get": ("GET", "/api/v1/pipelines/{id}/runs/{run_id}"),
    "builds_list": ("GET", "/api/v1/builds"),
    "build_create": ("POST", "/api/v1/builds"),
    "build_get": ("GET", "/api/v1/builds/{id}"),
    "build_abort": ("POST", "/api/v1/builds/{id}/abort"),
    "build_jobs": ("GET", "/api/v1/builds/{id}/jobs"),
    "job_get": ("GET", "/api/v1/jobs/{id}"),
    # Build/Job forward chain (contracts read from current handlers):
    #   POST /v1/job-specs/{kind}  (handler.CreateJobSpecRequest)
    #   POST /api/v1/dry-run/resolve
    #   GET  /v1/builds/{rid}  POST /v1/builds/{rid}:abort
    #   GET  /v1/jobs/{rid}/outputs | /input-resolutions | /logs  POST /logs
    "spec_publish": ("POST", "/v1/job-specs/TRANSFORM"),
    "dryrun_resolve": ("POST", "/api/v1/dry-run/resolve"),
    "build_get_rid": ("GET", "/v1/builds/{rid}"),
    "build_abort_rid": ("POST", "/v1/builds/{rid}:abort"),
    "job_outputs": ("GET", "/v1/jobs/{rid}/outputs"),
    "job_input_res": ("GET", "/v1/jobs/{rid}/input-resolutions"),
    "job_logs_get": ("GET", "/v1/jobs/{rid}/logs"),
    "job_logs_post": ("POST", "/v1/jobs/{rid}/logs"),
    "ds_branch_create": ("POST", "/v1/datasets/{id}/branches"),
    # explicit branch: the handler defaults to "master" when absent
    "ds_schema_get": ("GET", "/v1/datasets/{id}/schema?branch={branch}"),
    "ds_transactions_list": ("GET", "/v1/datasets/{id}/transactions"),

    # connector-management-service (data-connection)
    "src_create": ("POST", "/api/v1/data-connection/sources"),
    "src_list": ("GET", "/api/v1/data-connection/sources"),
    "src_get": ("GET", "/api/v1/data-connection/sources/{id}"),
    "src_test": ("POST", "/api/v1/data-connection/sources/{id}/test-connection"),
    "sync_create": ("POST", "/api/v1/data-connection/syncs"),  # {source_id, output_dataset_id}
    "sync_get": ("GET", "/api/v1/data-connection/syncs/{id}"),
    "sync_update": ("PATCH", "/api/v1/data-connection/syncs/{id}"),  # {output_dataset_id,...}
    "sync_run": ("POST", "/api/v1/data-connection/syncs/{id}/run"),
    "sync_runs": ("GET", "/api/v1/data-connection/syncs/{id}/runs"),
    "sync_run_get": ("GET", "/api/v1/data-connection/syncs/{id}/runs/{run_id}"),

    # sync run status vocabulary (syncdomain.RunStatus)
    "sync_terminal": ("succeeded", "failed", "cancelled", "error"),
    "sync_ok": ("succeeded",),

    # Fixture REST source the connector service reaches by HOSTNAME over
    # the compose network. The final acceptance MUST exercise the Source
    # session's egress fix (config.egress survives Normalize and the
    # snapshot path reads the per-connection egress policy); the earlier
    # fixed-private-IP exploration stays in history only and must NOT be
    # used to bypass the hostname insecure-HTTP path.
    # `src` keys isolate behaviour per Source.
    "fixture_internal": "http://fixture:8090/v1/inventory",

    # rest_api Source config, aligned 1:1 with
    # services/connector-management-service/internal/models/rest_api_sync.go
    # (RESTAPISyncConfig). `src` rides query_params — NEVER the path.
    "source_config": {
        "connector_type_key": "connector_type",
        "connector_type_value": "rest_api",
        "config": {
            "domain": "fixture:8090",
            "base_url": "http://fixture:8090",
            "auth": {"type": "none"},
            "runtime": {
                "worker": "foundry",
                "timeout_ms": 10000,
                "retry_count": 0,
                "allowed_methods": ["GET"],
                "allow_private_networks": True,
            },
            "permissions": {
                "discoverable": True,
                "syncable": True,
                "invokable": False,
                "usable_in_code": False,
                "allowed_egress_hosts": ["fixture:8090"],
            },
            # Kept intentionally: the Source repair keeps this block alive
            # through Normalize and wires it into the snapshot egress
            # policy, which is exactly what the final acceptance verifies.
            "egress": {
                "allow_insecure_http": True,
                "allow_private_networks": True,
            },
            "sync": {
                "resource_path": "/v1/inventory",
                "records_path": "items",
                "pagination": {
                    "type": "page",
                    "page_param": "page",
                    "page_size_param": "page_size",
                    "page_size": 50,
                    "start_page": 1,
                },
                "limits": {
                    "max_pages": 20,
                    "max_records": 1000,
                    "max_response_bytes": 8388608,
                    "max_total_bytes": 67108864,
                    "request_timeout_ms": 10000,
                },
                "query_params": {},  # filled per source: {"src": <key>}
            },
        },
    },
}

DENY = {403, 404}          # acceptable rejection statuses for isolation asserts
FORGED_HEADERS = ("X-Org-Id", "X-Tenant-Id", "X-Organization-Id")

# Aspects that could not be exercised through the real HTTP surface (e.g.
# a build plan the API cannot resolve). These are recorded, reported, and
# counted — a run with uncovered items must never be reported as "full
# acceptance" (see README). A 400 from validation/resolution is NOT
# tenant-denial evidence and lands here instead.
UNCOVERED = []


def mark_uncovered(scenario, aspect, reason):
    UNCOVERED.append({"scenario": scenario, "aspect": aspect, "reason": reason[:400]})
    print(f"[smoke] UNCOVERED {scenario}/{aspect}: {reason[:200]}", flush=True)

# ------------------------------------------------------------------- env ----
ARGS = None
ENV = {}
BASES = {}
TOKENS = {}
SECRET_BYTES = b""


def load_env(env_dir: Path):
    global ENV, BASES, SECRET_BYTES
    env_file = env_dir / ".env"
    ENV = dict(
        line.split("=", 1) for line in env_file.read_text().splitlines() if "=" in line
    )
    for key in ("OF_P1_PG_PASSWORD", "OF_P1_JWT_SECRET", "OF_P1_SERVICE_JWT"):
        if key not in ENV:
            raise SystemExit(f"{env_file} missing {key}; run run.sh setup first")
    SECRET_BYTES = ENV["OF_P1_JWT_SECRET"].encode()
    BASES = {
        "dataset": ENV.get("OF_P1_DATASET_BASE", "http://127.0.0.1:17078"),
        "pipeline": ENV.get("OF_P1_PIPELINE_BASE", "http://127.0.0.1:17081"),
        "connector": ENV.get("OF_P1_CONNECTOR_BASE", "http://127.0.0.1:17088"),
        "fixture": ENV.get("OF_P1_FIXTURE_BASE", "http://127.0.0.1:17090"),
    }


# ---------------------------------------------------------------- tokens ----
def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def mint_token(*, sub=None, org=None, roles=None, permissions=None,
               email="user@openfoundry.local", name="User", ttl=4 * 3600):
    """HS256 JWT exactly like libs/auth-middleware Claims (org_id nullable).

    Every acceptance identity carries the dataset.write permission (both
    spellings canWriteDataset accepts) so writes are gated by tenant, not
    by a missing scope.
    """
    now = int(time.time())
    header = _b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = {
        "sub": sub or str(uuid.uuid4()),
        "jti": str(uuid.uuid4()),
        "iat": now,
        "exp": now + ttl,
        "email": email,
        "name": name,
        "roles": roles or [],
        "permissions": permissions if permissions is not None else ["dataset.write", "dataset:write"],
    }
    if org is not None:
        payload["org_id"] = org
    message = f"{header}.{_b64(json.dumps(payload).encode())}"
    sig = _b64(hmac.new(SECRET_BYTES, message.encode(), hashlib.sha256).digest())
    token = f"{message}.{sig}"
    TOKENS[token] = True  # registry for log scrubbing
    return token


class Identity:
    def __init__(self, label, *, sub=None, org=None, roles=None, email=None, name=None):
        self.label = label
        self.sub = sub or str(uuid.uuid4())
        self.org = org
        self.roles = roles or []
        self.token = mint_token(
            sub=self.sub, org=org, roles=self.roles,
            email=email or f"{label}-{self.sub[:8]}@openfoundry.local",
            name=name or label,
        )


def build_identities():
    org_a = str(uuid.uuid4())
    org_b = str(uuid.uuid4())
    # P1.1 headline: ONE user (same sub) with a token per org. Everything
    # the two tokens create must stay isolated per org.
    dual_sub = str(uuid.uuid4())
    return {
        "org_a": org_a,
        "org_b": org_b,
        "dual_sub": dual_sub,
        "user_a": Identity("dual-user", sub=dual_sub, org=org_a, name="Dual Org User"),
        "user_b": Identity("dual-user", sub=dual_sub, org=org_b, name="Dual Org User"),
        "user2_a": Identity("user2-a", org=org_a, name="Second Org A Member"),
        "admin_a": Identity("admin-a", org=org_a, roles=["admin"], name="Org A Admin"),
        "legacy": Identity("legacy", org=None, name="Legacy Personal User"),
        "legacy2": Identity("legacy2", org=None, name="Second Personal User"),
    }


def scrub(text: str) -> str:
    for token in TOKENS:
        if token in text:
            text = text.replace(token, "<redacted-jwt>")
    return text[:2000]


# ----------------------------------------------------------------- http -----
class HttpResult:
    def __init__(self, status, body):
        self.status = status
        self.body = body


def http(service, path, payload=None, *, method=None, content_type=None,
         token=None, raw=False, headers=None, form=None):
    """Request against one of BASES. Never raises on HTTP error status.

    Returns HttpResult; .body is parsed JSON (dict/list) unless raw=True.
    """
    base = BASES[service]
    data = None
    hdrs = dict(headers or {})
    if token is not None:
        hdrs["Authorization"] = "Bearer " + token
    if form is not None:
        data = form
        hdrs["Content-Type"] = content_type
    elif payload is not None:
        data = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
        hdrs["Content-Type"] = content_type or "application/json"
    req = Request(base + path, data=data, headers=hdrs, method=method or ("POST" if data is not None else "GET"))
    try:
        with urlopen(req, timeout=30) as resp:
            body = resp.read()
            status = resp.status
    except HTTPError as exc:
        body = exc.read(4000)
        status = exc.code
    except URLError as exc:
        raise ConnectionError(f"{req.get_method()} {service}{urlsplit(path).path}: {exc.reason}") from None
    if raw:
        return HttpResult(status, body)
    try:
        return HttpResult(status, json.loads(body) if body else None)
    except ValueError:
        return HttpResult(status, {"_raw": body[:400].decode(errors="replace")})


def api(service, key, *, path_args=None, token=None, payload=None, headers=None,
        form=None, content_type=None, raw=False):
    method, template = CONTRACT[key]
    url = template.format(**(path_args or {}))
    return http(service, url, payload=payload, method=method, token=token,
                headers=headers, form=form, content_type=content_type, raw=raw)


def expect(condition, message, evidence=None):
    if not condition:
        raise AssertionError(scrub(message if evidence is None else f"{message} :: {evidence}"))


def ids_everywhere(value, found=None):
    """Recursively collect uuid-shaped strings from any response envelope."""
    found = set() if found is None else found
    if isinstance(value, str):
        if len(value) == 36 and value.count("-") == 4:
            found.add(value)
    elif isinstance(value, dict):
        for item in value.values():
            ids_everywhere(item, found)
    elif isinstance(value, list):
        for item in value:
            ids_everywhere(item, found)
    return found


def wait_health(timeout=90):
    for service, base in BASES.items():
        deadline = time.monotonic() + timeout
        while True:
            try:
                res = http(service, "/healthz")
                if res.status == 200 and isinstance(res.body, dict) and res.body.get("status") == "ok":
                    break
            except ConnectionError:
                pass
            if time.monotonic() >= deadline:
                raise AssertionError(f"{service} healthz not ready after {timeout}s")
            time.sleep(1.0)


def poll(predicate, timeout, interval=1.0, describe="condition"):
    deadline = time.monotonic() + timeout
    last = None
    while True:
        last = predicate()
        if last is not None:
            return last
        if time.monotonic() >= deadline:
            raise AssertionError(f"timeout waiting for {describe}; last={scrub(str(last))}")
        time.sleep(interval)


# ------------------------------------------------------- fixture control ----
FIXTURE_INTERNAL = CONTRACT["fixture_internal"]


def fixture_admin(src, patch):
    res = http("fixture", f"/admin/src/{quote(src)}/set", patch,
               content_type="application/json")
    expect(res.status == 200 and not res.body.get("errors"),
           f"fixture admin set {src} failed", {"status": res.status, "body": scrub(str(res.body))})
    return res.body["state"]


PUBLISH_BLOCK_CONSTRAINT = "p1_smoke_block_publish"


def psql(sql, database="datasets"):
    """Run SQL against THIS isolation environment's compose Postgres only.

    The compose file/project are pinned to the acceptance environment
    (openfoundry-platform-p1-q3v9); no other environment is reachable.
    """
    import subprocess
    env_dir = Path(ARGS.env_dir)
    cmd = ["docker", "compose", "-f", str(env_dir / "compose.yaml"),
           "--project-name", "openfoundry-platform-p1-q3v9",
           "exec", "-T", "postgres", "psql", "-U", "openfoundry",
           "-d", database, "-v", "ON_ERROR_STOP=1", "-q", "-A", "-t", "-c", sql]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        raise RuntimeError(f"psql failed ({proc.returncode}): {scrub(proc.stderr)[:400]}")
    return proc.stdout.strip()


def block_target_publish(dataset_id):
    """Fail the PUBLISH stage only: a NOT VALID table CHECK that rejects
    NEW dataset_files rows for this dataset with sync-* logical paths.
    Existing rows (the prior valid version) stay untouched — NOT VALID
    skips them — so GET prechecks pass, the fetch completes, and the
    outputs:commit INSERT violates the constraint with a real 500.
    """
    target = str(uuid.UUID(dataset_id))  # parse+canonicalise: injection-safe
    psql(
        f"ALTER TABLE dataset_files DROP CONSTRAINT IF EXISTS {PUBLISH_BLOCK_CONSTRAINT}; "
        f"ALTER TABLE dataset_files ADD CONSTRAINT {PUBLISH_BLOCK_CONSTRAINT} "
        f"CHECK (NOT (dataset_id = '{target}'::uuid AND logical_path LIKE 'sync-%')) NOT VALID;"
    )


def unblock_target_publish():
    psql(f"ALTER TABLE dataset_files DROP CONSTRAINT IF EXISTS {PUBLISH_BLOCK_CONSTRAINT};")


def fixture_url(src):
    return f"{FIXTURE_INTERNAL}?src={quote(src)}"


def source_payload(name, src, *, pagination=None, limits=None):
    """Build a rest_api Source payload with the final sync contract.

    pagination/limits override CONTRACT defaults (deep-merged at the top
    level of those blocks), e.g. cursor mode or a small max_pages to make
    the page-limit overrun deterministic.
    """
    cfg = CONTRACT["source_config"]
    config = json.loads(json.dumps(cfg["config"]))  # deep copy
    config["sync"]["query_params"] = {"src": src}
    if pagination:
        config["sync"]["pagination"].update(pagination)
    if limits:
        config["sync"]["limits"].update(limits)
    return {
        "name": name,
        cfg["connector_type_key"]: cfg["connector_type_value"],
        "config": config,
    }


# ------------------------------------------------- expected inventory data --
def expected_record(index, content=1):
    """Must mirror fixtures/rest_source.py `record` exactly."""
    if content == 1:
        return {
            "sku": f"{index:06d}", "qty": index, "name": f"Widget {index:03d}",
            "unit_price_cents": 900 + index, "category": ("A", "B", "C")[index % 3],
            "is_active": index % 7 != 0, "updated_at": "2026-11-13T00:00:00Z",
        }
    if content == 2:
        return {
            "sku": f"{index:06d}", "qty": index + 1000, "name": f"Widget X {index:03d}",
            "unit_price_cents": 1900 + index, "category": ("A", "B", "C")[index % 3],
            "is_active": index % 7 == 0, "updated_at": "2026-11-13T00:00:00Z",
        }
    raise ValueError(content)


def expected_records(total=151, content=1):
    return [expected_record(i, content) for i in range(1, total + 1)]


# ------------------------------------------------------- dataset helpers ----
def create_dataset(token, name, fmt):
    res = api("dataset", "ds_create", token=token, payload={"name": name, "format": fmt})
    expect(res.status in (200, 201) and isinstance(res.body, dict) and res.body.get("id"),
           f"dataset create failed for {name}", {"status": res.status, "body": scrub(str(res.body))})
    return res.body


def upload_csv(token, dataset_id, rows):
    csv_data = ("\ufeffsku,qty\n" + "".join(f"{r['sku']},{r['qty']}\n" for r in rows)).encode()
    boundary = "OfP1Smoke" + uuid.uuid4().hex
    multipart = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="inventory.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode() + csv_data + f"\r\n--{boundary}--\r\n".encode()
    res = api("dataset", "ds_upload", path_args={"id": dataset_id}, token=token,
              form=multipart, content_type=f"multipart/form-data; boundary={boundary}")
    expect(res.status in (200, 201), f"upload to {dataset_id} failed",
           {"status": res.status, "body": scrub(str(res.body))})
    return res.body, csv_data


def list_files(token, dataset_id):
    res = api("dataset", "ds_files", path_args={"id": dataset_id}, token=token)
    expect(res.status == 200, f"files list failed for {dataset_id}",
           {"status": res.status, "body": scrub(str(res.body))})
    files = res.body.get("files") if isinstance(res.body, dict) else None
    if files is None and isinstance(res.body, list):
        files = res.body
    expect(files is not None, "files envelope unrecognized", scrub(str(res.body))[:400])
    return files


def download_file(token, dataset_id, file_id):
    return api("dataset", "ds_file_download", path_args={"id": dataset_id, "file_id": file_id},
               token=token, raw=True)


def download_ndjson(token, dataset_id, *, expected_count=1):
    """Dataset-level NDJSON download for pipeline/upload flows.

    `expected_count` pins the file-set size so taking the first file is a
    deterministic choice, not an ordering guess.
    """
    files = list_files(token, dataset_id)
    expect(len(files) == expected_count,
           f"dataset {dataset_id}: expected {expected_count} published file(s), got {len(files)}",
           [f.get("id") for f in files])
    res = download_file(token, dataset_id, files[0]["id"])
    expect(res.status == 200, f"download failed for {dataset_id}",
           {"status": res.status, "body": res.body[:200].decode(errors="replace")})
    lines = [line for line in res.body.splitlines() if line.strip()]
    rows = [json.loads(line) for line in lines]
    return rows, files[0], res.body


def download_run_artifact(token, dataset_id, run, *, branch="main"):
    """Download the artifact a sync run ACTUALLY published, bound by:

        run.dataset_version_id (== outputs:commit transaction.id)
          == file.transaction_id
        sha256(raw bytes) == run.content_hash
                            == transaction.metadata.content_hash

    Never files[0]: the file is selected by the run's own transaction.
    Returns (rows, file_meta, raw_bytes).
    """
    version_id = run.get("dataset_version_id")
    content_hash = run.get("content_hash")
    expect(version_id, "run has no dataset_version_id (transaction id)", run)
    expect(content_hash, "run has no content_hash", run)

    files = list_files(token, dataset_id)
    bound = [f for f in files if f.get("transaction_id") == version_id]
    expect(bound, f"no published file bound to run transaction {version_id}",
           [{"id": f.get("id"), "txn": f.get("transaction_id")} for f in files])
    expect(all(f.get("transaction_id") == version_id for f in bound),
           "file set mixes transactions", bound)
    file_meta = bound[0]
    res = download_file(token, dataset_id, file_meta["id"])
    expect(res.status == 200, "run artifact download failed",
           {"status": res.status, "file": file_meta.get("id")})

    digest = hashlib.sha256(res.body).hexdigest()
    expect(digest == content_hash,
           "sha256(run artifact bytes) != run.content_hash",
           {"sha256": digest, "run_hash": content_hash, "bytes": len(res.body)})

    txn = api("dataset", "ds_txn_get",
              path_args={"id": dataset_id, "branch": branch, "txn": version_id},
              token=token)
    expect(txn.status == 200, "transaction read failed",
           {"status": txn.status, "body": scrub(str(txn.body))[:300]})
    metadata = (txn.body or {}).get("metadata") or {}
    txn_hash = metadata.get("content_hash")
    expect(txn_hash == content_hash,
           "transaction.metadata.content_hash != run.content_hash",
           {"txn_hash": txn_hash, "run_hash": content_hash})

    lines = [line for line in res.body.splitlines() if line.strip()]
    rows = [json.loads(line) for line in lines]
    return rows, file_meta, res.body


def download_local(uri):
    key = urlsplit(uri).path.lstrip("/")
    expires = int(time.time()) + 600
    timestamp = datetime.fromtimestamp(expires, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    signature = hmac.new(SECRET_BYTES, f"{key}\n{timestamp}".encode(), hashlib.sha256).hexdigest()
    query = urlencode({"expires": expires, "sig": signature})
    return http("dataset", f"/v1/_internal/local-fs/{quote(key, safe='/:')}?{query}", raw=True)


def commit_uploaded_csv(token, dataset_id, upload, csv_bytes):
    """Metadata-only outputs:commit reusing an UploadData-stored object.

    UploadData only populates the file index; publishing requires an
    outputs:commit whose files entry references the uploaded storage key
    (physical_uri minus the local:/// scheme prefix), size, and logical
    path. After the commit, /files exposes real dataset_files rows.
    """
    storage_path = upload.get("physical_uri", "")
    prefix = "local:///"
    if storage_path.startswith(prefix):
        storage_path = storage_path[len(prefix):]
    body = {
        "branch": "main",
        "transaction_type": "SNAPSHOT",
        "summary": "P1 smoke metadata-only commit of uploaded CSV",
        "files": [{
            "logical_path": upload.get("path") or "inventory.csv",
            "size_bytes": upload.get("size_bytes") or len(csv_bytes),
            "storage_path": storage_path,
        }],
    }
    res = api("dataset", "ds_output_commit", path_args={"id": dataset_id},
              token=token, payload=body)
    expect(res.status in (200, 201), "metadata-only outputs:commit failed",
           {"status": res.status, "body": scrub(str(res.body))[:400]})
    return res.body


def assert_denied(res, what):
    expect(res.status in DENY, f"{what}: expected denial in {sorted(DENY)}",
           {"status": res.status, "body": scrub(str(res.body))[:400]})


def extract_items(body):
    """Best-effort array extraction from a list envelope (data/items/...).

    A key that is PRESENT but JSON null (Go nil slice) is an empty list —
    e.g. ListJobs answers {"data": null, "total": 0} when correctly
    filtered to nothing.
    """
    if isinstance(body, list):
        return body
    if isinstance(body, dict):
        for key in ("data", "items", "files", "rows", "runs", "jobs",
                    "connections", "sources", "syncs", "results"):
            if key in body:
                value = body.get(key)
                if isinstance(value, list):
                    return value
                # STRICT null shape: empty ONLY when the key is present as
                # null AND the envelope's own total confirms zero. A null
                # with total>0, or any other non-list value, is an
                # unrecognized envelope and FAILs — never a blanket
                # None -> [] coercion.
                if value is None and body.get("total") == 0:
                    return []
                return None
    return None


def assert_list_isolated(res, what, forbidden_ids=()):
    """Cross-org LIST probe.

    Tenant-filtered lists may legitimately answer 200 with an EMPTY set —
    that is a correct filter, not a leak. Accept 403/404, or 200 with an
    explicitly asserted-empty (and foreign-id-free) array. Anything else
    fails.
    """
    if res.status in DENY:
        return
    expect(res.status == 200, f"{what}: expected 403/404 or 200-empty",
           {"status": res.status, "body": scrub(str(res.body))[:400]})
    items = extract_items(res.body)
    expect(items is not None, f"{what}: unrecognized list envelope",
           scrub(str(res.body))[:400])
    expect(not items, f"{what}: 200 but list not empty", items[:5])
    leaked = ids_everywhere(res.body) & set(forbidden_ids or ())
    expect(not leaked, f"{what}: foreign ids present in envelope", leaked)


def assert_global_list_no_foreign(res, what, forbidden_ids):
    """Global /api/v1/builds isolation probe.

    The caller usually owns legitimate builds in their own tenant domain,
    so a correctly filtered global list is NOT empty. Assert: HTTP 200 and
    NONE of the foreign build UUIDs/RIDs appear anywhere in the envelope.
    (Foreign-parent child lists like /builds/{id}/jobs still use the
    stricter empty-or-403/404 rule via assert_list_isolated.)
    """
    expect(res.status == 200, f"{what}: global build list not 200",
           {"status": res.status, "body": scrub(str(res.body))[:300]})
    seen = ids_everywhere(res.body)
    leaked = seen & set(forbidden_ids or ())
    # string RIDs may not be uuid-shaped, so scan the raw text too
    text = json.dumps(res.body)
    text_leaks = [fid for fid in (forbidden_ids or []) if fid and fid in text]
    expect(not leaked and not text_leaks,
           f"{what}: foreign build ids present in own global list",
           {"leaked": sorted(leaked | set(text_leaks))})


def snapshot_dataset(token, dataset_id):
    """Full published-state snapshot: ordered file ids + per-file bytes."""
    files = list_files(token, dataset_id)
    return {
        "ids": [f.get("id") for f in files],
        "bytes": {f["id"]: download_file(token, dataset_id, f["id"]).body for f in files if f.get("id")},
    }


# -------------------------------------------------------- pipeline helpers --
def pipeline_nodes(input_id, output_id):
    return [
        {"id": "input", "label": "Dataset", "transform_type": "dataset_input",
         "input_dataset_ids": [input_id], "config": {"source_kind": "dataset", "dataset_id": input_id}},
        {"id": "filter", "label": "Quantity over 3", "transform_type": "filter",
         "depends_on": ["input"], "config": {"predicate": "qty > 3"}},
        {"id": "output", "label": "Filtered inventory", "transform_type": "output_dataset",
         "depends_on": ["filter"], "output_dataset_id": output_id, "config": {}},
    ]


def create_pipeline(token, name, input_id, output_id):
    res = api("pipeline", "pl_create", token=token, payload={
        "name": name, "pipeline_type": "FASTER",
        "nodes": pipeline_nodes(input_id, output_id),
    })
    expect(res.status in (200, 201) and res.body.get("id"), f"pipeline create failed for {name}",
           {"status": res.status, "body": scrub(str(res.body))})
    return res.body


def run_pipeline(token, pipeline_id, timeout=240):
    res = api("pipeline", "pl_run", path_args={"id": pipeline_id}, token=token,
              payload={"skip_unchanged": False})
    expect(res.status in (200, 201, 202) and res.body.get("id"),
           "pipeline run trigger failed", {"status": res.status, "body": scrub(str(res.body))})
    run_id = res.body["id"]

    def check():
        got = api("pipeline", "pl_run_get", path_args={"id": pipeline_id, "run_id": run_id}, token=token)
        if got.status == 200 and isinstance(got.body, dict) and got.body.get("status") in ("succeeded", "failed", "cancelled"):
            return got.body
        return None

    final = poll(check, timeout, describe=f"pipeline run {run_id} terminal")
    return final


# -------------------------------------------------------- connector helpers -
def create_source(token, name, src, *, pagination=None, limits=None):
    res = api("connector", "src_create", token=token,
              payload=source_payload(name, src, pagination=pagination, limits=limits))
    expect(res.status in (200, 201) and res.body.get("id"), f"source create failed for {name}",
           {"status": res.status, "body": scrub(str(res.body))[:600]})
    return res.body


def create_sync(token, source_id, output_dataset_id):
    res = api("connector", "sync_create", token=token,
              payload={"source_id": source_id, "output_dataset_id": output_dataset_id})
    return res  # caller decides: 2xx or rejection


def run_sync(token, sync_id, timeout=300):
    """Trigger and poll to terminal state; returns final run dict."""
    res = api("connector", "sync_run", path_args={"id": sync_id}, token=token)
    expect(res.status in (200, 201, 202), f"sync run trigger failed for {sync_id}",
           {"status": res.status, "body": scrub(str(res.body))[:600]})
    run = res.body if isinstance(res.body, dict) else {}
    run_id = run.get("id")

    def fetch():
        if run_id is not None and CONTRACT["sync_run_get"][1]:
            got = api("connector", "sync_run_get", path_args={"id": sync_id, "run_id": run_id}, token=token)
            if got.status == 200 and isinstance(got.body, dict):
                return got.body
        got = api("connector", "sync_runs", path_args={"id": sync_id}, token=token)
        if got.status != 200 or not isinstance(got.body, (dict, list)):
            return None
        runs = got.body if isinstance(got.body, list) else (
            got.body.get("runs") or got.body.get("items") or got.body.get("data") or [])
        for candidate in runs:
            if run_id is None or candidate.get("id") == run_id:
                return candidate
        return None

    def check():
        current = fetch()
        if current and current.get("status") in CONTRACT["sync_terminal"]:
            return current
        return None

    return poll(check, timeout, interval=1.5, describe=f"sync {sync_id} run terminal")


def sync_failure_detail(run):
    return scrub(str(run.get("error") or run.get("message") or run.get("status")))


# ===================================================================== scenarios
def scenario_dataset_isolation(ids, suffix):
    ev = {}
    name = f"p1-ds-{suffix}"
    ds_a = create_dataset(ids["user_a"].token, name, "csv")
    ds_b = create_dataset(ids["user_b"].token, name, "csv")
    ev["same_name_both_orgs"] = {"a": ds_a["id"], "b": ds_b["id"], "distinct": ds_a["id"] != ds_b["id"]}
    expect(ds_a["id"] != ds_b["id"], "A/B datasets share an id")

    # list isolation
    list_a = api("dataset", "ds_list", token=ids["user_a"].token)
    list_b = api("dataset", "ds_list", token=ids["user_b"].token)
    expect(list_a.status == 200 and list_b.status == 200, "dataset list failed")
    ev["list_a_has_a_not_b"] = ds_a["id"] in ids_everywhere(list_a.body) and ds_b["id"] not in ids_everywhere(list_a.body)
    ev["list_b_has_b_not_a"] = ds_b["id"] in ids_everywhere(list_b.body) and ds_a["id"] not in ids_everywhere(list_b.body)
    expect(ev["list_a_has_a_not_b"] and ev["list_b_has_b_not_a"], "dataset list leaks across orgs")

    # get / upload / files / rows / output-commit cross-org denied
    assert_denied(api("dataset", "ds_get", path_args={"id": ds_b["id"]}, token=ids["user_a"].token),
                  "A get B dataset")
    rows = expected_records()
    upload_b, csv_bytes = upload_csv(ids["user_b"].token, ds_b["id"], rows)
    ev["b_upload_ok"] = True
    # publish B's upload so its file set is real (cross-org list probes
    # then carry actual forbidden ids)
    commit_uploaded_csv(ids["user_b"].token, ds_b["id"], upload_b, csv_bytes)
    files_b = list_files(ids["user_b"].token, ds_b["id"])
    ev["b_files_after_commit"] = len(files_b)
    expect(len(files_b) == 1, "B commit did not expose exactly one dataset_files row",
           [f.get("id") for f in files_b])
    # same VALID multipart as the legitimate upload, so a rejection can
    # only come from the org boundary, not from a malformed request
    boundary = "OfP1SmokeDeny" + uuid.uuid4().hex
    deny_multipart = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="cross-org.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode() + csv_bytes + f"\r\n--{boundary}--\r\n".encode()
    assert_denied(api("dataset", "ds_upload", path_args={"id": ds_b["id"]}, token=ids["user_a"].token,
                      form=deny_multipart,
                      content_type=f"multipart/form-data; boundary={boundary}"),
                  "A upload into B dataset")
    assert_list_isolated(api("dataset", "ds_files", path_args={"id": ds_b["id"]}, token=ids["user_a"].token),
                         "A list B files", forbidden_ids=[f.get("id") for f in files_b])
    assert_denied(api("dataset", "ds_rows", path_args={"id": ds_b["id"], "limit": 3}, token=ids["user_a"].token),
                  "A read B rows")
    assert_denied(api("dataset", "ds_output_commit", path_args={"id": ds_b["id"]}, token=ids["user_a"].token,
                      payload={"branch": "main", "summary": "cross-org commit attempt"}),
                  "A output-commit into B dataset")

    # file-level isolation: publish A's upload the same way, take the REAL
    # dataset_files.id from /files, verify byte-exact own-org download,
    # then cross-org and parent/child mismatch denials on that concrete id.
    upload_a, csv_bytes_a = upload_csv(ids["user_a"].token, ds_a["id"], rows)
    commit_uploaded_csv(ids["user_a"].token, ds_a["id"], upload_a, csv_bytes_a)
    files_a = list_files(ids["user_a"].token, ds_a["id"])
    ev["a_files_after_commit"] = len(files_a)
    expect(len(files_a) == 1, "A commit did not expose exactly one dataset_files row",
           [f.get("id") for f in files_a])
    file_a_id = files_a[0]["id"]
    own = download_file(ids["user_a"].token, ds_a["id"], file_a_id)
    expect(own.status == 200 and own.body == csv_bytes_a,
           "A own-org download is not byte-identical to the uploaded CSV",
           {"status": own.status, "bytes": len(own.body or b""), "want": len(csv_bytes_a)})
    assert_denied(download_file(ids["user_b"].token, ds_a["id"], file_a_id),
                  "B download A file under A dataset")
    assert_denied(download_file(ids["user_a"].token, ds_b["id"], file_a_id),
                  "A download own file addressed under B dataset")
    assert_denied(download_file(ids["user_b"].token, ds_b["id"], file_a_id),
                  "B download A file under B dataset (owner mismatch)")

    # owner rows read on the published file — branch-less form (UploadData
    # writes the file index; the published snapshot has no other branch).
    # Assert directly on body.rows and verify values against the CSV head.
    rows_res = api("dataset", "ds_rows_nobranch", path_args={"id": ds_a["id"], "limit": 3},
                   token=ids["user_a"].token)
    expect(rows_res.status == 200, "owner branch-less rows read failed",
           {"status": rows_res.status, "body": scrub(str(rows_res.body))[:300]})
    rows_body = rows_res.body.get("rows") if isinstance(rows_res.body, dict) else None
    expect(isinstance(rows_body, list), "rows envelope has no .rows array",
           scrub(str(rows_res.body))[:300])
    ev["a_rows_limit3"] = len(rows_body)
    expect(len(rows_body) == 3, "branch-less rows did not return exactly 3 rows", rows_body)
    expect(rows_body == [{"sku": r["sku"], "qty": r["qty"]} for r in rows[:3]],
           "branch-less rows values differ from the uploaded CSV head", rows_body)
    forged = {h: ids["org_b"] for h in FORGED_HEADERS}
    forged_create = api("dataset", "ds_create", token=ids["user_a"].token,
                        payload={"name": f"p1-ds-forged-{suffix}", "format": "json",
                                 "organization_id": ids["org_b"], "tenant_id": ids["org_b"],
                                 "owner_id": ids["user2_a"].sub},
                        headers=forged)
    expect(forged_create.status in (200, 201), "forged create unexpectedly rejected",
           {"status": forged_create.status, "body": scrub(str(forged_create.body))[:400]})
    forged_id = forged_create.body["id"]
    list_b_after = api("dataset", "ds_list", token=ids["user_b"].token)
    ev["forged_not_in_b_list"] = forged_id not in ids_everywhere(list_b_after.body)
    expect(ev["forged_not_in_b_list"], "B list sees A-created resource despite forged tenant fields")
    assert_denied(api("dataset", "ds_get", path_args={"id": forged_id}, token=ids["user_b"].token),
                  "B get forged-tenant resource")
    assert_denied(api("dataset", "ds_get", path_args={"id": ds_a["id"]}, token=ids["user_b"].token,
                      headers={h: ids["org_a"] for h in FORGED_HEADERS}),
                  "B get A dataset with forged A headers")

    # org admin: within own org OK, cross-org denied
    got_admin = api("dataset", "ds_get", path_args={"id": ds_a["id"]}, token=ids["admin_a"].token)
    expect(got_admin.status == 200, "org admin cannot read own-org dataset",
           {"status": got_admin.status})
    assert_denied(api("dataset", "ds_get", path_args={"id": ds_b["id"]}, token=ids["admin_a"].token),
                  "org A admin reads org B dataset")
    assert_denied(api("dataset", "ds_files", path_args={"id": ds_b["id"]}, token=ids["admin_a"].token),
                  "org A admin lists org B files")

    # second member of org A shares the org domain
    expect(api("dataset", "ds_get", path_args={"id": ds_a["id"]}, token=ids["user2_a"].token).status == 200,
           "second org A member cannot read org A dataset")
    return ev


def scenario_legacy_regression(ids, suffix):
    """P0 file-IO regression under a no-OrgID token (personal domain)."""
    ev = {}
    rows = expected_records()
    ds = create_dataset(ids["legacy"].token, f"p1-legacy-{suffix}", "csv")
    upload, csv_bytes = upload_csv(ids["legacy"].token, ds["id"], rows)
    ev["uploaded_csv_rows"] = len(rows)
    presign = download_local(upload["physical_uri"])
    expect(presign.status == 200 and presign.body == csv_bytes,
           "legacy presign download round trip failed", {"status": presign.status})

    target = create_dataset(ids["legacy"].token, f"p1-legacy-out-{suffix}", "json")
    pipeline = create_pipeline(ids["legacy"].token, f"p1-legacy-pipe-{suffix}", ds["id"], target["id"])
    preview = api("pipeline", "pl_preview",
                  path_args={"id": pipeline["id"], "node": "filter", "n": 8},
                  token=ids["legacy"].token)
    expect(preview.status == 200 and preview.body.get("rows"),
           "legacy preview failed", {"status": preview.status, "body": scrub(str(preview.body))[:400]})
    ev["preview_rows"] = len(preview.body["rows"])
    final = run_pipeline(ids["legacy"].token, pipeline["id"])
    expect(final["status"] == "succeeded", "legacy pipeline run failed",
           scrub(str(final.get("node_results"))[:600]))
    out_rows, _, _ = download_ndjson(ids["legacy"].token, target["id"])
    # The CSV upload carries sku+qty only, so the expected projection is
    # sku/qty dicts — full value-by-value equality, leading zeros intact.
    expected = [{"sku": r["sku"], "qty": r["qty"]} for r in rows][3:]
    expect(out_rows == expected, "legacy NDJSON output mismatch",
           {"got": len(out_rows), "want": len(expected),
            "first_got": out_rows[0] if out_rows else None, "first_want": expected[0]})
    expect(bool(out_rows) and isinstance(out_rows[0]["sku"], str) and out_rows[0]["sku"] == "000004",
           "legacy output lost SKU leading zeros", out_rows[0] if out_rows else None)
    ev["output_rows"] = len(out_rows)
    branch = api("dataset", "ds_rows", path_args={"id": target["id"], "limit": 3}, token=ids["legacy"].token)
    expect(branch.status == 200, "legacy branch rows failed", {"status": branch.status})

    # personal domain stays personal: org users and other legacy users denied
    assert_denied(api("dataset", "ds_get", path_args={"id": ds["id"]}, token=ids["user_a"].token),
                  "org A user reads legacy personal dataset")
    assert_denied(api("dataset", "ds_get", path_args={"id": ds["id"]}, token=ids["legacy2"].token),
                  "second personal user reads legacy dataset")
    assert_denied(api("pipeline", "pl_get", path_args={"id": pipeline["id"]}, token=ids["legacy2"].token),
                  "second personal user reads legacy pipeline")
    ev["personal_domain_isolated"] = True
    return ev


def scenario_pipeline_isolation(ids, suffix):
    ev = {}
    name = f"p1-pipe-{suffix}"
    ds_a_in = create_dataset(ids["user_a"].token, f"p1-pipe-in-a-{suffix}", "csv")
    ds_a_out = create_dataset(ids["user_a"].token, f"p1-pipe-out-a-{suffix}", "json")
    ds_b_in = create_dataset(ids["user_b"].token, f"p1-pipe-in-b-{suffix}", "csv")
    ds_b_out = create_dataset(ids["user_b"].token, f"p1-pipe-out-b-{suffix}", "json")
    upload_csv(ids["user_a"].token, ds_a_in["id"], expected_records())
    upload_csv(ids["user_b"].token, ds_b_in["id"], expected_records())

    pipe_a = create_pipeline(ids["user_a"].token, name, ds_a_in["id"], ds_a_out["id"])
    pipe_b = create_pipeline(ids["user_b"].token, name, ds_b_in["id"], ds_b_out["id"])
    ev["same_name_both_orgs"] = {"a": pipe_a["id"], "b": pipe_b["id"]}

    list_a = api("pipeline", "pl_list", token=ids["user_a"].token)
    list_b = api("pipeline", "pl_list", token=ids["user_b"].token)
    expect(list_a.status == 200 and list_b.status == 200, "pipeline list failed")
    ev["list_a_isolated"] = pipe_a["id"] in ids_everywhere(list_a.body) and pipe_b["id"] not in ids_everywhere(list_a.body)
    ev["list_b_isolated"] = pipe_b["id"] in ids_everywhere(list_b.body) and pipe_a["id"] not in ids_everywhere(list_b.body)
    expect(ev["list_a_isolated"] and ev["list_b_isolated"], "pipeline list leaks across orgs")

    assert_denied(api("pipeline", "pl_get", path_args={"id": pipe_b["id"]}, token=ids["user_a"].token),
                  "A get B pipeline")
    assert_denied(api("pipeline", "pl_preview", path_args={"id": pipe_b["id"], "node": "filter", "n": 5},
                      token=ids["user_a"].token),
                  "A preview B pipeline")
    assert_denied(api("pipeline", "pl_run", path_args={"id": pipe_b["id"]}, token=ids["user_a"].token,
                      payload={"skip_unchanged": False}),
                  "A run B pipeline")
    assert_denied(api("pipeline", "pl_get", path_args={"id": pipe_b["id"]}, token=ids["admin_a"].token),
                  "org A admin reads B pipeline")
    # admin within own org allowed
    expect(api("pipeline", "pl_get", path_args={"id": pipe_a["id"]}, token=ids["admin_a"].token).status == 200,
           "org admin cannot read own-org pipeline")

    # cross-org input dataset: pipeline in A referencing B's dataset must
    # not run through. B's input dataset state is snapshotted (file ids +
    # per-file bytes) before the attempt and compared in full afterwards.
    b_in_before = snapshot_dataset(ids["user_b"].token, ds_b_in["id"])
    res = api("pipeline", "pl_create", token=ids["user_a"].token, payload={
        "name": f"p1-pipe-cross-{suffix}", "pipeline_type": "FASTER",
        "nodes": pipeline_nodes(ds_b_in["id"], ds_a_out["id"]),
    })
    if res.status in (200, 201):
        final = run_pipeline(ids["user_a"].token, res.body["id"])
        ev["cross_input_run_status"] = final["status"]
        expect(final["status"] != "succeeded", "pipeline consumed cross-org input dataset")
        b_in_after = snapshot_dataset(ids["user_b"].token, ds_b_in["id"])
        ev["cross_input_b_unchanged"] = b_in_after == b_in_before
        expect(b_in_after == b_in_before,
               "cross-org input run mutated B's input dataset",
               {"before": b_in_before["ids"], "after": b_in_after["ids"]})
    else:
        ev["cross_input_rejected_at_create"] = res.status
        expect(res.status in DENY | {400, 409, 422}, "cross-org input rejected with unexpected status",
               {"status": res.status, "body": scrub(str(res.body))[:300]})
        expect(snapshot_dataset(ids["user_b"].token, ds_b_in["id"]) == b_in_before,
               "rejected cross-org input attempt still mutated B's dataset")

    # cross-org output dataset: run must not publish into B (full snapshot)
    b_out_before = snapshot_dataset(ids["user_b"].token, ds_b_out["id"])
    res = api("pipeline", "pl_create", token=ids["user_a"].token, payload={
        "name": f"p1-pipe-cross-out-{suffix}", "pipeline_type": "FASTER",
        "nodes": pipeline_nodes(ds_a_in["id"], ds_b_out["id"]),
    })
    if res.status in (200, 201):
        final = run_pipeline(ids["user_a"].token, res.body["id"])
        ev["cross_output_run_status"] = final["status"]
        expect(final["status"] != "succeeded", "pipeline published into cross-org output dataset")
    else:
        ev["cross_output_rejected_at_create"] = res.status
        expect(res.status in DENY | {400, 409, 422}, "cross-org output rejected with unexpected status",
               {"status": res.status, "body": scrub(str(res.body))[:300]})
    b_out_after = snapshot_dataset(ids["user_b"].token, ds_b_out["id"])
    ev["b_out_files_after_denied_runs"] = len(b_out_after["ids"])
    expect(b_out_after == b_out_before,
           "cross-org output dataset received changes from denied runs",
           {"before": b_out_before["ids"], "after": b_out_after["ids"]})

    # run-level isolation: a successful run under B stays invisible to A
    run_b = run_pipeline(ids["user_b"].token, pipe_b["id"])
    expect(run_b["status"] == "succeeded", "B pipeline run failed",
           scrub(str(run_b.get("node_results"))[:400]))
    ev["b_run_ok"] = run_b["id"]
    expect(api("pipeline", "pl_run_get",
               path_args={"id": pipe_b["id"], "run_id": run_b["id"]},
               token=ids["user_b"].token).status == 200,
           "B cannot read own run")
    assert_denied(api("pipeline", "pl_run_get",
                      path_args={"id": pipe_b["id"], "run_id": run_b["id"]},
                      token=ids["user_a"].token),
                  "A reads B run by id")
    assert_list_isolated(api("pipeline", "pl_runs_list", path_args={"id": pipe_b["id"]},
                             token=ids["user_a"].token),
                         "A lists B runs", forbidden_ids=[run_b["id"]])
    assert_list_isolated(api("pipeline", "pl_runs_list", path_args={"id": pipe_b["id"]},
                             token=ids["admin_a"].token),
                         "org A admin lists B runs", forbidden_ids=[run_b["id"]])

    # builds/jobs coverage now lives in the dedicated build_positive /
    # build_isolation scenarios (spec publish is a hard precondition, so a
    # bare CreateBuild here would only ever fail on a missing spec).

    # forged headers do not widen pipeline visibility
    assert_denied(api("pipeline", "pl_get", path_args={"id": pipe_a["id"]}, token=ids["user_b"].token,
                      headers={h: ids["org_a"] for h in FORGED_HEADERS}),
                  "B get A pipeline with forged A headers")
    return ev


def scenario_connector_isolation(ids, suffix):
    ev = {}
    name = f"p1-src-{suffix}"
    src_a = create_source(ids["user_a"].token, name, f"iso-a-{suffix}")
    src_b = create_source(ids["user_b"].token, name, f"iso-b-{suffix}")
    ev["same_name_both_orgs"] = {"a": src_a["id"], "b": src_b["id"]}

    list_a = api("connector", "src_list", token=ids["user_a"].token)
    list_b = api("connector", "src_list", token=ids["user_b"].token)
    expect(list_a.status == 200 and list_b.status == 200, "source list failed")
    ev["list_a_isolated"] = src_a["id"] in ids_everywhere(list_a.body) and src_b["id"] not in ids_everywhere(list_a.body)
    ev["list_b_isolated"] = src_b["id"] in ids_everywhere(list_b.body) and src_a["id"] not in ids_everywhere(list_b.body)
    expect(ev["list_a_isolated"] and ev["list_b_isolated"], "source list leaks across orgs")

    assert_denied(api("connector", "src_get", path_args={"id": src_b["id"]}, token=ids["user_a"].token),
                  "A get B source")
    assert_denied(api("connector", "src_test", path_args={"id": src_b["id"]}, token=ids["user_a"].token,
                      payload={}),
                  "A test-connection on B source")
    assert_denied(api("connector", "src_get", path_args={"id": src_b["id"]}, token=ids["admin_a"].token),
                  "org A admin reads B source")
    expect(api("connector", "src_get", path_args={"id": src_a["id"]}, token=ids["admin_a"].token).status == 200,
           "org admin cannot read own-org source")

    # syncs: same-org binding OK; cross-org source and cross-org target rejected
    ds_a = create_dataset(ids["user_a"].token, f"p1-sync-a-{suffix}", "json")
    ds_b = create_dataset(ids["user_b"].token, f"p1-sync-b-{suffix}", "json")
    ok = create_sync(ids["user_a"].token, src_a["id"], ds_a["id"])
    expect(ok.status in (200, 201), "same-org sync create failed",
           {"status": ok.status, "body": scrub(str(ok.body))[:400]})
    ev["sync_a_id"] = ok.body.get("id")

    cross_target = create_sync(ids["user_a"].token, src_a["id"], ds_b["id"])
    if cross_target.status in (200, 201):
        final = run_sync(ids["user_a"].token, cross_target.body["id"], timeout=180)
        ev["cross_target_run_status"] = final["status"]
        expect(final["status"] not in CONTRACT["sync_ok"],
               "sync published into cross-org target dataset")
    else:
        ev["cross_target_rejected_at_create"] = cross_target.status
        expect(cross_target.status in DENY | {400, 409, 422}, "cross-org target rejected with unexpected status",
               {"status": cross_target.status, "body": scrub(str(cross_target.body))[:300]})

    cross_source = create_sync(ids["user_a"].token, src_b["id"], ds_a["id"])
    if cross_source.status in (200, 201):
        final = run_sync(ids["user_a"].token, cross_source.body["id"], timeout=180)
        ev["cross_source_run_status"] = final["status"]
        expect(final["status"] not in CONTRACT["sync_ok"], "sync ran against cross-org source")
    else:
        ev["cross_source_rejected_at_create"] = cross_source.status
        expect(cross_source.status in DENY | {400, 409, 422}, "cross-org source rejected with unexpected status",
               {"status": cross_source.status, "body": scrub(str(cross_source.body))[:300]})
    files_b = list_files(ids["user_b"].token, ds_b["id"])
    ev["b_target_files"] = len(files_b)
    expect(not files_b, "cross-org sync attempts left files on B target", files_b)

    # trigger isolation + forged headers
    assert_denied(api("connector", "sync_run", path_args={"id": ev["sync_a_id"]}, token=ids["user_b"].token),
                  "B triggers A sync")
    assert_denied(api("connector", "sync_run", path_args={"id": ev["sync_a_id"]}, token=ids["user_b"].token,
                      headers={h: ids["org_a"] for h in FORGED_HEADERS}),
                  "B triggers A sync with forged A headers")
    assert_denied(api("connector", "sync_get", path_args={"id": ev["sync_a_id"]}, token=ids["user_b"].token,
                      headers={h: ids["org_a"] for h in FORGED_HEADERS}),
                  "B get A sync with forged A headers")
    runs = api("connector", "sync_runs", path_args={"id": ev["sync_a_id"]}, token=ids["user_b"].token)
    assert_list_isolated(runs, "B lists A sync runs")

    # modify isolation: B cannot PATCH A's sync; A cannot repoint own sync
    # at a cross-org target (rejection at PATCH or, if accepted, the sync
    # definition must still target the org-A dataset).
    assert_denied(api("connector", "sync_update", path_args={"id": ev["sync_a_id"]},
                      token=ids["user_b"].token, payload={"file_glob": "*.csv"}),
                  "B patches A sync")
    repoint = api("connector", "sync_update", path_args={"id": ev["sync_a_id"]},
                  token=ids["user_a"].token, payload={"output_dataset_id": ds_b["id"]})
    ev["repoint_cross_org_target"] = {"status": repoint.status}
    if repoint.status in (200, 201):
        after = api("connector", "sync_get", path_args={"id": ev["sync_a_id"]}, token=ids["user_a"].token)
        target_now = (after.body or {}).get("output_dataset_id")
        ev["repoint_cross_org_target"]["effective_target"] = target_now
        expect(target_now != ds_b["id"],
               "A sync was repointed at a cross-org target dataset", target_now)
    else:
        expect(repoint.status in DENY | {400, 409, 422},
               "cross-org repoint rejected with unexpected status",
               {"status": repoint.status, "body": scrub(str(repoint.body))[:300]})
    return ev


def scenario_sync_real_rest(ids, suffix):
    ev = {}
    src_key = f"main-{suffix}"
    fixture_admin(src_key, {"mode": "ok", "content": 1, "total": 151, "page_size": 50})

    source = create_source(ids["user_a"].token, f"p1-rest-src-{suffix}", src_key)
    target = create_dataset(ids["user_a"].token, f"p1-rest-out-{suffix}", "json")
    created = create_sync(ids["user_a"].token, source["id"], target["id"])
    expect(created.status in (200, 201), "rest sync create failed",
           {"status": created.status, "body": scrub(str(created.body))[:400]})
    sync_id = created.body["id"]

    run = run_sync(ids["user_a"].token, sync_id)
    expect(run["status"] in CONTRACT["sync_ok"], "rest sync run not successful",
           sync_failure_detail(run))
    ev["run"] = {k: run.get(k) for k in ("id", "status", "bytes_written", "files_written", "rows_written", "content_hash", "dataset_version_id")}
    expect((run.get("bytes_written") or 0) > 0, "sync reported zero bytes")
    expect(bool(run.get("content_hash")), "sync reported no content hash")

    rows, file_meta, raw = download_run_artifact(ids["user_a"].token, target["id"], run)
    expected = expected_records(151, 1)
    ev["downloaded_rows"] = len(rows)
    ev["expected_rows"] = len(expected)
    expect(rows == expected, "NDJSON download does not match source records value-by-value",
           {"got": len(rows), "want": len(expected),
            "first_got": rows[0] if rows else None, "first_want": expected[0],
            "sku_leading_zero_kept": isinstance(rows[0].get("sku"), str) and rows[0]["sku"].startswith("0") if rows else False})
    ev["sku_leading_zero_kept"] = isinstance(rows[0]["sku"], str) and rows[0]["sku"] == "000001"
    ev["file_size_bytes"] = file_meta.get("size_bytes")
    ev["content_hash"] = run.get("content_hash")
    ev["artifact_bound"] = {"transaction_id": run.get("dataset_version_id"),
                            "file_id": file_meta.get("id"),
                            "sha256_verified": True, "txn_hash_verified": True}

    rows_preview = api("dataset", "ds_rows", path_args={"id": target["id"], "limit": 3}, token=ids["user_a"].token)
    expect(rows_preview.status == 200, "synced dataset rows read failed", {"status": rows_preview.status})

    # cursor pagination exercises the second runtime mechanism end to end
    cur_src_key = f"cursor-{suffix}"
    fixture_admin(cur_src_key, {"mode": "ok", "content": 1, "total": 151, "page_size": 40})
    cur_source = create_source(
        ids["user_a"].token, f"p1-cursor-src-{suffix}", cur_src_key,
        pagination={"type": "cursor", "cursor_param": "cursor", "next_cursor_path": "next_cursor"},
    )
    cur_target = create_dataset(ids["user_a"].token, f"p1-cursor-out-{suffix}", "json")
    cur_sync = create_sync(ids["user_a"].token, cur_source["id"], cur_target["id"])
    expect(cur_sync.status in (200, 201), "cursor sync create failed",
           {"status": cur_sync.status, "body": scrub(str(cur_sync.body))[:300]})
    cur_run = run_sync(ids["user_a"].token, cur_sync.body["id"])
    expect(cur_run["status"] in CONTRACT["sync_ok"], "cursor sync run failed", sync_failure_detail(cur_run))
    cur_rows, _, _ = download_run_artifact(ids["user_a"].token, cur_target["id"], cur_run)
    expect(cur_rows == expected, "cursor-mode NDJSON download mismatch",
           {"got": len(cur_rows), "want": len(expected)})
    ev["cursor_mode_rows"] = len(cur_rows)

    # pipeline consumes the synced dataset: filter qty>3 -> 148 rows
    out_ds = create_dataset(ids["user_a"].token, f"p1-rest-pipe-out-{suffix}", "json")
    pipeline = create_pipeline(ids["user_a"].token, f"p1-rest-pipe-{suffix}", target["id"], out_ds["id"])
    final = run_pipeline(ids["user_a"].token, pipeline["id"])
    expect(final["status"] == "succeeded", "pipeline over synced dataset failed",
           scrub(str(final.get("node_results"))[:600]))
    out_rows, _, _ = download_ndjson(ids["user_a"].token, out_ds["id"])
    expect(len(out_rows) == 148, "filtered output row count wrong", {"got": len(out_rows)})
    expect(out_rows == expected[3:], "filtered output rows mismatch")
    ev["filtered_rows"] = len(out_rows)
    ev["sync_id"] = sync_id
    ev["source_id"] = source["id"]
    ev["target_dataset_id"] = target["id"]
    return ev


def scenario_hash_change(ids, suffix, real_rest_evidence):
    ev = {}
    src_key = f"main-{suffix}"
    fixture_admin(src_key, {"content": 2})
    sync_id = real_rest_evidence["sync_id"]
    target_id = real_rest_evidence["target_dataset_id"]

    run = run_sync(ids["user_a"].token, sync_id)
    expect(run["status"] in CONTRACT["sync_ok"], "content-flip sync run failed", sync_failure_detail(run))
    new_hash = run.get("content_hash")
    expect(new_hash and new_hash != real_rest_evidence["content_hash"],
           "content hash did not change after source content changed",
           {"old": real_rest_evidence["content_hash"], "new": new_hash})

    rows, _, _ = download_run_artifact(ids["user_a"].token, target_id, run)
    expected_v2 = expected_records(151, 2)
    expect(rows == expected_v2, "flipped-content download mismatch",
           {"got": len(rows), "want": len(expected_v2)})
    ev = {"old_hash": real_rest_evidence["content_hash"], "new_hash": new_hash,
          "downloaded_rows": len(rows)}
    fixture_admin(src_key, {"content": 1})  # restore for later scenarios
    return ev


def scenario_sync_failures(ids, suffix):
    ev = {}

    # target with a prior valid version: good run first, then a failing run
    prior_src = f"prior-good-{suffix}"
    fixture_admin(prior_src, {"mode": "ok", "content": 1, "total": 151, "page_size": 50})
    good_source = create_source(ids["user_a"].token, f"p1-prior-src-{suffix}", prior_src)
    prior_target = create_dataset(ids["user_a"].token, f"p1-prior-out-{suffix}", "json")
    good_sync = create_sync(ids["user_a"].token, good_source["id"], prior_target["id"])
    expect(good_sync.status in (200, 201), "prior-good sync create failed",
           {"status": good_sync.status, "body": scrub(str(good_sync.body))[:300]})
    good_run = run_sync(ids["user_a"].token, good_sync.body["id"])
    expect(good_run["status"] in CONTRACT["sync_ok"], "prior-good run failed", sync_failure_detail(good_run))
    good_rows, good_file, good_bytes = download_run_artifact(
        ids["user_a"].token, prior_target["id"], good_run)
    expect(good_rows == expected_records(151, 1), "prior-good download mismatch")
    # full pre-failure state: run-bound file, branch file list, PUBLIC
    # files/index endpoint, and lineage links — each asserted separately.
    good_file_id = good_file["id"]
    good_files_list = [f.get("id") for f in list_files(ids["user_a"].token, prior_target["id"])]
    good_public_index = api("dataset", "ds_file_index", path_args={"id": prior_target["id"]},
                            token=ids["user_a"].token)
    expect(good_public_index.status == 200, "public files/index read failed",
           {"status": good_public_index.status})
    good_lineage = api("dataset", "ds_lineage", path_args={"id": prior_target["id"]},
                       token=ids["user_a"].token)
    expect(good_lineage.status == 200, "lineage read failed", {"status": good_lineage.status})
    good_branch = api("dataset", "ds_branch_get", path_args={"id": prior_target["id"], "branch": "main"},
                      token=ids["user_a"].token)
    expect(good_branch.status == 200, "branch read failed",
           {"status": good_branch.status, "body": scrub(str(good_branch.body))[:200]})
    ev["prior_good_rows"] = len(good_rows)

    for mode, label in (("fail_page2", "page2-500"), ("invalid_json", "invalid-json"),
                        ("loop", "pagination-loop"), ("many_pages", "page-limit-overrun")):
        src_key = f"{mode}-{suffix}"
        fixture_admin(src_key, {"mode": mode, "content": 1, "total": 151, "page_size": 50})
        # loop needs CURSOR pagination: the runtime ignores next_page in
        # page mode (it just increments), but in cursor mode the fixture's
        # cycling next_cursor makes it re-fetch the same pages until the
        # max_pages guard fires ("another cursor pending"). The overrun
        # case (page mode, 1 record/page, 151 pages) exceeds max_pages=5
        # before an empty page arrives.
        pagination = ({"type": "cursor", "cursor_param": "cursor",
                       "next_cursor_path": "next_cursor"} if mode == "loop" else None)
        limits = {"max_pages": 5} if mode in ("loop", "many_pages") else None
        source = create_source(ids["user_a"].token, f"p1-{label}-src-{suffix}", src_key,
                               pagination=pagination, limits=limits)
        fresh_target = create_dataset(ids["user_a"].token, f"p1-{label}-out-{suffix}", "json")
        created = create_sync(ids["user_a"].token, source["id"], fresh_target["id"])
        expect(created.status in (200, 201), f"{label} sync create failed",
               {"status": created.status, "body": scrub(str(created.body))[:300]})
        attempt_started = time.time()
        run = run_sync(ids["user_a"].token, created.body["id"], timeout=300)
        elapsed = round(time.time() - attempt_started, 1)
        detail = sync_failure_detail(run)
        ev[label] = {"run_status": run["status"], "error": detail[:300], "seconds": elapsed}
        expect(run["status"] not in CONTRACT["sync_ok"],
               f"{label}: run unexpectedly succeeded")
        if mode == "loop":
            # Cursor-cycle detection is a DISTINCT runtime guard; its actual
            # error reads: pagination cursor "..." repeated — detected
            # pagination loop. Requiring max_pages here would misjudge.
            expect("pagination loop" in detail or "repeated" in detail,
                   f"{label}: failure did not come from cursor-loop detection", detail)
            expect(elapsed < 120, f"{label}: guard fired too slowly", elapsed)
        elif mode == "many_pages":
            # Page-limit overrun is the max_pages guard ("exceeded
            # max_pages limit (N) without an empty page").
            expect("max_pages" in detail,
                   f"{label}: failure did not come from the max_pages guard", detail)
            expect(elapsed < 120, f"{label}: guard fired too slowly", elapsed)
        files = list_files(ids["user_a"].token, fresh_target["id"])
        ev[label]["target_files"] = len(files)
        expect(not files, f"{label}: failed run published files to fresh target", files)

    # failing run against the target that already has a valid version
    fail_src = f"prior-fail-{suffix}"
    fixture_admin(fail_src, {"mode": "fail_page2", "content": 1, "total": 151, "page_size": 50})
    fail_source = create_source(ids["user_a"].token, f"p1-prior-fail-src-{suffix}", fail_src)
    fail_sync = create_sync(ids["user_a"].token, fail_source["id"], prior_target["id"])
    expect(fail_sync.status in (200, 201), "prior-fail sync create failed",
           {"status": fail_sync.status})
    fail_run = run_sync(ids["user_a"].token, fail_sync.body["id"])
    expect(fail_run["status"] not in CONTRACT["sync_ok"], "prior-fail run unexpectedly succeeded")
    files_after = list_files(ids["user_a"].token, prior_target["id"])
    ids_after = [f.get("id") for f in files_after]
    ev["prior_version_protected"] = {
        "files_before": good_files_list, "files_after": ids_after,
        "same_file": ids_after == good_files_list,
    }
    expect(ids_after == good_files_list,
           "failed run changed the target's published file set", ids_after)
    # old version download bytes unchanged (run-bound file id)
    still = download_file(ids["user_a"].token, prior_target["id"], good_file_id)
    expect(still.status == 200 and still.body == good_bytes,
        "prior valid version no longer byte-identical after failed run")
    # PUBLIC files/index unchanged — asserted separately from branch listing
    index_after = api("dataset", "ds_file_index", path_args={"id": prior_target["id"]},
                      token=ids["user_a"].token)
    ev["prior_public_index_unchanged"] = (
        index_after.status == good_public_index.status and index_after.body == good_public_index.body
    )
    expect(ev["prior_public_index_unchanged"], "failed run mutated the public files/index")
    # lineage links unchanged
    lineage_after = api("dataset", "ds_lineage", path_args={"id": prior_target["id"]},
                        token=ids["user_a"].token)
    ev["prior_lineage_unchanged"] = (
        lineage_after.status == good_lineage.status and lineage_after.body == good_lineage.body
    )
    expect(ev["prior_lineage_unchanged"], "failed run mutated the target's lineage links")

    # empty source: a zero-record source MUST succeed cleanly — rows 0,
    # bytes 0, and a downloadable empty artifact bound to the run. A
    # failed run here is NOT acceptable.
    empty_src = f"empty-{suffix}"
    fixture_admin(empty_src, {"mode": "ok", "content": 1, "total": 0, "page_size": 50})
    empty_source = create_source(ids["user_a"].token, f"p1-empty-src-{suffix}", empty_src)
    empty_target = create_dataset(ids["user_a"].token, f"p1-empty-out-{suffix}", "json")
    empty_sync = create_sync(ids["user_a"].token, empty_source["id"], empty_target["id"])
    expect(empty_sync.status in (200, 201), "empty-source sync create failed",
           {"status": empty_sync.status})
    empty_run = run_sync(ids["user_a"].token, empty_sync.body["id"])
    expect(empty_run["status"] in CONTRACT["sync_ok"],
           "empty-source run failed (zero-record snapshots must succeed)",
           sync_failure_detail(empty_run))
    expect((empty_run.get("bytes_written") or 0) == 0,
           "empty-source run reported non-zero bytes", empty_run.get("bytes_written"))
    empty_rows, _, empty_raw = download_run_artifact(
        ids["user_a"].token, empty_target["id"], empty_run)
    expect(empty_rows == [] and empty_raw == b"",
           "empty-source artifact is not a zero-row empty file",
           {"rows": len(empty_rows), "bytes": len(empty_raw)})
    ev["empty_source_run"] = {"status": empty_run["status"], "rows": 0, "bytes": 0,
                              "empty_artifact_verified": True}

    # --- precheck negative: bind a sync, then DELETE its target dataset.
    # This evidences the run PRECHECK refusing a missing target; it is NOT
    # publish-stage evidence (the fault injection below is).
    del_target = create_dataset(ids["user_a"].token, f"p1-del-out-{suffix}", "json")
    del_sync = create_sync(ids["user_a"].token, good_source["id"], del_target["id"])
    expect(del_sync.status in (200, 201), "delete-target sync create failed",
           {"status": del_sync.status})
    deleted = api("dataset", "ds_delete", path_args={"id": del_target["id"]}, token=ids["user_a"].token)
    if deleted.status in (200, 202, 204):
        run = run_sync(ids["user_a"].token, del_sync.body["id"], timeout=180)
        ev["deleted_target_precheck_run_status"] = run["status"]
        expect(run["status"] not in CONTRACT["sync_ok"],
               "sync succeeded against a deleted target dataset")
    else:
        # dataset delete refused: exercise the non-existent target binding instead
        ev["dataset_delete_status"] = deleted.status
        ghost = create_sync(ids["user_a"].token, good_source["id"], str(uuid.uuid4()))
        if ghost.status in (200, 201):
            run = run_sync(ids["user_a"].token, ghost.body["id"], timeout=180)
            ev["ghost_target_precheck_run_status"] = run["status"]
            expect(run["status"] not in CONTRACT["sync_ok"],
                   "sync succeeded against a non-existent target dataset")
        else:
            ev["ghost_target_rejected_at_create"] = ghost.status
            expect(ghost.status in DENY | {400, 409, 422},
                   "ghost target rejected with unexpected status", {"status": ghost.status})

    # --- REAL publish-stage fault injection (P1.3 "目标发布失败").
    # A NOT VALID CHECK on THIS isolation env's datasets PG rejects only
    # NEW dataset_files rows for the prior target with sync-* logical
    # paths: GET prechecks still pass (v1 rows predate the constraint),
    # the fetch completes (content_hash recorded), and the outputs:commit
    # INSERT violates the constraint with a genuine 500. The run must
    # fail with fetch-stage hash evidence and leave NO new version,
    # unchanged branch head / files / public index / lineage, and the
    # prior version byte-identical. The constraint is always removed in
    # the finally block.
    block_target_publish(prior_target["id"])
    try:
        pub_sync = create_sync(ids["user_a"].token, good_source["id"], prior_target["id"])
        expect(pub_sync.status in (200, 201), "publish-fault sync create failed",
               {"status": pub_sync.status, "body": scrub(str(pub_sync.body))[:300]})
        pub_run = run_sync(ids["user_a"].token, pub_sync.body["id"])
        ev["publish_fault"] = {
            "run_status": pub_run["status"],
            "error": sync_failure_detail(pub_run)[:300],
            "content_hash_present": bool(pub_run.get("content_hash")),
            "dataset_version_id": pub_run.get("dataset_version_id"),
        }
        expect(pub_run["status"] not in CONTRACT["sync_ok"],
               "sync run succeeded although target publish was injected to fail")
        expect(pub_run.get("content_hash"),
               "publish-fault run recorded no content_hash — the fetch stage "
               "did not complete before the publish failure")
        expect(pub_run.get("dataset_version_id") in (None, ""),
               "publish-fault run still recorded a new dataset version",
               pub_run.get("dataset_version_id"))
        # no new valid version / unchanged branch head. Only substantive
        # fields are compared: run attempts legitimately refresh
        # last_activity_at, but head_transaction_id must not move.
        def branch_state(res):
            body = dict(res.body or {})
            body.pop("last_activity_at", None)
            return body

        branch_after = api("dataset", "ds_branch_get",
                           path_args={"id": prior_target["id"], "branch": "main"},
                           token=ids["user_a"].token)
        ev["publish_fault"]["branch_head_unchanged"] = (
            branch_after.status == good_branch.status
            and branch_state(branch_after) == branch_state(good_branch))
        expect(ev["publish_fault"]["branch_head_unchanged"],
               "publish-fault run changed the target's branch head "
               "(head_transaction_id or other substantive field)",
               {"before": branch_state(good_branch), "after": branch_state(branch_after)})
        # files set, public index, lineage, and prior bytes all unchanged
        pub_files_after = list_files(ids["user_a"].token, prior_target["id"])
        expect([f.get("id") for f in pub_files_after] == good_files_list,
               "publish-fault run changed the target's published file set")
        pub_index_after = api("dataset", "ds_file_index",
                              path_args={"id": prior_target["id"]}, token=ids["user_a"].token)
        expect(pub_index_after.status == good_public_index.status
               and pub_index_after.body == good_public_index.body,
               "publish-fault run changed the public files/index")
        pub_lineage_after = api("dataset", "ds_lineage",
                                path_args={"id": prior_target["id"]}, token=ids["user_a"].token)
        expect(pub_lineage_after.status == good_lineage.status
               and pub_lineage_after.body == good_lineage.body,
               "publish-fault run changed the target's lineage links")
        pub_still = download_file(ids["user_a"].token, prior_target["id"], good_file_id)
        expect(pub_still.status == 200 and pub_still.body == good_bytes,
               "prior valid version not byte-identical after publish-fault run")
        ev["publish_fault"]["prior_version_protected"] = True
    finally:
        unblock_target_publish()
        # sanity: the constraint is gone and a fresh good run succeeds again
        recovery_sync = create_sync(ids["user_a"].token, good_source["id"], prior_target["id"])
        expect(recovery_sync.status in (200, 201), "recovery sync create failed",
               {"status": recovery_sync.status})
        recovery_run = run_sync(ids["user_a"].token, recovery_sync.body["id"])
        ev["publish_fault"]["recovery_after_unblock"] = recovery_run["status"]
        expect(recovery_run["status"] in CONTRACT["sync_ok"],
               "good sync did not succeed after removing the test constraint",
               sync_failure_detail(recovery_run))
    return ev


def build_chain(token, suffix, tag, branch="main"):
    """Bring up the FULL forward chain for one tenant: input dataset with
    schema-ful committed upload, output dataset with an explicitly created
    branch, pipeline, PUBLISHED TRANSFORM spec. Returns a dict of handles.
    (Pipeline CRUD does not auto-publish specs; POST /v1/job-specs/TRANSFORM
    is the sanctioned publish entry.)
    """
    rows = expected_records()
    input_ds = create_dataset(token, f"p1b-in-{tag}-{suffix}", "csv")
    # explicit branch BEFORE any commit touches it (a commit against a
    # branch the dataset never had lands unreadable)
    in_branch_payload = {"name": branch}
    if branch != "main":
        # formal root creation (BranchSource.as_root) — the input's
        # feature branch must exist as a real root BEFORE the commit, or
        # the commit silently falls back to main and the schema lands
        # on the wrong branch (output_commit.go keeps a legacy fallback
        # for missing non-main branches).
        in_branch_payload["source"] = {"as_root": True}
    made_in = api("dataset", "ds_branch_create", path_args={"id": input_ds["id"]},
                  token=token, payload=in_branch_payload)
    expect(made_in.status in (200, 201, 409), f"input branch create unexpected ({tag})",
           {"status": made_in.status, "body": scrub(str(made_in.body))[:300]})
    upload, csv_bytes = upload_csv(token, input_ds["id"], rows)
    # metadata-only commit WITH a schema so resolution sees real columns
    storage_path = upload.get("physical_uri", "")
    if storage_path.startswith("local:///"):
        storage_path = storage_path[len("local:///"):]
    commit = api("dataset", "ds_output_commit", path_args={"id": input_ds["id"]},
                 token=token, payload={
                     "branch": branch,
                     "transaction_type": "SNAPSHOT",
                     "summary": f"p1 build-chain input {tag}",
                     # file_format uses the model's formal enum
                     # (PARQUET/AVRO/TEXT) — CSV payloads are TEXT.
                     "schema": {"fields": [
                         {"name": "sku", "type": "string"},
                         {"name": "qty", "type": "integer"}],
                         "file_format": "TEXT"},
                     "files": [{
                         "logical_path": upload.get("path") or "inventory.csv",
                         "size_bytes": upload.get("size_bytes") or len(csv_bytes),
                         "storage_path": storage_path,
                     }],
                 })
    expect(commit.status in (200, 201), f"build-chain input commit failed ({tag})",
           {"status": commit.status, "body": scrub(str(commit.body))[:400]})
    committed_branch = (commit.body or {}).get("branch")
    expect(committed_branch == branch,
           f"input commit fell back to another branch ({tag})",
           {"requested": branch, "committed": committed_branch})
    schema = api("dataset", "ds_schema_get",
                 path_args={"id": input_ds["id"], "branch": branch}, token=token)
    expect(schema.status == 200, f"input schema GET != 200 ({tag})",
           {"status": schema.status, "body": scrub(str(schema.body))[:300]})

    # explicit branch creation over legitimate HTTP on the output too
    # (never rely on DryRun implicitly creating it); 409 = already exists.
    output_ds = create_dataset(token, f"p1b-out-{tag}-{suffix}", "json")
    made_out = api("dataset", "ds_branch_create", path_args={"id": output_ds["id"]},
                   token=token, payload={"name": branch})
    expect(made_out.status in (200, 201, 409), f"output branch create unexpected ({tag})",
           {"status": made_out.status, "body": scrub(str(made_out.body))[:300]})

    pipeline = create_pipeline(token, f"p1b-pipe-{tag}-{suffix}", input_ds["id"], output_ds["id"])
    spec = api("pipeline", "spec_publish", token=token, payload={
        "pipeline_rid": pipeline["id"],
        "branch_name": branch,
        "inputs": [{"dataset_rid": input_ds["id"]}],
        "output_dataset_rids": [output_ds["id"]],
        "logic_payload": {"kind": "filter", "predicate": "qty > 3"},
    })
    expect(spec.status == 201 and spec.body.get("rid"),
           f"job-spec publish != 201 ({tag})",
           {"status": spec.status, "body": scrub(str(spec.body))[:400]})
    return {"input": input_ds, "output": output_ds, "pipeline": pipeline,
            "spec": spec.body, "branch": branch, "csv_bytes": csv_bytes}


def verify_build_and_abort(token, chain, build_id, org_tag, branch="main", abort_by_rid=False):
    """STRICT build lifecycle evidence, shared by the main and feature-x
    chains: non-empty jobs bound to the published canonical spec RID,
    complete job/txn set matching across abort (no all([]) false
    positives), per-transaction remote ABORTED via the dataset surface,
    idempotent abort, formal state enums, and the PG zero-lock check
    AFTER the abort (before any second build re-acquires locks). With
    abort_by_rid=True the FIRST (and idempotent retry) abort go strictly
    through the /v1/builds/{rid}:abort route — proving real RID cancel,
    not just RID idempotency; there is no UUID fallback."""
    by_uuid = api("pipeline", "build_get", path_args={"id": build_id}, token=token)
    expect(by_uuid.status == 200, f"GET build by UUID ({org_tag})", {"status": by_uuid.status})
    build_rid = by_uuid.body.get("rid")
    expect(build_rid, f"build has no RID ({org_tag})", by_uuid.body)
    by_rid = api("pipeline", "build_get_rid", path_args={"rid": build_rid}, token=token)
    expect(by_rid.status == 200, f"GET build by RID ({org_tag})", {"status": by_rid.status})

    jobs_before = extract_items(api("pipeline", "build_jobs",
                                    path_args={"id": build_id}, token=token).body) or []
    expect(jobs_before, f"build jobs empty ({org_tag})")
    jobs_before_rids = {j.get("rid") for j in jobs_before}
    job = jobs_before[0]
    expect(job.get("build_id") == build_id, f"job.build_id mismatch ({org_tag})", job.get("build_id"))
    expect(job.get("job_spec_rid") == chain["spec"]["rid"],
           f"job not bound to the PUBLISHED canonical spec RID ({org_tag})",
           {"job_spec_rid": job.get("job_spec_rid"), "published": chain["spec"]["rid"]})
    job_rid = job.get("rid")
    expect(job_rid and api("pipeline", "job_get", path_args={"id": job_rid},
                           token=token).status == 200,
           f"GET job by RID ({org_tag})")

    files_before_abort = [f.get("id") for f in list_files(token, chain["output"]["id"])]
    outs_before = api("pipeline", "job_outputs", path_args={"rid": job_rid}, token=token)
    expect(outs_before.status == 200, f"job outputs ({org_tag})", {"status": outs_before.status})
    txn_rids = {row.get("transaction_rid") for row in (outs_before.body or {}).get("outputs") or []}
    expect(txn_rids and all(txn_rids), f"no output transactions pre-abort ({org_tag})")

    if abort_by_rid:
        abort1 = api("pipeline", "build_abort_rid", path_args={"rid": build_rid}, token=token)
        expect(abort1.status == 200, f"abort #1 via RID != 200 ({org_tag})",
               {"status": abort1.status, "body": scrub(str(abort1.body))[:300]})
    else:
        abort1 = api("pipeline", "build_abort", path_args={"id": build_id}, token=token)
        expect(abort1.status == 200, f"abort #1 != 200 ({org_tag})",
               {"status": abort1.status, "body": scrub(str(abort1.body))[:300]})
    final_build = api("pipeline", "build_get", path_args={"id": build_id}, token=token)
    expect(final_build.status == 200 and final_build.body,
           f"GET build after abort not 200/non-empty ({org_tag})", {"status": final_build.status})
    build_state = str(final_build.body.get("state", "")).upper()
    expect(build_state in ("BUILD_ABORTING", "BUILD_ABORTED"),
           f"build state not BUILD_ABORTING/BUILD_ABORTED ({org_tag})", build_state)

    jobs_after = extract_items(api("pipeline", "build_jobs", path_args={"id": build_id},
                                   token=token).body) or []
    expect(jobs_after, f"jobs after abort empty ({org_tag})")
    expect({j.get("rid") for j in jobs_after} == jobs_before_rids,
           f"job set changed across abort ({org_tag})")
    expect(all(str(j.get("state", "")).upper() in ("ABORT_PENDING", "ABORTED") for j in jobs_after),
           f"job states not ABORT_PENDING/ABORTED ({org_tag})", [j.get("state") for j in jobs_after])

    outs_after = api("pipeline", "job_outputs", path_args={"rid": job_rid}, token=token)
    rows_after = (outs_after.body or {}).get("outputs") or []
    expect(rows_after and {r.get("transaction_rid") for r in rows_after} == txn_rids,
           f"output txn set changed across abort ({org_tag})")
    expect(all(row.get("aborted") for row in rows_after),
           f"job-outputs rows not flagged aborted ({org_tag})", rows_after)
    for rid in txn_rids:
        txn_tail = str(rid).rsplit(".", 1)[-1]
        remote = api("dataset", "ds_txn_get",
                     path_args={"id": chain["output"]["id"], "branch": branch, "txn": txn_tail},
                     token=token)
        status_value = str(((remote.body or {}).get("status", "")) or "").upper()
        expect(remote.status == 200 and status_value == "ABORTED",
               f"remote output txn not ABORTED on dataset surface ({org_tag})",
               {"txn": txn_tail, "http": remote.status, "status": status_value})

    abort2_route = "build_abort_rid" if abort_by_rid else "build_abort"
    abort2_args = {"rid": build_rid} if abort_by_rid else {"id": build_id}
    abort2 = api("pipeline", abort2_route, path_args=abort2_args, token=token)
    expect(abort2.status == 200, f"idempotent abort != 200 ({org_tag})",
           {"status": abort2.status})

    files_after_abort = [f.get("id") for f in list_files(token, chain["output"]["id"])]
    expect(files_after_abort == files_before_abort,
           f"abort changed the output's published file set ({org_tag})",
           {"before": files_before_abort, "after": files_after_abort})

    # PG zero-lock proof: AFTER this build's abort, BEFORE any re-create
    residual = psql(
        f"SELECT count(*) FROM build_input_locks WHERE output_dataset_rid = "
        f"'{chain['output']['id']}' OR output_dataset_rid LIKE "
        f"'%.{chain['output']['id']}';", database="pipelines")
    expect(residual == "0", f"residual build_input_locks after abort ({org_tag})", residual)
    return {"build_rid": build_rid, "job_rid": job_rid,
            "remote_txns": len(txn_rids), "locks_after_abort": residual}


def snapshot_build_side_effects(token, output_id, branch="main"):
    """Full no-side-effect snapshot for a tenant domain. Every GET here
    MUST answer 200 — a failed read is a FAIL, never a silent None/empty
    that would fake an 'unchanged' comparison."""
    files = list_files(token, output_id)
    index = api("dataset", "ds_file_index", path_args={"id": output_id}, token=token)
    expect(index.status == 200, "files/index GET failed in snapshot",
           {"status": index.status})
    lineage = api("dataset", "ds_lineage", path_args={"id": output_id}, token=token)
    expect(lineage.status == 200, "lineage GET failed in snapshot",
           {"status": lineage.status})
    builds = api("pipeline", "builds_list", token=token)
    expect(builds.status == 200, "builds list failed in snapshot", {"status": builds.status})
    txns = api("dataset", "ds_transactions_list", path_args={"id": output_id}, token=token)
    expect(txns.status == 200, "transactions list failed in snapshot",
           {"status": txns.status})
    branch_head = api("dataset", "ds_branch_get",
                      path_args={"id": output_id, "branch": branch}, token=token)
    expect(branch_head.status == 200, "branch head GET failed in snapshot",
           {"status": branch_head.status})
    head_view = dict(branch_head.body or {})
    head_view.pop("last_activity_at", None)  # activity timestamp, not state
    open_txns = sorted({
        row.get("rid") or row.get("id")
        for row in (extract_items(txns.body) or [])
        if str((row.get("status") or "")).upper() == "OPEN"})
    return {"file_ids": [f.get("id") for f in files],
            "index": index.body,
            "lineage": lineage.body,
            "builds_ids": sorted(ids_everywhere(builds.body)),
            "open_txn_ids": open_txns,
            "branch_head": head_view,
            "file_bytes": {f["id"]: download_file(token, output_id, f["id"]).body
                           for f in files if f.get("id")}}


def scenario_build_positive(ids, suffix):
    """P1.1 forward Build/Job chain, same-sub dual org, fully over HTTP.

    For org A AND org B (same subject): schema-ful input, explicit output
    branch, published spec; DryRun succeeds WITHOUT creating builds/locks;
    CreateBuild 202 with jobs bound to the published spec RID and output
    transactions readable via the dataset surface; owner abort transitions
    build/jobs/remote transactions to ABORTED, is idempotent, and the same
    output accepts a fresh CreateBuild afterwards (locks and OPEN txns
    cleaned). Also exercises a non-main branch build + cancel.
    """
    ev = {}
    chains = {}
    ev_keys = {"a": "org_a", "b": "org_b", "legacy": "legacy"}
    identities = (("a", ids["user_a"]), ("b", ids["user_b"]),
                  ("legacy", ids["legacy"]))  # no-Org personal domain (P1.5)
    for org_tag, identity in identities:
        chain = build_chain(identity.token, suffix, org_tag)
        chains[org_tag] = chain

        # DryRun must succeed with a REAL resolution and leave NOTHING
        # behind: no builds, no jobs, no OPEN transactions, no locks —
        # proven by the strict seven-dimension snapshot plus PG row counts
        # (this stack runs no parallel workloads, so global counts are a
        # faithful baseline).
        dry_before_snap = snapshot_build_side_effects(identity.token, chain["output"]["id"])
        dry_before_pg = psql(
            "SELECT (SELECT count(*) FROM jobs), "
            "(SELECT count(*) FROM build_input_locks), "
            "(SELECT count(*) FROM build_pending_output_txns);",
            database="pipelines")
        dry = api("pipeline", "dryrun_resolve", token=identity.token, payload={
            "pipeline_rid": chain["pipeline"]["id"],
            "build_branch": "main",
            "output_dataset_rids": [chain["output"]["id"]],
        })
        expect(dry.status == 200, f"dry-run resolve HTTP {dry.status} ({org_tag})",
               {"status": dry.status, "body": scrub(str(dry.body))[:400]})
        dry_errors = (dry.body or {}).get("errors") or []
        expect(not dry_errors, f"dry-run reported errors ({org_tag})",
               scrub(str(dry_errors))[:400])
        dry_jobs = (dry.body or {}).get("jobs") or []
        expect(dry_jobs, f"dry-run resolved no jobs ({org_tag})", dry.body)
        dry_job = dry_jobs[0]
        expect(dry_job.get("job_spec_rid") == chain["spec"]["rid"],
               f"dry-run job not bound to the canonical published spec RID ({org_tag})",
               {"resolved": dry_job.get("job_spec_rid"), "published": chain["spec"]["rid"]})
        expect(dry_job.get("resolved_inputs"), f"dry-run resolved no inputs ({org_tag})", dry_job)
        expect(dry_job.get("resolved_outputs"), f"dry-run resolved no outputs ({org_tag})", dry_job)
        dry_after_snap = snapshot_build_side_effects(identity.token, chain["output"]["id"])
        dry_after_pg = psql(
            "SELECT (SELECT count(*) FROM jobs), "
            "(SELECT count(*) FROM build_input_locks), "
            "(SELECT count(*) FROM build_pending_output_txns);",
            database="pipelines")
        expect(dry_after_snap == dry_before_snap,
               f"dry-run changed the output snapshot ({org_tag})",
               {"files": [dry_before_snap["file_ids"], dry_after_snap["file_ids"]],
                "open_txns": [dry_before_snap["open_txn_ids"], dry_after_snap["open_txn_ids"]],
                "branch_head_moved": dry_after_snap["branch_head"] != dry_before_snap["branch_head"]})
        expect(dry_after_pg == dry_before_pg,
               f"dry-run changed PG jobs/locks/pending-txn counts ({org_tag})",
               {"before": dry_before_pg, "after": dry_after_pg})

        # CreateBuild: 202 with jobs, then the FULL strict lifecycle
        made = api("pipeline", "build_create", token=identity.token, payload={
            "pipeline_rid": chain["pipeline"]["id"],
            "build_branch": "main",
            "output_dataset_rids": [chain["output"]["id"]],
        })
        expect(made.status == 202, f"CreateBuild != 202 ({org_tag})",
               {"status": made.status, "body": scrub(str(made.body))[:400]})
        expect((made.body.get("job_count") or 0) >= 1, f"job_count < 1 ({org_tag})", made.body)
        build_id = made.body["build_id"]
        state = (made.body.get("state") or "").upper()
        expect(state in ("WAITING", "BUILD_RESOLUTION", "QUEUED", "RESOLVING", "RUNNING"),
               f"unexpected build state ({org_tag})", state)

        lifecycle = verify_build_and_abort(identity.token, chain, build_id, org_tag)
        ev[ev_keys[org_tag]] = {
            "build_id": build_id, "build_rid": lifecycle["build_rid"],
            "job_rid": lifecycle["job_rid"], "spec_rid": chain["spec"]["rid"],
            "job_spec_bound": True, "abort_idempotent": True,
            "remote_txns_aborted": lifecycle["remote_txns"],
            "input_locks_after_abort": lifecycle["locks_after_abort"],
            "pipeline_id": chain["pipeline"]["id"],
            "input_id": chain["input"]["id"], "output_id": chain["output"]["id"],
        }

        # same output accepts a fresh build — the HTTP-level lock/OPEN-txn
        # cleanup evidence (the PG zero-lock proof already ran inside the
        # lifecycle helper, BEFORE this second build re-acquires locks)
        again = api("pipeline", "build_create", token=identity.token, payload={
            "pipeline_rid": chain["pipeline"]["id"],
            "build_branch": "main",
            "output_dataset_rids": [chain["output"]["id"]],
        })
        expect(again.status == 202 and (again.body.get("job_count") or 0) >= 1,
               f"re-CreateBuild on same output failed ({org_tag})",
               {"status": again.status, "body": scrub(str(again.body))[:300]})
        ev[ev_keys[org_tag]]["rebuild_ok"] = True
        # cleanup of the second build MUST succeed (asserted, not fire-and-forget)
        cleanup = api("pipeline", "build_abort", path_args={"id": again.body["build_id"]},
                      token=identity.token)
        expect(cleanup.status == 200, f"second-build cleanup abort failed ({org_tag})",
               {"status": cleanup.status, "body": scrub(str(cleanup.body))[:300]})

        # non-main branch: same STRICT lifecycle (jobs + canonical binding +
        # remote txn aborts + zero locks), never a 202/GET/abort empty shell
        branch_chain = build_chain(identity.token, suffix, f"{org_tag}-feat", branch="feature-x")
        made_b = api("pipeline", "build_create", token=identity.token, payload={
            "pipeline_rid": branch_chain["pipeline"]["id"],
            "build_branch": "feature-x",
            "output_dataset_rids": [branch_chain["output"]["id"]],
        })
        expect(made_b.status == 202, f"feature-branch CreateBuild != 202 ({org_tag})",
               {"status": made_b.status, "body": scrub(str(made_b.body))[:300]})
        feat = verify_build_and_abort(identity.token, branch_chain, made_b.body["build_id"],
                                      f"{org_tag}-feat", branch="feature-x",
                                      abort_by_rid=True)
        expect(feat["build_rid"], f"feature build has no RID ({org_tag})")
        ev[ev_keys[org_tag]]["feature_branch_cancelled"] = True

    spec_rids = {tag: chains[tag]["spec"]["rid"] for tag, _ in identities}
    expect(len(set(spec_rids.values())) == len(spec_rids),
           "published spec RIDs collide across tenants", spec_rids)
    ev["dual_org_distinct"] = True
    return ev


def scenario_build_isolation(ids, suffix, positive_evidence):
    """Cross-tenant denials on the now-LEGITIMATE Build/Job surface.

    Preconditions are provably valid: the same-org positive chain
    (build_positive) already returned 201/202 on these exact resources, so
    a foreign rejection can only come from the tenant boundary — never
    from schema-404 or missing-spec-400. Entry points (publish / dry-run /
    create-build) are probed with A's legitimate pipeline+input+output,
    and A's data/build surface is snapshotted before/after to prove the
    rejections left no side effects.
    """
    if not positive_evidence or "org_a" not in (positive_evidence or {}):
        raise AssertionError("build_positive evidence missing (run it first)")
    a = positive_evidence["org_a"]
    b = positive_evidence["org_b"]
    leg = positive_evidence.get("legacy")
    if not leg:
        raise AssertionError("build_positive legacy (personal-domain) evidence missing")
    token_a = ids["user_a"].token
    token_b = ids["user_b"].token
    token_leg = ids["legacy"].token
    ev = {}

    # read/modify/abort by UUID and by RID; outputs, resolutions, logs.
    # B -> A resources, AND admin-of-A -> B resources (cross-tenant admin).
    probes_b_on_a = [
        ("build_get", {"id": a["build_id"]}, None),
        ("build_get_rid", {"rid": a["build_rid"]}, None),
        ("build_abort", {"id": a["build_id"]}, {}),
        ("build_abort_rid", {"rid": a["build_rid"]}, {}),
        ("job_get", {"id": a["job_rid"]}, None),
        ("job_outputs", {"rid": a["job_rid"]}, None),
        ("job_input_res", {"rid": a["job_rid"]}, None),
    ]
    for key, path_args, payload in probes_b_on_a:
        assert_denied(api("pipeline", key, path_args=path_args, token=token_b, payload=payload),
                      f"B {key} on A build/job surface")
    assert_list_isolated(api("pipeline", "job_logs_get", path_args={"rid": a["job_rid"]},
                             token=token_b),
                         "B reads A job logs")
    probes_admin_on_b = [
        ("build_get", {"id": b["build_id"]}, None),
        ("build_get_rid", {"rid": b["build_rid"]}, None),
        ("build_abort", {"id": b["build_id"]}, {}),
        ("build_abort_rid", {"rid": b["build_rid"]}, {}),
        ("job_get", {"id": b["job_rid"]}, None),
        ("job_outputs", {"rid": b["job_rid"]}, None),
        ("job_input_res", {"rid": b["job_rid"]}, None),
        ("job_logs_post", {"rid": b["job_rid"]}, {"level": "INFO", "message": "admin-cross"}),
    ]
    for key, path_args, payload in probes_admin_on_b:
        assert_denied(api("pipeline", key, path_args=path_args,
                          token=ids["admin_a"].token, payload=payload),
                      f"org A admin {key} on B build/job surface")
    assert_list_isolated(api("pipeline", "job_logs_get", path_args={"rid": b["job_rid"]},
                             token=ids["admin_a"].token),
                         "org A admin reads B job logs")
    assert_list_isolated(api("pipeline", "build_jobs", path_args={"id": a["build_id"]},
                             token=token_b),
                         "B lists A build jobs", forbidden_ids=[a["job_rid"]])

    # personal domain (P1.5): the no-Org owner's REAL build/job must be
    # denied to another personal subject (legacy2) AND to the org-A user,
    # by UUID and by RID, including abort/outputs/resolutions/log write.
    for who, who_token in (("legacy2", ids["legacy2"].token), ("orgA-user", token_a)):
        for key, path_args, payload in [
                ("build_get", {"id": leg["build_id"]}, None),
                ("build_get_rid", {"rid": leg["build_rid"]}, None),
                ("build_abort", {"id": leg["build_id"]}, {}),
                ("build_abort_rid", {"rid": leg["build_rid"]}, {}),
                ("job_get", {"id": leg["job_rid"]}, None),
                ("job_outputs", {"rid": leg["job_rid"]}, None),
                ("job_input_res", {"rid": leg["job_rid"]}, None),
                ("job_logs_post", {"rid": leg["job_rid"]},
                 {"level": "INFO", "message": f"foreign-{who}"})]:
            assert_denied(api("pipeline", key, path_args=path_args,
                              token=who_token, payload=payload),
                          f"{who} {key} on personal-domain build/job")
        assert_list_isolated(api("pipeline", "job_logs_get",
                                 path_args={"rid": leg["job_rid"]}, token=who_token),
                             f"{who} reads personal-domain job logs")
    for who, who_token in (("legacy2", ids["legacy2"].token), ("orgA-user", token_a)):
        assert_list_isolated(api("pipeline", "build_jobs", path_args={"id": leg["build_id"]},
                                 token=who_token),
                             f"{who} lists personal-domain build jobs",
                             forbidden_ids=[leg["job_rid"]])
        assert_global_list_no_foreign(api("pipeline", "builds_list", token=who_token),
                                       f"{who} global build list vs personal domain",
                                       forbidden_ids=[leg["build_id"], leg["build_rid"]])
    ev["personal_domain_isolated"] = True

    # lists may be 200-empty but must not contain A ids/markers
    builds_b = api("pipeline", "builds_list", token=token_b)
    assert_global_list_no_foreign(builds_b, "B global build list",
                                  forbidden_ids=[a["build_id"], a["build_rid"],
                                                 leg["build_id"], leg["build_rid"]])

    # same-org admin forward access is fine
    expect(api("pipeline", "build_get", path_args={"id": a["build_id"]},
               token=ids["admin_a"].token).status == 200,
           "same-org admin cannot read build")

    # forged headers/body identity do not widen access
    assert_denied(api("pipeline", "build_get", path_args={"id": a["build_id"]}, token=token_b,
                      headers={h: ids["org_a"] for h in FORGED_HEADERS}),
                  "B reads A build with forged A headers")
    assert_denied(api("pipeline", "build_create", token=token_b, headers={h: ids["org_a"] for h in FORGED_HEADERS},
                      payload={"pipeline_rid": a["pipeline_id"], "build_branch": "main",
                               "output_dataset_rids": [a["output_id"]],
                               "owner_id": ids["user_a"].sub, "tenant_id": ids["org_a"]}),
                  "B creates build on A pipeline with forged identity")

    # job logs: foreign tokens can neither read nor write (owner markers
    # were proven for all three domains at the top of this scenario)
    assert_denied(api("pipeline", "job_logs_post", path_args={"rid": a["job_rid"]},
                      token=token_b, payload={"level": "INFO", "message": "foreign-write"}),
                  "B writes into A job logs")
    assert_list_isolated(api("pipeline", "job_logs_get",
                              path_args={"rid": a["job_rid"]}, token=token_b),
                         "B reads A job logs")

    # entry-point denials, provably-valid targets, MIXED-resource variants.
    # Full-foreign (all A resources) only proves the pipeline guard; the
    # mixed variants prove the DATASET boundary itself: B's own pipeline
    # with B's own output but a FOREIGN input, and B's own pipeline+input
    # with a FOREIGN output.
    #
    # Body-shape validity is proven FIRST (a build on B's own fully-own
    # chain, immediately aborted) so the later snapshots are stable.
    def publish_body(pipeline, input_id, output_id):
        return {"pipeline_rid": pipeline, "branch_name": "main",
                "inputs": [{"dataset_rid": input_id}],
                "output_dataset_rids": [output_id],
                "logic_payload": {"kind": "filter", "predicate": "qty > 3"}}

    def build_body(pipeline, output_id):
        return {"pipeline_rid": pipeline, "build_branch": "main",
                "output_dataset_rids": [output_id]}

    own_create = api("pipeline", "build_create", token=token_b,
                     payload=build_body(b["pipeline_id"], b["output_id"]))
    expect(own_create.status == 202, "B cannot create a build on its OWN chain "
           "(so the mixed-variant rejections cannot be blamed on the body)",
           {"status": own_create.status, "body": scrub(str(own_create.body))[:300]})
    own_abort = api("pipeline", "build_abort", path_args={"id": own_create.body["build_id"]},
                    token=token_b)
    expect(own_abort.status == 200, "B own-chain cleanup abort failed",
           {"status": own_abort.status})

    before_a = snapshot_build_side_effects(token_a, a["output_id"])
    before_b = snapshot_build_side_effects(token_b, b["output_id"])
    before_leg = snapshot_build_side_effects(token_leg, leg["output_id"])

    # Entry-point rejections by variant. CreateBuildRequest carries only
    # pipeline_rid/build_branch/output_dataset_rids — an input is NEVER in
    # the Create body, so a "foreign-input Create" cannot be expressed
    # over HTTP without faking a same-body 403. Coverage per review:
    #   full-foreign / foreign-output -> Publish + DryRun + Create (all
    #     three carry the foreign reference explicitly)
    #   foreign-input -> Publish (inputs array) + DryRun with an inline
    #     spec whose inputs reference the foreign dataset (the formal
    #     inline_specs contract); Create via the PG legacy-row special
    #     case below.
    for label, pipe, input_id, output_id in (
            ("full-foreign", a["pipeline_id"], a["input_id"], a["output_id"]),
            ("own-pipe-foreign-out", b["pipeline_id"], b["input_id"], a["output_id"])):
        assert_denied(api("pipeline", "spec_publish", token=token_b,
                          payload=publish_body(pipe, input_id, output_id)),
                      f"B publish spec variant {label}")
        assert_denied(api("pipeline", "dryrun_resolve", token=token_b, payload={
            "pipeline_rid": pipe, "build_branch": "main",
            "output_dataset_rids": [output_id]}),
                      f"B dry-run variant {label}")
        assert_denied(api("pipeline", "build_create", token=token_b,
                          payload=build_body(pipe, output_id)),
                      f"B create-build variant {label}")

    # foreign-input: Publish rejects the explicit inputs array...
    assert_denied(api("pipeline", "spec_publish", token=token_b,
                      payload=publish_body(b["pipeline_id"], a["input_id"], b["output_id"])),
                  "B publish spec with foreign input")
    # ...and DryRun rejects via the FORMAL inline_specs contract carrying
    # the foreign input (models.JobSpec shape).
    inline = [{
        "rid": f"ri.foundry.main.jobspec.{uuid.uuid4()}",
        "pipeline_rid": b["pipeline_id"], "branch_name": "main",
        "inputs": [{"dataset_rid": a["input_id"]}],
        "output_dataset_rids": [b["output_id"]],
        "logic_kind": "TRANSFORM",
        "logic_payload": {"kind": "filter", "predicate": "qty > 3"},
        "content_hash": uuid.uuid4().hex,
    }]
    assert_denied(api("pipeline", "dryrun_resolve", token=token_b, payload={
        "pipeline_rid": b["pipeline_id"], "build_branch": "main",
        "output_dataset_rids": [b["output_id"]],
        "inline_specs": inline}),
                  "B dry-run inline spec with foreign input")

    # foreign-input Create is NOT probed here: inserting a bare job_specs
    # row would not touch the pipeline_job_specs canonical-rid mapping, so
    # resolution may legitimately keep using the original valid spec — a
    # rejection could never be attributed to tenancy, and an acceptance
    # could never be proven. This layer is covered by the Pipeline-side PG
    # legacy-mapping suite instead (tracked by the service session); the
    # HTTP layer covers foreign-input via Publish + inline-spec DryRun
    # above. Recorded explicitly so the layered evidence is visible.
    ev["foreign_input_create_coverage"] = (
        "layered: HTTP covers Publish+inline DryRun (above); CreateBuild "
        "foreign-input semantics covered by the Pipeline PG legacy-mapping "
        "test suite (service-side), not constructible over HTTP")

    # adminA -> B resources: the SAME three entry points as B -> A
    # (same-org admin forward validity is proven right below).
    assert_denied(api("pipeline", "spec_publish", token=ids["admin_a"].token,
                      payload=publish_body(b["pipeline_id"], b["input_id"], b["output_id"])),
                  "org A admin publishes spec on B resources")
    assert_denied(api("pipeline", "dryrun_resolve", token=ids["admin_a"].token, payload={
        "pipeline_rid": b["pipeline_id"], "build_branch": "main",
        "output_dataset_rids": [b["output_id"]]}),
                  "org A admin dry-run on B pipeline")
    assert_denied(api("pipeline", "build_create", token=ids["admin_a"].token,
                      payload=build_body(b["pipeline_id"], b["output_id"])),
                  "org A admin creates build on B pipeline")

    # same-org admin publishes the SAME fully-valid body on A's resources:
    # proves the rejections above can only come from the tenant boundary
    admin_publish = api("pipeline", "spec_publish", token=ids["admin_a"].token,
                        payload=publish_body(a["pipeline_id"], a["input_id"], a["output_id"]))
    expect(admin_publish.status == 201, "same-org admin cannot publish a valid spec "
           "(so the B rejections above cannot be attributed to tenancy)",
           {"status": admin_publish.status, "body": scrub(str(admin_publish.body))[:300]})
    # (B's own-chain body-shape validity was already proven BEFORE the
    # snapshots — see own_create/own_abort above.)

    # both domains unchanged across every denied attempt
    after_a = snapshot_build_side_effects(token_a, a["output_id"])
    after_b = snapshot_build_side_effects(token_b, b["output_id"])
    after_leg = snapshot_build_side_effects(token_leg, leg["output_id"])
    ev["a_side_effects_unchanged"] = after_a == before_a
    ev["b_side_effects_unchanged"] = after_b == before_b
    ev["personal_side_effects_unchanged"] = after_leg == before_leg
    expect(after_a == before_a, "denied attempts left side effects on A",
           {"files": [before_a["file_ids"], after_a["file_ids"]],
            "open_txns": [before_a["open_txn_ids"], after_a["open_txn_ids"]],
            "builds_grew": sorted(set(after_a["builds_ids"] or []) - set(before_a["builds_ids"] or []))[:5],
            "branch_head_moved": after_a["branch_head"] != before_a["branch_head"]})
    expect(after_leg == before_leg, "denied attempts left side effects on the personal domain",
           {"files": [before_leg["file_ids"], after_leg["file_ids"]]})
    expect(after_b == before_b, "own-chain activity leaked into the B snapshot",
           {"builds_grew": sorted(set(after_b["builds_ids"] or []) - set(before_b["builds_ids"] or []))[:5]})
    # locks: HTTP cannot list locks directly; the legitimate same-output
    # re-create in build_positive plus the PG build_input_locks check there
    # are the lock-cleanup evidence. No "no locks at all" claim is made here.
    ev["lock_evidence"] = "via build_positive re-create + PG build_input_locks count"
    return ev


SCENARIOS = (
    ("dataset_isolation", scenario_dataset_isolation),
    ("pipeline_isolation", scenario_pipeline_isolation),
    ("connector_isolation", scenario_connector_isolation),
    ("legacy_regression", scenario_legacy_regression),
    ("sync_real_rest", scenario_sync_real_rest),
    ("hash_change", scenario_hash_change),
    ("sync_failures", scenario_sync_failures),
    ("build_positive", scenario_build_positive),
    ("build_isolation", scenario_build_isolation),
)


def main():
    global ARGS
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-dir", required=True)
    parser.add_argument("--wait-only", action="store_true")
    parser.add_argument("--only", nargs="*", default=None)
    ARGS = parser.parse_args()
    env_dir = Path(ARGS.env_dir)
    load_env(env_dir)

    suffix = uuid.uuid4().hex[:8]
    print(f"[smoke] run suffix {suffix}", flush=True)
    wait_health()
    print("[smoke] all four endpoints healthy", flush=True)
    if ARGS.wait_only:
        return 0

    ids = build_identities()
    results = {}
    evidence = {}
    for name, fn in SCENARIOS:
        if ARGS.only and name not in ARGS.only:
            continue
        started = time.time()
        try:
            if name == "hash_change":
                if not evidence.get("sync_real_rest"):
                    raise AssertionError("sync_real_rest evidence missing (run it first)")
                ev = fn(ids, suffix, evidence["sync_real_rest"])
            elif name == "build_isolation":
                if not evidence.get("build_positive"):
                    raise AssertionError("build_positive evidence missing (run it first)")
                ev = fn(ids, suffix, evidence["build_positive"])
            else:
                ev = fn(ids, suffix)
            results[name] = {"status": "pass", "seconds": round(time.time() - started, 1)}
            evidence[name] = ev
            print(f"[smoke] PASS {name} ({results[name]['seconds']}s)", flush=True)
        except (AssertionError, ConnectionError, ValueError, KeyError, TypeError) as exc:
            results[name] = {"status": "fail", "seconds": round(time.time() - started, 1),
                             "error": scrub(str(exc))}
            evidence[name] = {"error": scrub(str(exc))}
            print(f"[smoke] FAIL {name}: {scrub(str(exc))}", flush=True)

    failed = [n for n, r in results.items() if r["status"] == "fail"]
    summary = {"suffix": suffix, "started": datetime.now(timezone.utc).isoformat(),
               "results": results, "evidence": evidence,
               "uncovered": UNCOVERED,
               "counts": {"pass": len(results) - len(failed), "fail": len(failed),
                          "uncovered": len(UNCOVERED)}}
    out = env_dir / "result.json"
    out.write_text(json.dumps(summary, indent=2) + "\n")
    # sub/org UUIDs only — NEVER tokens/secrets — so post-restart probes
    # can mint a matching identity and re-read the artifacts this run created.
    (env_dir / "last-identities.json").write_text(json.dumps({
        key: {"sub": ident.sub, "org": ident.org}
        for key, ident in ids.items() if isinstance(ident, Identity)
    }, indent=2) + "\n")
    if UNCOVERED:
        print(f"[smoke] UNCOVERED aspects ({len(UNCOVERED)}) — full acceptance NOT claimed:", flush=True)
        for item in UNCOVERED:
            print(f"[smoke]   - {item['scenario']}/{item['aspect']}: {item['reason'][:160]}", flush=True)
    print(f"[smoke] pass={summary['counts']['pass']} fail={summary['counts']['fail']} "
          f"uncovered={summary['counts']['uncovered']} -> {out}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

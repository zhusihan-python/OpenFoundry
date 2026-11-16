#!/usr/bin/env python3
"""P2.6 W8 CURRENT-state preflight collector (q3 old-state preservation).

The W6 switch-guard (`p26_switch_guard.py guard`) proves the HISTORICAL
sealed baseline is intact (full seal + derived projection). It does NOT
prove the live q3 substrate still matches it. THIS tool performs the
fresh CURRENT read-only comparison and keeps the two verdicts strictly
separate in every result:

  historical = sealed P25 evidence chain still valid (seal FULL + derive)
  current    = live q3 DB rows / public API views / container identity /
               fixture counters / grants / old artifacts == sealed baseline

Modes:
  plan  (default) Filesystem facts ONLY: full seal verify + derive + the
        fixed-origin substrate files (compose/env key names). Emits the
        EXACT argv / URLs / pins / origin allowlist / tool SHAs / attempt
        schema for Root review. ZERO docker / HTTP / SQL calls (the pure
        selftest proves this by injecting counting fakes).
  run   (--execute --release-file) Executes the current checks read-only.
        Requires the Root release token to list phase
        'q3-readonly-preflight'; otherwise the whole run REFUSES with zero
        live calls. Every actual command keeps its ORIGINAL rc + scrubbed
        raw output in an append-only unique attempt dir. Any error /
        missing / extra / duplicate / partial => fail-closed (never
        quiet-empty).

Safety rails (both modes): outputs only under results/w8 (or an explicit
--out-dir inside p2-6); nothing inside the frozen P25 tree is ever
written; secret values live only in memory (the q3 env file is read
solely to sign the testing JWT / to list key NAMES) and are scrubbed from
every recorded artifact; the signed JWT / presigned query HMAC / DSN
never reach evidence; HTTP NEVER follows redirects automatically and
NEVER forwards Authorization to a redirect target (exactly one validated
anonymous follow for the download 302).
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import sys
import time
import uuid as uuidlib
from pathlib import Path
from urllib import request as urlreq, error as urlerr

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402
from cohort import p26_cohort_cli as C  # noqa: E402

TOOL = "p26_current_preflight"
PHASE = "q3-readonly-preflight"
# Root R1 scope ruling: first preflight slice is CORE preservation only;
# full all-historical Build/Job regressions stay explicitly incomplete.
SCOPE = "current_core_preservation"
RESULTS_W8 = G.ROOT_P26 / "results" / "w8"

# --- fixed q3 substrate (Root handoff 2026-11-15: origins are FROZEN) -------
Q3_DIR = Path("/private/tmp/openfoundry-platform-p1-q3v9")
COMPOSE_FILE = Q3_DIR / "compose.yaml"
Q3_ENV_FILE = Q3_DIR / ".env"
# Root-fixed host origins (cross-checked against compose at load time).
ORIGINS = {
    "dataset": "http://127.0.0.1:17078",
    "pipeline": "http://127.0.0.1:17081",
    "connector": "http://127.0.0.1:17088",
    "fixture": "http://127.0.0.1:17090",
}
EXPECTED_PORT_MAP = {"17078": "50078", "17081": "50081",
                     "17088": "50088", "17090": "8090"}
DOWNLOAD_REDIRECT_PREFIX = "/v1/_internal/local-fs/"
# Old fixture MUST NEVER restart (Root handoff boot pin; PID1 python).
EXPECTED_FIXTURE_STARTED_AT = "2026-11-13T18:29:14.176785055Z"
Q3_FIXTURE = f"{G.Q3_PROJECT}-fixture-1"
Q3_POSTGRES = f"{G.Q3_PROJECT}-postgres-1"
Q3_DATASET = f"{G.Q3_PROJECT}-dataset-1"
Q3_PIPELINE = f"{G.Q3_PROJECT}-pipeline-1"
Q3_CONNECTOR = f"{G.Q3_PROJECT}-connector-1"
EXPECTED_CONFIG_IMAGE = {Q3_FIXTURE: "python:3.12-alpine",
                         Q3_POSTGRES: "postgres:16-alpine"}

# Connector loop knobs (config.go names). compose does NOT set them =>
# accepted loops-off baseline is their ABSENCE (defaults); presence of any
# of these env NAMES on the live container is a deviation. Values never
# read.
LOOP_ENV_NAMES = (
    "OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS",
    "OPENFOUNDRY_SYNC_AUTO_RECOVERY_ENABLED",
    "OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS",
    "SYNC_POLL_INTERVAL_SECS",
    "OPENFOUNDRY_DEV_AUTH",
)

# --- sealed catalog members (manifest-verified reads; never hand-copied) ---
SEAL_MEMBERS = {
    "grants_cmd": "owned-quiesce/attempt1/state.json.raw/grants.cmd.json",
    "counters": "postfacts-attempts/attempt-1/03-counters.read1.derived",
    "h14_download_cmd": "tail-h14-supplement/attempt2/download.cmd.json",
    "h14_download_body": "tail-h14-supplement/attempt2/download.body",
    "h14_txn_cmd": "tail-h14-supplement/attempt1/transaction.cmd.json",
    "h14_txn_body": "tail-h14-supplement/attempt1/transaction.body",
    "final_running": "final-running.json",
    "quiesce_before": C.QUIESCE_BEFORE,
}

# Expected docker network identity (compose `p1net` under the q3 project)
EXPECTED_NETWORKS = frozenset({f"{G.Q3_PROJECT}_p1net"})
EXPECTED_FIXTURE_IP = "172.28.9.9"          # compose ipv4_address pin
# Health expectations: fixture+postgres carry compose healthchecks; the
# base3 service containers have none (liveness proven via StartedAt/
# Running/RestartCount pins instead).
EXPECTED_HEALTH = {Q3_FIXTURE: "healthy", Q3_POSTGRES: "healthy"}
HEALTH_NONE = "no-healthcheck"

# HTTP / process bounds
HTTP_TIMEOUT_S = 15
HTTP_MAX_BODY = 4 << 20          # 4 MiB read cap
JWT_TTL_S = 3600
RUNNER_TIMEOUT_S = 120
COUNTER_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
# presigned query VALUES are redacted in ANY textual context (query,
# whitespace- or quote-delimited exception text); a bare 64-hex scan is
# deliberately NOT used — it would also mask safe content_hash values.
SIG_QUERY_RE = re.compile(
    r"((?:^|[?&\s\"'])(?:sig|signature|token|access_token)=)"
    r"[^&\s\"']+", re.I | re.M)
BEARER_RE = re.compile(r"(Bearer\s+)\S+")


class PreflightRefusal(Exception):
    """Fail-closed refusal BEFORE any live call."""


# ===========================================================================
# sealed baseline loading (historical; read-only filesystem)
# ===========================================================================
def _manifest(final_dir: Path):
    hm = json.loads((final_dir / "hash-manifest.json").read_text())
    files = hm.get("files")
    if not isinstance(files, dict) or not files:
        raise PreflightRefusal("hash-manifest has no files map")
    return files


def _verified_read(final_dir: Path, rel: str, files: dict) -> bytes:
    if rel not in files:
        raise PreflightRefusal(f"{rel} not indexed by the seal manifest")
    p = final_dir / rel
    if not p.is_file():
        raise PreflightRefusal(f"{rel} missing from the sealed dir")
    if G.sha256_file(p) != files[rel]:
        raise PreflightRefusal(f"{rel} sha256 mismatch vs manifest — seal "
                               "broken, refusing")
    return p.read_bytes()


def load_baseline(final_dir: Path):
    """Full seal verify + derive + sealed catalog members.

    Returns (historical, catalog, derived). Raises PreflightRefusal on ANY
    baseline problem (fail-closed: a current comparison against a broken
    baseline is meaningless)."""
    seal = C.verify_seal(final_dir, G.EXPECTED_SEAL_SHA256,
                         G.EXPECTED_SEALED_FILES)
    if not (seal["ok"] and not seal["partial"]):
        raise PreflightRefusal("historical seal not fully green: "
                               + "; ".join(seal["problems"][:4]))
    derived = C.derive_protected(final_dir, G.EXPECTED_SEAL_SHA256,
                                 G.EXPECTED_SEALED_FILES)
    files = _manifest(final_dir)

    def vread(rel):
        return _verified_read(final_dir, rel, files)

    grants_argv = json.loads(vread(SEAL_MEMBERS["grants_cmd"]))
    if not (isinstance(grants_argv, list) and grants_argv[:1] == ["docker"]):
        raise PreflightRefusal("sealed grants cmd is not a docker argv")

    counters = json.loads(vread(SEAL_MEMBERS["counters"]))
    counter_shas = counters.get("bodies_sha256")
    if not isinstance(counter_shas, dict) or not counter_shas:
        raise PreflightRefusal("sealed counters derived lacks bodies_sha256")
    for name, sha in counter_shas.items():
        if not COUNTER_NAME_RE.match(name):
            raise PreflightRefusal(f"sealed counter name unsafe: {name!r}")
        if not HEX64_RE.match(sha):
            raise PreflightRefusal(f"sealed counter sha malformed: {name}")
        if (counters.get(name) or {}).get("http_status") != 200:
            raise PreflightRefusal(f"sealed counter {name}: http_status "
                                   "!= 200")

    dl_cmd = json.loads(vread(SEAL_MEMBERS["h14_download_cmd"]))
    dl_body = vread(SEAL_MEMBERS["h14_download_body"])
    txn_cmd = json.loads(vread(SEAL_MEMBERS["h14_txn_cmd"]))
    txn = json.loads(vread(SEAL_MEMBERS["h14_txn_body"]))
    for what, cmd in (("download", dl_cmd), ("txn", txn_cmd)):
        if not (isinstance(cmd, dict) and cmd.get("method") == "GET"
                and isinstance(cmd.get("url"), str)
                and cmd["url"].startswith(ORIGINS["dataset"] + "/")):
            raise PreflightRefusal(f"sealed H14 {what} cmd malformed or "
                                   "origin drift vs fixed allowlist")
    md = txn.get("metadata") or {}
    h14_txn_projection = {
        "id": txn.get("id"), "dataset_id": txn.get("dataset_id"),
        "branch_name": txn.get("branch_name"), "status": txn.get("status"),
        "tx_type": txn.get("tx_type"),
        "metadata_rows": md.get("rows"), "metadata_bytes": md.get("bytes"),
        "metadata_file_count": md.get("file_count"),
        "metadata_content_hash": md.get("content_hash"),
    }
    if not (G.UUID_RE.match(str(h14_txn_projection["id"] or ""))
            and h14_txn_projection["status"] == "COMMITTED"
            and HEX64_RE.match(str(md.get("content_hash") or ""))):
        raise PreflightRefusal("sealed H14 txn projection invalid")

    final_running = json.loads(vread(SEAL_MEMBERS["final_running"]))
    base3 = {}
    for svc in ("dataset", "pipeline", "connector"):
        e = (final_running.get("services") or {}).get(svc) or {}
        if not (e.get("running")
                and str(e.get("image", "")).startswith("sha256:")
                and e.get("started_at")):
            raise PreflightRefusal(f"sealed final-running lacks {svc} "
                                   "running image baseline")
        base3[svc] = {"image": e["image"], "started_at": e["started_at"]}

    before = json.loads(vread(SEAL_MEMBERS["quiesce_before"]))
    if not isinstance(before, dict) or not before:
        raise PreflightRefusal("sealed quiesce before.json empty")

    catalog = {
        "grants_argv": grants_argv,
        "counter_names": sorted(counter_shas),
        "counter_bodies_sha256": counter_shas,
        "h14_download_url": dl_cmd["url"],
        "h14_download_body_sha256": hashlib.sha256(dl_body).hexdigest(),
        "h14_download_body_len": len(dl_body),
        "h14_txn_url": txn_cmd["url"],
        "h14_txn_projection": h14_txn_projection,
        "base3_container_baseline": base3,
        "quiesce_before": before,
        "owner_parents": _load_owner_parents(final_dir, files, derived),
    }
    historical = {
        "seal": {k: seal[k] for k in ("ok", "files_total", "files_checked",
                                      "partial", "seal_sha256")},
        "derived": {"protected9": len(derived["protected9"]),
                    "cp8": len(derived["cp8"]),
                    "h10_run_id": derived.get("h10_run_id")},
        "note": "historical = sealed evidence chain valid; NOT a statement "
                "about the live q3 state (the current section proves that)",
    }
    return historical, catalog, derived


def _load_owner_parents(final_dir: Path, files: dict, derived):
    """Root R1#1: complete safe Def→Source parents baseline for the 8 CP
    owner defs, derived from the SEALED quiesce driver's actual public
    responses (sync_get + connections GET, manifest-verified). Fields are
    IDs/org/owner/name ONLY — never config/auth/opaque. Field values the
    sealed responses lack are reported as baseline_absent (never guessed).
    All 8 defs/sources MUST be covered — the quiesce driver resolved every
    parent before any mutation."""
    def_ids = {e["def_id"]: e for e in derived["cp8"]}
    source_ids = {e["source_id"] for e in derived["cp8"]}
    base = {"by_def": {}, "source_baseline": {}}
    http_prefix = "owned-quiesce/attempt1/http-"
    for rel in sorted(files):
        if not (rel.startswith(http_prefix) and rel.endswith(".cmd.json")):
            continue
        cmd = json.loads(_verified_read(final_dir, rel, files))
        url = (cmd or {}).get("url", "")
        m = re.search(r"/api/v1/data-connection/syncs/([0-9a-f-]{36})$",
                      url)
        if m and m.group(1) in def_ids and m.group(1) not in base["by_def"]:
            body = json.loads(_verified_read(
                final_dir, rel[:-len(".cmd.json")] + ".body", files))
            if not isinstance(body, dict) or body.get("id") != m.group(1):
                raise PreflightRefusal(f"sealed sync_get {rel}: body/def "
                                       "mismatch")
            base["by_def"][m.group(1)] = {
                "def_id": body.get("id"), "org_id": body.get("org_id"),
                "source_id": body.get("source_id"),
                "output_dataset_id": body.get("output_dataset_id")}
            continue
        m = re.search(r"/api/v1/connections/([0-9a-f-]{36})$", url)
        if m and m.group(1) in source_ids and \
                m.group(1) not in base["source_baseline"]:
            body = json.loads(_verified_read(
                final_dir, rel[:-len(".cmd.json")] + ".body", files))
            if not isinstance(body, dict) or body.get("id") != m.group(1):
                raise PreflightRefusal(f"sealed connections {rel}: body/"
                                       "source mismatch")
            base["source_baseline"][m.group(1)] = {
                "conn_id": body.get("id"), "conn_org_id":
                body.get("org_id"), "conn_owner_id": body.get("owner_id"),
                "source_name": body.get("name"), "connector_type":
                body.get("connector_type")}
    missing_defs = sorted(set(def_ids) - set(base["by_def"]))
    missing_srcs = sorted(source_ids - set(base["source_baseline"]))
    if missing_defs or missing_srcs:
        raise PreflightRefusal(
            f"sealed parents baseline incomplete: defs {missing_defs} "
            f"sources {missing_srcs} — quiesce resolved every parent, so "
            "gaps mean a seal problem (refusing)")
    return base


def build_parents_argv(def_ids):
    """W8-own read-only parents SELECT (R1 #1; fields = IDs/org/owner/name
    ONLY — no config/auth/opaque/token; new collect query as authorized).
    def_ids injected programmatically from the derived cp8 roster."""
    ids = ",".join(f"'{d}'" for d in sorted(def_ids))
    return ["docker", "compose", "-f", str(COMPOSE_FILE),
            "--project-name", G.Q3_PROJECT, "exec", "-T", "postgres",
            "psql", "-U", "openfoundry", "-d", "connectors", "-t", "-A",
            "-v", "ON_ERROR_STOP=1",
            "-c", "SET TimeZone='UTC'; SET DateStyle='ISO, MDY';", "-c",
            "SELECT json_agg(row_to_json(t)) FROM ( SELECT "
            "d.id::text AS def_id, "
            "d.org_id::text, d.source_id::text, d.output_dataset_id::text, "
            "c.id::text AS conn_id, c.org_id::text AS conn_org_id, "
            "c.owner_id::text AS conn_owner_id, c.name AS source_name, "
            "c.connector_type FROM batch_sync_defs d JOIN connections c "
            f"ON c.id = d.source_id WHERE d.id IN ({ids})) t;"]


# keys the parents SQL projection MUST deliver (aliases match
# build_parents_argv; a row lacking any of them is a structured refusal,
# never a KeyError crash)
PARENTS_REQUIRED_KEYS = frozenset({
    "def_id", "org_id", "source_id", "output_dataset_id", "conn_id",
    "conn_org_id", "conn_owner_id", "source_name", "connector_type"})


def compare_parents(baseline, sealed_cp8, sealed_run_binding, current_text):
    """R1 #1 comparator: whole-8 def parents vs sealed; cross-field
    Def.source==CP.source / Def.output==CP.target; tenant binding
    Def.org==Conn.org (+sealed run org where present). Baseline-absent
    fields are REPORTED (coverage), never guessed or skipped."""
    problems, coverage = [], {"baseline_absent": [], "compared_fields": [
        "def_id", "org_id", "source_id", "output_dataset_id", "conn_id",
        "conn_org_id", "conn_owner_id", "source_name", "connector_type"]}
    try:
        rows = C._parse_rows_stdout(current_text, "current-cp-parents")
    except C.CohortRefusal as e:
        return {"ok": False, "problems": [f"parse: {e}"], "coverage":
                coverage, "counts": {}}
    cur = {}
    for i, r in enumerate(rows):
        missing = PARENTS_REQUIRED_KEYS - set(r)
        if missing:
            problems.append(f"row[{i}] missing schema keys "
                            f"{sorted(missing)} — structured fail-closed")
            continue
        if r["def_id"] in cur:
            problems.append(f"duplicate parent row {r['def_id']}")
            continue
        cur[r["def_id"]] = r
    base = baseline["by_def"]
    srcs = baseline["source_baseline"]
    for did in sorted(set(cur) - set(base)):
        problems.append(f"EXTRA parent def row {did}")
    for did in sorted(set(base) - set(cur)):
        problems.append(f"MISSING parent def row {did}")
    sealed_cp = {e["run_id"]: e["cp_owner_row_full"] for e in sealed_cp8}
    cp_by_def = {e["def_id"]: e["cp_owner_row_full"] for e in sealed_cp8}
    for did in sorted(set(cur) & set(base)):
        row, b, src = cur[did], base[did], srcs[base[did]["source_id"]]
        for k in ("def_id", "org_id", "source_id", "output_dataset_id"):
            if row.get(k) != b[k]:
                problems.append(f"def {did} field {k}: current "
                                f"{row.get(k)!r} != sealed {b[k]!r}")
        for k in ("conn_id", "conn_org_id", "conn_owner_id", "source_name",
                  "connector_type"):
            if k not in src:
                coverage["baseline_absent"].append(f"{did}.{k}")
                continue
            if row.get(k) != src[k]:
                problems.append(f"def {did} source field {k}: current "
                                f"{row.get(k)!r} != sealed {src[k]!r}")
        # cross-field: actual Def.source==CP.source, Def.output==CP.target
        sealed_row = cp_by_def.get(did, {})
        if row.get("source_id") != sealed_row.get("source_id"):
            problems.append(f"def {did}: Def.source {row.get('source_id')!r}"
                            f" != sealed CP.source "
                            f"{sealed_row.get('source_id')!r}")
        if row.get("output_dataset_id") != \
                sealed_row.get("output_dataset_id"):
            problems.append(
                f"def {did}: Def.output {row.get('output_dataset_id')!r} "
                f"!= sealed CP.target "
                f"{sealed_row.get('output_dataset_id')!r}")
        # tenant binding Def.org == Conn.org (current) == sealed values
        if row.get("org_id") != row.get("conn_org_id"):
            problems.append(f"def {did}: Def.org {row.get('org_id')!r} != "
                            f"Conn.org {row.get('conn_org_id')!r}")
        rb = sealed_run_binding.get(did)
        if rb and rb.get("org_id") is not None and \
                row.get("org_id") != rb["org_id"]:
            problems.append(f"def {did}: Def.org != sealed Run.org "
                            f"{rb['org_id']!r}")
    coverage["baseline_absent"] = sorted(set(coverage["baseline_absent"]))
    return {"ok": not problems and len(cur) == len(base), "problems":
            problems, "coverage": coverage,
            "counts": {"current": len(cur), "sealed": len(base)}}


def public_view_baselines(derived, catalog):
    """Per-def sealed baselines for the public-view checks. The REAL
    sealed shape (verified against the actual seal): the CP owner-8 ALREADY
    contains H10 — roster origins are failedhealthy x2 + h10-held x1 +
    tail x5, and derive proves the h10-held roster entry IS the controlled
    p25h13 run. So public views = the 8 owner defs; the protected9/cp8
    overlap must be exactly that one def (asserted here). Whole Run sets
    come from before.json (complete sets)."""
    before = catalog["quiesce_before"]
    out = []
    for e in derived["cp8"]:
        d = before.get(e["def_id"])
        if not isinstance(d, dict) or not isinstance(d.get("runs"), list):
            raise PreflightRefusal(f"owner def {e['def_id']} missing from "
                                   "sealed before.json")
        out.append({"def_id": e["def_id"], "kind": "owner",
                    "run_id": e["run_id"],
                    "baseline_cp": e["quiesce_checkpoint_full"],
                    "baseline_runs": d["runs"]})
    ids = [e["def_id"] for e in out]
    if len(ids) != 8 or len(set(ids)) != 8:
        raise PreflightRefusal(f"public-view defs {len(ids)}/"
                               f"{len(set(ids))} != 8 unique owner defs")
    # protected9 ∩ owner defs must be EXACTLY the H10 def
    h10_def = None
    for r in derived["protected9"]:
        for q in derived["quiesce_before_chain"]:
            if q["run_id"] == r["run_id"] and q.get("in_quiesce_before"):
                h10_def = r["def_id"]
    if h10_def is None:
        raise PreflightRefusal("no protected9 member is inside the quiesce "
                               "before.json (H10 chain broken)")
    overlap = sorted({r["def_id"] for r in derived["protected9"]}
                     & set(ids))
    if overlap != [h10_def]:
        raise PreflightRefusal(f"protected9/owner-def overlap {overlap} != "
                               "exactly [H10 def]")
    for e in out:
        if e["def_id"] == h10_def:
            e["kind"] = "owner+h10"
    return out


# ===========================================================================
# fixed-origin substrate facts (filesystem only; plan+run share these)
# ===========================================================================
def load_substrate_facts():
    if not COMPOSE_FILE.is_file():
        raise PreflightRefusal(f"missing {COMPOSE_FILE}")
    compose = COMPOSE_FILE.read_text()
    if f"name: {G.Q3_PROJECT}" not in compose:
        raise PreflightRefusal("compose project name drift")
    ports = dict(re.findall(r'- "127\.0\.0\.1:(\d+):(\d+)"', compose))
    if ports != EXPECTED_PORT_MAP:
        raise PreflightRefusal(f"compose host:container port map drift: "
                               f"{ports} != {EXPECTED_PORT_MAP}")
    env_refs = sorted(set(re.findall(r"\$\{([A-Z0-9_]+):?\??\}", compose)))
    if not env_refs:
        raise PreflightRefusal("compose references no env vars (parse bug)")
    if "dataset-files:/home/nonroot" not in compose:
        raise PreflightRefusal("dataset-files:/home/nonroot mount missing "
                               "from compose")
    env_present = Q3_ENV_FILE.is_file()
    env_mode = None
    env_key_names = None
    if env_present:
        env_mode = oct(os.stat(Q3_ENV_FILE).st_mode & 0o777)
        names = []
        for ln in Q3_ENV_FILE.read_text().splitlines():
            ln = ln.strip()
            if ln and not ln.startswith("#") and "=" in ln:
                names.append(ln.split("=", 1)[0])
        env_key_names = sorted(names)   # names ONLY; values never retained
    return {
        "compose_file": str(COMPOSE_FILE),
        "port_map": ports,
        "compose_env_refs": env_refs,
        "dataset_mount": "dataset-files:/home/nonroot",
        "env_file": str(Q3_ENV_FILE),
        "env_file_present": env_present,
        "env_file_mode": env_mode,
        "env_file_key_names": env_key_names,
        "fixture_boot_pin": EXPECTED_FIXTURE_STARTED_AT,
    }


def expect_images_from_args(args):
    """Root R2: optional EXPLICIT post-switch image pins (bound by the
    Root release). Values must be sha256:<64hex>. Only dataset/connector
    may be pinned this way; everything else always keeps the old pins."""
    expects = {}
    for svc, val in (("dataset", getattr(args, "expect_dataset_image",
                                         None)),
                     ("connector", getattr(args, "expect_connector_image",
                                           None))):
        if not val:
            continue
        if not (isinstance(val, str) and val.startswith("sha256:")
                and HEX64_RE.match(val[len("sha256:"):])):
            raise PreflightRefusal(
                f"--expect-{svc}-image must be sha256:<64hex>, got "
                f"{val!r}")
        expects[svc] = val
    return expects


def check_substrate(substrate):
    """Fail-closed substrate gates (env mode/keys). Returns problem list."""
    problems = []
    if not substrate["env_file_present"]:
        problems.append(f"q3 env file missing: {substrate['env_file']}")
        return problems
    if substrate["env_file_mode"] != "0o600":
        problems.append(f"env file mode {substrate['env_file_mode']} != "
                        "0o600")
    if substrate["env_file_key_names"] != substrate["compose_env_refs"]:
        problems.append(f"env key set drift: "
                        f"{substrate['env_file_key_names']} != compose refs "
                        f"{substrate['compose_env_refs']}")
    return problems


# ===========================================================================
# JWT (in-memory only; recipe == accepted P1 mint_token.py shape)
# ===========================================================================
def read_jwt_secret() -> str:
    """Read OF_P1_JWT_SECRET from the q3 env file into memory. The value is
    returned ONLY to the signer and is on the scrub list; never evidence."""
    if not Q3_ENV_FILE.is_file():
        raise PreflightRefusal(f"{Q3_ENV_FILE} missing")
    for ln in Q3_ENV_FILE.read_text().splitlines():
        if ln.startswith("OF_P1_JWT_SECRET="):
            val = ln.split("=", 1)[1].strip()
            if not val:
                raise PreflightRefusal("OF_P1_JWT_SECRET empty")
            return val
    raise PreflightRefusal("OF_P1_JWT_SECRET absent from q3 env file")


def jwt_identity_from_baseline(derived, catalog):
    """Root fix (post attempt1): the testing JWT identity comes from the
    SEALED H10 run's public projection (requested_by/org_id) — the P25
    main tenant that owns the whole CP-8 population — NOT from
    protected9[0], which is a foreign legacy P21 tenant row. Cross-asserts
    EVERY sealed owner parent carries that same conn_owner_id/org before
    signing; any mismatch or absent identity field refuses (fail-closed)."""
    h10_run = derived.get("h10_run_id")
    h10 = next((r for r in derived["protected9"]
                if r["run_id"] == h10_run), None)
    if h10 is None:
        raise PreflightRefusal("H10 run absent from derived protected9 — "
                               "cannot select JWT identity")
    proj = h10["public_projection_27"]
    sub, org = proj["requested_by"], proj["org_id"]
    if not (G.UUID_RE.match(str(sub or ""))
            and G.UUID_RE.match(str(org or ""))):
        raise PreflightRefusal("H10 run projection lacks UUID "
                               "requested_by/org_id — refusing to sign")
    for e in derived["cp8"]:
        src = (catalog["owner_parents"]["source_baseline"]
               .get(e["source_id"]) or {})
        for field, want in (("conn_owner_id", sub),
                            ("conn_org_id", org)):
            got = src.get(field)
            if got is None:
                raise PreflightRefusal(
                    f"sealed parent of def {e['def_id']} lacks {field} "
                    "(baseline absent) — cannot cross-assert JWT identity, "
                    "refusing")
            if got != want:
                raise PreflightRefusal(
                    f"sealed parent of def {e['def_id']} {field} {got!r} "
                    f"!= H10 identity {want!r} — whole-8 tenant identity "
                    "inconsistent, refusing to sign")
    return sub, org


def sign_testing_jwt(secret: str, sub: str, org_id: str, now=None):
    """HS256 JWT, stdlib only. Claims mirror libs/auth-middleware.Claims and
    the accepted P1 mint_token recipe (sub/jti/iat/exp/email/name/roles/
    permissions + org_id). Roles []/permissions dataset.write mirror the
    accepted P1 main-actor token shape; a live 401/403 fails CLOSED."""
    now = int(now if now is not None else time.time())

    def b64(raw: bytes) -> str:
        return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()

    header = b64(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    payload = {
        "sub": sub, "jti": str(uuidlib.uuid4()), "iat": now,
        "exp": now + JWT_TTL_S,
        "email": "w8-preflight@openfoundry.local",
        "name": "P26 W8 Current Preflight",
        "roles": [], "permissions": ["dataset.write", "dataset:write"],
        "org_id": org_id,
    }
    message = f"{header}.{b64(json.dumps(payload).encode())}"
    sig = b64(hmac.new(secret.encode(), message.encode(),
                       hashlib.sha256).digest())
    return f"{message}.{sig}"


# ===========================================================================
# no-redirect HTTP transport (own small urllib; W7 lib NOT imported: WIP)
# ===========================================================================
class _NoRedirectHandler(urlreq.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None   # a 3xx then surfaces as HTTPError, never followed


class HttpResponse:
    def __init__(self, status, headers, body, url, sent_auth, error=None):
        self.status, self.headers, self.body = status, headers, body
        self.url, self.sent_auth, self.error = url, sent_auth, error


def http_call(url, method="GET", auth=None, extra_headers=None,
              timeout_s=HTTP_TIMEOUT_S):
    """Single request, NO redirect following (3xx returned as-is).
    Authorization is attached only to this exact URL and never recorded.
    Body read is capped."""
    req = urlreq.Request(url, method=method)
    if auth:
        req.add_header("Authorization", f"Bearer {auth}")
    for k, v in (extra_headers or {}).items():
        req.add_header(k, v)
    opener = urlreq.build_opener(_NoRedirectHandler)
    try:
        with opener.open(req, timeout=timeout_s) as resp:
            body = resp.read(HTTP_MAX_BODY + 1)
            if len(body) > HTTP_MAX_BODY:
                raise PreflightRefusal(f"body exceeds {HTTP_MAX_BODY} cap")
            return HttpResponse(resp.status, dict(resp.headers), body,
                                url, bool(auth))
    except urlerr.HTTPError as e:
        body = e.read(HTTP_MAX_BODY + 1)
        if len(body) > HTTP_MAX_BODY:
            raise PreflightRefusal(f"body exceeds {HTTP_MAX_BODY} cap")
        return HttpResponse(e.code, dict(e.headers or {}), body, url,
                            bool(auth))
    except urlerr.URLError as e:
        return HttpResponse(None, {}, b"", url, bool(auth), error=str(e))


def safe_url_projection(url):
    """origin+path only; the query (presigned HMAC!) becomes a sha256:16
    digest + length and is NEVER recorded verbatim."""
    m = re.match(r"^(https?://[^/]+)(/[^?#]*)(\?[^#]*)?(#.*)?$", url or "")
    if not m:
        return {"url": "<unparseable>", "query_digest": None}
    q = m.group(3)
    digest = (f"sha256-{hashlib.sha256(q.encode()).hexdigest()[:16]}"
              f":len={len(q)}") if q else None
    return {"url": m.group(1) + m.group(2), "query_digest": digest}


def redact_text(text, secrets):
    text, n = G.scrub(text, secrets)
    text = SIG_QUERY_RE.sub(r"\1<redacted>", text)
    text = BEARER_RE.sub(r"\1***", text)
    return text, n


# ===========================================================================
# comparators (pure; negative controls in the selftest)
# ===========================================================================
def compare_controlled9(baseline_rows, current_text):
    """EXACT whole-set comparison of the 9 controlled rows. No UUID
    filtering: extra/missing/duplicate/changed-field all fail."""
    problems = []
    try:
        rows = C._parse_rows_stdout(current_text, "current-controlled-rows")
        for i, r in enumerate(rows):
            C._check_row_schema(r, "current-controlled-rows", i)
    except C.CohortRefusal as e:
        return {"ok": False, "problems": [f"parse/schema: {e}"],
                "counts": {}}
    cur = {}
    for r in rows:
        if r["id"] in cur:
            problems.append(f"duplicate run id {r['id']}")
            continue
        cur[r["id"]] = r
    base = {b["run_id"]: b["public_projection_27"] for b in baseline_rows}
    for rid in sorted(set(cur) - set(base)):
        problems.append(f"EXTRA controlled row {rid} (never filtered)")
    for rid in sorted(set(base) - set(cur)):
        problems.append(f"MISSING controlled row {rid}")
    for rid in sorted(set(cur) & set(base)):
        for k in C.SCHEMA_27:
            if cur[rid][k] != base[rid][k]:
                problems.append(f"row {rid} field {k}: current "
                                f"{cur[rid][k]!r} != sealed {base[rid][k]!r}")
    return {"ok": not problems and len(cur) == 9, "problems": problems,
            "counts": {"current": len(cur), "sealed": len(base)}}


def compare_cp_owners(baseline_cp8, current_text):
    """EXACT whole-set owner-8 comparison. NO protected-9 exemption: an
    extra owner is refused regardless of identity; count-only is never
    enough (per-field compare, duplicates refused)."""
    problems = []
    try:
        owners = C._parse_rows_stdout(current_text, "current-cp-owners")
    except C.CohortRefusal as e:
        return {"ok": False, "problems": [f"parse: {e}"], "counts": {}}
    by_run, by_def = {}, {}
    for o in owners:
        rid = o.get("run_id")
        if rid in by_run:
            problems.append(f"duplicate owner run {rid}")
            continue
        by_run[rid] = o
        if o.get("sync_def_id") in by_def:
            problems.append(f"duplicate owner def {o.get('sync_def_id')}")
        by_def[o.get("sync_def_id")] = o
    base = {e["run_id"]: e["cp_owner_row_full"] for e in baseline_cp8}
    for rid in sorted(set(by_run) - set(base)):
        problems.append(f"EXTRA CP owner run {rid} (no protected9 "
                        "exemption)")
    for rid in sorted(set(base) - set(by_run)):
        problems.append(f"MISSING CP owner run {rid}")
    for rid in sorted(set(by_run) & set(base)):
        for k, v in base[rid].items():
            if by_run[rid].get(k) != v:
                problems.append(f"owner {rid} field {k}: current "
                                f"{by_run[rid].get(k)!r} != sealed {v!r}")
    return {"ok": not problems and len(by_run) == 8, "problems": problems,
            "counts": {"current": len(by_run), "sealed": len(base)}}


def compare_public_view(def_id, baseline_cp, baseline_runs, cp_status,
                        cp_json, runs_status, runs_json):
    """Current public checkpoint + complete Run set for ONE def must equal
    the sealed originals (expired/failed owners still MUST retain)."""
    problems = []
    if cp_status == 404:
        return {"ok": False, "problems": [
            f"def {def_id}: checkpoint now 404 (was initialized) — owner "
            "lost"], "counts": {}}
    if cp_status != 200 or not isinstance(cp_json, dict):
        return {"ok": False, "problems": [
            f"def {def_id}: checkpoint status {cp_status} != 200"],
            "counts": {}}
    for k, v in baseline_cp.items():
        if cp_json.get(k) != v:
            problems.append(f"def {def_id} checkpoint.{k}: current "
                            f"{cp_json.get(k)!r} != sealed {v!r}")
    if runs_status != 200 or not isinstance(runs_json, list):
        return {"ok": False, "problems": problems + [
            f"def {def_id}: runs status {runs_status} != 200 or not a "
            "list"], "counts": {}}
    base_runs = {r["id"]: r for r in baseline_runs}
    cur_runs = {}
    for r in runs_json:
        if not isinstance(r, dict) or not G.UUID_RE.match(str(r.get("id")
                                                              or "")):
            problems.append(f"def {def_id}: malformed run entry")
            continue
        if r["id"] in cur_runs:
            problems.append(f"def {def_id}: duplicate run {r['id']}")
        cur_runs[r["id"]] = r
        if r.get("sync_def_id") != def_id:
            problems.append(f"def {def_id}: run {r['id']} parent "
                            f"{r.get('sync_def_id')!r} differs")
    for rid in sorted(set(cur_runs) - set(base_runs)):
        problems.append(f"def {def_id}: EXTRA public run {rid}")
    for rid in sorted(set(base_runs) - set(cur_runs)):
        problems.append(f"def {def_id}: MISSING public run {rid}")
    for rid in sorted(set(cur_runs) & set(base_runs)):
        for k, v in base_runs[rid].items():
            if cur_runs[rid].get(k) != v:
                problems.append(f"def {def_id} run {rid} field {k}: "
                                f"current {cur_runs[rid].get(k)!r} != "
                                f"sealed {v!r}")
    return {"ok": not problems, "problems": problems,
            "counts": {"current_runs": len(cur_runs),
                       "sealed_runs": len(base_runs)}}


def validate_download_redirect(location):
    """302 Location must be the SAME Dataset origin with the internal
    local-fs prefix and a presigned query. Returns the exact follow URL
    (kept in memory) or refuses."""
    m = re.match(r"^(https?://[^/]+)(/[^?#]*)(\?.*)?$", location or "")
    if not m:
        raise PreflightRefusal("download redirect unparseable: "
                               f"{safe_url_projection(location)}")
    origin, path, query = m.group(1), m.group(2), m.group(3) or ""
    if origin != ORIGINS["dataset"]:
        raise PreflightRefusal(
            f"download redirect origin {origin} != fixed allowlist "
            f"{ORIGINS['dataset']} — WRONG ORIGIN refused")
    if not path.startswith(DOWNLOAD_REDIRECT_PREFIX):
        raise PreflightRefusal(f"download redirect path {path!r} lacks "
                               f"prefix {DOWNLOAD_REDIRECT_PREFIX!r}")
    if not query:
        raise PreflightRefusal("download redirect lacks presigned query "
                               "(cannot be the local-fs proxy)")
    return location


def compare_grants_stdout(text):
    """Sealed grants-0 shape: the two SET confirmations and NO rows."""
    lines = [ln for ln in text.splitlines() if ln.strip()]
    problems = []
    if lines[:2] != ["SET", "SET"]:
        problems.append(f"unexpected psql preamble {lines[:2]!r}")
    rows = [ln for ln in lines[2:] if ln.strip()]
    if rows:
        problems.append(f"grants NOT zero: {len(rows)} row(s) returned")
    return {"ok": not problems, "problems": problems,
            "counts": {"rows": len(rows)}}


def compare_h14_txn(sealed_projection, status, body_json):
    """Old committed txn safe projection must be unchanged."""
    cur = body_json if isinstance(body_json, dict) else {}
    md = cur.get("metadata") or {}
    proj = {"id": cur.get("id"), "dataset_id": cur.get("dataset_id"),
            "branch_name": cur.get("branch_name"),
            "status": cur.get("status"), "tx_type": cur.get("tx_type"),
            "metadata_rows": md.get("rows"), "metadata_bytes":
            md.get("bytes"), "metadata_file_count": md.get("file_count"),
            "metadata_content_hash": md.get("content_hash")}
    problems = []
    if status != 200:
        problems.append(f"txn status {status} != 200")
    for k, v in sealed_projection.items():
        if proj.get(k) != v:
            problems.append(f"txn field {k}: current {proj.get(k)!r} != "
                            f"sealed {v!r}")
    return {"ok": not problems, "problems": problems, "counts": {}}


# ===========================================================================
# attempt evidence (append-only, unique dirs)
# ===========================================================================
class Attempt:
    def __init__(self, root: Path, prefix: str):
        root = root.resolve()
        if G.ROOT_P25 in root.parents or root == G.ROOT_P25:
            raise PreflightRefusal("attempt dir inside frozen P25 tree")
        if G.ROOT_P26 not in root.parents:
            raise PreflightRefusal("attempt dir must stay under p2-6")
        token = f"{G.now_utc()}-{uuidlib.uuid4().hex[:6]}"
        self.dir = root / f"{prefix}-{token}"
        if self.dir.exists():
            raise PreflightRefusal("attempt dir collision — append-only")
        self.dir.mkdir(parents=True)
        self.n = 0
        self.secrets = []

    def add_secret(self, value):
        if value:
            self.secrets.append(value)

    def _next(self, sid):
        self.n += 1
        return self.dir / f"{self.n:02d}-{sid}"

    def redact(self, text):
        out, _ = redact_text(text, self.secrets)
        return out

    def step_begin(self, sid, cmd):
        """R1 #3: the SAFE cmd projection is on disk BEFORE the actual
        call runs — a later exception can never lose the step."""
        p = self._next(sid)
        cmd_s = self.redact(json.dumps(cmd, sort_keys=True))
        p.with_suffix(".cmd.json").write_text(cmd_s + "\n")
        p.with_suffix(".rc").write_text("pending\n")
        return str(p)

    def step_finish(self, sid, path, raw, rc, meta=None, kind="call"):
        """Complete a begun step: ORIGINAL rc + scrubbed raw + meta."""
        p = Path(path)
        if isinstance(raw, bytes):
            raw = raw.decode(errors="replace")
        raw_s, n_scrub = redact_text(str(raw), self.secrets)
        p.with_suffix(".raw").write_text(raw_s + "\n")
        p.with_suffix(".rc").write_text(f"{rc}\n")
        meta = dict(meta or {}, kind=kind, scrub_hits=n_scrub,
                    utc=G.now_utc())
        p.with_suffix(".meta.json").write_text(
            self.redact(json.dumps(meta, sort_keys=True)) + "\n")
        return str(p)

    def record(self, sid, cmd, raw, rc, meta=None, kind="call"):
        """One-shot evidence unit for LOCAL (already-finished) steps. rc is
        the ORIGINAL rc; raw is scrubbed; cmd is redacted."""
        path = self.step_begin(sid, cmd)
        return self.step_finish(sid, path, raw, rc, meta, kind)

    def gate(self, sid, result):
        p = self._next(sid + "-gate")
        res_s = self.redact(json.dumps(result, sort_keys=True))
        p.with_suffix(".gate.json").write_text(res_s + "\n")
        p.with_suffix(".gate.rc").write_text(
            f"{0 if result.get('ok') else 1}\n")
        return result.get("ok", False)

    def finish(self, payload):
        """R1 #3: terminal RESULT is scrubbed with EVERY registered secret
        (plus sig/Bearer patterns) before it reaches disk."""
        (self.dir / "RESULT.json").write_text(
            self.redact(json.dumps(payload, default=str, indent=1,
                                   sort_keys=True)) + "\n")


# ===========================================================================
# step planning (single source for plan review AND run execution)
# ===========================================================================
DOCKER_PS_FORMAT = "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}"


def docker_inspect_probes(container):
    return [
        ("identity", ["docker", "inspect", container, "--format",
                      "{{.Id}}\n{{.Image}}\n{{.Config.Image}}\n"
                      "{{.State.StartedAt}}\n{{.State.Running}}\n"
                      "{{.RestartCount}}\n{{json .Config.Cmd}}"]),
        ("mounts", ["docker", "inspect", container, "--format",
                    "{{range .Mounts}}{{.Name}}:{{.Destination}}:{{.Type}}"
                    " {{end}}"]),
        ("ports", ["docker", "inspect", container, "--format",
                   "{{json .NetworkSettings.Ports}}\n"
                   "{{json .HostConfig.PortBindings}}"]),
        # health whitelist (R1 #4): real health where a healthcheck exists;
        # base3 has none (recorded as-is, liveness via identity pins)
        ("health", ["docker", "inspect", container, "--format",
                    "{{if .State.Health}}{{.State.Health.Status}}{{else}}"
                    "no-healthcheck{{end}}"]),
        # network identity whitelist (R1 #4): network name(s) + IP; only
        # the fixture IP is pinned (compose static address)
        ("network", ["docker", "inspect", container, "--format",
                     "{{range $k, $v := .NetworkSettings.Networks}}"
                     "{{$k}}={{$v.IPAddress}} {{end}}"]),
        # env NAMES only — the template cuts at '=' so values never even
        # reach this process
        ("env-names", ["docker", "inspect", container, "--format",
                       '{{range .Config.Env}}{{println (index (split . "=") 0)}}{{end}}']),
    ]


def build_step_specs(catalog):
    """Ordered live-check specs ('argv' via bounded runner, 'http' via the
    no-redirect transport, comparators attached as gate=fn(raw, state))."""
    specs = []
    add = specs.append
    add({"id": "docker-ps", "kind": "argv",
         "argv": ["docker", "ps", "--format", DOCKER_PS_FORMAT],
         "purpose": "q3 base3+fixture present; q3 5 containers intact"})
    for container in (Q3_POSTGRES, Q3_FIXTURE, Q3_DATASET, Q3_PIPELINE,
                      Q3_CONNECTOR):
        short = container.replace(f"{G.Q3_PROJECT}-", "")
        for probe, argv in docker_inspect_probes(container):
            add({"id": f"inspect-{short}-{probe}", "kind": "argv",
                 "argv": argv, "container": container,
                 "purpose": f"whitelisted {probe} facts for {container}"})
    for svc in ("dataset", "pipeline", "connector"):
        add({"id": f"service-healthz-{svc}", "kind": "http",
             "url": ORIGINS[svc] + "/healthz", "auth": False,
             "purpose": f"base3 {svc} SERVICE health (anonymous GET "
                        f"/healthz; docker no-healthcheck only proves no "
                        f"probe, not service health)"})
    add({"id": "sql-controlled-9", "kind": "argv",
         "argv": C.OLDPG_READONLY_ARGV["controlled_rows_27col"],
         "purpose": "current controlled WHOLE set (SELECT-only, verbatim "
                    "accepted argv; no UUID filtering)",
         "gate": ("controlled9", lambda raw, st:
                  compare_controlled9(st["baseline_p9"], raw))})
    add({"id": "sql-cp-owners", "kind": "argv",
         "argv": C.OLDPG_READONLY_ARGV["cp_owners"],
         "purpose": "current CP owner WHOLE set (SELECT-only, verbatim "
                    "accepted argv)",
         "gate": ("cp8", lambda raw, st:
                  compare_cp_owners(st["baseline_cp8"], raw))})
    owner_defs = [e["def_id"] for e in catalog["public_views"]]
    run_binding = {}
    for e in catalog["public_views"]:
        for r in e["baseline_runs"]:
            if r.get("id") == e["run_id"]:
                run_binding[e["def_id"]] = {"org_id": r.get("org_id"),
                                            "owner_id": r.get("owner_id"),
                                            "requested_by":
                                            r.get("requested_by")}
    add({"id": "sql-cp-parents", "kind": "argv",
         "argv": build_parents_argv(owner_defs),
         "purpose": "current WHOLE-8 owner Def→Source/output/tenant/owner "
                    "safe parents (W8-own SELECT: IDs/org/owner/name only; "
                    "compared to the sealed quiesce public responses)",
         "gate": ("cp-parents", lambda raw, st, rb=run_binding:
                  compare_parents(st["catalog"]["owner_parents"],
                                  st["baseline_cp8"], rb, raw))})
    add({"id": "sql-grants", "kind": "argv", "argv": catalog["grants_argv"],
         "purpose": "owned-defs schedule grants must be ZERO (sealed argv "
                    "verbatim)",
         "gate": ("grants", lambda raw, st: compare_grants_stdout(raw))})
    add({"id": "fixture-healthz", "kind": "http",
         "url": ORIGINS["fixture"] + "/healthz", "auth": False,
         "purpose": "old fixture alive (public read-only, NO JWT)"})
    for name in catalog["counter_names"]:
        add({"id": f"fixture-counter-{name}", "kind": "http",
             "url": f"{ORIGINS['fixture']}/admin/src/{name}", "auth": False,
             "purpose": "counter state dump (read-only, NO JWT); body sha "
                        "must equal the sealed baseline",
             "counter": name,
             "expect_body_sha256": catalog["counter_bodies_sha256"][name]})
    add({"id": "jwt-sign", "kind": "jwt",
         "purpose": "sign testing JWT in memory (secret/JWT never in "
                    "evidence)"})
    for entry in catalog["public_views"]:
        did = entry["def_id"]
        add({"id": f"view-cp-{did}", "kind": "http", "def": did,
             "url": f"{ORIGINS['connector']}/api/v1/data-connection/syncs/"
                    f"{did}/checkpoint", "auth": True,
             "purpose": f"current public checkpoint def {did} "
                        f"({entry['kind']})"})
        add({"id": f"view-runs-{did}", "kind": "http", "def": did,
             "url": f"{ORIGINS['connector']}/api/v1/data-connection/syncs/"
                    f"{did}/runs", "auth": True,
             "purpose": f"current complete public Run set def {did}",
             "gate": (f"view-{did[:8]}",
                      lambda raw, st, e=entry: compare_public_view(
                          e["def_id"], e["baseline_cp"], e["baseline_runs"],
                          st["cp_status"][e["def_id"]], st["cp_json"][
                              e["def_id"]],
                          st["runs_status"][e["def_id"]], st["runs_json"][
                              e["def_id"]]))})
    add({"id": "h14-txn", "kind": "http", "auth": True,
         "url": catalog["h14_txn_url"],
         "purpose": "old committed txn safe projection unchanged",
         "gate": ("h14-txn", lambda raw, st: compare_h14_txn(
             st["catalog"]["h14_txn_projection"], st["h14_txn_status"],
             st["h14_txn_json"]))})
    add({"id": "h14-download", "kind": "download", "auth": True,
         "url": catalog["h14_download_url"],
         "purpose": "old file download: 302 -> validated same-origin "
                    "local-fs presigned -> ONE anonymous follow; HMAC "
                    "never recorded"})
    return specs


# ===========================================================================
# inspect predicates
# ===========================================================================
def evaluate_inspect(sid, container, raw, catalog, expects=None):
    """Whitelisted inspect predicates. Returns (ok, problem|None).

    `expects` maps an ALREADY-SWITCHED base3 service (Root-released
    --expect-*-image) to its new full image sha. For such a service the
    OLD image/StartedAt pins are replaced by the expect image pin (a new
    container legitimately has a new boot); health/Running/RestartCount/
    loops/network/volume checks still apply. Everything NOT switched
    (Pipeline, fixture, PG, and any service without an expect) keeps the
    STRICT old pins."""
    expects = expects or {}
    lines = raw.splitlines()
    if sid.endswith("-identity"):
        if len(lines) < 7:
            return False, "identity probe malformed"
        image, config_image, started, running, restarts = (
            lines[1], lines[2], lines[3], lines[4], lines[5])
        if container == Q3_FIXTURE:
            if started != EXPECTED_FIXTURE_STARTED_AT:
                return False, (f"fixture StartedAt {started!r} != pin "
                               f"{EXPECTED_FIXTURE_STARTED_AT!r} "
                               "(restart?)")
            if config_image != EXPECTED_CONFIG_IMAGE[Q3_FIXTURE]:
                return False, (f"fixture Config.Image {config_image!r} != "
                               f"{EXPECTED_CONFIG_IMAGE[Q3_FIXTURE]!r}")
            if "python3" not in raw or "rest_source.py" not in raw:
                return False, "fixture PID1 cmd no longer the old python " \
                              "rest_source"
        elif container == Q3_POSTGRES:
            if config_image != EXPECTED_CONFIG_IMAGE[Q3_POSTGRES]:
                return False, (f"postgres Config.Image {config_image!r} != "
                               f"{EXPECTED_CONFIG_IMAGE[Q3_POSTGRES]!r}")
        else:
            svc = {"dataset": "dataset", "pipeline": "pipeline",
                   "connector": "connector"}[
                container.replace(f"{G.Q3_PROJECT}-", "").replace("-1", "")]
            base = catalog["base3_container_baseline"][svc]
            if svc in expects:
                if image != expects[svc]:
                    return False, (f"{svc} image {image} != Root-released "
                                   f"expect {expects[svc]}")
                # switched container: new boot is legitimate; StartedAt
                # recorded as a fact, NOT compared to the old pin
            else:
                if image != base["image"]:
                    return False, (f"{svc} image {image} != sealed "
                                   f"{base['image']}")
                if started != base["started_at"]:
                    return False, (f"{svc} StartedAt {started} != sealed "
                                   f"{base['started_at']} (restart?)")
        if running != "true":
            return False, f"{container} not Running"
        if restarts != "0":
            return False, f"{container} RestartCount {restarts} != 0"
    elif sid.endswith("-ports"):
        if container == Q3_POSTGRES:
            parts = raw.splitlines()
            ok1, d1 = G.parse_ports_mapping(parts[0] if parts else "")
            ok2, d2 = G.parse_empty_bindings(
                parts[1] if len(parts) > 1 else "")
            if not (ok1 and ok2):
                return False, f"postgres published-port drift: {d1}/{d2}"
    elif sid.endswith("-health"):
        got = raw.strip()
        want = EXPECTED_HEALTH.get(container)
        if want is not None and got != want:
            return False, (f"{container} health {got!r} != {want!r}")
        if want is None and got != HEALTH_NONE:
            return False, (f"{container} has an unexpected healthcheck "
                           f"state {got!r}")
    elif sid.endswith("-network"):
        entries = [e for e in raw.split() if "=" in e]
        nets = {e.split("=", 1)[0] for e in entries}
        if nets != set(EXPECTED_NETWORKS):
            return False, (f"{container} networks {sorted(nets)} != "
                           f"{sorted(EXPECTED_NETWORKS)}")
        if container == Q3_FIXTURE:
            ips = {e.split("=", 1)[1] for e in entries}
            if EXPECTED_FIXTURE_IP not in ips:
                return False, (f"fixture IP {sorted(ips)} lacks compose pin "
                               f"{EXPECTED_FIXTURE_IP}")
    elif sid.endswith("-env-names"):
        if container == Q3_CONNECTOR:
            names = {ln.strip() for ln in lines if ln.strip()}
            hit = sorted(names & set(LOOP_ENV_NAMES))
            if hit:
                return False, (f"connector loop env names present {hit} "
                               "(baseline = absent/defaults => loops off)")
    elif sid.endswith("-mounts"):
        if container == Q3_DATASET:
            norm = raw.replace(f"{G.Q3_PROJECT}_dataset-files",
                               "dataset-files")
            if "dataset-files:/home/nonroot" not in norm:
                return False, ("dataset container lost the dataset-files:"
                               "/home/nonroot volume mount")
    return True, None


# ===========================================================================
# mode: plan (filesystem facts only; ZERO docker/HTTP/SQL)
# ===========================================================================
def cmd_plan(args):
    att = Attempt(Path(args.out_dir), "plan")
    historical, catalog, derived = load_baseline(G.P25_FINAL_DIR)
    substrate = load_substrate_facts()
    substrate_problems = check_substrate(substrate)
    catalog["public_views"] = public_view_baselines(derived, catalog)
    specs = build_step_specs(catalog)

    baseline_projection = {
        "protected9": [{"run_id": r["run_id"], "def_id": r["def_id"],
                        "origin": r["origin"]} for r in
                       derived["protected9"]],
        "cp8": [{"def_id": e["def_id"], "run_id": e["run_id"],
                 "origin": e["origin"], "tag": e["tag"]}
                for e in derived["cp8"]],
        "h10_run_id": derived.get("h10_run_id"),
    }
    att.record("historical-seal",
               {"mode": "plan", "action": "verify-seal FULL (read-only)"},
               json.dumps(historical, sort_keys=True), 0, kind="local")
    att.record("substrate-facts", {"files": [str(COMPOSE_FILE),
                                             str(Q3_ENV_FILE)]},
               json.dumps({**substrate,
                           "env_file_values": "NEVER READ in plan mode; "
                           "run mode reads OF_P1_JWT_SECRET into memory "
                           "only"}, sort_keys=True), 0, kind="local")
    att.gate("substrate", {"ok": not substrate_problems,
                           "problems": substrate_problems})

    plan = {
        "tool": TOOL, "phase": PHASE, "generated_utc": G.now_utc(),
        "live": False, "scope": SCOPE,
        "full_old_regressions_complete": False,
        "switch_policy": {
            "default": "strict OLD pins (sealed image+StartedAt) for all "
                       "base3 + fixture + PG",
            "post_switch_opt_in": "--expect-dataset-image / "
                                  "--expect-connector-image "
                                  "(sha256:<64hex>, Root-release-bound): "
                                  "the switched service drops the OLD "
                                  "StartedAt pin and pins the NEW image "
                                  "instead; health/Running/RestartCount/"
                                  "loops/network/volume checks unchanged; "
                                  "Pipeline/fixture/PG and unflagged "
                                  "services always keep old pins",
            "expects": {k: v for k, v in expect_images_from_args(args)
                        .items()}},
        "what_this_proves": "plan = filesystem facts + exact intended "
                            "argv/URLs for Root review. NOT a live verdict; "
                            "the current state is only proven by `run` "
                            "under a Root release token.",
        "historical": historical,
        "baseline_projection": baseline_projection,
        "sealed_catalog_pins": {k: v for k, v in catalog.items()
                                if k not in ("public_views",
                                             "quiesce_before")},
        "public_view_defs": [{"def_id": e["def_id"], "kind": e["kind"],
                              "run_id": e["run_id"],
                              "baseline_runs_count": len(e["baseline_runs"]),
                              "baseline_cp_keys": sorted(e["baseline_cp"])}
                             for e in catalog["public_views"]],
        "substrate": substrate,
        "origin_allowlist": ORIGINS,
        "redirect_policy": {
            "download_prefix": DOWNLOAD_REDIRECT_PREFIX,
            "follows": "exactly ONE, anonymous (Authorization NEVER "
                       "forwarded), only after origin+prefix validation",
            "presigned_query": "never recorded (sha256:16 digest + len "
                               "only)"},
        "jwt_policy": {
            "alg": "HS256",
            "secret_source": "OF_P1_JWT_SECRET from the q3 env file "
                             "(memory only; scrubbed from all evidence)",
            "claims": "sub/org_id from the SEALED H10 run "
                      "(0dd8f052…) public projection — the P25 main "
                      "tenant owning the whole CP-8 population (Root fix: "
                      "protected9[0] is a FOREIGN legacy P21 tenant); "
                      "cross-asserted against every sealed owner parent's "
                      "conn_owner_id/conn_org_id before signing; roles=[], "
                      "permissions=['dataset.write','dataset:write']",
            "note_for_root": "roles/permissions mirror the accepted P1 "
                             "mint_token main-actor shape; if the live "
                             "service rejects these reads the run fails "
                             "CLOSED with the concrete status — never "
                             "bypassed, no retry-around"},
        "release_schema": {"path": G.P26_RELEASE_FILE,
                           "released_for_must_include": PHASE},
        "attempt_schema": {
            "files_per_step": ["NN-<sid>.cmd.json (redacted)",
                               "NN-<sid>.raw (scrubbed safe projection)",
                               "NN-<sid>.rc (ORIGINAL rc)",
                               "NN-<sid>.meta.json"],
            "gates": "NN-<sid>-gate.gate.json + .gate.rc (wrapper verdict "
                     "kept DISTINCT from the original rc)",
            "dir": "append-only, UTC+token unique, never overwritten"},
        "steps": [{"id": s["id"], "kind": s["kind"], "argv": s.get("argv"),
                   "url": s.get("url"), "auth": s.get("auth", False),
                   "purpose": s["purpose"]} for s in specs],
        "deferred": {"build_job_all_history":
                    "explicitly DEFERRED to the final old P1/P2 HTTP "
                    "regressions (Root scope ruling); core slice is "
                    "current_core_preservation and never claims full "
                    "preflight completeness"},
        "tool_sha256": {str(p.relative_to(G.ROOT_P26)): G.sha256_file(p)
                        for p in [
                            Path(__file__),
                            Path(__file__).parent /
                            "selftest_p26_current_preflight.py",
                            G.ROOT_P26 / "common" / "p26_gates.py",
                            G.ROOT_P26 / "cohort" / "p26_cohort_cli.py",
                        ]},
    }
    att.record("plan", {"mode": "plan", "live": False},
               json.dumps({"steps": len(specs), "note": "full plan body "
                          "in plan.json"}, sort_keys=True), 0, kind="local")
    G.write_json(att.dir / "plan.json", plan)
    att.finish({"tool": TOOL, "mode": "plan", "live": False,
                "ok": not substrate_problems,
                "verdict": "plan-written",
                "plan_json": str(att.dir / "plan.json"),
                "note": "plan is filesystem facts + intended argv only; "
                        "no live verdict is claimed"})
    print(f"[{TOOL}] plan (filesystem facts only) -> "
          f"{att.dir / 'plan.json'}")
    return 0 if not substrate_problems else 1


# ===========================================================================
# mode: run (read-only live checks; needs --execute + Root release)
# ===========================================================================
def cmd_run(args, transport=None, runner=None):
    att = Attempt(Path(args.out_dir), "run")
    release = G.load_release(args.release_file)
    if (not args.execute or release is None
            or PHASE not in release.get("released_for", [])):
        att.finish({"tool": TOOL, "phase": PHASE, "verdict": "refused",
                    "live": True, "ok": False, "scope": SCOPE,
                    "full_old_regressions_complete": False,
                    "refused": "--execute + a release token listing "
                               f"{PHASE!r} are required; ZERO live calls "
                               "were made"})
        print(f"[{TOOL}] run REFUSED (needs --execute + release listing "
              f"{PHASE!r}); zero live calls", file=sys.stderr)
        return 2

    transport = transport or http_call
    runner = runner or (lambda argv: G.real_runner(argv, RUNNER_TIMEOUT_S))
    try:
        expects = expect_images_from_args(args)
    except PreflightRefusal as e:
        att.finish({"tool": TOOL, "phase": PHASE, "verdict": "refused",
                    "live": True, "ok": False, "scope": SCOPE,
                    "full_old_regressions_complete": False,
                    "refused": str(e)})
        print(f"[{TOOL}] REFUSED: {e}", file=sys.stderr)
        return 2

    def fail(sid, detail, rc=None):
        detail = att.redact(str(detail))       # R1 #3: never bypass redact
        att.finish({"tool": TOOL, "phase": PHASE, "verdict": "fail",
                    "live": True, "ok": False,
                    "scope": SCOPE,
                    "full_old_regressions_complete": False,
                    "historical": historical,
                    "first_error": {"sid": sid, "rc": rc,
                                    "detail": detail}})
        print(f"[{TOOL}] FAIL-CLOSED at {sid}: {detail}", file=sys.stderr)
        return 1

    # ---- historical baseline must be green before any current claim ------
    try:
        historical, catalog, derived = load_baseline(G.P25_FINAL_DIR)
        substrate = load_substrate_facts()
        substrate_problems = check_substrate(substrate)
    except PreflightRefusal as e:
        att.record("baseline-refusal", {"mode": "run"}, str(e), 2,
                   kind="local")
        att.finish({"verdict": "refused", "ok": False, "live": True,
                    "refused": str(e)})
        print(f"[{TOOL}] REFUSED: {e}", file=sys.stderr)
        return 2
    att.record("historical-seal", {"mode": "run"},
               json.dumps(historical, sort_keys=True), 0, kind="local")
    att.record("substrate-facts", {"mode": "run"},
               json.dumps(substrate, sort_keys=True), 0, kind="local")
    if not att.gate("substrate", {"ok": not substrate_problems,
                                  "problems": substrate_problems}):
        return fail("substrate", "; ".join(substrate_problems))
    # R1 #3 (deep): register the JWT secret for scrubbing from the VERY
    # START of the run, so exception text from ANY step (even pre-JWT
    # anonymous ones) can never leak it. The value stays memory-only.
    try:
        att.add_secret(read_jwt_secret())
    except PreflightRefusal as e:
        return fail("jwt-secret", str(e))
    att.record("jwt-secret-preload",
               {"source": "q3 env OF_P1_JWT_SECRET (memory only)"},
               "secret loaded for whole-run scrub; value NEVER recorded",
               0, kind="local")

    catalog["public_views"] = public_view_baselines(derived, catalog)
    specs = build_step_specs(catalog)

    state = {"baseline_p9": derived["protected9"],
             "baseline_cp8": derived["cp8"],
             "substrate": substrate, "catalog": catalog,
             "cp_status": {}, "cp_json": {}, "runs_status": {},
             "runs_json": {}, "jwt": None, "http_records": [],
             "h14_txn_status": None, "h14_txn_json": None}

    for s in specs:
        sid = s["id"]
        if s["kind"] == "argv":
            violations = G.check_step_gates(s["argv"])
            if violations:
                return fail(sid, f"argv gate violations: {violations}")
            path = att.step_begin(sid, {"argv": G.redact_argv(
                s["argv"], att.secrets)})
            try:
                rc, raw = runner(s["argv"])
            except Exception as e:              # R1 #3: exception must not
                att.step_finish(                # lose the actual step
                    sid, path, f"runner exception: {type(e).__name__}: {e}",
                    1, meta={"purpose": s["purpose"], "exception":
                             type(e).__name__})
                return fail(sid, f"runner exception {type(e).__name__}",
                            1)
            att.step_finish(sid, path, raw, rc,
                            meta={"purpose": s["purpose"]})
            if rc != 0:
                return fail(sid, f"original rc={rc} (raw retained)", rc)
            if sid == "docker-ps":
                names = {ln.split("\t")[1] for ln in raw.splitlines()
                         if ln.count("\t") >= 3}
                gone = sorted(set(G.Q3_CONTAINERS) - names)
                if not att.gate("docker-ps", {"ok": not gone,
                                              "problems": [
                        f"q3 containers missing: {gone}"] if gone else [],
                        "q3_seen": sorted(names & set(G.Q3_CONTAINERS))}):
                    return fail(sid, f"q3 containers missing: {gone}")
            elif sid.startswith("inspect-"):
                ok, detail = evaluate_inspect(sid, s["container"], raw,
                                              catalog, expects)
                if not att.gate(sid, {"ok": ok,
                                      "problems": [detail] if detail
                                      else []}):
                    return fail(sid, detail or "inspect predicate failed")
            elif "gate" in s:
                gname, gfn = s["gate"]
                result = gfn(raw, state)
                if not att.gate(gname, result):
                    return fail(sid, "; ".join(result["problems"][:3]))
        elif s["kind"] == "http":
            auth = state["jwt"] if s.get("auth") else None
            proj = safe_url_projection(s["url"])
            path = att.step_begin(sid, {"url": proj["url"],
                                        "auth": bool(auth),
                                        "query_digest":
                                        proj["query_digest"]})
            try:
                resp = transport(s["url"], auth=auth)
            except Exception as e:              # R1 #3: cap/socket errors
                att.step_finish(               # must not lose the step
                    sid, path,
                    f"transport exception: {type(e).__name__}: {e}", 1,
                    meta={"purpose": s["purpose"], "exception":
                          type(e).__name__})
                return fail(sid, f"transport exception "
                                 f"{type(e).__name__}", 1)
            rec = {"method": "GET", **proj, "status": resp.status,
                   "body_sha256": hashlib.sha256(resp.body).hexdigest(),
                   "body_len": len(resp.body), "sent_auth": bool(auth),
                   "error": resp.error}
            # R1 #2: successful public JSON views are archived as FULL
            # safe projection objects (recomputable), not just hash/len.
            raw_out = json.dumps(rec, sort_keys=True)
            if resp.error is None and resp.status == 200 and \
                    s["id"].startswith(("view-cp-", "view-runs-",
                                        "h14-txn")):
                raw_out = (json.dumps(_try_json(resp.body, fail, sid),
                                      indent=1, sort_keys=True, default=str)
                           + "\n--- safe-response-summary ---\n"
                           + json.dumps(rec, sort_keys=True))
                raw_out = "[safe_projection: full public object archived " \
                          "for independent field compare]\n" + raw_out
            att.step_finish(sid, path, raw_out,
                            0 if resp.error is None else 1,
                            meta={"purpose": s["purpose"]})
            state["http_records"].append(rec)
            if resp.error is not None:
                return fail(sid, f"transport error: {resp.error}")
            if sid.startswith("service-healthz-"):
                svc = sid[len("service-healthz-"):]
                if not att.gate(f"service-healthz-{svc}",
                                {"ok": resp.status == 200,
                                 "problems": [] if resp.status == 200
                                 else [f"base3 {svc} /healthz "
                                       f"{resp.status} != 200"]}):
                    return fail(sid, f"base3 {svc} service /healthz "
                                     f"{resp.status} != 200")
            elif sid == "fixture-healthz":
                if not att.gate("fixture-healthz", {"ok":
                                                    resp.status == 200,
                                                    "problems": [] if
                                                    resp.status == 200 else
                                                    [f"healthz "
                                                     f"{resp.status}"]}):
                    return fail(sid, f"healthz {resp.status} != 200")
            elif sid.startswith("fixture-counter-"):
                name = s["counter"]
                got = hashlib.sha256(resp.body).hexdigest()
                ok = (resp.status == 200
                      and got == s["expect_body_sha256"])
                if not att.gate(f"counter-{name}",
                                {"ok": ok, "problems": [] if ok else [
                                    f"status {resp.status}, body sha "
                                    f"{got} != sealed "
                                    f"{s['expect_body_sha256']}"],
                                 "counts": {}}):
                    return fail(sid, f"fixture counter {name} drifted")
            elif s["id"].startswith("view-cp-"):
                did = s["def"]
                state["cp_status"][did] = resp.status
                state["cp_json"][did] = _try_json(resp.body, fail, sid)
                if state["cp_json"][did] is _FAILED:
                    return fail(sid, f"checkpoint body not JSON (status "
                                     f"{resp.status})")
            elif s["id"].startswith("view-runs-"):
                did = s["def"]
                state["runs_status"][did] = resp.status
                parsed = _try_json(resp.body, fail, sid)
                if parsed is _FAILED:
                    return fail(sid, f"runs body not JSON (status "
                                     f"{resp.status})")
                state["runs_json"][did] = parsed
                gname, gfn = s["gate"]
                result = gfn("", state)
                if not att.gate(gname, result):
                    return fail(sid, "; ".join(result["problems"][:3]))
            elif sid == "h14-txn":
                state["h14_txn_status"] = resp.status
                parsed = _try_json(resp.body, fail, sid)
                if parsed is _FAILED:
                    return fail(sid, f"txn body not JSON (status "
                                     f"{resp.status})")
                state["h14_txn_json"] = parsed
                gname, gfn = s["gate"]
                result = gfn("", state)
                if not att.gate(gname, result):
                    return fail(sid, "; ".join(result["problems"][:3]))
        elif s["kind"] == "download":
            proj = safe_url_projection(s["url"])
            path = att.step_begin(sid, {"url": proj["url"], "auth": True,
                                        "query_digest":
                                        proj["query_digest"]})
            try:
                resp = transport(s["url"], auth=state["jwt"])
            except Exception as e:
                att.step_finish(sid, path,
                                f"transport exception: "
                                f"{type(e).__name__}: {e}", 1,
                                meta={"purpose": s["purpose"],
                                      "exception": type(e).__name__})
                return fail(sid, f"transport exception "
                                 f"{type(e).__name__}", 1)
            rec = {"method": "GET", **proj, "status": resp.status,
                   "body_sha256": hashlib.sha256(resp.body).hexdigest(),
                   "body_len": len(resp.body), "sent_auth": True,
                   "error": resp.error}
            att.step_finish(sid, path, json.dumps(rec, sort_keys=True),
                            0 if resp.error is None else 1,
                            meta={"purpose": s["purpose"]})
            state["http_records"].append(rec)
            if resp.error is not None:
                return fail(sid, f"transport error: {resp.error}")
            if resp.status != 302:
                return fail(sid, f"download status {resp.status} != 302 "
                                 "(redirect shape expected)")
            location = resp.headers.get("Location", "")
            try:
                follow_url = validate_download_redirect(location)
            except PreflightRefusal as e:
                att.record(sid + "-redirect-refused",
                           {"location": safe_url_projection(location)},
                           str(e), 2, kind="local")
                return fail(sid, str(e))
            att.record(sid + "-redirect-ok",
                       {"location": safe_url_projection(location)},
                       "validated same-origin local-fs presigned redirect "
                       "(query HMAC not recorded)", 0, kind="local")
            fpath = att.step_begin(sid + "-follow",
                                   {"url": safe_url_projection(
                                       follow_url)["url"], "auth": False})
            try:
                follow = transport(follow_url, auth=None)  # ANONYMOUS
            except Exception as e:
                att.step_finish(sid + "-follow", fpath,
                                f"transport exception: "
                                f"{type(e).__name__}: {e}", 1,
                                meta={"exception": type(e).__name__})
                return fail(sid, f"follow transport exception "
                                 f"{type(e).__name__}", 1)
            frec = {"method": "GET", **safe_url_projection(follow_url),
                    "status": follow.status,
                    "body_sha256": hashlib.sha256(
                        follow.body).hexdigest(),
                    "body_len": len(follow.body), "sent_auth": False,
                    "error": follow.error}
            # R1 #2: the old artifact BYTES are archived (the sealed
            # baseline archived the same bytes; recomputable compare)
            att.step_finish(sid + "-follow", fpath,
                            "[safe_projection: artifact bytes follow]\n" +
                            follow.body.decode(errors="replace"),
                            0 if follow.error is None else 1,
                            meta=dict(frec))
            state["http_records"].append(frec)
            want_sha = catalog["h14_download_body_sha256"]
            want_len = catalog["h14_download_body_len"]
            ok = (follow.error is None and follow.status == 200
                  and frec["body_sha256"] == want_sha
                  and frec["body_len"] == want_len)
            if not att.gate("h14-download", {"ok": ok, "problems":
                                             [] if ok else [
                    f"follow status {follow.status}, sha "
                    f"{frec['body_sha256']} != sealed {want_sha}, len "
                    f"{frec['body_len']} != {want_len}"],
                    "counts": {}}):
                return fail(sid, "H14 download bytes drifted")
        elif s["kind"] == "jwt":
            try:
                secret = read_jwt_secret()
                sub, org = jwt_identity_from_baseline(derived, catalog)
                jwt = sign_testing_jwt(secret, sub=sub, org_id=org)
            except PreflightRefusal as e:
                return fail(sid, str(e))
            att.add_secret(secret)
            att.add_secret(jwt)
            state["jwt"] = jwt
            att.record(sid, {"claims": "sub/org_id derived from the sealed "
                            "rows; token value NEVER recorded"},
                       "JWT signed in memory (scrubbed from evidence)",
                       0, kind="local")
        else:
            return fail(sid, f"unknown step kind {s['kind']!r} — refusing "
                             "(a step must never be silently skipped)")

    result = {"tool": TOOL, "phase": PHASE, "live": True, "ok": True,
              "verdict": "pass", "scope": SCOPE,
              "full_old_regressions_complete": False,
              "image_expectations": {
                  "switched_services": {k: v for k, v in expects.items()},
                  "strict_old_pins_for": sorted(
                      {"dataset", "pipeline", "connector"} - set(expects)
                  ) + ["fixture", "postgres"]},
              "historical": historical,
              "current": {"steps_recorded": att.n,
                          "http_requests": len(state["http_records"]),
                          "comparators": "exact whole-set + per-field "
                                         "(gate files alongside steps)"},
              "deferred": {"build_job_all_history":
                           "NOT part of this core slice; explicitly "
                           "deferred to the final old P1/P2 HTTP "
                           "regressions (Root will run them before any "
                           "complete claim) — never skipped to green"},
              "attempt_dir": str(att.dir)}
    att.finish(result)
    print(f"[{TOOL}] run PASS -> {att.dir / 'RESULT.json'}")
    return 0


_FAILED = object()


def _try_json(body, fail, sid):
    try:
        return json.loads(body)
    except ValueError:
        return _FAILED


# ===========================================================================
def main(argv=None):
    ap = argparse.ArgumentParser(prog=TOOL)
    ap.add_argument("mode", choices=["plan", "run"])
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--release-file", default=G.P26_RELEASE_FILE)
    ap.add_argument("--out-dir", default=str(RESULTS_W8))
    ap.add_argument("--expect-dataset-image", default=None,
                    help="post-switch ONLY: new dataset full image "
                         "sha256:<64hex> (Root release binds it); old "
                         "StartedAt pin dropped for this service alone")
    ap.add_argument("--expect-connector-image", default=None,
                    help="post-switch ONLY: new connector full image "
                         "sha256:<64hex> (Root release binds it)")
    args = ap.parse_args(argv)
    try:
        if args.mode == "plan":
            return cmd_plan(args)
        return cmd_run(args)
    except PreflightRefusal as e:
        print(f"[{TOOL}] REFUSED: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

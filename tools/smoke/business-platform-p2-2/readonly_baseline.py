#!/usr/bin/env python3
"""READ-ONLY pre-release baseline collector for P2.2 (2026-11-14).

Authorized scope (root, 2026-11-14): observe the RUNNING q3v9 stack and
archive a baseline for the final acceptance — nothing here starts,
stops, restarts, builds, or writes service data. Every step records the
REAL command outcome; read-only failures are recorded as-is and never
worked around by starting anything. No secret ever reaches the output:
.env is stat'ed and its KEY NAMES listed only; errors pass through the
P1 scrub registry; JWTs are minted in-memory exactly like every smoke
identity.

Baseline content:
  1. stack ps (Name/State/Health/Image per service, whitelisted keys)
  2. .env permission mode + key names (never values)
  3. P21 legacy B/C fixed-UUID whitelist projections (13 safe columns)
  4. B/C parent chain (root-pinned JOIN; public ownership columns only)
  5. B/C fixture src counters (request/inventory/active)
  6. B/C targets LIVE via the datasets API as their REAL source owners
     (minted sub/org from the chain): dataset GET, file list, downloads
     asserted HTTP-200 BEFORE hashing, txn status map
  7. the archived P1 old artifact (suffix 8582e649 handles from the P21
     final archive): LIVE dataset/file/txn GETs + sha256 compared to the
     ARCHIVED pre-restart hash — the old bytes themselves, never a
     re-uploaded stand-in
  8. the archived old Builds/Jobs (three domains): LIVE build/job GETs

Usage: python3 -I readonly_baseline.py [--env-dir <tmp>]
Exit 0 iff every step recorded ok=True (failures are still archived).
"""

import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = Path(__file__).resolve().parent
P1_SMOKE = HERE.parent / "business-platform-p1" / "smoke" / "smoke.py"
P21_ARCHIVE = HERE.parent / "business-platform-p2-1" / "results" / "2026-11-13-p21-final"
PROJECT = "openfoundry-platform-p1-q3v9"

P21_LEGACY_RUNS = {
    "B_lease_lost_unpublished": "6d6dc56f-1498-4b13-871e-bab489e8b631",
    "C_committed_unconfirmed": "4ff175b3-65d7-47b6-9a83-d46e03bbfe23",
}
P21_SUFFIX = "b6ec49ab"
PROJECTION_SQL = (
    "SELECT status, (lease_holder IS NOT NULL), COALESCE(heartbeat_at::text, ''), "
    "COALESCE(lease_expires_at::text, ''), (lease_token IS NOT NULL), "
    "(cancel_requested_at IS NOT NULL), (publish_entered_at IS NOT NULL), "
    "COALESCE(control_mode, ''), COALESCE(attempt::text, ''), "
    "COALESCE(generation::text, ''), (content_hash IS NOT NULL), "
    "(dataset_version_id IS NOT NULL), COALESCE(finished_at::text, '') "
    "FROM sync_runs WHERE id = '{rid}'::uuid;")
PROJECTION_KEYS = ("status", "has_holder", "heartbeat", "expires",
                   "has_token_col", "cancel_requested", "gate_entered",
                   "control_mode", "attempt", "generation",
                   "has_content_hash", "has_version", "finished_at")

ARGS = None
M = None
STEPS = []


def record(name, ok, detail):
    STEPS.append({"step": name, "ok": bool(ok), "detail": detail})
    print(f"[baseline] {'ok  ' if ok else 'FAIL'} {name}: "
          f"{M.scrub(str(detail))[:220]}", flush=True)


def http_json(url, token=None, timeout=10):
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, json.loads(r.read() or b"null")


def psql(sql, database):
    """Read-only psql via the stack's postgres container (P1 helper)."""
    return M.psql(sql, database=database)


# ------------------------------------------------------------------ steps --
def step_stack_ps(env_dir):
    proc = subprocess.run(
        ["docker", "compose", "-f", str(env_dir / "compose.yaml"),
         "--project-name", PROJECT, "ps", "--format", "json"],
        capture_output=True, text=True, timeout=60)
    if proc.returncode != 0:
        record("stack_ps", False, {"rc": proc.returncode,
                                   "stderr": M.scrub(proc.stderr)[:200]})
        return {}
    services = {}
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            d = json.loads(line)
        except ValueError:
            continue
        services[d.get("Name", "?")] = {"state": d.get("State"),
                                        "health": d.get("Health"),
                                        "image": d.get("Image")}
    record("stack_ps", bool(services), services)
    return services


def step_env_guard(env_dir):
    env_file = env_dir / ".env"
    if not env_file.exists():
        record("env_guard", False, {"missing": str(env_file)})
        return None
    mode = oct(os.stat(env_file).st_mode & 0o777)
    keys = []
    for line in env_file.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            keys.append(line.split("=", 1)[0])  # NAMES only, never values
    ok = mode == "0o600" and keys
    record("env_guard", ok, {"mode": mode, "key_names": sorted(keys)})
    return {"mode": mode, "keys": keys}


FALLBACK_PROJECTION_SQL = (
    "SELECT status, (lease_holder IS NOT NULL), COALESCE(heartbeat_at::text, ''), "
    "COALESCE(lease_expires_at::text, ''), (lease_token IS NOT NULL), "
    "COALESCE(attempt::text, ''), COALESCE(generation::text, ''), "
    "(content_hash IS NOT NULL), (dataset_version_id IS NOT NULL), "
    "COALESCE(finished_at::text, '') FROM sync_runs WHERE id = '{rid}'::uuid;")
FALLBACK_KEYS = ("status", "has_holder", "heartbeat", "expires", "has_token_col",
                 "attempt", "generation", "has_content_hash", "has_version",
                 "finished_at")


def step_legacy_projections():
    """Full 13-column projection when the P2.2 columns exist; otherwise
    the pre-P2.2 10-column fallback, with the variant RECORDED — the
    running stack may still be the pre-P2.2 build and that fact is the
    baseline, never something to paper over."""
    out = {"variant": None}
    ok_all = True
    try:
        raw = psql(PROJECTION_SQL.format(rid=P21_LEGACY_RUNS["B_lease_lost_unpublished"]),
                   "connectors")
        if len(raw.split("|")) != 13:
            raise ValueError("unexpected shape")
        out["variant"] = "p22-full-13col"
        for label, rid in P21_LEGACY_RUNS.items():
            out[label] = dict(zip(PROJECTION_KEYS,
                                  psql(PROJECTION_SQL.format(rid=rid),
                                       "connectors").split("|")))
    except Exception:
        out["variant"] = "pre-p22-fallback-10col"
        for label, rid in P21_LEGACY_RUNS.items():
            try:
                out[label] = dict(zip(FALLBACK_KEYS,
                                      psql(FALLBACK_PROJECTION_SQL.format(rid=rid),
                                           "connectors").split("|")))
            except Exception as exc:
                ok_all = False
                out[label] = {"error": M.scrub(str(exc))[:200]}
    record("p21_bc_projection", ok_all, out)
    return out


def step_legacy_chain():
    """Root-pinned JOIN; public ownership columns + target ids only."""
    out = {}
    ok_all = True
    target_cols = psql(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'batch_sync_defs' AND (column_name LIKE '%dataset%' "
        "OR column_name LIKE '%output%' OR column_name LIKE '%target%') "
        "ORDER BY column_name;", "connectors").split()
    for label, rid in P21_LEGACY_RUNS.items():
        try:
            row = psql(
                "SELECT COALESCE(r.org_id::text, ''), COALESCE(r.owner_id::text, ''), "
                "COALESCE(r.sync_def_id::text, ''), COALESCE(c.owner_id::text, ''), "
                "COALESCE(c.org_id::text, '') "
                "FROM sync_runs r "
                "JOIN batch_sync_defs d ON d.id = r.sync_def_id "
                "JOIN connections c ON c.id = d.source_id "
                f"WHERE r.id = '{rid}'::uuid;", "connectors")
            parts = row.split("|")
            if len(parts) != 5 or any(len(p) != 36 for p in parts[:5]):
                raise ValueError("chain row not uuid-shaped")
            entry = dict(zip(("run_org", "run_owner", "sync_def",
                              "source_owner", "source_org"), parts))
            for col in target_cols:
                value = psql(f"SELECT COALESCE({col}::text, '') FROM batch_sync_defs "
                             f"WHERE id = '{entry['sync_def']}'::uuid;", "connectors").strip()
                if len(value) == 36:
                    entry[f"target_via_{col}"] = value
            out[label] = entry
        except Exception as exc:
            ok_all = False
            out[label] = {"error": M.scrub(str(exc))[:200]}
    record("p21_bc_chain", ok_all and bool(target_cols),
           {"target_column_candidates": target_cols, "rows": out})
    return out


def step_fixture_counts(fixture_up):
    out = {}
    ok_all = True
    if not fixture_up:
        record("p21_bc_fixture_counts", False, {"skipped": "fixture not running"})
        return out
    for name in (f"p21a-{P21_SUFFIX}", f"p21b-{P21_SUFFIX}", f"p21c-{P21_SUFFIX}"):
        try:
            status, snap = http_json(f"{M.BASES['fixture']}/admin/src/{name}")
            keep = {k: snap.get(k) for k in ("request_count", "inventory_started",
                                             "inventory_finished", "active",
                                             "total", "mode")}
            out[name] = {"status": status, **keep}
        except Exception as exc:
            ok_all = False
            out[name] = {"error": M.scrub(str(exc))[:150]}
    record("p21_bc_fixture_counts", ok_all, out)
    return out


def _live_target(token, dataset_id, label, out):
    status, body = http_json(
        f"{M.BASES['dataset']}/v1/datasets/{dataset_id}", token=token)
    out[label]["dataset_get"] = {"status": status}
    status, files = http_json(
        f"{M.BASES['dataset']}/v1/datasets/{dataset_id}/files", token=token)
    file_list = files.get("files", files if isinstance(files, list) else [])
    out[label]["files_count"] = len(file_list)
    for f in file_list:
        fid = f.get("id")
        url = f"{M.BASES['dataset']}/v1/datasets/{dataset_id}/files/{fid}/download"
        req = urllib.request.Request(url, headers={"Authorization": "Bearer " + token})
        with urllib.request.urlopen(req, timeout=30) as r:
            dl_status, blob = r.status, r.read()
        digest = hashlib.sha256(blob).hexdigest()
        entry = {"download_status": dl_status, "bytes": len(blob), "sha256": digest}
        txn_id = f.get("transaction_id")
        if txn_id:
            tstatus, txn = http_json(
                f"{M.BASES['dataset']}/v1/datasets/{dataset_id}/branches/main/"
                f"transactions/{txn_id}", token=token)
            entry["txn_status"] = (txn or {}).get("status")
            entry["txn_content_hash"] = ((txn or {}).get("metadata") or {}).get("content_hash")
        out[label][f"file_{fid}"] = entry


def step_legacy_targets_live(chain, dataset_up):
    out = {}
    ok_all = True
    if not dataset_up:
        record("p21_bc_targets_live", False, {"skipped": "dataset not running"})
        return out
    for label, entry in chain.items():
        out[label] = {}
        if not isinstance(entry, dict) or "error" in entry:
            ok_all = False
            out[label]["error"] = "chain unresolved"
            continue
        target = next((v for k, v in entry.items()
                       if k.startswith("target_via_")), None)
        if not target:
            ok_all = False
            out[label]["error"] = "no uuid target id on the sync def"
            continue
        out[label]["target_dataset_id"] = target
        try:
            token = M.mint_token(sub=entry["source_owner"], org=entry["source_org"])
            _live_target(token, target, label, out)
        except Exception as exc:
            ok_all = False
            out[label]["error"] = M.scrub(str(exc))[:200]
    record("p21_bc_targets_live", ok_all, out)
    return out


def step_p1_old_artifact(dataset_up):
    handles = json.loads((P21_ARCHIVE / "logs" / "restart-persistence-probe.json")
                         .read_text())
    ids = json.loads((P21_ARCHIVE / "logs" / "p21-phase-identities.json").read_text())
    out = {"archived": {k: handles.get(k) for k in
                        ("dataset_id", "transaction_id", "file_id",
                         "pre_restart_content_hash")}}
    if not dataset_up:
        record("p1_old_artifact_live", False, {"skipped": "dataset not running"})
        return out
    try:
        user_a = ids["user_a"]
        token = M.mint_token(sub=user_a["sub"], org=user_a["org"])
        out["live"] = {}
        _live_target(token, handles["dataset_id"], "live", out)
        live = out["live"].get(f"file_{handles['file_id']}", {})
        out["sha256_matches_archive"] = (
            live.get("sha256") == handles["pre_restart_content_hash"])
        out["txn_hash_matches_archive"] = (
            live.get("txn_content_hash") == handles["pre_restart_content_hash"])
        record("p1_old_artifact_live",
               out["sha256_matches_archive"] and out["txn_hash_matches_archive"]
               and live.get("download_status") == 200, out)
    except Exception as exc:
        record("p1_old_artifact_live", False,
               {"error": M.scrub(str(exc))[:200], "partial": out})
    return out


def step_p1_old_buildjob(pipeline_up):
    phase = json.loads((P21_ARCHIVE / "logs" / "p21-phase-result-p1.json").read_text())
    ids = json.loads((P21_ARCHIVE / "logs" / "p21-phase-identities.json").read_text())
    builds = phase["evidence"]["build_positive"]
    out = {}
    ok_all = True
    if not pipeline_up:
        record("p1_old_buildjob_live", False, {"skipped": "pipeline not running"})
        return out
    domains = (("org_a", "user_a"), ("org_b", "user_b"), ("legacy", "legacy"))
    for domain, ident_key in domains:
        entry = builds.get(domain) or {}
        rid = entry.get("build_rid")
        job_rid = entry.get("job_rid")
        if not rid or not job_rid:
            ok_all = False
            out[domain] = {"error": "no archived build/job rid"}
            continue
        try:
            who = ids[ident_key]
            token = M.mint_token(sub=who["sub"], org=who["org"])
            status_build, build = http_json(
                f"{M.BASES['pipeline']}/v1/builds/{rid}", token=token)
            # job METADATA probe: parent Build binding (uuid + rid) via the
            # same /api/v1/jobs/{rid} route the P1 probe used
            status_meta, job = http_json(
                f"{M.BASES['pipeline']}/api/v1/jobs/{job_rid}", token=token)
            blob = job if isinstance(job, dict) else {}
            parent = blob.get("build_id") or (blob.get("build") or {}).get("id")
            spec_rid = blob.get("spec_rid") or blob.get("job_spec_rid")
            binding = {
                "job_meta_get": status_meta,
                "parent_build_matches": str(parent) == str(entry.get("build_id")),
                "job_spec_rid_present": bool(spec_rid),
            }
            status_job, _outputs = http_json(
                f"{M.BASES['pipeline']}/v1/jobs/{job_rid}/outputs", token=token)
            # identity-binding probe: ANOTHER domain's token must be denied
            wrong = {"org_a": "user_b", "org_b": "user_a",
                     "legacy": "user_a"}[domain]
            other = ids[wrong]
            wrong_token = M.mint_token(sub=other["sub"], org=other["org"])
            try:
                wrong_status, _ = http_json(
                    f"{M.BASES['pipeline']}/api/v1/jobs/{job_rid}",
                    token=wrong_token)
            except urllib.error.HTTPError as exc:
                wrong_status = exc.code
            out[domain] = {"build_get": status_build,
                           "build_state": (build or {}).get("state")
                           if isinstance(build, dict) else None,
                           **binding,
                           "job_outputs_get": status_job,
                           "cross_domain_job_get": wrong_status}
            good = (status_build == 200 and status_job == 200
                    and status_meta == 200 and binding["parent_build_matches"]
                    and binding["job_spec_rid_present"]
                    and wrong_status in (403, 404))
            if not good:
                ok_all = False
        except Exception as exc:
            ok_all = False
            out[domain] = {"error": M.scrub(str(exc))[:200]}
    record("p1_old_buildjob_live", ok_all, out)
    return out


# ------------------------------------------------------------------- main --
def main():
    global ARGS, M
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-dir", default="/private/tmp/openfoundry-platform-p1-q3v9")
    ARGS = parser.parse_args()
    env_dir = Path(ARGS.env_dir)

    spec = importlib.util.spec_from_file_location("p1smoke", P1_SMOKE)
    M = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(M)
    M.ARGS = type("A", (), {"env_dir": str(env_dir)})()
    M.load_env(env_dir)  # in-memory only; nothing printed

    stamp = time.strftime("%Y-%m-%dT%H%M%S")
    out_dir = HERE / "results" / "preparation" / f"{stamp}-readonly-baseline"
    out_dir.mkdir(parents=True, exist_ok=True)

    services = step_stack_ps(env_dir)
    expected = ("postgres", "dataset", "pipeline", "fixture")
    up = {svc: any(name.endswith(f"-{svc}-1") and info.get("state") == "running"
                   for name, info in services.items())
          for svc in expected}
    # strict: the EXPECTED service set must exist AND every one running
    record("service_up_map", bool(up) and all(up.values()),
           {"expected": list(expected), **up})

    step_env_guard(env_dir)
    if up["postgres"]:
        step_legacy_projections()
        chain = step_legacy_chain()
    else:
        record("p21_bc_projection", False, {"skipped": "postgres not running"})
        record("p21_bc_chain", False, {"skipped": "postgres not running"})
        chain = {}
    step_fixture_counts(up["fixture"])
    step_legacy_targets_live(chain, up["dataset"])
    step_p1_old_artifact(up["dataset"])
    step_p1_old_buildjob(up["pipeline"])

    manifest = {"client_date": time.strftime("%Y-%m-%d"),
                "note": "read-only pre-release baseline; no service was "
                        "started/stopped/restarted/built by this collector",
                "steps": STEPS,
                "all_ok": all(s["ok"] for s in STEPS)}
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"\n[baseline] all_ok={manifest['all_ok']} -> {out_dir}", flush=True)
    return 0 if manifest["all_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Value-level before/after snapshots for the P2.3 source-admission
acceptance (2026-11-14; review-hardened same day).

Why this exists: the P2.2 I/M scenarios proved their per-member
invariants only as REVIEWED runtime assertions; result_p22.json kept
summaries, so the layer could not be independently recomputed. For P2.3
the occupancy/interval invariants get RAW value-level snapshots taken
live at scenario time and archived in pairs
(before-<base>/after-<base>/compare-<base>), so an independent reader
can recompute every verdict from the archived values.

Hard rules (root, 2026-11-14, review batches 1+3):
  * Snapshots are ONLY ever taken live. Nothing is backfilled; a value
    that could not be read is a collection FAILURE, never a zero/empty
    that would compare equal on both sides.
  * The snapshot embeds a SANITIZED handle echo — IDs, sync->source
    bindings and expected counts ONLY. Owner tokens passed for target
    probing stay in memory and are NEVER written to any file.
  * compare() is STRICT: it validates the FULL handle universe on BOTH
    sides (every expected source/sync/run/target present, no extras,
    correct types, entry counts matching the echo, sync->source run
    binding consistent). A key missing on either side fails; JSON null
    fails EXCEPT sources.<id>.stamp where an explicit null (never
    admitted) is a legal value; changed values outside the caller's
    allow-list fail. compare({},{}) FAILS — an empty pair is never ok.
  * Targets: files come from a STRICT list envelope (a real list of
    objects with unique non-empty ids — an HTTP 200 with files={} or ""
    is a FAILURE, never an empty iteration). Transactions are enumerated
    INDEPENDENTLY of files — the FULL Dataset transaction set via the
    strict models.Page {data, next_cursor, has_more} envelope with
    bounded cursor following — so OPEN/empty txns without files cannot
    be missed. A required target without a valid owner token FAILS
    collection (never a skip that compares 0==0).
  * Sources: existence is verified (a nonexistent source id is a
    collection failure, not stamp=''/0/[]).
  * No secrets: only whitelisted public projection columns plus
    connections.sync_last_admitted_at (a PRIVATE timestamp read ONLY
    here as a safe text column — never expected in any HTTP response).
    No lease token/holder values, no credentials, no full config bodies.

CLI:
  python3 -I admission_snapshots.py collect --env-dir <tmp> \
      --handles handles.json --out before.json
  python3 -I admission_snapshots.py compare --before b.json --after a.json \
      --out c.json [--allow sources.<id>.inflight]

Exit 0 iff collect recorded no failures / compare ok.
"""

import argparse
import importlib.util
import json
import sys
import time
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
P1_SMOKE = HERE.parent / "business-platform-p1" / "smoke" / "smoke.py"

RUN_PROJECTION_SQL = (
    "SELECT status, (lease_holder IS NOT NULL), COALESCE(heartbeat_at::text, ''), "
    "COALESCE(lease_expires_at::text, ''), (lease_token IS NOT NULL), "
    "(cancel_requested_at IS NOT NULL), (publish_entered_at IS NOT NULL), "
    "COALESCE(control_mode, ''), COALESCE(attempt::text, ''), "
    "COALESCE(generation::text, ''), (content_hash IS NOT NULL), "
    "(dataset_version_id IS NOT NULL), COALESCE(finished_at::text, '') "
    "FROM sync_runs WHERE id = '{rid}'::uuid;")
RUN_PROJECTION_KEYS = ("status", "has_holder", "heartbeat", "expires",
                       "has_token_col", "cancel_requested", "gate_entered",
                       "control_mode", "attempt", "generation",
                       "has_content_hash", "has_version", "finished_at")

SOURCE_KEYS = ("stamp", "inflight", "run_ids")
FIXTURE_KEYS = ("request_count", "inventory_started", "inventory_finished",
                "active", "barrier_waiters")
TARGET_KEYS = ("file_ids", "txn_status")

STAMP_COLUMN = "sync_last_admitted_at"  # connections (PRIVATE: PG-only read)
TXN_MAX_PAGES = 10
# Transaction status vocabulary — ROOT-PINNED (closeout batch-4): the
# CURRENT runtime model carries exactly OPEN/COMMITTED/ABORTED (FAILED
# does not exist and was removed); an unknown status fails collection
# fail-closed and surfaces at the first formal run.
TXN_STATUS_VOCAB = ("OPEN", "COMMITTED", "ABORTED")


# ----------------------------------------------------- strict pure parsers
def parse_files_envelope(body):
    """STRICT files-list envelope: {"files": [ {id: non-empty}, ... ]}.
    Anything else — missing key, non-list (dict/string), non-object
    items, missing/duplicate ids — raises ValueError."""
    if not isinstance(body, dict):
        raise ValueError(f"files body not an object ({type(body).__name__})")
    if "files" not in body:
        raise ValueError("files body missing the 'files' key")
    files = body["files"]
    if not isinstance(files, list):
        raise ValueError(f"'files' is not a list ({type(files).__name__})")
    ids = []
    for i, f in enumerate(files):
        if not isinstance(f, dict):
            raise ValueError(f"files[{i}] not an object")
        fid = f.get("id")
        if not isinstance(fid, str) or not fid:
            raise ValueError(f"files[{i}] missing a non-empty id")
        ids.append(fid)
    if len(set(ids)) != len(ids):
        raise ValueError("duplicate file ids in the list")
    return files


def parse_txns_page(body):
    """STRICT models.Page {data, next_cursor?, has_more} (dataset
    transactions.go envelope). Returns (items, next_cursor, has_more).
    data null -> legal empty; has_more true without a cursor raises;
    every item id must be a non-empty STRING (batch-A)."""
    if not isinstance(body, dict):
        raise ValueError(f"txns body not an object ({type(body).__name__})")
    for key in ("data", "has_more"):
        if key not in body:
            raise ValueError(f"txns envelope missing '{key}' — refusing to "
                             "default an unknown envelope to zero")
    data = body["data"]
    if data is None:                     # legal nil slice (no omitempty)
        data = []
    if not isinstance(data, list):
        raise ValueError(f"txns data not a list ({type(data).__name__})")
    for i, t in enumerate(data):
        if not isinstance(t, dict) or not isinstance(t.get("id"), str) \
                or not t["id"]:
            raise ValueError(f"txns data[{i}] id not a non-empty string")
    has_more = body["has_more"]
    if not isinstance(has_more, bool):
        raise ValueError("txns has_more not a bool")
    cursor = body.get("next_cursor")
    if has_more and (not isinstance(cursor, str) or not cursor):
        raise ValueError("has_more=true without a next_cursor")
    return data, cursor, has_more


def enumerate_target_txns(fetch, max_pages=TXN_MAX_PAGES):
    """Full transaction set via strict pages. `fetch(cursor_or_None)`
    must return (status, body); non-200 raises. Follows next_cursor
    until has_more=false; exceeding max_pages raises (never truncates).
    Batch-A: duplicate txn ids FAIL (never silently overwrite) and a
    status outside the pinned vocabulary FAILS (never defaults)."""
    out, cursor, pages = {}, None, 0
    while True:
        pages += 1
        if pages > max_pages:
            raise ValueError(f"txn pagination exceeded {max_pages} pages")
        status, body = fetch(cursor)
        if status != 200:
            raise ValueError(f"transactions list status {status}")
        items, cursor, has_more = parse_txns_page(body)
        for t in items:
            tid = t["id"]
            if tid in out:
                raise ValueError(f"duplicate txn id across pages: {tid}")
            txn_status = t.get("status")
            if not isinstance(txn_status, str) or not txn_status:
                raise ValueError(f"txn {tid} status not a non-empty string")
            txn_status = txn_status.upper()
            if txn_status not in TXN_STATUS_VOCAB:
                raise ValueError(
                    f"txn {tid} status {txn_status!r} outside the pinned "
                    f"vocabulary {TXN_STATUS_VOCAB} — root must pin the "
                    "business value (fail-closed, never defaulted)")
            out[tid] = txn_status
        if not has_more:
            return out


# ------------------------------------------------------------- collection
def _psql(M, sql):
    return M.psql(sql, database="connectors")


def _fixture_json(M, src):
    url = f"{M.BASES['fixture']}/admin/src/{src}"
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read() or "null")


def _strict_files(M, token, dataset_id):
    url = f"{M.BASES['dataset']}/v1/datasets/{dataset_id}/files"
    req = urllib.request.Request(url, headers={
        "Authorization": "Bearer " + token})
    with urllib.request.urlopen(req, timeout=30) as r:
        status, raw = r.status, r.read()
    body = json.loads(raw or b"null")
    files = parse_files_envelope(body)
    return [str(f["id"]) for f in files]


def sanitize_handles(handles):
    """Whitelisted echo ONLY: ids, sync->source/target bindings, expected
    counts. Owner tokens must never be persisted (batch-3 rule)."""
    return {
        "sources": [{"id": s["id"], "src": s.get("src")}
                    for s in handles.get("sources", [])],
        "syncs": [{"id": s["id"], "source_id": s["source_id"],
                   "target_id": s["target_id"]}
                  for s in handles.get("syncs", [])],
        "runs": list(handles.get("runs", [])),
        "targets": [t["dataset_id"] for t in handles.get("targets", [])],
    }


def collect(M, handles):
    """Take ONE live snapshot. Every read failure lands in
    collection_failures and makes the snapshot unusable for a green
    compare (compare refuses snapshots carrying failures)."""
    out = {"taken_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
           "handles": sanitize_handles(handles),
           "collection_failures": [],
           "sources": {}, "runs": {}, "syncs": {}, "fixture": {},
           "targets": {}}

    def fail(where, detail):
        out["collection_failures"].append({"where": where,
                                           "detail": str(detail)[:200]})

    for s in handles.get("sources", []):
        sid, src = s["id"], s.get("src")
        try:
            exists = _psql(
                M, f"SELECT count(*) FROM connections "
                   f"WHERE id = '{sid}'::uuid;").strip()
            if exists != "1":
                raise ValueError(f"source row count {exists!r} (absent?)")
            null_flag, value = _psql(
                M, f"SELECT ({STAMP_COLUMN} IS NULL), "
                   f"COALESCE({STAMP_COLUMN}::text, '') "
                   f"FROM connections WHERE id = '{sid}'::uuid;").strip() \
                .split("|", 1)
            stamp = None if null_flag == "t" else value
            inflight_raw = _psql(
                M, "SELECT count(*) FROM sync_runs r "
                   "JOIN batch_sync_defs d ON d.id = r.sync_def_id "
                   f"WHERE d.source_id = '{sid}'::uuid "
                   "AND r.status IN ('pending','running');").strip()
            if not inflight_raw.isdigit():
                raise ValueError(f"inflight not numeric: {inflight_raw!r}")
            agg = _psql(
                M, "SELECT COALESCE(string_agg(r.id::text, ',' "
                   "ORDER BY r.id::text), '') FROM sync_runs r "
                   "JOIN batch_sync_defs d ON d.id = r.sync_def_id "
                   f"WHERE d.source_id = '{sid}'::uuid;").strip()
            out["sources"][sid] = {
                "stamp": stamp,
                "inflight": int(inflight_raw),
                "run_ids": sorted(x for x in agg.split(",") if x)}
        except Exception as exc:  # noqa: BLE001 — recorded, never swallowed
            fail(f"sources.{sid}", M.scrub(str(exc)))
        if src:
            try:
                snap = _fixture_json(M, src)
                entry = {k: snap.get(k) for k in FIXTURE_KEYS}
                if any(v is None for v in entry.values()):
                    raise ValueError(f"fixture key missing in {src}")
                if not all(isinstance(v, int) for v in entry.values()):
                    raise ValueError(f"fixture counters not ints in {src}")
                out["fixture"][src] = entry
            except Exception as exc:  # noqa: BLE001
                fail(f"fixture.{src}", M.scrub(str(exc)))

    for rid in handles.get("runs", []):
        try:
            raw = _psql(M, RUN_PROJECTION_SQL.format(rid=rid))
            parts = raw.split("|")
            if len(parts) != len(RUN_PROJECTION_KEYS):
                raise ValueError(f"projection shape {len(parts)}")
            out["runs"][rid] = dict(zip(RUN_PROJECTION_KEYS, parts))
        except Exception as exc:  # noqa: BLE001
            fail(f"runs.{rid}", M.scrub(str(exc)))

    for sy in handles.get("syncs", []):
        try:
            # batch-A: the sync row must EXIST and its real
            # source_id / output_dataset_id must match the expected
            # bindings (an unrelated row is a collection failure)
            row = _psql(
                M, "SELECT source_id::text, output_dataset_id::text "
                   f"FROM batch_sync_defs WHERE id = '{sy['id']}'::uuid;"
            ).strip()
            parts = row.split("|")
            if len(parts) != 2 or not all(len(p) == 36 for p in parts):
                raise ValueError(f"sync row absent/ill-shaped: {row[:80]!r}")
            if parts[0] != sy["source_id"]:
                raise ValueError(
                    f"sync source_id mismatch: db={parts[0]} "
                    f"expected={sy['source_id']}")
            if parts[1] != sy["target_id"]:
                raise ValueError(
                    f"sync output_dataset_id mismatch: db={parts[1]} "
                    f"expected={sy['target_id']}")
            agg = _psql(
                M, "SELECT COALESCE(string_agg(id::text, ',' "
                   "ORDER BY id::text), '') FROM sync_runs "
                   f"WHERE sync_def_id = '{sy['id']}'::uuid;").strip()
            out["syncs"][sy["id"]] = sorted(x for x in agg.split(",") if x)
        except Exception as exc:  # noqa: BLE001
            fail(f"syncs.{sy['id']}", M.scrub(str(exc)))

    handled_sync_ids = {sy["id"] for sy in handles.get("syncs", [])}
    for rid in handles.get("runs", []):
        # batch-A: a handled run must really belong to one of the
        # handled syncs (an unrelated run id is a collection failure —
        # identical-on-both-sides can never make it green)
        try:
            owner_sync = _psql(
                M, "SELECT sync_def_id::text FROM sync_runs "
                   f"WHERE id = '{rid}'::uuid;").strip()
            if owner_sync not in handled_sync_ids:
                raise ValueError(
                    f"run bound to sync outside the handled universe: "
                    f"{owner_sync[:40]}")
        except Exception as exc:  # noqa: BLE001
            fail(f"runs.{rid}.binding", M.scrub(str(exc)))

    # batch-A: every sync's target must be inside the handled target
    # universe (no un-probed targets smuggled in via syncs)
    target_ids = {t["dataset_id"] for t in handles.get("targets", [])}
    for sy in handles.get("syncs", []):
        if sy["target_id"] not in target_ids:
            fail(f"syncs.{sy['id']}.target_universe",
                 f"sync target {sy['target_id']} not in handled targets")

    # Targets REQUIRE a valid owner token in memory (a required target
    # without one FAILS — never a skip that would compare 0==0).
    for t in handles.get("targets", []):
        tid, token = t["dataset_id"], t.get("token")
        try:
            if not token:
                raise ValueError("required target has no owner token")
            file_ids = _strict_files(M, token, tid)

            def fetch(cursor, _tid=tid, _tok=token):
                url = f"{M.BASES['dataset']}/v1/datasets/{_tid}/transactions"
                if cursor:
                    url += f"?cursor={cursor}"
                req = urllib.request.Request(url, headers={
                    "Authorization": "Bearer " + _tok})
                with urllib.request.urlopen(req, timeout=30) as r:
                    body = json.loads(r.read() or b"null")
                return r.status, body

            txns = enumerate_target_txns(fetch)
            out["targets"][tid] = {"file_ids": sorted(file_ids),
                                   "txn_status": txns}
        except Exception as exc:  # noqa: BLE001
            fail(f"targets.{tid}", M.scrub(str(exc)))
    return out


# ------------------------------------------------------------- comparison
def _handle_universe(snap, label, violations):
    h = snap.get("handles")
    if not isinstance(h, dict) or not h:
        violations.append(f"{label}: snapshot carries no sanitized handles")
        return None
    return h


def _check_types(section, key, value, problems, where):
    if section == "sources":
        if not isinstance(value, dict):
            problems.append(f"{where}: not an object")
            return
        for k in SOURCE_KEYS:
            if k not in value:
                problems.append(f"{where}.{k}: missing key")
        if not (value.get("inflight") is None
                or isinstance(value.get("inflight"), int)):
            problems.append(f"{where}.inflight: not an int")
        if not isinstance(value.get("run_ids"), list):
            problems.append(f"{where}.run_ids: not a list")
    elif section == "runs":
        if not isinstance(value, dict):
            problems.append(f"{where}: not an object")
            return
        for k in RUN_PROJECTION_KEYS:
            if k not in value:
                problems.append(f"{where}.{k}: missing projection key")
            elif value[k] is None:
                problems.append(f"{where}.{k}: null projection value")
    elif section == "fixture":
        if not isinstance(value, dict):
            problems.append(f"{where}: not an object")
            return
        for k in FIXTURE_KEYS:
            if not isinstance(value.get(k), int):
                problems.append(f"{where}.{k}: not an int")
    elif section == "targets":
        if not isinstance(value, dict):
            problems.append(f"{where}: not an object")
            return
        for k in TARGET_KEYS:
            if k not in value:
                problems.append(f"{where}.{k}: missing key")
        if not isinstance(value.get("file_ids"), list):
            problems.append(f"{where}.file_ids: not a list")
        if not isinstance(value.get("txn_status"), dict):
            problems.append(f"{where}.txn_status: not an object")


def compare(before, after, allowed=()):
    """STRICT compare against the sanitized handle echo on both sides:
    full universe presence, entry counts, types, bindings, then values.
    Null is illegal everywhere EXCEPT sources.<id>.stamp (explicit
    never-admitted null). Changed values must be explicitly allowed."""
    allowed = set(allowed)
    violations, changes = [], {}

    hb = _handle_universe(before, "before", violations)
    ha = _handle_universe(after, "after", violations)
    if hb and ha:
        if hb != ha:
            violations.append("handle universes differ between sides")

    expect = {
        "sources": [s["id"] for s in (hb or {}).get("sources", [])],
        "runs": list((hb or {}).get("runs", [])),
        "syncs": [s["id"] for s in (hb or {}).get("syncs", [])],
        "targets": list((hb or {}).get("targets", [])),
    }
    src_of = {s["id"]: s.get("src") for s in (hb or {}).get("sources", [])}
    sync_src = {s["id"]: s["source_id"] for s in (hb or {}).get("syncs", [])}

    # batch-A universe rules: sources/syncs/targets handle universes are
    # REQUIRED non-empty; runs may legitimately be empty; every source
    # handle must carry its fixture src (fixture derives from it).
    for name in ("sources", "syncs", "targets"):
        if not expect[name]:
            violations.append(f"{name}: handle universe empty (required)")
    for sid, src in src_of.items():
        if not src:
            violations.append(f"sources.{sid}: handle missing fixture src")

    for section in ("sources", "runs", "syncs", "fixture", "targets"):
        b, a = before.get(section, {}), after.get(section, {})
        if section == "fixture":
            expected_keys = {v for v in src_of.values() if v}
        else:
            expected_keys = set(expect[section])
        extra_b = set(b) - expected_keys
        extra_a = set(a) - expected_keys
        if extra_b:
            violations.append(f"{section}: unexpected keys on before {sorted(extra_b)}")
        if extra_a:
            violations.append(f"{section}: unexpected keys on after {sorted(extra_a)}")
        for key in sorted(expected_keys):
            if key not in b:
                violations.append(f"{section}.{key}: missing on BEFORE side")
                continue
            if key not in a:
                violations.append(f"{section}.{key}: missing on AFTER side")
                continue
            vb, va = b[key], a[key]
            # closeout-4: per-section strong typing happens OUTSIDE any
            # branch — a syncs entry that is a dict (e.g. {} on both
            # sides) must NEVER ride the dict branch to green (the old
            # bypass), and dict sections never compare non-dicts.
            if section == "syncs":
                types_ok = True
                for side_val, lbl in ((vb, "before"), (va, "after")):
                    if not isinstance(side_val, list) or not all(
                            isinstance(x, str) and x for x in side_val):
                        violations.append(
                            f"syncs.{key}: {lbl} value not a non-empty "
                            "list of run-id strings "
                            f"({type(side_val).__name__})")
                        types_ok = False
                if not types_ok:
                    continue
                if vb != va:
                    path = f"syncs.{key}"
                    if path in allowed:
                        changes[path] = {"before": vb, "after": va}
                    else:
                        violations.append(
                            f"{path}: changed without allow "
                            f"({vb!r} -> {va!r})")
                continue
            types_ok = True
            for side_val, lbl in ((vb, "before"), (va, "after")):
                if not isinstance(side_val, dict):
                    violations.append(
                        f"{section}.{key}: {lbl} value not an object "
                        f"({type(side_val).__name__})")
                    types_ok = False
            if not types_ok:
                continue
            _check_types(section, key, vb, violations,
                         f"{section}.{key}.before")
            _check_types(section, key, va, violations,
                         f"{section}.{key}.after")
            for sub in sorted(set(vb) | set(va)):
                if sub not in vb or sub not in va:
                    violations.append(
                        f"{section}.{key}.{sub}: missing on one side")
                    continue
                x, y = vb[sub], va[sub]
                if section == "sources" and sub == "stamp":
                    # stamp null is a LEGAL value (never admitted);
                    # null==null agrees, null<->value is an ordinary
                    # change that must be explicitly allowed
                    if x == y:
                        continue
                    path = f"{section}.{key}.{sub}"
                    if path in allowed:
                        changes[path] = {"before": x, "after": y}
                    else:
                        violations.append(
                            f"{path}: changed without allow "
                            f"({x!r} -> {y!r})")
                    continue
                if x is None or y is None:
                    violations.append(
                        f"{section}.{key}.{sub}: null value (only "
                        "sources.<id>.stamp may be an explicit null)")
                    continue
                if x != y:
                    path = f"{section}.{key}.{sub}"
                    if path in allowed:
                        changes[path] = {"before": x, "after": y}
                    else:
                        violations.append(
                            f"{path}: changed without allow "
                            f"({x!r} -> {y!r})")

    # binding: every handled sync's runs must sit under that sync's
    # source's full run-id set (checked on BOTH sides, after values).
    # Defensive against non-dict source entries (already flagged as
    # type violations above — never crash on them here).
    for side_name, side in (("before", before), ("after", after)):
        for sy, source_id in sync_src.items():
            runs = side.get("syncs", {}).get(sy)
            if not isinstance(runs, list):
                continue
            owner = side.get("sources", {}).get(source_id)
            if not isinstance(owner, dict):
                owner = {}
            universe = set(owner.get("run_ids") or [])
            leaked = sorted(set(runs) - universe)
            if leaked:
                violations.append(
                    f"{side_name}: sync {sy} carries runs outside its "
                    f"source {source_id}: {leaked}")

    for snap, label in ((before, "before"), (after, "after")):
        if snap.get("collection_failures"):
            violations.append(
                f"{label} snapshot carries collection failures: "
                f"{len(snap['collection_failures'])}")

    unused = sorted(allowed - set(changes))
    return {"ok": not violations,
            "violations": violations,
            "changes": changes,
            "allowed_unused": unused}


def _load_module():
    spec = importlib.util.spec_from_file_location("p1smoke", P1_SMOKE)
    M = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(M)
    return M


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_col = sub.add_parser("collect")
    p_col.add_argument("--env-dir", required=True)
    p_col.add_argument("--handles", required=True,
                       help="JSON: {sources:[{id,src}], "
                            "syncs:[{id,source_id}], runs:[ids], "
                            "targets:[{dataset_id,token}]} — tokens stay "
                            "in memory and are NEVER written to output")
    p_col.add_argument("--out", required=True)

    p_cmp = sub.add_parser("compare")
    p_cmp.add_argument("--before", required=True)
    p_cmp.add_argument("--after", required=True)
    p_cmp.add_argument("--out", required=True)
    p_cmp.add_argument("--allow", action="append", default=[])

    args = parser.parse_args()
    if args.cmd == "collect":
        env_dir = Path(args.env_dir)          # load_env needs a Path
        M = _load_module()
        M.ARGS = type("A", (), {"env_dir": str(env_dir)})()
        M.load_env(env_dir)
        snap = collect(M, json.loads(Path(args.handles).read_text()))
        Path(args.out).write_text(json.dumps(snap, indent=2) + "\n")
        print(f"[p23-snap] collected -> {args.out} "
              f"(failures={len(snap['collection_failures'])})")
        return 1 if snap["collection_failures"] else 0

    before = json.loads(Path(args.before).read_text())
    after = json.loads(Path(args.after).read_text())
    result = compare(before, after, allowed=args.allow)
    Path(args.out).write_text(json.dumps(result, indent=2) + "\n")
    print(f"[p23-snap] compare ok={result['ok']} "
          f"changes={sorted(result['changes'])} "
          f"violations={len(result['violations'])}")
    for v in result["violations"][:20]:
        print("  violation:", v)
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

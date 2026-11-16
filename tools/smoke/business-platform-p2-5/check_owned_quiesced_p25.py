#!/usr/bin/env python3
"""P25 OWNED-QUIESCE checker (round3, review-runtime-tools-round2 #5).

Distinguishes — per root rules — (a) the P25-owned defs whose schedules
MUST be disabled/cursors NULL and whose runs are terminal-eligible,
from (b) UNRESOLVED DURABLE CHECKPOINT OWNERS (may reference TERMINAL
FAILED runs) which must be RETAINED, never cleaned/adopted/replayed,
with admission still blocking new runs for those defs.

Strictness (this round):
  * the EXPECTED def set is an EXACT UUID set (--expected-defs, pinned
    pre-tail): missing/extra defs both fail; every comparison is
    UUID-to-UUID — def_tag strings never match owner defs;
  * the runs_terminal evidence is ACTUALLY CONSUMED: every listed run
    of an expected def must be terminal; the only tolerated
    nonterminal run is an owner def's OWN held run (run_ref same);
  * the blocked-admission proof binds PER DEF/Run: for EVERY owner
    def UUID the proof must carry its own pre-ABC POST entry
    (http_status non-2xx, recorded rc, body sha256 hex64, phase
    pre-abc) — a POST after ABC is forbidden, so the SAVED proof
    artifact is consumed; missing evidence fails closed;
  * unknown owner rows (terminal-failed or not) are neither dropped
    nor adopted — they FAIL the check truthfully (the declaration must
    cover them; they are never cleaned to make the checker green).

Pure core + CLI; this tool never writes anything.
"""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                     r"[0-9a-f]{4}-[0-9a-f]{12}$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")
TERMINAL = ("succeeded", "failed", "aborted")
RELEASED_FOR_EXECUTION = True   # root released pre-ABC owned quiesce

ENV_DIR_DEFAULT = "/private/tmp/openfoundry-platform-p1-q3v9"
PROJECT = "openfoundry-platform-p1-q3v9"


def _uuid(v):
    return isinstance(v, str) and UUID_RE.match(v) is not None


# --- live collection (readonly psql; projections pinned to the REAL
# DDL: batch_sync_defs.schedule_enabled / schedule_next_fire_at,
# sync_runs.id/status, sync_def_checkpoints.active_run_id — the same
# shapes the frozen P24 persist collector and the Archive postfacts
# collector use) ---------------------------------------------
DEFS_SQL_TMPL = ("SELECT json_agg(row_to_json(t)) FROM (SELECT"
                 " d.id::text, (d.schedule_enabled)::text"
                 " schedule_enabled,"
                 " (d.schedule_next_fire_at IS NULL)::text cursor_null"
                 " FROM batch_sync_defs d WHERE d.id IN ({ids})"
                 " ORDER BY d.id) t;")
RUNS_SQL_TMPL = ("SELECT json_agg(row_to_json(t)) FROM (SELECT"
                 " r.sync_def_id::text, r.id::text, r.status,"
                 " COALESCE(r.started_at::text,'') started_at"
                 " FROM sync_runs r WHERE r.sync_def_id IN ({ids})"
                 " ORDER BY r.sync_def_id, r.id) t;")
GRANTS_SQL_TMPL = ("SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json) FROM (SELECT"
                   " g.sync_def_id::text FROM sync_schedule_grants g"
                   " WHERE g.sync_def_id IN ({ids}) ORDER BY g.sync_def_id) t;")
OWNERS_SQL = (
    "SELECT json_agg(row_to_json(t)) FROM ("
    "  SELECT c.sync_def_id::text AS def_id, c.source_id::text AS source_id,"
    "         d.source_id::text AS def_source_id,"
    "         c.active_run_id::text AS run_id,"
    "         r.sync_def_id::text AS run_def_id,"
    "         r.status AS run_status,"
    "         CASE WHEN r.status = 'failed' OR (r.status = 'running'"
    "              AND NOT (r.lease_expires_at IS NOT NULL"
    "                       AND r.lease_expires_at > clock_timestamp()))"
    "              THEN 'requires_reconciliation' ELSE '' END"
    "              AS blocked_reason"
    "  FROM sync_def_checkpoints c"
    "  JOIN batch_sync_defs d ON d.id = c.sync_def_id"
    "  LEFT JOIN sync_runs r ON r.id = c.active_run_id"
    "  WHERE c.active_run_id IS NOT NULL"
    "  ORDER BY c.sync_def_id) t;")


def psql_argv(env_dir, sql):
    return ["docker", "compose", "-f", str(Path(env_dir) / "compose.yaml"),
            "--project-name", PROJECT, "exec", "-T", "postgres",
            "psql", "-U", "openfoundry", "-d", "connectors", "-t",
            "-A", "-v", "ON_ERROR_STOP=1",
            "-c", "SET TimeZone='UTC'; SET DateStyle='ISO, MDY';",
            "-c", sql]


def collect_body(args, run_cli):
    """Live readonly state gathering (release-gated; run_cli injectable
    for pure selftests). Writes the adjudicator's state.json with the
    NEW-runs-since-declaration set computed from real started_at."""
    expected = json.loads(Path(args.expected_defs).read_text())
    if not isinstance(expected, list) or not expected \
            or not all(_uuid(d) for d in expected):
        print("[p25-quiesce] expected-defs must be a non-empty UUID "
              "list", file=sys.stderr)
        return 2
    ids = ",".join(f"'{d}'" for d in sorted(expected))
    problems = []
    ledger = []
    raw_dir = Path(str(args.out) + ".raw")
    raw_dir.mkdir(parents=True, exist_ok=False)

    def query(sql, tag):
        argv = psql_argv(args.env_dir, sql)
        cmd = raw_dir / (tag + ".cmd.json")
        cmd.write_text(json.dumps(argv) + "\n")
        rc, out, err = run_cli(argv)
        artifacts = {"cmd": cmd}
        for key, value in (("stdout", out), ("stderr", err), ("rc", str(rc) + "\n")):
            p = raw_dir / (tag + "." + key)
            p.write_text(value)
            artifacts[key] = p
        ledger.append({"tag": tag, "rc": rc, "artifacts": {
            k: {"path": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
            for k, p in artifacts.items()}})
        if rc != 0:
            problems.append(f"{tag}: psql rc={rc}: {err[-160:]}")
            return []
        lines = [l for l in (out or "").strip().splitlines() if l.strip()]
        try:
            return json.loads(lines[-1]) or []
        except (ValueError, IndexError):
            problems.append(f"{tag}: output not JSON")
            return []

    defs = [d for d in query(DEFS_SQL_TMPL.format(ids=ids), "defs")]
    for d in defs:                      # accept the actual SQL text only
        for key in ("schedule_enabled", "cursor_null"):
            value = d.get(key)
            if value not in ("true", "false"):
                problems.append(f"defs.{key}: invalid text bool")
                d[key] = None
            else:
                d[key] = value == "true"
    runs = query(RUNS_SQL_TMPL.format(ids=ids), "runs")
    owners = query(OWNERS_SQL, "owners")
    grants = query(GRANTS_SQL_TMPL.format(ids=ids), "grants")
    declared_at = args.declared_at or ""
    new_runs = []
    if declared_at:
        # Compare timestamps in PostgreSQL, never ISO-vs-SQL strings.
        from datetime import datetime
        instant = datetime.fromisoformat(declared_at.replace("Z", "+00:00"))
        if instant.tzinfo is None:
            raise ValueError("quiesce guard timestamp must include UTC offset")
        timestamp = instant.isoformat()
        new_runs = query("SELECT json_agg(row_to_json(t)) FROM (SELECT"
                         " r.id::text,r.sync_def_id::text FROM sync_runs r"
                         f" WHERE r.sync_def_id IN ({ids}) AND r.started_at >"
                         f" '{timestamp}'::timestamptz ORDER BY r.id) t;", "new-runs")
    state = {"schema": "p25-quiesce-state-v1",
             "declared_at": declared_at,
             "defs": defs,
             "runs_terminal": runs,
             "owner_rows": owners,
             "grant_rows": grants,
             "query_ledger": ledger,
             "new_runs_since_decl": new_runs}
    Path(args.out).write_text(json.dumps(state, indent=1,
                                         sort_keys=True) + "\n")
    print(f"QUIESCE_STATE={args.out} defs={len(defs)} runs={len(runs)} "
          f"owners={len(owners)} new={len(new_runs)} problems={problems}")
    return 0 if not problems else 1


def real_run_cli(argv):
    import subprocess
    res = subprocess.run(argv, capture_output=True, text=True,
                         timeout=120)
    return res.returncode, res.stdout, res.stderr


def adjudicate_quiesce(state: dict, owner_roster: list,
                       blocked_proof: dict,
                       expected_def_ids: list) -> dict:
    reasons = []
    if not isinstance(state, dict):
        return {"ok": False, "reasons": ["state not an object"]}
    if not isinstance(expected_def_ids, list) \
            or not all(_uuid(d) for d in expected_def_ids) \
            or not expected_def_ids:
        return {"ok": False, "reasons": [
            "expected-defs must be a NON-EMPTY list of UUIDs — an "
            "empty/missing expected set is not quiescence"]}
    expected = set(expected_def_ids)
    if len(expected) != len(expected_def_ids):
        reasons.append("expected-defs contains duplicates")
    if state.get("grant_rows") != []:
        reasons.append("owned grant rows missing/not empty")

    # 1) the P25-owned defs: EXACT expected UUID set, disabled, NULL
    defs = state.get("defs") or []
    got_ids = set()
    for d in defs:
        did = (d or {}).get("id")
        if not _uuid(did):
            reasons.append(f"owned def id not a UUID: {did!r}")
            continue
        got_ids.add(did)
        if d.get("schedule_enabled") is not False:
            reasons.append(f"owned def {did} schedule not disabled")
        if d.get("cursor_null") is not True:
            reasons.append(f"owned def {did} cursor not NULL")
    if got_ids != expected:
        reasons.append(f"owned def set != expected: missing="
                       f"{sorted(expected - got_ids)} extra="
                       f"{sorted(got_ids - expected)} (UUID-to-UUID)")

    # 2) owner roster: UUID-keyed declarations (def_tag is commentary)
    declared = {}
    for o in owner_roster or []:
        did = (o or {}).get("def_id")
        if not _uuid(did):
            reasons.append(f"owner roster def_id not a UUID: {o}")
            continue
        if did in declared:
            reasons.append(f"duplicate declared owner def: {did}")
        declared[did] = o
        if not _uuid(o.get("source_id")) or not _uuid(o.get("run_id")):
            reasons.append(f"owner declaration missing actual source/run UUID: {did}")
        if did not in expected:
            reasons.append(f"owner def not in exact owned set: {did}")

    # 3) actual owner rows: every declared owner RETAINED; unknown
    #    owners neither dropped nor adopted — they block truthfully
    owner_rows = state.get("owner_rows") or []
    got_owners = {}
    unknown = []
    for a in owner_rows:
        did = (a or {}).get("def_id")
        if not _uuid(did):
            reasons.append(f"owner row not def_id-UUID keyed: {a}")
            continue
        if did in got_owners:
            reasons.append(f"duplicate owner row for {did}")
        got_owners[did] = a
        if a.get("blocked_reason") != "requires_reconciliation":
            reasons.append(f"owner row {did} without actual reconciliation reason")
        if not _uuid(a.get("run_id")):
            reasons.append(f"owner row {did} run_id not a UUID: "
                           f"{a.get('run_id')!r}")
    for did, o in sorted(declared.items()):
        a = got_owners.get(did)
        if a is None:
            reasons.append(f"declared owner {o.get('tag')} ({did}) NOT "
                           "retained — owners are never cleaned")
            continue
        if any(a.get(key) != o.get(key) for key in ("source_id", "run_id")) or a.get("run_def_id") != did or a.get("def_source_id") != o.get("source_id"):
            reasons.append(f"owner {did}: exact Def/Source/Run parent binding differs")
        if o.get("run_ref") == "terminal-failed" \
                and a.get("run_status") not in ("failed", "aborted"):
            reasons.append(
                f"owner {o.get('tag')} declared terminal-failed but "
                f"run_status={a.get('run_status')!r}")
    unknown = sorted(set(got_owners) - set(declared))
    if unknown:
        reasons.append(f"UNKNOWN owner rows present (not in the frozen "
                       f"roster): {unknown} — retained truthfully and "
                       "NEVER cleaned/adopted; the declaration must be "
                       "extended instead")

    # 4) runs_terminal evidence CONSUMED: every run of an expected def
    #    terminal; the only allowed nonterminal run is an owner def's
    #    OWN held run (run_ref == same)
    runs_terminal = state.get("runs_terminal") or []
    for did, owner in got_owners.items():
        matching = [r for r in runs_terminal if r.get("id") == owner.get("run_id")]
        if len(matching) != 1 or matching[0].get("sync_def_id") != did or matching[0].get("status") != owner.get("run_status"):
            reasons.append(f"owner {did}: referenced actual Run evidence missing/mismatched")
    held_runs = {a.get("run_id") for did, a in got_owners.items()
                 if (declared.get(did) or {}).get("run_ref") == "same"}
    for r in runs_terminal:
        if not isinstance(r, dict) or not _uuid(r.get("sync_def_id")) \
                or not _uuid(r.get("id")):
            reasons.append(f"runs_terminal row malformed: {r}")
            continue
        if r["sync_def_id"] not in expected \
                and r["sync_def_id"] not in declared:
            continue          # runs of unrelated defs are out of scope
        st = r.get("status")
        if st not in TERMINAL and r["id"] not in held_runs:
            reasons.append(f"expected def {r['sync_def_id']} run "
                           f"{r['id']} status {st!r} not terminal (and "
                           "not an owner's held run)")

    # 5) EXACT post-declaration first-Run set from actual tail bindings.
    # Old baseline/h10 runs predate this declaration; all other new IDs
    # (even under a nonowner P25 def), duplicates and omissions fail.
    expected_new = {(o.get("def_id"), o.get("run_id")) for o in owner_roster
                    if o.get("origin") == "intentional-tail"}
    actual_new = state.get("new_runs_since_decl")
    observed_new = set()
    if not isinstance(actual_new, list):
        reasons.append("new_runs_since_decl must be an actual list")
    else:
        for row in actual_new:
            if not isinstance(row, dict) or set(row) != {"sync_def_id", "id"} or not _uuid(row.get("sync_def_id")) or not _uuid(row.get("id")):
                reasons.append("new Run row malformed/non-UUID")
                continue
            pair = (row["sync_def_id"], row["id"])
            if pair in observed_new:
                reasons.append("duplicate new Run identity")
            observed_new.add(pair)
        if observed_new != expected_new:
            reasons.append(f"new Run exact set differs: missing={sorted(expected_new-observed_new)} extra={sorted(observed_new-expected_new)}")

    # 6) per-Def/Run PRE-ABC blocked-POST identity binding: EVERY
    #    owner def needs its own saved proof entry (UUID-keyed)
    if declared:
        proof_ok = isinstance(blocked_proof, dict) \
            and blocked_proof.get("ok") is True \
            and blocked_proof.get("phase") == "pre-abc"
        if not proof_ok:
            reasons.append(
                "owner-blocked admission proof missing/not pre-abc — "
                "POST probes after ABC are forbidden; consume the "
                "SAVED pre-ABC proof artifact")
        else:
            entries = blocked_proof.get("entries") or {}
            if not isinstance(entries, dict):
                reasons.append("blocked-proof entries must be an object")
                entries = {}
            if set(entries) != set(declared):
                reasons.append("blocked-proof owner Def set is not exact")
            for did, o in sorted(declared.items()):
                e = entries.get(did)
                if not isinstance(e, dict):
                    reasons.append(f"owner def {did} "
                                   f"({o.get('tag')}) has NO pre-ABC "
                                   "blocked-POST entry — per-def "
                                   "evidence is mandatory")
                    continue
                st = e.get("http_status")
                if not isinstance(st, int) or isinstance(st, bool) \
                        or st != 409:
                    reasons.append(f"owner def {did} proof "
                                   f"http_status {st!r} not a recorded "
                                   "4xx admission block")
                rc_ = e.get("rc")
                if not isinstance(rc_, int) or isinstance(rc_, bool) or rc_ != 0:
                    reasons.append(f"owner def {did} proof rc not a "
                                   f"strict int: {rc_!r}")
                body_sha = e.get("body_sha256")
                if not isinstance(body_sha, str) \
                        or not HEX64.match(body_sha):
                    reasons.append(f"owner def {did} proof body_sha256 "
                                   f"not hex64: {body_sha!r}")
                run_id = e.get("run_id")
                if not _uuid(run_id):
                        reasons.append(f"owner def {did} proof run_id "
                                       f"not a UUID: {run_id!r}")
                elif did in got_owners \
                        and (got_owners[did].get("run_id") != run_id or run_id != o.get("run_id")):
                        reasons.append(f"owner def {did} proof run_id "
                                       "!= the retained owner run — "
                                       "identity binding broken")
                try:
                    raw = e["raw"]
                    if set(raw) != {"cmd", "body", "status", "rc", "stderr"}:
                        raise ValueError("raw blocked record set differs")
                    for member in raw.values():
                        if hashlib.sha256(Path(member["path"]).read_bytes()).hexdigest() != member["sha256"]:
                            raise ValueError("raw blocked artifact SHA drift")
                    if raw["body"]["sha256"] != e["body_sha256"]:
                        raise ValueError("body SHA differs from exact raw")
                    command = json.loads(Path(raw["cmd"]["path"]).read_text())
                    if command.get("method") != "POST" or command.get("url") != f"http://127.0.0.1:17094/api/v1/data-connection/syncs/{did}/run?async=true":
                        raise ValueError("blocked request Def/verb differs")
                    if Path(raw["status"]["path"]).read_text().strip() != "409" or Path(raw["rc"]["path"]).read_text().strip() != "0":
                        raise ValueError("blocked original status/rc differ")
                    body = json.loads(Path(raw["body"]["path"]).read_text())
                    if body.get("code") != "sync_incremental_busy" or body.get("active_run_id") != o.get("run_id"):
                        raise ValueError("blocked body is not exact busy/same owner Run")
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    reasons.append(f"owner def {did} raw blocked proof invalid: {exc}")
    return {"ok": not reasons, "reasons": reasons,
            "expected_defs": len(expected),
            "owners_retained": len(set(got_owners) & set(declared)),
            "unknown_owners": unknown}


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="mode", required=True)
    c = sub.add_parser("collect")
    c.add_argument("--env-dir", default=ENV_DIR_DEFAULT)
    c.add_argument("--expected-defs", required=True)
    c.add_argument("--declared-at", default="",
                   help="the frozen tail declaration's declared_at — "
                        "new runs after it are adoption candidates")
    c.add_argument("--out", required=True)
    a = sub.add_parser("check")
    a.add_argument("--state", required=True)
    a.add_argument("--owner-roster", required=True,
                   help="the frozen tail-declaration owner_rows "
                        "(def_id UUID keyed)")
    a.add_argument("--expected-defs", required=True,
                   help="json list of the exact expected P25-owned "
                        "def UUIDs (root-pinned pre-tail)")
    a.add_argument("--blocked-proof")
    a.add_argument("--out")
    args = ap.parse_args()
    if args.mode == "collect":
        if not RELEASED_FOR_EXECUTION:
            print("[p25-quiesce] collect is LIVE (psql via docker "
                  "exec, readonly) and not released — root flips "
                  "RELEASED_FOR_EXECUTION", file=sys.stderr)
            return 3
        return collect_body(args, real_run_cli)
    state = json.loads(Path(args.state).read_text())
    spec = json.loads(Path(args.owner_roster).read_text())
    # accept BOTH the frozen declaration ({spec: {owner_rows}}) and a
    # bare owner_rows list
    if isinstance(spec, list):
        roster = spec
    elif isinstance(spec, dict):
        roster = (spec.get("spec") or {}).get("owner_rows") \
            or spec.get("owner_rows") or []
    else:
        roster = []
    expected = json.loads(Path(args.expected_defs).read_text())
    proof = json.loads(Path(args.blocked_proof).read_text()) \
        if args.blocked_proof else {}
    verdict = adjudicate_quiesce(state, roster, proof, expected)
    text = json.dumps(verdict, indent=1, sort_keys=True) + "\n"
    if args.out:
        Path(args.out).write_text(text)
    else:
        print(text, end="")
    return 0 if verdict["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

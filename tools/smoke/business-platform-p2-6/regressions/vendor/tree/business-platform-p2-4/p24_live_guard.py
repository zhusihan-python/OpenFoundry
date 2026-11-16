#!/usr/bin/env python3
"""P2.4 live-task guard — 4-row fixed-exception policy (DRAFT 2026-11-14).

DRAFT — NOT RELEASED FOR EXECUTION. Root must review pin-provenance.md
and flip RELEASED_FOR_EXECUTION to True before any run. This file never
writes to the database, never cancels/rewrites rows, and never reads
lease holder/token VALUES (presence booleans only).

Semantics (inherits the root-approved p23_live_guard.py policy, widened
ONLY in the pinned list — 4 rows instead of 2; every other rule is
byte-for-byte as strict):

  * The controlled pending/running set is queried WHOLESALE — every row,
    including started_at IS NULL, old or new, must be accounted for by
    the PINNED exception list. NO blanket "started_at < boot ⇒ ignore",
    NO blanket "expired ⇒ ignore", NO discovery, NO auto-add.
  * The exception list is EXACTLY the 4 root-handoff rows (p24_pins.py,
    GENERATED from the frozen P2.3 final archive by derive_pins_p24.py;
    see pin-provenance.md):
      old B 74d62557-e4e4-4a1a-a5b9-d63f3415d626  (96351e3b lease loss)
      old C c14b09f0-1341-4127-9e60-23abe7c36739  (96351e3b committed-unconfirmed)
      new B e08c0b07-4b08-4a88-ba6f-aad4f77eddf2  (528455c2 lease loss)
      new C 84055551-5d3b-4ba0-94a9-0bc8738d4335  (528455c2 committed-unconfirmed)
  * Every pinned field must match EXACTLY — the 24 frozen safe-projection
    keys plus conn_org_id/def_org_id/run_org_id: a STRICTER
    INFERRED-equality constraint (all three pinned to the archived
    org_id; the archived generating SQL did not record which org column
    it read, and the three pins are NOT independently-archived
    historical column values — they are verified by the fresh read-only
    3-table collection in results/…-p24-final/00-preflight/). Any drift
    (status, heartbeat, lease, gate, identity, org, bindings) BLOCKS. A
    pinned row that disappeared or left the controlled pending/running
    set BLOCKS.
  * Each row's started_at must predate the target container's
    State.StartedAt, compared as REAL datetimes via _parse_ts (a raw
    string comparison across psql ('… 19:22:35…+00') and RFC3339
    ('…T21:09:…Z') formats is meaningless — ' ' < 'T' would false-pass).
    Anything else BLOCKS.
  * Legacy-suffix b6ec49ab rows are NOT this guard's scope: they stay
    protected by the frozen P2.1/P2.2 baseline suites re-run as P2.4
    regressions (see formal_run_p24.sh).

Usage: python3 -I p24_live_guard.py <env_dir> <service>
Exit 0 iff every controlled pending/running row is exactly one of the
4 pinned, undrifted, pre-boot rows.
"""

import importlib.util
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Root released the guard for READ-ONLY adjudication on 2026-11-14
# after reviewing the corrected guard body and the fresh 4x27 preflight
# snapshot (root message: "准许你将RELEASED_FOR_EXECUTION改true并对
# 当前原Connector只读执行guard一次"). The guard only ever SELECTs and
# docker-inspects; the formal swap phase stays separately gated behind
# P24_CONFIRM in formal_run_p24.sh.
RELEASED_FOR_EXECUTION = True

PROJECT = "openfoundry-platform-p1-q3v9"
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def _load_pins():
    """Load the GENERATED pin list from this file's directory (works
    under python3 -I, which does not put the script dir on sys.path)."""
    pins_path = Path(__file__).resolve().parent / "p24_pins.py"
    spec = importlib.util.spec_from_file_location("p24_pins", pins_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.PINNED_EXCEPTIONS


PINNED_EXCEPTIONS = _load_pins()
assert len(PINNED_EXCEPTIONS) == 4, \
    "pin list must be exactly the 4 root-handoff rows — regenerate with " \
    "derive_pins_p24.py and re-review, never hand-edit p24_pins.py"

# Timezone/DateStyle are pinned so the TEXT rendering of timestamptz is
# deterministic ('2026-11-13 19:22:06.812306+00') regardless of server
# env drift — pins are exact strings.
ROW_SQL = """
SELECT json_agg(row_to_json(t)) FROM (
  SELECT r.id::text, r.sync_def_id::text, d.source_id::text,
         d.output_dataset_id::text, c.name AS source_name,
         (c.config->'sync'->'query_params'->>'src') AS fixture_src,
         c.owner_id::text AS owner_id,
         r.owner_id::text AS run_owner_id,
         r.requested_by::text,
         c.org_id::text AS conn_org_id,
         d.org_id::text AS def_org_id,
         r.org_id::text AS run_org_id,
         r.org_id::text AS org_id,
         r.status, r.control_mode,
         (r.lease_holder IS NOT NULL)::text AS has_holder,
         (r.lease_token IS NOT NULL)::text AS has_token,
         COALESCE(r.started_at::text, '') AS started_at,
         COALESCE(r.heartbeat_at::text, '') AS heartbeat_at,
         COALESCE(r.lease_expires_at::text, '') AS lease_expires_at,
         (r.cancel_requested_at IS NOT NULL)::text AS cancel_requested,
         (r.publish_entered_at IS NOT NULL)::text AS gate_entered,
         COALESCE(r.attempt::text, '') AS attempt,
         COALESCE(r.generation::text, '') AS generation,
         (r.content_hash IS NOT NULL)::text AS has_hash,
         (r.dataset_version_id IS NOT NULL)::text AS has_version,
         COALESCE(r.finished_at::text, '') AS finished_at
  FROM sync_runs r
  JOIN batch_sync_defs d ON d.id = r.sync_def_id
  JOIN connections c ON c.id = d.source_id
  WHERE r.control_mode = 'controlled'
    AND r.status IN ('pending','running')) t;"""

SESSION_SQL = "SET TimeZone='UTC'; SET DateStyle='ISO, MDY';"


def _parse_ts(value):
    """Normalize psql text timestamps ('2026-11-13 19:22:35.1+00') and
    RFC3339 ('2026-11-13T19:22:48.7Z') to datetime — a RAW string
    comparison across the two formats would be meaningless (space < 'T'
    would make every psql ts 'earlier' and false-pass)."""
    value = value.strip().replace(" ", "T")
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    elif value.endswith("+00"):
        value = value[:-3] + "+00:00"
    return datetime.fromisoformat(value)


def account(rows, pinned, boot_started_at):
    """PURE policy check. rows: list of dicts as returned by ROW_SQL
    (may include started_at == '' for NULL). pinned: the exception list.
    boot_started_at: RFC3339 string of the container's State.StartedAt.
    Returns (ok, reasons, accounted_ids)."""
    reasons = []
    pinned_by_id = {p["id"]: p for p in pinned}
    if len(pinned_by_id) != len(pinned):
        reasons.append("pinned list carries duplicate ids — refusing")
    rows_by_id = {}
    for row in rows:
        rid = row.get("id")
        if rid in rows_by_id:
            reasons.append(f"duplicate row id {rid}")
            continue
        rows_by_id[rid] = row
    # unknown rows (old/new/NULL-started) all BLOCK — never auto-join
    for rid, row in sorted(rows_by_id.items()):
        if rid not in pinned_by_id:
            detail = f"status={row.get('status')} started_at={row.get('started_at')!r}"
            reasons.append(f"UNACCOUNTED controlled row {rid} ({detail}) "
                           "— not on the fixed exception list; blocking")
    # pinned rows must be present, undrifted, and pre-boot
    for pid, pin in sorted(pinned_by_id.items()):
        row = rows_by_id.get(pid)
        if row is None:
            reasons.append(f"pinned row {pid} MISSING from the "
                           "controlled pending/running set (deleted or "
                           "terminalized — must never be touched)")
            continue
        for key, want in sorted(pin.items()):
            got = row.get(key)
            if got != want:
                reasons.append(f"pinned row {pid} DRIFT on {key}: "
                               f"got={got!r} want={want!r}")
        started = row.get("started_at") or ""
        if not started:
            reasons.append(f"pinned row {pid} started_at is NULL")
        elif boot_started_at:
            try:
                if not (_parse_ts(started) < _parse_ts(boot_started_at)):
                    reasons.append(
                        f"pinned row {pid} started_at {started} is NOT "
                        f"before the container boot {boot_started_at} — "
                        "original executing process may still be alive")
            except ValueError as exc:
                reasons.append(f"pinned row {pid} timestamp unparseable: "
                               f"{exc}")
    return (not reasons), reasons, sorted(rows_by_id)


def pick_container(ps_stdout, ps_rc):
    """PURE: strict `compose ps -q <service>` adjudication (root review
    round 2): ps must be rc0 AND yield EXACTLY ONE container id — a
    multi-id result must BLOCK instead of silently inspecting [0].
    Returns (cid, error); cid is None whenever error is set."""
    if ps_rc != 0:
        return None, f"compose ps rc={ps_rc}"
    lines = [l for l in ps_stdout.strip().splitlines() if l.strip()]
    if not lines:
        return None, "compose ps returned an empty stdout (no container id)"
    if len(lines) > 1:
        return None, (f"compose ps returned {len(lines)} container ids "
                      f"({lines}) — ambiguous target, refusing to pick [0]")
    return lines[0], None


def parse_inspect_boot(ins_stdout, ins_rc):
    """PURE: strict StartedAt|Image extraction. Empty or non-RFC3339
    stdout is a friendly BLOCK (it used to IndexError — fail-closed but
    opaque). Returns (boot, image, error)."""
    if ins_rc != 0:
        return None, None, f"docker inspect rc={ins_rc}"
    line = ins_stdout.strip()
    if not line:
        return None, None, "docker inspect returned an empty stdout"
    parts = line.split("|")
    if len(parts) < 2 or not RFC3339.match(parts[0]):
        return None, None, f"StartedAt not strict RFC3339: {parts[0]!r}"
    return parts[0], parts[1], None


def main():
    if not RELEASED_FOR_EXECUTION:
        print("[p24-live-guard] DRAFT: not released — root must review "
              "pin-provenance.md and set RELEASED_FOR_EXECUTION=True",
              file=sys.stderr)
        return 3
    if len(sys.argv) != 3:
        print("usage: p24_live_guard.py <env_dir> <service>",
              file=sys.stderr)
        return 2
    env_dir, service = Path(sys.argv[1]), sys.argv[2]
    files = ["-f", str(env_dir / "compose.yaml")]
    override = env_dir / "compose-p22-connector2.yaml"
    if override.exists():
        files += ["-f", str(override)]

    def compose(*args, timeout=120):
        return subprocess.run(
            ["docker", "compose", *files, "--project-name", PROJECT, *args],
            capture_output=True, text=True, timeout=timeout)

    ps = compose("ps", "-q", service)
    cid, err = pick_container(ps.stdout, ps.returncode)
    if err:
        print(f"[p24-live-guard] {service}: {err}", file=sys.stderr)
        return 1
    ins = subprocess.run(["docker", "inspect", "--format",
                          "{{.State.StartedAt}}|{{.Image}}", cid],
                         capture_output=True, text=True, timeout=60)
    started_at, image, err = parse_inspect_boot(ins.stdout, ins.returncode)
    if err:
        print(f"[p24-live-guard] {err}", file=sys.stderr)
        return 1
    print(f"[p24-live-guard] {service} boot={started_at} "
          f"image={image[:19]}…")

    psql = compose("exec", "-T", "postgres", "psql", "-U", "openfoundry",
                   "-d", "connectors", "-t", "-A", "-v",
                   "ON_ERROR_STOP=1", "-c", SESSION_SQL, "-c", ROW_SQL)
    if psql.returncode != 0:
        print(f"[p24-live-guard] projection query rc={psql.returncode} — "
              "refusing", file=sys.stderr)
        return 1
    try:
        rows = json.loads(psql.stdout.strip().splitlines()[-1] or "null") or []
    except ValueError:
        print("[p24-live-guard] projection output not JSON — refusing",
              file=sys.stderr)
        return 1
    print(f"[p24-live-guard] controlled pending/running rows: {len(rows)}")

    ok, reasons, accounted = account(rows, PINNED_EXCEPTIONS, started_at)
    for rid in accounted:
        print(f"[p24-live-guard]   row: {rid}")
    if not ok:
        for r in reasons:
            print(f"[p24-live-guard] BLOCK: {r}", file=sys.stderr)
        return 1
    print(f"[p24-live-guard] ok: every controlled row is an undrifted "
          f"pre-boot pinned exception ({len(accounted)}/{len(PINNED_EXCEPTIONS)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""P2.3 live-task guard — fixed-exception policy (root, 2026-11-14).

Semantics (STRICT, replaces the earlier process-window draft):
  * The controlled pending/running set is queried WHOLESALE — every row,
    including started_at IS NULL, old or new, must be accounted for by
    the PINNED exception list below. There is NO blanket
    "started_at < boot ⇒ ignore" and NO blanket "expired ⇒ ignore".
  * The exception list is EXACTLY TWO fixed rows, identified by full
    id, whose provenance is archived raw under
    results/2026-11-14-p23-final/01b-swap-guard/investigation/
    (P22-final round's P21 B/C, suffix 96351e3b):
      - C: archive-uuid-pinned (frozen P22 terminal_commit_fault
        evidence) — c14b09f0-1341-4127-9e60-23abe7c36739
      - B: strong-chain provenance (full parent chain + fixture key +
        P21-B semantics + timing window) —
        74d62557-e4e4-4a1a-a5b9-d63f3415d626
    PROVISIONAL until root confirms from the raw archive; the guard
    only ever COMPARES against these pins — it never discovers,
    auto-adds, or forgives rows.
  * For each pinned row, EVERY pinned field must match EXACTLY (any
    drift — status, heartbeat, lease, gate, bindings — BLOCKS). A
    pinned row that disappeared or left the controlled pending/running
    set BLOCKS (it must never be cancelled/rewritten/terminalized).
  * Each row must predate the target container's State.StartedAt
    (strict RFC3339; the original executing process has exited).
    Anything else BLOCKS.
  * The two rows are never cancelled, never re-triggered, never given
    P23 quota release; the frozen swap_guard.py and frozen archives are
    untouched.

Usage: python3 -I p23_live_guard.py <env_dir> <service>
Exit 0 iff every controlled pending/running row is exactly one of the
two pinned, undrifted, pre-boot rows.
"""

import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

PROJECT = "openfoundry-platform-p1-q3v9"
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")

# Pinned from the RAW archive q1_controlled_rows.out / q1c_fixture_src.out
# (2026-11-14). ROOT-APPROVED 2026-11-14 (raw q1/q1c/q2b/q3 reviewed +
# audit_dataset cross-checked): exactly these two rows, exact parent
# chain + 13-col safe projection; all other controlled pending/running
# (incl. started_at NULL) block; the list never auto-grows and the
# control_mode scope is never widened. Comparisons are exact — no
# defaults, no forgiveness.
PINNED_EXCEPTIONS = [
    {"id": "c14b09f0-1341-4127-9e60-23abe7c36739",
     "sync_def_id": "4df5bf51-f688-43e9-8480-48fe8d45ad4d",
     "source_id": "65fee85e-6768-48c0-ae0f-3c7531ca3880",
     "output_dataset_id": "73a3072c-3150-4a9b-a722-555166f39c9f",
     "source_name": "p21-c-96351e3b",
     "fixture_src": "p21c-96351e3b",
     "owner_id": "c1d5b198-3a87-4abb-b4ed-859176fa3b56",
     "requested_by": "c1d5b198-3a87-4abb-b4ed-859176fa3b56",
     "status": "running", "control_mode": "controlled",
     "started_at": "2026-11-13 19:22:35.118352+00",
     "heartbeat_at": "2026-11-13 19:22:35.205969+00",
     "lease_expires_at": "2026-11-13 19:23:35.205969+00",
     "cancel_requested": "false", "gate_entered": "true",
     "attempt": "1", "generation": "1",
     "has_hash": "false", "has_version": "false", "finished_at": "",
     "has_holder": "true", "has_token": "true"},
    {"id": "74d62557-e4e4-4a1a-a5b9-d63f3415d626",
     "sync_def_id": "0cf8f83c-7c72-4672-ada1-95a6641f3232",
     "source_id": "b540d8de-a231-4808-9e3d-c52118c6a297",
     "output_dataset_id": "59ed4b73-8a4d-400a-90be-2d361d7bdfc5",
     "source_name": "p21-p21b-96351e3b",
     "fixture_src": "p21b-96351e3b",
     "owner_id": "0fd0389a-ab28-46e6-8f9d-51cf8d260a2c",
     "requested_by": "0fd0389a-ab28-46e6-8f9d-51cf8d260a2c",
     "status": "running", "control_mode": "controlled",
     "started_at": "2026-11-13 19:22:06.812306+00",
     "heartbeat_at": "2026-11-13 19:22:06.812307+00",
     "lease_expires_at": "2026-11-13 19:22:02.20804+00",
     "cancel_requested": "false", "gate_entered": "false",
     "attempt": "1", "generation": "1",
     "has_hash": "false", "has_version": "false", "finished_at": "",
     "has_holder": "true", "has_token": "true"},
]

ROW_SQL = """
SELECT json_agg(row_to_json(t)) FROM (
  SELECT r.id::text, r.sync_def_id::text, d.source_id::text,
         d.output_dataset_id::text, c.name AS source_name,
         (c.config->'sync'->'query_params'->>'src') AS fixture_src,
         c.owner_id::text, r.requested_by::text,
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
        for key, want in pin.items():
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


def main():
    if len(sys.argv) != 3:
        print("usage: p23_live_guard.py <env_dir> <service>",
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

    cid = compose("ps", "-q", service).stdout.strip().splitlines()
    if not cid:
        print(f"[p23-live-guard] {service}: no container id", file=sys.stderr)
        return 1
    ins = subprocess.run(["docker", "inspect", "--format",
                          "{{.State.StartedAt}}|{{.Image}}", cid[0]],
                         capture_output=True, text=True, timeout=60)
    if ins.returncode != 0:
        print(f"[p23-live-guard] inspect rc={ins.returncode}", file=sys.stderr)
        return 1
    started_at, image = (ins.stdout.strip().split("|") + [""])[:2]
    if not RFC3339.match(started_at):
        print(f"[p23-live-guard] StartedAt not strict RFC3339: "
              f"{started_at!r}", file=sys.stderr)
        return 1
    print(f"[p23-live-guard] {service} boot={started_at} "
          f"image={image[:19]}…")

    psql = compose("exec", "-T", "postgres", "psql", "-U", "openfoundry",
                   "-d", "connectors", "-t", "-A", "-v",
                   "ON_ERROR_STOP=1", "-c", ROW_SQL)
    if psql.returncode != 0:
        print(f"[p23-live-guard] projection query rc={psql.returncode} — "
              "refusing", file=sys.stderr)
        return 1
    try:
        rows = json.loads(psql.stdout.strip() or "null") or []
    except ValueError:
        print("[p23-live-guard] projection output not JSON — refusing",
              file=sys.stderr)
        return 1
    print(f"[p23-live-guard] controlled pending/running rows: {len(rows)}")

    ok, reasons, accounted = account(rows, PINNED_EXCEPTIONS, started_at)
    for rid in accounted:
        print(f"[p23-live-guard]   row: {rid}")
    if not ok:
        for r in reasons:
            print(f"[p23-live-guard] BLOCK: {r}", file=sys.stderr)
        return 1
    print(f"[p23-live-guard] ok: every controlled row is an undrifted "
          f"pre-boot pinned exception ({len(accounted)}/{len(PINNED_EXCEPTIONS)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())

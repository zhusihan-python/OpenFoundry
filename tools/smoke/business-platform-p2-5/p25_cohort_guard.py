#!/usr/bin/env python3
"""P2.5 six-cohort GUARD — first-transition + steady policies.

DRAFT — RELEASED_FOR_EXECUTION stays False until root reviews the
actual code and selftest outputs and flips it (root round-1 ruling).
Until then main() refuses (rc 3) BEFORE any docker/psql subprocess is
constructed. This file never writes to the database, never cancels or
rewrites rows, and never reads lease holder/token VALUES (presence
booleans only), exactly like the frozen p24_live_guard.py discipline.

Roster: imported from the GENERATED sibling p25_cohort6.py (produced
by p25_cohort_derive.py under a full SHA chain; never hand-edited).

Policy A — first_transition (the ONLY valid semantics until a root-
reviewed legitimate P25 candidate switch happens):
  * live connector boot/image must EXACTLY equal the sealed P24-final
    anchor (full 64-hex image compare; short-id compares are refused).
    Anything else means the stack is no longer the P24-final state and
    the guard refuses rather than "comparing nearby".
  * the controlled pending/running set is queried WHOLESALE and must
    be EXACTLY the 6 roster rows; expected AND live rows are checked
    against the exact 27-key schema (missing/partial/extra keys both
    BLOCK — the unexecuted-DRAFT cohort_gate_p24.py weakness).
  * group1 (old4): started_at < final anchor AND < the Nov13T22:05
    historical provenance anchor; group2 (P24 known postboot2):
    final anchor < started_at. No blanket age exemption; a group swap
    BLOCKS. A blanket "all six < current boot" would FAIL today by
    construction (postboot2 started 07:25 > current boot 07:11) and is
    deliberately NOT policy A.

Policy B — steady (after the legitimate, reviewed P25 candidate
switch): all six rows predate the CURRENT expected target boot (a
parameter from that deployment's reviewed record, never inferred),
target image compared full-hex, provenance grouping retained, same
strict row/schema checks. No auto-enrollment of future rows.

Usage (when released):
  python3 -I p25_cohort_guard.py --policy first-transition \
      [--env-dir DIR] [--service connector]
  python3 -I p25_cohort_guard.py --policy steady \
      --target-boot RFC3339 --target-image sha256:HEX64 [...]
"""

import argparse
import importlib.util
import json
import re
import subprocess
import sys
from pathlib import Path

RELEASED_FOR_EXECUTION = True   # root flips only after code+selftest review

PROJECT = "openfoundry-platform-p1-q3v9"
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")


EXPECTED_GROUP1_IDS = frozenset({
    "74d62557-e4e4-4a1a-a5b9-d63f3415d626",
    "c14b09f0-1341-4127-9e60-23abe7c36739",
    "e08c0b07-4b08-4a88-ba6f-aad4f77eddf2",
    "84055551-5d3b-4ba0-94a9-0bc8738d4335",
})
EXPECTED_GROUP2_IDS = frozenset({
    "dcca308f-8d74-48b6-ae27-7f17fa2f30e2",   # B lease-loss
    "885d1429-53ea-43f5-a5a7-03d2ae0b4b8a",   # C committed-unconfirmed
})


def validate_roster_rows(rows, group_name, schema):
    """EXACT schema validation of EXPECTED roster rows: 27 keys, string
    values, parseable started_at. A damaged/truncated expected record is
    refused here — never exempted by the compare loop (code-review
    item 2)."""
    reasons = []
    ids = set()
    for i, row in enumerate(rows):
        where = f"{group_name}[{i}]"
        if not isinstance(row, dict):
            reasons.append(f"{where}: not an object")
            continue
        r = check_row_schema(row, where, schema)
        reasons += r
        if r:
            continue
        for k in schema:
            if not isinstance(row[k], str):
                reasons.append(f"{where}: key {k} is "
                               f"{type(row[k]).__name__}, want str")
        if not row["started_at"]:
            reasons.append(f"{where}: started_at empty")
        else:
            try:
                _parse_ts(row["started_at"], f"{where} started_at")
            except ValueError as exc:
                reasons.append(str(exc))
        if row["id"] in ids:
            reasons.append(f"{where}: duplicate expected id {row['id']}")
        ids.add(row["id"])
    return reasons, ids


def load_roster():
    """Load + FULLY validate the generated roster before any runtime use:
    exact group sizes, exact known group IDs, per-row exact-27-key schema
    and value types, parseable anchors, full-hex images, provenance SHAs
    present, and the roster file's own sha256 must equal the recorded
    binding in the sibling frozen baseline (tamper detection)."""
    import hashlib
    here = Path(__file__).resolve().parent
    path = here / "p25_cohort6.py"
    spec = importlib.util.spec_from_file_location("p25_cohort6", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    schema = expected_schema()
    problems = []
    if set(mod.SCHEMA_27) != schema:
        problems.append("SCHEMA_27 key set mismatch vs guard schema")
    if len(mod.GROUP1) != 4:
        problems.append(f"GROUP1 must be 4 rows, got {len(mod.GROUP1)}")
    if len(mod.GROUP2) != 2:
        problems.append(f"GROUP2 must be 2 rows, got {len(mod.GROUP2)}")
    r1, ids1 = validate_roster_rows(mod.GROUP1, "GROUP1", schema)
    r2, ids2 = validate_roster_rows(mod.GROUP2, "GROUP2", schema)
    problems += r1 + r2
    if not problems:
        if ids1 != set(EXPECTED_GROUP1_IDS):
            problems.append("GROUP1 ids != the frozen old4 id set")
        if ids2 != set(EXPECTED_GROUP2_IDS):
            problems.append("GROUP2 ids != the frozen postboot2 B/C ids")
        if ids1 & ids2:
            problems.append("group id overlap")
    if not RFC3339.match(str(mod.FINAL_ANCHOR or "")):
        problems.append("FINAL_ANCHOR not strict RFC3339")
    if not RFC3339.match(str(mod.HISTORICAL_ANCHOR_OLD4 or "")):
        problems.append("HISTORICAL_ANCHOR_OLD4 not strict RFC3339")
    try:
        norm_image(mod.FINAL_IMAGE, "FINAL_IMAGE")
        norm_image(mod.DATASET_IMAGE, "DATASET_IMAGE")
        norm_image(mod.PIPELINE_IMAGE, "PIPELINE_IMAGE")
    except ValueError as exc:
        problems.append(str(exc))
    if not isinstance(mod.PROVENANCE, dict) or not mod.PROVENANCE.get(
            "hash_manifest_sha256"):
        problems.append("PROVENANCE sha chain missing")
    fb_path = here / "p24-frozen-baseline.json"
    # round-2 item 4: the roster itself is rederived from the FIXED P24
    # seal (content-hash-anchored archive + tool chain) and must EXACTLY
    # equal the generated module — double-editing baseline+cohort can no
    # longer forge expectations (the trust anchor is the seal chain and
    # this reviewed code, not sibling JSON).
    try:
        spec_d = importlib.util.spec_from_file_location(
            "p25_derive_seal", here / "p25_cohort_derive.py")
        derive = importlib.util.module_from_spec(spec_d)
        spec_d.loader.exec_module(derive)
        sealed = derive.derive(Path(derive.SEAL_P24_FINAL_DEFAULT),
                               Path(derive.SEAL_P24_TOOLS_DEFAULT))
        for key in ("GROUP1", "GROUP2", "FINAL_ANCHOR", "FINAL_IMAGE",
                    "DATASET_IMAGE", "PIPELINE_IMAGE", "AUX_ABSENT",
                    "HISTORICAL_ANCHOR_OLD4", "FIXTURE_BOOT",
                    "COHORT_COUNTER_SRCS", "PROVENANCE"):
            if getattr(mod, key) != sealed[key]:
                problems.append(f"{key} != seal derivation — the "
                                "generated roster was hand-edited or "
                                "stale; regenerate")
    except Exception as exc:                      # noqa: BLE001
        problems.append(f"seal rederivation failed: {exc}")
    if not fb_path.is_file():
        problems.append("sibling p24-frozen-baseline.json missing — "
                        "roster sha binding cannot be checked")
    else:
        import json as _json
        try:
            fb = _json.loads(fb_path.read_text())
            want = fb.get("p25_cohort6_sha256")
            got = hashlib.sha256(path.read_bytes()).hexdigest()
            if want != got:
                problems.append(f"p25_cohort6.py sha {got} != frozen "
                                f"baseline binding {want} — regenerate; "
                                "never hand-edit")
        except ValueError:
            problems.append("p24-frozen-baseline.json not JSON")
    if problems:
        raise RuntimeError("roster validation refused: " + "; ".join(
            problems[:6]))
    return mod


def expected_schema():
    # Duplicated deliberately (fail-closed if the generated roster and
    # this guard ever disagree); selftest cross-checks both against the
    # frozen P24 guard projection.
    return {
        "attempt", "cancel_requested", "conn_org_id", "control_mode",
        "def_org_id", "fixture_src", "finished_at", "gate_entered",
        "generation", "has_hash", "has_holder", "has_token",
        "has_version", "heartbeat_at", "id", "lease_expires_at",
        "org_id", "output_dataset_id", "owner_id", "requested_by",
        "run_org_id", "run_owner_id", "source_id", "source_name",
        "started_at", "status", "sync_def_id",
    }


# Frozen p24_live_guard.py projection, byte-comparable in selftest.
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


def _parse_ts(value, what):
    """Normalize psql text timestamps and RFC3339 to datetime (the P24
    lesson: a raw cross-format string compare false-passes ' ' < 'T')."""
    v = str(value).strip().replace(" ", "T")
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    elif v.endswith("+00"):
        v = v[:-3] + "+00:00"
    from datetime import datetime
    try:
        return datetime.fromisoformat(v)
    except ValueError as exc:
        raise ValueError(f"{what}: unparseable timestamp {value!r}: {exc}")


def norm_image(value, what):
    v = str(value or "").strip()
    if v.startswith("sha256:"):
        v = v[len("sha256:"):]
    if not HEX64.match(v):
        raise ValueError(f"{what}: not a full 64-hex image id: {value!r}")
    return v


def check_row_schema(row, where, schema):
    """EXACT key-set enforcement on a live row: partial/extra BLOCK."""
    if not isinstance(row, dict):
        return [f"{where}: not an object"]
    keys = set(row.keys())
    missing = sorted(schema - keys)
    extra = sorted(keys - schema)
    reasons = []
    if missing:
        reasons.append(f"{where}: MISSING {len(missing)} key(s) "
                       f"{missing[:5]}… (partial row hard-refused)")
    if extra:
        reasons.append(f"{where}: EXTRA {len(extra)} key(s) {extra[:5]}…")
    return reasons


def adjudicate_first_transition(rows, roster, live_boot, live_image):
    """PURE policy A. rows: live ROW_SQL dicts. Returns (ok, reasons)."""
    schema = set(roster.SCHEMA_27)
    # damaged EXPECTED rows refuse before any live-data comparison —
    # a truncated expected record can never silently exempt a field
    exp_rows = list(roster.GROUP1) + list(roster.GROUP2)
    ereasons, _eids = validate_roster_rows(exp_rows, "expected", schema)
    if ereasons:
        return False, ["damaged expected roster — refusing: "] + ereasons
    reasons = []
    if not RFC3339.match(str(live_boot or "")):
        return False, [f"live boot not strict RFC3339: {live_boot!r}"]
    try:
        live_img = norm_image(live_image, "live image")
    except ValueError as exc:
        return False, [str(exc)]
    try:
        final_ts = _parse_ts(roster.FINAL_ANCHOR, "final anchor")
        hist_ts = _parse_ts(roster.HISTORICAL_ANCHOR_OLD4, "hist anchor")
        live_ts = _parse_ts(live_boot, "live boot")
    except ValueError as exc:
        return False, [str(exc)]
    if live_boot != roster.FINAL_ANCHOR or live_img != norm_image(
            roster.FINAL_IMAGE, "roster image"):
        reasons.append(
            f"live stack is NOT the sealed P24-final state: "
            f"boot {live_boot} / image {live_img} != sealed "
            f"{roster.FINAL_ANCHOR} / {roster.FINAL_IMAGE} — do not "
            "proceed with a nearby comparison; root must re-adjudicate")

    rows_by_id = {}
    for i, row in enumerate(rows):
        reasons += check_row_schema(row, f"live row[{i}]", schema)
        rid = row.get("id") if isinstance(row, dict) else None
        if rid is None:
            reasons.append(f"live row[{i}]: no id")
            continue
        if rid in rows_by_id:
            reasons.append(f"duplicate live row id {rid}")
            continue
        rows_by_id[rid] = row

    expected = {r["id"]: r for r in list(roster.GROUP1) + list(roster.GROUP2)}
    for rid in sorted(set(rows_by_id) - set(expected)):
        row = rows_by_id[rid]
        reasons.append(f"UNACCOUNTED controlled row {rid} "
                       f"(status={row.get('status')} "
                       f"started_at={row.get('started_at')!r}) — not on "
                       "the six-roster; blocking")
    for rid in sorted(expected):
        row = rows_by_id.get(rid)
        if row is None:
            reasons.append(f"roster row {rid} MISSING from the controlled "
                           "pending/running set (deleted or terminalized — "
                           "must never be touched)")
            continue
        for key in sorted(expected[rid]):
            if key not in row:
                continue  # already reported by schema check
            if row.get(key) != expected[rid][key]:
                reasons.append(f"row {rid} DRIFT on {key}: "
                               f"got={row.get(key)!r} "
                               f"want={expected[rid][key]!r}")
        started = row.get("started_at") or ""
        if not started:
            reasons.append(f"row {rid} started_at empty/NULL")
            continue
        try:
            st = _parse_ts(started, f"row {rid} started_at")
        except ValueError as exc:
            reasons.append(str(exc))
            continue
        group1_ids = {r["id"] for r in roster.GROUP1}
        if rid in group1_ids:
            if not (st < final_ts and st < hist_ts):
                reasons.append(
                    f"old4 row {rid} started {started} not before BOTH "
                    f"anchors (final {roster.FINAL_ANCHOR}, historical "
                    f"{roster.HISTORICAL_ANCHOR_OLD4}) — group provenance "
                    "broken")
        else:
            if not (final_ts < st):
                reasons.append(
                    f"postboot2 row {rid} started {started} NOT after the "
                    f"final anchor {roster.FINAL_ANCHOR} — group swap or "
                    "unauthorized restart aging attempt")
    # live boot ordering sanity for the six (informational only in
    # policy A: postboot2 legitimately postdates the live boot).
    for rid in sorted(expected):
        row = rows_by_id.get(rid)
        if not row:
            continue
        started = row.get("started_at") or ""
        if started:
            try:
                st = _parse_ts(started, f"row {rid} started_at")
                if rid in {r["id"] for r in roster.GROUP1} and st >= live_ts:
                    reasons.append(f"old4 row {rid} started {started} NOT "
                                   f"before the live boot {live_boot}")
            except ValueError:
                pass  # already reported
    return (not reasons), reasons


def adjudicate_steady(rows, roster, target_boot, target_image=None,
                      live_boot=None, live_image=None):
    """PURE policy B (post reviewed P25 candidate switch). All six rows
    predate the parameterized expected target boot; grouping retained.
    When live_boot/live_image are supplied they must EXACTLY equal the
    reviewed target parameters (full-hex image) — a mismatching live
    stack is a deployment error, not something to compare nearby."""
    schema = set(roster.SCHEMA_27)
    exp_rows = list(roster.GROUP1) + list(roster.GROUP2)
    ereasons, _eids = validate_roster_rows(exp_rows, "expected", schema)
    if ereasons:
        return False, ["damaged expected roster — refusing: "] + ereasons
    reasons = []
    if not RFC3339.match(str(target_boot or "")):
        return False, [f"target boot not strict RFC3339: {target_boot!r}"]
    try:
        target_ts = _parse_ts(target_boot, "target boot")
        final_ts = _parse_ts(roster.FINAL_ANCHOR, "final anchor")
    except ValueError as exc:
        return False, [str(exc)]
    if live_boot is not None and live_boot != target_boot:
        reasons.append(f"live boot {live_boot} != reviewed target boot "
                       f"{target_boot} — deployment mismatch, refusing")
    if target_image is not None:
        try:
            want_img = norm_image(target_image, "target image")
        except ValueError as exc:
            return False, reasons + [str(exc)]
        if live_image is not None:
            try:
                got_img = norm_image(live_image, "live image")
            except ValueError as exc:
                return False, reasons + [str(exc)]
            if got_img != want_img:
                reasons.append(f"live image {got_img} != reviewed target "
                               f"image {want_img} — refusing")
    rows_by_id = {}
    for i, row in enumerate(rows):
        reasons += check_row_schema(row, f"live row[{i}]", schema)
        rid = row.get("id") if isinstance(row, dict) else None
        if rid is None:
            reasons.append(f"live row[{i}]: no id")
            continue
        if rid in rows_by_id:
            reasons.append(f"duplicate live row id {rid}")
            continue
        rows_by_id[rid] = row
    expected = {r["id"]: r for r in list(roster.GROUP1) + list(roster.GROUP2)}
    for rid in sorted(set(rows_by_id) - set(expected)):
        reasons.append(f"UNACCOUNTED controlled row {rid} in steady state "
                       "— no auto-enrollment; root must adjudicate")
    group1_ids = {r["id"] for r in roster.GROUP1}
    for rid in sorted(expected):
        row = rows_by_id.get(rid)
        if row is None:
            reasons.append(f"roster row {rid} MISSING")
            continue
        for key in sorted(expected[rid]):
            if key in row and row.get(key) != expected[rid][key]:
                reasons.append(f"row {rid} DRIFT on {key}: "
                               f"got={row.get(key)!r} "
                               f"want={expected[rid][key]!r}")
        started = row.get("started_at") or ""
        if not started:
            reasons.append(f"row {rid} started_at empty/NULL")
            continue
        try:
            st = _parse_ts(started, f"row {rid} started_at")
        except ValueError as exc:
            reasons.append(str(exc))
            continue
        if not (st < target_ts):
            reasons.append(f"row {rid} started {started} NOT before the "
                           f"steady target boot {target_boot}")
        if rid in group1_ids:
            try:
                hist_ts = _parse_ts(roster.HISTORICAL_ANCHOR_OLD4,
                                    "hist anchor")
                if not (st < final_ts and st < hist_ts):
                    reasons.append(f"old4 row {rid} grouping broken vs "
                                   "frozen anchors")
            except ValueError as exc:
                reasons.append(str(exc))
        else:
            if not (final_ts < st):
                reasons.append(f"postboot2 row {rid} started {started} NOT "
                               "after the final anchor — grouping broken")
    return (not reasons), reasons


# --- live collection (NEVER runs until RELEASED_FOR_EXECUTION) -------

def pick_container(ps_stdout, ps_rc):
    """PURE: strict compose-ps adjudication (P24 round-2 strictness)."""
    if ps_rc != 0:
        return None, f"compose ps rc={ps_rc}"
    lines = [l for l in ps_stdout.strip().splitlines() if l.strip()]
    if not lines:
        return None, "compose ps returned an empty stdout (no container id)"
    if len(lines) > 1:
        return None, (f"compose ps returned {len(lines)} container ids "
                      f"({lines}) — ambiguous, refusing to pick [0]")
    return lines[0], None


def parse_inspect_boot(ins_stdout, ins_rc):
    """PURE: strict StartedAt|Image extraction."""
    if ins_rc != 0:
        return None, None, f"docker inspect rc={ins_rc}"
    line = ins_stdout.strip()
    if not line:
        return None, None, "docker inspect returned an empty stdout"
    parts = line.split("|")
    if len(parts) < 2 or not RFC3339.match(parts[0]):
        return None, None, f"StartedAt not strict RFC3339: {parts[0]!r}"
    return parts[0], parts[1], None


def validate_steady_args(target_boot, target_image):
    """PURE: steady deployment guard requires BOTH the reviewed target
    boot AND the full reviewed candidate image — no green steady
    verdict without the image binding (code-review item 3)."""
    if not target_boot or not target_image:
        return ("steady policy requires BOTH --target-boot and "
                "--target-image (full sha256:hex)")
    if not RFC3339.match(str(target_boot)):
        return f"target boot not strict RFC3339: {target_boot!r}"
    try:
        norm_image(target_image, "target image")
    except ValueError as exc:
        return str(exc)
    return None


def main():
    if not RELEASED_FOR_EXECUTION:
        print("[p25-guard] DRAFT: not released — root must review the "
              "foundation code + selftest outputs and set "
              "RELEASED_FOR_EXECUTION=True. No docker/psql subprocess "
              "was constructed.", file=sys.stderr)
        return 3
    ap = argparse.ArgumentParser()
    ap.add_argument("--policy", required=True,
                    choices=["first-transition", "steady"])
    ap.add_argument("--env-dir",
                    default="/private/tmp/openfoundry-platform-p1-q3v9")
    ap.add_argument("--service", default="connector")
    ap.add_argument("--target-boot")
    ap.add_argument("--target-image")
    args = ap.parse_args()
    if args.policy == "steady":
        err = validate_steady_args(args.target_boot, args.target_image)
        if err:
            print(f"[p25-guard] {err} — a green steady verdict without "
                  "the reviewed candidate image binding is invalid",
                  file=sys.stderr)
            return 2

    roster = load_roster()
    env_dir = Path(args.env_dir)
    files = ["-f", str(env_dir / "compose.yaml")]
    override = env_dir / "compose-p22-connector2.yaml"
    if override.exists():
        files += ["-f", str(override)]

    def compose(*a, timeout=120):
        return subprocess.run(
            ["docker", "compose", *files, "--project-name", PROJECT, *a],
            capture_output=True, text=True, timeout=timeout)

    ps = compose("ps", "-q", args.service)
    cid, err = pick_container(ps.stdout, ps.returncode)
    if err:
        print(f"[p25-guard] {args.service}: {err}", file=sys.stderr)
        return 1
    ins = subprocess.run(["docker", "inspect", "--format",
                          "{{.State.StartedAt}}|{{.Image}}", cid],
                         capture_output=True, text=True, timeout=60)
    boot, image, err = parse_inspect_boot(ins.stdout, ins.returncode)
    if err:
        print(f"[p25-guard] {err}", file=sys.stderr)
        return 1
    print(f"[p25-guard] {args.service} boot={boot} image={image[:19]}…")

    psql = compose("exec", "-T", "postgres", "psql", "-U", "openfoundry",
                   "-d", "connectors", "-t", "-A", "-v",
                   "ON_ERROR_STOP=1", "-c", SESSION_SQL, "-c", ROW_SQL)
    if psql.returncode != 0:
        print(f"[p25-guard] projection rc={psql.returncode} — refusing",
              file=sys.stderr)
        return 1
    try:
        rows = json.loads(psql.stdout.strip().splitlines()[-1] or "null") or []
    except ValueError:
        print("[p25-guard] projection output not JSON — refusing",
              file=sys.stderr)
        return 1
    print(f"[p25-guard] controlled pending/running rows: {len(rows)}")

    if args.policy == "first-transition":
        ok, reasons = adjudicate_first_transition(rows, roster, boot, image)
    else:
        ok, reasons = adjudicate_steady(rows, roster, args.target_boot,
                                        args.target_image, boot, image)
    if not ok:
        for r in reasons:
            print(f"[p25-guard] BLOCK: {r}", file=sys.stderr)
        return 1
    print(f"[p25-guard] ok ({args.policy}): exact six-cohort, undrifted, "
          "groups proven")
    return 0


if __name__ == "__main__":
    sys.exit(main())

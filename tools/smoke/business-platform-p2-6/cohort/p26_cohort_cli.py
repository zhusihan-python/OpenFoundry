#!/usr/bin/env python3
"""P2.6 W6 preservation cohort CLI (sealed P25 evidence -> protected sets).

Everything DERIVED, never transcribed, from the FROZEN P25 final seal
(5016 files, seal sha256 a1e760b7…; complete-package bundle manifest sha
1fd0329e…). The P25 tree is only ever OPENED READ-ONLY; outputs are
written only under tools/smoke/business-platform-p2-6. Current-tree source
locks are NOT applied to P26 (P26 Go/SQL legitimately changed the tree);
the historical a1 source is exported by hash from the sealed bundle copy.

Modes:
  verify-seal       sha256(hash-manifest.json) == pinned seal; every
                    indexed file exists and hashes to the manifest
  derive-protected  protected9 Runs (original 6 + H10 held + final ABC2)
                    and 8CP owners (old-failed 2 + H10 + tail5) from the
                    postfacts/roster/abc/quiesce chain: exact-27-column
                    schema, parent chain, whole-set CP originals, unknown
                    rows REFUSED (never count-filtered)
  export-baseline   verify bundle manifest sha, read the sealed a1 source
                    hash pins, export the bundle's historical repo copy
                    by hash into p2-6 (read-only export)
  plan-quiet-checks current-quiet verdict plan from a docker-ps whitelist
                    snapshot; REMOVED P25 fixture/proxy URLs can never be
                    reported quiet
  plan-oldpg-readonly  emit the exact read-only SELECT argv (verbatim from
                    the sealed .cmd files) for later Root release — SQL is
                    NOT sent this round
"""

import argparse
import json
import re
import sys
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402

TOOL = "p26_cohort_cli"
OUT_ROOT = G.RESULTS_W6 / "cohort"

# Frozen 27-field safe projection (p24_live_guard ROW_SQL order, sorted):
# presence booleans only for holder/token — no secret values anywhere.
SCHEMA_27 = (
    "attempt", "cancel_requested", "conn_org_id", "control_mode",
    "def_org_id", "fixture_src", "finished_at", "gate_entered",
    "generation", "has_hash", "has_holder", "has_token", "has_version",
    "heartbeat_at", "id", "lease_expires_at", "org_id", "output_dataset_id",
    "owner_id", "requested_by", "run_org_id", "run_owner_id",
    "source_id", "source_name", "started_at", "status", "sync_def_id",
)
assert len(SCHEMA_27) == 27

# Composition pins (structural, independent of the checked files):
#   9 controlled rows = 4 fixture-pair groups {p21b-X, p21c-X} + 1 p25h13.
#   One pair group is the final ABC pair (its hex == abc-result suffix);
#   among the original 6, the legacy B/C pair is pinned by the frozen P24
#   GROUP2 run ids (lease-loss / committed-unconfirmed).
H10_TAG = "h10-held"
LEGACY_B_ID = "dcca308f-8d74-48b6-ae27-7f17fa2f30e2"
LEGACY_C_ID = "885d1429-53ea-43f5-a5a7-03d2ae0b4b8a"
PAIR_RE = re.compile(r"^p21([bc])-([0-9a-f]{8})$")
ROSTER_ORIGINS = {"failedhealthy": 2, "h10-prepublish-crash": 1,
                  "intentional-tail": 5}
ROSTER_TAGS = {"h11-no_txn_200", "h11-not_committed", "h11-wrong_target",
               "h11-http_500", "h14-drop", H10_TAG}
CONTROLLED_FILES = {
    "rows": "postfacts-attempts/attempt-1/01-controlled-rows.rows.json.stdout",
    "owners": "postfacts-attempts/attempt-1/02-cp-owners.owners.json.stdout",
}
ROSTER_FILE = "tail-owner-roster.json"
ABC_FILE = "abc-result.json"
QUIESCE_BEFORE = "owned-quiesce/attempt1/before.json"

# Verbatim read-only SELECT argv (from the sealed postfacts .cmd files —
# released later by Root; NOT executed by this tool in any mode).
OLDPG_READONLY_ARGV = {
    "controlled_rows_27col": [
        "docker", "compose", "-f",
        "/private/tmp/openfoundry-platform-p1-q3v9/compose.yaml",
        "--project-name", G.Q3_PROJECT, "exec", "-T", "postgres", "psql",
        "-U", "openfoundry", "-d", "connectors", "-t", "-A",
        "-v", "ON_ERROR_STOP=1", "-c", "SET TimeZone='UTC'; "
        "SET DateStyle='ISO, MDY';", "-c",
        "SELECT json_agg(row_to_json(t)) FROM ( SELECT r.id::text, "
        "r.sync_def_id::text, d.source_id::text, d.output_dataset_id::text, "
        "c.name AS source_name, (c.config->'sync'->'query_params'->>'src') "
        "AS fixture_src, c.owner_id::text, r.owner_id::text AS run_owner_id, "
        "r.requested_by::text, c.org_id::text AS conn_org_id, d.org_id::text "
        "AS def_org_id, r.org_id::text AS run_org_id, r.org_id::text AS "
        "org_id, r.status, r.control_mode, (r.lease_holder IS NOT NULL)::text "
        "AS has_holder, (r.lease_token IS NOT NULL)::text AS has_token, "
        "COALESCE(r.started_at::text,'') AS started_at, "
        "COALESCE(r.heartbeat_at::text,'') AS heartbeat_at, "
        "COALESCE(r.lease_expires_at::text,'') AS lease_expires_at, "
        "(r.cancel_requested_at IS NOT NULL)::text AS cancel_requested, "
        "(r.publish_entered_at IS NOT NULL)::text AS gate_entered, "
        "COALESCE(r.attempt::text,'') AS attempt, "
        "COALESCE(r.generation::text,'') AS generation, "
        "(r.content_hash IS NOT NULL)::text AS has_hash, "
        "(r.dataset_version_id IS NOT NULL)::text AS has_version, "
        "COALESCE(r.finished_at::text,'') AS finished_at FROM sync_runs r "
        "JOIN batch_sync_defs d ON d.id = r.sync_def_id JOIN connections c "
        "ON c.id = d.source_id WHERE r.control_mode = 'controlled' AND "
        "r.status IN ('pending','running')) t;",
    ],
    "cp_owners": [
        "docker", "compose", "-f",
        "/private/tmp/openfoundry-platform-p1-q3v9/compose.yaml",
        "--project-name", G.Q3_PROJECT, "exec", "-T", "postgres", "psql",
        "-U", "openfoundry", "-d", "connectors", "-t", "-A",
        "-v", "ON_ERROR_STOP=1", "-c",
        "SELECT json_agg(row_to_json(t)) FROM ( SELECT c.sync_def_id::text, "
        "c.source_id::text, c.output_dataset_id::text, c.mode, "
        "c.confirmed_epoch::text, c.active_run_id::text AS run_id, "
        "COALESCE(c.active_since::text,'') AS active_since, r.status AS "
        "run_status, CASE WHEN r.status='failed' OR (r.status='running' AND "
        "NOT (r.lease_expires_at IS NOT NULL AND r.lease_expires_at > "
        "clock_timestamp())) THEN 'requires_reconciliation' ELSE '' END AS "
        "blocked_reason FROM sync_def_checkpoints c LEFT JOIN sync_runs r "
        "ON r.id = c.active_run_id WHERE c.active_run_id IS NOT NULL ORDER "
        "BY c.sync_def_id) t;",
    ],
}


class CohortRefusal(Exception):
    pass


def die(msg):
    raise CohortRefusal(msg)


# ------------------------------------------------------------------ seal ---
def _load_manifest(final_dir: Path):
    hm_path = final_dir / "hash-manifest.json"
    if not hm_path.is_file():
        die(f"missing {hm_path}")
    hm = json.loads(hm_path.read_text())
    files = hm.get("files")
    if not isinstance(files, dict) or not files:
        die("hash-manifest has no files map")
    return hm_path, files


SEAL_ROOT_FILES = frozenset({"hash-manifest.json"})


def _member_path_safe(final_dir: Path, rel: str):
    """No absolute/traversal/symlink members (root round1 appendix)."""
    if not rel or rel.startswith("/") or ".." in Path(rel).parts:
        return f"unsafe member path: {rel!r}"
    p = final_dir / rel
    if p.is_symlink():
        return f"member is a symlink: {rel}"
    try:
        contained = p.resolve().is_relative_to(final_dir.resolve())
    except (OSError, ValueError):
        contained = False
    if not contained:
        return f"member escapes sealed dir: {rel}"
    return None


def verify_seal(final_dir: Path, expect_sha: str, expect_count: int,
                limit=None):
    if limit is not None and limit <= 0:
        die(f"--limit must be a positive sample size, got {limit}")
    hm_path, files = _load_manifest(final_dir)
    actual_seal = G.sha256_file(hm_path)
    problems = []
    if actual_seal != expect_sha:
        problems.append(f"seal sha256 {actual_seal} != expected {expect_sha}")
    if len(files) != expect_count:
        problems.append(f"manifest lists {len(files)} files, expected "
                        f"{expect_count}")
    for rel in sorted(files):
        unsafe = _member_path_safe(final_dir, rel)
        if unsafe:
            problems.append(unsafe)
    # actual DISK file set: extra files on disk matter (appendix)
    disk = {str(q.relative_to(final_dir)) for q in final_dir.rglob("*")
            if q.is_file()}
    extras = sorted(disk - set(files) - SEAL_ROOT_FILES)
    if extras:
        problems.append(f"EXTRA files on disk not in manifest: "
                        f"{extras[:5]}… ({len(extras)} total)")
    full = limit is None or limit >= len(files)
    checked = 0
    cap = len(files) if full else limit
    for rel, want in sorted(files.items()):
        if checked >= cap:
            break
        p = final_dir / rel
        if not p.is_file():
            problems.append(f"{rel}: missing from sealed dir")
        else:
            got = G.sha256_file(p)
            if got != want:
                problems.append(f"{rel}: sha256 {got} != manifest {want}")
        checked += 1
    partial = not full
    if partial and not problems:
        problems.append(f"PARTIAL SAMPLE ({checked}/{len(files)}) — never "
                        "a formal complete-seal ok")
    return {"final_dir": str(final_dir), "seal_sha256": actual_seal,
            "seal_ok": actual_seal == expect_sha,
            "files_total": len(files), "files_checked": checked,
            "partial": partial, "problems": problems,
            "ok": not problems}


def _manifest_verified_read(final_dir: Path, rel: str, files: dict):
    if rel not in files:
        die(f"{rel} is NOT indexed in the frozen hash-manifest — refusing")
    p = final_dir / rel
    if not p.is_file():
        die(f"{rel} missing from the frozen archive")
    if G.sha256_file(p) != files[rel]:
        die(f"{rel} sha256 mismatch vs manifest — seal broken, refusing")
    return p.read_text()


def _parse_rows_stdout(text, what):
    i = text.find("[")
    j = text.rfind("]")
    if i < 0 or j < i:
        die(f"{what}: no JSON array found")
    try:
        rows = json.loads(text[i:j + 1])
    except ValueError as e:
        die(f"{what}: unparseable JSON: {e}")
    if not isinstance(rows, list) or not rows:
        die(f"{what}: empty or non-list")
    return rows


def _check_row_schema(row, origin, idx):
    where = f"{origin} row[{idx}]"
    if not isinstance(row, dict):
        die(f"{where}: not an object")
    keys = set(row)
    want = set(SCHEMA_27)
    missing, extra = sorted(want - keys), sorted(keys - want)
    if missing or extra:
        die(f"{where}: schema violation missing={missing[:4]}… "
            f"extra={extra[:4]}… — partial records hard-refused")
    for k in ("id", "sync_def_id", "source_id", "output_dataset_id",
              "owner_id", "run_owner_id", "requested_by", "conn_org_id",
              "def_org_id", "run_org_id", "org_id"):
        if not G.UUID_RE.match(str(row[k] or "")):
            die(f"{where}: {k} not a UUID: {row[k]!r}")
    if not (row["conn_org_id"] == row["def_org_id"] == row["run_org_id"]
            == row["org_id"]):
        die(f"{where}: org parent chain inconsistent")


# ------------------------------------------------------ derive protected ---
def derive_protected(final_dir: Path, expect_sha: str, expect_count: int):
    """Derive protected9 + 8CP from the SHA-verified sealed chain."""
    _, files = _load_manifest(final_dir)
    if G.sha256_file(final_dir / "hash-manifest.json") != expect_sha:
        die("seal sha mismatch — refusing to derive")
    if len(files) != expect_count:
        die(f"sealed file count {len(files)} != {expect_count}")
    rows_text = _manifest_verified_read(final_dir, CONTROLLED_FILES["rows"],
                                        files)
    own_text = _manifest_verified_read(final_dir, CONTROLLED_FILES["owners"],
                                       files)
    roster_text = _manifest_verified_read(final_dir, ROSTER_FILE, files)
    abc_text = _manifest_verified_read(final_dir, ABC_FILE, files)
    before_text = _manifest_verified_read(final_dir, QUIESCE_BEFORE, files)

    rows = _parse_rows_stdout(rows_text, "controlled-rows")
    for i, r in enumerate(rows):
        _check_row_schema(r, "controlled-rows", i)
    if len(rows) != 9:
        die(f"controlled rows: {len(rows)} rows, expected exactly 9")
    for i, r in enumerate(rows):
        if r["status"] != "running" or r["control_mode"] != "controlled":
            die(f"controlled-rows row[{i}]: status/control_mode "
                f"{r['status']}/{r['control_mode']} — protected rows must "
                "still be held running")
        if r["has_holder"] != "true" or r["has_token"] != "true":
            die(f"controlled-rows row[{i}]: lost holder/token")

    abc = json.loads(abc_text)
    abc_suffix = abc.get("suffix")
    if not abc_suffix:
        die("abc-result: missing suffix")
    for k, v in (abc.get("results") or {}).items():
        if v.get("status") != "pass":
            die(f"abc-result: {k} status {v.get('status')} != pass")
    tcf = ((abc.get("evidence") or {}).get("terminal_commit_fault") or {})
    if tcf.get("txn_status") != "COMMITTED":
        die("abc-result: terminal_commit_fault txn not COMMITTED")

    pairs, h13 = {}, None
    for idx, r in enumerate(rows):
        fs = r["fixture_src"]
        m = PAIR_RE.match(fs or "")
        if m:
            side, hexpart = m.group(1), m.group(2)
            g = pairs.setdefault(hexpart, {})
            if side in g:
                die(f"pair group {hexpart}: duplicate side {side}")
            g[side] = r
        elif fs == "p25h13":
            if h13 is not None:
                die("multiple p25h13 rows")
            h13 = r
        else:
            die(f"controlled-rows row[{idx}]: unknown fixture_src {fs!r} "
                "— REFUSED, never filtered")
    if h13 is None:
        die("p25h13 (H10 held) row missing from controlled 9")
    if len(pairs) != 4:
        die(f"expected 4 fixture pair-groups (3 original + 1 ABC), got "
            f"{sorted(pairs)}")
    for hexpart, g in pairs.items():
        if set(g) != {"b", "c"}:
            die(f"pair group {hexpart}: sides {sorted(g)} != ['b','c']")
    if abc_suffix not in pairs:
        die(f"abc suffix {abc_suffix!r} has no pair group")
    abc_pair = pairs.pop(abc_suffix)
    legacy_hex = None
    for hexpart in list(pairs):
        ids = {pairs[hexpart]["b"]["id"], pairs[hexpart]["c"]["id"]}
        if ids == {LEGACY_B_ID, LEGACY_C_ID}:
            legacy_hex = hexpart
    if legacy_hex is None:
        die("legacy B/C pair (P24 GROUP2 run ids) not found in original 6")
    original6 = []
    for hexpart in sorted(pairs):
        original6 += [pairs[hexpart]["b"], pairs[hexpart]["c"]]
    protected9 = original6 + [h13, abc_pair["b"], abc_pair["c"]]

    # logical_path of the ABC commit fault must carry the c-side run id
    lp = tcf.get("logical_path") or ""
    if abc_pair["c"]["id"] not in lp:
        die(f"abc logical_path {lp!r} does not carry ABC c-side run "
            f"{abc_pair['c']['id']}")

    # --- roster 8 CP owners ------------------------------------------------
    try:
        roster = json.loads(roster_text)
    except ValueError as e:
        die(f"tail-owner-roster: {e}")
    if not isinstance(roster, list) or len(roster) != 8:
        die(f"tail-owner-roster: {len(roster)} entries, expected exactly 8")
    origins = {}
    for e in roster:
        if e.get("kind") != "owner":
            die(f"roster entry {e.get('run_id')}: kind {e.get('kind')!r} "
                "!= owner — refused")
        o = e.get("origin")
        origins[o] = origins.get(o, 0) + 1
        if o not in ROSTER_ORIGINS:
            die(f"roster entry {e.get('run_id')}: unknown origin {o!r}")
        tag = e.get("tag") or ""
        if tag == H10_TAG:
            pass
        elif tag.startswith("failedhealthy-h13-"):
            pass
        elif tag in ROSTER_TAGS:
            pass
        else:
            die(f"roster entry {e.get('run_id')}: unknown tag {tag!r}")
        for k in ("def_id", "source_id", "run_id"):
            if not G.UUID_RE.match(str(e.get(k) or "")):
                die(f"roster entry: {k} not a UUID: {e.get(k)!r}")
    if origins != ROSTER_ORIGINS:
        die(f"roster origins {origins} != required {ROSTER_ORIGINS}")

    h10_entries = [e for e in roster if e["tag"] == H10_TAG]
    if len(h10_entries) != 1:
        die("roster must carry exactly one h10-held entry")
    if h10_entries[0]["run_id"] != h13["id"]:
        die("h10-held roster run_id != controlled p25h13 run id")
    if h10_entries[0]["def_id"] != h13["sync_def_id"]:
        die("h10-held def mismatch vs controlled row parent chain")

    # --- roster-8 uniqueness (appendix: exactly 8 unique runs AND defs) --
    roster_runs = {e["run_id"] for e in roster}
    roster_defs = {e["def_id"] for e in roster}
    if len(roster_runs) != 8:
        die(f"roster has {len(roster_runs)} unique run ids, want exactly 8")
    if len(roster_defs) != 8:
        die(f"roster has {len(roster_defs)} unique def ids, want exactly 8")

    # --- quiesce before.json (parsed FIRST: 8CP originals join it) -------
    # before.json covers the OWNED defs snapshotted at quiesce time (31 in
    # the real seal); ALL 8 roster defs are present; protected9's H10 def
    # is in that population, the other 8 fixture-run defs are not (their
    # chains live in other sealed files).
    try:
        before = json.loads(before_text)
    except ValueError as e:
        die(f"owned-quiesce before.json: {e}")
    if not isinstance(before, dict) or not before:
        die("owned-quiesce before.json: empty or not an object")

    # --- CP owners EXACT whole set (appendix: no protected9 exemption) ---
    owners = _parse_rows_stdout(own_text, "cp-owners")
    by_run = {}
    by_def = {}
    for i, o in enumerate(owners):
        for k in ("sync_def_id", "source_id", "output_dataset_id",
                  "run_id"):
            if not G.UUID_RE.match(str(o.get(k) or "")):
                die(f"cp-owners row[{i}]: {k} not a UUID: {o.get(k)!r}")
        rid = o["run_id"]
        if rid in by_run:
            die(f"cp-owners: duplicate run_id {rid}")
        by_run[rid] = o
        if o["sync_def_id"] in by_def:
            die(f"cp-owners: duplicate def_id {o['sync_def_id']}")
        by_def[o["sync_def_id"]] = o
    if set(by_run) != roster_runs:
        extra = sorted(set(by_run) - roster_runs)
        missing = sorted(roster_runs - set(by_run))
        die(f"cp-owners run set != roster-8 run set (whole-set rule; "
            f"extra={extra} missing={missing}) — an extra owner is a "
            "refusal regardless of who it is")
    if set(by_def) != roster_defs:
        die(f"cp-owners def set != roster-8 def set")

    # --- 8CP FULL originals: roster entry + full owner row + complete
    #     public checkpoint from sealed before.json + matched public Run --
    cp8 = []
    for e in roster:
        o = by_run.get(e["run_id"])
        if o is None:
            die(f"roster run {e['run_id']} has no CP owner row")
        if o.get("blocked_reason") != "requires_reconciliation":
            die(f"CP owner of {e['run_id']}: blocked_reason "
                f"{o.get('blocked_reason')!r}")
        if (o["sync_def_id"] != e["def_id"]
                or o["source_id"] != e["source_id"]):
            die(f"CP owner of {e['run_id']}: def/source parent chain "
                "mismatch vs roster")
        d = before.get(e["def_id"])
        if not isinstance(d, dict):
            die(f"roster def {e['def_id']} missing from sealed "
                "before.json — ALL 8 expected owners must be captured")
        ck = d.get("checkpoint")
        if not isinstance(ck, dict) or not {"initialized", "mode",
                                            "confirmed_epoch", "owner"} <=                 set(ck):
            die(f"before.json checkpoint of def {e['def_id']} lacks the "
                "complete public fields (initialized/mode/confirmed_epoch/"
                "owner)")
        matched_run = next((r for r in (d.get("runs") or [])
                            if r.get("id") == e["run_id"]), None)
        if matched_run is None:
            die(f"roster run {e['run_id']} not present under its def in "
                "before.json")
        # NOTE: checkpoint.owner (CP owner identity) and the matched
        # run's owner_id are DIFFERENT concepts in the sealed legacy data
        # (legitimately unequal); both are preserved as facts, not
        # asserted equal (appendix requires completeness + join, not
        # owner equality).
        cp8.append({"def_id": e["def_id"], "source_id": e["source_id"],
                    "run_id": e["run_id"], "origin": e["origin"],
                    "tag": e["tag"],
                    "cp_owner_row_full": o,
                    "quiesce_checkpoint_full": ck,
                    "matched_public_run": matched_run})
    quiesce_chain = []
    for r in protected9:
        d = before.get(r["sync_def_id"])
        if d is None:
            quiesce_chain.append({"run_id": r["id"],
                                  "in_quiesce_before": False})
            continue
        if not isinstance(d, dict):
            die(f"quiesce before: def {r['sync_def_id']} malformed")
        run_ids = {x.get("id") for x in (d.get("runs") or [])}
        if r["id"] not in run_ids:
            die(f"quiesce before: run {r['id']} missing under its def — "
                "parent chain broken")
        quiesce_chain.append({"run_id": r["id"],
                              "in_quiesce_before": True,
                              "checkpoint": d.get("checkpoint")})

    pub = lambda r: {k: r[k] for k in SCHEMA_27}  # noqa: E731
    return {
        "seal": {"final_dir": str(final_dir), "seal_sha256": expect_sha,
                 "files": expect_count},
        "protected9": [{"run_id": r["id"], "def_id": r["sync_def_id"],
                        "source_id": r["source_id"],
                        "output_dataset_id": r["output_dataset_id"],
                        "origin": ("h10-held" if r is h13 else
                                   "abc-final" if r in (abc_pair["b"],
                                                        abc_pair["c"])
                                   else "original6"),
                        "fixture_src": r["fixture_src"],
                        "public_projection_27": pub(r)}
                       for r in protected9],
        "cp8": cp8,
        "quiesce_before_chain": quiesce_chain,
        "composition": {"protected9": "original6 + h10-held + abc-final2",
                        "cp8": "failedhealthy x2 + h10-held + "
                               "intentional-tail x5",
                        "origins": origins,
                        "abc_suffix": abc_suffix,
                        "legacy_bc_hex": legacy_hex},
        "h10_run_id": h13["id"],
    }


# ------------------------------------------------------------- baseline ----
def export_baseline(bundle_dir: Path, expect_bundle_sha: str, out_dir: Path):
    out_dir = Path(out_dir).resolve()
    if G.ROOT_P25 in out_dir.parents or out_dir == G.ROOT_P25:
        die("output under the frozen P25 tree refused")
    if G.ROOT_P26 not in out_dir.parents:
        die("baseline export output must stay under business-platform-p2-6")
    bm_path = bundle_dir / "bundle-hash-manifest.json"
    if not bm_path.is_file():
        die(f"missing {bm_path}")
    bm_sha = G.sha256_file(bm_path)
    if bm_sha != expect_bundle_sha:
        die(f"bundle manifest sha {bm_sha} != expected {expect_bundle_sha}")
    bm = json.loads(bm_path.read_text())
    files = bm["files"]
    for rel in files:
        unsafe = _member_path_safe(bundle_dir, rel)
        if unsafe:
            die(f"bundle manifest member unsafe: {unsafe}")
    if out_dir.exists() and not out_dir.is_dir():
        die("baseline out path exists and is not a directory")

    # sealed a1 source-hash pins: the FINAL accepted attempt per phase
    # (new-normal / new-race / unified, compile-* excluded — pre-acceptance
    # attempts legitimately carry other hashes and stay historical facts).
    # Latest attempt (max UTC name suffix) per phase must agree on the pin.
    phase_latest = {}
    for rel, want in files.items():
        m = re.match(r"^dependencies/evidence/pg/(new-normal|new-race|"
                     r"unified)_(\d{8}T\d{6}Z(?:-\d+)?)\.meta$", rel)
        if not m:
            continue
        p = bundle_dir / rel
        if G.sha256_file(p) != want:
            die(f"{rel}: hash mismatch vs bundle manifest")
        pins = {}
        for ln in p.read_text().splitlines():
            if ln.startswith("source_content_sha256_before="):
                pins["before"] = ln.split("=", 1)[1]
            if ln.startswith("source_content_sha256_after="):
                pins["after"] = ln.split("=", 1)[1]
        phase, stamp = m.group(1), m.group(2)
        if phase not in phase_latest or stamp > phase_latest[phase][0]:
            phase_latest[phase] = (stamp, pins)
    if set(phase_latest) != {"new-normal", "new-race", "unified"}:
        die(f"bundle lacks final metas for all three phases; found "
            f"{sorted(phase_latest)}")
    a1_pins = {f"{ph}_{st}": pins
               for ph, (st, pins) in phase_latest.items()}
    shas = {v.get("before") for v in a1_pins.values()} | \
           {v.get("after") for v in a1_pins.values()}
    if None in shas or len(shas - {None}) != 1:
        die(f"final-attempt source pins disagree: {shas}")
    a1 = (shas - {None}).pop()
    if a1 != G.EXPECTED_P25_A1_SOURCE_SHA:
        die(f"sealed a1 {a1} != pinned {G.EXPECTED_P25_A1_SOURCE_SHA}")

    exported = {}
    for rel, want in sorted(files.items()):
        if not rel.startswith("dependencies/repo/"):
            continue
        p = bundle_dir / rel
        if G.sha256_file(p) != want:
            die(f"{rel}: hash mismatch vs bundle manifest")
        tail = rel[len("dependencies/repo/"):]
        if not tail or ".." in Path(tail).parts:
            die(f"unsafe repo member tail: {rel!r}")
        dst = (out_dir / tail)
        try:
            contained = dst.resolve().is_relative_to(out_dir.resolve())
        except (OSError, ValueError):
            contained = False
        if not contained:
            die(f"destination escapes out_dir: {rel}")
        if dst.exists():
            die(f"refusing to overwrite pre-existing target: {dst}")
        dst.parent.mkdir(parents=True, exist_ok=True)
        data = p.read_bytes()
        dst.write_bytes(data)
        exported[str(dst)] = G.sha256_bytes(data)
    G.write_json(out_dir / "BASELINE-EXPORT-MANIFEST.json",
                 {"bundle": str(bundle_dir), "bundle_manifest_sha256":
                  bm_sha, "a1_source_sha256_sealed": a1,
                  "a1_pins": a1_pins, "exported_files": len(exported),
                  "note": "read-only export of the historical a1 source "
                          "copy carried by the sealed P25 complete package; "
                          "NOT a claim about the current tree"})
    return {"ok": True, "a1_source_sha256": a1,
            "exported_files": len(exported), "out_dir": str(out_dir),
            "bundle_manifest_sha256": bm_sha}


# ---------------------------------------------------------------- quiet ----
def plan_quiet_checks(input_path: Path):
    """Input schema: {docker_ps_lines: ["<id>\\t<name>\\t<image>\\t<status>"],
    removed_p25_url_patterns: [..], candidate_urls: [..],
    expected_base_containers: [..], expected_fixture: str}.

    Rule: only containers PRESENT in the whitelist snapshot can be quiet;
    URLs hitting REMOVED P25 fixture/proxy are 'removed_not_quiet' and can
    never be counted as current quiet evidence.
    """
    spec = json.loads(Path(input_path).read_text())
    lines = spec.get("docker_ps_lines") or []
    inventory = {}
    for ln in lines:
        parts = ln.split("\t")
        if len(parts) >= 4:
            inventory[parts[1]] = {"id": parts[0], "image": parts[2],
                                   "status": parts[3]}
    verdicts = {}
    for url in spec.get("candidate_urls") or []:
        removed = any(p in url for p in
                      spec.get("removed_p25_url_patterns") or [])
        if removed:
            verdicts[url] = "removed_not_quiet(NEVER green)"
            continue
        verdicts[url] = ("unknown_not_quiet(no inventory match — needs "
                         "container-backed proof)")
    missing_base = [c for c in spec.get("expected_base_containers") or []
                    if c not in inventory]
    fx = spec.get("expected_fixture")
    return {"input": str(input_path),
            "inventory_names": sorted(inventory),
            "missing_expected_base": missing_base,
            "fixture_present": (fx in inventory) if fx else None,
            "url_verdicts": verdicts,
            "planned_reads_after_release": [
                ["docker", "ps", "--format",
                 "{{.ID}}\\t{{.Names}}\\t{{.Image}}\\t{{.Status}}"]],
            "rule": "removed P25 fixture/proxy can never be current quiet"}


def main(argv=None):
    ap = argparse.ArgumentParser(prog=TOOL)
    ap.add_argument("mode", choices=["verify-seal", "derive-protected",
                                     "export-baseline", "plan-quiet-checks",
                                     "plan-oldpg-readonly"])
    ap.add_argument("--final-dir", default=str(G.P25_FINAL_DIR))
    ap.add_argument("--bundle-dir", default=str(G.P25_BUNDLE_DIR))
    ap.add_argument("--seal-sha", default=G.EXPECTED_SEAL_SHA256)
    ap.add_argument("--files-count", type=int,
                    default=G.EXPECTED_SEALED_FILES)
    ap.add_argument("--bundle-sha",
                    default=G.EXPECTED_BUNDLE_MANIFEST_SHA256)
    ap.add_argument("--limit", type=int, default=None,
                    help="verify-seal: check only first N manifest files "
                         "(self-test aid; full sweep is the released form)")
    ap.add_argument("--allow-alternate-dir", action="store_true",
                    help="permit a non-pinned final-dir (synthetic "
                         "self-test fixtures only; recorded in output)")
    ap.add_argument("--out", required=True,
                    help="output JSON path (must be under p2-6)")
    ap.add_argument("--baseline-out-dir")
    args = ap.parse_args(argv)

    out = Path(args.out).resolve()
    if G.ROOT_P25 in out.parents or out == G.ROOT_P25:
        print("[p26-cohort] REFUSED: output inside frozen P25 tree",
              file=sys.stderr)
        return 2
    if G.ROOT_P26 not in out.parents and args.mode != "plan-quiet-checks":
        print("[p26-cohort] REFUSED: output outside business-platform-p2-6",
              file=sys.stderr)
        return 2

    final_dir = Path(args.final_dir).resolve()
    alt_note = {}
    seal_modes = {"verify-seal", "derive-protected", "export-baseline"}
    if (args.mode in seal_modes and final_dir != G.P25_FINAL_DIR
            and not args.allow_alternate_dir):
        print("[p26-cohort] REFUSED: --final-dir must be the pinned P25 "
              "seal path unless --allow-alternate-dir (self-test only)",
              file=sys.stderr)
        return 2
    if args.allow_alternate_dir and final_dir != G.P25_FINAL_DIR:
        alt_note = {"alternate_final_dir": str(final_dir),
                    "reason": "synthetic self-test fixture"}

    try:
        if args.mode == "verify-seal":
            res = verify_seal(final_dir, args.seal_sha, args.files_count,
                              args.limit)
        elif args.mode == "derive-protected":
            res = derive_protected(final_dir, args.seal_sha,
                                   args.files_count)
            res.update(alt_note)
        elif args.mode == "export-baseline":
            if not args.baseline_out_dir:
                print("[p26-cohort] export-baseline needs "
                      "--baseline-out-dir", file=sys.stderr)
                return 2
            res = export_baseline(Path(args.bundle_dir), args.bundle_sha,
                                  Path(args.baseline_out_dir))
        elif args.mode == "plan-quiet-checks":
            res = plan_quiet_checks(Path(args.final_dir))
            G.write_json(out, res)
            print(json.dumps(res, indent=1, sort_keys=True)[:4000])
            return 0
        else:  # plan-oldpg-readonly
            res = {"header": G.plan_header(TOOL, "oldpg-readonly"),
                   "argv_by_check": OLDPG_READONLY_ARGV,
                   "constraints": [
                       "SELECT-only verbatim from sealed postfacts .cmd "
                       "files; -t -A; ON_ERROR_STOP=1; TimeZone UTC",
                       "NOT executed this round; needs explicit Root "
                       "release; SQL is only LISTED here",
                       "read-only against q3 postgres container via "
                       "docker compose exec; no Config.Env read, no "
                       "secrets in output"],
                   }
    except CohortRefusal as e:
        G.write_json(out, {"ok": False, "refused": str(e),
                           **alt_note})
        print(f"[p26-cohort] REFUSED: {e}", file=sys.stderr)
        return 1

    res.setdefault("ok", True)
    G.write_json(out, res)
    print(f"[p26-cohort] {args.mode}: ok={res.get('ok')} -> {out}")
    return 0 if res.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())

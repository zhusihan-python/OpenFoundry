#!/usr/bin/env python3
"""P2.5 six-cohort roster DERIVER (foundation, root round-1 released).

Generates p25_cohort6.py from SHA-bound frozen P24 evidence. DERIVED,
never transcribed: every input file must (a) be indexed in the P24
final 859 hash-manifest and (b) hash exactly to the manifest value;
the tool-chain chain (p24_pins.py via 16-tools-sha-final.json) is
verified the same way. Any partial row, missing/extra key, wrong SHA,
unknown id, bad type, unparseable time, anchor mismatch, or group
inconsistency aborts non-zero BEFORE anything is written.

Root round-1 rulings implemented here:
  * transition anchor = P24 FINAL connector boot 2026-11-14T07:11:20.
    883030213Z + FULL image sha256:fe3208b3fcbd… (15-final-base3-
    images.json); NOT the Nov13T22:05 preflight boot, which stays a
    group-1-only historical provenance anchor (preflight-verdict-r1.
    json connector_boot).
  * exact-27-key schema enforced on BOTH source sides (p24_pins rows
    and FINAL-NEW-TWO-PROVENANCE rows) — a partial record can never
    weaken the check the way unexecuted-DRAFT cohort_gate_p24.py
    could (it compared only the keys a record happened to carry).
  * owned-history binding (17 defs / owned_history_sha256 / p24
    result sha) is re-verified from 13-persist-post-r1 manifest.
  * postboot2 B/C semantics + live-C artifact values re-verified.

Output is written atomically (tmp file + os.replace) and re-validated
(ast.parse + fresh import + literal equality) after writing.

Usage (python3 -I):
  p25_cohort_derive.py --p24-final-dir DIR --p24-tools-dir DIR \
      [--out FILE] [--check] [--emit-frozen-baseline FILE]
--check: re-derive everything and compare against the existing output
module (literal equality); tampered/stale output exits 1.
"""

import argparse
import ast
import hashlib
import importlib.util
import json
import os
import re
import sys
import tempfile
from pathlib import Path

# Fixed root-reviewed seal locations (round-2 item 4): the FORMAL
# verifier/guard rederive every expectation from THESE paths; every
# file inside is content-hash anchored, so pointing them elsewhere
# cannot forge expectations without failing the chain.
SEAL_P24_FINAL_DEFAULT = ("/Volumes/ExternalSD/projects/OpenFoundry/"
                          "tools/smoke/business-platform-p2-4/results/"
                          "2026-11-14-p24-final")
SEAL_P24_TOOLS_DEFAULT = ("/Volumes/ExternalSD/projects/OpenFoundry/"
                          "tools/smoke/business-platform-p2-4")

# --- root-pinned expected constants (verified against files below).
# NOTE (code review lesson): these are SINGLE-LINE literals — hand-split
# continuation strings corrupted two of them already (dropped chars);
# never split them again.
EXPECTED_FINAL_BOOT = "2026-11-14T07:11:20.883030213Z"
EXPECTED_FINAL_IMAGE_HEX = "fe3208b3fcbd4d5fe428647e106d6ba6aa5b71fd94e9ba51144a9565258dc4c3"
EXPECTED_HIST_BOOT = "2026-11-13T22:05:01.092139798Z"
EXPECTED_FIXTURE_BOOT = "2026-11-13T18:29:14.176785055Z"
EXPECTED_OWNED_HISTORY_SHA = "8c18999a723fef3fe819612a33d821fc7731aad16c0634506a5bc0a622a18001"
EXPECTED_P24_RESULT_SHA = "3beb1bce97934e214239fe44126e44ae0cda8e0c82a1f55c76b0441e151a23d0"
EXPECTED_HASH_MANIFEST_SHA = "a27e7483fdf04d8649b468a675a5a80e6e1586fdf51d53f06cd26fada3382a6f"
EXPECTED_SOURCE_COMBINED = "8cd25d4220a6bdae843a693e1c1afa928991f537f5902a31e3e970704db6386b"
EXPECTED_OWNED_IDS_LEN = 17
EXPECTED_C_TXN_STATUS = "COMMITTED"
EXPECTED_C_DOWNLOAD_HTTP = 200
EXPECTED_C_BYTES = 22766
EXPECTED_C_SHA256 = "7e69fbf30a774211f7d13a7cfc7a57b9340edee5c0e07d0459979df1c36c6a92"

GROUP2_B_ID = "dcca308f-8d74-48b6-ae27-7f17fa2f30e2"   # lease-loss
GROUP2_C_ID = "885d1429-53ea-43f5-a5a7-03d2ae0b4b8a"   # committed-unconfirmed

# The frozen 27-field safe projection (p24_live_guard ROW_SQL order,
# sorted): presence booleans only for holder/token; no secret values.
SCHEMA_27 = (
    "attempt", "cancel_requested", "conn_org_id", "control_mode",
    "def_org_id", "fixture_src", "finished_at", "gate_entered",
    "generation", "has_hash", "has_holder", "has_token", "has_version",
    "heartbeat_at", "id", "lease_expires_at", "org_id", "output_dataset_id",
    "owner_id", "requested_by", "run_org_id", "run_owner_id",
    "source_id", "source_name", "started_at", "status", "sync_def_id",
)
assert len(SCHEMA_27) == 27

RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")


def die(msg):
    print(f"[p25-derive] ABORT: {msg}", file=sys.stderr)
    sys.exit(1)


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def norm_image(value) -> str:
    """Normalize to bare 64-hex; reject anything short/odd."""
    v = str(value or "").strip()
    if v.startswith("sha256:"):
        v = v[len("sha256:"):]
    if not HEX64.match(v):
        raise ValueError(f"not a full 64-hex image id: {value!r}")
    return v


def parse_ts(value, what):
    """Same normalization family as p24_live_guard._parse_ts."""
    v = str(value).strip().replace(" ", "T")
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    elif v.endswith("+00"):
        v = v[:-3] + "+00:00"
    from datetime import datetime
    try:
        return datetime.fromisoformat(v)
    except ValueError as exc:
        die(f"{what}: unparseable timestamp {value!r}: {exc}")


def verify_manifest_indexed(final_dir: Path, rel: str, manifest: dict) -> str:
    """File must exist, be manifest-indexed, and hash to the manifest value."""
    if rel not in manifest["files"]:
        die(f"{rel} is NOT indexed in the frozen hash-manifest — refusing")
    path = final_dir / rel
    if not path.is_file():
        die(f"{rel} missing from the frozen archive")
    actual = sha256_file(path)
    if actual != manifest["files"][rel]:
        die(f"{rel} sha256 {actual} != manifest {manifest['files'][rel]}")
    return actual


def validate_row_schema(row, origin, idx):
    where = f"{origin} row[{idx}]"
    if not isinstance(row, dict):
        die(f"{where}: not an object")
    keys = set(row.keys())
    want = set(SCHEMA_27)
    missing = sorted(want - keys)
    extra = sorted(keys - want)
    if missing:
        die(f"{where}: MISSING {len(missing)} schema key(s): {missing[:5]}…"
            " — partial records are hard-refused")
    if extra:
        die(f"{where}: EXTRA {len(extra)} key(s): {extra[:5]}…")
    for k in SCHEMA_27:
        if not isinstance(row[k], str):
            die(f"{where}: key {k} is {type(row[k]).__name__}, want str")
        # Non-terminal rows carry '' finished_at; every OTHER key must be
        # a non-empty string in the frozen evidence.
        if row[k] == "" and k not in ("finished_at",):
            die(f"{where}: key {k} unexpectedly empty")
    if row["started_at"] == "":
        die(f"{where}: started_at empty (NULL) — hard-refused")
    parse_ts(row["started_at"], f"{where} started_at")


def load_pins(p24_tools_dir: Path, tools_sha: dict) -> list:
    pins_path = p24_tools_dir / "p24_pins.py"
    if not pins_path.is_file():
        die(f"p24_pins.py missing: {pins_path}")
    actual = sha256_file(pins_path)
    recorded = tools_sha["files"].get("p24_pins.py")
    if recorded is None:
        die("p24_pins.py is not covered by 16-tools-sha-final.json")
    if actual != recorded:
        die(f"p24_pins.py sha {actual} != toolSHA {recorded}")
    spec = importlib.util.spec_from_file_location("p24_pins_src", pins_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    pins = getattr(mod, "PINNED_EXCEPTIONS", None)
    if not isinstance(pins, list) or len(pins) != 4:
        die(f"p24_pins.PINNED_EXCEPTIONS must be exactly 4 rows, "
            f"got {len(pins) if isinstance(pins, list) else type(pins)}")
    for i, row in enumerate(pins):
        validate_row_schema(row, "p24_pins(old4)", i)
    return pins


def derive(p24_final_dir: Path, p24_tools_dir: Path) -> dict:
    final_dir = p24_final_dir.resolve()
    tools_dir = p24_tools_dir.resolve()

    # 1. archive seal: the manifest itself must hash to the P24-final
    #    recorded value, then every input is index+hash verified.
    hm_path = final_dir / "hash-manifest.json"
    if not hm_path.is_file():
        die(f"hash-manifest.json missing under {final_dir}")
    if sha256_file(hm_path) != EXPECTED_HASH_MANIFEST_SHA:
        die("hash-manifest.json does not match the P24-final recorded "
            f"sha {EXPECTED_HASH_MANIFEST_SHA[:12]}… — wrong archive?")
    manifest = json.loads(hm_path.read_text())
    if not isinstance(manifest.get("files"), dict) or not manifest["files"]:
        die("hash-manifest.json files map missing/empty")

    shas = {}
    for rel in ("FINAL-NEW-TWO-PROVENANCE.json",
                "15-final-base3-images.json",
                "00-preflight/preflight-verdict-r1.json",
                "13-persist-post-r1/persist-manifest.json",
                "16-tools-sha-final.json",
                "01-source-hash-pre.txt"):
        shas[rel] = verify_manifest_indexed(final_dir, rel, manifest)

    tools_sha = json.loads((final_dir / "16-tools-sha-final.json").read_text())
    if not isinstance(tools_sha.get("files"), dict):
        die("16-tools-sha-final.json has no files map")

    # 2. old4 via the toolSHA-verified pins module.
    group1 = [dict(r) for r in load_pins(tools_dir, tools_sha)]

    # 3. postboot2 via the provenance json (manifest-verified above).
    prov = json.loads((final_dir / "FINAL-NEW-TWO-PROVENANCE.json").read_text())
    rows = prov.get("rows")
    if not isinstance(rows, dict) or len(rows) != 2:
        die(f"provenance rows must be exactly 2, got "
            f"{len(rows) if isinstance(rows, dict) else type(rows)}")
    group2 = []
    for i, (rid, row) in enumerate(sorted(rows.items())):
        validate_row_schema(row, "provenance(postboot2)", i)
        if row["id"] != rid:
            die(f"provenance row key {rid} != row.id {row['id']}")
        group2.append(dict(row))

    g1_ids = {r["id"] for r in group1}
    g2_ids = {r["id"] for r in group2}
    if len(g1_ids) != 4:
        die("old4 ids are not 4 distinct values")
    if g1_ids & g2_ids:
        die(f"postboot2 collides with old4: {sorted(g1_ids & g2_ids)}")
    if g2_ids != {GROUP2_B_ID, GROUP2_C_ID}:
        die(f"postboot2 ids must be exactly B {GROUP2_B_ID} / C "
            f"{GROUP2_C_ID}, got {sorted(g2_ids)}")

    # 4. transition anchor (final boot + FULL image) + aux absence.
    images = json.loads((final_dir / "15-final-base3-images.json").read_text())
    running = images.get("running")
    if not isinstance(running, dict):
        die("15-final-base3-images.json has no running map")
    conn = running.get("connector") or {}
    try:
        final_image = norm_image(conn.get("image"))
        dataset_image = norm_image(running["dataset"]["image"])
        pipeline_image = norm_image(running["pipeline"]["image"])
    except (KeyError, ValueError) as exc:
        die(f"final-base3-images image field invalid: {exc}")
    final_boot = str(conn.get("started_at") or "")
    if not RFC3339.match(final_boot):
        die(f"connector started_at not strict RFC3339: {final_boot!r}")
    if final_boot != EXPECTED_FINAL_BOOT or final_image != EXPECTED_FINAL_IMAGE_HEX:
        die(f"final anchor drift: boot {final_boot} image {final_image}")
    if str(conn.get("running")).lower() != "true":
        die("frozen final images file does not show connector running")
    aux_absent = {}
    for svc in ("connector2(absent)", "connector3(absent)",
                "dataset-proxy(absent)"):
        entry = running.get(svc) or {}
        containers = entry.get("containers")
        if containers != 0:
            die(f"aux service {svc} containers != 0 in frozen evidence")
        aux_absent[svc] = containers

    # 5. group-1 historical provenance anchor (preflight verdict).
    verdict = json.loads(
        (final_dir / "00-preflight/preflight-verdict-r1.json").read_text())
    hist_boot = verdict.get("connector_boot")
    if hist_boot != EXPECTED_HIST_BOOT:
        die(f"historical connector_boot {hist_boot!r} != expected "
            f"{EXPECTED_HIST_BOOT!r}")

    # 6. owned-history frozen binding.
    persist = json.loads(
        (final_dir / "13-persist-post-r1/persist-manifest.json").read_text())
    owned_sha = persist.get("owned_history_sha256")
    if owned_sha != EXPECTED_OWNED_HISTORY_SHA:
        die(f"owned_history_sha256 {owned_sha} != expected")
    owned_ids = persist.get("owned_ids")
    if not isinstance(owned_ids, list) or len(owned_ids) != EXPECTED_OWNED_IDS_LEN:
        die(f"owned_ids must be {EXPECTED_OWNED_IDS_LEN}, got "
            f"{len(owned_ids) if isinstance(owned_ids, list) else 'non-list'}")
    result_sha = persist.get("p24_result_sha256")
    if result_sha != EXPECTED_P24_RESULT_SHA:
        die(f"p24_result_sha256 {result_sha} != expected")

    # 7. group semantics + ordering vs anchors (real datetimes).
    fb = parse_ts(final_boot, "final anchor")
    hb = parse_ts(hist_boot, "historical anchor")
    for r in group1:
        st = parse_ts(r["started_at"], f"old4 {r['id']}")
        if not (st < hb):
            die(f"old4 {r['id']} started {r['started_at']} NOT before the "
                f"historical anchor {hist_boot}")
        if not (st < fb):
            die(f"old4 {r['id']} started {r['started_at']} NOT before the "
                f"final anchor {final_boot}")
    by_id2 = {r["id"]: r for r in group2}
    b_row, c_row = by_id2[GROUP2_B_ID], by_id2[GROUP2_C_ID]
    for r in group2:
        st = parse_ts(r["started_at"], f"postboot2 {r['id']}")
        if not (fb < st):
            die(f"postboot2 {r['id']} started {r['started_at']} is NOT "
                f"after the final anchor {final_boot} — grouping broken")
    if b_row["gate_entered"] != "false":
        die("postboot2 B gate_entered != false (lease-loss semantics)")
    if c_row["gate_entered"] != "true":
        die("postboot2 C gate_entered != true (committed-unconfirmed)")
    bl = parse_ts(b_row["lease_expires_at"], "B lease_expires_at")
    bh = parse_ts(b_row["heartbeat_at"], "B heartbeat_at")
    if not (bl < bh):
        die("B lease_expires_at not before heartbeat_at (lease-loss)")

    # 8. live-C artifact values re-verified.
    c_live = prov.get("c_target_live") or {}
    if c_live.get("txn_status") != EXPECTED_C_TXN_STATUS:
        die(f"C txn_status {c_live.get('txn_status')!r} != COMMITTED")
    if c_live.get("download_http") != EXPECTED_C_DOWNLOAD_HTTP:
        die(f"C download_http {c_live.get('download_http')!r} != 200")
    if c_live.get("bytes") != EXPECTED_C_BYTES:
        die(f"C bytes {c_live.get('bytes')!r} != {EXPECTED_C_BYTES}")
    if c_live.get("sha256") != EXPECTED_C_SHA256:
        die("C artifact sha256 != frozen 7e69fbf3… value")
    if c_live.get("lineage_run") != GROUP2_C_ID:
        die("C lineage_run does not bind to the C run id")
    if c_live.get("txn_id") != "98e0892f-5a59-4e05-8d6e-e8060355008c":
        die("C txn_id is not the frozen 98e0892f… value")

    cohort_counter_srcs = sorted({r["fixture_src"] for r in group2})

    # 9. sealed content-gate references (root code-review items 6/7/8/10
    #    + round-2 items 2/3/5/6).
    #    Every file is manifest-index + SHA verified above-style before use.
    pf_counts_rel = "00-preflight/09-fixture-counts.json"
    po_dir = "13-postfacts-attempt-20261114T152612"
    gate_files = {
        "preflight_counts": pf_counts_rel,
        "postfacts_counts": f"{po_dir}/09-fixture-counts.json",
        "c_live": f"{po_dir}/11-c-target-live.json",
        "volumes": f"{po_dir}/04-volumes.txt",
        "env_mode": f"{po_dir}/05-env-mode.txt",
        "fixture_state": f"{po_dir}/03a-inspect-fixture-state.txt",
        "fixture_pid1": f"{po_dir}/03b-inspect-fixture-pid1.txt",
        "owned_defs": "13-persist-post-r1/defs.json",
        "owned_ticks": "13-persist-post-r1/ticks.json",
        "owned_runs": "13-persist-post-r1/runs.json",
        "owned_grants": "13-persist-post-r1/grants.json",
        "legacy_b": f"{po_dir}/08-legacy-B_lease_lost_unpublished.txt",
        "legacy_c": f"{po_dir}/08-legacy-C_committed_unconfirmed.txt",
        "global_nonempty_cron": f"{po_dir}/12c-global-nonempty-cron.json",
    }
    gate_shas = {}
    for label, rel in gate_files.items():
        gate_shas[label] = verify_manifest_indexed(final_dir, rel, manifest)

    # source baseline CONTENT verification (not just a hardcoded string)
    src_text = (final_dir / "01-source-hash-pre.txt").read_text()
    if hashlib.sha256(src_text.encode()).hexdigest() != EXPECTED_SOURCE_COMBINED:
        die("01-source-hash-pre.txt combined content hash != expected "
            "8cd25d42… — sealed source baseline corrupt")
    src_lines = [l for l in src_text.splitlines() if l.strip()]
    if len(src_lines) != 373:
        die(f"sealed source baseline must carry 373 lines, got "
            f"{len(src_lines)}")

    pf_counts = json.loads((final_dir / pf_counts_rel).read_text())["counts"]
    po_counts = json.loads(
        (final_dir / gate_files["postfacts_counts"]).read_text())["counts"]
    for src, snap in list(pf_counts.items()) + list(po_counts.items()):
        rc = snap.get("request_count")
        if not isinstance(rc, int) or isinstance(rc, bool):
            die(f"sealed counter {src}: request_count not a plain int")
        if snap.get("active") != 0:
            die(f"sealed counter {src}: active != 0 in sealed record")
        if snap.get("inventory_started") != snap.get("inventory_finished"):
            die(f"sealed counter {src}: inventory started != finished")
    for src in ("p21a-b6ec49ab", "p21b-b6ec49ab", "p21c-b6ec49ab"):
        if pf_counts.get(src) != po_counts.get(src):
            die(f"sealed b6ec49ab trio drift between preflight and final "
                f"postfacts for {src}")
    protected_expected = dict(pf_counts)
    protected_expected.update(po_counts)   # postfacts (latest) wins
    if sorted(protected_expected) != sorted([
            "p21a-b6ec49ab", "p21b-b6ec49ab", "p21c-b6ec49ab",
            "p21b-96351e3b", "p21c-96351e3b",
            "p21b-528455c2", "p21c-528455c2",
            "p21b-1020a565", "p21c-1020a565"]):
        die(f"protected counter src set unexpected: "
            f"{sorted(protected_expected)}")
    if protected_expected.get("p21b-1020a565", {}).get("request_count") != 2 \
            or protected_expected.get("p21c-1020a565",
                                      {}).get("request_count") != 5:
        die("cohort 1020a565 sealed counters are not the reviewed b=2/c=5")

    c_live2 = json.loads((final_dir / gate_files["c_live"]).read_text())
    if (c_live2.get("txn_status") != EXPECTED_C_TXN_STATUS
            or c_live2.get("download_http") != EXPECTED_C_DOWNLOAD_HTTP
            or c_live2.get("bytes") != EXPECTED_C_BYTES
            or c_live2.get("sha256") != EXPECTED_C_SHA256
            or c_live2.get("run_id") != GROUP2_C_ID):
        die("sealed postfacts C-live artifact disagrees with the "
            "provenance-pinned values")

    env_mode = (final_dir / gate_files["env_mode"]).read_text().strip()
    if env_mode != "600":
        die(f"sealed env mode {env_mode!r} != '600'")
    fx_state = (final_dir / gate_files["fixture_state"]).read_text().strip()
    fx_pid1 = (final_dir / gate_files["fixture_pid1"]).read_text().strip()
    fx_parts = fx_state.split("|")
    if len(fx_parts) != 4 or fx_parts[0] != EXPECTED_FIXTURE_BOOT:
        die(f"sealed fixture state not the frozen boot line: {fx_state!r}")
    if fx_pid1 != 'null|["python3","-I","/srv/rest_source.py","0.0.0.0","8090"]':
        die(f"sealed fixture PID1 line unexpected: {fx_pid1!r}")

    owned_defs = json.loads((final_dir / gate_files["owned_defs"]).read_text())
    owned_ticks = json.loads(
        (final_dir / gate_files["owned_ticks"]).read_text())
    owned_runs = json.loads((final_dir / gate_files["owned_runs"]).read_text())
    owned_grants = json.loads(
        (final_dir / gate_files["owned_grants"]).read_text())
    if len(owned_defs) != 17 or len(owned_ticks) != 32 or len(owned_runs) != 15:
        die(f"sealed owned fullstate sizes != 17/32/15: "
            f"{len(owned_defs)}/{len(owned_ticks)}/{len(owned_runs)}")
    if owned_grants != []:
        die("sealed owned grants not empty")
    if sorted(d["id"] for d in owned_defs) != sorted(owned_ids):
        die("sealed persist defs ids != persist-manifest owned_ids")

    # round-2: global nonempty-cron roster must be an exact sealed set and
    # a subset of the owned 17 (cron stays intact on disabled defs).
    global_cron = json.loads(
        (final_dir / gate_files["global_nonempty_cron"]).read_text())
    cron_ids = sorted({str(e["id"]) for e in global_cron})
    for cid in cron_ids:
        if not re.match(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                        r"[0-9a-f]{4}-[0-9a-f]{12}$", cid):
            die(f"global nonempty-cron id not a UUID: {cid!r}")
    if not set(cron_ids) <= set(owned_ids):
        die("sealed global nonempty-cron ids escape the owned-17 set")

    legacy_b_line = (final_dir / gate_files["legacy_b"]).read_text().strip()
    legacy_c_line = (final_dir / gate_files["legacy_c"]).read_text().strip()
    for name, line in (("B", legacy_b_line), ("C", legacy_c_line)):
        rows = [l for l in line.splitlines() if l.strip()]
        if len(rows) != 3 or rows[:2] != ["SET", "SET"]:
            die(f"sealed legacy {name} projection shape unexpected")

    # owned-history file binding: the archived file must hash to the
    # persist-manifest's owned_history_sha256.
    hist_hit = None
    for rel in manifest["files"]:
        if "owned-history" in rel and rel.endswith(".json"):
            p = final_dir / rel
            if not p.is_file():
                continue          # mini-archive copies may omit candidates
            if sha256_file(p) == EXPECTED_OWNED_HISTORY_SHA:
                hist_hit = rel
                break
    if hist_hit is None:
        die("no archived owned-history file hashes to the sealed "
            "owned_history_sha256 8c18999a…")

    # frozen P22 legacy projection + P21 fault names (frozen tools dir,
    # read-only; recorded with actual sha; they predate the 859 archive)
    p22_baseline = tools_dir.parent / "business-platform-p2-2" / \
        "readonly_baseline.py"
    if not p22_baseline.is_file():
        die(f"frozen P22 readonly_baseline.py missing: {p22_baseline}")
    spec2 = importlib.util.spec_from_file_location("p22rb", p22_baseline)
    p22 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(p22)
    legacy_runs = dict(p22.P21_LEGACY_RUNS)
    legacy_sql = p22.PROJECTION_SQL
    legacy_keys = list(p22.PROJECTION_KEYS)
    if sorted(legacy_runs) != ["B_lease_lost_unpublished",
                               "C_committed_unconfirmed"]:
        die("frozen P22 legacy run labels unexpected")
    if "{rid}" not in legacy_sql:
        die("frozen P22 legacy SQL lost its {rid} placeholder")

    p21_smoke = tools_dir.parent / "business-platform-p2-1" / "smoke_p21.py"
    if not p21_smoke.is_file():
        die(f"frozen P21 smoke_p21.py missing: {p21_smoke}")
    p21_text = p21_smoke.read_text()
    fn = re.search(r'P21C_FUNCTION\s*=\s*"([a-z0-9_]+)"', p21_text)
    trg = re.search(r'P21C_TRIGGER\s*=\s*"([a-z0-9_]+)"', p21_text)
    if not fn or not trg or fn.group(1) != "p21_c_terminal_fault" \
            or trg.group(1) != "p21_c_terminal_fault_trg":
        die("frozen smoke_p21.py fault names differ from the reviewed "
            "p21_c_terminal_fault / p21_c_terminal_fault_trg")

    content_gates = {
        "protected_counters": {
            "expected": {k: dict(v) for k, v in
                         sorted(protected_expected.items())},
            "sealed_files": {"preflight": gate_shas["preflight_counts"],
                             "postfacts": gate_shas["postfacts_counts"]},
        },
        "c_live_sealed": {k: c_live2[k] for k in (
            "run_id", "txn_id", "txn_status", "download_http", "bytes",
            "sha256", "target", "bound_files", "lineage_run")},
        "fixture": {"boot": fx_parts[0], "running": fx_parts[1],
                    "image": fx_parts[2], "restart_policy": fx_parts[3],
                    "pid1": fx_pid1.split("|", 1)[1]},
        "volumes_expected_names": [
            "openfoundry-platform-p1-q3v9_dataset-files",
            "openfoundry-platform-p1-q3v9_go-build-cache",
            "openfoundry-platform-p1-q3v9_postgres-data"],
        "env_mode": env_mode,
        "owned_fullstate": {"defs": owned_defs, "ticks": owned_ticks,
                            "runs": owned_runs, "grants": owned_grants},
        "global_catalog": {"grants_total": 0,
                           "nonempty_cron_ids": cron_ids},
        "owned_history_file": hist_hit,
        "legacy": {"runs": legacy_runs, "projection_sql": legacy_sql,
                   "keys": legacy_keys,
                   "expected_lines": {
                       "B_lease_lost_unpublished": legacy_b_line.splitlines()[-1],
                       "C_committed_unconfirmed": legacy_c_line.splitlines()[-1]},
                   "p22_readonly_baseline_sha256": sha256_file(p22_baseline)},
        "fault_absent": {"function": fn.group(1), "trigger": trg.group(1),
                         "source": "tools/smoke/business-platform-p2-1/"
                                   "smoke_p21.py:401-402",
                         "p21_smoke_sha256": sha256_file(p21_smoke)},
        "sealed_file_shas": gate_shas,
    }
    return {
        "SCHEMA_27": SCHEMA_27,
        "GROUP1": group1,
        "GROUP2": group2,
        "FINAL_ANCHOR": final_boot,
        "FINAL_IMAGE": f"sha256:{final_image}",
        "DATASET_IMAGE": f"sha256:{dataset_image}",
        "PIPELINE_IMAGE": f"sha256:{pipeline_image}",
        "AUX_ABSENT": aux_absent,
        "HISTORICAL_ANCHOR_OLD4": hist_boot,
        "FIXTURE_BOOT": EXPECTED_FIXTURE_BOOT,
        "COHORT_COUNTER_SRCS": cohort_counter_srcs,
        "CONTENT_GATES": content_gates,
        "PROVENANCE": {
            "archive": str(final_dir),
            "hash_manifest_sha256": EXPECTED_HASH_MANIFEST_SHA,
            "verified_file_shas": shas,
            "p24_pins_sha256": tools_sha["files"]["p24_pins.py"],
            "owned_history_sha256": owned_sha,
            "owned_ids_len": len(owned_ids),
            "owned_ids": sorted(owned_ids),
            "p24_result_sha256": result_sha,
            "c_txn_id": c_live.get("txn_id"),
            "c_artifact_sha256": c_live.get("sha256"),
            "source_baseline_file": "01-source-hash-pre.txt",
            "source_combined_sha256": EXPECTED_SOURCE_COMBINED,
        },
    }


def render_module(data: dict) -> str:
    body = json.dumps({k: data[k] for k in (
        "SCHEMA_27", "GROUP1", "GROUP2", "FINAL_ANCHOR", "FINAL_IMAGE",
        "DATASET_IMAGE", "PIPELINE_IMAGE", "AUX_ABSENT",
        "HISTORICAL_ANCHOR_OLD4", "FIXTURE_BOOT", "COHORT_COUNTER_SRCS",
        "PROVENANCE")}, indent=1, sort_keys=False, ensure_ascii=False)
    return (
        "# GENERATED by p25_cohort_derive.py from SHA-bound frozen P24\n"
        "# evidence — DO NOT HAND-EDIT. Regenerate + `--check` instead.\n"
        "# Root round-1 anchors: FINAL connector boot 2026-11-14T07:11:20Z\n"
        "# (Nov13T22:05 is old4 historical provenance only).\n"
        f"DATA = {body}\n\n"
        "SCHEMA_27 = tuple(DATA[\"SCHEMA_27\"])\n"
        "GROUP1 = DATA[\"GROUP1\"]\n"
        "GROUP2 = DATA[\"GROUP2\"]\n"
        "FINAL_ANCHOR = DATA[\"FINAL_ANCHOR\"]\n"
        "FINAL_IMAGE = DATA[\"FINAL_IMAGE\"]\n"
        "DATASET_IMAGE = DATA[\"DATASET_IMAGE\"]\n"
        "PIPELINE_IMAGE = DATA[\"PIPELINE_IMAGE\"]\n"
        "AUX_ABSENT = DATA[\"AUX_ABSENT\"]\n"
        "HISTORICAL_ANCHOR_OLD4 = DATA[\"HISTORICAL_ANCHOR_OLD4\"]\n"
        "FIXTURE_BOOT = DATA[\"FIXTURE_BOOT\"]\n"
        "COHORT_COUNTER_SRCS = DATA[\"COHORT_COUNTER_SRCS\"]\n"
        "PROVENANCE = DATA[\"PROVENANCE\"]\n"
    )


def import_module_at(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def atomic_generate(out: Path, text: str, data: dict):
    out.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(out.parent), prefix=".p25-cohort6-",
                               suffix=".py")
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(text)
        ast.parse(text)                       # syntax safety before rename
        mod = import_module_at(Path(tmp), "p25_cohort6_tmp")
        for key in ("SCHEMA_27", "GROUP1", "GROUP2", "FINAL_ANCHOR",
                    "FINAL_IMAGE", "DATASET_IMAGE", "PIPELINE_IMAGE",
                    "AUX_ABSENT", "HISTORICAL_ANCHOR_OLD4", "FIXTURE_BOOT",
                    "COHORT_COUNTER_SRCS", "PROVENANCE"):
            got = getattr(mod, key)
            want = tuple(data[key]) if key == "SCHEMA_27" else data[key]
            if got != want:
                die(f"generated module {key} differs from derivation")
        os.replace(tmp, out)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    print(f"[p25-derive] generated {out}")


def check_existing(out: Path, data: dict):
    if not out.is_file():
        die(f"--check: {out} does not exist")
    mod = import_module_at(out, "p25_cohort6_check")
    for key in ("SCHEMA_27", "GROUP1", "GROUP2", "FINAL_ANCHOR",
                "FINAL_IMAGE", "DATASET_IMAGE", "PIPELINE_IMAGE",
                "AUX_ABSENT", "HISTORICAL_ANCHOR_OLD4", "FIXTURE_BOOT",
                "COHORT_COUNTER_SRCS", "PROVENANCE"):
        got = getattr(mod, key)
        want = tuple(data[key]) if key == "SCHEMA_27" else data[key]
        if got != want:
            die(f"--check: {key} in {out.name} does not match a fresh "
                "derivation — regenerate; never hand-edit")
    print(f"[p25-derive] --check ok: {out} equals a fresh derivation")


def frozen_baseline_dict(data: dict, cohort6_sha: str) -> dict:
    """Canonical frozen-baseline content (round-2 item 4): the formal
    verifier rederives this and requires the sibling file to EXACTLY
    equal it (key sets included)."""
    return {
        "client_date": "2026-11-14",
        "note": ("P25 source-only frozen baseline; every sha re-verified "
                 "by p25_cohort_derive.py at emission time"),
        "transition_anchor": {
            "connector_boot": data["FINAL_ANCHOR"],
            "connector_image": data["FINAL_IMAGE"],
            "dataset_image": data["DATASET_IMAGE"],
            "pipeline_image": data["PIPELINE_IMAGE"],
            "aux_absent": data["AUX_ABSENT"],
        },
        "historical_anchor_old4": data["HISTORICAL_ANCHOR_OLD4"],
        "fixture_boot": data["FIXTURE_BOOT"],
        "schema_27": list(data["SCHEMA_27"]),
        "cohort": {
            "group1_old4_ids": sorted(r["id"] for r in data["GROUP1"]),
            "group2_postboot2_ids": sorted(r["id"] for r in data["GROUP2"]),
            "cohort_counter_srcs": data["COHORT_COUNTER_SRCS"],
        },
        "owned_history": {
            "defs": data["PROVENANCE"]["owned_ids_len"],
            "ticks": 32,
            "terminal_runs": 15,
            "grants": 0,
            "owned_ids": data["PROVENANCE"]["owned_ids"],
            "owned_history_sha256": data["PROVENANCE"]["owned_history_sha256"],
            "p24_result_sha256": data["PROVENANCE"]["p24_result_sha256"],
        },
        "source_baseline": {
            "file": "source-p24-baseline.txt",
            "frozen_reference": data["PROVENANCE"]["source_baseline_file"],
            "combined_sha256": data["PROVENANCE"]["source_combined_sha256"],
            "files": 373,
        },
        "verified_shas": data["PROVENANCE"]["verified_file_shas"],
        "p24_pins_sha256": data["PROVENANCE"]["p24_pins_sha256"],
        "hash_manifest_sha256": data["PROVENANCE"]["hash_manifest_sha256"],
        "p25_cohort6_sha256": cohort6_sha,
        "content_gates": data["CONTENT_GATES"],
    }


def emit_frozen_baseline(path: Path, data: dict, cohort6_sha: str):
    path.write_text(json.dumps(frozen_baseline_dict(data, cohort6_sha),
                               indent=1, sort_keys=False) + "\n")
    print(f"[p25-derive] emitted {path}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--p24-final-dir", required=True)
    ap.add_argument("--p24-tools-dir", required=True)
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent
                                         / "p25_cohort6.py"))
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--emit-frozen-baseline", metavar="JSON")
    args = ap.parse_args()

    data = derive(Path(args.p24_final_dir), Path(args.p24_tools_dir))
    out = Path(args.out)
    if args.check:
        check_existing(out, data)
    else:
        atomic_generate(out, render_module(data), data)
    if args.emit_frozen_baseline:
        emit_frozen_baseline(Path(args.emit_frozen_baseline), data,
                             sha256_file(out))
    print("[p25-derive] ok: 6 rows (4+2) derived under full SHA chain")
    return 0


if __name__ == "__main__":
    sys.exit(main())

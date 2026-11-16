#!/usr/bin/env python3
"""P24 POST-FACTS final-state verifier (LOCKED FALLBACK, review closure).

Consumes EXACTLY ONE attempt: collect_postfacts_p24.py's fresh dir +
its attempt-manifest.json (every file sha256+rc bound to THIS attempt;
never per-stem "latest" mixing). Hard checks:

  M   manifest integrity: every listed file exists, sha256 matches,
      rc==0; abc/p24 result sha256 match the supplied inputs
  A'  controlled set is UNCONDITIONALLY exactly 6 UNIQUE ids:
      old four — every one of the 27 pinned fields unchanged — plus a
      diff of EXACTLY two bound to THIS ABC attempt (missing/terminal
      new rows, duplicates, or anything else FAIL; there is no
      "if unknown" shortcut anymore)
  A2' each new row's COMPLETE parent chain vs the attempt's raw parent
      SQL: sync_def/source/output ids, source_name, fixture key,
      owner/org/run_owner/requested_by ALL non-empty and equal to the
      raw connection/def; conn/def/run org triple identical; exactly
      ONE run per attempt def and it is this row
  A3' B: fixture p21b + gate false + the REAL lease-loss evidence of
      THIS ABC run (expiry_injected true, http 409, lease_lost_marker
      true, pages_before_inject int>=2 with pages_after_wait equal,
      target_files 0, evidence db_status == the controlled row) with
      the official non-partial 3-pass/0-fail envelope and
      results.lease_loss_fencing.status=pass — the old derived
      lease_expires_at<started_at datetime guess is REMOVED; C:
      fixture p21c + gate true + id == the UUID inside the ABC
      result's logical_path + result evidence COMMITTED /
      lineage_run_match / artifact_sha256_matches_txn + LIVE target
      (target == C's own output, txn AND download HTTP 200, real
      txn_id + exactly one valid bound file, txn COMMITTED, lineage
      bound to C, sha256(bytes)==content_hash)
  B'  old four predate the live boot; the new two are REPORTED
      post-boot (never claimed pre-boot)
  F'  this attempt's two fixture keys: active==0 (counters recorded);
      legacy trio == frozen P23-final baseline
  O'  owned final state by FRESH SQL: the p24 result's EXACT owned id
      set; every row disabled + NULL cursor + source/target match the
      result's bindings; NO grant rows; GLOBAL non-empty cron set ==
      exactly the owned defs that carry a non-empty cron
  C'-G' fixture unchanged (exact boot pin), base5 POSITIVE Up,
      volumes, env600, legacy b6ec49ab B/C unchanged

Usage: python3 -I verify_postfacts_p24.py --attempt-dir <dir> \
         --abc-result <result_p21.json> --p24-result <result_p24.json> \
         [--owned-history <p24-owned-history-v1.json>]
Exit 0 iff all checks pass. Fixed pins stay EXACTLY four. --owned-history,
when supplied, must match the manifest's owned_history_sha256 binding
exactly (and vice versa) and its validated UNION drives the owned checks.
"""

import argparse
import hashlib
import importlib.util
import json
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
P23_FINAL = HERE.parent / "business-platform-p2-3" / "results" / \
    "2026-11-14-p23-final"
RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$")


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


GUARD = load_module(HERE / "p24_live_guard.py", "p24_guard")
BASE = load_module(HERE.parent / "business-platform-p2-2" /
                   "readonly_baseline.py", "p22_baseline")


def _sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def load_owned_history(hist_path, latest_result_path):
    """Validate a p24-owned-history-v1 roster (same contract as the
    collectors); returns (union, sha, errs). Duplicated inline so this
    verifier stays a standalone -I script."""
    errs = []
    try:
        hist = json.loads(Path(hist_path).read_text())
    except Exception as exc:  # noqa: BLE001
        return None, None, [f"history unreadable: {exc}"]
    if hist.get("schema") != "p24-owned-history-v1":
        errs.append("schema != p24-owned-history-v1")
    attempts = hist.get("attempts")
    if not (isinstance(attempts, list) and attempts):
        errs.append("attempts missing/empty")
        attempts = []
    latest_abs = Path(latest_result_path).resolve()
    latest_sha = _sha(latest_result_path)
    latest_in = False
    union = {}
    for i, a in enumerate(attempts):
        rp = (a or {}).get("result_path")
        if not (isinstance(rp, str) and os.path.isabs(rp)):
            errs.append(f"attempts[{i}].result_path not absolute")
            continue
        p = Path(rp)
        if not p.is_file():
            errs.append(f"attempts[{i}] result missing: {rp}")
            continue
        try:
            actual = _sha(p)
        except OSError as exc:
            errs.append(f"attempts[{i}] unreadable: {exc}")
            continue
        if actual != (a or {}).get("result_sha256"):
            errs.append(f"attempts[{i}] sha mismatch")
            continue
        try:
            res = json.loads(p.read_text())
        except ValueError:
            errs.append(f"attempts[{i}] result not JSON")
            continue
        if res.get("suffix") != (a or {}).get("suffix"):
            errs.append(f"attempts[{i}] suffix mismatch")
        ids = res.get("owned_sync_ids") or {}
        if not isinstance(ids, dict):
            errs.append(f"attempts[{i}] owned_sync_ids not a map")
            continue
        for k, v in ids.items():
            if not (isinstance(v, dict)
                    and isinstance(v.get("source_id"), str) and v["source_id"]
                    and isinstance(v.get("target_id"), str) and v["target_id"]):
                errs.append(f"attempts[{i}] binding {k[:8]} not safe")
                continue
            if k in union and union[k] != v:
                errs.append(f"binding {k[:8]} conflicts across attempts")
            union[k] = v
        if p.resolve() == latest_abs and actual == latest_sha:
            latest_in = True
    if not latest_in:
        errs.append("latest success (--p24-result) not among attempts")
    declared = hist.get("owned_sync_ids")
    if not (isinstance(declared, dict) and declared):
        errs.append("declared owned_sync_ids missing/empty")
    elif declared != union:
        errs.append(f"declared union != rebuilt ({len(declared)} vs "
                    f"{len(union)})")
    if errs:
        return None, None, errs
    return union, _sha(hist_path), []


def parse_ts(v):
    return GUARD._parse_ts(v)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--attempt-dir", required=True)
    ap.add_argument("--abc-result", required=True)
    ap.add_argument("--p24-result", required=True)
    ap.add_argument("--owned-history", default=None)
    args = ap.parse_args()
    d = Path(args.attempt_dir)
    checks = []

    def check(name, ok, detail=""):
        checks.append({"name": name, "ok": bool(ok), "detail": detail})
        print(f"[postfacts] {'ok  ' if ok else 'FAIL':4} {name} {detail}")

    def jload(name):
        p = d / name
        try:
            return json.loads(p.read_text())
        except Exception:
            return None

    # ---- M: manifest integrity + STRICT consumption whitelist (item2) ---
    abc_pre = json.loads(Path(args.abc_result).read_text())
    p24r_pre = json.loads(Path(args.p24_result).read_text())
    suffix_pre = abc_pre.get("suffix")
    # Optional exact global roster (retry-evidence): when supplied it is
    # validated exactly like the collectors validate it, its sha256 must
    # equal the manifest's owned_history_sha256 binding, and its UNION
    # (failed attempts included) becomes the owned expected map. Symmetry:
    # a manifest bound to a history file REQUIRES the flag, and vice versa.
    hist_union, hist_sha, hist_errs = (None, None, [])
    if args.owned_history:
        hist_union, hist_sha, hist_errs = load_owned_history(
            args.owned_history, args.p24_result)
    owned_pre = sorted(hist_union if hist_union is not None
                       else (p24r_pre.get("owned_sync_ids") or {}))

    txt_steps = ["01-docker-ps", "02-inspect-connector",
                 "03a-inspect-fixture-state", "03b-inspect-fixture-pid1",
                 "04-volumes", "05-env-mode", "06-controlled",
                 "10a-abc-connections", "10b-abc-defs", "10c-abc-runs"]
    txt_steps += [f"08-legacy-{lbl}" for lbl in BASE.P21_LEGACY_RUNS]
    txt_steps += ["12a-owned-defs", "12b-owned-grants"] if owned_pre \
        else []
    txt_steps += ["12c-global-nonempty-cron"]
    required = {f"{n}{ext}" for n in txt_steps
                for ext in (".txt", ".txt.err", ".txt.rc")}
    required |= {f"{n}.json" for n in txt_steps
                 if n not in ("01-docker-ps", "02-inspect-connector",
                              "03a-inspect-fixture-state",
                              "03b-inspect-fixture-pid1", "04-volumes",
                              "05-env-mode")
                 and not n.startswith("08-legacy-")}
    required |= {"09-fixture-counts.json", "11-c-target-live.json"}
    derived_pairs = [(f"{n}.txt", f"{n}.json")
                     for n in ("06-controlled", "10a-abc-connections",
                               "10b-abc-defs", "10c-abc-runs",
                               "12a-owned-defs", "12b-owned-grants",
                               "12c-global-nonempty-cron")]

    mf = jload("attempt-manifest.json")
    m_bad = []
    files = {}
    if not mf:
        m_bad.append("attempt-manifest.json missing/unreadable")
    else:
        files = mf.get("files") or {}
        if not files:
            m_bad.append("manifest lists no files")
        # EVERY consumed file of THIS attempt is whitelisted, present, sha
        # bound and rc==0; an arbitrary non-empty subset can never pass.
        declared = set(mf.get("consumed_by_verifier") or [])
        if declared != required:
            m_bad.append(f"consumed whitelist != required set "
                         f"(missing={sorted(required - declared)[:4]} "
                         f"extra={sorted(declared - required)[:4]})")
        for fname in sorted(required):
            meta = files.get(fname)
            p = d / fname
            if meta is None:
                m_bad.append(f"{fname}: not in manifest")
            elif not p.is_file():
                m_bad.append(f"{fname}: missing")
            elif _sha(p) != meta.get("sha256"):
                m_bad.append(f"{fname}: sha256 mismatch")
            elif meta.get("rc") != 0:
                m_bad.append(f"{fname}: rc={meta.get('rc')}")
        # The .rc ORIGINAL files must literally restate rc 0 (original rc ==
        # manifest rc, checked from bytes, not from trust).
        for n in txt_steps:
            rcp = d / f"{n}.txt.rc"
            if rcp.is_file() and rcp.read_text() != "0\n":
                m_bad.append(f"{n}.txt.rc content != 0")
        # Derived JSON must reparse EQUAL from the raw .txt last line.
        for raw_name, der_name in derived_pairs:
            raw_p, der_p = d / raw_name, d / der_name
            if raw_p.is_file() and der_p.is_file():
                lines = [l for l in raw_p.read_text().splitlines() if l]
                try:
                    reparsed = json.loads(lines[-1]) if lines else []
                    if reparsed is None:
                        reparsed = []
                    if reparsed != json.loads(der_p.read_text()):
                        m_bad.append(f"{der_name}: derived != raw reparse")
                except ValueError:
                    m_bad.append(f"{der_name}: raw last line not JSON")
        # Attempt inputs must agree with the manifest's own binding.
        if mf.get("bad_steps"):
            m_bad.append(f"manifest bad_steps not empty: "
                         f"{mf.get('bad_steps')[:4]}")
        if mf.get("attempt_dir") != d.name:
            m_bad.append("manifest attempt_dir mismatch")
        if mf.get("abc_suffix") != suffix_pre:
            m_bad.append("manifest abc_suffix mismatch")
        if mf.get("owned_ids") != owned_pre:
            m_bad.append("manifest owned_ids mismatch")
        mf_hist = mf.get("owned_history_sha256")
        hist_bad = []
        if hist_errs:
            hist_bad.append(f"owned-history invalid: {hist_errs[:3]}")
        if args.owned_history and hist_sha is not None \
                and mf_hist != hist_sha:
            hist_bad.append("owned-history sha != manifest binding")
        if (args.owned_history is None) != (mf_hist is None):
            hist_bad.append("owned-history flag/manifest binding asymmetric")
        if hist_bad:
            m_bad[:0] = hist_bad  # binding-level failures lead the detail
        if _sha(args.abc_result) != mf.get("abc_result_sha256"):
            m_bad.append("abc-result sha != manifest binding")
        if _sha(args.p24_result) != mf.get("p24_result_sha256"):
            m_bad.append("p24-result sha != manifest binding")
    check("M-attempt-manifest-integrity", not m_bad, "; ".join(m_bad[:8]))

    abc = abc_pre
    suffix = suffix_pre
    names = {"B": f"p21-p21b-{suffix}", "C": f"p21-c-{suffix}"}
    fx_of = {"B": f"p21b-{suffix}", "C": f"p21c-{suffix}"}
    p24r = p24r_pre
    # The owned expected map is the validated HISTORY UNION when supplied
    # (failed attempts' disabled/NULL/terminal defs stay owned — the exact
    # global roster), else the latest result's own set.
    owned_bindings = hist_union if hist_union is not None \
        else (p24r.get("owned_sync_ids") or {})

    # ---- A': controlled set is EXACTLY six, old four unchanged ----------
    rows = jload("06-controlled.json") or []
    by_id = {}
    dup = []
    for r in rows:
        if r["id"] in by_id:
            dup.append(r["id"])
        by_id[r["id"]] = r
    pins = {p["id"]: p for p in GUARD.PINNED_EXCEPTIONS}
    drift = []
    for rid, want in sorted(pins.items()):
        row = by_id.get(rid)
        if row is None:
            drift.append(f"{rid} MISSING")
            continue
        for k, v in sorted(want.items()):
            if row.get(k) != v:
                drift.append(f"{rid}.{k}")
    new = sorted(set(by_id) - set(pins))
    a1_ok = (not dup and not drift and len(new) == 2
             and len(by_id) == len(pins) + 2)
    check("A-prime-exact-six-unique-old4-unchanged", a1_ok,
          f"total={len(by_id)} dup={dup} drift={drift[:4]} new={new}")

    # ---- A2': new two COMPLETE parent chain vs raw parent SQL -----------
    conns = jload("10a-abc-connections.json") or []
    defs = jload("10b-abc-defs.json") or []
    abc_runs = jload("10c-abc-runs.json") or []
    chain_bad = []
    new_detail = {}
    if a1_ok:
        by_name = {c["name"]: c for c in conns}
        if sorted(by_name) != sorted(names.values()) or len(defs) != 2:
            chain_bad.append(f"raw parent universe wrong: "
                             f"{sorted(by_name)} defs={len(defs)}")
        else:
            for rid in new:
                row = by_id[rid]
                conn = by_name.get(row.get("source_name")) or \
                    next((c for c in conns
                          if c["id"] == row.get("source_id")), None)
                dfn = next((x for x in defs
                            if x["id"] == row.get("sync_def_id")), None)
                if not (conn and dfn):
                    chain_bad.append(f"{rid[:8]}: def/conn not in this "
                                     "attempt's raw parents")
                    continue
                chain_keys = ("sync_def_id", "source_id",
                              "output_dataset_id", "source_name",
                              "fixture_src", "owner_id", "org_id",
                              "run_owner_id", "requested_by")
                if not all(row.get(k) for k in chain_keys):
                    chain_bad.append(f"{rid[:8]}: chain key empty")
                if not (row["source_id"] == conn["id"] == dfn["source_id"]
                        and row["output_dataset_id"] ==
                        dfn["output_dataset_id"]
                        and row["owner_id"] == conn["owner_id"]
                        and row["org_id"] == conn.get("org_id")
                        and row["conn_org_id"] == row["def_org_id"]
                        == row["run_org_id"] == row["org_id"]
                        and row["run_owner_id"] == conn["owner_id"]
                        and row["requested_by"] == conn["owner_id"]):
                    chain_bad.append(f"{rid[:8]}: chain field mismatch")
                # Explicit identity pairings (item3): the row's denormalized
                # source_name IS the raw connection's name, and the row's
                # def_org_id IS the raw def's org.
                if row.get("source_name") != conn.get("name"):
                    chain_bad.append(f"{rid[:8]}: source_name != conn.name")
                if row.get("def_org_id") != dfn.get("org_id"):
                    chain_bad.append(f"{rid[:8]}: def_org_id != def.org_id")
                runs_of_def = [r for r in abc_runs
                                if r["sync_def_id"] == dfn["id"]]
                if len(runs_of_def) != 1 or runs_of_def[0]["id"] != rid:
                    chain_bad.append(f"{rid[:8]}: runs-of-def != exactly "
                                     f"this row ({len(runs_of_def)})")
                else:
                    # 06↔10c SAME-ROW completeness (item3): the controlled
                    # projection row and the ALL-runs raw projection row for
                    # the SAME unique run must agree on EVERY one of the 27
                    # safe fields — an ID-only match cannot carry this.
                    controlled_row = next(
                        (r for r in rows if r["id"] == rid), None)
                    if controlled_row is None or \
                            {k: str(v) for k, v in controlled_row.items()} \
                            != {k: str(v) for k, v in runs_of_def[0].items()}:
                        diff_keys = sorted(
                            {k for k in set(controlled_row or {})
                             | set(runs_of_def[0])
                             if str((controlled_row or {}).get(k))
                             != str(runs_of_def[0].get(k))})
                        chain_bad.append(
                            f"{rid[:8]}: 06 vs 10c full-row diff "
                            f"{diff_keys[:6]}")
                new_detail[rid] = {"fixture": row.get("fixture_src"),
                                   "gate": row.get("gate_entered")}
    check("A2-prime-new-two-full-parent-chain", a1_ok and not chain_bad,
          "; ".join(chain_bad[:4]))

    # ---- A3': B/C shapes + C bound to COMMITTED/lineage/hash -----------
    a3_bad = []
    b_row = c_row = None
    if a1_ok:
        b_row = next((by_id[r] for r in new
                      if by_id[r].get("fixture_src") == fx_of["B"]), None)
        c_row = next((by_id[r] for r in new
                      if by_id[r].get("fixture_src") == fx_of["C"]), None)
        if not (b_row and c_row):
            a3_bad.append("B/C fixture keys not the attempt pair")
        else:
            # ABC envelope: this must be the official, non-partial,
            # 3-pass/0-fail full run whose lease_loss_fencing PASSED.
            if not (abc.get("official") is True
                    and abc.get("partial") is False
                    and (abc.get("counts") or {}).get("pass") == 3
                    and (abc.get("counts") or {}).get("fail") == 0):
                a3_bad.append("ABC envelope not official/3pass/0fail")
            if ((abc.get("results") or {}).get(
                    "lease_loss_fencing") or {}).get("status") != "pass":
                a3_bad.append("results.lease_loss_fencing.status != pass")
            # B shape: gate false plus the REAL lease-loss evidence from
            # THIS ABC run (review item1). The old derived datetime guess
            # (lease_expires_at < started_at) is GONE — the frozen B
            # evidence carries no run UUID and none is invented; the
            # suffix binds p21-p21b-<suffix>/fixture p21b-<suffix> to the
            # exact Source/Def/unique Run proven in A2'.
            ev_b = ((abc.get("evidence") or {}).get(
                "lease_loss_fencing") or {})
            if b_row.get("gate_entered") != "false":
                a3_bad.append("B: gate != false")
            if ev_b.get("expiry_injected") is not True:
                a3_bad.append("B: expiry_injected not true")
            if ev_b.get("http_status") != 409:
                a3_bad.append(f"B: http_status {ev_b.get('http_status')!r}"
                              " != 409")
            if ev_b.get("lease_lost_marker") is not True:
                a3_bad.append("B: lease_lost_marker not true")
            pbi, paw = ev_b.get("pages_before_inject"), \
                ev_b.get("pages_after_wait")
            if not (isinstance(pbi, int) and not isinstance(pbi, bool)
                    and pbi >= 2):
                a3_bad.append(f"B: pages_before_inject {pbi!r} not int>=2")
            elif paw != pbi:
                a3_bad.append(f"B: pages_after_wait {paw!r} != before "
                              f"{pbi!r}")
            if ev_b.get("target_files") != 0:
                a3_bad.append(f"B: target_files {ev_b.get('target_files')!r}"
                              " != 0")
            if not (isinstance(ev_b.get("db_status"), str)
                    and ev_b.get("db_status") == b_row.get("status")):
                a3_bad.append(f"B: evidence db_status "
                              f"{ev_b.get('db_status')!r} != controlled row "
                              f"{b_row.get('status')!r}")
            # C shape: gate true + id inside the result's logical_path +
            # result evidence COMMITTED / lineage / hash true.
            ev = ((abc.get("evidence") or {}).get(
                "terminal_commit_fault") or {})
            lp = ev.get("logical_path") or ""
            m = re.search(r"sync-([0-9a-f-]{36})", lp)
            if not (c_row.get("gate_entered") == "true"
                    and m and m.group(1) == c_row["id"]):
                a3_bad.append("C: gate!=true or logical_path UUID mismatch")
            if not (ev.get("txn_status") == "COMMITTED"
                    and ev.get("lineage_run_match") is True
                    and ev.get("artifact_sha256_matches_txn") is True):
                a3_bad.append("C: result evidence not COMMITTED/lineage/"
                              "hash-true")
            # C LIVE (item3): target explicitly == the C run's own output,
            # txn AND download both HTTP 200, a real txn id + exactly one
            # valid bound file, COMMITTED, lineage bound to C, and the
            # downloaded bytes' sha256 == the txn's content hash.
            live = jload("11-c-target-live.json") or {}
            if not (live.get("ok") is True
                    and live.get("run_id") == c_row["id"]):
                a3_bad.append("C live target incomplete/unbound")
            else:
                if live.get("target") != c_row.get("output_dataset_id"):
                    a3_bad.append("C live target != C output_dataset_id")
                if live.get("txn_http") != 200:
                    a3_bad.append(f"C live txn_http "
                                  f"{live.get('txn_http')!r} != 200")
                if live.get("download_http") != 200:
                    a3_bad.append(f"C live download_http "
                                  f"{live.get('download_http')!r} != 200")
                if not (isinstance(live.get("txn_id"), str)
                        and live.get("txn_id")):
                    a3_bad.append("C live txn_id missing")
                if not (isinstance(live.get("bound_files"), list)
                        and len(live["bound_files"]) == 1
                        and all(isinstance(x, str) and x
                                for x in live["bound_files"])):
                    a3_bad.append(f"C live bound_files "
                                  f"{live.get('bound_files')!r} != [1 valid]")
                if not (live.get("txn_status") == "COMMITTED"
                        and live.get("lineage_run") == c_row["id"]
                        and live.get("sha256")
                        and live.get("sha256") == live.get("content_hash")):
                    a3_bad.append("C live txn/lineage/hash mismatch")
    check("A3-prime-B-lease-shape-C-committed-bound",
          a1_ok and not a3_bad, "; ".join(a3_bad[:4]))

    # ---- B': old four pre-boot; new two reported post-boot --------------
    boot_raw = (d / "02-inspect-connector.txt").read_text() \
        if (d / "02-inspect-connector.txt").is_file() else ""
    boot = boot_raw.strip().split("|")[0] if boot_raw.strip() else ""
    pre_ok = RFC3339.match(boot) is not None
    post_new = []
    if pre_ok:
        for rid in sorted(pins):
            st = (by_id.get(rid) or {}).get("started_at", "")
            try:
                pre_ok = pre_ok and parse_ts(st) < parse_ts(boot)
            except ValueError:
                pre_ok = False
        for rid in new:
            st = (by_id.get(rid) or {}).get("started_at", "")
            try:
                post_new.append(parse_ts(st) > parse_ts(boot))
            except ValueError:
                post_new.append(False)
    check("B-prime-old4-pre-boot-new2-reported-post-boot",
          pre_ok and all(post_new) and len(post_new) == 2,
          f"boot={boot} new2_post_boot={post_new}")

    # ---- F': fixture counters ---------------------------------------------
    fx = jload("09-fixture-counts.json") or {}
    counts = fx.get("counts") or {}
    bl = json.loads((P23_FINAL / "00-baseline-manifest.json").read_text())
    bl_steps = {s["step"]: s["detail"] for s in bl["steps"]}
    want_c = bl_steps.get("p21_bc_fixture_counts", {})
    trio_ok = all(
        all(counts.get(n, {}).get(k) == want_c.get(n, {}).get(k)
            for k in ("request_count", "inventory_started",
                      "inventory_finished", "active", "total", "mode"))
        for n in ("p21a-b6ec49ab", "p21b-b6ec49ab", "p21c-b6ec49ab"))
    pair_ok = all((counts.get(fx_of[k]) or {}).get("active") == 0
                  for k in ("B", "C"))
    check("F-prime-fixture-counts-legacy-equal-attempt-inactive",
          trio_ok and pair_ok,
          f"trio={trio_ok} attempt_pair={pair_of(counts, fx_of)}")

    # ---- O': owned final state by FRESH SQL --------------------------------
    defs_rows = jload("12a-owned-defs.json")
    grants = jload("12b-owned-grants.json")
    glob = jload("12c-global-nonempty-cron.json")
    o_bad = []
    if defs_rows is None or grants is None or glob is None:
        o_bad.append("12a/b/c missing")
    else:
        got_ids = sorted(r["id"] for r in defs_rows)
        if got_ids != sorted(owned_bindings):
            o_bad.append(f"owned set mismatch {got_ids} vs "
                         f"{sorted(owned_bindings)}")
        for r in defs_rows:
            b = owned_bindings.get(r["id"], {})
            if not (r.get("enabled") == "false"
                    and r.get("cursor_null") == "true"
                    and r.get("source_id") == b.get("source_id")
                    and r.get("output_dataset_id") == b.get("target_id")):
                o_bad.append(f"{r['id'][:8]}: not disabled/NULL/mismatch")
        if grants:
            o_bad.append(f"grant rows remain: {grants}")
        want_cron = sorted(r["id"] for r in defs_rows if r.get("cron"))
        if sorted(x["id"] for x in glob) != want_cron:
            o_bad.append(f"global nonempty-cron set != owned cron set")
    check("O-prime-owned-disabled-nullcursor-revoked-cronset",
          not o_bad and bool(owned_bindings), "; ".join(o_bad[:4]))

    # ---- C'-G': fixture facts / base5 / volumes / env / legacy ------------
    fa = (d / "03a-inspect-fixture-state.txt").read_text() \
        if (d / "03a-inspect-fixture-state.txt").is_file() else ""
    fa = fa.strip().split("|")
    fb = (d / "03b-inspect-fixture-pid1.txt").read_text() \
        if (d / "03b-inspect-fixture-pid1.txt").is_file() else ""
    try:
        cmd = json.loads(fb.strip().split("|")[1]) \
            if "|" in fb else None
    except (ValueError, IndexError):
        cmd = None
    check("C-prime-fixture-unchanged",
          len(fa) > 3 and fa[0] == "2026-11-13T18:29:14.176785055Z"
          and fa[1] == "true" and fa[3] == "no"
          and cmd == ["python3", "-I", "/srv/rest_source.py",
                      "0.0.0.0", "8090"],
          f"boot={fa[0] if fa else None} pid1={cmd}")

    lines = (d / "01-docker-ps.txt").read_text().splitlines() \
        if (d / "01-docker-ps.txt").is_file() else [""]
    header = lines[0]
    up_ok, ups = False, []
    if "STATUS" in header:
        s_i = header.index("STATUS")
        p_i = header.find("PORTS", s_i)
        p_i = p_i if p_i != -1 else len(header) + 60
        up_ok = True
        for s in ("connector", "dataset", "pipeline", "fixture",
                  "postgres"):
            line = next((l for l in lines[1:] if f"-{s}-1" in l), "")
            status = line[s_i:p_i].strip() if line else ""
            up_ok = up_ok and status.startswith("Up")
            ups.append(f"{s}:'{status or 'NO-LINE'}'")
    check("D-prime-base5-positive-up", up_ok, " ".join(ups))

    vols = (d / "04-volumes.txt").read_text() \
        if (d / "04-volumes.txt").is_file() else ""
    missing = [v for v in (
        "openfoundry-platform-p1-q3v9_dataset-files",
        "openfoundry-platform-p1-q3v9_go-build-cache",
        "openfoundry-platform-p1-q3v9_postgres-data") if v not in vols]
    check("E-prime-volumes", not missing, f"missing={missing}")

    mode = (d / "05-env-mode.txt").read_text().strip() \
        if (d / "05-env-mode.txt").is_file() else ""
    check("F-prime-env-600", mode == "600", f"mode={mode!r}")

    leg_ok = True
    for label in BASE.P21_LEGACY_RUNS:
        raw = (d / f"08-legacy-{label}.txt").read_text() \
            if (d / f"08-legacy-{label}.txt").is_file() else ""
        ls = [l for l in raw.splitlines()
              if l and not l.startswith("SET")]
        want = bl_steps.get("p21_bc_projection", {}).get(label, {})
        if not ls or not want:
            leg_ok = False
            continue
        vals = dict(zip(BASE.PROJECTION_KEYS, ls[-1].split("|")))
        leg_ok = leg_ok and all(vals.get(k) == str(want.get(k))
                                for k in BASE.PROJECTION_KEYS)
    check("G-prime-legacy-bc-unchanged", leg_ok)

    all_ok = all(c["ok"] for c in checks)
    vp = d.parent / f"postfacts-verdict-{d.name}.json"
    n = 0
    while vp.exists():
        n += 1
        vp = d.parent / f"postfacts-verdict-{d.name}-r{n}.json"
    vp.write_text(json.dumps(
        {"checks": checks, "all_ok": all_ok,
         "locked_fallback": True,
         "attempt_dir": d.name,
         "new_two_full_chain": {rid: by_id.get(rid)
                                for rid in new},
         "note": "LOCKED SAFE FALLBACK: the ABC new two did NOT "
                 "undergo the final restart; full-chain rows archived "
                 "for root review — never auto-pinned (fixed pins "
                 "stay 4)"}, indent=1, sort_keys=True) + "\n")
    print(f"[postfacts] all_ok={all_ok} -> {vp.name}")
    return 0 if all_ok else 1


def pair_of(counts, fx_of):
    return {k: (counts.get(v) or {}).get("active")
            for k, v in fx_of.items()}


if __name__ == "__main__":
    sys.exit(main())

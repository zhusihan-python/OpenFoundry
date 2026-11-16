#!/usr/bin/env python3
"""P2.5 foundation SELFTEST — cohort derive/guard (pure; no runtime).

Layers:
  A. REAL frozen-archive derivation into a tmp output (pure file reads
     only): generate → import → --check → tamper → --check must fail.
  B. Copied-tree mutations: any tampering with a manifest-indexed
     input, the pins module, or the manifest index itself must abort
     derive non-zero (the SHA chain is the designed defense).
  C. Direct schema-validator battery: partial/extra/type-error rows.
  D. Guard adjudicator battery on synthetic rows built from the real
     roster (both policies; every root round-1 hard-refuse case).
  E. Text equality of ROW_SQL/SESSION_SQL/_parse_ts behavior vs the
     frozen p24_live_guard.py (transcription-drift guard).

Exit 0 iff every case behaves as expected; failures print loudly.
"""

import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
REAL_FINAL = HERE.parent / "business-platform-p2-4" / "results" / \
    "2026-11-14-p24-final"
REAL_TOOLS = HERE.parent / "business-platform-p2-4"

PASS, FAIL = [], []


def check(name, cond, detail=""):
    if cond:
        PASS.append(name)
        print(f"  ok: {name}")
    else:
        FAIL.append(f"{name} {detail}")
        print(f"  FAIL: {name} {detail}")


def run_derive(final_dir, tools_dir, out, *extra):
    return subprocess.run(
        [sys.executable, "-I", str(HERE / "p25_cohort_derive.py"),
         "--p24-final-dir", str(final_dir),
         "--p24-tools-dir", str(tools_dir),
         "--out", str(out), *extra],
        capture_output=True, text=True, timeout=120)


def loadmod(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def make_mini_archive(tmp: Path):
    """Copy only the files derive needs; manifest+inputs stay
    byte-identical so the real SHA chain keeps verifying."""
    mini_final = tmp / "final"
    for sub in ("00-preflight", "13-persist-post-r1",
                "13-postfacts-attempt-20261114T152612"):
        (mini_final / sub).mkdir(parents=True, exist_ok=True)
    keep = ["hash-manifest.json", "FINAL-NEW-TWO-PROVENANCE.json",
            "15-final-base3-images.json", "16-tools-sha-final.json",
            "01-source-hash-pre.txt",
            "00-preflight/preflight-verdict-r1.json",
            "00-preflight/09-fixture-counts.json",
            "13-persist-post-r1/persist-manifest.json",
            "13-persist-post-r1/defs.json",
            "13-persist-post-r1/ticks.json",
            "13-persist-post-r1/runs.json",
            "13-persist-post-r1/grants.json",
            "13-postfacts-attempt-20261114T152612/09-fixture-counts.json",
            "13-postfacts-attempt-20261114T152612/11-c-target-live.json",
            "13-postfacts-attempt-20261114T152612/04-volumes.txt",
            "13-postfacts-attempt-20261114T152612/05-env-mode.txt",
            "13-postfacts-attempt-20261114T152612/"
            "03a-inspect-fixture-state.txt",
            "13-postfacts-attempt-20261114T152612/"
            "03b-inspect-fixture-pid1.txt",
            "13-postfacts-attempt-20261114T152612/"
            "08-legacy-B_lease_lost_unpublished.txt",
            "13-postfacts-attempt-20261114T152612/"
            "08-legacy-C_committed_unconfirmed.txt",
            "13-postfacts-attempt-20261114T152612/"
            "12c-global-nonempty-cron.json",
            "owned-history-20261114T150304.json"]
    for rel in keep:
        shutil.copy2(REAL_FINAL / rel, mini_final / rel)
    mini_tools = tmp / "tools"
    mini_tools.mkdir(exist_ok=True)
    shutil.copy2(REAL_TOOLS / "p24_pins.py", mini_tools / "p24_pins.py")
    # derive also reads the frozen P22/P21 tool files relative to the
    # tools dir — mirror that layout in the mini tree
    p21_dir = tmp / "business-platform-p2-1"
    p21_dir.mkdir(exist_ok=True)
    shutil.copy2(REAL_TOOLS.parent / "business-platform-p2-1" /
                 "smoke_p21.py", p21_dir / "smoke_p21.py")
    p22_dir = tmp / "business-platform-p2-2"
    p22_dir.mkdir(exist_ok=True)
    shutil.copy2(REAL_TOOLS.parent / "business-platform-p2-2" /
                 "readonly_baseline.py", p22_dir / "readonly_baseline.py")
    return mini_final, mini_tools


def main():
    derive = loadmod(HERE / "p25_cohort_derive.py", "derive_mod")
    guard = loadmod(HERE / "p25_cohort_guard.py", "guard_mod")

    print("[A] real-archive derivation (pure file reads)")
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        out = tmp / "p25_cohort6.py"
        r = run_derive(REAL_FINAL, REAL_TOOLS, out)
        check("A1 derive real archive rc0", r.returncode == 0, r.stderr)
        check("A2 generated file exists", out.is_file())
        mod = loadmod(out, "gen1")
        check("A3 GROUP1 == 4", len(mod.GROUP1) == 4)
        check("A4 GROUP2 == 2", len(mod.GROUP2) == 2)
        check("A5 final anchor", mod.FINAL_ANCHOR ==
              "2026-11-14T07:11:20.883030213Z", mod.FINAL_ANCHOR)
        check("A6 full image", mod.FINAL_IMAGE ==
              "sha256:fe3208b3fcbd4d5fe428647e106d6ba6aa5b71fd94e9ba51144a9565258dc4c3")
        check("A7 aux absent 3x0", set(mod.AUX_ABSENT.values()) == {0})
        r = run_derive(REAL_FINAL, REAL_TOOLS, out, "--check")
        check("A8 --check rc0", r.returncode == 0, r.stderr)
        text = out.read_text()
        out.write_text(text.replace('"status": "running"',
                                    '"status": "succeeded"', 1))
        r = run_derive(REAL_FINAL, REAL_TOOLS, out, "--check")
        check("A9 tampered output --check fails",
              r.returncode != 0, r.stdout)

    print("[B] copied-tree tamper battery (SHA chain hard-refuse)")
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        mini_final, mini_tools = make_mini_archive(tmp)
        out = tmp / "cohort6.py"
        r = run_derive(mini_final, mini_tools, out)
        check("B0 mini archive derives rc0 (copies keep SHAs)",
              r.returncode == 0, r.stderr)

        (mini_final / "FINAL-NEW-TWO-PROVENANCE.json").write_text(
            (mini_final / "FINAL-NEW-TWO-PROVENANCE.json").read_text()
            .replace("running", "RUNNING", 1))
        r = run_derive(mini_final, mini_tools, out)
        check("B1 mutated provenance aborts", r.returncode != 0)

        mini_final2, mini_tools2 = make_mini_archive(tmp)
        # drop one row -> 1 postboot2 row
        prov = json.loads((mini_final2 /
                           "FINAL-NEW-TWO-PROVENANCE.json").read_text())
        prov["rows"].pop(next(iter(prov["rows"])))
        (mini_final2 / "FINAL-NEW-TWO-PROVENANCE.json").write_text(
            json.dumps(prov, indent=1))
        # fix the manifest entry so the SHA check fires on CONTENT
        r = run_derive(mini_final2, mini_tools2, out)
        check("B2 provenance row removed aborts (sha)", r.returncode != 0)

        mini_final3, mini_tools3 = make_mini_archive(tmp)
        (mini_tools3 / "p24_pins.py").write_text(
            "PINNED_EXCEPTIONS = []\n")
        r = run_derive(mini_final3, mini_tools3, out)
        check("B3 tampered pins aborts (toolSHA)", r.returncode != 0)

        mini_final4, mini_tools4 = make_mini_archive(tmp)
        hm = json.loads((mini_final4 / "hash-manifest.json").read_text())
        hm["files"].pop("15-final-base3-images.json")
        (mini_final4 / "hash-manifest.json").write_text(json.dumps(hm))
        # manifest self-sha now differs -> aborts even earlier
        r = run_derive(mini_final4, mini_tools4, out)
        check("B4 deindexed anchor file aborts (manifest seal)",
              r.returncode != 0)

        mini_final5, mini_tools5 = make_mini_archive(tmp)
        img = json.loads(
            (mini_final5 / "15-final-base3-images.json").read_text())
        img["running"]["connector"]["image"] = "fe3208b3fcbd"  # short id
        (mini_final5 / "15-final-base3-images.json").write_text(
            json.dumps(img))
        r = run_derive(mini_final5, mini_tools5, out)
        check("B5 short-id image aborts (sha fires first)",
              r.returncode != 0)

    print("[C] schema validator battery (direct calls)")
    base = {k: f"v-{k}" for k in derive.SCHEMA_27}
    base["started_at"] = "2026-11-13 19:22:35.118352+00"
    base["finished_at"] = ""
    try:
        derive.validate_row_schema(dict(base), "t", 0)
        check("C1 full 27-key row accepted", True)
    except SystemExit:
        check("C1 full 27-key row accepted", False, "exited")
    partial = dict(base)
    del partial["generation"]
    try:
        derive.validate_row_schema(partial, "t", 0)
        check("C2 partial (26 keys) refused", False, "accepted")
    except SystemExit:
        check("C2 partial (26 keys) refused", True)
    extra = dict(base)
    extra["surprise"] = "x"
    try:
        derive.validate_row_schema(extra, "t", 0)
        check("C3 extra 28th key refused", False, "accepted")
    except SystemExit:
        check("C3 extra 28th key refused", True)
    badtype = dict(base)
    badtype["attempt"] = 1
    try:
        derive.validate_row_schema(badtype, "t", 0)
        check("C4 non-string value refused", False, "accepted")
    except SystemExit:
        check("C4 non-string value refused", True)
    nulstart = dict(base)
    nulstart["started_at"] = ""
    try:
        derive.validate_row_schema(nulstart, "t", 0)
        check("C5 empty started_at refused", False, "accepted")
    except SystemExit:
        check("C5 empty started_at refused", True)
    badts = dict(base)
    badts["started_at"] = "not-a-time"
    try:
        derive.validate_row_schema(badts, "t", 0)
        check("C6 unparseable started_at refused", False, "accepted")
    except SystemExit:
        check("C6 unparseable started_at refused", True)

    print("[D] guard adjudicator battery")
    roster = loadmod(HERE / "p25_cohort6.py", "roster_real")
    fb, img = roster.FINAL_ANCHOR, roster.FINAL_IMAGE
    rows = [dict(r) for r in roster.GROUP1 + roster.GROUP2]
    ok, reasons = guard.adjudicate_first_transition(rows, roster, fb, img)
    check("D1 first-transition valid 6 passes", ok, reasons)

    rows_bad = [dict(r) for r in roster.GROUP1 + roster.GROUP2]
    del rows_bad[0]["generation"]
    ok, _ = guard.adjudicate_first_transition(rows_bad, roster, fb, img)
    check("D2 live partial row (26 keys) blocks", not ok)

    rows_bad = [dict(r) for r in roster.GROUP1 + roster.GROUP2]
    rows_bad[1]["extra_key"] = "x"
    ok, _ = guard.adjudicate_first_transition(rows_bad, roster, fb, img)
    check("D3 live extra-key row blocks", not ok)

    rows_bad = [dict(r) for r in roster.GROUP1 + roster.GROUP2]
    rows_bad.append(dict(rows_bad[0], id="00000000-0000-0000-0000-000000000009"))
    ok, _ = guard.adjudicate_first_transition(rows_bad, roster, fb, img)
    check("D4 unknown 7th controlled row blocks", not ok)

    rows_bad = [dict(r) for r in roster.GROUP1 + roster.GROUP2]
    rows_bad[2] = dict(rows_bad[1])   # duplicate id
    ok, _ = guard.adjudicate_first_transition(rows_bad, roster, fb, img)
    check("D5 duplicate row id blocks", not ok)

    rows_bad = [dict(r) for r in roster.GROUP1 + roster.GROUP2]
    del rows_bad[3]
    ok, _ = guard.adjudicate_first_transition(rows_bad, roster, fb, img)
    check("D6 missing roster row blocks", not ok)

    rows_bad = [dict(r) for r in roster.GROUP1 + roster.GROUP2]
    rows_bad[0]["heartbeat_at"] = "2030-01-01 00:00:00+00"
    ok, _ = guard.adjudicate_first_transition(rows_bad, roster, fb, img)
    check("D7 field drift blocks", not ok)

    rows_bad = [dict(r) for r in roster.GROUP1 + roster.GROUP2]
    g2 = 4  # first group2 index
    rows_bad[0]["started_at"], rows_bad[g2]["started_at"] = (
        rows_bad[g2]["started_at"], rows_bad[0]["started_at"])
    ok, _ = guard.adjudicate_first_transition(rows_bad, roster, fb, img)
    check("D8 group swap (started_at exchange) blocks", not ok)

    ok, _ = guard.adjudicate_first_transition(rows, roster,
                                              "2026-11-14T06:35:00.000000000Z",
                                              img)
    check("D9 wrong live boot (not sealed final) blocks", not ok)
    ok, _ = guard.adjudicate_first_transition(rows, roster, fb,
                                              "sha256:" + "0" * 64)
    check("D10 wrong live image blocks", not ok)
    ok, _ = guard.adjudicate_first_transition(rows, roster, fb, "fe3208b3fcbd")
    check("D11 short-id live image blocks", not ok)

    steady_boot = "2026-11-14T23:00:00.000000000Z"
    ok, reasons = guard.adjudicate_steady(rows, roster, steady_boot)
    check("D12 steady valid passes (all6 < target)", ok, reasons)
    ok, _ = guard.adjudicate_steady(rows, roster,
                                    "2026-11-14T07:20:00.000000000Z")
    check("D13 steady target boot before postboot2 blocks", not ok)
    ok, _ = guard.adjudicate_steady(rows, roster, steady_boot,
                                    "sha256:" + "a" * 64,
                                    live_boot=steady_boot,
                                    live_image="sha256:" + "b" * 64)
    check("D14 steady live/target image mismatch blocks", not ok)
    rows_bad = [dict(r) for r in roster.GROUP1 + roster.GROUP2]
    rows_bad[5]["started_at"] = "2026-11-15 00:00:00+00"
    ok, _ = guard.adjudicate_steady(rows_bad, roster, steady_boot)
    check("D15 steady future-started row blocks", not ok)

    print("[F] roster-load hardening (code-review items 2/3)")
    try:
        r_mod = guard.load_roster()
        check("F1 load_roster accepts the real generated roster",
              len(r_mod.GROUP1) == 4)
    except RuntimeError as exc:
        check("F1 load_roster accepts the real generated roster",
              False, str(exc))

    import types
    damaged_row = dict(roster.GROUP1[0])
    damaged_row.pop("generation")          # truncated expected record
    dmg = types.SimpleNamespace(
        SCHEMA_27=roster.SCHEMA_27,
        GROUP1=[damaged_row] + list(roster.GROUP1[1:]),
        GROUP2=list(roster.GROUP2),
        FINAL_ANCHOR=roster.FINAL_ANCHOR,
        HISTORICAL_ANCHOR_OLD4=roster.HISTORICAL_ANCHOR_OLD4,
        FINAL_IMAGE=roster.FINAL_IMAGE)
    rows_d = [dict(r) for r in roster.GROUP1 + roster.GROUP2]
    rows_d[0]["generation"] = "9"          # drift the missing key exempts
    ok, reasons = guard.adjudicate_first_transition(rows_d, dmg, fb, img)
    check("F2 damaged expected row + live drift REFUSED (not exempted)",
          (not ok) and any("damaged expected roster" in r
                           for r in reasons), str(reasons[:2]))

    err = guard.validate_steady_args("2026-11-14T23:00:00Z", None)
    check("F3a steady without target-image refused", err is not None)
    err = guard.validate_steady_args("2026-11-14T23:00:00Z", "deadbeef")
    check("F3b steady short-id image refused", err is not None)
    err = guard.validate_steady_args(
        "2026-11-14T23:00:00Z", "sha256:" + "c" * 64)
    check("F3c steady full image+boot accepted", err is None)

    with tempfile.TemporaryDirectory() as td:
        t = Path(td)
        for f in ("p25_cohort6.py", "p24-frozen-baseline.json",
                  "p25_cohort_guard.py"):
            shutil.copy2(HERE / f, t / f)
        (t / "p25_cohort6.py").write_text(
            (t / "p25_cohort6.py").read_text() + "# tampered\n")
        g2 = loadmod(t / "p25_cohort_guard.py", "g_tamper")
        try:
            g2.load_roster()
            check("F4 tampered roster file refused by sha binding",
                  False, "no RuntimeError")
        except RuntimeError as exc:
            check("F4 tampered roster file refused by sha binding",
                  "sha" in str(exc), str(exc)[:120])

    print("[E] frozen-P24 text/behavior parity")
    p24_guard = loadmod(REAL_TOOLS / "p24_live_guard.py", "p24_guard")
    check("E1 ROW_SQL identical to frozen P24",
          guard.ROW_SQL == p24_guard.ROW_SQL)
    check("E2 SESSION_SQL identical", guard.SESSION_SQL ==
          p24_guard.SESSION_SQL)
    a = guard._parse_ts("2026-11-13 19:22:35.1+00", "x")
    b = p24_guard._parse_ts("2026-11-13 19:22:35.1+00")  # 1-arg in P24
    check("E3 _parse_ts same semantics", a == b)
    check("E4 guard RELEASED_FOR_EXECUTION is False",
          guard.RELEASED_FOR_EXECUTION is False)
    check("E5 schema 27 keys", len(roster.SCHEMA_27) == 27 and
          set(roster.SCHEMA_27) == guard.expected_schema())

    print(f"\n[selftest-p25-cohort] pass={len(PASS)} fail={len(FAIL)}")
    for f in FAIL:
        print(f"  FAILED: {f}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""P2.4 preflight VERIFIER (pure — reads only 00-preflight artifacts).

Root fix ② (2026-11-14): a green content check can NEVER produce
all_ok=true on top of a failed/partial collection —
  * every consumed artifact is BOUND in the verdict: concrete raw path,
    derived-JSON path, its .rc file, the rc VALUE, sha256 of both, and
    the -rN attempt number;
  * ALL bound rcs must exist and be 0 (rc gate participates in all_ok);
  * derived-JSON rc files never exist on their own — the rc always sits
    on the same-stem RAW .txt artifact (that is where the collector
    writes it); the old verdict's artifact_rc map wrongly cited the
    retired failed 03-inspect-fixture and a nonexistent 06 json.rc;
  * verdicts are collision-safe (-rN, history never overwritten) plus a
    single preflight-verdict-INDEX.json pointer file (the ONLY file this
    verifier rewrites — sanctioned by root as "update the index").

Hard checks (unchanged semantics): A 4x27 exact pins (incl. live org
triple) / B real-datetime pre-boot / C fixture unchanged / D base5 /
E volumes / F env600 / G legacy==frozen baseline / H fixture counts /
I no non-empty schedule_cron (+ correct sync.admission.min_interval_ms
sweep evidence, root fix ③).

Usage: python3 -I verify_preflight_p24.py
Exit 0 iff all checks ok AND the rc gate passes.
"""

import hashlib
import importlib.util
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "results" / "2026-11-14-p24-final" / "00-preflight"
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
    return hashlib.sha256(p.read_bytes()).hexdigest()


def artifact(stem):
    """Collision-safe NEW output path for this verifier's own writes."""
    p = OUT / stem
    n = 0
    while p.exists():
        n += 1
        p = OUT / re.sub(r"(\.\w+)?$", rf"-r{n}\1", stem, count=1)
    return p


def bind(base, ext="txt"):
    """Resolve the LATEST collection attempt for a step and bind its
    evidence: raw artifact (+sha), derived json (+sha) when it exists,
    and the rc VALUE read from the RAW artifact's .rc sidecar."""
    m = re.match(r"^(.*?)(?:-r(\d+))?$", base)
    core = m.group(1)
    best = None
    for p in OUT.iterdir():
        mm = re.fullmatch(re.escape(core) + r"(?:-r(\d+))?\." +
                          re.escape(ext.lstrip(".")), p.name)
        if mm:
            n = int(mm.group(1) or 0)
            if best is None or n > best[0]:
                best = (n, p)
    if best is None:
        return {"base": base, "attempt": None, "missing": True}
    n, raw = best
    entry = {"base": base, "attempt": n, "raw": raw.name,
             "raw_sha256": _sha(raw)}
    rc_file = OUT / (raw.name + ".rc")
    if rc_file.exists():
        entry["rc_file"] = rc_file.name
        entry["rc"] = int(rc_file.read_text().strip())
    else:
        entry["rc"] = None
        entry["rc_file"] = None
    derived = raw.with_suffix("").with_suffix(".json") \
        if ext == "txt" else None
    if derived and derived.exists():
        entry["derived"] = derived.name
        entry["derived_sha256"] = _sha(derived)
    return entry


def latest_json(entry):
    """Path of the derived json for a binding (None if absent)."""
    name = entry.get("derived")
    return OUT / name if name else None


def main():
    checks = []
    bindings = {}

    def check(name, ok, detail="", binds=()):
        for b in binds:
            bindings[b["base"]] = b
        checks.append({"name": name, "ok": bool(ok), "detail": detail})
        print(f"[verify] {'ok  ' if ok else 'FAIL'} {name} {detail}")

    b01 = bind("01-docker-ps")
    b02 = bind("02-inspect-connector")
    b03a = bind("03a-inspect-fixture-state")
    b03b = bind("03b-inspect-fixture-pid1")
    b04 = bind("04-volumes")
    b05 = bind("05-env-mode")
    b06 = bind("06-controlled-24safe3org")
    b07a = bind("07a-cron-column-sweep")
    b07b = bind("07b-sync-admission-min-interval")
    b08b = bind("08-legacy-B_lease_lost_unpublished")
    b08c = bind("08-legacy-C_committed_unconfirmed")
    b09 = bind("09-fixture-counts", ext="json")

    # ---- A. controlled rows == pins (27 fields) ------------------------
    rows_path = latest_json(b06)
    rows = json.loads(rows_path.read_text()) if rows_path else []
    pins = {p["id"]: p for p in GUARD.PINNED_EXCEPTIONS}
    got = {r["id"]: r for r in rows} if rows else {}
    drift = []
    for rid, pin in sorted(pins.items()):
        row = got.get(rid)
        if row is None:
            drift.append(f"{rid} MISSING (rows={sorted(got)})")
            continue
        for k, want in pin.items():
            if row.get(k) != want:
                drift.append(f"{rid}.{k} got={row.get(k)!r} want={want!r}")
    unknown = sorted(set(got) - set(pins))
    check("A-controlled-4rows-27fields", not drift and not unknown
          and len(got) == 4, f"drift={drift[:6]} unknown={unknown}",
          binds=[b06])

    # ---- B. real-datetime started < connector boot ----------------------
    raw02 = (OUT / b02["raw"]).read_text() if b02.get("raw") else ""
    boot = raw02.strip().split("|")[0] if raw02.strip() else ""
    pre_ok, pre_detail = False, [f"connector inspect unreadable: {raw02!r}"]
    if RFC3339.match(boot):
        pre_detail = []
        pre_ok = True
        for rid, pin in sorted(pins.items()):
            st = (got.get(rid) or {}).get("started_at", "")
            try:
                earlier = GUARD._parse_ts(st) < GUARD._parse_ts(boot)
            except ValueError as exc:
                earlier, st = False, f"unparseable: {exc}"
            pre_ok = pre_ok and earlier
            pre_detail.append(f"{rid[:8]} started={st} < boot={boot} "
                              f"-> {earlier}")
    check("B-started-pre-boot-real-datetime", pre_ok, "; ".join(pre_detail),
          binds=[b02, b06])

    # ---- C. fixture facts (03a state + 03b PID1) -------------------------
    fa = ((OUT / b03a["raw"]).read_text() if b03a.get("raw")
          else "").strip().split("|")
    while len(fa) < 4:
        fa.append("")
    f_started, f_running, f_image, f_restart = fa[:4]
    fb = ((OUT / b03b["raw"]).read_text() if b03b.get("raw")
          else "").strip().split("|")
    try:
        cmd = json.loads(fb[1]) if len(fb) > 1 and fb[1] else None
    except ValueError:
        cmd = None
    check("C-fixture-unchanged",
          f_started == "2026-11-13T18:29:14.176785055Z"
          and f_running == "true"
          and cmd == ["python3", "-I", "/srv/rest_source.py",
                      "0.0.0.0", "8090"]
          and f_restart == "no",
          f"boot={f_started} running={f_running} pid1_cmd={cmd} "
          f"restart={f_restart}", binds=[b03a, b03b])

    # ---- D. base5 running — POSITIVE truth (root fix: Restarting/Dead are
    # NOT up; parse the STATUS column by header offsets, require "Up" prefix)
    ps = (OUT / b01["raw"]).read_text() if b01.get("raw") else ""
    ps_lines = ps.splitlines()
    ps_ok, ps_d = False, ["docker ps output unreadable"]
    if ps_lines:
        header = ps_lines[0]
        try:
            s_i = header.index("STATUS")
            p_i = header.find("PORTS", s_i)
            if p_i == -1:
                p_i = len(header) + 40
            ps_ok = True
            ps_d = []
            for s in ("connector", "dataset", "pipeline", "fixture",
                      "postgres"):
                line = next((l for l in ps_lines[1:] if f"-{s}-1" in l), "")
                status = line[s_i:p_i].strip() if line else ""
                good = status.startswith("Up")   # positive; Restarting/
                ps_ok = ps_ok and good           # Dead/Exited all fail here
                ps_d.append(f"{s}:'{status or 'NO-LINE'}'")
        except ValueError:
            ps_ok, ps_d = False, ["STATUS column not found in header"]
    check("D-base5-running", ps_ok, " ".join(ps_d), binds=[b01])

    # ---- E. volumes ---------------------------------------------------------
    vols = (OUT / b04["raw"]).read_text() if b04.get("raw") else ""
    missing = sorted(v for v in (
        "openfoundry-platform-p1-q3v9_dataset-files",
        "openfoundry-platform-p1-q3v9_go-build-cache",
        "openfoundry-platform-p1-q3v9_postgres-data") if v not in vols)
    check("E-volumes-present", not missing, f"missing={missing}",
          binds=[b04])

    # ---- F. env mode --------------------------------------------------------
    mode = ((OUT / b05["raw"]).read_text() if b05.get("raw")
            else "").strip()
    check("F-env-mode-600", mode == "600",
          f"mode={mode!r} (values never read)", binds=[b05])

    # ---- G. legacy B/C vs frozen P23-final baseline --------------------------
    bl = json.loads((P23_FINAL / "00-baseline-manifest.json").read_text())
    bl_steps = {s["step"]: s["detail"] for s in bl["steps"]}
    leg_ok, leg_d = True, []
    for label, be in (("B_lease_lost_unpublished", b08b),
                      ("C_committed_unconfirmed", b08c)):
        raw = (OUT / be["raw"]).read_text() if be.get("raw") else ""
        lines = [l for l in raw.splitlines() if l and not l.startswith("SET")]
        want = bl_steps.get("p21_bc_projection", {}).get(label, {})
        if not lines or not want:
            leg_ok = False
            leg_d.append(f"{label}: missing raw or baseline")
            continue
        vals = dict(zip(BASE.PROJECTION_KEYS, lines[-1].split("|")))
        diffs = {k: (vals.get(k), want.get(k)) for k in
                 BASE.PROJECTION_KEYS if vals.get(k) != str(want.get(k))}
        leg_ok = leg_ok and not diffs
        leg_d.append(f"{label}: " + ("equal" if not diffs else f"{diffs}"))
    check("G-legacy-bc-frozen-equal", leg_ok, "; ".join(leg_d),
          binds=[b08b, b08c])

    # ---- H. fixture counters b6ec49ab trio ------------------------------------
    fx_path = latest_json(b09) or (OUT / b09["raw"] if b09.get("raw")
                                   else None)
    counts = json.loads(fx_path.read_text())["counts"] if fx_path else {}
    want_counts = bl_steps.get("p21_bc_fixture_counts", {})
    cnt_ok, cnt_d = True, []
    for name in ("p21a-b6ec49ab", "p21b-b6ec49ab", "p21c-b6ec49ab"):
        w = want_counts.get(name, {})
        g = counts.get(name, {})
        same = all(g.get(k) == w.get(k) for k in
                   ("request_count", "inventory_started",
                    "inventory_finished", "active", "total", "mode"))
        cnt_ok = cnt_ok and same
        cnt_d.append(f"{name}: "
                     + ("equal-to-baseline" if same else f"{g} != {w}"))
    others = {k: v for k, v in counts.items() if "b6ec49ab" not in k}
    check("H-fixture-counts-frozen-equal", cnt_ok,
          "; ".join(cnt_d) + f" | others(reported): {others}",
          binds=[b09])

    # ---- I. schedule_cron sweep — REQUIRED SET derived from THIS run's 07a
    # catalog (root fix: a table whose query is merely ABSENT must FAIL,
    # never pseudo-green). Each catalog hit must have raw+rc+derived JSON
    # with rc==0; cron tables must be empty lists.
    catalog_path = latest_json(b07a)
    catalog = json.loads(catalog_path.read_text()) if catalog_path else None
    cron_tables, interval_tables, problems_i = {}, {}, []
    if catalog is None:
        problems_i.append("07a catalog derived JSON missing — required set "
                          "cannot be derived (refuse, never vacuous-pass)")
        catalog = []
    elif not catalog:
        problems_i.append("07a catalog EMPTY — schema guarantees cron/"
                          "interval hits, so the sweep itself is broken")

    required_binds = [b07a, b07b]
    for c in catalog or []:
        tbl, col = c["table_name"], c["column_name"]
        if not (col == "schedule_cron" or col.endswith("_seconds")
                or col.endswith("_ms")):
            continue          # collector never queries other column shapes
        stem = f"07-nonempty-{tbl}-{col}"
        b = bind(stem)
        required_binds.append(b)
        if b.get("missing") or "raw" not in b:
            problems_i.append(f"{stem}: query artifact MISSING")
            continue
        if b.get("rc") != 0:
            problems_i.append(f"{stem}: rc={b.get('rc')}")
            continue
        dp = latest_json(b)
        if dp is None:
            problems_i.append(f"{stem}: derived JSON missing")
            continue
        try:
            rows_i = json.loads(dp.read_text())
        except ValueError:
            problems_i.append(f"{stem}: derived JSON unparsable")
            continue
        if col == "schedule_cron":
            cron_tables[stem] = rows_i
        else:
            interval_tables[stem] = rows_i
    check("I-no-nonempty-schedule-cron",
          not problems_i and not any(v for v in cron_tables.values()),
          f"required={len(cron_tables) + len(interval_tables)} "
          f"problems={problems_i} "
          f"nonempty_cron={ {k: v for k, v in cron_tables.items() if v} } "
          f"| interval-columns(reported): {interval_tables} "
          f"| admission-interval path=sync.admission.min_interval_ms "
          f"(root fix ③; old 07b wrong-path outputs are NOT evidence)",
          binds=required_binds)

    # ---- rc gate: every bound artifact must exist with rc==0 ----------------
    rc_problems = []
    for base, b in sorted(bindings.items()):
        if b.get("missing"):
            rc_problems.append(f"{base}: no artifact")
        elif b.get("rc") != 0:
            rc_problems.append(f"{base}: rc={b.get('rc')} "
                               f"({b.get('rc_file')})")
    check("RC-gate-all-bound-rc0", not rc_problems,
          "; ".join(rc_problems) or "all bound artifacts rc=0")
    # (binds already registered per-check above; RC gate owns no new binds)

    all_ok = all(c["ok"] for c in checks)
    verdict = {"client_date": bl.get("client_date"),
               "connector_boot": boot,
               "checks": checks, "all_ok": all_ok,
               "bindings": bindings}
    vp = artifact("preflight-verdict.json")
    vp.write_text(json.dumps(verdict, indent=1, sort_keys=True) + "\n")

    # index — the ONLY rewritten file (root: "update the index")
    idx = OUT / "preflight-verdict-INDEX.json"
    entries = []
    for p in sorted(OUT.glob("preflight-verdict*.json")):
        if p.name == idx.name:
            continue
        try:
            entries.append({"file": p.name, "sha256": _sha(p),
                            "all_ok": json.loads(p.read_text())["all_ok"]})
        except Exception:
            entries.append({"file": p.name, "error": "unreadable"})
    idx.write_text(json.dumps({"newest": vp.name, "verdicts": entries},
                              indent=1) + "\n")
    print(f"[verify] all_ok={all_ok} -> {vp.name} (index updated)")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())

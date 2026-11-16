#!/usr/bin/env python3
"""Compare two readonly_baseline manifests — STRICT, mode-aware.

Modes:
  upgrade (pre->post upgrade): before = the FIXED pre-upgrade baseline
      (pre-P22 10-col fallback), after = the post-up 13-col baseline.
      Compares the PERSISTED handles only: the exact 10 fallback
      projection keys must be PRESENT on both sides and equal (a missing
      key is a FAIL, never None==None); the after side must additionally
      show the P2.2 control fields with the LEGACY values
      (control_mode=legacy, cancel_requested=f, gate_entered=f).
      Fixture counters are NEVER compared across the upgrade (expected
      reset by the one-time fixture restart).
  restart (post-restart re-read): both sides must be 13-col; ALL 13
      projection keys present on both sides and equal; targets/artifact/
      buildjob compared on their full expected key sets.

Both modes first gate on the SOURCE manifests: the relevant steps must
have ok=True and the collection all_ok=True — a half-failed collection
is never a green comparison.

Exit 0 iff every section ok; compare.json carries per-section facts.
"""

import argparse
import json
import sys

FALLBACK_KEYS = ("status", "has_holder", "heartbeat", "expires",
                 "has_token_col", "attempt", "generation",
                 "has_content_hash", "has_version", "finished_at")
FULL_KEYS = FALLBACK_KEYS[:5] + ("cancel_requested", "gate_entered",
                                 "control_mode") + FALLBACK_KEYS[5:]
LEGACY_EXPECT = {"control_mode": "legacy", "cancel_requested": "f",
                 "gate_entered": "f"}


def step(manifest, name):
    for s in manifest.get("steps", []):
        if s.get("step") == name:
            return s
    return None


def gated_step(manifest, name, problems):
    s = step(manifest, name)
    if s is None:
        problems.append(f"missing step {name}")
        return None
    if not s.get("ok"):
        problems.append(f"step {name} not ok in source collection")
    return s


def compare_projection(bdet, adet, mode, problems):
    keys = FALLBACK_KEYS if mode == "upgrade" else FULL_KEYS
    out = {"keys_expected": list(keys), "before_variant": bdet.get("variant"),
           "after_variant": adet.get("variant"), "rows": {}, "ok": True}
    if mode == "upgrade" and bdet.get("variant") != "pre-p22-fallback-10col":
        problems.append("upgrade mode needs a pre-P22 fallback before-side")
        out["ok"] = False
    if mode == "restart" and adet.get("variant") != "p22-full-13col":
        problems.append("restart mode needs 13-col sides")
        out["ok"] = False
    for label in ("B_lease_lost_unpublished", "C_committed_unconfirmed"):
        rb, ra = bdet.get(label) or {}, adet.get(label) or {}
        missing = [k for k in keys if k not in rb or k not in ra]
        diff = {k: (rb.get(k), ra.get(k)) for k in keys
                if k in rb and k in ra and rb[k] != ra[k]}
        legacy_bad = ({k: ra.get(k) for k, v in LEGACY_EXPECT.items()
                       if ra.get(k) != v} if mode == "upgrade" else {})
        row_ok = not missing and not diff and not legacy_bad
        out["rows"][label] = {"missing_keys": missing, "diff": diff,
                              "legacy_expect_mismatch": legacy_bad,
                              "ok": row_ok}
        out["ok"] = out["ok"] and row_ok
    return out


def compare_targets(b, a, problems):
    out, ok = {}, True
    for label in ("B_lease_lost_unpublished", "C_committed_unconfirmed"):
        rb, ra = b.get(label, {}), a.get(label, {})
        need = ("files_count",)
        missing = [k for k in need if k not in rb or k not in ra]
        sha_b = {k: v.get("sha256") for k, v in rb.items()
                 if k.startswith("file_") and isinstance(v, dict)}
        sha_a = {k: v.get("sha256") for k, v in ra.items()
                 if k.startswith("file_") and isinstance(v, dict)}
        txn_a = {k: v.get("txn_status") for k, v in ra.items()
                 if k.startswith("file_") and isinstance(v, dict)}
        entry = {"missing_keys": missing,
                 "files": (rb.get("files_count"), ra.get("files_count")),
                 "sha_equal": sha_b == sha_a, "sha_a": sha_a}
        row_ok = (not missing and rb.get("files_count") == ra.get("files_count")
                  and entry["sha_equal"])
        if label == "C_committed_unconfirmed":
            row_ok = row_ok and bool(sha_a) and set(txn_a.values()) == {"COMMITTED"}
            entry["txn_after"] = sorted(set(txn_a.values()))
        entry["ok"] = row_ok
        out[label] = entry
        ok = ok and row_ok
    return {"ok": ok, "rows": out}


def compare_artifact(b, a, problems):
    need = ("sha256_matches_archive",)
    missing = [k for k in need if k not in b or k not in a]
    ok = (not missing and b.get("sha256_matches_archive")
          and a.get("sha256_matches_archive")
          and a.get("txn_hash_matches_archive"))
    return {"ok": bool(ok), "missing_keys": missing,
            "before": {k: b.get(k) for k in need},
            "after": {k: a.get(k) for k in
                      ("sha256_matches_archive", "txn_hash_matches_archive")}}


def compare_buildjob(b, a):
    out, ok = {}, True
    for dom in ("org_a", "org_b", "legacy"):
        rb, ra = b.get(dom, {}), a.get(dom, {})
        # expected keys = the COMMON core both collections must carry;
        # binding fields added later are recorded as after-side extras
        need = ("build_get", "job_outputs_get")
        missing = [k for k in need if k not in rb or k not in ra]
        good = (not missing and rb.get("build_get") == 200
                and ra.get("build_get") == 200 and ra.get("job_outputs_get") == 200
                and rb.get("build_state") == ra.get("build_state"))
        out[dom] = {"missing_keys": missing, "ok": bool(good),
                    "before": rb, "after": ra}
        ok = ok and good
    return {"ok": ok, "rows": out}


def compare(before, after, mode):
    problems = []
    if not before.get("all_ok"):
        problems.append("before collection all_ok != True")
    if not after.get("all_ok"):
        problems.append("after collection all_ok != True")

    bp = gated_step(before, "p21_bc_projection", problems)
    ap = gated_step(after, "p21_bc_projection", problems)
    bt = gated_step(before, "p21_bc_targets_live", problems)
    at = gated_step(after, "p21_bc_targets_live", problems)
    ba = gated_step(before, "p1_old_artifact_live", problems)
    aa = gated_step(after, "p1_old_artifact_live", problems)
    bb = gated_step(before, "p1_old_buildjob_live", problems)
    ab = gated_step(after, "p1_old_buildjob_live", problems)

    out = {"mode": mode, "source_gates": problems, "ok": not problems}
    if bp and ap:
        out["p21_bc_projection"] = compare_projection(bp["detail"],
                                                      ap["detail"], mode,
                                                      problems)
        out["ok"] = out["ok"] and out["p21_bc_projection"]["ok"]
    if bt and at:
        out["p21_bc_targets"] = compare_targets(bt["detail"], at["detail"],
                                                problems)
        out["ok"] = out["ok"] and out["p21_bc_targets"]["ok"]
    if ba and aa:
        out["p1_old_artifact"] = compare_artifact(ba["detail"], aa["detail"],
                                                  problems)
        out["ok"] = out["ok"] and out["p1_old_artifact"]["ok"]
    if bb and ab:
        out["p1_old_buildjob"] = compare_buildjob(bb["detail"], ab["detail"])
        out["ok"] = out["ok"] and out["p1_old_buildjob"]["ok"]
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--before", required=True)
    parser.add_argument("--after", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--mode", choices=("restart", "upgrade"),
                        default="restart")
    args = parser.parse_args()
    before = json.load(open(args.before))
    after = json.load(open(args.after))
    result = compare(before, after, args.mode)
    with open(args.out, "w") as fh:
        json.dump(result, fh, indent=2)
        fh.write("\n")
    print(f"[compare:{args.mode}] ok =", result["ok"])
    if result["source_gates"]:
        print("  source gates:", result["source_gates"])
    for key, sec in result.items():
        if isinstance(sec, dict) and "ok" in sec:
            print(f"  {key}: ok={sec['ok']}")
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

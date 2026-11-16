#!/usr/bin/env python3
"""P24 OWNED-HISTORY manifest — schema p24-owned-history-v1 (root).

Union of owned Sync defs across ALL real 10c-p24-scenarios attempts
(failed AND green), so the final global-nonempty-cron/persist roster
can never let an earlier attempt's defs fall out of scope as
"unknown". Safe public bindings only — NEVER tokens.

Schema (consumed by PG collectors via optional --owned-history and by
the tools finalizer/quiesce):
  {
    "schema": "p24-owned-history-v1",
    "attempts": [ {"result_path": "<ABSOLUTE ARCHIVED step result>",
                   "result_sha256": "<sha256>", "suffix": "<sfx>",
                   "step_rc": <int>, "official": <bool|null>,
                   "cleanup_ok": <bool|null>} , ... ],
    "owned_sync_ids": { "<uuid>": {"tag": …, "source_id": …,
                                   "target_id": …, "fixture_src": …,
                                   "suffix": …}, ... },
    "history_sha256": "<sha256 of this file's canonical bytes>",  # see note
    "all_attempts_valid": <bool>, "conflicts": [...]
  }

Rules (fail-closed):
  * result_path is the ABSOLUTE ARCHIVED copy inside the attempt's
    step dir (the /private/tmp RESULT_FILE is only a locator);
  * every attempt result must exist + hash-match its recorded sha;
  * a duplicate UUID with DIFFERENT bindings across attempts is a
    CONFLICT → exit 1 (never silently merged);
  * failed attempts are preserved as-is (their defs must later stay
    disabled/NULL/grant-gone/terminal with cron intact — masking by
    deletion/clearing is detectable via this union);
  * output path is fresh + collision-safe (-rN), printed as
    OWNED_HISTORY=<abs> for attempt-bound consumption.

Usage: python3 -I owned_history_p24.py --ev <EV-dir> [--out <path>]
"""

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path


def sha(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ev", required=True)
    ap.add_argument("--out")
    args = ap.parse_args()
    ev = Path(args.ev).resolve()

    steps = []
    for ln in open(ev / "manifest.jsonl"):
        ln = ln.strip()
        if not ln:
            continue
        try:
            steps.append(json.loads(ln))
        except ValueError as exc:
            print(f"[owned-history] BLOCK: unparsable manifest line: "
                  f"{ln[:60]!r} ({exc})", file=sys.stderr)
            return 1

    attempts, union, conflicts, ok_all = [], {}, [], True
    for idx, s in enumerate(steps):
        if s.get("step") != "10c-p24-scenarios":
            continue
        sdir = (ev / s.get("dir", "").strip("/")) if s.get("dir") else None
        archived = None
        if sdir and sdir.is_dir():
            hits = sorted(sdir.glob("result_p24-*.json"))
            if hits:
                archived = hits[-1].resolve()
        entry = {"step_index": idx, "step_rc": s.get("rc"),
                 "result_path": str(archived) if archived else None,
                 "result_sha256": None, "suffix": None,
                 "official": None, "cleanup_ok": None, "valid": False}
        if archived and archived.is_file():
            try:
                res = json.loads(archived.read_text())
                entry.update({
                    "result_sha256": sha(archived),
                    "suffix": res.get("suffix"),
                    "official": res.get("official"),
                    "cleanup_ok": (res.get("cleanup") or {}).get("ok"),
                    "valid": True})
                for oid, bind in (res.get("owned_sync_ids")
                                  or {}).items():
                    want = {"tag": bind.get("tag"),
                            "source_id": bind.get("source_id"),
                            "target_id": bind.get("target_id"),
                            "fixture_src": bind.get("fixture_src"),
                            "suffix": res.get("suffix")}
                    if oid in union and union[oid] != want:
                        conflicts.append(
                            {"uuid": oid, "existing": union[oid],
                             "attempt_suffix": res.get("suffix"),
                             "new": want})
                        continue
                    union[oid] = want
            except (ValueError, OSError) as exc:
                entry["error"] = str(exc)[:150]
        else:
            entry["error"] = "archived attempt result missing"
        ok_all = ok_all and entry["valid"]
        attempts.append(entry)

    out = Path(args.out).resolve() if args.out else ev / (
        f"owned-history-{time.strftime('%Y%m%dT%H%M%S')}.json")
    n = 0
    while out.exists():
        n += 1
        out = out.with_name(out.name.replace(
            ".json", f"-r{n}.json"))
    manifest = {"schema": "p24-owned-history-v1",
                "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
                "attempts": attempts,
                "owned_sync_ids": union,
                "union_count": len(union),
                "conflicts": conflicts,
                "all_attempts_valid": ok_all and not conflicts,
                "policy": "union across ALL 10c attempts (failed and "
                          "green); duplicate-UUID binding conflicts are "
                          "rejected; failed defs stay disabled/NULL/"
                          "grant-gone/terminal with cron intact; no "
                          "tokens ever"}
    out.write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    print(f"OWNED_HISTORY={out}")
    print(f"[owned-history] attempts={len(attempts)} "
          f"valid={sum(1 for a in attempts if a['valid'])} "
          f"union_owned={len(union)} conflicts={len(conflicts)} "
          f"ok={manifest['all_attempts_valid']}")
    for c in conflicts[:3]:
        print(f"[owned-history] CONFLICT {c['uuid']}", file=sys.stderr)
    return 0 if manifest["all_attempts_valid"] else 1


if __name__ == "__main__":
    sys.exit(main())

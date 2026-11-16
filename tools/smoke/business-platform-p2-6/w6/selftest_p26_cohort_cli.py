#!/usr/bin/env python3
"""PURE self-test for cohort/p26_cohort_cli.py on SYNTHETIC fixtures.

Builds a miniature sealed final dir (hash-manifest with REAL sha256s of
synthetic evidence files) inside results/w6, then checks derive-protected
positive + the negative contracts (bad SHA / wrong composition / unknown
row / partial schema / tampered file / frozen-tree output refusal).
The REAL P25 seal is never opened here.
"""

import json
import shutil
import sys
import tempfile
import uuid
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402
from cohort import p26_cohort_cli as C  # noqa: E402

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(f"[cohort] {'PASS' if cond else 'FAIL'} {name}"
          + (f" — {detail}" if detail and not cond else ""))


def uid():
    return str(uuid.uuid4())


ORG = uid()
OWNER = uid()


def mk_row(fixture_src, run_id=None, def_id=None, src_id=None, ds_id=None):
    return {"id": run_id or uid(), "sync_def_id": def_id or uid(),
            "source_id": src_id or uid(),
            "output_dataset_id": ds_id or uid(),
            "source_name": "p21-x", "fixture_src": fixture_src,
            "owner_id": OWNER, "run_owner_id": OWNER,
            "requested_by": OWNER, "conn_org_id": ORG, "def_org_id": ORG,
            "run_org_id": ORG, "org_id": ORG, "status": "running",
            "control_mode": "controlled", "has_holder": "true",
            "has_token": "true", "started_at": "2026-11-14 17:00:00+00",
            "heartbeat_at": "2026-11-14 17:00:01+00",
            "lease_expires_at": "2026-11-14 17:00:02+00",
            "cancel_requested": "false", "gate_entered": "false",
            "attempt": "1", "generation": "1", "has_hash": "false",
            "has_version": "false", "finished_at": ""}


def build_fixture(base: Path, mutate=None):
    """Mini sealed final dir; returns (dir, seal_sha, files_count)."""
    d = base / "final"
    d.mkdir(parents=True)
    h10_run, abc_b, abc_c = uid(), uid(), uid()
    abc_suffix = "cd8cdca7"
    legacy_hex = "1020a565"
    rows = []
    for hexpart in ("96351e3b", "528455c2", legacy_hex, abc_suffix):
        for side in ("b", "c"):
            if hexpart == legacy_hex and side == "b":
                rid = C.LEGACY_B_ID
            elif hexpart == legacy_hex and side == "c":
                rid = C.LEGACY_C_ID
            elif hexpart == abc_suffix and side == "b":
                rid = abc_b
            elif hexpart == abc_suffix and side == "c":
                rid = abc_c
            else:
                rid = uid()
            rows.append(mk_row(f"p21{side}-{hexpart}", run_id=rid))
    h10 = mk_row("p25h13", run_id=h10_run)
    rows.append(h10)

    roster = []
    for tag in ("h11-no_txn_200", "h11-not_committed", "h11-wrong_target",
                "h11-http_500", "h14-drop"):
        roster.append({"def_id": uid(), "source_id": uid(), "run_id": uid(),
                       "kind": "owner", "mode": None, "tag": tag,
                       "run_ref": "terminal-failed",
                       "blocked_reason": "requires_reconciliation",
                       "origin": "intentional-tail"})
    for _ in range(2):
        roster.append({"def_id": uid(), "source_id": uid(), "run_id": uid(),
                       "kind": "owner", "mode": None,
                       "tag": "failedhealthy-h13-" + uid()[:8],
                       "run_ref": "terminal-failed",
                       "blocked_reason": "requires_reconciliation",
                       "origin": "failedhealthy"})
    roster.append({"def_id": h10["sync_def_id"],
                   "source_id": h10["source_id"], "run_id": h10_run,
                   "kind": "owner", "mode": None, "tag": "h10-held",
                   "run_ref": "same",
                   "blocked_reason": "requires_reconciliation",
                   "origin": "h10-prepublish-crash"})

    owners = [{"sync_def_id": e["def_id"], "source_id": e["source_id"],
               "output_dataset_id": uid(), "mode": "cursor",
               "confirmed_epoch": "0", "run_id": e["run_id"],
               "active_since": "2026-11-14 17:36:23+00",
               "run_status": "failed",
               "blocked_reason": "requires_reconciliation"}
              for e in roster]

    abc = {"suffix": abc_suffix,
           "results": {"heartbeat_progress": {"status": "pass"},
                       "lease_loss_fencing": {"status": "pass"},
                       "terminal_commit_fault": {"status": "pass"}},
           "evidence": {"terminal_commit_fault": {
               "txn_status": "COMMITTED",
               "logical_path": f"sync-{abc_c}.ndjson"}}}
    before = {h10["sync_def_id"]: {"checkpoint": {"mode": "cursor",
                                                  "confirmed_epoch": 2},
                                   "runs": [{"id": h10_run}]}}
    for e in roster:
        ck_owner = uid()
        before[e["def_id"]] = {
            "checkpoint": {"initialized": True, "mode": "cursor",
                           "confirmed_epoch": 0, "owner": ck_owner},
            "runs": [{"id": e["run_id"], "owner_id": ck_owner,
                      "status": "failed"}]}

    files = {
        C.CONTROLLED_FILES["rows"]: "SET\nSET\n" + json.dumps(rows) + "\n",
        C.CONTROLLED_FILES["owners"]: "SET\nSET\n" + json.dumps(owners)
        + "\n",
        C.ROSTER_FILE: json.dumps(roster),
        C.ABC_FILE: json.dumps(abc),
        C.QUIESCE_BEFORE: json.dumps(before),
        "extra.txt": "synthetic sealed filler\n",
    }
    if mutate:
        mutate(files)
    manifest = {"client_date": "2026-11-15", "files": {}}
    for rel, text in files.items():
        p = d / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)
        manifest["files"][rel] = G.sha256_bytes(text.encode())
    (d / "hash-manifest.json").write_text(json.dumps(manifest))
    seal = G.sha256_file(d / "hash-manifest.json")
    return d, seal, len(manifest["files"])


def run_cli(td, *args):
    out = Path(td) / "out.json"
    rc = C.main(["--out", str(out), "--allow-alternate-dir", *args])
    payload = json.loads(out.read_text()) if out.exists() else {}
    return rc, payload


def main():
    w6dir = Path(__file__).resolve().parents[1] / "results" / "w6"
    w6dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=str(w6dir)) as td:
        td = Path(td)
        d, seal, count = build_fixture(td)

        # positive: derive
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d),
                        "--seal-sha", seal, "--files-count", str(count))
        check("derive-ok", rc == 0 and p.get("ok") is True)
        check("derive-protected9",
              len(p.get("protected9", [])) == 9)
        origins = {}
        for r in p.get("protected9", []):
            origins[r["origin"]] = origins.get(r["origin"], 0) + 1
        check("derive-composition-9",
              origins == {"original6": 6, "h10-held": 1, "abc-final": 2},
              str(origins))
        check("derive-cp8", len(p.get("cp8", [])) == 8)
        kinds = {}
        for e in p.get("cp8", []):
            kinds[e["origin"]] = kinds.get(e["origin"], 0) + 1
        check("derive-composition-8",
              kinds == {"intentional-tail": 5, "failedhealthy": 2,
                        "h10-prepublish-crash": 1}, str(kinds))
        check("derive-cp-originals-full",
              all(e["cp_owner_row_full"]["mode"] == "cursor" and
                  e["cp_owner_row_full"]["blocked_reason"] ==
                  "requires_reconciliation" and
                  {"initialized", "mode", "confirmed_epoch", "owner"} <=
                  set(e["quiesce_checkpoint_full"]) and
                  e["matched_public_run"]["id"] == e["run_id"]
                  for e in p.get("cp8", [])))
        check("derive-27col-projection",
              all(len(r["public_projection_27"]) == 27
                  for r in p.get("protected9", [])))
        qc = p.get("quiesce_before_chain", [])
        check("derive-quiesce-intersection-facts",
              len(qc) == 9 and
              sum(1 for x in qc if x.get("in_quiesce_before")) == 1)
        roster_out = td / "roster.json"
        roster_out.write_text(json.dumps(p))

        # verify-seal positive + limit
        rc, p = run_cli(td, "verify-seal", "--final-dir", str(d),
                        "--seal-sha", seal, "--files-count", str(count))
        check("verify-seal-ok", rc == 0 and p.get("ok") and
              p.get("seal_ok"))
        rc, p = run_cli(td, "verify-seal", "--final-dir", str(d),
                        "--seal-sha", seal, "--files-count", str(count),
                        "--limit", "2")
        check("verify-seal-partial-never-ok",
              rc == 1 and p.get("partial") is True and
              p.get("files_checked") == 2 and
              any("PARTIAL SAMPLE" in q for q in p.get("problems", [])))
        rc, p = run_cli(td, "verify-seal", "--final-dir", str(d),
                        "--seal-sha", seal, "--files-count", str(count))
        check("verify-seal-full-ok", rc == 0 and p.get("ok") and
              p.get("partial") is False)
        rc, p = run_cli(td, "verify-seal", "--final-dir", str(d),
                        "--seal-sha", seal, "--files-count", str(count),
                        "--limit", "0")
        check("verify-seal-limit0-rejected", rc == 1)

        # negative: bad seal sha
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d),
                        "--seal-sha", "f" * 64, "--files-count", str(count))
        check("derive-bad-seal-refused", rc == 1 and
              "seal" in p.get("refused", ""))

        # negative: wrong files count
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d),
                        "--seal-sha", seal, "--files-count", "9999")
        check("derive-bad-count-refused", rc == 1)

        # negative: tampered sealed file after manifest (bad SHA)
        (d / "extra.txt").write_text("tampered\n")
        rc, p = run_cli(td, "verify-seal", "--final-dir", str(d),
                        "--seal-sha", seal, "--files-count", str(count))
        check("tamper-detected", rc == 1 and p.get("problems"))
        (d / "extra.txt").write_text("synthetic sealed filler\n")

        # negative: roster wrong composition (count-only cheat must fail)
        d2, seal2, count2 = build_fixture(
            td / "v2", mutate=lambda f: f.__setitem__(
                C.ROSTER_FILE, json.dumps(
                    json.loads(f[C.ROSTER_FILE])[:7])))
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d2),
                        "--seal-sha", seal2, "--files-count", str(count2))
        check("roster-7-entries-refused", rc == 1)

        d3, seal3, count3 = build_fixture(
            td / "v3", mutate=lambda f: f.__setitem__(
                C.ROSTER_FILE, json.dumps(
                    [{"def_id": uid(), "source_id": uid(), "run_id": uid(),
                      "kind": "owner", "mode": None, "tag":
                      "failedhealthy-h13-" + uid()[:8],
                      "run_ref": "terminal-failed",
                      "blocked_reason": "x", "origin":
                      "unknown-origin"}] +
                    json.loads(f[C.ROSTER_FILE])[:7])))
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d3),
                        "--seal-sha", seal3, "--files-count", str(count3))
        check("roster-unknown-origin-refused",
              rc == 1 and "unknown origin" in p.get("refused", ""))

        # negative: controlled rows with unknown fixture_src (count stays 9
        # so the UNKNOWN-row guard itself is what must fire)
        def unknown_row(files):
            rows = json.loads(files[C.CONTROLLED_FILES["rows"]]
                              .split("\n", 2)[2].rsplit("\n", 1)[0])
            rows[0]["fixture_src"] = "p99-mystery"
            files[C.CONTROLLED_FILES["rows"]] = ("SET\nSET\n" +
                                                 json.dumps(rows) + "\n")
        d4, seal4, count4 = build_fixture(td / "v4", mutate=unknown_row)
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d4),
                        "--seal-sha", seal4, "--files-count", str(count4))
        check("unknown-fixture-row-refused",
              rc == 1 and "unknown fixture_src" in p.get("refused", ""))

        # negative: partial 26-col row (schema violation)
        def partial_row(files):
            rows = json.loads(files[C.CONTROLLED_FILES["rows"]]
                              .split("\n", 2)[2].rsplit("\n", 1)[0])
            rows[0].pop("finished_at")
            files[C.CONTROLLED_FILES["rows"]] = ("SET\nSET\n" +
                                                 json.dumps(rows) + "\n")
        d5, seal5, count5 = build_fixture(td / "v5", mutate=partial_row)
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d5),
                        "--seal-sha", seal5, "--files-count", str(count5))
        check("partial-27col-row-refused",
              rc == 1 and "schema violation" in p.get("refused", ""))

        # negative: CP owners with unknown extra run
        def extra_owner(files):
            owners = json.loads(files[C.CONTROLLED_FILES["owners"]]
                                .split("\n", 2)[2].rsplit("\n", 1)[0])
            owners.append({"sync_def_id": uid(), "source_id": uid(),
                           "output_dataset_id": uid(), "mode": "cursor",
                           "confirmed_epoch": "0", "run_id": uid(),
                           "active_since": "", "run_status": "running",
                           "blocked_reason": "requires_reconciliation"})
            files[C.CONTROLLED_FILES["owners"]] = ("SET\nSET\n" +
                                                   json.dumps(owners)
                                                   + "\n")
        d6, seal6, count6 = build_fixture(td / "v6", mutate=extra_owner)
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d6),
                        "--seal-sha", seal6, "--files-count", str(count6))
        check("cp-owners-unknown-run-refused",
              rc == 1 and "run set != roster-8" in p.get("refused", ""))

        # negative: legacy B/C ids missing
        def drop_legacy(files):
            rows = json.loads(files[C.CONTROLLED_FILES["rows"]]
                              .split("\n", 2)[2].rsplit("\n", 1)[0])
            for r in rows:
                if r["id"] in (C.LEGACY_B_ID, C.LEGACY_C_ID):
                    r["id"] = uid()
            files[C.CONTROLLED_FILES["rows"]] = ("SET\nSET\n" +
                                                 json.dumps(rows) + "\n")
        d7, seal7, count7 = build_fixture(td / "v7", mutate=drop_legacy)
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d7),
                        "--seal-sha", seal7, "--files-count", str(count7))
        check("legacy-bc-missing-refused", rc == 1)

        # negative: h10 def present in before.json but run missing
        def h10_run_missing(files):
            b = json.loads(files[C.QUIESCE_BEFORE])
            for v in b.values():
                v["runs"] = [{"id": uid()}]
            files[C.QUIESCE_BEFORE] = json.dumps(b)
        d8, seal8, count8 = build_fixture(td / "v8", mutate=h10_run_missing)
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d8),
                        "--seal-sha", seal8, "--files-count", str(count8))
        check("h10-quiesce-chain-broken-refused",
              rc == 1 and ("parent chain broken" in p.get("refused", "") or
                           "not present under its def" in
                           p.get("refused", "")))

        # negative: output inside frozen P25 tree refused by CLI
        rc = C.main(["--out", str(G.ROOT_P25 / "x" / "y.json"),
                     "verify-seal", "--final-dir", str(d),
                     "--seal-sha", seal, "--files-count", str(count)])
        check("out-in-p25-refused", rc == 2)

        # negative: EXTRA disk file not in manifest (appendix)
        (d / "stray-extra.txt").write_text("extra\n")
        rc, p = run_cli(td, "verify-seal", "--final-dir", str(d),
                        "--seal-sha", seal, "--files-count", str(count))
        check("verify-seal-extra-disk-file-refused",
              rc == 1 and any("EXTRA files" in q
                              for q in p.get("problems", [])))
        (d / "stray-extra.txt").unlink()

        # negative: traversal member in manifest
        def traversal_manifest(files):
            files["../evil.txt"] = "0" * 64
        d9, seal9, count9 = build_fixture(td / "v9",
                                          mutate=traversal_manifest)
        rc, p = run_cli(td, "verify-seal", "--final-dir", str(d9),
                        "--seal-sha", seal9, "--files-count", str(count9))
        check("verify-seal-traversal-member-refused",
              rc == 1 and any("unsafe member path" in q
                              for q in p.get("problems", [])))

        # negative: symlink member
        d10, seal10, count10 = build_fixture(td / "v10")
        (d10 / "link-extra.txt").symlink_to("/etc/hosts")
        rc, p = run_cli(td, "verify-seal", "--final-dir", str(d10),
                        "--seal-sha", seal10, "--files-count", str(count10))
        check("verify-seal-symlink-refused",
              rc == 1 and any("EXTRA files" in q or "symlink" in q
                              for q in p.get("problems", [])))

        # negative: extra cp-owner whose run is one of protected9
        def owner_from_protected(files):
            owners = json.loads(files[C.CONTROLLED_FILES["owners"]]
                                .split("\n", 2)[2].rsplit("\n", 1)[0])
            rows = json.loads(files[C.CONTROLLED_FILES["rows"]]
                              .split("\n", 2)[2].rsplit("\n", 1)[0])
            owners.append({"sync_def_id": rows[0]["sync_def_id"],
                           "source_id": rows[0]["source_id"],
                           "output_dataset_id": rows[0]["output_dataset_id"],
                           "mode": "cursor", "confirmed_epoch": "0",
                           "run_id": rows[0]["id"], "active_since": "",
                           "run_status": "running",
                           "blocked_reason": "requires_reconciliation"})
            files[C.CONTROLLED_FILES["owners"]] = ("SET\nSET\n" +
                                                   json.dumps(owners)
                                                   + "\n")
        d11, seal11, count11 = build_fixture(td / "v11",
                                             mutate=owner_from_protected)
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d11),
                        "--seal-sha", seal11, "--files-count", str(count11))
        check("cp-owners-protected9-extra-refused",
              rc == 1 and "run set != roster-8" in p.get("refused", ""))

        # negative: roster with duplicate def (8 unique runs, 7 defs)
        def dup_def_roster(files):
            r = json.loads(files[C.ROSTER_FILE])
            r[1]["def_id"] = r[0]["def_id"]
            files[C.ROSTER_FILE] = json.dumps(r)
        d12, seal12, count12 = build_fixture(td / "v12",
                                             mutate=dup_def_roster)
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d12),
                        "--seal-sha", seal12, "--files-count", str(count12))
        check("roster-duplicate-def-refused", rc == 1 and
              "unique def" in p.get("refused", ""))

        # negative: roster def missing from before.json
        def drop_before_def(files):
            b = json.loads(files[C.QUIESCE_BEFORE])
            r = json.loads(files[C.ROSTER_FILE])
            b.pop(r[0]["def_id"])
            files[C.QUIESCE_BEFORE] = json.dumps(b)
        d13, seal13, count13 = build_fixture(td / "v13",
                                             mutate=drop_before_def)
        rc, p = run_cli(td, "derive-protected", "--final-dir", str(d13),
                        "--seal-sha", seal13, "--files-count", str(count13))
        check("cp8-before-def-missing-refused", rc == 1 and
              "before.json" in p.get("refused", ""))

        # export-baseline on synthetic bundle
        bdir = td / "bundle"
        (bdir / "dependencies" / "evidence" / "pg").mkdir(parents=True)
        (bdir / "dependencies" / "repo" / "svc").mkdir(parents=True)
        a1 = G.EXPECTED_P25_A1_SOURCE_SHA
        for ph, stamp in (("new-normal", "20261114T141338Z"),
                          ("new-race", "20261114T141638Z"),
                          ("unified", "20261114T141737Z")):
            (bdir / "dependencies" / "evidence" / "pg" /
             f"{ph}_{stamp}.meta").write_text(
                f"phase={ph}\nsource_content_sha256_before={a1}\n"
                f"source_content_sha256_after={a1}\n")
        # pre-acceptance compile attempt with a DIFFERENT hash: historical
        # fact, must not break the final-pin derivation
        (bdir / "dependencies" / "evidence" / "pg" /
         "compile-normal_20261114T122224Z.meta").write_text(
            "phase=compile-normal\n"
            "source_content_sha256_before=%s\n"
            "source_content_sha256_after=%s\n" % ("d" * 64, "d" * 64))
        (bdir / "dependencies" / "repo" / "svc" / "a.go").write_text(
            "package svc\n")
        bfiles = {}
        for pth in bdir.rglob("*"):
            if pth.is_file():
                rel = str(pth.relative_to(bdir))
                bfiles[rel] = G.sha256_file(pth)
        bm = {"client_date": "2026-11-15", "files": bfiles}
        (bdir / "bundle-hash-manifest.json").write_text(json.dumps(bm))
        bm_sha = G.sha256_file(bdir / "bundle-hash-manifest.json")
        bo = td / "baseline-out"
        rc, p = run_cli(td, "export-baseline", "--bundle-dir", str(bdir),
                        "--bundle-sha", bm_sha,
                        "--baseline-out-dir", str(bo))
        check("baseline-export-ok", rc == 0 and p.get("ok")
              and p.get("exported_files") == 1)
        check("baseline-file-copied",
              (bo / "svc" / "a.go").read_text() == "package svc\n")
        check("baseline-manifest-written",
              (bo / "BASELINE-EXPORT-MANIFEST.json").is_file())
        rc, p = run_cli(td, "export-baseline", "--bundle-dir", str(bdir),
                        "--bundle-sha", "0" * 64,
                        "--baseline-out-dir", str(td / "bo2"))
        check("baseline-bad-bundle-sha-refused", rc == 1)
        # pre-existing target overwrite refused
        bo3 = td / "bo3"
        (bo3 / "svc").mkdir(parents=True)
        (bo3 / "svc" / "a.go").write_text("OLD CONTENT\n")
        rc, p = run_cli(td, "export-baseline", "--bundle-dir", str(bdir),
                        "--bundle-sha", bm_sha,
                        "--baseline-out-dir", str(bo3))
        check("baseline-preexisting-target-refused",
              rc == 1 and "pre-existing target" in p.get("refused", ""))
        # traversal bundle member refused
        bdir_t = td / "bundle-t"
        (bdir_t / "dependencies" / "repo").mkdir(parents=True)
        bad_files = {"dependencies/repo/../escape.txt": "0" * 64}
        (bdir_t / "bundle-hash-manifest.json").write_text(
            json.dumps({"client_date": "x", "files": bad_files}))
        bm_sha_t = G.sha256_file(bdir_t / "bundle-hash-manifest.json")
        rc, p = run_cli(td, "export-baseline", "--bundle-dir", str(bdir_t),
                        "--bundle-sha", bm_sha_t,
                        "--baseline-out-dir", str(td / "bo4"))
        check("baseline-traversal-member-refused",
              rc == 1 and "unsafe" in p.get("refused", ""))
        rc = C.main(["--out", str(td / "o3.json"), "export-baseline",
                     "--bundle-dir", str(bdir), "--bundle-sha", bm_sha,
                     "--baseline-out-dir",
                     str(G.ROOT_P25 / "nope")])
        refused_txt = ""
        o3 = td / "o3.json"
        if o3.exists():
            refused_txt = json.loads(o3.read_text()).get("refused", "")
        check("baseline-out-in-p25-refused",
              rc == 1 and "frozen P25" in refused_txt)

        # plan-quiet-checks: removed P25 fixture URL can never be quiet
        spec = td / "quiet.json"
        spec.write_text(json.dumps({
            "docker_ps_lines": [
                "17078abc\topenfoundry-platform-p1-q3v9-connector-1\t"
                "img\tUp 9 hours",
                "17090abc\topenfoundry-platform-p1-q3v9-fixture-1\t"
                "python:3.12-alpine\tUp 31 hours (healthy)"],
            "expected_base_containers": [
                "openfoundry-platform-p1-q3v9-connector-1",
                "openfoundry-platform-p1-q3v9-dataset-1",
                "openfoundry-platform-p1-q3v9-pipeline-1"],
            "expected_fixture": "openfoundry-platform-p1-q3v9-fixture-1",
            "removed_p25_url_patterns": [":18101", "p25-fixture"],
            "candidate_urls": ["http://127.0.0.1:18101/stats",
                               "http://127.0.0.1:18200/health"]}))
        rc = C.main(["plan-quiet-checks", "--final-dir", str(spec),
                     "--out", str(td / "quiet-out.json")])
        q = json.loads((td / "quiet-out.json").read_text())
        check("quiet-plan-ok", rc == 0)
        check("quiet-removed-never-green",
              q["url_verdicts"]["http://127.0.0.1:18101/stats"] ==
              "removed_not_quiet(NEVER green)")
        check("quiet-unknown-not-green",
              q["url_verdicts"]["http://127.0.0.1:18200/health"].startswith(
                  "unknown_not_quiet"))
        check("quiet-missing-base-reported",
              q["missing_expected_base"] == [
                  "openfoundry-platform-p1-q3v9-dataset-1",
                  "openfoundry-platform-p1-q3v9-pipeline-1"])
        check("quiet-fixture-present", q["fixture_present"] is True)

        # plan-oldpg-readonly lists verbatim SQL argv, executes nothing
        rc = C.main(["plan-oldpg-readonly", "--out",
                     str(td / "sql-plan.json")])
        sp = json.loads((td / "sql-plan.json").read_text())
        check("oldpg-plan-listed", rc == 0 and
              set(sp["argv_by_check"]) == {"controlled_rows_27col",
                                           "cp_owners"})
        check("oldpg-plan-not-executed-note",
              any("NOT executed" in c for c in sp["constraints"]))

    npass = sum(1 for _, ok, _ in RESULTS if ok)
    print(f"[cohort] {npass}/{len(RESULTS)} PASS")
    return 0 if npass == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())

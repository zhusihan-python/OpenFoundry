#!/usr/bin/env python3
"""PURE self-tests for the unified acceptance tooling (round: roster /
build executor / switch guard). Synthetic fixtures + fake runners only;
no docker/HTTP/build execution."""

import json
import os
import sys
import tempfile
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402
from pg import p26_unified_roster as UR  # noqa: E402
from build import p26_build_executor as BE  # noqa: E402
from preflight import p26_switch_guard as SG  # noqa: E402

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(f"[accept] {'PASS' if cond else 'FAIL'} {name}"
          + (f" — {detail}" if detail and not cond else ""))


def j(a):
    return " ".join(str(x) for x in a)


class Fake:
    def __init__(self, h):
        self.calls = []
        self.h = h

    def __call__(self, argv, timeout_s=None):
        argv = [str(x) for x in argv]
        self.calls.append(argv)
        return self.h(argv)


def main():
    w6 = Path(__file__).resolve().parents[1] / "results" / "w6"
    w6.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=str(w6)) as td:
        td = Path(td)

        # ---------- roster: synthetic sources + sealed raw --------------
        root = td / "src"
        for rel in UR.SOURCES.values():
            p = root / rel
            p.parent.mkdir(parents=True, exist_ok=True)
        (root / UR.SOURCES["p1_run_tests"]).write_text(
            'REQUIRED_DATASET="TestD1 TestD2"\n'
            'REQUIRED_PIPELINE="TestP1"\n'
            'REQUIRED_CONNECTOR="TestC1 TestC2"\n')
        (root / UR.SOURCES["p21_run_tests"]).write_text(
            'REQUIRED_LEASE="TestL1"\n')
        n22, n23, n24 = 4, 4, 4
        (root / UR.SOURCES["p22_names"]).write_text(
            "\n".join(f"Test22{i}" for i in range(n22)) + "\n")
        (root / UR.SOURCES["p23_names"]).write_text(
            "\n".join(f"Test23{i}" for i in range(n23)) + "\n")
        (root / UR.SOURCES["p24_names"]).write_text(
            "\n".join(f"Test24{i}" for i in range(n24)) + "\n")
        # 87 total = 5(d/p/c)+1(lease)+12(22/23/24) → need 69 from p25
        p25n = 87 - (5 + 1 + 12)
        (root / UR.SOURCES["p25_names"]).write_text(
            "\n".join(f"Test25{i:02d}" for i in range(p25n)) + "\n")
        frozen_names = ([f"TestD{i}" for i in (1, 2)] + ["TestP1"] +
                        ["TestC1", "TestC2"] + ["TestL1"] +
                        [f"Test22{i}" for i in range(4)] +
                        [f"Test23{i}" for i in range(4)] +
                        [f"Test24{i}" for i in range(4)] +
                        [f"Test25{i:02d}" for i in range(p25n)])
        assert len(frozen_names) == 87
        raw_lines = []
        for n in frozen_names:
            raw_lines += [f"=== RUN   {n}", f"--- PASS: {n} (0.1s)"]
        raw_lines.append("ok  \tpkg\t1s")
        raw = "\n".join(raw_lines) + "\n"
        bdir = td / "bundle"
        bdir.mkdir()
        (bdir / "unified.raw").write_text(raw)
        (bdir / "bundle-hash-manifest.json").write_text(json.dumps(
            {"files": {"unified.raw": G.sha256_bytes(raw.encode())}}))
        # current cohorts: point names files at the REAL frozen files but
        # stub source scan to absent-safe: roster requires presence —
        # monkeypatch scan to a tiny index containing our 25 names
        (root / "services/x").mkdir(parents=True, exist_ok=True)
        (root / "services/connector-management-service/internal/repo"
         ).mkdir(parents=True, exist_ok=True)
        (root / "services/dataset-versioning-service/internal/repo"
         ).mkdir(parents=True, exist_ok=True)
        (root / "services/pipeline-build-service/internal/postgres"
         ).mkdir(parents=True, exist_ok=True)
        cur_names = ["TestCurD1", "TestCurC1"]
        (td / "cur-ds.txt").write_text("\n".join(cur_names[:1]) + "\n")
        (td / "cur-cn.txt").write_text("\n".join(cur_names[1:]) + "\n")
        idx = {n: [str(root / "services/x/y_test.go")]
               for n in frozen_names + cur_names}
        orig_scan = UR.scan_source_functions
        UR.scan_source_functions = lambda *a, **k: idx
        orig_src = UR.SRC
        try:
            UR.SRC = root
            out = td / "roster.json"
            rc = UR.main(["emit", "--out", str(out),
                          "--sources-root", str(root),
                          "--sealed-raw", str(bdir / "unified.raw"),
                          "--sealed-manifest",
                          str(bdir / "bundle-hash-manifest.json"),
                          "--dataset-names", str(td / "cur-ds.txt"),
                          "--connector-names", str(td / "cur-cn.txt")])
            r = json.loads(out.read_text())
            check("roster-frozen-87-pername",
                  rc == 0 and len(r["roster"]["p25_frozen_87"]) == 87 and
                  len(r["roster"]["dataset_current"]) == 1 and
                  len(r["roster"]["connector_current"]) == 1 and
                  all(e["sealed_green_1_1_0_0"] for e in
                      r["roster"]["p25_frozen_87"]))
            check("roster-frozen-name-listing",
                  {e["name"] for e in r["roster"]["p25_frozen_87"]} ==
                  set(frozen_names))
            def neg_emit(tag, **kw):
                try:
                    rc2 = UR.main(["emit", "--out", str(td / tag),
                                   "--sources-root", str(root),
                                   "--sealed-raw",
                                   str(bdir / "unified.raw"),
                                   "--sealed-manifest",
                                   str(bdir / "bundle-hash-manifest.json"),
                                   "--dataset-names", str(td / "cur-ds.txt"),
                                   "--connector-names",
                                   str(td / "cur-cn.txt")])
                except SystemExit as e2:
                    rc2 = e2.code if isinstance(e2.code, int) else 1
                return rc2

            (bdir / "unified.raw").write_text(raw + "EXTRA\n")
            check("roster-tampered-sealed-refused",
                  neg_emit("r2.json") != 0)
            (bdir / "unified.raw").write_text(raw)
            bad = raw.replace("--- PASS: TestD1 (0.1s)", "")
            (bdir / "unified.raw").write_text(bad)
            (bdir / "bundle-hash-manifest.json").write_text(json.dumps(
                {"files": {"unified.raw":
                           G.sha256_bytes(bad.encode())}}))
            rc3 = neg_emit("r3.json")
            ok3 = False
            r3p = td / "r3.json"
            if rc3 == 0 and r3p.exists():
                ok3 = not all(e["sealed_green_1_1_0_0"] for e in
                              json.loads(r3p.read_text())
                              ["roster"]["p25_frozen_87"])
            check("roster-per-name-seal-not-count", rc3 != 0 or ok3)
        except SystemExit as e:
            check("roster-frozen-87-pername", False, f"exit {e.code}")
        finally:
            UR.scan_source_functions = orig_scan
            UR.SRC = orig_src

        # ---------- build executor: Root fixed offline recipe ----------
        import os as _os
        _os.environ["P26_BUILD_EV_DIR"] = str(td / "fake-build-ev")
        S = "f" * 64
        # plan requires expect sha
        rc = BE.main(["plan", "--service", "dataset"])
        check("build-plan-needs-expect-sha", rc == 2)
        # pipeline refused (no P26 delta)
        tmp_rel = td / "rel.json"
        tmp_rel.write_text(json.dumps(
            {"released_for": ["build-connector", "build-dataset"],
             "released_by": "root", "expires_utc": "29991231T000000Z"}))
        rc = BE.main(["run", "--execute", "--service", "pipeline",
                      "--release-file", str(tmp_rel)])
        check("build-pipeline-refused-no-delta", rc == 2)
        rc = BE.main(["run", "--execute", "--service", "dataset",
                      "--release-file", str(tmp_rel)])
        check("build-expect-pin-required", rc == 2)

        fake_root = td / "repo"
        for rel in BE.CONFIG_YAML_CANDIDATES["connector"]:
            q = fake_root / rel
            q.parent.mkdir(parents=True, exist_ok=True)
        orig_open = BE.G.OPENFOUNDRY
        orig_hash = BE.G.candidate_hash
        BE.G.OPENFOUNDRY = fake_root
        BE.G.candidate_hash = lambda root: S
        try:
            # source mismatch -> zero docker calls
            BE.G.candidate_hash = lambda root: "0" * 64
            f0 = Fake(lambda a: (_ for _ in ()).throw(
                AssertionError("no docker allowed on source mismatch")))
            rc = BE.main(["run", "--execute", "--service", "connector",
                          "--release-file", str(tmp_rel),
                          "--expect-source-sha", S], runner=f0)
            check("build-source-mismatch-zero-docker", rc == 2 and
                  f0.calls == [])
            BE.G.candidate_hash = lambda root: S

            # go-cache drift -> refuse BEFORE compile
            def h_nogo(argv, timeout_s=None):
                sa = j(argv)
                if "image inspect golang:1.25" in sa:
                    return 1, "No such image\n"
                if sa.startswith("docker inspect openfoundry"):
                    return 0, "sha256:" + BE.OLD_FULL_IDS["connector"] + \
                              "\n"
                if "image inspect openfoundry" in sa:
                    return 0, "sha256:" + BE.OLD_FULL_IDS["connector"] + \
                              "\n"
                raise AssertionError(sa)
            f1 = Fake(h_nogo)
            rc = BE.main(["run", "--execute", "--service", "connector",
                          "--release-file", str(tmp_rel),
                          "--expect-source-sha", S], runner=f1)
            check("build-go-cache-miss-refused-pre-compile",
                  rc == 2 and not any("go build" in j(c) for c in
                                      f1.calls))

            # config.yaml present -> STOP
            (fake_root / BE.CONFIG_YAML_CANDIDATES["connector"][0]
             ).write_text("x: 1\n")
            rc = BE.main(["run", "--execute", "--service", "connector",
                          "--release-file", str(tmp_rel),
                          "--expect-source-sha", S], runner=Fake(
                lambda a: (0, "sha256:" + BE.OLD_FULL_IDS["connector"])))
            check("build-config-yaml-present-stops", rc == 2)
            (fake_root / BE.CONFIG_YAML_CANDIDATES["connector"][0]
             ).unlink()

            # happy path, full recipe sequencing
            cid = "b" * 64
            new_full = "c" * 64
            cfg = "nonroot|/app|[\"/service\"]|null"
            state = {"compile": 0, "rm": 0, "tag_moves": 0}

            def h_ok(argv, timeout_s=None):
                sa = j(argv)
                if "image inspect golang:1.25" in sa:
                    return 0, "sha256:" + BE.GO_FULL_IDS["connector"] + \
                              "\n"
                if "image inspect openfoundry-platform-p1-q3v9-connector" \
                        in sa or ("docker inspect openfoundry-platform-"
                                  "p1-q3v9-connector --format" in sa):
                    return 0, ("sha256:" + BE.OLD_FULL_IDS["connector"] +
                               "\n")
                if ".Image}} {{.Config.Image}}" in sa:
                    return 0, ("sha256:" + BE.OLD_FULL_IDS["connector"] +
                               " sha256:" +
                               BE.OLD_FULL_IDS["connector"] + "\n")
                if "go build" in sa or "go version &&" in sa:
                    state["compile"] += 1
                    host_out = [t for t in argv if t.endswith(":/out")][0]
                    host_out = host_out.split(":")[0]
                    (Path(host_out) / "service").write_bytes(
                        b"FAKEBIN" * 16)
                    (Path(host_out) / "service").chmod(0o755)
                    return 0, "go version go1.25.14 linux/arm64\n" \
                              "abc123  /out/service\n"
                if sa.startswith("docker volume inspect"):
                    return 0, ""
                if sa.startswith("docker create"):
                    state["created"] = argv
                    return 0, cid + "\n"
                if sa.startswith("docker cp"):
                    return 0, ""
                if sa.startswith("docker commit"):
                    state["commit"] = argv
                    return 0, "sha256:" + new_full + "\n"
                if "openfoundry-p26-connector" in sa and \
                        "{{.Id}}|" in sa:
                    return 0, "sha256:" + new_full + "|" + cfg + "\n"
                if f"sha256:{BE.OLD_FULL_IDS['connector']}" in sa and \
                        ".Config.User" in sa:
                    return 0, cfg + "\n"
                if new_cid_probe(sa):
                    return 0, cid + " p26-build\n"
                if sa == f"docker rm {cid}":
                    state["rm"] += 1
                    return 0, cid + "\n"
                if sa.startswith("docker tag") or "compose" in sa:
                    state["tag_moves"] += 1
                    return 0, ""
                raise AssertionError(sa)

            def new_cid_probe(sa):
                return ("docker inspect " + cid) in sa and \
                        "of-p26-owner" in sa
            f2 = Fake(h_ok)
            rc = BE.main(["run", "--execute", "--service", "connector",
                          "--release-file", str(tmp_rel),
                          "--expect-source-sha", S], runner=f2)
            check("build-recipe-happy-rc0", rc == 0)
            check("build-recipe-no-old-tag-moves",
                  state["tag_moves"] == 0 and
                  not any("compose" in j(c) for c in f2.calls))
            check("build-recipe-offline-flags",
                  state["compile"] == 1 and
                  any("--network" in c and "none" in c for c in f2.calls))
            create = state.get("created") or []
            check("build-recipe-create-from-old-fullid",
                  f"sha256:{BE.OLD_FULL_IDS['connector']}" in create and
                  "--label" in create and "of-p26-build" in j(create) and
                  "-e" not in create and "-p" not in create)
            check("build-recipe-cleanup-exact-cid-only",
                  state["rm"] == 1 and
                  j(f2.calls[-1]) == f"docker rm {cid}")
            happy_dir = max((td / "fake-build-ev").glob("build-connector_*"),
                            key=lambda q: q.stat().st_mtime)
            # negative: volume missing -> ZERO compile/create
            def h_novol(argv, timeout_s=None):
                sa = j(argv)
                if sa.startswith("docker volume inspect"):
                    return 1, "no such volume\n"
                if "image inspect golang:1.25" in sa:
                    return 0, "sha256:" + BE.GO_FULL_IDS["connector"] + \
                              "\n"
                if "q3v9-connector" in sa or \
                        "q3v9-connector-1" in sa:
                    return 0, "sha256:" + BE.OLD_FULL_IDS["connector"] + \
                              "\n"
                return 127, "UNEXPECTED:" + sa
            f3 = Fake(h_novol)
            rc = BE.main(["run", "--execute", "--service", "connector",
                          "--release-file", str(tmp_rel),
                          "--expect-source-sha", S], runner=f3)
            check("build-volume-missing-zero-compile-create",
                  rc == 2 and not any(
                      "go build" in j(c) or
                      j(c).startswith("docker create") for c in f3.calls))
            # negative: first probe rc!=0 -> immediate stop (later probes
            # never called)
            f4 = Fake(lambda a: (1, "daemon down\n"))
            rc = BE.main(["run", "--execute", "--service", "connector",
                          "--release-file", str(tmp_rel),
                          "--expect-source-sha", S], runner=f4)
            check("build-probe-first-error-immediate-stop",
                  rc == 2 and len(f4.calls) == 1 and
                  f4.calls[0][0:3] == ["docker", "inspect",
                                       "openfoundry-platform-p1-q3v9-"
                                       "connector"])
            ev = sorted((td / "fake-build-ev").glob(
                "build-connector_*"))
            check("build-recipe-unique-attempts", len(ev) >= 4 and
                  len({e.name for e in ev}) == len(ev))
            latest = happy_dir
            comps = sorted(p.name.split("-")[0] for p in
                           latest.glob("*.rc"))
            check("build-recipe-components",
                  {"00", "01", "02", "03", "04", "05", "06", "07",
                   "08", "09", "10", "11", "12"} <=
                  {c for c in comps})
            br = json.loads((latest / "BUILD-RESULT.json").read_text())
            check("build-recipe-result-binds",
                  br["ok"] and br["source_post"] == S and
                  br["binary"]["mode"] == "0755" and
                  br["new_full_id"] == f"sha256:{new_full}")
        finally:
            BE.G.OPENFOUNDRY = orig_open
            BE.G.candidate_hash = orig_hash
            _os.environ.pop("P26_BUILD_EV_DIR", None)

        # ---------- switch guard: synthetic seal via cohort fixture -----
        sys.path.insert(0, str(Path(__file__).parent))
        from selftest_p26_cohort_cli import build_fixture, C as _C
        d, seal, count = build_fixture(td / "fin")
        rc = SG.main(["guard", "--final-dir", str(d), "--seal-sha", seal,
                      "--files-count", str(count),
                      "--out", str(td / "guard.json")])
        g = json.loads((td / "guard.json").read_text())
        check("guard-synthetic-green", rc == 0 and g["ok"] and
              g["checks"]["derive_protected"]["protected9"] == 9 and
              g["checks"]["derive_protected"]["owner8_full_cp_originals"])
        # tamper one sealed file -> guard refuses
        (d / "extra.txt").write_text("tamper\n")
        rc = SG.main(["guard", "--final-dir", str(d), "--seal-sha", seal,
                      "--files-count", str(count),
                      "--out", str(td / "guard2.json")])
        g2 = json.loads((td / "guard2.json").read_text())
        check("guard-tamper-refused", rc == 1 and g2["ok"] is False)
        (d / "extra.txt").unlink()
        # deploy-plan: forbidden old port collision refused
        bad_res = [dict(SG.NEW_RESOURCES[0], host_port=17090)]
        orig_res = SG.NEW_RESOURCES
        SG.NEW_RESOURCES = bad_res
        try:
            rc = SG.main(["deploy-plan", "--out", str(td / "dp.json")])
            check("deploy-plan-old-port-refused", rc == 1)
        finally:
            SG.NEW_RESOURCES = orig_res
        rc = SG.main(["deploy-plan", "--out", str(td / "dp2.json")])
        dp = json.loads((td / "dp2.json").read_text())
        check("deploy-plan-typed-ledger",
              rc == 0 and len(dp["typed_ledger"]) == 4 and
              {r["host_port"] for r in dp["typed_ledger"]} ==
              {17095, 17096, 17097, 17098} and
              all("17090" not in json.dumps(c) or "17090-17094" in
                  json.dumps(dp["never"]) for c in
                  dp["exact_commands_after_release"][:1]))

    npass = sum(1 for _, ok, _ in RESULTS if ok)
    print(f"[accept] {npass}/{len(RESULTS)} PASS")
    return 0 if npass == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())

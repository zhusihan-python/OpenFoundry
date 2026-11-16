#!/usr/bin/env python3
"""PURE self-test for common/p26_gates.py (round-1 rework).

No docker/HTTP: fakes only; P25 module import for recipe parity is
read-only (dont_write_bytecode set BEFORE import — no old __pycache__).
"""

import json
import os
import sys
import tempfile
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(f"[gates] {'PASS' if cond else 'FAIL'} {name}"
          + (f" — {detail}" if detail and not cond else ""))


class FakeRunner:
    def __init__(self, script=None):
        self.calls = []
        self.script = script or {}

    def __call__(self, argv, timeout_s=None):
        self.calls.append([str(x) for x in argv])
        for pat, val in self.script.items():
            if pat in " ".join(str(x) for x in argv):
                return val[0], val[1]
        return 0, ""


def main():
    w6dir = Path(__file__).resolve().parents[1] / "results" / "w6"
    w6dir.mkdir(parents=True, exist_ok=True)

    # argv_lint: --pull=never allowed; every other pull refused
    check("lint-pull-never-ok",
          G.argv_lint(["docker", "run", "--pull=never", "x"]) == [])
    check("lint-pull-bare", bool(G.argv_lint(["docker", "run", "--pull",
                                              "x"])))
    check("lint-pull-always",
          bool(G.argv_lint(["docker", "run", "--pull=always", "x"])))
    check("lint-docker-pull", bool(G.argv_lint(["docker", "pull", "x"])))
    check("lint-build-pull-flag",
          bool(G.argv_lint(["docker", "build", "--pull", "."])))
    check("lint-publish-p",
          bool(G.argv_lint(["docker", "run", "-p", "5432:5432", "x"])))
    check("lint-net-host",
          bool(G.argv_lint(["docker", "run", "--network", "host", "x"])))
    check("lint-net-host-eq",
          bool(G.argv_lint(["docker", "run", "--network=host", "x"])))
    check("lint-clean", G.argv_lint(["docker", "ps", "--format", "x"]) == [])

    # readonly classifier
    check("ro-ps", G.is_readonly_docker(["docker", "ps"]))
    check("ro-version", G.is_readonly_docker(["docker", "version"]))
    check("ro-inspect",
          G.is_readonly_docker(["docker", "inspect", "x"]))
    check("ro-volume-inspect",
          G.is_readonly_docker(["docker", "volume", "inspect", "v"]))
    check("ro-image-inspect",
          G.is_readonly_docker(["docker", "image", "inspect", "i"]))
    check("not-ro-run", not G.is_readonly_docker(["docker", "run", "x"]))
    check("not-ro-volume-create",
          not G.is_readonly_docker(["docker", "volume", "create", "v"]))
    check("not-ro-network-rm",
          not G.is_readonly_docker(["docker", "network", "rm", "n"]))

    # old substrate refusals
    check("old-rm-of25pg",
          bool(G.old_substrate_write_refusal(["docker", "rm", "-f",
                                              "of-p25-pg"])))
    check("old-stop-q3-connector",
          bool(G.old_substrate_write_refusal(
              ["docker", "stop", f"{G.Q3_PROJECT}-connector-1"])))
    check("old-exec-of24pg",
          bool(G.old_substrate_write_refusal(
              ["docker", "exec", "of-p24-pg", "psql"])))
    check("old-inspect-allowed",
          G.old_substrate_write_refusal(
              ["docker", "inspect", "of-p25-pg", "--format", "{{.Id}}"])
          == [])
    check("compose-wrong-project",
          bool(G.old_substrate_write_refusal(
              ["docker", "compose", "-f", "x", "--project-name",
               "other", "build"])))
    check("compose-down-refused",
          bool(G.old_substrate_write_refusal(
              ["docker", "compose", "--project-name", G.Q3_PROJECT,
               "down"])))
    check("compose-build-q3-allowed",
          G.old_substrate_write_refusal(
              ["docker", "compose", "-f", "y", "--project-name",
               G.Q3_PROJECT, "build", "connector"]) == [])
    check("p26-rm-allowed",
          G.old_substrate_write_refusal(["docker", "rm", "-f", G.P26_PG])
          == [])

    # redaction / scrub
    red = G.redact_argv(["-e", "POSTGRES_PASSWORD=s3cr3tX"], ["s3cr3tX"])
    check("redact-secret", "s3cr3tX" not in " ".join(red))
    scrubbed, n = G.scrub("a s3cr3tX b s3cr3tX", ["s3cr3tX"])
    check("scrub-counts", n == 2 and "s3cr3tX" not in scrubbed)

    # names gate
    good = ["go version go1.25.14 linux/arm64", "=== RUN   TestA",
            "--- PASS: TestA (1.0s)", "ok  \tpkg\t3s"]
    stats, fw = G.parse_go_test_verbose(good)
    check("names-good",
          G.required_names_gate(stats, fw, ["TestA"])["ok"])
    miss = G.required_names_gate(stats, fw, ["TestA", "TestC"])
    check("names-missing-fails", not miss["ok"])
    sk = ["=== RUN   TestA", "--- SKIP: TestA", "ok  \tpkg"]
    st2, fw2 = G.parse_go_test_verbose(sk)
    check("names-skip-fails",
          not G.required_names_gate(st2, fw2, ["TestA"])["ok"])
    fl = ["=== RUN   TestA", "--- FAIL: TestA", "FAIL", "FAIL\tpkg"]
    st3, fw3 = G.parse_go_test_verbose(fl)
    check("names-fail-fails",
          not G.required_names_gate(st3, fw3, ["TestA"])["ok"])
    dup = ["=== RUN   TestA", "--- PASS: TestA", "=== RUN   TestA",
           "--- PASS: TestA", "ok  \tpkg"]
    st4, fw4 = G.parse_go_test_verbose(dup)
    check("names-dup-fails",
          not G.required_names_gate(st4, fw4, ["TestA"])["ok"])

    # source gate helper
    check("source-gate-ok", G.source_gate("a" * 64, "a" * 64)["ok"])
    check("source-gate-drift", not G.source_gate("a" * 64, "b" * 64)["ok"])

    # run_phase: release/first-error/retry semantics
    mut = [dict(G.step("s1", "up", "x", ["docker", "network", "create",
                                         "of-p26-pg-net"]),
                kind="argv", write_class="mutating")]
    fr = FakeRunner()
    res = G.run_phase(mut, "up", fr, release=None, execute=True)
    check("mut-no-release-refused-zero-calls",
          res["verdict"] == "refused" and fr.calls == [])
    rel_other = {"released_for": ["other"], "released_by": "root",
                 "expires_utc": "29991231T000000Z"}
    fr = FakeRunner()
    res = G.run_phase(mut, "up", fr, release=rel_other, execute=True)
    check("mut-phase-not-released-zero-calls",
          res["verdict"] == "refused" and fr.calls == [])
    rel = {"released_for": ["up"], "released_by": "root",
           "expires_utc": "29991231T000000Z"}
    fr = FakeRunner()
    res = G.run_phase(mut, "up", fr, release=rel, execute=True)
    check("mut-released-runs", res["verdict"] == "pass" and
          len(fr.calls) == 1)
    fr = FakeRunner()
    res = G.run_phase(mut, "up", fr, release=rel, execute=False)
    check("mut-no-execute-flag-refused",
          res["verdict"] == "refused" and fr.calls == [])

    ro = [dict(G.step("r1", "facts", "x", ["docker", "ps"]),
               kind="argv", write_class="none")]
    fr = FakeRunner()
    res = G.run_phase(ro, "facts", fr, release=None, execute=False)
    check("readonly-runs-without-release",
          res["verdict"] == "pass" and len(fr.calls) == 1)

    seq = [dict(G.step("a", "up", "x", ["docker", "ps"]),
                kind="argv", write_class="none"),
           dict(G.step("b", "up", "x", ["docker", "network", "create",
                                        "of-p26-pg-net"]),
                kind="argv", write_class="mutating")]
    fr = FakeRunner(script={"network create": (1, "boom")})
    res = G.run_phase(seq, "up", fr, release=rel, execute=True)
    check("first-error-stops",
          res["verdict"] == "fail" and res["first_error"]["id"] == "b"
          and len(fr.calls) == 2)

    gate_bad = [dict(G.step("g", "up", "x", ["docker", "pull", "x"]),
                     kind="argv", write_class="mutating")]
    fr = FakeRunner()
    res = G.run_phase(gate_bad, "up", fr, release=rel, execute=True)
    check("gate-violation-zero-calls",
          res["verdict"] == "fail" and fr.calls == [] and
          "gate" in res["first_error"]["detail"])

    # bounded retry (readiness-style): 3 attempts, then fail; sleeps faked
    retr = [dict(G.step("p", "up", "probe", ["docker", "exec", "c",
                                             "pg_isready"]),
                 kind="argv", write_class="mutating",
                 retry={"attempts": 3, "sleep_s": 0})]
    fr = FakeRunner(script={"pg_isready": (1, "no")})
    sleeps = []
    res = G.run_phase(retr, "up", fr, release=rel, execute=True,
                      sleep_fn=lambda s: sleeps.append(s))
    check("retry-bounded-fails",
          res["verdict"] == "fail" and len(fr.calls) == 3 and
          len(sleeps) == 2 and res["steps"][0]["tries"] == 3)
    fr = FakeRunner(script={"pg_isready": (1, "no")})
    fr_calls = []

    def flaky(argv, timeout_s=None):
        fr_calls.append(argv)
        return (0, "ok") if len(fr_calls) == 3 else (1, "not yet")

    res = G.run_phase(retr, "up", flaky, release=rel, execute=True,
                      sleep_fn=lambda s: None)
    check("retry-bounded-succeeds-third",
          res["verdict"] == "pass" and len(fr_calls) == 3)

    # check predicate failure stops the phase
    chk = [dict(G.step("c1", "up", "x", ["docker", "ps"]),
                kind="argv", write_class="none",
                check=lambda r: (False, "label mismatch")),
           dict(G.step("c2", "up", "y", ["docker", "ps"]),
                kind="argv", write_class="none")]
    fr = FakeRunner()
    res = G.run_phase(chk, "up", fr)
    check("check-failure-stops",
          res["verdict"] == "fail" and len(fr.calls) == 1 and
          "label mismatch" in res["first_error"]["detail"])

    # required names loader
    with tempfile.TemporaryDirectory(dir=str(w6dir)) as td:
        empty = Path(td) / "names.txt"
        empty.write_text("# only comments\n")
        try:
            G.load_required_names(empty)
            check("names-loader-empty-refuses", False)
        except G.GateRefusal:
            check("names-loader-empty-refuses", True)
        nf = Path(td) / "n2.txt"
        nf.write_text("# c\nTestA\nTestB\n")
        check("names-loader-good", G.load_required_names(nf) ==
              ["TestA", "TestB"])

    # SOURCE RECIPE PARITY vs the actual P25 module (read-only import)
    with tempfile.TemporaryDirectory(dir=str(w6dir)) as td:
        root = Path(td) / "repo"
        for rel in ("services/connector-management-service/x",
                    "services/dataset-versioning-service/y",
                    "services/pipeline-build-service/z",
                    "libs/scheduling-cron/w"):
            (root / rel).mkdir(parents=True)
        (root / "go.mod").write_text("module m\n")
        (root / "go.sum").write_text("sum\n")
        (root / "services/connector-management-service/x/a.go").write_text(
            "package x\n")
        (root / "services/connector-management-service/x/b.sql").write_text(
            "SELECT 1;\n")
        (root / "services/dataset-versioning-service/y/c.go").write_text(
            "package y\n")
        (root / "services/pipeline-build-service/z/d.go").write_text(
            "package z\n")
        (root / "libs/scheduling-cron/w/e.go").write_text("package w\n")
        (root / "libs/scheduling-cron/w/f.txt").write_text("ignored\n")
        mine = G.candidate_hash(root)
        # INDEPENDENT expected-stream construction (root boundary: never
        # import/execute frozen P25 tools in selftests — their source is
        # read as reference only; this fixture re-derives the documented
        # recipe layout independently).
        import hashlib as _hl
        expected = b"go.mod\n" + (root / "go.mod").read_bytes()
        expected += b"go.sum\n" + (root / "go.sum").read_bytes()
        expected += b"content-hashes\n"
        paths = []
        for rel, exts in G.SOURCE_ROOTS:
            for q in (root / rel).rglob("*"):
                if q.is_file() and q.suffix in exts:
                    paths.append(q)
        for q in sorted(paths):
            expected += (f"{_hl.sha256(q.read_bytes()).hexdigest()}  "
                         f"{q}\n").encode()
        import hashlib as _h2
        theirs = _h2.sha256(expected).hexdigest()
        check("source-recipe-independent-fixture", mine == theirs,
              f"mine={mine} fixture={theirs}")
        check("fixture-excludes-nonroot-file", b"f.txt" not in expected)
        # newlines/absolute-path record shape spot check
        stream = G.candidate_stream(root)
        check("recipe-record-shape",
              b"content-hashes\n" in stream and
              b"  " + str(root).encode() + b"/" in stream and
              stream.endswith(b".go\n"))

    # round2 #3: real subprocess timeout (local sleep — no docker/http)
    rc, out = G.real_runner([sys.executable, "-c",
                             "import time; time.sleep(5)"], timeout_s=1)
    check("real-runner-timeout-bounded",
          rc == 124 and "TIMEOUT after 1s" in out)
    rc, out = G.real_runner([sys.executable, "-c", "print('hi')"],
                            timeout_s=10)
    check("real-runner-normal", rc == 0 and "hi" in out)

    # round2-final #1: TimeoutExpired streams are bytes/str/None mixed
    import subprocess as _sp
    e1 = _sp.TimeoutExpired(cmd=["x"], timeout=1,
                            output=b"raw-out", stderr=None)
    check("timeout-bytes-none", G._timeout_output(e1) == "raw-out")
    e2 = _sp.TimeoutExpired(cmd=["x"], timeout=1,
                            output="", stderr=b"raw-err")
    check("timeout-str-bytes", G._timeout_output(e2) == "raw-err")
    e3 = _sp.TimeoutExpired(cmd=["x"], timeout=1, output=b"o",
                                    stderr=b"e")
    check("timeout-bytes-bytes", G._timeout_output(e3) == "oe")
    e4 = _sp.TimeoutExpired(cmd=["x"], timeout=1)
    check("timeout-none-none", G._timeout_output(e4) == "")

    # round2 #1: EXPOSE != host publishing — mapping parser fixtures
    check("ports-empty-obj", G.parse_ports_mapping("{}")[0])
    check("ports-null", G.parse_ports_mapping("null")[0])
    check("ports-blank", G.parse_ports_mapping("")[0])
    check("ports-exposed-unpublished",
          G.parse_ports_mapping('{"5432/tcp": null}')[0])
    check("ports-exposed-empty-list",
          G.parse_ports_mapping('{"5432/tcp": []}')[0])
    check("ports-empty-binding-objects",
          G.parse_ports_mapping(
              '{"5432/tcp": [{"HostIp": "", "HostPort": ""}]}')[0])
    check("ports-real-binding-refused",
          not G.parse_ports_mapping(
              '{"5432/tcp": [{"HostIp": "", "HostPort": "15432"}]}')[0])
    check("ports-hostip-binding-refused",
          not G.parse_ports_mapping(
              '{"5432/tcp": [{"HostIp": "0.0.0.0", "HostPort": ""}]}')[0])
    check("ports-garbage-refused",
          not G.parse_ports_mapping("not json")[0])
    check("bindings-empty-ok", G.parse_empty_bindings("{}")[0])
    check("bindings-null-ok", G.parse_empty_bindings("null")[0])
    check("bindings-mapping-null-ok",
          G.parse_empty_bindings('{"5432/tcp": null}')[0])
    check("bindings-hostport-refused",
          not G.parse_empty_bindings(
              '{"5432/tcp": [{"HostIp": "", "HostPort": "15432"}]}')[0])

    npass = sum(1 for _, ok, _ in RESULTS if ok)
    print(f"[gates] {npass}/{len(RESULTS)} PASS")
    return 0 if npass == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())

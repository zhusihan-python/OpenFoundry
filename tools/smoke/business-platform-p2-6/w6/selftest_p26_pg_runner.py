#!/usr/bin/env python3
"""PURE self-test for the EXECUTABLE pg/p26_pg_runner.py (round-2 rework).

All docker interactions are scripted fakes injected through main(argv,
runner=fake); P26_SECRET_FILE is overridden to a temp path so the REAL
private file is never created/read. Covers root round2 blockers: ports
mapping (EXPOSE != publish), per-probe preflight evidence, strict image
pins, monotonic readiness wall cap with every attempt retained, bounded
subprocess timeouts, unique exclusive test env file, go-test -timeout.
"""

import json
import os
import re
import sys
import tempfile
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402
from pg import p26_pg_runner as R  # noqa: E402

RESULTS = []
PGID = R.PG_IMAGE_ID_RECORDED
GOID = R.GO_IMAGE_IDS_RECORDED["golang:1.25"]
GOID_DS = R.GO_IMAGE_IDS_RECORDED["golang:1.26.3"]


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(f"[pg-runner] {'PASS' if cond else 'FAIL'} {name}"
          + (f" — {detail}" if detail and not cond else ""))


class Fake:
    def __init__(self, handler):
        self.calls = []
        self.handler = handler

    def __call__(self, argv, timeout_s=None):
        argv = [str(x) for x in argv]
        self.calls.append(argv)
        rc, out = self.handler(argv)
        return rc, out


def j(argv):
    return " ".join(argv)


def prepare_handler(state, pg_ready_after=2, ready_rc=None,
                    pg_id=None, ports_json='{"5432/tcp": null}',
                    bindings_json="{}", pg_version="160004\n"):
    pg_id = pg_id or PGID
    ready_count = [0]

    def h(argv):
        s = j(argv)
        if s == "docker network inspect of-p26-pg-net --format {{.Id}}":
            return 1, "Error: No such network: of-p26-pg-net\n"
        if s == "docker inspect of-p26-pg --format {{.Id}}":
            return 1, "Error: No such object: of-p26-pg\n"
        if "image inspect postgres:16" in s:
            return 0, f"sha256:{pg_id} postgres:16\n"
        if s.startswith("docker network create"):
            state["net_created"] = True
            return 0, "netid123\n"
        if "network inspect of-p26-pg-net --format" in s:
            return 0, "p26-pg-isolated\n"
        if s.startswith("docker run -d --pull=never"):
            state["container_run"] = argv
            return 0, "ctr123\n"
        if "pg_isready" in s:
            ready_count[0] += 1
            state["ready_calls"] = ready_count[0]
            if ready_rc is not None:
                return ready_rc, "nope\n"
            if ready_count[0] > pg_ready_after:
                return 0, "accepting connections\n"
            return 1, "no response\n"
        if "inspect of-p26-pg --format" in s and "Config.Labels" in s:
            return 0, "p26-pg-isolated\n"
        if ".State.Running" in s:
            return 0, "true\n"
        if "{{.Image}}" in s:
            return 0, f"sha256:{pg_id}\n"
        if "NetworkSettings.Ports" in s:
            return 0, ports_json + "\n"
        if "HostConfig.PortBindings" in s:
            return 0, bindings_json + "\n"
        if s == "docker port of-p26-pg":
            return 0, ""
        if "SHOW server_version_num" in s:
            return (0, pg_version) if not pg_version.startswith("FAIL") \
                else (1, "boom\n")
        raise AssertionError(f"unexpected argv: {argv}")
    return h


def release_file(td, phases):
    p = Path(td) / "release.json"
    p.write_text(json.dumps({"released_for": phases, "released_by": "root",
                             "expires_utc": "29991231T000000Z"}))
    return str(p)


_REL = ["/nonexistent-release"]


def run(argv, handler, env_overrides=None, now_fn=None):
    old = {}
    env_overrides = env_overrides or {}
    for k, v in env_overrides.items():
        old[k] = os.environ.get(k)
        os.environ[k] = v
    fake = Fake(handler)
    try:
        try:
            rc = R.main(argv + ["--release-file", _REL[0]], runner=fake,
                        sleep_fn=lambda s: None,
                        now_fn=now_fn if now_fn else
                        __import__("time").monotonic)
        except SystemExit as e:
            rc = int(e.code or 0)
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return rc, fake


def rt_handler(state, log=None, volume_rc=0, image="golang:1.25",
               go_id=None):
    log = log if log is not None else (
        "go version go1.25.14 linux/arm64\n"
        "=== RUN   TestA\n--- PASS: TestA (0.1s)\n"
        "=== RUN   TestB\n--- PASS: TestB (0.1s)\n"
        "ok  \t./internal/repo\t0.2s\nENDMARK-7f3a\n")
    if image == "golang:1.26.3":
        log = log.replace("go1.25.14", "go1.26.3")
    state["run_argv"] = None

    def h(argv):
        s = j(argv)
        if "network inspect of-p26-pg-net --format" in s:
            return 0, "p26-pg-isolated\n"
        if "inspect of-p26-pg --format" in s and "Config.Labels" in s:
            return 0, "p26-pg-isolated\n"
        if ".State.Running" in s:
            return 0, "true\n"
        if s.startswith("docker volume inspect"):
            return volume_rc, "" if volume_rc == 0 else "no such volume\n"
        if "CREATE DATABASE ofp26_unified_" in s:
            return 0, "CREATE DATABASE\n"
        if s.endswith("SHOW server_version"):
            return 0, "16.15\n"
        tags = {"golang:1.25": "golang:1.25,golang:1.25.14",
                "golang:1.26.3": "golang:1.26.3"}[image]
        if f"image inspect {image}" in s:
            return 0, f"sha256:{go_id or GOID} {tags}\n"
        if s.startswith("docker run --pull=never --rm"):
            state["run_argv"] = argv
            state["env_path"] = argv[argv.index("--env-file") + 1]
            return 0, log + "leak: postgres://ofp26:PWLEAK@x\n"
        raise AssertionError(f"unexpected argv: {argv}")
    return h


def main():
    global _REL
    w6dir = Path(__file__).resolve().parents[1] / "results" / "w6"
    w6dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=str(w6dir)) as td:
        td = Path(td)
        secret = td / "p26.env"
        _REL = [str(td / "release.json")]
        os.environ["P26_PG_EV_DIR"] = str(td / "fake-pg-ev")
        os_env = {"P26_SECRET_FILE": str(secret)}

        check("names-dataset-9", len(G.load_required_names(
            R.HERE / "p26-dataset-pg-names.txt")) == 9 and
            "TestPGP26DatasetWireCommitAckLossReplaysReceipt" in
            G.load_required_names(R.HERE / "p26-dataset-pg-names.txt"))
        ucoh = R.load_unified_cohorts()
        check("unified-cohorts-loaded",
              set(ucoh) == {"dataset", "pipeline", "connector"} and
              ucoh["pipeline"]["dsn_env"] == "PIPELINE_DB_DSN" and
              "./services/pipeline-build-service/internal/postgres/" in
              ucoh["pipeline"]["packages"])
        for cs in ucoh.values():
            for pk in cs["packages"]:
                if not (R.HERE.parents[1] / "services").is_dir():
                    pass
                assert pk.startswith("./services/"), pk
        check("names-connector-18", len(G.load_required_names(
            R.HERE / "p26-connector-pg-names.txt")) == 18 and
            "TestPGP26FinalizeWireCommitAckLossOnce" in
            G.load_required_names(R.HERE / "p26-connector-pg-names.txt"))

        # ---- prepare: existing secret preserved, exit 3, zero writes ----
        secret.write_text("POSTGRES_PASSWORD=" + "b" * 48 + "\n")
        state = {}
        rc, fake = run(["prepare"], prepare_handler(state),
                       env_overrides=os_env)
        mut = [c for c in fake.calls
               if j(c).startswith(("docker network create", "docker run"))]
        check("prepare-existing-secret-preserved",
              rc == 3 and mut == [])

        # ---- prepare: docker FAILURE distinguished (rc 2, stops) --------
        def daemon_down(argv, timeout_s=None):
            if "network inspect of-p26-pg-net --format {{.Id}}" in j(argv):
                return 1, "Cannot connect to the Docker daemon"
            raise AssertionError(f"unexpected {argv}")
        if secret.exists():
            secret.unlink()
        rc, fake = run(["prepare"], daemon_down, env_overrides=os_env)
        check("prepare-docker-failure-distinguished",
              rc == 2 and not any(j(c).startswith("docker run")
                                  for c in fake.calls))

        # ---- P4: strict not-found classification (fail-closed) --------
        def make_daemon_form(state):
            inner = prepare_handler(state)

            def h(argv, timeout_s=None):
                sa = j([str(x) for x in argv])
                if sa == ("docker network inspect of-p26-pg-net "
                          "--format {{.Id}}"):
                    return 1, ("Error response from daemon: network "
                               "of-p26-pg-net not found\n")
                if sa == "docker inspect of-p26-pg --format {{.Id}}":
                    return 1, ("Error response from daemon: No such "
                               "object: of-p26-pg\n")
                return inner(argv)
            return h
        _REL[0] = str(td / "nonexistent.json")
        state = {}
        rc, fake = run(["prepare"], make_daemon_form(state),
                       env_overrides=os_env)
        check("prepare-daemon-form-notfound-accepted-no-release",
              rc == 2 and  # proceeds past probes, refuses at release gate
              not any(j(c).startswith("docker network create")
                      for c in fake.calls))
        _REL[0] = release_file(td, ["prepare"])
        if _REL[0].startswith("/nonexistent"):
            _REL[0] = str(td / "release.json")
            Path(_REL[0]).write_text(json.dumps(
                {"released_for": ["prepare"], "released_by": "root",
                 "expires_utc": "29991231T000000Z"}))
        state = {}
        rc, fake = run(["prepare", "--execute"], make_daemon_form(state),
                       env_overrides=os_env)
        check("prepare-daemon-form-full-happy",
              rc == 0 and state.get("net_created") is True)
        if secret.exists():
            secret.unlink()
        _REL[0] = str(td / "nonexistent2.json")
        def empty_out(argv, timeout_s=None):
            if "network inspect of-p26-pg-net --format {{.Id}}" in j(argv):
                return 1, ""  # rc!=0 + EMPTY output must NOT be absent
            raise AssertionError(f"unexpected {argv}")
        rc, fake = run(["prepare"], empty_out, env_overrides=os_env)
        check("prepare-empty-out-fail-closed",
              rc == 2 and not any(j(c).startswith("docker network create")
                                  for c in fake.calls))

        def wrong_name(argv, timeout_s=None):
            if "network inspect of-p26-pg-net --format {{.Id}}" in j(argv):
                return 1, "Error: No such network: other-net\n"
            raise AssertionError(f"unexpected {argv}")
        rc, fake = run(["prepare"], wrong_name, env_overrides=os_env)
        check("prepare-wrong-name-notfound-fail-closed",
              rc == 2 and not any(j(c).startswith("docker network create")
                                  for c in fake.calls))

        # ---- per-probe preflight evidence + summary gate (round2 #4) ----
        _REL[0] = release_file(td, ["prepare"])
        state = {}
        rc, fake = run(["prepare", "--execute"], prepare_handler(state),
                       env_overrides=os_env)
        check("prepare-happy-rc0", rc == 0 and state.get("net_created"))
        pdirs = sorted(R.ev_dir().glob("prepare_*"))
        pd = [p for p in pdirs if (p / "PREPARE-RESULT.json").exists()][-1]
        def anyv(pat, want):
            vals = [p.read_text().strip() for p in pd.glob(pat)]
            return vals and want in vals
        rc_a_ok = anyv("00a-probe-network*.rc", "1")
        raw_a_ok = any(
            "No such network" in p.read_text() or
            "network of-p26-pg-net not found" in p.read_text()
            for p in pd.glob("00a-probe-network*.raw"))
        check("prepare-probe-original-rc-recorded",
              rc_a_ok and raw_a_ok)
        check("prepare-summary-gate-separate",
              anyv("00-preflight-summary*.gate.rc", "0"))
        check("prepare-probe-format-whitelisted",
              "{{.Id}}" in next(pd.glob("00a-probe-network*.cmd"))
              .read_text() and
              all("Config.Env" not in p.read_text()
                  for p in pd.glob("00b-probe-container*.cmd")))
        check("prepare-pg-version-probe",
              anyv("07-pg-version*.gate.rc", "0") and
              any("server_version_num" in p.read_text()
                  for p in pd.glob("07-pg-version*.cmd")))
        probe_ids = ["06a-label", "06b-running", "06c-image",
                     "06d-ports-mapping", "06e-port-bindings",
                     "06f-docker-port"]
        per_probe_ok = all(
            anyv(f"{pid}*.rc", "0") and
            any(p.read_text().startswith("docker") for p in
                pd.glob(f"{pid}*.cmd"))
            for pid in probe_ids)
        check("prepare-postvalidate-per-probe-evidence", per_probe_ok)
        check("prepare-postvalidate-summary-separate",
              anyv("06-post-validate-summary*.gate.rc", "0"))
        pwval = secret.read_text().strip().split("=", 1)[1]
        run_argv = state.get("container_run") or []
        check("prepare-run-argv-shape",
              "--pull=never" in run_argv and "--env-file" in run_argv and
              str(secret) in run_argv and f"sha256:{PGID}" in run_argv and
              not any(t.startswith(("-p", "-P", "--publish"))
                      for t in run_argv))
        check("prepare-secret-created",
              oct(secret.stat().st_mode & 0o777) == "0o600" and
              re.fullmatch(r"POSTGRES_PASSWORD=[0-9a-f]{48}\n",
                           secret.read_text()) is not None)

        # strict image pin drift -> refuse, zero creates (round2 #6)
        if secret.exists():
            secret.unlink()
        state = {}
        rc, fake = run(["prepare", "--execute"],
                       prepare_handler(state, pg_id="e" * 64),
                       env_overrides=os_env)
        check("prepare-image-drift-refused",
              rc == 2 and "net_created" not in state and
              not any(j(c).startswith("docker run") for c in fake.calls))

        # ports: real host binding -> refuse (round2 #1)
        if secret.exists():
            secret.unlink()
        state = {}
        rc, fake = run(["prepare", "--execute"],
                       prepare_handler(state, ports_json=
                                       '{"5432/tcp": [{"HostIp": "", '
                                       '"HostPort": "15432"}]}'),
                       env_overrides=os_env)
        check("prepare-host-binding-refused", rc == 2)

        # pg major != 16 -> refuse (round2 #4)
        if secret.exists():
            secret.unlink()
        state = {}
        rc, fake = run(["prepare", "--execute"],
                       prepare_handler(state, pg_version="170000\n"),
                       env_overrides=os_env)
        check("prepare-pg-major-refused", rc == 2)

        # readiness: deadline BEFORE each probe, probe timeout capped to
        # remaining, every attempt retained (round2-final #2)
        if secret.exists():
            secret.unlink()
        R.READY_ATTEMPTS = 20
        clock = [0.0]

        def now_flat():
            return clock[0]

        def slow_sleep(seconds):
            clock[0] += 40.0  # simulate probe+sleep cost exceeding budget
        old_run = None
        state = {}
        rc, fake = run(["prepare", "--execute"],
                       prepare_handler(state, ready_rc=1),
                       env_overrides=os_env, now_fn=now_flat)
        # (sleep_fn is faked to no-op inside run(); emulate via direct
        # call with a sleep-advancing clock below)
        rc2, fake2 = None, None
        # direct call with injectable sleep so the clock model is exact
        # (env override applied HERE TOO — the call sits outside run())
        if secret.exists():
            secret.unlink()
        import argparse as _ap
        rel_obj = {"released_for": ["prepare"], "released_by": "root",
                   "expires_utc": "29991231T000000Z"}
        args_p = _ap.Namespace(execute=True)
        fake_d = Fake(prepare_handler({}, ready_rc=1))
        os.environ["P26_SECRET_FILE"] = str(secret)
        os.environ["P26_PG_EV_DIR"] = str(td / "fake-pg-ev")
        try:
            try:
                rc_d = R.cmd_prepare(args_p, fake_d, rel_obj, True,
                                     sleep_fn=slow_sleep, now_fn=now_flat)
            except SystemExit as e:
                rc_d = int(e.code or 0)
            except R.Abort:
                rc_d = 2
        finally:
            os.environ.pop("P26_SECRET_FILE", None)
        rdirs = sorted(R.ev_dir().glob("prepare_*"))
        rdy_raws = [p for d in rdirs for p in d.glob("05-ready*")
                    if p.name.endswith(".raw") and
                    ".gate." not in p.name]
        last = max(rdy_raws, key=lambda p: p.stat().st_mtime)
        content = last.read_text()
        n_attempts = sum(1 for ln in content.splitlines()
                         if ln.startswith("attempt="))
        check("prepare-deadline-before-probe",
              rc_d == 2 and n_attempts == 3 and
              "probe_timeout_s=10" in content and
              "BEFORE next probe" in content and
              "attempt=1 rc=1" in content and
              "attempt=3 rc=1" in content,
              detail=f"rc={rc_d} n={n_attempts} head={content[:200]!r}")

        # ---- run-tests (CLI entry, both modes; round2 #2) ---------------
        G_candidate_orig = G.candidate_hash
        G.candidate_hash = lambda root: "f" * 64
        try:
            names = td / "two.txt"
            names.write_text("# t\nTestA\nTestB\n")
            _REL[0] = release_file(td, ["test-connector-normal",
                                        "test-connector-race",
                                        "test-dataset-race"])
            secret.write_text("POSTGRES_PASSWORD=" + "a" * 48 + "\n")
            os.chmod(secret, 0o600)
            state = {}
            rc, fake = run(["run-tests", "--execute", "--service",
                            "connector", "--test-mode", "normal",
                            "--names", str(names)],
                           rt_handler(state), env_overrides=os_env)
            check("runtests-normal-wrapper0", rc == 0)
            argv = state["run_argv"]
            check("runtests-normal-argv",
                  argv and "--pull=never" in argv and
                  "CGO_ENABLED=0" in j(argv) and "-race" not in j(argv)
                  and f"-timeout {R.GO_TEST_TIMEOUT_FLAG}" in j(argv)
                  and "-w /src/services/connector-management-service"
                  in j(argv) and "openfoundry-gomod" in j(argv) and
                  f"sha256:{GOID}" in j(argv) and
                  "./internal/repo/" in j(argv) and
                  "./internal/handlers/" not in j(argv))
            env1 = state["env_path"]
            check("runtests-unique-env-file",
                  re.fullmatch(
                      r".*openfoundry-p26-pg-test-connector-"
                      r"[0-9a-f]{8}\.env", env1) is not None and
                  not Path(env1).exists())
            state = {}
            rc, fake = run(["run-tests", "--execute", "--service",
                            "dataset", "--test-mode", "race",
                            "--names", str(names)],
                           rt_handler(state, image="golang:1.26.3",
                                      go_id=GOID_DS),
                           env_overrides=os_env)
            argv = state["run_argv"]
            check("runtests-dataset-race-argv",
                  rc == 0 and argv and "-race " in j(argv) and
                  "CGO_ENABLED=1" in j(argv) and "-w /src" in j(argv) and
                  "of-go-mod" in j(argv) and f"sha256:{GOID_DS}"
                  in j(argv) and
                  "./services/dataset-versioning-service/internal/repo/"
                  in j(argv))
            env2 = state["env_path"]
            check("runtests-env-unique-per-run", env1 != env2 and
                  not Path(env2).exists())

            # toolchain pin drift -> precheck refuse, zero run (round2 #6)
            state = {}
            rc, fake = run(["run-tests", "--execute", "--service",
                            "connector", "--test-mode", "normal",
                            "--names", str(names)],
                           rt_handler(state, go_id="c" * 64),
                           env_overrides=os_env)
            check("runtests-toolchain-drift-refused",
                  rc == 2 and state.get("run_argv") is None)

            # missing required name -> wrapper 2, original rc preserved
            bad_log = ("go version go1.25.14 linux/arm64\n"
                       "=== RUN   TestA\n--- PASS: TestA (0.1s)\n"
                       "ok  \t./internal/repo\t0.1s\n")
            state = {}
            rc, fake = run(["run-tests", "--execute", "--service",
                            "connector", "--test-mode", "normal",
                            "--names", str(names)],
                           rt_handler(state, log=bad_log),
                           env_overrides=os_env)
            gates = sorted(R.ev_dir().glob(
                "test-connector-normal_*/01-go-test*.names-gate.rc"),
                key=lambda q: q.stat().st_mtime)
            rcs = sorted([p for p in R.ev_dir().glob(
                "test-connector-normal_*/01-go-test*.rc") if
                "gate" not in p.name and "wrapper" not in p.name],
                key=lambda q: q.stat().st_mtime)
            check("runtests-missing-name-fails",
                  rc == 2 and rcs[-1].read_text() == "0\n" and
                  gates[-1].read_text() == "1\n",
                  detail=f"rc={rc} n={len(rcs)} last={rcs[-1].name}:{rcs[-1].read_text()!r} g={gates[-1].read_text()!r}")

            # no release -> refuse after read-only prechecks
            _REL[0] = str(td / "none.json")
            state = {}
            rc, fake = run(["run-tests", "--execute", "--service",
                            "connector", "--test-mode", "normal",
                            "--names", str(names)],
                           rt_handler(state), env_overrides=os_env)
            check("runtests-no-release-zero-run",
                  rc == 2 and state.get("run_argv") is None)
        finally:
            G.candidate_hash = G_candidate_orig

        # ---- unified suite + expect pin (pure, fake runner) -------------
        _stable = G.candidate_hash
        G.candidate_hash = lambda root: "f" * 64
        _REL[0] = release_file(td, ["test-unified-pipeline-normal",
                                    "test-unified-connector-normal"])
        state = {}
        rc, fake = run(["run-tests", "--execute", "--suite", "unified",
                        "--service", "pipeline", "--test-mode", "normal",
                        "--names", str(names)], rt_handler(
                            state, image="golang:1.26.3", go_id=GOID_DS),
                       env_overrides=os_env)
        argv = state.get("run_argv")
        check("unified-pipeline-argv",
              rc == 0 and argv and
              "./services/pipeline-build-service/internal/postgres/" in
              j(argv) and
              f"sha256:{GOID_DS}" in j(argv) and
              "openfoundry-gomod" in j(argv))
        env_used = state.get("env_path")
        env_content = Path(env_used).read_text() if False else None
        udbs = sorted(R.ev_dir().glob("test-unified-pipeline-*/00b-fresh-db*"))
        check("unified-fresh-db-steps",
              bool(udbs) and any("CREATE DATABASE ofp26_unified_" in
                                 q.read_text() for q in udbs
                                 if q.suffix == ".cmd"))
        # expect mismatch -> precheck refusal, zero run
        state = {}
        rc, fake = run(["run-tests", "--execute", "--suite", "unified",
                        "--service", "connector", "--test-mode", "normal",
                        "--names", str(names),
                        "--expect-source-sha", "0" * 64],
                       rt_handler(state, image="golang:1.26.3",
                                  go_id=GOID_DS), env_overrides=os_env)
        check("unified-expect-mismatch-refused",
              rc == 2 and state.get("run_argv") is None)
        # expect ok -> wrapper 0 (uses patched stable hash)
        state = {}
        rc, fake = run(["run-tests", "--execute", "--suite", "unified",
                        "--service", "connector", "--test-mode", "normal",
                        "--names", str(names),
                        "--expect-source-sha", "f" * 64],
                       rt_handler(state, image="golang:1.26.3",
                                  go_id=GOID_DS), env_overrides=os_env)
        check("unified-expect-ok-green", rc == 0)
        un = sorted(R.ev_dir().glob(
            "test-unified-connector-*/01-go-test*.gates.json"),
            key=lambda q: q.stat().st_mtime)
        gates_u = json.loads(un[-1].read_text())
        check("unified-gates-carry-expect",
              gates_u["expect_pin_ok"] is True and
              gates_u["expect_source_sha256"] == "f" * 64,
              detail=f"file={un[-1].name} keys={sorted(gates_u)}")
        G.candidate_hash = _stable

        # ---- down --------------------------------------------------------
        _REL[0] = release_file(td, ["down"])

        def down_handler(state, ct_label="p26-pg-isolated"):
            def h(argv):
                s = j(argv)
                if "network inspect of-p26-pg-net --format" in s:
                    return 0, "p26-pg-isolated\n"
                if "inspect of-p26-pg --format" in s and \
                        "Config.Labels" in s:
                    return 0, ct_label + "\n"
                if s == "docker rm -f of-p26-pg":
                    state["rm"] = True
                    return 0, "of-p26-pg\n"
                if s == "docker network rm of-p26-pg-net":
                    state["netrm"] = True
                    return 0, "of-p26-pg-net\n"
                raise AssertionError(f"unexpected {argv}")
            return h

        state = {}
        rc, fake = run(["down"], down_handler(state, ct_label="other"))
        check("down-label-mismatch-zero-writes",
              rc == 2 and "rm" not in state and "netrm" not in state)
        state = {}
        rc, fake = run(["down", "--execute"], down_handler(state))
        check("down-happy", rc == 0 and state.get("rm") and
              state.get("netrm"))

        # ---- check-log (pure) -------------------------------------------
        G_candidate_orig = G.candidate_hash
        good = ("go version go1.25.14 linux/arm64\n"
                "=== RUN   TestA\n--- PASS: TestA\n"
                "=== RUN   TestB\n--- PASS: TestB\nok  \tp\t0s\n")
        logf = td / "g.log"
        logf.write_text(good)
        names = td / "two.txt"
        rc = R.main(["check-log", "--log", str(logf), "--names",
                     str(names)])
        check("check-log-good", rc == 0)
        badf = td / "b.log"
        badf.write_text(good.replace("=== RUN   TestB\n--- PASS: "
                                     "TestB\n", ""))
        rc = R.main(["check-log", "--log", str(badf), "--names",
                     str(names)])
        check("check-log-missing-fails", rc == 1)

    os.environ.pop("P26_PG_EV_DIR", None)
    npass = sum(1 for _, ok, _ in RESULTS if ok)
    print(f"[pg-runner] {npass}/{len(RESULTS)} PASS")
    return 0 if npass == len(RESULTS) else 1


_T0 = 0
if __name__ == "__main__":
    import time as _t
    _T0 = _t.time()
    sys.exit(main())

#!/usr/bin/env python3
"""Targeted pure adapter/shim checks; all guard/process IO is injected."""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

HERE = Path(__file__).resolve().parent


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    adapter = load("adapter_pure", HERE / "p25_old_suite_adapter.py")
    shim = load("shim_pure", HERE / "p23_q_guard_shim_p25.py")
    checks = []

    def check(name, condition):
        assert condition, name
        checks.append(name)
        print("ok: " + name)

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        selected = json.loads((HERE / "results/2026-11-14-p25-final/selected-build.json").read_text())
        selected_path = tmp / "selected.json"
        selected_path.write_text(json.dumps(selected))
        record = {"schema": "p25-deployment-record-v1",
                  "connector_boot": "2026-11-14T16:19:08.688601428Z",
                  "connector_image": selected["images"]["connector"],
                  "source_sha256": adapter.CANDIDATE_SOURCE_SHA,
                  "selected_build_path": str(selected_path),
                  "selected_build_sha256": adapter.sha256(selected_path)}
        rec_path = tmp / "record.json"
        rec_path.write_text(json.dumps(record))
        check("record/selected exact bytes accepted", adapter.verify_deployment_record(rec_path, selected_path) == record)

        def refuses(rec, sel):
            with contextlib.redirect_stderr(io.StringIO()):
                try:
                    adapter.verify_deployment_record(rec, sel)
                except SystemExit:
                    return True
            return False

        check("missing record rejected", refuses(None, selected_path))
        check("missing selected-build rejected", refuses(rec_path, None))
        for key, value in [("schema", "wrong"), ("source_sha256", "0" * 64),
                           ("connector_image", "sha256:bad"),
                           ("connector_boot", "not-a-time"),
                           ("selected_build_sha256", "0" * 64),
                           ("selected_build_path", str(tmp / "elsewhere"))]:
            bad = tmp / "bad-record.json"
            bad.write_text(json.dumps(dict(record, **{key: value})))
            check("record " + key + " drift rejected", refuses(bad, selected_path))
        for value in [None, []]:
            bad = tmp / "bad-record.json"
            bad.write_text(json.dumps(value))
            check("non-object record rejected " + repr(value), refuses(bad, selected_path))
        for mutate, label in [
                (lambda x: x.update(ok=False), "not green"),
                (lambda x: x.update(source_sha256="0" * 64), "wrong source"),
                (lambda x: x["images"].pop("python-3.12-alpine"), "incomplete images"),
                (lambda x: x["images"].update(connector="bad"), "not full image")]:
            badsel = json.loads(json.dumps(selected)); mutate(badsel)
            selected_path.write_text(json.dumps(badsel))
            badrec = dict(record, selected_build_sha256=adapter.sha256(selected_path))
            rec_path.write_text(json.dumps(badrec))
            check("selected-build " + label + " rejected even with matching SHA", refuses(rec_path, selected_path))
        selected_path.write_text(json.dumps(selected)); rec_path.write_text(json.dumps(record))
        selected_path.write_text(json.dumps(selected) + "\n")
        check("selected byte-only drift rejected", refuses(rec_path, selected_path))
        selected_path.write_text(json.dumps(selected))

        env = {"P25_SHIM_POLICY": "steady", "P25_TARGET_BOOT": record["connector_boot"],
               "P25_TARGET_IMAGE": record["connector_image"],
               "P25_DEPLOYMENT_RECORD": str(rec_path), "P25_SELECTED_BUILD": str(selected_path)}

        class Guard:
            RELEASED_FOR_EXECUTION = True
            calls = []

            def main(self):
                self.calls.append(list(sys.argv))
                return 0

        guard = Guard()

        class FakeProcessIO:
            """Calls actual shim with injected guard; never starts a process."""
            calls = []

            def run(self, argv, *args, **kwargs):
                self.calls.append((argv, kwargs))
                if argv[2] == str(adapter.SHIM):
                    old = sys.argv
                    try:
                        sys.argv = argv[2:]
                        rc = shim.main(guard=guard, env=kwargs["env"])
                    finally:
                        sys.argv = old
                    return subprocess.CompletedProcess(argv, rc, "injected guard stdout", "")
                return subprocess.CompletedProcess(argv, 7, "passthrough", "")

        real = FakeProcessIO(); state = []
        facade = adapter.GuardSubstitute(real, state, env)
        argv = ["python3", "-I", str(adapter.P23_GUARD), "/tmp/pure-no-docker", "connector"]
        os.environ["P25_PURE_PARENT_SENTINEL"] = "parent"
        try:
            for supplied in [None, {"P25_PURE_SUITE_SENTINEL": "suite", "P25_TARGET_BOOT": "wrong"}]:
                kwargs = {} if supplied is None else {"env": supplied}
                proc = facade.run(argv, capture_output=True, text=True, **kwargs)
                child = real.calls[-1][1]["env"]
                check("guard inherits PATH/parent and record wins " + str(supplied is None),
                      proc.returncode == 0 and child.get("PATH") == os.environ.get("PATH")
                      and child["P25_PURE_PARENT_SENTINEL"] == "parent"
                      and child["P25_TARGET_BOOT"] == record["connector_boot"]
                      and (supplied is None or child["P25_PURE_SUITE_SENTINEL"] == "suite"))
            check("real guard output hashes archived", state[0]["stdout_sha256"] == hashlib.sha256(b"injected guard stdout").hexdigest())
            check("non-guard argv unchanged", facade.run(["pure", "no", "io"]).returncode == 7)

            # Actual adapter.main -> fake frozen suite -> actual shim -> injected guard.
            adapter.RELEASED_FOR_EXECUTION = True; adapter.subprocess = real
            for suite, count, wanted_rc in [("smoke_p23.py", 1, 0), ("smoke_p24.py", 2, 0), ("smoke_p24.py", 0, 1)]:
                # Separate paths avoid Python bytecode cache reuse when
                # equal-length fixture source changes within one second.
                suite_dir = tmp / f"{suite}-{count}"
                suite_dir.mkdir()
                (suite_dir / suite).write_text("import subprocess\ndef main():\n"
                    + "    for _ in range(" + str(count) + "):\n"
                    + "        p = subprocess.run(" + repr(argv) + ", capture_output=True, text=True)\n"
                    + "        assert p.returncode == 0\n    return 0\n")
                old = sys.argv
                sys.argv = ["adapter", "--suite", suite, "--suite-dir", str(suite_dir),
                            "--deployment-record", str(rec_path), "--selected-build", str(selected_path),
                            "--record-dir", str(tmp / "records"), "--", "--env-dir", "/tmp/pure-no-docker"]
                try:
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                        rc = adapter.main()
                finally:
                    sys.argv = old
                check(f"adapter.main exact count {suite}/{count}", rc == wanted_rc)
            for changed in [{"P25_DEPLOYMENT_RECORD": ""}, {"P25_SELECTED_BUILD": ""}, {"P25_TARGET_BOOT": "wrong"}, {"P25_SHIM_POLICY": "first-transition"}]:
                old = sys.argv; before = len(guard.calls); sys.argv = ["shim", "/tmp/pure", "connector"]
                try:
                    with contextlib.redirect_stderr(io.StringIO()):
                        rc = shim.main(guard=guard, env={**env, **changed})
                finally:
                    sys.argv = old
                check("shim refuses before IO " + next(iter(changed)), rc == 2 and len(guard.calls) == before)
        finally:
            os.environ.pop("P25_PURE_PARENT_SENTINEL", None)
    print(f"adapter binding pure checks: {len(checks)} passed; zero subprocess/HTTP/Docker IO")
    return 0


if __name__ == "__main__":
    sys.exit(main())

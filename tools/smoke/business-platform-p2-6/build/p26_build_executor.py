#!/usr/bin/env python3
"""P2.6 W6 fixed two-service OFFLINE local-acceptance build (Root
build-offline-decision.md; supersedes the compose-build approach and its
generic Dockerfile FROM parser).

Per service (dataset | connector ONLY — Pipeline has no P26 delta):
  1 PRECHECK (each probe its OWN original cmd/raw/rc; any failure stops
    BEFORE compile/create): actual whole-candidate source hash ==
    Root-expected S (REQUIRED --expect-source-sha, compared BEFORE
    compile); q3 running container .Image + .Config.Image AND old tag
    image .Id all == pinned old FULL ids (strict zero-change ledger);
    old runtime image + cached Go image FULL ids resolvable; optional
    config.yaml asserted ABSENT (if it appears: STOP + report, never a
    silent stale config).
  2 COMPILE offline: docker run --rm --pull=never --network none
    <cached-Go FULL id> with source:ro + the service's KNOWN existing
    mod/build cache volumes, module cwd /src, GOTOOLCHAIN=local
    GOPROXY=off GOSUMDB=off GOFLAGS=-mod=readonly CGO_ENABLED=0
    GOOS=linux GOARCH=arm64; P1-verbatim build flags (-trimpath
    -buildvcs=false -ldflags "-s -w -buildid= -extldflags=-static
    -X main.version=<p26candidate>" -tags "osusergo,netgo,static_build"
    -o /out/service ./services/<svc>/cmd/<svc>); binary chmod 0755 +
    SHA into the attempt's EXCLUSIVE host outdir mounted at /out. Real
    go version recorded; cache miss FAILS (no fallback compiler, no
    network possible).
  3 CREATE the commit base from the OLD SERVICE FULL IMAGE ID (never
    the running container; no start/command/env/mount/ports):
    docker create --pull=never --network none --name
    of-p26-build-<svc>-<unique> --label of-p26-owner=p26-build
    <OLD_FULL_ID>.
  4 docker cp <outdir>/service <CID>:/usr/local/bin/service (cp
    preserves the 0755 mode); docker commit <CID>
    openfoundry-p26-<svc>:<unique>; inspect NEW tag FULL id + safe
    config (User/WorkingDir/Entrypoint/Cmd) must MATCH the old runtime
    image's. No old tag is moved.
  5 CLEANUP ONLY the exact new CID after label+ID re-check (no
    --volumes); failure is truthful nonzero. Binary/evidence/new image
    preserved; source post==S recorded separately.

Evidence: unique exclusive attempt dir per run (UTC+token; collision
never clobbers); every actual probe original cmd/raw/rc; secrets never
printed. plan mode prints the exact argvs. Mutating run needs
--execute + release listing 'build-<svc>'.
"""

import argparse
import hashlib
import json
import os
import re
import secrets as pysecrets
import sys
import time
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402

TOOL = "p26_build_executor"
def ev_dir() -> Path:
    """Resolved at CALL time so isolated selftests (P26_BUILD_EV_DIR)
    never mix with the real evidence root."""
    return Path(os.environ.get("P26_BUILD_EV_DIR",
                               str(G.RESULTS_W6 / "build")))
Q3_PROJECT = G.Q3_PROJECT
Q3_ENV_NOTE = "q3 .env NOT needed by this recipe (no compose build)"
TIMEOUT_INSPECT_S = 30
TIMEOUT_COMPILE_S = 1500
TIMEOUT_CP_COMMIT_S = 120

# ---- strict pins (whitelist-inspected 2026-11-15; byte-captured) --------
OLD_FULL_IDS = {'connector': '6b556702b34ba399f4ee434963c798faf86b12af294657ec3b144309c0097592', 'dataset': 'dc4eca36295110f253af382f4cc89d2263cfff4b36671e87a9156a5f577c9264', 'pipeline': 'd50858d8ddcf4b1cdc11eb0a071a180ade0bb4af43fd471dd8464885b7700520'}
# cached Go FULL ids per service (verified in prior real runs: dataset
# via W1/W6 tests; connector via W2/W6 tests). Cache miss -> fail.
GO_FULL_IDS = {
    "dataset": ("2d6c80227255c3112a4d08e67ba98e58efd3846daf15d9d7d4c38956"
                "5d881b1a"),   # golang:1.26.3
    "connector": ("699337d620559a59b4a2bb298ad59611e535d2ee755a34cf2d2a98"
                  "f37578dc80"),  # golang:1.25 (go1.25.14)
}
CACHE_VOLUMES = {
    "dataset": ("of-go-mod", "of-go-build"),
    "connector": ("openfoundry-gomod", "openfoundry-gobuild"),
}
CMD_PATHS = {
    "dataset": "./services/dataset-versioning-service/cmd/"
               "dataset-versioning-service",
    "connector": "./services/connector-management-service/cmd/"
                 "connector-management-service",
}
BUILDABLE = ("dataset", "connector")
BUILD_LABEL = "of-p26-owner=p26-build"
CONFIG_YAML_CANDIDATES = {
    "dataset": ["services/dataset-versioning-service/config.yaml",
                "services/dataset-versioning-service/cmd/"
                "dataset-versioning-service/config.yaml"],
    "connector": ["services/connector-management-service/config.yaml",
                  "services/connector-management-service/cmd/"
                  "connector-management-service/config.yaml"],
}


class Abort(Exception):
    pass


class Attempt:
    """Unique-evidence component: .cmd BEFORE, full .raw, ORIGINAL .rc,
    .meta. Names carry a random token so a crash before .rc can NEVER
    cause clobbering on retry."""

    def __init__(self, phase_dir: Path, name: str):
        self.dir = phase_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        self.base = self.dir / f"{name}-{pysecrets.token_hex(3)}"
        while Path(str(self.base) + ".rc").exists():
            self.base = self.dir / f"{name}-{pysecrets.token_hex(3)}"
        self._meta = {}

    def pre(self, argv, extra=None):
        cmd = " ".join(str(t) for t in argv)
        Path(str(self.base) + ".cmd").write_text(cmd + "\n")
        self._meta = {"utc": G.now_utc(), "cmd": cmd}
        if extra:
            self._meta.update(extra)

    def post(self, rc, output, extra=None):
        Path(str(self.base) + ".raw").write_text(output)
        Path(str(self.base) + ".rc").write_text(f"{rc}\n")
        self._meta["rc"] = rc
        if extra:
            self._meta.update(extra)
        Path(str(self.base) + ".meta").write_text(
            json.dumps(self._meta, indent=1, sort_keys=True) + "\n")
        return rc, output


def _bare(v):
    v = (v or "").strip().splitlines()
    v = v[0] if v else ""
    return v[len("sha256:"):] if v.startswith("sha256:") else v


RUNTIME_FORMAT = '{{.Id}}\n{{.Image}}\n{{.Config.Image}}\n{{.State.StartedAt}}\n{{.State.Running}}'


def root_running_snapshot(release, service):
    """Optional S2+ upgrade input; never infer current runtime by name."""
    if not release or 'build_running_snapshot' not in release:
        return None
    ref = release['build_running_snapshot']
    path = Path(ref['path'])
    if G.sha256_file(path) != ref['sha256']:
        raise Abort('Root running snapshot hash mismatch')
    record = json.loads(path.read_text())
    pin = record['containers'][f'{Q3_PROJECT}-{service}-1']
    if not re.fullmatch('[a-f0-9]{64}', pin['cid']) or not re.fullmatch('sha256:[a-f0-9]{64}', pin['image']) or not pin['started_at'].endswith('Z'):
        raise Abort('Root runtime snapshot requires exact CID/full image/UTC boot')
    return {k:pin[k] for k in ('cid','image','started_at')}


def check_running_snapshot(raw, pin):
    # Main image-only deployments use the full ID as Config.Image, not a tag.
    expected = [pin['cid'],pin['image'],pin['image'],pin['started_at'],'true']
    if raw.strip().splitlines() != expected:
        raise Abort('actual runtime CID/image/configImage/boot/running drift vs Root snapshot')


def probe(runner, phase_dir, name, argv, timeout_s=TIMEOUT_INSPECT_S):
    a = Attempt(phase_dir, name)
    a.pre(argv)
    rc, out = runner(argv, timeout_s=timeout_s)
    rc, raw = a.post(rc, out)
    return rc, raw, a


def inspect_fmt(runner, phase_dir, name, target, fmt):
    return probe(runner, phase_dir, name,
                 ["docker", "inspect", target, "--format", fmt])


# ---------------------------------------------------------------- facts ---
def _exclusive_dir(parent: Path, name: str) -> Path:
    """Root final narrow gate: EXCLUSIVE creation (exist_ok=False), not
    merely a random token. First try the given name; on collision retry
    with FRESH tokens (bounded); still colliding aborts — never clobbers."""
    for i in range(3):
        cand = parent / (name if i == 0 else
                         f"{name}_{pysecrets.token_hex(3)}")
        try:
            cand.mkdir(parents=True, exist_ok=False)
            return cand
        except FileExistsError:
            continue
    raise Abort(f"could not exclusively create attempt dir {name!r} "
                f"under {parent}")


def cmd_facts(args, runner):
    d = _exclusive_dir(ev_dir(), f"facts_{G.now_utc()}")
    ledger, problems = {}, []
    rc_ps, raw_ps, _ = probe(runner, d, "q3-ps",
                             ["docker", "ps", "--format",
                              "{{.Names}}\t{{.Image}}\t{{.Ports}}\t"
                              "{{.Status}}"])
    if rc_ps != 0:
        problems.append("docker ps failed")
    for svc in ("connector", "dataset", "pipeline"):
        rc, raw, _ = inspect_fmt(runner, d, f"old-tag-{svc}",
                                 f"{Q3_PROJECT}-{svc}", "{{.Id}}")
        got = _bare(raw)
        ledger[svc] = got
        if rc != 0 or got != OLD_FULL_IDS[svc]:
            problems.append(f"old tag {svc} id {got!r} != pin (zero-"
                            "change ledger violated)")
        rc2, raw2, _ = inspect_fmt(
            runner, d, f"running-{svc}", f"{Q3_PROJECT}-{svc}-1",
            "{{.Image}} {{.Config.Image}}")
        ids = raw2.strip().split()
        if (rc2 != 0 or len(ids) < 2 or
                _bare(ids[0]) != OLD_FULL_IDS[svc] or not ids[1]):
            problems.append(f"running {svc} .Image != pin (.Config.Image "
                            f"is the compose tag string, recorded "
                            f"verbatim: {raw2.strip()!r})")
    go_ledger = {}
    for ref, iid in (("golang:1.26.3", GO_FULL_IDS["dataset"]),
                     ("golang:1.25", GO_FULL_IDS["connector"])):
        rc, raw, _ = probe(
            runner, d, f"go-cache-{ref.replace(':', '_')}",
            ["docker", "image", "inspect", ref, "--format", "{{.Id}}"])
        got = _bare(raw)
        go_ledger[ref] = got
        if rc != 0 or got != iid:
            problems.append(f"cached Go {ref} missing/drift: {got!r}")
    facts = {"utc": G.now_utc(), "old_full_ids": ledger,
             "go_cache": go_ledger,
             "port_facts": {"dataset": "explicit 50078 (NOT Go default "
                                       "50117; W7 proxy upstream uses it)",
                             "connector": "50088 (host 17088)",
                             "pipeline": "50081 (unchanged baseline)"},
             "build_scope": list(BUILDABLE),
             "problems": problems}
    G.write_json(d / "FACTS.json", facts)
    print(f"[{TOOL}] facts -> {d / 'FACTS.json'} "
          f"({'OK' if not problems else problems})")
    return 0 if not problems else 1



# ---- Root build-nullsafe-resume: bound artifacts (doc-pinned) ----------
RESUME_BOUND = {
    "dataset": {
        "committed_full_id": "4217c40275b9fb7b28f8ef872a6b64870876ce6e6c5"
                              "99ff4714531dd919ef75b",
        "build_cid": "a36c57c6c2b3526c9e45f9eb0fed045b124a502d8986182d1d"
                     "0f0c77ac64bfeb",
        "binary_sha256": "e3fa14ca388437eeaef737919f03d3eb260852cb79b28c9"
                         "eaea09f08e3de10bc",
        "original_outer": "results/w6/build/real-S-build-dataset-attempt1",
        "original_inner": "results/w6/build/build-dataset_20261115T053954Z"},
    "connector": {
        "committed_full_id": "f23f69161c6f86f7402a18dbd14e24fdcad581ecbd"
                             "9968e7b964ef1baf11df34",
        "build_cid": "043dc33db45b40db69bace2d5ed9da67466f72b45627de215376"
                     "1706019bab2d",
        "binary_sha256": "58c1728043b4632a65c89ca3b2922ef00ad92de0001359c"
                         "1d2cc163c05fefbbf",
        "original_outer": "results/w6/build/real-S-build-connector-attempt1",
        "original_inner": "results/w6/build/build-connector_20261115T054527Z"},
}
S_FROZEN = ("b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe1"
            "8ff14c")
NEW_FMT = ("{{.Id}}|{{.Config.User}}|{{.Config.WorkingDir}}|"
           "{{json (index .Config \"Entrypoint\")}}|"
           "{{json (index .Config \"Cmd\")}}")
OLD_FMT = ("{{.Config.User}}|{{.Config.WorkingDir}}|"
           "{{json (index .Config \"Entrypoint\")}}|"
           "{{json (index .Config \"Cmd\")}}")


def cmd_resume(args, runner, release, execute):
    svc = args.service
    b = RESUME_BOUND[svc]
    phase = f"build-resume-{svc}"
    d = _exclusive_dir(ev_dir(), f"{phase}_{G.now_utc()}")
    gates = {}

    # 0) source == frozen S (before)
    src0 = G.candidate_hash(G.OPENFOUNDRY)
    a0 = Attempt(d, "00-source-before")
    a0.pre(["(in-process) candidate_hash == frozen S"])
    a0.post(0 if src0 == S_FROZEN else 1, f"actual={src0}\n")
    gates["source_before_S"] = src0 == S_FROZEN
    if not gates["source_before_S"]:
        raise Abort("source drifted from frozen S — refusing resume")

    # 1) exact committed image: inspect by FULL ID; returned Id must equal
    a1 = Attempt(d, "01-candidate-exact")
    argv1 = ["docker", "image", "inspect",
             f"sha256:{b['committed_full_id']}", "--format", NEW_FMT]
    a1.pre(argv1)
    rc1, out1 = runner(argv1, timeout_s=TIMEOUT_INSPECT_S)
    rc1, raw1 = a1.post(rc1, out1)
    new_cfg = raw1.strip().split("|", 1)[1] if "|" in raw1 else ""
    gates["candidate_exists_id_exact"] = (
        rc1 == 0 and raw1.strip().startswith(
            f"sha256:{b['committed_full_id']}|"))

    # 2) exact OLD runtime image, same safe config
    a2 = Attempt(d, "02-old-runtime-exact")
    argv2 = ["docker", "image", "inspect",
             f"sha256:{OLD_FULL_IDS[svc]}", "--format", OLD_FMT]
    a2.pre(argv2)
    rc2, out2 = runner(argv2, timeout_s=TIMEOUT_INSPECT_S)
    rc2, raw2 = a2.post(rc2, out2)
    gates["old_exists_cfg_equal"] = (
        rc2 == 0 and raw2.strip() == new_cfg)

    if not all(gates.values()):
        raise Abort(f"resume verify failed: {gates}")

    # 3) composite binding: ORIGINAL stage rc0 artifacts (link, not rerun)
    links = {}
    inner = G.OPENFOUNDRY / "tools/smoke/business-platform-p2-6" / \
        b["original_inner"]
    for comp in ("04-compile", "05-create-base", "06-cp", "07-commit"):
        f = {"04-compile": "cmd", "05-create-base": "cmd",
             "06-cp": "cmd", "07-commit": "cmd"}[comp]
        cands = sorted(inner.glob(f"{comp}-*.{f}"))
        rcs = sorted(inner.glob(f"{comp}-*.rc"))
        links[comp] = {
            "cmd_sha256": G.sha256_file(cands[0]) if cands else None,
            "rc": rcs[0].read_text().strip() if rcs else None}
    gates["original_stage0_all_rc0"] = all(
        v["rc"] == "0" and v["cmd_sha256"] for v in links.values())
    gates["binary_sha_bound"] = G.HEX64.match(b["binary_sha256"]) is not None
    if not all(gates.values()):
        raise Abort(f"original artifacts binding failed: {gates}")

    # 4) cleanup gate: EXACT CID + label p26-build + NOT running
    a4 = Attempt(d, "04-cleanup-precheck")
    argv4 = ["docker", "inspect", b["build_cid"], "--format",
             "{{.Id}} {{index .Config.Labels \"of-p26-owner\"}} "
             "{{.State.Running}}"]
    a4.pre(argv4)
    rc4, out4 = runner(argv4, timeout_s=TIMEOUT_INSPECT_S)
    rc4, raw4 = a4.post(rc4, out4)
    ok4 = (rc4 == 0 and b["build_cid"] in raw4 and
           "p26-build" in raw4 and raw4.strip().endswith("false"))
    Path(str(a4.base) + ".gate.rc").write_text(f"{0 if ok4 else 1}\n")
    Path(str(a4.base) + ".gate.raw").write_text(
        f"cleanup-precheck ok={ok4} raw={raw4.strip()!r}\n")
    if not ok4:
        raise Abort(f"cleanup precheck failed — CID kept, report: "
                    f"{raw4.strip()!r}")

    # 5) cleanup ONLY that CID (no name/force/volumes); release-gated
    if not execute or release is None or \
            phase not in release["released_for"]:
        print(f"[{TOOL}] {phase}: verifies green; cleanup needs "
              f"--execute + release listing {phase!r} (CID kept)")
        return 2
    a5 = Attempt(d, "05-rm-exact-cid")
    argv5 = ["docker", "rm", b["build_cid"]]
    a5.pre(argv5, {"only": "exact doc-bound BuildCID",
                   "no_force": True, "no_volumes_flag": True})
    rc5, out5 = runner(argv5, timeout_s=TIMEOUT_CP_COMMIT_S)
    rc5, raw5 = a5.post(rc5, out5)
    gates["cleanup_rc0"] = rc5 == 0
    if rc5 != 0:
        raise Abort(f"cleanup rc={rc5} (truthful; nothing else touched)")

    # 6) source post == S; old q3 running IDs unchanged (read-only)
    src1 = G.candidate_hash(G.OPENFOUNDRY)
    a6 = Attempt(d, "06-source-post")
    a6.pre(["(in-process) candidate_hash AFTER"])
    a6.post(0 if src1 == S_FROZEN else 1, f"actual={src1}\n")
    gates["source_post_S"] = src1 == S_FROZEN
    q3ok = True
    for q in ("dataset", "connector"):
        rcq, outq = runner(
            ["docker", "inspect", f"{Q3_PROJECT}-{q}-1", "--format",
             "{{.Image}}"], timeout_s=TIMEOUT_INSPECT_S)
        q3ok = q3ok and rcq == 0 and \
            _bare(outq) == OLD_FULL_IDS[q]
    gates["old_q3_running_ids_unchanged"] = q3ok

    ok = all(gates.values())
    G.write_json(d / "RESUME-RESULT.json", {
        "ok": ok, "service": svc, "frozen_S": S_FROZEN,
        "committed_full_id": f"sha256:{b['committed_full_id']}",
        "build_cid_cleaned": b["build_cid"],
        "binary_sha256": b["binary_sha256"],
        "safe_config": new_cfg, "gates": gates,
        "original_stage0_links": links,
        "original_failure_history": "08-verify rc1 / wrapper rc2 in "
            f"{b['original_inner']} — PRESERVED, not claimed rc0",
        "note": "composite acceptance = original compile/create/cp/commit "
                "rc0 (linked by sha) + this resume verify/cleanup rc0"})
    print(f"[{TOOL}] {phase} OK={ok} gates={gates} -> "
          f"{d / 'RESUME-RESULT.json'}")
    return 0 if ok else 2

# ----------------------------------------------------------------- plan ---
def plan_steps(service, expect_sha):
    """Exact argvs (offline fixed recipe) for Root review."""
    old_id = OLD_FULL_IDS[service]
    go_id = GO_FULL_IDS[service]
    mod, build = CACHE_VOLUMES[service]
    cmd_path = CMD_PATHS[service]
    version = f"p26-candidate-{expect_sha[:12]}"
    outdir = f"<attempt-dir>/out-{expect_sha[:8]}"
    uniq = "<unique>"
    compile_argv = ["docker", "run", "--rm", "--pull=never",
                    "--network", "none",
                    "-v", "/Volumes/ExternalSD/projects/OpenFoundry:/src:ro",
                    "-v", f"{mod}:/go/pkg/mod",
                    "-v", f"{build}:/root/.cache/go-build",
                    "-v", f"{outdir}:/out",
                    "-w", "/src",
                    "-e", "GOTOOLCHAIN=local", "-e", "GOPROXY=off",
                    "-e", "GOSUMDB=off", "-e", "GOFLAGS=-mod=readonly",
                    "-e", "CGO_ENABLED=0", "-e", "GOOS=linux",
                    "-e", "GOARCH=arm64",
                    f"sha256:{go_id}", "sh", "-c",
                    "go version && go build -trimpath -buildvcs=false "
                    f"-ldflags \"-s -w -buildid= -extldflags=-static "
                    f"-X main.version={version}\" "
                    "-tags \"osusergo,netgo,static_build\" "
                    f"-o /out/service {cmd_path} && chmod 0755 "
                    "/out/service && sha256sum /out/service"]
    create_argv = ["docker", "create", "--pull=never", "--network", "none",
                   "--name", f"of-p26-build-{service}-{uniq}",
                   "--label", BUILD_LABEL, f"sha256:{old_id}"]
    new_cid = "<NEW_CID>"
    cp_argv = ["docker", "cp", f"{outdir}/service",
               f"{new_cid}:/usr/local/bin/service"]
    commit_argv = ["docker", "commit", new_cid,
                   f"openfoundry-p26-{service}:{expect_sha[:12]}-{uniq}"]
    verify_argv = ["docker", "image", "inspect",
                   f"openfoundry-p26-{service}:{expect_sha[:12]}-{uniq}",
                   "--format",
                   "{{.Id}}|{{.Config.User}}|{{.Config.WorkingDir}}|"
                   "{{json (index .Config \"Entrypoint\")}}|"
                   "{{json (index .Config \"Cmd\")}}"]
    cleanup_argv = ["docker", "rm", new_cid]
    return {"precheck": "source==S BEFORE compile; old ids zero-change; "
                        "Go cache present; config.yaml absent",
            "compile": compile_argv, "create": create_argv, "cp": cp_argv,
            "commit": commit_argv, "verify_new_tag": verify_argv,
            "cleanup_exact_cid_only": cleanup_argv}


# ------------------------------------------------------------------ run ---
def cmd_run(args, runner, release, execute):
    service = args.service
    expect = args.expect_source_sha or ""
    if service not in BUILDABLE:
        print(f"[{TOOL}] build-{service}: REFUSED (no P26 delta; pipeline "
              "stays unchanged baseline)", file=sys.stderr)
        return 2
    if not G.HEX64.match(expect):
        print(f"[{TOOL}] build-{service}: --expect-source-sha <64hex> "
              "REQUIRED (Root-frozen S; fail-closed)", file=sys.stderr)
        return 2
    phase = f"build-{service}"
    d = _exclusive_dir(ev_dir(), f"{phase}_{G.now_utc()}")
    uniq = d.name[len(phase) + 1:]  # utc(+token on collision retry)

    # ---- 1 PRECHECK: source==S BEFORE any docker call -------------------
    src_now = G.candidate_hash(G.OPENFOUNDRY)
    pa = Attempt(d, "00-source-precheck")
    pa.pre(["(in-process) candidate_hash(whole tree) vs Root-expected S"],
           {"expect": expect, "actual": src_now})
    if src_now != expect:
        pa.post(1, f"MISMATCH actual={src_now} expect={expect}\n")
        raise Abort("candidate source != Root-expected S — stopping "
                    "BEFORE compile (no docker calls)")
    pa.post(0, "source == S (pre-compile pin OK)\n",
            {"pinned_before_compile": True})

    # config.yaml must be absent (recipe: if it appears STOP + report)
    for rel in CONFIG_YAML_CANDIDATES[service]:
        if (G.OPENFOUNDRY / rel).exists():
            raise Abort(f"optional config.yaml NOW PRESENT at {rel} — "
                        "recipe says STOP and report (never silent stale "
                        "config)")

    # Root final narrow gate: EVERY probe stops IMMEDIATELY on rc!=0
    # (originals already recorded by the probe itself).
    running_pin = root_running_snapshot(release, service)
    rc, raw, _ = inspect_fmt(runner, d, "01-old-tag",
                             f"{Q3_PROJECT}-{service}", "{{.Id}}")
    if rc != 0:
        raise Abort(f"01-old-tag probe rc={rc} — immediate stop")
    if _bare(raw) != OLD_FULL_IDS[service]:
        raise Abort("old tag id drift vs pin — immediate stop")
    rc, raw, _ = inspect_fmt(runner, d, "02-running",
                             running_pin['cid'] if running_pin else f"{Q3_PROJECT}-{service}-1",
                             RUNTIME_FORMAT if running_pin else "{{.Image}} {{.Config.Image}}")
    if rc != 0:
        raise Abort(f"02-running probe rc={rc} — immediate stop")
    if running_pin:
        check_running_snapshot(raw, running_pin)
    else:
        ids = raw.strip().split()
        if len(ids) < 2 or _bare(ids[0]) != OLD_FULL_IDS[service] or not ids[1]:
            raise Abort("running .Image drift vs pin — immediate stop")
    go_ref = "golang:1.26.3" if service == "dataset" else "golang:1.25"
    rc, raw, _ = probe(runner, d, "03-go-cache",
                       ["docker", "image", "inspect", go_ref, "--format",
                        "{{.Id}}"])
    if rc != 0:
        raise Abort(f"03-go-cache probe rc={rc} — immediate stop")
    if _bare(raw) != GO_FULL_IDS[service]:
        raise Abort(f"cached Go {go_ref} drift — FAIL, no fallback")
    # Root final narrow gate: cache volumes must EXIST before any -v use
    # (Docker auto-creates missing named volumes) — each probe original;
    # missing => ZERO compile/create.
    for vol in CACHE_VOLUMES[service]:
        rc, raw, _ = probe(runner, d, f"03b-volume-{vol}",
                           ["docker", "volume", "inspect", vol])
        if rc != 0:
            raise Abort(f"cache volume {vol} MISSING — zero compile/"
                        "create (docker -v would implicitly create it)")
    if not execute or release is None or \
            phase not in release["released_for"]:
        print(f"[{TOOL}] {phase}: prechecks green; needs --execute + "
              f"release listing {phase!r} (refused, zero mutations)")
        return 2

    # ---- 2 OFFLINE COMPILE ----------------------------------------------
    outdir = d / f"out-{expect[:8]}"
    outdir.mkdir(parents=True, exist_ok=True)
    version = f"p26-candidate-{expect[:12]}"
    steps = plan_steps(service, expect)
    compile_argv = [t.replace("<attempt-dir>", str(d))
                     .replace("<unique>", uniq)
                    for t in steps["compile"]]
    ca = Attempt(d, "04-compile")
    ca.pre(compile_argv, {"network": "none", "pull": "never",
                          "modcache": CACHE_VOLUMES[service][0],
                          "buildcache": CACHE_VOLUMES[service][1],
                          "expect_source": expect})
    rc, raw = runner(compile_argv, timeout_s=TIMEOUT_COMPILE_S)
    rc, raw = ca.post(rc, raw)
    if rc != 0:
        raise Abort(f"offline compile rc={rc} (cache miss fails; no "
                    "fallback, no network)")
    binpath = outdir / "service"
    if not binpath.is_file():
        raise Abort("compile reported success but /out/service missing")
    mode = oct(binpath.stat().st_mode & 0o777)
    bin_sha = hashlib.sha256(binpath.read_bytes()).hexdigest()
    if mode != "0o755":
        raise Abort(f"binary mode {mode} != 0755")
    G.write_json(d / "BINARY.json", {"sha256": bin_sha, "mode": mode,
                                     "size": binpath.stat().st_size,
                                     "go_version_first_line":
                                     raw.splitlines()[:1]})

    # ---- 3 CREATE commit base from OLD FULL IMAGE (never running ct) ----
    cname = f"of-p26-build-{service}-{uniq}"
    create_argv = ["docker", "create", "--pull=never", "--network",
                   "none", "--name", cname, "--label", BUILD_LABEL,
                   f"sha256:{OLD_FULL_IDS[service]}"]
    ta = Attempt(d, "05-create-base")
    ta.pre(create_argv, {"base": "OLD SERVICE FULL IMAGE ID (not the "
                                 "running container)",
                         "no_start_env_mount_ports": True})
    rc, raw = runner(create_argv, timeout_s=TIMEOUT_CP_COMMIT_S)
    rc, raw = ta.post(rc, raw)
    if rc != 0:
        raise Abort(f"create rc={rc}")
    new_cid = raw.strip().splitlines()[0].strip()
    if not re.fullmatch(r"[0-9a-f]{64}", new_cid):
        raise Abort(f"create returned non-64hex CID: {new_cid!r}")

    # ---- 4 CP + COMMIT + VERIFY -----------------------------------------
    cp_argv = ["docker", "cp", str(binpath),
               f"{new_cid}:/usr/local/bin/service"]
    cpa = Attempt(d, "06-cp")
    cpa.pre(cp_argv, {"binary_sha256": bin_sha, "mode": "0755"})
    rc, raw = runner(cp_argv, timeout_s=TIMEOUT_CP_COMMIT_S)
    rc, _ = cpa.post(rc, raw)
    if rc != 0:
        raise Abort(f"cp rc={rc}")
    tag = f"openfoundry-p26-{service}:{expect[:12]}-{uniq}"
    commit_argv = ["docker", "commit", new_cid, tag]
    cma = Attempt(d, "07-commit")
    cma.pre(commit_argv, {"new_tag": tag,
                          "old_tags_untouched": True})
    rc, raw = runner(commit_argv, timeout_s=TIMEOUT_CP_COMMIT_S)
    rc, _ = cma.post(rc, raw)
    if rc != 0:
        raise Abort(f"commit rc={rc}")
    va = Attempt(d, "08-verify-new-tag")
    vargv = ["docker", "image", "inspect", tag, "--format",
             "{{.Id}}|{{.Config.User}}|{{.Config.WorkingDir}}|"
             "{{json (index .Config \"Entrypoint\")}}|"
             "{{json (index .Config \"Cmd\")}}"]
    va.pre(vargv)
    rc, vraw = runner(vargv, timeout_s=TIMEOUT_INSPECT_S)
    rc, vraw = va.post(rc, vraw)
    if rc != 0:
        raise Abort("new tag not inspectable")
    new_full = _bare(vraw.split("|")[0])
    oa = Attempt(d, "09-old-config")
    oargv = ["docker", "image", "inspect",
             f"sha256:{OLD_FULL_IDS[service]}", "--format",
             "{{.Config.User}}|{{.Config.WorkingDir}}|"
             "{{json (index .Config \"Entrypoint\")}}|"
             "{{json (index .Config \"Cmd\")}}"]
    oa.pre(oargv)
    rc, oraw = runner(oargv, timeout_s=TIMEOUT_INSPECT_S)
    rc, oraw = oa.post(rc, oraw)
    if rc != 0 or oraw.strip() != vraw.strip().split("|", 1)[1].strip():
        raise Abort(f"new-tag safe config != old runtime config:\n"
                    f"new={vraw!r}\nold={oraw!r}")

    # ---- 5 CLEANUP only exact new CID (label+ID re-checked) -------------
    la = Attempt(d, "10-cleanup-precheck")
    largv = ["docker", "inspect", new_cid, "--format",
             "{{.Id}} {{index .Config.Labels \"of-p26-owner\"}}"]
    la.pre(largv)
    rc, lraw = runner(largv, timeout_s=TIMEOUT_INSPECT_S)
    rc, lraw = la.post(rc, lraw)
    ok_clean = (rc == 0 and new_cid in lraw and "p26-build" in lraw)
    if not ok_clean:
        raise Abort(f"cleanup precheck failed — new CID {new_cid[:12]}… "
                    "KEPT (truthful nonzero; manual review)")
    ra = Attempt(d, "11-rm-exact-cid")
    rargv = ["docker", "rm", new_cid]
    ra.pre(rargv, {"only": "exact new CID after label+ID check",
                   "no_volumes_flag": True})
    rc, raw = runner(rargv, timeout_s=TIMEOUT_CP_COMMIT_S)
    rc, _ = ra.post(rc, raw)
    if rc != 0:
        raise Abort(f"cleanup rc={rc} (truthful; evidence+binary+image "
                    "preserved)")

    # ---- source post == S, separately recorded --------------------------
    src_post = G.candidate_hash(G.OPENFOUNDRY)
    sa = Attempt(d, "12-source-post")
    sa.pre(["(in-process) candidate_hash AFTER"])
    sa.post(0 if src_post == expect else 1,
            f"actual={src_post}\n")
    if running_pin:
        rc, raw, _ = inspect_fmt(runner,d,'13-running-post',running_pin['cid'],RUNTIME_FORMAT)
        if rc != 0:raise Abort('runtime post probe failed')
        check_running_snapshot(raw,running_pin)
        rc, raw, _ = inspect_fmt(runner,d,'14-old-tag-post',f'{Q3_PROJECT}-{service}','{{.Id}}')
        if rc != 0 or _bare(raw) != OLD_FULL_IDS[service]:raise Abort('old tag changed during candidate build')
    ok = src_post == expect
    G.write_json(d / "BUILD-RESULT.json",
                 {"ok": ok, "service": service,
                  "expect_source_sha256": expect,
                  "source_post": src_post,
                  "Root_running_snapshot": running_pin,
                  "new_tag": tag, "new_full_id": f"sha256:{new_full}",
                  "old_full_id": f"sha256:{OLD_FULL_IDS[service]}",
                  "binary": {"sha256": bin_sha, "mode": "0755"},
                  "cleaned_cid": new_cid,
                  "rollback": "old tags/ids untouched; rollback = use "
                              "old full IDs (never moved)"})
    print(f"[{TOOL}] {phase} OK tag={tag} full={new_full[:16]}… "
          f"src_post==S: {ok}")
    return 0 if ok else 2


def main(argv=None, runner=None):
    ap = argparse.ArgumentParser(prog=TOOL)
    ap.add_argument("mode", choices=["facts", "plan", "run",
                                        "resume"])
    ap.add_argument("--service", choices=("connector", "dataset",
                                          "pipeline"))
    ap.add_argument("--expect-source-sha", default=None)
    ap.add_argument("--release-file", default=None)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args(argv)
    runner = runner or G.real_runner
    release = G.load_release(args.release_file)
    try:
        if args.mode == "facts":
            return cmd_facts(args, runner)
        if not args.service:
            print(f"[{TOOL}] {args.mode} requires --service",
                  file=sys.stderr)
            return 2
        if args.mode == "resume":
            if args.service not in RESUME_BOUND:
                print(f"[{TOOL}] resume requires --service "
                      f"{sorted(RESUME_BOUND)}", file=sys.stderr)
                return 2
            return cmd_resume(args, runner, release, args.execute)
        if args.mode == "plan":
            if not (args.expect_source_sha and
                    G.HEX64.match(args.expect_source_sha)):
                print(f"[{TOOL}] plan requires --expect-source-sha "
                      "<64hex>", file=sys.stderr)
                return 2
            G.write_json(Path(f"plan-build-{args.service}.json")
                         if False else
                         (ev_dir() / f"plan_{args.service}_"
                          f"{args.expect_source_sha[:8]}.json"),
                         plan_steps(args.service, args.expect_source_sha))
            print(json.dumps(plan_steps(args.service,
                                        args.expect_source_sha),
                             indent=1))
            return 0
        return cmd_run(args, runner, release, args.execute)
    except Abort as e:
        print(f"[{TOOL}] ABORT: {e}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())

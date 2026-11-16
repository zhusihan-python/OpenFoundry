#!/usr/bin/env python3
"""P2.6 W6 isolated-PG runner — EXECUTABLE (round-2 review rework).

Root round2 blockers implemented:
  #1 EXPOSE != host publishing: NetworkSettings.Ports parsed as a mapping
     ({"5432/tcp": null} is the normal unpublished shape); independently
     docker port empty AND HostConfig.PortBindings empty.
  #2 CLI fields: phase/race read args.test_mode (exercised via the real
     argparse path in selftests for both modes).
  #3 every subprocess bounded (per-call timeout, rc=124+TIMEOUT marker on
     expiry); readiness = per-probe timeout + MONOTONIC 90s wall-clock cap
     with EVERY probe attempt's rc/raw retained; go test carries an
     explicit -timeout and an outer process bound.
  #4 preflight probes are whitelisted ({{.Id}}-format only — never
     Config.Env) with per-probe ORIGINAL cmd/raw/rc evidence; the
     composite summary is clearly labeled and its aggregate verdict lives
     in a separate .gate.rc; PG major verified 16 via owned-container
     `psql -t -A -c "SHOW server_version_num"` after ready.
  #5 test DSN env file: exclusively-created UNIQUE 0600 temp file
     (O_CREAT|O_EXCL — refuses pre-existing file/symlink), bounded
     cleanup of ONLY that file; value never archived.
  #6 image IDs are STRICT pins: postgres/golang actual full ids must
     equal the reviewed recorded ids — drift REFUSES and reports for Root
     (no silent NOTE-and-continue); no pull fallback.

Execution policy unchanged: mutating phases need --execute + Root release
token; first-error stop; the ONLY retry is the bounded readiness probe.
"""

import argparse
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

TOOL = "p26_pg_runner"
HERE = Path(__file__).resolve().parent
def _phase_dir(phase: str) -> Path:
    """Unique EXCLUSIVE attempt dir (UTC+token; mkdir exist_ok=False,
    bounded fresh-token retries — same-second double runs never share a
    dir, mirroring the accepted build-executor pattern)."""
    import secrets as _sec
    base = f"{phase}_{G.now_utc()}"
    for i in range(3):
        cand = ev_dir() / (base if i == 0 else
                           f"{base}_{_sec.token_hex(3)}")
        try:
            cand.mkdir(parents=True, exist_ok=False)
            return cand
        except FileExistsError:
            continue
    raise Abort(f"could not exclusively create phase dir for {phase}")


def ev_dir():
    """Call-time resolution so isolated selftests (P26_PG_EV_DIR) never
    mix synthetic evidence into the real results root."""
    import os as _os
    return Path(_os.environ.get("P26_PG_EV_DIR", str(G.RESULTS_W6 / "pg")))


EV_DIR = None  # historical name; use ev_dir()

# Strict pins (root round2 #6), byte-captured from cached images on
# 2026-11-15 (whitelisted inspect; see results/w6/pg/facts_*). Actual ids
# are re-resolved at EVERY execution and compared — drift refuses.
PG_IMAGE = "postgres:16"
PG_IMAGE_ID_RECORDED = "e17e86066e5ef83e0952a9347f5c792b7ece00972e2aa787a6986f471b3dd3d5"
GO_IMAGE_IDS_RECORDED = {
    "golang:1.25": "699337d620559a59b4a2bb298ad59611e535d2ee755a34cf2d2a98f37578dc80",   # golang:1.25,golang:1.25.14
    "golang:1.26.3": "2d6c80227255c3112a4d08e67ba98e58efd3846daf15d9d7d4c389565d881b1a",  # golang:1.26.3
}

TIMEOUT_INSPECT_S = 30
TIMEOUT_PROBE_S = 15
TIMEOUT_CONTAINER_S = 60
TIMEOUT_GOTEST_OUTER_S = 1500
GO_TEST_TIMEOUT_FLAG = "20m"

READY_ATTEMPTS = 20
READY_SLEEP_S = 3
READY_WALLCAP_S = 90

COHORTS = {
    "connector": {
        "image": "golang:1.25",
        "expect_go": "go1.25.14",
        "expect_tag": "golang:1.25.14",
        "mod_volume": "openfoundry-gomod",
        "build_volume": "openfoundry-gobuild",
        "cwd": "/src/services/connector-management-service",
        # repo-only this round (Root: all 17 names live in internal/repo;
        # handlers re-included only in the final unified regression to
        # avoid depending on W3's in-flight handlers churn)
        "packages": ["./internal/repo/"],
        "dsn_env": "CONNECTOR_DB_DSN",
        "names_file": "p26-connector-pg-names.txt",
    },
    "dataset": {
        "image": "golang:1.26.3",
        "expect_go": "go1.26.3",
        "expect_tag": "golang:1.26.3",
        "mod_volume": "of-go-mod",
        "build_volume": "of-go-build",
        "cwd": "/src",
        "packages": ["./services/dataset-versioning-service/internal/repo/"],
        "dsn_env": "DATASET_DB_DSN",
        "names_file": "p26-dataset-pg-names.txt",
    },
}


UNIFIED_COHORTS_JSON = HERE / "unified" / "p26-unified-cohorts.json"


def load_unified_cohorts():
    """Suite=unified cohorts, single source of truth = the roster tool's
    emit-names output (names files + packages + per-service DSN env +
    image/volume facts; NOT hand-maintained here)."""
    if not UNIFIED_COHORTS_JSON.is_file():
        raise Abort(f"unified cohorts file missing: {UNIFIED_COHORTS_JSON}"
                    " (run: p26_unified_roster.py emit-names)")
    data = json.loads(UNIFIED_COHORTS_JSON.read_text())
    cohorts = {}
    for svc, c in data["services"].items():
        cohorts[svc] = {
            "image": c["image_ref"], "expect_go": c["expect_go"],
            "expect_tag": c["image_ref"],
            "mod_volume": c["volumes"][0], "build_volume": c["volumes"][1],
            "cwd": "/src", "packages": c["packages"],
            "dsn_env": c["dsn_env"],
            "names_file": f"unified/{c['names_file']}",
        }
    return cohorts


class Abort(Exception):
    pass


def secret_file() -> Path:
    return Path(os.environ.get("P26_SECRET_FILE", G.P26_SECRET_FILE))


def die(msg, rc=2):
    print(f"[p26-pg] ABORT: {msg}", file=sys.stderr)
    raise SystemExit(rc)


# ------------------------------------------------------------- evidence ----
class Attempt:
    """One evidence set: .cmd BEFORE execution, .raw/.rc/.meta after.
    .rc always keeps the ORIGINAL process rc (124 = subprocess timeout)."""

    def __init__(self, phase_dir: Path, name: str, secrets=()):
        self.dir = phase_dir
        self.dir.mkdir(parents=True, exist_ok=True)
        base = self.dir / name
        n = 1
        while Path(str(base) + ".rc").exists():
            n += 1
            base = self.dir / f"{name}-{n}"
        self.base = base
        self.secrets = list(secrets)
        self._meta = {}

    def pre(self, argv, meta_extra=None):
        cmd, _ = G.scrub(" ".join(str(t) for t in argv), self.secrets)
        Path(str(self.base) + ".cmd").write_text(cmd + "\n")
        self._meta = {"utc": G.now_utc(), "cmd": cmd}
        if meta_extra:
            self._meta.update(meta_extra)

    def post(self, rc, output, meta_extra=None):
        raw, n_scrub = G.scrub(output, self.secrets)
        Path(str(self.base) + ".raw").write_text(raw)
        Path(str(self.base) + ".rc").write_text(f"{rc}\n")
        self._meta["rc"] = rc
        self._meta["secret_scrubbed_occurrences"] = n_scrub
        if meta_extra:
            self._meta.update(meta_extra)
        Path(str(self.base) + ".meta").write_text(
            json.dumps(self._meta, indent=1, sort_keys=True) + "\n")
        return rc, raw

    def gate(self, ok, note):
        Path(str(self.base) + ".gate.rc").write_text(f"{0 if ok else 1}\n")
        Path(str(self.base) + ".gate.raw").write_text(note + "\n")


def resolve_image(runner, image_ref, timeout_s=TIMEOUT_INSPECT_S):
    """(full id, repo_tags_line) resolved in memory; NO pull fallback."""
    argv = ["docker", "image", "inspect", image_ref, "--format",
            "{{.Id}} {{join .RepoTags \",\"}}"]
    rc, out = runner(argv, timeout_s=timeout_s)
    if rc != 0:
        raise Abort(f"image {image_ref} not inspectable (rc={rc}) — no "
                    "pull fallback")
    line = out.strip().splitlines()[0] if out.strip() else ""
    parts = line.split(" ", 1)
    v = parts[0]
    if v.startswith("sha256:"):
        v = v[len("sha256:"):]
    if not G.HEX64.match(v):
        raise Abort(f"image {image_ref} id not full 64-hex: {line!r}")
    return v, (parts[1].strip() if len(parts) > 1 else "")


def strict_pin(actual_id, recorded_id, what):
    if actual_id != recorded_id:
        raise Abort(
            f"{what} cached image id DRIFT vs reviewed pin — refusing; "
            f"actual={actual_id} recorded={recorded_id}; report to Root "
            "for re-review (no pull, no silent continue)")


def classify_not_found(rc, out, name):
    """Strict not-found (root round2 final patch #4): only the expected
    non-zero rc AND Docker's specific missing-object/network message
    containing the EXACT requested name counts as absent. Daemon/socket
    errors, empty output, wrong-name 'no such', arbitrary 'not found'
    all FAIL CLOSED (returned as a failure detail)."""
    text = (out or "").strip()
    if rc == 0:
        return None  # exists
    lower = text.lower()
    if (not text) or "cannot connect" in lower or "docker.sock" in lower:
        return f"rc={rc} non-classifiable output (empty/daemon): {text[:120]!r}"
    needles = (
        f"no such object: {name}",          # CLI form (containers)
        f"no such network: {name}",         # CLI form (networks)
        f"network {name} not found",        # daemon form (networks; the
                                            # form real docker prints via
                                            # compose-less network inspect)
    )
    if any(n in lower for n in needles):
        return None  # genuinely absent (exact-name docker message)
    return f"rc={rc} unrecognized failure (no exact '{name}' not-found " \
           f"message): {text[:120]!r}"


def label_of(runner, kind, name):
    argv = (["docker", "network", "inspect", name, "--format",
             "{{index .Labels \"of-p26-owner\"}}"] if kind == "network"
            else ["docker", "inspect", name, "--format",
                  "{{index .Config.Labels \"of-p26-owner\"}}"])
    rc, out = runner(argv, timeout_s=TIMEOUT_INSPECT_S)
    return rc, out.strip()


def read_secret_password():
    p = secret_file()
    if not p.is_file():
        raise Abort(f"secret env file missing: {p} (run prepare first)")
    if p.is_symlink():
        raise Abort(f"secret env file is a symlink — refusing: {p}")
    mode = oct(p.stat().st_mode & 0o777)
    if mode != "0o600":
        raise Abort(f"secret env file mode {mode} != 0o600 — refusing")
    val = None
    for ln in p.read_text().splitlines():
        if ln.startswith("POSTGRES_PASSWORD="):
            val = ln.split("=", 1)[1].strip()
    if not val or not re.fullmatch(r"[0-9a-f]{48}", val or ""):
        raise Abort("secret env file POSTGRES_PASSWORD not expected "
                    "<48-hex> format")
    return val


# ---------------------------------------------------------------- facts ----
def cmd_facts(args, runner):
    out_dir = _phase_dir("facts")
    out_dir.mkdir(parents=True, exist_ok=True)
    facts = {"utc": G.now_utc(),
             "note": "whitelist: docker version/ps/image inspect Id+"
                     "RepoTags only; no Config.Env, no full inspect"}
    steps = [
        ("server_version", ["docker", "version", "--format",
                            "{{.Server.Version}}"]),
        ("ps", ["docker", "ps", "--format",
                "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}"]),
        ("image_postgres_16", ["docker", "image", "inspect", PG_IMAGE,
                               "--format", "{{.Id}} {{join .RepoTags \",\"}}"]),
    ] + [(f"image_{c['image'].replace(':', '_')}",
          ["docker", "image", "inspect", c["image"], "--format",
           "{{.Id}} {{join .RepoTags \",\"}}"])
         for c in COHORTS.values()]
    for name, argv in steps:
        a = Attempt(out_dir, name)
        a.pre(argv)
        rc, out = runner(argv, timeout_s=TIMEOUT_INSPECT_S)
        rc, raw = a.post(rc, out)
        facts[name] = {"rc": rc, "stdout": raw.strip()[:500]}
        print(f"[p26-pg] facts {name}: rc={rc}")
    G.write_json(out_dir / "facts.json", facts)
    print(f"[p26-pg] facts -> {out_dir / 'facts.json'}")
    return 0


# -------------------------------------------------------------- prepare ----
def cmd_prepare(args, runner, release, execute, sleep_fn=time.sleep,
                now_fn=time.monotonic):
    phase_dir = _phase_dir("prepare")

    # -- component 0: per-probe existence evidence (ORIGINAL rc/raw each);
    #    composite summary + separate aggregate .gate.rc -------------------
    pa = Attempt(phase_dir, "00a-probe-network")
    pa_argv = ["docker", "network", "inspect", G.P26_NET, "--format",
               "{{.Id}}"]
    pa.pre(pa_argv, {"purpose": "whitelisted existence probe "
                     "({{.Id}} only; never Config.Env)"})
    rc_a, out_a = runner(pa_argv, timeout_s=TIMEOUT_INSPECT_S)
    pa.post(rc_a, out_a)
    net_exists = rc_a == 0
    net_fail = None if net_exists else classify_not_found(rc_a, out_a,
                                                         G.P26_NET)
    if net_fail:
        die(f"network existence probe fail-closed: {net_fail} "
            f"(originals: {pa.base}.raw)", rc=2)

    pb = Attempt(phase_dir, "00b-probe-container")
    pb_argv = ["docker", "inspect", G.P26_PG, "--format", "{{.Id}}"]
    pb.pre(pb_argv, {"purpose": "whitelisted existence probe"})
    rc_b, out_b = runner(pb_argv, timeout_s=TIMEOUT_INSPECT_S)
    pb.post(rc_b, out_b)
    ct_exists = rc_b == 0
    ct_fail = None if ct_exists else classify_not_found(rc_b, out_b,
                                                       G.P26_PG)
    if ct_fail:
        die(f"container existence probe fail-closed: {ct_fail} "
            f"(originals: {pb.base}.raw)", rc=2)

    pc = Attempt(phase_dir, "00c-probe-secret")
    sf = secret_file()
    pc.pre(["(in-process) lstat " + str(sf) + " (existence only; content "
           "NEVER read here)"])
    pc.post(0, f"exists={sf.exists()} symlink={sf.is_symlink()}\n")
    secret_exists = sf.exists()

    summ = Attempt(phase_dir, "00-preflight-summary")
    summ.pre(["(composite summary — per-probe ORIGINALS are 00a/00b/00c "
              "*.cmd/.raw/.rc above; this file is NOT docker evidence)"])
    exists_any = net_exists or ct_exists or secret_exists
    summ.post(0, f"network_exists={net_exists}\ncontainer_exists="
                 f"{ct_exists}\nsecret_exists={secret_exists}\n")
    summ.gate(not exists_any,
              "proceed" if not exists_any else
              f"preserve-existing(net={net_exists},ct={ct_exists},"
              f"secret={secret_exists})")
    if exists_any:
        print(f"[p26-pg] prepare: EXISTING resource preserved untouched "
              f"(net={net_exists} container={ct_exists} secret="
              f"{secret_exists}) — no recreation")
        return 3

    if not execute or release is None or "prepare" not in \
            release["released_for"]:
        print("[p26-pg] prepare: nothing exists; mutating phase needs "
              "--execute + release listing 'prepare' (refused, zero "
              "writes)")
        return 2

    # -- component 1: strict-pinned cached PG image ------------------------
    img = Attempt(phase_dir, "01-image-record")
    inspect_argv = ["docker", "image", "inspect", PG_IMAGE, "--format",
                    "{{.Id}} {{join .RepoTags \",\"}}"]
    img.pre(inspect_argv, {"purpose": "resolve + STRICT-compare cached "
                           "postgres:16 full id against reviewed pin"})
    rc, out = runner(inspect_argv, timeout_s=TIMEOUT_INSPECT_S)
    rc, raw = img.post(rc, out)
    if rc != 0:
        raise Abort(f"postgres:16 not inspectable (rc={rc}) — no pull")
    pg_id, pg_tags = resolve_image(runner, PG_IMAGE)
    strict_pin(pg_id, PG_IMAGE_ID_RECORDED, "postgres:16")
    img.gate(True, f"id==pin sha256:{pg_id} tags={pg_tags}")

    # -- component 2: secret env file EXCLUSIVE create (0600) --------------
    sec = Attempt(phase_dir, "02-secret-env600")
    sec.pre(["(in-process) os.open(" + str(sf) +
             ", O_CREAT|O_EXCL|O_WRONLY, 0o600); write "
             "'POSTGRES_PASSWORD=<48-hex>'"],
            {"purpose": "fresh random creds; value NEVER printed/archived"})
    pw = pysecrets.token_hex(24)
    try:
        fd = os.open(str(sf), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.write(fd, f"POSTGRES_PASSWORD={pw}\n".encode())
        os.close(fd)
        rc, out = 0, "created mode=0600 (value redacted)"
    except FileExistsError:
        rc, out = 3, "EXISTS — preserved untouched (no overwrite)"
    sec.post(rc, out)
    if rc != 0:
        raise Abort("secret env file appeared between preflight and "
                    "creation — preserved; aborting")

    # -- component 3: network create + label verify ------------------------
    net = Attempt(phase_dir, "03-net-create")
    net_argv = ["docker", "network", "create", "--label",
                G.P26_OWNER_LABEL, G.P26_NET]
    net.pre(net_argv)
    rc, out = runner(net_argv, timeout_s=TIMEOUT_INSPECT_S)
    rc, raw = net.post(rc, out)
    if rc != 0:
        raise Abort(f"network create rc={rc}")
    lrc, label = label_of(runner, "network", G.P26_NET)
    net.gate(lrc == 0 and label == G.P26_OWNER_VALUE,
             f"label rc={lrc} value={label!r}")
    if lrc != 0 or label != G.P26_OWNER_VALUE:
        raise Abort(f"network label verify rc={lrc} label={label!r}")

    # -- component 4: container run (no host port; secret via env-file) ----
    ctr = Attempt(phase_dir, "04-container-run", secrets=[pw])
    ctr_argv = ["docker", "run", "-d", "--pull=never",
                "--name", G.P26_PG,
                "--network", G.P26_NET,
                "--label", G.P26_OWNER_LABEL,
                "--env-file", str(sf),
                "-e", f"POSTGRES_USER={G.P26_PGUSER}",
                "-e", "POSTGRES_DB=postgres",
                f"sha256:{pg_id}"]
    ctr.pre(ctr_argv, {"image_id": f"sha256:{pg_id}",
                       "secret_injection": "--env-file (value never in "
                                           "argv/raw/meta)"})
    rc, out = runner(ctr_argv, timeout_s=TIMEOUT_CONTAINER_S)
    rc, raw = ctr.post(rc, out)
    if rc != 0:
        raise Abort(f"container run rc={rc}")

    # -- component 5: readiness — per-probe timeout + monotonic wall cap;
    #    EVERY attempt retained (root round2 #3) ---------------------------
    rdy = Attempt(phase_dir, "05-ready")
    rdy_argv = ["docker", "exec", G.P26_PG, "pg_isready", "-U",
                G.P26_PGUSER, "-d", "postgres"]
    rdy.pre(rdy_argv, {"retry_policy":
                       f"deadline checked BEFORE each probe; probe "
                       f"timeout = min({TIMEOUT_PROBE_S}s, remaining); "
                       f"sleeps inside the bound; attempts <= "
                       f"{READY_ATTEMPTS}; MONOTONIC wall cap "
                       f"{READY_WALLCAP_S}s (actual clock); every "
                       "attempt's rc/raw retained"})
    t0 = now_fn()
    attempts_log, ready_rc, ready_out, attempt_n = [], None, "", 0
    while attempt_n < READY_ATTEMPTS:
        remaining = READY_WALLCAP_S - (now_fn() - t0)
        if remaining <= 0:
            attempts_log.append(f"deadline {READY_WALLCAP_S}s reached "
                                "BEFORE next probe — no further probe\n")
            break
        probe_t = min(TIMEOUT_PROBE_S, remaining)
        attempt_n += 1
        ready_rc, ready_out = runner(rdy_argv, timeout_s=probe_t)
        attempts_log.append(f"attempt={attempt_n} rc={ready_rc} "
                            f"probe_timeout_s={probe_t}\n"
                            f"{ready_out.strip()}\n")
        if ready_rc == 0:
            break
        remaining_after = READY_WALLCAP_S - (now_fn() - t0)
        if remaining_after <= 0:
            attempts_log.append(f"deadline {READY_WALLCAP_S}s reached "
                                "after probe — no further probe/sleep\n")
            break
        sleep_fn(min(READY_SLEEP_S, remaining_after))
    elapsed = now_fn() - t0
    rdy.post(ready_rc if ready_rc is not None else 1,
             f"attempts={attempt_n} elapsed_s={elapsed:.1f}\n" +
             "\n".join(attempts_log),
             {"attempts": attempt_n, "elapsed_s": round(elapsed, 1)})
    rdy.gate(ready_rc == 0,
             f"ready rc={ready_rc} attempts={attempt_n} elapsed="
             f"{elapsed:.1f}s")
    if ready_rc != 0:
        raise Abort(f"pg_isready not ready: attempts={attempt_n} "
                    f"elapsed={elapsed:.1f}s (cap {READY_WALLCAP_S}s)")

    # -- component 6: post-validate — each probe gets its OWN original
    #    cmd/raw/rc evidence (root round2 final patch #3); the aggregate
    #    predicate stays a separate summary + .gate.rc --------------------
    probe_specs = [
        ("06a-label", ["docker", "inspect", G.P26_PG, "--format",
                       "{{index .Config.Labels \"of-p26-owner\"}}"]),
        ("06b-running", ["docker", "inspect", G.P26_PG, "--format",
                         "{{.State.Running}}"]),
        ("06c-image", ["docker", "inspect", G.P26_PG, "--format",
                       "{{.Image}}"]),
        ("06d-ports-mapping", ["docker", "inspect", G.P26_PG, "--format",
                               "{{json .NetworkSettings.Ports}}"]),
        ("06e-port-bindings", ["docker", "inspect", G.P26_PG, "--format",
                               "{{json .HostConfig.PortBindings}}"]),
        ("06f-docker-port", ["docker", "port", G.P26_PG]),
    ]
    probe_out = {}
    for pid, pargv in probe_specs:
        pa6 = Attempt(phase_dir, pid)
        pa6.pre(pargv)
        prc, pout = runner(pargv, timeout_s=TIMEOUT_INSPECT_S)
        prc, praw = pa6.post(prc, pout)
        probe_out[pid] = (prc, praw)
    lrc, label = probe_out["06a-label"]
    rrc, running = probe_out["06b-running"]
    irc, image = probe_out["06c-image"]
    prc, ports = probe_out["06d-ports-mapping"]
    brc, bindings = probe_out["06e-port-bindings"]
    hrc, host_ports = probe_out["06f-docker-port"]
    ports_ok, ports_detail = G.parse_ports_mapping(ports)
    bind_ok, bind_detail = G.parse_empty_bindings(bindings)
    findings = {
        "label_ok": lrc == 0 and label.strip() == G.P26_OWNER_VALUE,
        "running_ok": rrc == 0 and running.strip() == "true",
        "image_ok": irc == 0 and image.strip() == f"sha256:{pg_id}",
        "ports_mapping_ok": prc == 0 and ports_ok,
        "port_bindings_ok": brc == 0 and bind_ok,
        "host_ports_empty_ok": hrc == 0 and host_ports.strip() == "",
    }
    details = {"ports_mapping": ports_detail, "port_bindings": bind_detail,
               "ports_raw": ports.strip()[:200],
               "bindings_raw": bindings.strip()[:200],
               "per_probe_evidence":
                   [pid for pid, _ in probe_specs]}
    ok = all(findings.values())
    post = Attempt(phase_dir, "06-post-validate-summary")
    post.pre(["(composite summary — per-probe ORIGINALS are 06a..06f "
              "*.cmd/.raw/.rc above; this file is NOT docker evidence)"])
    post.post(0 if ok else 1,
              "\n".join(f"{k}={v}" for k, v in findings.items()) +
              f"\n{json.dumps(details)}\n"
              f"resolved_image=sha256:{pg_id}\n"
              f"docker_port_stdout={host_ports.strip()!r}\n")
    post.gate(ok, json.dumps(findings))
    if not ok:
        raise Abort(f"post-validate failed: {findings} {details}")

    # -- component 7: PG major version via OWNED container (round2 #4) -----
    ver = Attempt(phase_dir, "07-pg-version")
    ver_argv = ["docker", "exec", G.P26_PG, "psql", "-U", G.P26_PGUSER,
                "-d", "postgres", "-t", "-A", "-c",
                "SHOW server_version_num"]
    ver.pre(ver_argv, {"purpose": "owned-container read-only version "
                       "check; must be major 16"})
    rc, out = runner(ver_argv, timeout_s=TIMEOUT_PROBE_S)
    rc, raw = ver.post(rc, out)
    major_ok = rc == 0 and raw.strip().startswith("16")
    ver.gate(major_ok, f"server_version_num={raw.strip()!r}")
    if not major_ok:
        raise Abort(f"PG major version check failed: {raw.strip()!r}")

    G.write_json(phase_dir / "PREPARE-RESULT.json",
                 {"ok": True, "image_id": f"sha256:{pg_id}",
                  "pg_version_num": raw.strip(),
                  "utc": G.now_utc(),
                  "substrate": {"network": G.P26_NET, "container": G.P26_PG,
                                "owner_label": G.P26_OWNER_LABEL,
                                "host_ports": "zero"},
                  "secret": {"path": str(sf), "mode": "0600",
                             "value_recorded": False}})
    print(f"[p26-pg] prepare OK -> {phase_dir / 'PREPARE-RESULT.json'}")
    return 0


# ------------------------------------------------------------ run-tests ----
def _unique_test_env(service, env_line):
    """Exclusively-created UNIQUE 0600 temp env file (round2 #5): refuses
    any pre-existing path/symlink; caller must unlink ONLY this path."""
    d = secret_file().parent
    for _ in range(8):
        p = d / (f"openfoundry-p26-pg-test-{service}-"
                 f"{pysecrets.token_hex(4)}.env")
        try:
            fd = os.open(str(p), os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                         0o600)
        except FileExistsError:
            continue
        os.write(fd, f"{env_line}\n".encode())
        os.close(fd)
        if oct(os.stat(p).st_mode & 0o777) != "0o600":
            os.unlink(p)
            raise Abort("test env file mode != 0600 after create")
        return p
    raise Abort("could not exclusively create a unique test env file")


def cmd_run_tests(args, runner, release, execute):
    suite = getattr(args, "suite", "new") or "new"
    cohorts = load_unified_cohorts() if suite == "unified" else COHORTS
    if args.service not in cohorts:
        raise Abort(f"service {args.service!r} not available in suite "
                    f"{suite!r} (unified adds pipeline via the roster-"
                    "emitted cohorts file)")
    cohort = cohorts[args.service]
    names_path = Path(getattr(args, "_names_override", None)
                      or HERE / cohort["names_file"])
    names = G.load_required_names(names_path)
    phase = (f"test-unified-{args.service}-{args.test_mode}"
             if suite == "unified" else
             f"test-{args.service}-{args.test_mode}")
    race = args.test_mode == "race"
    phase_dir = _phase_dir(phase)

    # -- prechecks: every probe recorded with its ORIGINAL rc --------------
    pre = Attempt(phase_dir, "00-precheck")
    pre.pre(["(whitelisted read-only probes) network/container "
             "ownership+running; cache volume existence; secret "
             "mode+format; toolchain image pin"],
            {"service": args.service, "mode": args.test_mode,
             "names": names})
    problems, probes = [], []
    lrc, label = label_of(runner, "network", G.P26_NET)
    probes.append(f"network_label rc={lrc} value={label!r}")
    if lrc != 0 or label != G.P26_OWNER_VALUE:
        problems.append(f"network label rc={lrc} label={label!r}")
    clrc, clabel = label_of(runner, "container", G.P26_PG)
    rrc, running = runner(["docker", "inspect", G.P26_PG, "--format",
                           "{{.State.Running}}"],
                          timeout_s=TIMEOUT_INSPECT_S)
    probes.append(f"container_label rc={clrc} value={clabel!r}; "
                  f"running rc={rrc} value={running.strip()!r}")
    if (clrc != 0 or clabel != G.P26_OWNER_VALUE or rrc != 0 or
            running.strip() != "true"):
        problems.append("container owned+running check failed")
    for vol in (cohort["mod_volume"], cohort["build_volume"]):
        vrc, vout = runner(["docker", "volume", "inspect", vol],
                           timeout_s=TIMEOUT_INSPECT_S)
        probes.append(f"volume {vol} rc={vrc}")
        if vrc != 0:
            problems.append(f"cache volume {vol} missing — REFUSING "
                            "(never silently created)")
    password = None
    try:
        password = read_secret_password()
        probes.append("secret mode/format ok (value redacted)")
    except Abort as e:
        problems.append(str(e))
    go_id = None
    try:
        go_id, go_tags = resolve_image(runner, cohort["image"])
        strict_pin(go_id, GO_IMAGE_IDS_RECORDED[cohort["image"]],
                   cohort["image"])
        if cohort["expect_tag"] not in go_tags:
            problems.append(f"{cohort['image']} tags {go_tags!r} lack "
                            f"exact {cohort['expect_tag']!r}")
        probes.append(f"toolchain pin ok sha256:{go_id} tags={go_tags}")
    except (Abort, KeyError) as e:
        problems.append(f"toolchain pin: {e}")
    expect = getattr(args, "expect_source_sha", None)
    if expect:
        if not G.HEX64.match(expect):
            problems.append("--expect-source-sha not 64-hex")
        else:
            actual = G.candidate_hash(Path(G.P26_SRC))
            probes.append(f"expect-source-sha actual={actual[:16]}… "
                          f"expect={expect[:16]}…")
            if actual != expect:
                problems.append("source != expected S — three-way gate "
                                "refuses BEFORE any test run")
    pre.post(0 if not problems else 1,
             "\n".join(probes) + ("\nPROBLEMS:\n" + "\n".join(problems)
                                  if problems else "\nall prechecks ok\n"))
    pre.gate(not problems, "proceed" if not problems else "precheck-fail")
    if problems:
        die(f"prechecks failed (zero writes): {problems}", rc=2)

    if not execute or release is None or \
            phase not in release["released_for"]:
        print(f"[p26-pg] {phase}: needs --execute + release listing "
              f"{phase!r} (refused with zero test writes)")
        return 2

    src_before = G.candidate_hash(Path(G.P26_SRC))
    db_name = "postgres"
    if suite == "unified":
        # P25-precedent: fresh lowercase db per unified sweep + separate
        # SHOW-version gate (each its OWN original cmd/raw/rc).
        db_name = (f"ofp26_unified_{args.service}_"
                   f"{G.now_utc().lower()}_{pysecrets.token_hex(3)}")
        fdb = Attempt(phase_dir, "00b-fresh-db")
        fargv = ["docker", "exec", G.P26_PG, "psql", "-U", G.P26_PGUSER,
                 "-d", "postgres", "-tAc", f"CREATE DATABASE {db_name}"]
        fdb.pre(fargv, {"purpose": "fresh db for the unified sweep "
                        "(P25 precedent; tests migrate it)"})
        rc, out = runner(fargv, timeout_s=TIMEOUT_PROBE_S)
        rc, _ = fdb.post(rc, out)
        if rc != 0:
            raise Abort(f"fresh db create rc={rc}")
        vb = Attempt(phase_dir, "00c-db-version")
        vargv = ["docker", "exec", G.P26_PG, "psql", "-U", G.P26_PGUSER,
                 "-d", "postgres", "-tAc", "SHOW server_version"]
        vb.pre(vargv, {"gate": "server_major_16"})
        rc, out = runner(vargv, timeout_s=TIMEOUT_PROBE_S)
        rc, vraw = vb.post(rc, out)
        vb.gate(rc == 0 and vraw.strip().startswith("16"),
                f"server_version={vraw.strip()!r}")
        if not (rc == 0 and vraw.strip().startswith("16")):
            raise Abort(f"unified db version gate failed: {vraw.strip()!r}")
    dsn = (f"postgres://{G.P26_PGUSER}:{password}@{G.P26_PG}:5432/"
           f"{db_name}")
    env_line = f"{cohort['dsn_env']}={dsn}"
    test_env = _unique_test_env(args.service, env_line)
    try:
        run_re = "^(" + "|".join(names) + ")$"
        pkgs = " ".join(cohort["packages"])
        sh = ("go version && go vet -tags=integration " + pkgs +
              " && go test -tags=integration " +
              ("-race " if race else "") +
              f"-count=1 -timeout {GO_TEST_TIMEOUT_FLAG} -v "
              f"-run '{run_re}' {pkgs}")
        argv = ["docker", "run", "--pull=never", "--rm",
                "--network", G.P26_NET,
                "-v", f"{G.P26_SRC}:/src:ro",
                "-v", f"{cohort['mod_volume']}:/go/pkg/mod",
                "-v", f"{cohort['build_volume']}:/root/.cache/go-build",
                "-w", cohort["cwd"],
                "--env-file", str(test_env),
                "-e", "GOTOOLCHAIN=local", "-e", "GOFLAGS=-mod=readonly",
                "-e", "GOPROXY=off",
                "-e", f"CGO_ENABLED={1 if race else 0}",
                f"sha256:{go_id}", "sh", "-c", sh]

        a = Attempt(phase_dir, "01-go-test", secrets=[password, dsn])
        a.pre(argv, {"utc_start": G.now_utc(),
                     "image": f"sha256:{go_id}",
                     "expected_go": cohort["expect_go"],
                     "required_names": names,
                     "source_before": src_before,
                     "secret_injection": f"--env-file {test_env} (unique "
                                         "exclusive 0600 temp file; value "
                                         "never in argv/raw/meta)",
                     "volumes": [cohort["mod_volume"],
                                 cohort["build_volume"]],
                     "bounds": {"go_test_timeout": GO_TEST_TIMEOUT_FLAG,
                                "outer_process_timeout_s":
                                    TIMEOUT_GOTEST_OUTER_S}})
        print(f"[p26-pg] {phase}: executing isolated-PG attempt -> "
              f"{Path(str(a.base) + '.raw')}")
        rc, out = runner(argv, timeout_s=TIMEOUT_GOTEST_OUTER_S)
        rc, raw = a.post(rc, out, {"utc_end": G.now_utc()})
    finally:
        try:
            os.unlink(test_env)
        except FileNotFoundError:
            pass

    src_after = G.candidate_hash(Path(G.P26_SRC))
    first_line = raw.splitlines()[:1]
    first_line = first_line[0] if first_line else ""
    version_ok = first_line.startswith(f"go version {cohort['expect_go']} ")
    stats, framework = G.parse_go_test_verbose(raw.splitlines())
    ngate = G.required_names_gate(stats, framework, names)
    sgate = G.source_gate(src_before, src_after)

    Path(str(a.base) + ".version-gate.rc").write_text(
        f"{0 if version_ok else 1}\n")
    Path(str(a.base) + ".names-gate.rc").write_text(
        f"{0 if ngate['ok'] else 1}\n")
    Path(str(a.base) + ".source-gate.rc").write_text(
        f"{0 if sgate['ok'] else 1}\n")
    expect_ok = (src_before == expect) if expect else True
    wrapper = 0 if (rc == 0 and version_ok and ngate["ok"] and
                    sgate["ok"] and expect_ok) else 2
    Path(str(a.base) + ".wrapper.rc").write_text(f"{wrapper}\n")
    meta_path = Path(str(a.base) + ".meta")
    meta = json.loads(meta_path.read_text())
    meta.update({"source_after": src_after,
                 "version_ok": version_ok,
                 "names_gate_ok": ngate["ok"],
                 "source_gate_ok": sgate["ok"],
                 "expect_pin_ok": expect_ok,
                 "wrapper_rc": wrapper})
    meta_path.write_text(json.dumps(meta, indent=1, sort_keys=True) + "\n")
    G.write_json(Path(str(a.base) + ".gates.json"),
                 {"original_rc": rc, "version_ok": version_ok,
                  "first_line": first_line, "names_gate": ngate,
                  "source_gate": sgate,
                  "expect_source_sha256": expect,
                  "expect_pin_ok": expect_ok, "wrapper_rc": wrapper})
    print(f"[p26-pg] {phase}: original rc={rc} wrapper rc={wrapper} "
          f"(version={version_ok} names={ngate['ok']} "
          f"source={sgate['ok']})")
    return wrapper



UNIT_COHORTS = {
    "dataset": {"image": "golang:1.26.3", "expect_go": "go1.26.3",
                "volumes": ("of-go-mod", "of-go-build"), "cwd": "/src",
                "pkgs": "./services/dataset-versioning-service/..."},
    "pipeline": {"image": "golang:1.26.3", "expect_go": "go1.26.3",
                 "volumes": ("openfoundry-gomod", "openfoundry-gobuild"),
                 "cwd": "/src",
                 "pkgs": "./services/pipeline-build-service/..."},
    "connector": {"image": "golang:1.25", "expect_go": "go1.25.14",
                  "volumes": ("openfoundry-gomod", "openfoundry-gobuild"),
                  "cwd": "/src/services/connector-management-service",
                  "pkgs": "./..."},
}


def cmd_run_unit(args, runner, release, execute):
    """Root unified-validation §9 final unit checks: whole service module
    UNIT tests (no integration tag), OFFLINE (--network none, no DSN, no
    PG), cached image/volumes, gates: version/source(S)/rc/wrapper."""
    c = UNIT_COHORTS[args.service]
    phase = f"unit-{args.service}"
    d = _phase_dir(phase)
    expect = getattr(args, "expect_source_sha", None)
    problems = []
    src0 = G.candidate_hash(Path(G.P26_SRC))
    if not expect or not G.HEX64.match(expect) or src0 != expect:
        problems.append("source!=S (expect="
                        + ("set" if expect else "MISSING")
                        + " actual=" + src0[:12] + "…)")
    for vol in c["volumes"]:
        rc, _ = runner(["docker", "volume", "inspect", vol],
                       timeout_s=TIMEOUT_INSPECT_S)
        if rc != 0:
            problems.append(f"volume {vol} missing")
    try:
        go_id, _ = resolve_image(runner, c["image"])
        strict_pin(go_id, GO_IMAGE_IDS_RECORDED[c["image"]], c["image"])
    except (Abort, KeyError) as e:
        problems.append(f"toolchain pin: {e}")
    pre = Attempt(d, "00-precheck")
    pre.pre(["(read-only) volumes; toolchain pin; source==S"],
            {"service": args.service, "expect": expect})
    pre.post(0 if not problems else 1,
             ("\n".join(problems) if problems else "prechecks ok\n"))
    if problems:
        raise Abort(f"unit prechecks failed (zero runs): {problems}")
    if not execute or release is None or \
            phase not in release["released_for"]:
        print(f"[p26-pg] {phase}: needs --execute + release listing "
              f"{phase!r}")
        return 2
    sh = (f"go version && go vet {c['pkgs']} && "
          f"go test -count=1 {c['pkgs']}")
    argv = ["docker", "run", "--pull=never", "--rm", "--network", "none",
            "-v", f"{G.P26_SRC}:/src:ro",
            "-v", f"{c['volumes'][0]}:/go/pkg/mod",
            "-v", f"{c['volumes'][1]}:/root/.cache/go-build",
            "-w", c["cwd"], "-e", "GOTOOLCHAIN=local",
            "-e", "GOFLAGS=-mod=readonly", "-e", "GOPROXY=off",
            "-e", "GOSUMDB=off", "-e", "CGO_ENABLED=0",
            f"sha256:{go_id}", "sh", "-c", sh]
    a = Attempt(d, "01-unit")
    a.pre(argv, {"utc_start": G.now_utc(), "image": f"sha256:{go_id}",
                 "expect_go": c["expect_go"], "source_before": src0,
                 "network": "none", "tags": "NONE (unit only)",
                 "bounds": {"outer_process_timeout_s":
                            TIMEOUT_GOTEST_OUTER_S}})
    print(f"[p26-pg] {phase}: executing offline unit -> "
          f"{Path(str(a.base) + '.raw')}")
    rc, out = runner(argv, timeout_s=TIMEOUT_GOTEST_OUTER_S)
    rc, raw = a.post(rc, out, {"utc_end": G.now_utc()})
    src1 = G.candidate_hash(Path(G.P26_SRC))
    first = raw.splitlines()[:1]
    first = first[0] if first else ""
    vok = first.startswith(f"go version {c['expect_go']} ")
    sok = src1 == expect
    wrapper = 0 if (rc == 0 and vok and sok) else 2
    Path(str(a.base) + ".version-gate.rc").write_text(f"{0 if vok else 1}\n")
    Path(str(a.base) + ".source-gate.rc").write_text(f"{0 if sok else 1}\n")
    Path(str(a.base) + ".wrapper.rc").write_text(f"{wrapper}\n")
    G.write_json(Path(str(a.base) + ".gates.json"),
                 {"original_rc": rc, "version_ok": vok, "first_line": first,
                  "source_gate": {"before": src0, "after": src1,
                                  "ok": sok}, "wrapper_rc": wrapper})
    print(f"[p26-pg] {phase}: rc={rc} wrapper={wrapper} "
          f"(version={vok} source={sok})")
    return wrapper

# ----------------------------------------------------------------- down ----
def cmd_down(args, runner, release, execute):
    phase_dir = _phase_dir("down")
    lrc, label = label_of(runner, "container", G.P26_PG)
    if lrc != 0 or label != G.P26_OWNER_VALUE:
        die(f"container {G.P26_PG} label check rc={lrc} label={label!r} "
            "— refusing to touch it (zero writes)")
    nlrc, nlabel = label_of(runner, "network", G.P26_NET)
    if nlrc != 0 or nlabel != G.P26_OWNER_VALUE:
        die(f"network {G.P26_NET} label check rc={nlrc} "
            f"label={nlabel!r} — refusing (zero writes)")
    if not execute or release is None or \
            "down" not in release["released_for"]:
        print("[p26-pg] down: labels verified; needs --execute + release "
              "listing 'down' (refused with zero writes)")
        return 2
    c = Attempt(phase_dir, "01-rm-container")
    c.pre(["docker", "rm", "-f", G.P26_PG],
          {"precheck": f"label=={G.P26_OWNER_VALUE} verified"})
    rc, out = runner(["docker", "rm", "-f", G.P26_PG],
                     timeout_s=TIMEOUT_CONTAINER_S)
    rc, _ = c.post(rc, out)
    if rc != 0:
        raise Abort(f"container remove rc={rc}")
    n = Attempt(phase_dir, "02-net-rm")
    n.pre(["docker", "network", "rm", G.P26_NET])
    rc, out = runner(["docker", "network", "rm", G.P26_NET],
                     timeout_s=TIMEOUT_INSPECT_S)
    rc, _ = n.post(rc, out)
    if rc != 0:
        raise Abort(f"network remove rc={rc}")
    print(f"[p26-pg] down OK (secret file {secret_file()} intentionally "
          f"kept — no deletion of secrets) -> {phase_dir}")
    return 0


# ------------------------------------------------------------ check-log ----
def check_log(log_path: Path, names_path: Path):
    names = G.load_required_names(names_path)
    text = log_path.read_text()
    stats, framework = G.parse_go_test_verbose(text.splitlines())
    gate = G.required_names_gate(stats, framework, names)
    version = text.splitlines()[:1]
    version_ok = bool(version and version[0].startswith("go version go"))
    return {"log": str(log_path), "first_line": version[0] if version
            else None, "version_ok": version_ok,
            "names_gate": gate, "verdict": "PASS" if (gate["ok"] and
                                                      version_ok) else "FAIL"}


def main(argv=None, runner=None, sleep_fn=time.sleep,
         now_fn=time.monotonic):
    ap = argparse.ArgumentParser(prog=TOOL)
    ap.add_argument("mode", choices=["facts", "prepare", "run-tests",
                                     "run-unit", "down", "check-log"])
    ap.add_argument("--service",
                    choices=sorted(set(COHORTS) | {"pipeline"}),
                    default=None)
    ap.add_argument("--suite", choices=["new", "unified"], default="new",
                    help="new = current cohorts only; unified = full "
                         "roster (old sealed lineage + current) incl. "
                         "pipeline adapter and connector handlers")
    ap.add_argument("--expect-source-sha", default=None,
                    help="64-hex frozen candidate S: three-way gate "
                         "(before==after==S) around the run")
    ap.add_argument("--test-mode", choices=["normal", "race"], default=None)
    ap.add_argument("--names", default=None)
    ap.add_argument("--log")
    ap.add_argument("--release-file", default=None)
    ap.add_argument("--execute", action="store_true")
    args = ap.parse_args(argv)

    runner = runner or G.real_runner
    release = G.load_release(args.release_file)

    try:
        if args.mode == "facts":
            return cmd_facts(args, runner)
        if args.mode == "prepare":
            return cmd_prepare(args, runner, release, args.execute,
                               sleep_fn=sleep_fn, now_fn=now_fn)
        if args.mode == "run-tests":
            allowed = (set(COHORTS) | {"pipeline"}) if \
                args.suite == "unified" else set(COHORTS)
            if args.service not in allowed:
                print(f"[p26-pg] run-tests --suite {args.suite} requires "
                      f"--service {sorted(allowed)}", file=sys.stderr)
                return 2
            if args.test_mode not in ("normal", "race"):
                print("[p26-pg] run-tests requires --test-mode "
                      "normal|race", file=sys.stderr)
                return 2
            args._names_override = args.names  # self-test hook only
            return cmd_run_tests(args, runner, release, args.execute)
        if args.mode == "run-unit":
            if args.service not in UNIT_COHORTS:
                print(f"[p26-pg] run-unit requires --service "
                      f"{sorted(UNIT_COHORTS)}", file=sys.stderr)
                return 2
            return cmd_run_unit(args, runner, release, args.execute)
        if args.mode == "down":
            return cmd_down(args, runner, release, args.execute)
        if args.mode == "check-log":
            if not (args.log and args.names):
                print("[p26-pg] check-log requires --log and --names",
                      file=sys.stderr)
                return 2
            out = check_log(Path(args.log), Path(args.names))
            print(json.dumps(out, indent=1))
            return 0 if out["verdict"] == "PASS" else 1
    except Abort as e:
        print(f"[p26-pg] ABORT: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

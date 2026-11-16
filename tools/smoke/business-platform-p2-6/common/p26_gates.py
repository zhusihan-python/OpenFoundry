#!/usr/bin/env python3
"""P2.6 W6 shared gates + executable step engine (round-1 review rework).

Mirrors P25 discipline but is a REAL executor, not a plan printer:
  * every step is linted (no pull except --pull=never, no host ports, no
    host network, no writes against frozen old substrates) and then RUN,
    capturing FULL stdout/stderr and the REAL rc;
  * first-error stop (a failed step aborts the phase — a failed same-name
    precheck can never continue into creation/removal), except steps that
    declare an explicit bounded retry (readiness probes only);
  * mutating steps additionally require --execute AND a Root release token
    listing the phase; without it the whole phase refuses with ZERO calls;
  * read-only whitelisted docker (version/ps/inspect/port/image|volume|
    network inspect) never needs a release;
  * step predicates (check=) are evaluated in-code after execution;
  * source content hash recipe is BYTE-EQUAL to the P25 established
    candidate_source_gate_p25.py:candidate_stream (absolute paths, record
    newlines, ROOTS ordering) — parity is proven against the actual P25
    module in the selftests and on the real repo (read-only).

Secrets: values live only in 0600 private files and in memory; they are
scrubbed from any raw/meta/cmd output before it is written (scrub count
recorded). Nothing under business-platform-p2-5 is ever written.
"""

import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.dont_write_bytecode = True

ROOT_P26 = Path(__file__).resolve().parents[1]          # business-platform-p2-6
ROOT_P25 = ROOT_P26.parent / "business-platform-p2-5"   # frozen, read-only
OPENFOUNDRY = ROOT_P26.parents[2]                       # repo root
RESULTS_W6 = ROOT_P26 / "results" / "w6"

# --- pinned facts (Root round-1 review + independently re-verified) --------
P25_FINAL_DIR = ROOT_P25 / "results" / "2026-11-14-p25-final"
P25_BUNDLE_DIR = (ROOT_P25 / "results" / "2026-11-14-p25-final-seal-meta" /
                  "final-bundle" / "attempt-1")
EXPECTED_SEAL_SHA256 = ("a1e760b7ae9d56d0771337ae307b2c0b5a706800"
                        "5537a26d4094a1450c0f0ad0")     # sha256(hash-manifest.json)
EXPECTED_SEALED_FILES = 5016
EXPECTED_BUNDLE_MANIFEST_SHA256 = ("1fd0329e2f9322e71316d266ab03440f"
                                   "0678f97bd05734de88b09973d39705bd")
# Historical frozen P25 candidate source sha (Root round-1 #6; byte-checked
# against the three sealed final metas). It is NOT the current P26 tree;
# P26 runs only use the before==after same-candidate gate.
EXPECTED_P25_A1_SOURCE_SHA = ("a1bff07f3760fce7b5661b169ba26a165712c950"
                              "035d4e4ec9948639d51ffd27")

# --- P26 isolated PG substrate ---------------------------------------------
P26_NET = "of-p26-pg-net"
P26_PG = "of-p26-pg"
P26_PGUSER = "ofp26"
P26_OWNER_KEY = "of-p26-owner"
P26_OWNER_VALUE = "p26-pg-isolated"
P26_OWNER_LABEL = f"{P26_OWNER_KEY}={P26_OWNER_VALUE}"
P26_SECRET_FILE = "/private/tmp/openfoundry-p26-pg.env"   # 0600, never evidence
P26_RELEASE_FILE = os.environ.get("P26_RELEASE_FILE",
                                  "/private/tmp/openfoundry-p26-release.json")
P26_SRC = str(OPENFOUNDRY)

OLD_PG_NAMES = frozenset({
    "of-p25-pg", "of-p25-pg-net", "of-p24-pg", "of-p23-source-pg",
    "of-p22-cancel-pg",
})
Q3_PROJECT = "openfoundry-platform-p1-q3v9"
Q3_CONTAINERS = frozenset({
    f"{Q3_PROJECT}-connector-1", f"{Q3_PROJECT}-dataset-1",
    f"{Q3_PROJECT}-pipeline-1", f"{Q3_PROJECT}-fixture-1",
    f"{Q3_PROJECT}-postgres-1",
})
DOCKER_MUTATING_VERBS = {
    "rm", "stop", "kill", "restart", "pause", "unpause", "create", "start",
}
# read-only docker subcommands allowed WITHOUT any release token
READONLY_DOCKER_SUBS = frozenset({
    "version", "ps", "inspect", "port", "images",
})

HEX64 = re.compile(r"^[0-9a-f]{64}$")
TEST_NAME_RE = re.compile(r"^Test[A-Za-z0-9_]+$")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-"
                     r"[0-9a-f]{12}$")


class GateRefusal(Exception):
    """A protection gate refused the operation BEFORE any execution."""


class PhaseAbort(Exception):
    """A step failed (or its predicate failed) — phase stops here."""


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_file(path) -> str:
    import hashlib
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_bytes(data: bytes) -> str:
    import hashlib
    return hashlib.sha256(data).hexdigest()


def scrub(text: str, secrets):
    """Replace every secret value with ***; returns (text, count)."""
    n = 0
    for sec in secrets:
        if sec:
            n += text.count(sec)
            text = text.replace(sec, "***")
    return text, n


# ------------------------------------------------- source recipe (P25) ------
# BYTE-EQUAL to candidate_source_gate_p25.py:candidate_stream, whose file
# set comes from source_hash_p25.py ROOTS (read-only imported semantics):
#   go.mod\n + bytes, go.sum\n + bytes, "content-hashes\n", then one
#   "<sha256>  <absolute-path>\n" record per file, paths SORTED, connector/
#   dataset/pipeline take .go+.sql, libs/scheduling-cron takes .go only.
SOURCE_ROOTS = (
    ("services/connector-management-service", (".go", ".sql")),
    ("services/dataset-versioning-service", (".go", ".sql")),
    ("services/pipeline-build-service", (".go", ".sql")),
    ("libs/scheduling-cron", (".go",)),
)


def candidate_stream(root: Path) -> bytes:
    root = Path(root).absolute()
    parts = [b"go.mod\n", (root / "go.mod").read_bytes(),
             b"go.sum\n", (root / "go.sum").read_bytes(),
             b"content-hashes\n"]
    paths = []
    for rel, exts in SOURCE_ROOTS:
        base = root / rel
        if not base.is_dir():
            raise GateRefusal(f"source recipe: missing dir {rel}")
        for p in base.rglob("*"):
            if p.is_file() and p.suffix in exts:
                paths.append(str(p))
    import hashlib
    for p in sorted(paths):
        digest = hashlib.sha256(Path(p).read_bytes()).hexdigest()
        parts.append(f"{digest}  {p}\n".encode())
    return b"".join(parts)


def candidate_hash(root: Path) -> str:
    import hashlib
    return hashlib.sha256(candidate_stream(root)).hexdigest()


# ---------------------------------------------------------------- gates ----
def argv_lint(argv):
    """Docker argv hygiene: --pull=never allowed; every other pull, host
    port publishing, host network, privileged refused."""
    v = []
    a = [str(x) for x in argv]
    if a[:1] == ["docker"]:
        sub = a[1] if len(a) > 1 else ""
        if sub == "pull":
            v.append("docker pull is forbidden (cached images only; use "
                     "--pull=never on run/build)")
    for i, tok in enumerate(a):
        if tok == "--pull":
            v.append("bare --pull is forbidden (only --pull=never)")
        elif tok.startswith("--pull=") and tok != "--pull=never":
            v.append(f"{tok} is forbidden (only --pull=never)")
    for bad in ("-p", "--publish", "-P", "--publish-all"):
        if bad in a:
            v.append(f"host port publish {bad!r} is forbidden")
    net_val = None
    for i, tok in enumerate(a):
        if tok == "--network":
            net_val = a[i + 1] if i + 1 < len(a) else None
        elif tok.startswith("--network="):
            net_val = tok.split("=", 1)[1]
    if net_val == "host":
        v.append("--network host is forbidden")
    if "--privileged" in a:
        v.append("--privileged is forbidden")
    return v


def _docker_targets(argv):
    return {t for t in argv[2:] if isinstance(t, str)
            and (t.startswith("of-") or t.startswith(Q3_PROJECT)
                 or t.startswith("openfoundry-"))}


def is_readonly_docker(argv):
    """Whitelisted read-only docker fact commands (no release needed)."""
    a = [str(x) for x in argv]
    if a[:1] != ["docker"] or len(a) < 2:
        return False
    sub = a[1]
    if sub in ("version", "ps", "port", "images"):
        return True
    if sub == "inspect":
        return True
    if sub in ("image", "volume", "network"):
        return len(a) > 2 and a[2] == "inspect"
    return False


def old_substrate_write_refusal(argv):
    """Refuse WRITES against frozen old PG containers/networks and q3
    base containers; read-only whitelisted inspection may mention them."""
    a = [str(x) for x in argv]
    v = []
    if not a or a[0] != "docker":
        for tok in a:
            if "/private/tmp/openfoundry-" in tok and "p26" not in tok:
                v.append(f"non-p26 private tmp path refused: {tok!r}")
        return v
    sub = a[1] if len(a) > 1 else ""
    targets = _docker_targets(a)
    if sub in DOCKER_MUTATING_VERBS:
        hit = targets & (OLD_PG_NAMES | Q3_CONTAINERS)
        if hit:
            v.append(f"direct mutation of frozen substrate refused: "
                     f"{sorted(hit)}")
    if sub in ("network", "volume") and len(a) > 2 and a[2] in (
            "create", "rm", "remove", "connect", "disconnect", "prune"):
        hit = targets & OLD_PG_NAMES
        if hit:
            v.append(f"network/volume mutation of old substrate refused: "
                     f"{sorted(hit)}")
    if sub in ("run", "exec") and (targets & OLD_PG_NAMES):
        v.append(f"{sub} against old PG substrate refused: "
                 f"{sorted(targets & OLD_PG_NAMES)}")
    if sub == "compose":
        proj = None
        for flag in ("--project-name", "-p"):
            if flag in a:
                i = a.index(flag)
                proj = a[i + 1] if i + 1 < len(a) else None
                break
        if proj is not None and proj != Q3_PROJECT:
            v.append(f"compose project {proj!r} != {Q3_PROJECT!r} — "
                     "wrong project refused")
        cssub, skip_next = None, False
        for tok in a[2:]:
            if skip_next:
                skip_next = False
                continue
            if tok in ("-f", "--file", "--project-name", "-p", "--env-file"):
                skip_next = True
                continue
            if tok.startswith("-"):
                continue
            cssub = tok
            break
        if cssub in ("down", "kill", "stop", "restart", "start", "run"):
            v.append(f"compose {cssub} against q3 refused (whole-project "
                     "mutation)")
    return v


def check_step_gates(s):
    argv = s["argv"] if isinstance(s, dict) else s
    v = argv_lint(argv) + old_substrate_write_refusal(argv)
    return v


def redact_argv(argv, secrets):
    out = []
    for tok in argv:
        tok = str(tok)
        for sec in secrets:
            if sec and sec in tok:
                tok = tok.replace(sec, "***")
        out.append(tok)
    return out


# ------------------------------------------------------- go test log gate --
def parse_go_test_verbose(lines):
    stats, framework = {}, []
    for ln in lines:
        t = ln.rstrip("\n")
        m = re.match(r"^=== RUN\s+(\S+)$", t)
        if m:
            d = stats.setdefault(m.group(1), {"run": 0, "pass": 0,
                                              "skip": 0, "fail": 0})
            d["run"] += 1
            continue
        m = re.match(r"^--- (PASS|SKIP|FAIL): (\S+)", t)
        if m:
            d = stats.setdefault(m.group(2), {"run": 0, "pass": 0,
                                              "skip": 0, "fail": 0})
            d[m.group(1).lower()] += 1
            continue
        if t.startswith("ok ") or t.startswith("FAIL\t") or t == "FAIL":
            framework.append(t)
    return stats, framework


def required_names_gate(stats, framework, required_names):
    per, problems = {}, []
    for name in required_names:
        d = stats.get(name)
        if d is None:
            problems.append(f"{name}: absent from log (never RUN) -> FAIL")
            per[name] = {"run": 0, "pass": 0, "skip": 0, "fail": 0,
                         "verdict": "fail"}
            continue
        okn = (d["run"] == 1 and d["pass"] == 1 and d["skip"] == 0
               and d["fail"] == 0)
        per[name] = dict(d, verdict="pass" if okn else "fail")
        if not okn:
            problems.append(f"{name}: RUN={d['run']} PASS={d['pass']} "
                            f"SKIP={d['skip']} FAIL={d['fail']} (want "
                            "1/1/0/0)")
    extra_fail = sorted(n for n, d in stats.items() if d["fail"] or d["skip"])
    if extra_fail:
        problems.append(f"non-required names with FAIL/SKIP: {extra_fail}")
    if any(ln == "FAIL" or ln.startswith("FAIL\t") for ln in framework):
        problems.append("framework FAIL line present")
    if not any(ln.startswith("ok ") for ln in framework):
        problems.append("no package 'ok' line present")
    return {"ok": not problems, "per_name": per, "problems": problems,
            "required_count": len(required_names)}


def load_required_names(path: Path):
    if not path.is_file():
        raise GateRefusal(f"required-names file missing: {path}")
    names, seen = [], set()
    for ln in path.read_text().splitlines():
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        if not TEST_NAME_RE.match(s):
            raise GateRefusal(f"invalid required name line: {s!r}")
        if s in seen:
            raise GateRefusal(f"duplicate required name: {s}")
        seen.add(s)
        names.append(s)
    if not names:
        raise GateRefusal(
            f"required-names file has ZERO active names: {path} — a PG-green "
            "claim is impossible without real required names (refusing)")
    return names


# ------------------------------------------------------- shared helpers ----
def step(sid, phase, purpose, argv, write_class="none", gates=(),
         env_schema=None, notes=""):
    return {"id": sid, "phase": phase, "purpose": purpose,
            "argv": list(argv), "write_class": write_class,
            "gates": list(gates), "env_schema": env_schema or {},
            "notes": notes}


def plan_header(tool, phase):
    return {"tool": tool, "phase": phase, "generated_utc": now_utc(),
            "execute_policy": "executable phases need --execute + Root "
                              f"release token listing {phase!r} + all argv "
                              "gates green (first-error stop)",
            "release_file": P26_RELEASE_FILE}


def write_json(path: Path, obj) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=1, sort_keys=True) + "\n")
    os.replace(tmp, path)


def source_gate(before: str, after: str):
    drift = before != after
    return {"ok": not drift, "before": before, "after": after,
            "drift": 1 if drift else 0}


# ----------------------------------------------------------- release token -
def load_release(path: str = None):
    p = Path(path or P26_RELEASE_FILE)
    if not p.is_file():
        return None
    try:
        r = json.loads(p.read_text())
    except ValueError:
        return None
    if (isinstance(r.get("released_for"), list) and r.get("released_by")
            and r.get("expires_utc")):
        if r["expires_utc"] < now_utc():
            return None
        return r
    return None


# ------------------------------------------------------ step engine --------
# Step kinds:
#   {"kind": "argv", "id", "argv": [...], "write_class": "none|mutating",
#    "retry": {"attempts": N, "sleep_s": S},   # ONLY for readiness probes
#    "check": fn(result_dict) -> (ok, msg), "secrets": [..], "purpose": str}
#   {"kind": "call", "id", "fn": callable -> (rc, out), ...same fields}
def run_phase(steps, phase, runner, release=None, execute=False,
              sleep_fn=time.sleep, on_step=None):
    """Run steps in order. FIRST-ERROR STOP: any rc!=0 (after the step's
    explicit bounded retry, readiness only) or failed check aborts the
    phase — later steps never run. Mutating steps need execute+release.
    Returns {"verdict": "pass"|"fail"|"refused", "steps": [...],
             "first_error": {...}|None, "runner_calls": N}."""
    mutating = [s for s in steps if s.get("write_class") != "none"]
    if mutating and (not execute or release is None
                     or phase not in release["released_for"]):
        return {"verdict": "refused",
                "refused": f"{len(mutating)} mutating step(s) need "
                           "--execute + release listing {phase!r}",
                "steps": [], "first_error": None, "runner_calls": 0}
    results = []
    for s in steps:
        violations = check_step_gates(s.get("argv", []))
        if violations:
            return {"verdict": "fail",
                    "first_error": {"id": s["id"], "rc": None,
                                    "detail": f"gate: {violations}"},
                    "steps": results, "runner_calls":
                        sum(1 for r in results if r.get("ran"))}
        attempts_allowed = (s.get("retry") or {}).get("attempts", 1)
        sleep_s = (s.get("retry") or {}).get("sleep_s", 0)
        rc, out, tries = None, "", 0
        while tries < attempts_allowed:
            tries += 1
            if s["kind"] == "argv":
                rc, out = runner(s["argv"],
                                 timeout_s=s.get("timeout_s",
                                                 DEFAULT_TIMEOUT_S))
            else:
                rc, out = s["fn"]()
            if rc == 0:
                break
            if tries < attempts_allowed:
                sleep_fn(sleep_s)
        res = {"id": s["id"], "ran": True, "rc": rc, "tries": tries,
               "output": out}
        if on_step:
            on_step(s, res)
        results.append(res)
        if rc != 0:
            return {"verdict": "fail",
                    "first_error": {"id": s["id"], "rc": rc,
                                    "detail": f"rc={rc} after {tries} "
                                    "attempt(s)"},
                    "steps": results,
                    "runner_calls": sum(1 for r in results if r.get("ran"))}
        check = s.get("check")
        if check:
            ok, msg = check(res)
            res["check_ok"], res["check_msg"] = ok, msg
            if not ok:
                return {"verdict": "fail",
                        "first_error": {"id": s["id"], "rc": rc,
                                        "detail": f"check: {msg}"},
                        "steps": results,
                        "runner_calls": sum(1 for r in results
                                            if r.get("ran"))}
    return {"verdict": "pass", "steps": results, "first_error": None,
            "runner_calls": sum(1 for r in results if r.get("ran"))}


DEFAULT_TIMEOUT_S = 300


def _to_text(stream):
    """TimeoutExpired payloads may be bytes OR str OR None independently
    (root round2 final patch #1) — normalize each BEFORE concatenating."""
    if stream is None:
        return ""
    if isinstance(stream, bytes):
        return stream.decode(errors="replace")
    return str(stream)


def _timeout_output(exc):
    """TimeoutExpired carries partial output as .output (and .stderr);
    never .stdout — accept both spellings defensively."""
    out = getattr(exc, "output", None)
    if out is None:
        out = getattr(exc, "stdout", None)
    return _to_text(out) + _to_text(getattr(exc, "stderr", None))


def real_runner(argv, timeout_s=None):
    """Real subprocess runner — full stdout+stderr, no shell, ALWAYS
    bounded (root round2 #3): a TimeoutExpired returns rc=124 with the
    partial output plus a TIMEOUT marker (recorded, never retried here)."""
    timeout_s = timeout_s or DEFAULT_TIMEOUT_S
    try:
        cp = subprocess.run([str(x) for x in argv], capture_output=True,
                            text=True, timeout=timeout_s)
        return cp.returncode, (cp.stdout or "") + (cp.stderr or "")
    except subprocess.TimeoutExpired as e:
        return 124, (_timeout_output(e) +
                     f"\n[p26] TIMEOUT after {timeout_s}s: "
                     f"{' '.join(str(x) for x in argv[:6])}…\n")


def parse_ports_mapping(text):
    """EXPOSE != host publishing (root round2 #1): {"5432/tcp": null} is
    the NORMAL non-published shape for an image with EXPOSE. Every value
    must be null / empty list / a binding with empty HostIp+HostPort."""
    import json as _json
    t = (text or "").strip()
    if t in ("", "{}", "null", "None"):
        return True, "empty"
    try:
        m = _json.loads(t)
    except ValueError as e:
        return False, f"unparseable ports json: {e}"
    if not isinstance(m, dict):
        return False, f"ports mapping not an object: {type(m).__name__}"
    for port, bindings in m.items():
        if bindings is None or bindings == []:
            continue
        if not isinstance(bindings, list):
            return False, f"{port}: binding {bindings!r} not a list"
        for b in bindings:
            if not isinstance(b, dict):
                return False, f"{port}: binding {b!r} not an object"
            hp, hip = b.get("HostPort"), b.get("HostIp")
            if hp not in ("", None) or hip not in ("", None):
                return False, (f"{port}: published HostPort={hp!r} "
                               f"HostIp={hip!r}")
    return True, "exposed-unpublished"


def parse_empty_bindings(text):
    """HostConfig.PortBindings must be empty ({} / null / all-empty)."""
    ok, detail = parse_ports_mapping(text)
    return ok, detail

#!/usr/bin/env python3
"""P25 FINALIZER v3 — CANONICAL required-stage set + per-stage real
content gates + two-step prepare→seal (hash-manifest LAST).

Root re-pinning (round2 #1 / formal #3, 2026-11-14):
  * NO substring --require matching and NO caller-shrinkable gate: a
    FIXED canonical stage set is ALWAYS enforced; every stage maps to
    an EXACT selected-index key; dropping/not-selecting any stage
    refuses. `--require` may only ADD exact-key assertions.
  * NO "any same-dir JSON with ok:true" substitution: each canonical
    stage carries its own VERDICT SCHEMA + REAL-CONTENT validator:
      - unit: the accepted connector evidence (attempt20 full suite:
        exactly 38 `ok` / 0 FAIL / rc0; attempt18 race: 4 ok / rc0) —
        raw/rc files RE-HASHED read-only from the pinned evidence
        tree, never rerun;
      - pg-isolated: bindings + the THREE real stage attempt trees
        (.cmd/.raw/.rc/.meta/.source-gate.rc/.version-gate.rc each
        re-hashed; every rc file must read 0 and equal the declared
        rc; raw: 0 FAIL, >=1 ok; names chain: sibling required-names
        file ACTIVE count == 15 == bindings; unified string
        "old72 + new15 = 87"; candidate source ==
        a1bff07f…ffd27);
      - build: 6 components (latest attempt each) rc 0 + cmd/raw/meta
        present, re-hashed;
      - preflight: base+supp — base verdict (all_ok false, honest) +
        FINAL verify-csupp verdict all_ok TRUE with the c-supplement
        attempt-summary RE-HASHED against its recorded sha256;
      - window: exact HTTP case set h1..h15, unique, each pass, each
        with a non-empty gate list;
      - regressions: p1pre9=9 / p23=8 / p22=11 / p24=7 / p1post9=9 —
        exact counts, unique passed-name lists;
      - p21abc: REAL result shape (official/partial/counts 3-0/three
        real case keys pass) + suffix 8-hex NOT in the historical
        suffix set (identity);
      - aux-down: BOTH archived quiet raws re-validated (strict
        non-bool ints);
      - final-images: base3 images == root-pinned full hexes; p25 +
        old-aux services explicit int 0;
      - postfacts/persist/source-freeze/standalone: their real
        artifact schemas (see validators);
  * prepare-internal mandatory (no selection can waive): the old P24
    archive re-verified BY RE-HASH (859 files == manifest + 27 tools
    == 16-tools-sha-final), the full tools tree SHA, and the frozen
    P24 seal inputs.
  * prepare→seal→reverify two-step seal unchanged (hash-manifest LAST;
    post-seal EV zero-write; sealer stdout/rc OUTSIDE the EV).
"""

import argparse
import hashlib
import importlib.util
import json
import re
import sys
sys.dont_write_bytecode = True  # Pure validation must not mutate copied evidence/tools.
import time
from pathlib import Path

SECRET_PATTERNS = [
    ("dsn", re.compile(r"postgres(?:ql)?://[^\s\"']+")),
    ("jwt", re.compile(r"eyJ[A-Za-z0-9_\-]{10,}\.[A-Za-z0-9_\-]{10,}"
                       r"\.[A-Za-z0-9_\-]{10,}")),
    ("bearer", re.compile(r"(?i)bearer\s+[A-Za-z0-9._\-]{8,}")),
    ("env-secret-assign", re.compile(
        r"(?i)^(OF_P1_PG_PASSWORD|OF_P1_JWT_SECRET|OF_P1_CREDENTIAL_KEY"
        r"|OF_P1_SERVICE_JWT)\s*=\\?\"?[A-Za-z0-9+/=_\-]{4,}")),
]

HASH_MANIFEST = "hash-manifest.json"
TOOLS_EXCLUDE_DIRS = {"__pycache__", "results", "build"}

HERE = Path(__file__).resolve().parent

# ---- root-pinned reference values -------------------------------------
PG_GREEN_SOURCE = ("a1bff07f3760fce7b5661b169ba26a165712c9500"
                   "35d4e4ec9948639d51ffd27")
EVIDENCE_ROOT = Path("/private/tmp/openfoundry-p25-repair-evidence")
PREFLIGHT_BASE = HERE / "results" / "2026-11-14-p25-preflight" / \
    "attempt-2"
UNIT_FILES = {
    "unit_log": "connector/attempt20-round2-build-vet-fullunit.log",
    "unit_rc": "connector/attempt20-round2-build-vet-fullunit.rc",
    "race_log": "connector/attempt18-reviewfix-race.log",
    "race_rc": "connector/attempt18-reviewfix-race.rc",
}
UNIT_EXPECT = {"unit_ok": 38, "race_ok": 4}
BUILD_COMPONENTS = ("render-aux", "build-base3", "build-oldaux2",
                    "build-newaux1", "cache-python312alpine",
                    "source-gate")
ANCHOR_IMAGES = {
    # SINGLE-LINE literals (the derive lesson: hand-splitting hex
    # corrupted the connector anchor once already — caught by G1-1)
    "connector": "fe3208b3fcbd4d5fe428647e106d6ba6aa5b71fd94e9ba51144a9565258dc4c3",
    "dataset": "12f3274daf1e68e8445bfb62329a0703896af031d79647f957fb1970768200db",
    "pipeline": "dd350b804842235c8fd2fd11e8366350a456376b8ca3fc4f9051b5342ebee981",
}
for _k, _v in ANCHOR_IMAGES.items():
    if not re.match(r"^[0-9a-f]{64}$", _v):        # fail fast on split
        raise RuntimeError(f"ANCHOR_IMAGES[{_k}] corrupted: {_v!r}")
H_CASES = tuple(f"h{i}" for i in range(1, 16))     # tools-design §5
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                     r"[0-9a-f]{4}-[0-9a-f]{12}$")
SUFFIX_RE = re.compile(r"^[0-9a-f]{8}$")
RFC3339_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
                        r"(\.\d+)?Z$")
P21_CASES = frozenset({"heartbeat_progress", "lease_loss_fencing",
                       "terminal_commit_fault"})
INFORMATIONAL = ("06b-preimpl-gate", "informational")


def sha(p: Path) -> str:
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()


def _artifact_identity(ctx, p, raw):
    rec = ctx.get("original_records", {}).get(str(p))
    digest = hashlib.sha256(raw).hexdigest()
    if rec and rec.get("copy") == digest:
        return rec["original"], rec.get("original_bytes", len(raw))
    return digest, len(raw)


def _si(v):
    return isinstance(v, int) and not isinstance(v, bool)


def scan_secrets(ev: Path):
    """Whole-EV secret scan; ONLY the EV-ROOT hash-manifest excluded
    (nested same-named files ARE scanned)."""
    hits = []
    for p in sorted(ev.rglob("*")):
        if not p.is_file() or p == ev / HASH_MANIFEST:
            continue
        try:
            text = p.read_text(errors="ignore")
        except OSError:
            continue
        for i, line in enumerate(text.splitlines(), 1):
            for name, pat in SECRET_PATTERNS:
                if pat.search(line):
                    hits.append({"file": str(p.relative_to(ev)),
                                 "line": i, "kind": name})
    return hits


def _truthy_verdict(obj) -> bool:
    if not isinstance(obj, dict):
        return False
    for key in ("all_ok", "final_ok", "ok"):
        v = obj.get(key)
        if isinstance(v, bool):
            return v
    return False


def _rc_ok(path: Path) -> bool:
    try:
        v = path.read_text().strip().lower()
    except OSError:
        return False
    return v in ("0", "rc=0")


def _count_lines(path: Path, pat) -> int:
    n = 0
    for line in path.read_text(errors="ignore").splitlines():
        if re.match(pat, line):
            n += 1
    return n


def _stage_json(step_dir: Path):
    """Structured verdict/content object for a stage: stdout LAST
    non-empty line JSON first, then any *verdict*/*.report*/*
    *manifest*/*.json artifact in the dir. Returns (obj|None, where)."""
    so = step_dir / "stdout.log"
    if so.is_file():
        lines = [l for l in so.read_text(errors="ignore").splitlines()
                 if l.strip()]
        if lines:
            try:
                obj = json.loads(lines[-1])
                if isinstance(obj, dict):
                    return obj, "stdout:last-line"
            except ValueError:
                pass
    for pat in ("*verdict*.json", "*report*.json", "*manifest*.json",
                "*-result*.json", "*declaration*.json"):
        for p in sorted(step_dir.rglob(pat)):
            if not p.is_file():
                continue
            try:
                obj = json.loads(p.read_text())
            except ValueError:
                continue
            if isinstance(obj, dict):
                return obj, p.name
    return None, "none"


# ---- per-stage REAL-CONTENT validators --------------------------------
# Each returns (reasons, imported) — imported records every consumed
# artifact's sha256 into the prepare manifest (import-green ledger).

def _imp(root: Path, rel: str, imported: dict, prefix: str):
    p = Path(root) / rel
    imported[f"{prefix}{rel}"] = sha(p)
    return p


def v_unit(step_dir, ctx):
    reasons, imported = [], {}
    root = ctx["evidence_root"]
    for key, rel in UNIT_FILES.items():
        p = root / rel
        if not p.is_file():
            reasons.append(f"unit: accepted evidence missing: {rel}")
            return reasons, imported
        _imp(root, rel, imported, "unit:")
    ul = root / UNIT_FILES["unit_log"]
    rl = root / UNIT_FILES["race_log"]
    ok_n = _count_lines(ul, r"^ok ")
    fail_n = _count_lines(ul, r"^(FAIL|--- FAIL)")
    if ok_n != UNIT_EXPECT["unit_ok"] or fail_n != 0:
        reasons.append(f"unit: full-suite log has {ok_n} ok / "
                       f"{fail_n} FAIL != 38/0")
    if not _rc_ok(root / UNIT_FILES["unit_rc"]):
        reasons.append("unit: accepted rc file != 0")
    rok = _count_lines(rl, r"^ok ")
    rfail = _count_lines(rl, r"^(FAIL|--- FAIL)")
    if rok != UNIT_EXPECT["race_ok"] or rfail != 0:
        reasons.append(f"unit: race log has {rok} ok / {rfail} FAIL "
                       "!= 4/0")
    if not _rc_ok(root / UNIT_FILES["race_rc"]):
        reasons.append("unit: race rc file != 0")
    return reasons, imported


PG_EXACT_VERDICTS = {
    "new-normal": "new-only distinct=15 bad=0 go_rc=0 (pinned)",
    "new-race": "new-only distinct=15 bad=0 go_rc=0 (pinned)",
    "unified": "unified distinct=87 bad=0 go_rc=0 (pinned)",
}


def _pg_required_names():
    return [l.strip() for l in
            (HERE / "p25-required-names.txt").read_text().splitlines()
            if l.strip() and not l.startswith("#")]


def v_pg(step_dir, ctx):
    """Consume the accepted r17 bytes and exact-check saved raw only."""
    import subprocess
    reasons, imported = [], {}
    canonical = HERE / "pg-candidate-bindings-r17.json"
    if sha(canonical) != "08b4ab4817c593d2a99816b3c5f15d24e41747041a70f39f26a5a85a021bb761" or sha(HERE / "ARCHIVE-PG-INPUTS.json") != "b33afbef78200c54809789cbd1c79edeba528a8777d52d4e40ac524839851eb0":
        return ["pg-isolated: fixed r17/provenance binding SHA mismatch"], imported
    binds = list(step_dir.rglob("*pg-candidate-bindings*.json"))
    if len(binds) != 1 or sha(binds[0]) != sha(canonical):
        return ["pg-isolated: expected ONE byte-exact accepted r17 binding"], imported
    b = json.loads(canonical.read_text())
    refs = json.loads((HERE / "ARCHIVE-PG-INPUTS.json").read_text())["files"]
    for path, entry in refs.items():
        p = Path(path)
        if not p.is_file() or _artifact_identity(ctx, p, p.read_bytes()) != (entry["sha256"], entry["bytes"]):
            reasons.append(f"pg-isolated: accepted artifact drift: {path}")
        else:
            imported["input:" + path] = entry["sha256"]
    checker = HERE / "pg_exact_check_p25.py"
    if sha(checker) != b["checker_sha256"]:
        reasons.append("pg-isolated: checker differs from r17")
    source_files = {"repo": "internal/repo/checkpoint_integration_test.go",
                    "handlers": "internal/handlers/checkpoint_integration_test.go"}
    for name, rel in source_files.items():
        p = REPO_ROOT / "services/connector-management-service" / rel
        if not p.is_file() or sha(p) != b["test_file_sha256"][name]:
            reasons.append(f"pg-isolated: {name} PG test bytes differ from r17")
    siblings = HERE.parent
    name_files = [siblings / "business-platform-p1/run_tests.sh",
                  siblings / "business-platform-p2-1/run_tests_p21.sh",
                  siblings / "business-platform-p2-2/p22-required-names.txt",
                  siblings / "business-platform-p2-3/p23-required-names.txt",
                  siblings / "business-platform-p2-4/p24-required-names.txt",
                  HERE / "p25-required-names.txt"]
    for st in b["stages"]:
        base = st["attempt_base"]
        meta = Path(base + ".meta").read_text()
        expected = "go_version_actual=go version " + st["go_version"]
        cmd = Path(base + ".cmd").read_text()
        cgo = "CGO_ENABLED=" + st["cgo"]
        if expected not in meta.splitlines() or (st["stage"] != "unified" and cgo not in cmd):
            reasons.append(f"pg-isolated: {st['stage']} actual Go/CGO mismatch")
        argv = [sys.executable, "-I", str(checker), "--meta", base + ".meta",
                "--expect-source-sha", PG_GREEN_SOURCE]
        if st["stage"] != "unified":
            argv.append("--new-only")
        argv += [base + ".raw", base + ".rc", *map(str, name_files)]
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=30)
        expected_n = 87 if st["stage"] == "unified" else 15
        if proc.returncode or f"distinct={expected_n} bad=0 go_rc=0" not in proc.stdout:
            reasons.append(f"pg-isolated: {st['stage']} exact checker refused: {proc.stdout[-1200:]}")
        if st["stage"] == "unified" and "required-test guard status: 0" not in Path(base + ".raw").read_text():
            reasons.append("pg-isolated: unified required-test guard != 0")
    return reasons, imported


def v_build(step_dir, ctx):
    """BUILD import via build-candidate-bindings-r18.json: the ROOT-
    accepted EXACT green attempts ONLY (6 groups; the failed 142634Z
    round is preserved history, NOT selected). NO ls -t / latest /
    wildcard selection: every cmd/raw/rc/meta is consumed at its
    pinned path and re-hashed (sha256 AND bytes) against the binding;
    rc file content == 0 == real_rc; hard gate source sha == a1bff…"""
    reasons, imported = [], {}
    b = None
    for p in sorted(step_dir.rglob("*build-candidate-bindings*.json")):
        try:
            b = json.loads(p.read_text())
            imported[f"build:bindings:{p.name}"] = sha(p)
            break
        except ValueError:
            reasons.append(f"build: {p.name} not JSON")
    if b is None:
        bpath = HERE / "build-candidate-bindings-r18.json"
        if not bpath.is_file():
            return (["build: r18 bindings artifact missing"], imported)
        b = json.loads(bpath.read_text())
        imported["build:bindings-r18"] = sha(bpath)
    if hashlib.sha256(json.dumps(b, sort_keys=True).encode()).hexdigest() != hashlib.sha256(json.dumps(json.loads((HERE / "build-candidate-bindings-r18.json").read_text()), sort_keys=True).encode()).hexdigest() or sha(HERE / "build-candidate-bindings-r18.json") != "a4e5774152c4ea9146f34dcc5968e987921e8c3e6f310c855137726acac9feef":
        reasons.append("build: binding differs from ROOT accepted r18")
    groups = b.get("selected_green_attempts") or {}
    want_groups = {"render-aux", "build-base3", "build-oldaux2",
                   "build-newaux1", "cache-python", "source-gate"}
    if set(groups) != want_groups:
        reasons.append(f"build: selected groups {sorted(groups)} != "
                       f"the exact 6 {sorted(want_groups)}")
    for name, entry in sorted(groups.items()):
        for kind in ("cmd", "raw", "rc", "meta"):
            ref = (entry or {}).get(kind) or {}
            p = Path(ref.get("path") or "")
            try:
                p.relative_to(ctx["evidence_root"])
            except ValueError:
                reasons.append(f"build: {name}.{kind} path outside "
                               "pinned evidence root")
                continue
            if not p.is_file():
                reasons.append(f"build: {name}.{kind} missing: "
                               f"{p.name}")
                continue
            raw = p.read_bytes()
            got, original_bytes = _artifact_identity(ctx, p, raw)
            if got != ref.get("sha256") or original_bytes != ref.get(
                    "bytes"):
                reasons.append(f"build: {name}.{kind} sha/bytes "
                               "mismatch vs binding")
                continue
            imported[f"build:{name}:{kind}"] = got
            if kind == "rc" and raw.decode(errors="ignore").strip() \
                    not in ("0", "rc=0"):
                reasons.append(f"build: {name} rc file != 0")
            if kind == "rc" and entry.get("real_rc") not in (None, 0):
                reasons.append(f"build: {name} real_rc != 0")
    hg = b.get("hard_gates") or {}
    if hg.get("source_gate_rc") != 0 or hg.get(
            "source_content_sha256") != PG_GREEN_SOURCE:
        reasons.append("build: hard gates != source rc0 / a1bff…"
                       "ffd27")
    return reasons, imported


PREFLIGHT_ACCEPTED = Path(
    "/Users/allen/.codex/visualizations/2026/11/13/"
    "01a1d419-a586-76c2-b72b-8b773a80c7b6/handoffs/2026-11-14-p25/"
    "preflight-accepted.json")


def v_preflight(step_dir, ctx):
    """PREFLIGHT import via the root-issued preflight-accepted.json
    (OFFICIALLY CLOSED — no disposition pending): status, candidate
    source, and ALL pinned artifacts re-hashed on disk against the
    recorded sha256s; the FINAL verdict (base2 verify-csupp.stdout —
    NOT the historical verify.stdout) must parse all_ok TRUE; its rc
    file must read 0. C is NOT exempted (the c-supplement chain is
    part of the accepted artifact set)."""
    reasons, imported = [], {}
    acc_path = ctx["preflight_accepted"]
    if not acc_path.is_file():
        return (["preflight: root-issued preflight-accepted.json "
                 "missing"], imported)
    try:
        acc = json.loads(acc_path.read_text())
    except ValueError as exc:
        return ([f"preflight: accepted.json not JSON: {exc}"],
                imported)
    imported["preflight:accepted-spec"] = sha(acc_path)
    if acc.get("status") != "first-transition-preflight-accepted":
        reasons.append(f"preflight: status {acc.get('status')!r} != "
                       "first-transition-preflight-accepted")
    if acc.get("candidate_source_sha256") != PG_GREEN_SOURCE:
        reasons.append("preflight: candidate source != a1bff…ffd27")
    verdict_path = None
    for path, want in sorted((acc.get("artifacts") or {}).items()):
        p = Path(path)
        if not p.is_file():
            reasons.append(f"preflight: accepted artifact missing: "
                           f"{Path(path).name}")
            continue
        got = sha(p)
        if got != want:
            reasons.append(f"preflight: accepted artifact sha "
                           f"mismatch: {Path(path).name}")
            continue
        # basename-collision-proof ledger key (two attempt summaries
        # share …/attempt-1/attempt-summary.json — the GRANDPARENT
        # disambiguates collect vs c-supplement; a basename key would
        # silently drop one)
        key = "preflight:" + str(p)
        if key in imported:
            reasons.append("preflight: accepted artifact key "
                           f"collision: {key}")
        imported[key] = got
        if path.endswith("verify-csupp.stdout"):
            verdict_path = p
        # INNER raw-ledger recompute for the accepted attempt
        # summaries (does NOT reopen the accepted facts — verifies the
        # recorded ledger against the files it claims; an unledgered
        # disk file is allowed ONLY if it is itself accepted-pinned,
        # e.g. c-supplement's base-binding.json)
        if p.name == "attempt-summary.json":
            try:
                inner = json.loads(p.read_text())
            except ValueError:
                reasons.append(f"preflight: {key} not JSON")
                continue
            ledger = inner.get("artifacts") or {}
            adir = p.parent
            disk = {str(q.relative_to(adir)) for q in adir.rglob("*")
                    if q.is_file()
                    and q.name != "attempt-summary.json"}
            extra = disk - set(ledger)
            pinned = {Path(k).resolve() for k in (acc.get("artifacts") or {})}
            if not {(adir / rel).resolve() for rel in extra} <= pinned:
                reasons.append(f"preflight: {key} inner ledger has "
                               f"unpinned extra files: "
                               f"{sorted(extra)[:3]}")
            if set(ledger) - disk:
                reasons.append(f"preflight: {key} inner ledger claims "
                               "missing files")
            for rel, ent in sorted(ledger.items()):
                q = adir / rel
                if not q.is_file():
                    reasons.append(f"preflight: {key} inner artifact "
                                   f"missing {rel}")
                    continue
                raw = q.read_bytes()
                imported["input:" + str(q)] = sha(q)
                if _artifact_identity(ctx, q, raw) != (ent.get("sha256"), ent.get("bytes")):
                    reasons.append(f"preflight: {key} inner ledger "
                                   f"re-hash mismatch {rel}")
    if verdict_path is None:
        reasons.append("preflight: final verdict (verify-csupp."
                       "stdout) not among accepted artifacts")
    else:
        try:
            sv = json.loads(verdict_path.read_text())
            if sv.get("all_ok") is not True:
                reasons.append("preflight: FINAL verdict all_ok is "
                               "not true")
        except ValueError:
            reasons.append("preflight: final verdict not JSON")
        rc = verdict_path.with_suffix(".rc")
        if not rc.is_file() or not _rc_ok(rc):
            reasons.append("preflight: verify-csupp.rc != 0")
    return reasons, imported


def v_swap(step_dir, ctx):
    # The actual swap CLI emits one complete, pretty-printed JSON record.
    try:
        obj = json.loads((step_dir / "stdout.log").read_text())
        if not isinstance(obj, dict):
            obj = None
    except (OSError, ValueError):
        obj = None
    if obj is None:
        obj, where = _stage_json(step_dir)
    if obj is None:
        return (["swap: no structured record"], {})
    reasons = []
    if not RFC3339_RE.match(str(obj.get("target_boot") or "")):
        reasons.append("swap: target_boot not RFC3339")
    img = str(obj.get("target_image") or "")
    if not re.match(r"^sha256:[0-9a-f]{64}$", img):
        reasons.append("swap: target_image not full sha256:hex64")
    return reasons, {}


def _scenario_ast(path):
    import ast
    tree = ast.parse(Path(path).read_text())
    return next(n.value for n in tree.body if isinstance(n, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == "SCENARIOS" for t in n.targets))


def _v_http_phase(step_dir, ctx, phase):
    import ast
    p = ctx["ev"] / ("window-result.json" if phase == "healthy" else "tail-result.json")
    if not p.is_file():
        return ["window: actual HTTP window-result.json missing"], {}
    obj = json.loads(p.read_text())
    specs = ast.literal_eval(_scenario_ast(HERE / "smoke_p25.py"))
    expected = {x["id"]: x for x in specs if x["phase"] == phase and x["id"] != "h10"}
    cases = obj.get("scenarios") or []
    reasons, imported = [], {"input:" + str(p): sha(p)}
    ids = [c.get("id") for c in cases]
    if set(ids) != set(expected) or len(ids) != len(set(ids)):
        reasons.append("window: exact actual healthy scenario set mismatch")
    if obj.get("phase") != phase or obj.get("failed") != [] or (obj.get("quiet") or {}).get("ok") is not True:
        reasons.append("window: actual phase/final failure/quiet verdict refused")
    evidence_dir = Path((obj.get("attempts") or {}).get("evidence_dir") or "")
    try:
        evidence_dir.resolve().relative_to(ctx["ev"].resolve())
    except ValueError:
        return reasons + ["window: evidence directory outside EV"], imported
    for case in cases:
        ident = case.get("id")
        gates = case.get("gates") or []
        names = {g.get("gate") for g in gates if isinstance(g, dict)}
        required = set((expected.get(ident) or {}).get("gates", []))
        if case.get("failed") is not False or not required <= names or any(not isinstance(g, dict) or g.get("ok") is not True for g in gates):
            reasons.append(f"window: {ident} mandatory actual gates/results refused")
        matches = []
        for raw in evidence_dir.glob(f"{ident}-attempt-*.json"):
            try:
                if json.loads(raw.read_text()) == case:
                    matches.append(raw)
            except ValueError:
                continue
        if len(matches) != 1:
            reasons.append(f"window: {ident} exact immutable scenario-result raw missing/duplicate")
        else:
            imported["input:" + str(matches[0])] = sha(matches[0])
    return reasons, imported


def v_window(step_dir, ctx):
    aggregate = ctx["ev"] / "window-aggregate.json"
    if aggregate.is_file():
        try:
            obj = json.loads(aggregate.read_text())
            if obj.get("schema") != "p25-healthy-aggregate-v1":
                return ["window: typed aggregate schema mismatch"], {}
            # Aggregate and its nested quiet/ledger verifiers explicitly
            # accept a read resolver. Keep their logical metadata paths intact;
            # only actual reads use this module's rebound Path constructor.
            import pathlib
            saved_path = pathlib.Path
            try:
                if ctx.get("logical_path_class") is not None:
                    pathlib.Path = ctx["logical_path_class"]
                helper = loadmod_sibling("aggregate_healthy_p25.py")
                verdict = helper.verify_aggregate(aggregate, resolve_path=lambda value: Path(value))
            finally:
                pathlib.Path = saved_path
            if verdict.get("ok") is not True:
                return ["window: actual aggregate source replay refused: " + str(verdict.get("problems", verdict.get("reasons", [])))], {}
            return [], {"input:" + str(aggregate): sha(aggregate),
                        "window:aggregate-verifier": sha(HERE / "aggregate_healthy_p25.py")}
        except Exception as exc:
            return [f"window: actual aggregate verifier failed: {type(exc).__name__}: {exc}"], {}
    return _v_http_phase(step_dir, ctx, "healthy")


def _v_count(n, label):
    """Frozen-suite real result shape + exact source-defined scenario names."""
    def v(step_dir, ctx):
        import ast
        suite = "p1" if label in ("regressions-p1pre9", "p1post9") else label.rsplit("-", 1)[-1]
        paths = {"p1": HERE.parent / "business-platform-p1/smoke/smoke.py",
                 "p22": HERE.parent / "business-platform-p2-2/smoke_p22.py",
                 "p23": HERE.parent / "business-platform-p2-3/smoke_p23.py",
                 "p24": HERE.parent / "business-platform-p2-4/smoke_p24.py"}
        registry = _scenario_ast(paths[suite])
        if isinstance(registry, ast.Dict):
            names = [ast.literal_eval(x.elts[0]) for x in registry.values]
        else:
            names = [ast.literal_eval(x.elts[0]) for x in registry.elts]
        candidates = []
        for p in step_dir.glob("*.json"):
            try:
                obj = json.loads(p.read_text())
                if isinstance(obj.get("results"), dict):
                    candidates.append((obj, p))
            except (ValueError, AttributeError):
                continue
        if not candidates:
            return [f"{label}: actual frozen suite result JSON missing"], {}
        obj, p = candidates[0]
        reasons = []
        if len(names) != n or set(obj["results"]) != set(names) or any(v.get("status") != "pass" for v in obj["results"].values()):
            reasons.append(f"{label}: exact frozen scenario set/status mismatch")
        if obj.get("counts", {}).get("pass") != n or obj.get("counts", {}).get("fail") != 0:
            reasons.append(f"{label}: actual counts != {n}/0")
        if suite != "p1" and (obj.get("official") is not True or obj.get("partial") is not False):
            reasons.append(f"{label}: official/partial verdict refused")
        if not re.fullmatch(r"[0-9a-f]{10}" if suite == "p24" else r"[0-9a-f]{8}", str(obj.get("suffix") or "")):
            reasons.append(f"{label}: actual suffix missing/invalid")
        if obj.get("suffix") in ctx.get("historical_suffixes", set()):
            reasons.append(f"{label}: historical suffix reused")
        for raw in ("cmd.txt", "stdout.log", "stderr.log", "rc"):
            q = step_dir / raw
            if not q.is_file() or (raw in ("cmd.txt", "stdout.log") and q.stat().st_size == 0) or (raw == "rc" and q.read_text().strip() != "0"):
                reasons.append(f"{label}: original cmd/rc/raw missing or failed: {raw}")
        return reasons, {"input:" + str(p): sha(p)}
    return v


def v_reread(step_dir, ctx):
    ev = ctx["ev"]
    try:
        before_dir = Path((ev / "00-baseline-pre-path.txt").read_text().strip())
        before = json.loads((before_dir / "manifest.json").read_text())
        after = json.loads((ev / "12-reread-after-manifest.json").read_text())
        report = json.loads((ev / "12-reread-compare/compare.json").read_text())
        spec = importlib.util.spec_from_file_location("p22_compare_archive", HERE.parent / "business-platform-p2-2/compare_baseline.py")
        comparison = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(comparison)
        # Compare the same JSON wire representation that the frozen CLI
        # writes (its in-memory target counts are tuples, serialized as lists).
        actual = json.loads(json.dumps(comparison.compare(before, after, "restart")))
        if actual != report or actual.get("ok") is not True or actual.get("source_gates") != []:
            return ["reread-artifacts: actual baseline compare replay refused/differs"], {}
        for name in ("p21_bc_projection", "p21_bc_targets", "p1_old_artifact", "p1_old_buildjob"):
            if (actual.get(name) or {}).get("ok") is not True:
                return [f"reread-artifacts: mandatory section {name} missing/refused"], {}
        return [], {"input:" + str(before_dir / "manifest.json"): sha(before_dir / "manifest.json"),
                    "input:" + str(ev / "12-reread-after-manifest.json"): sha(ev / "12-reread-after-manifest.json")}
    except (OSError, ValueError, KeyError) as exc:
        return [f"reread-artifacts: actual baseline artifacts unavailable: {exc}"], {}


def _v_h10_command_records(report, root):
    import shlex
    reasons, seen = [], set()
    for rec in report.get("commands") or []:
        try:
            cmd = Path(rec["cmd_file"])
            if cmd.parent in seen:
                reasons.append("h10 duplicate command record")
            seen.add(cmd.parent)
            for key, filename in (("argv_file", "argv.json"), ("rc_file", "rc"), ("artifact", "stdout.raw"), ("stderr_artifact", "stderr.raw")):
                if Path(rec[key]) != cmd.parent / filename:
                    reasons.append("h10 command raw member path does not belong to its original step")
            argv = json.loads(Path(rec["argv_file"]).read_text())
            if cmd.read_text() != rec["cmd"] + "\n" or shlex.join(argv) != rec["cmd"]:
                reasons.append("h10 original command/argv bytes differ from report")
            for path_key, digest_key in (("cmd_file", "cmd_sha256"), ("rc_file", "rc_sha256"), ("artifact", "artifact_sha256"), ("stderr_artifact", "stderr_sha256")):
                if sha(Path(rec[path_key])) != rec[digest_key]:
                    reasons.append(f"h10 original {path_key} SHA mismatch")
            if Path(rec["rc_file"]).read_text().strip() != str(rec["rc"]):
                reasons.append("h10 original rc differs from report")
            if json.loads((cmd.parent / "record.json").read_text()) != rec:
                reasons.append("h10 original command record differs from report")
        except (OSError, ValueError, KeyError, TypeError) as exc:
            reasons.append(f"h10 original command provenance missing: {exc}")
    disk = {q.parent for q in root.glob("[0-9][0-9]-*/record.json")}
    if not seen or seen != disk:
        reasons.append("h10 command record set is not complete/bidirectional")
    return reasons


class _H10ReplayAPI:
    """Strict ordered replay of saved HTTP responses; never constructs IO."""
    def __init__(self, records):
        self.records, self.index = records, 0

    def call(self, base, method, path, **kwargs):
        from urllib.parse import urlsplit
        if self.index >= len(self.records):
            raise ValueError("h10 raw replay exhausted")
        rec = self.records[self.index]
        self.index += 1
        if rec.get("base") != base or rec.get("method") != method or urlsplit(str(rec.get("path"))).path != path or rec.get("status") != 200 or rec.get("error") is not None:
            raise ValueError("h10 raw HTTP method/base/path/status/order mismatch")
        body = rec.get("resp_body")
        if not isinstance(body, str) or hashlib.sha256(body.encode()).hexdigest() != rec.get("resp_sha256") or len(body.encode()) != rec.get("resp_bytes"):
            raise ValueError("h10 raw HTTP body SHA/bytes mismatch")
        return {"status": 200, "json": json.loads(body), "error": None}

    def finish(self):
        if self.index != len(self.records):
            raise ValueError("h10 raw replay has unconsumed HTTP records")


def _v_h10_baseline(report, ctx, engine):
    """Reconstruct before/after protected facts from ordered HTTP bytes."""
    import types
    from urllib.parse import urlsplit
    reasons = []
    def load_records(entries):
        records, seen = [], set()
        for entry in entries:
            p = Path(entry["path"])
            if str(p) in seen or sha(p) != entry["sha256"]:
                raise ValueError("h10 baseline raw-record duplicate/SHA mismatch")
            seen.add(str(p))
            records.append(json.loads(p.read_text()))
        return records
    def handles(path):
        return json.loads(Path(path).read_text())["handles"]
    key = lambda row: tuple(row.get(k) for k in ("def_id", "source_id", "run_id", "run_status", "blocked_reason"))
    triple = lambda row: tuple(row.get(k) for k in ("def_id", "source_id", "run_id"))
    try:
        for path, digest in (report.get("inputs") or {}).items():
            if sha(Path(path)) != digest:
                reasons.append("h10 actual input SHA drift: " + path)
        bp, ap = Path(report["baseline_file"]), Path(report["baseline_after_file"])
        if sha(bp) != report["baseline_sha256"] or sha(ap) != report["baseline_after_sha256"]:
            return reasons + ["h10 baseline before/after file SHA mismatch"]
        before, after = json.loads(bp.read_text()), json.loads(ap.read_text())
        ag_path = ctx["ev"] / "window-aggregate.json"
        ag = json.loads(ag_path.read_text())
        input_map = {str(Path(p)): h for p, h in report["inputs"].items()}
        if input_map.get(str(ag_path)) != sha(ag_path):
            reasons.append("h10 baseline not bound to actual accepted healthy aggregate")
        original_path = Path(ag["original_failure_preserved"]["summary_path"])
        if sha(original_path) != ag["original_failure_preserved"]["summary_sha256"]:
            reasons.append("h10 original failedhealthy summary SHA drift")
        original = json.loads(original_path.read_text())
        owners = ag["final_owner_roster"]
        if len(owners) != 2 or sorted(map(key, owners)) != sorted(map(key, original["final_owner_roster"])):
            reasons.append("h10 baseline not exact two original failedhealthy owners")
        if before["origins"] != ag["retained_owner_origins"] or any(o.get("origin") != "failedhealthy" or Path(o.get("source_summary")) != original_path or o.get("source_summary_sha256") != sha(original_path) for o in before["origins"]):
            reasons.append("h10 baseline original failedhealthy origins not preserved")
        imported = handles(ag["attempts"]["handles_file"])
        all_handles = handles(report["merged_handles_file"])
        expected_defs = sorted({h["ref"] for h in imported if h.get("kind") in ("def", "h10-def") and h.get("ok")})
        all_defs = {h["ref"] for h in all_handles if h.get("kind") in ("def", "h10-def") and h.get("ok")}
        held = json.loads(Path(report["crash_record"]).read_text())["held"]
        if before["defs"] != expected_defs or all_defs != set(expected_defs) | {held["def_id"]}:
            reasons.append("h10 protected baseline/after whole def handle sets mismatch")
        before_entries, after_entries = before["raw_records"], after["raw_records"]
        if after_entries[:len(before_entries)] != before_entries:
            reasons.append("h10 before raw history is not an unchanged after prefix")
        raw_before, raw_after = load_records(before_entries), load_records(after_entries)
        record_dir = Path(report["merged_handles_file"]).parent
        disk = {str(p) for p in record_dir.glob("h10-prepare-attempt-*.json")}
        if {str(Path(e["path"])) for e in after_entries} != disk:
            reasons.append("h10 raw record ledger is not bidirectional")
        n = len(expected_defs)
        if len(raw_before) != 6 * n:
            reasons.append("h10 before raw call count != rosters+snapshot exact whole set")
        before_handles = [h for h in all_handles if h.get("kind") not in ("def", "h10-def") or h.get("ref") in expected_defs]
        instance = engine.P25Engine(None, {}, types.SimpleNamespace(handles=before_handles))
        api = _H10ReplayAPI(raw_before)
        rosters = instance.final_rosters(api)
        snapshot, ok = instance._h10_snapshot(api, expected_defs)
        api.finish()
        if not ok or rosters["problems"] or rosters["run_roster"] or rosters["owner_roster"] != before["owner_roster"] or snapshot != before["snapshot"] or sorted(map(key, rosters["owner_roster"])) != sorted(map(key, owners)):
            reasons.append("h10 before facts differ from ordered raw replay / original owner baseline")
        # Anchor old owner protected fields to the original failed-window raw
        # history as well: editing both before/after summaries is insufficient.
        original_records = load_records(ag["sources"]["original"]["evidence_records"])
        latest = {}
        for rec in original_records:
            if rec.get("method") == "GET" and rec.get("base") == "connector":
                latest[urlsplit(str(rec.get("path"))).path] = rec
        expected_old_calls = []
        owner_defs = sorted(o["def_id"] for o in owners)
        for did in owner_defs:
            for route_name in ("checkpoint_view", "sync_runs", "sync_get"):
                expected_old_calls.append(latest[engine.route(route_name, id=did)])
        old_api = _H10ReplayAPI(expected_old_calls)
        original_snapshot, original_ok = instance._h10_snapshot(old_api, owner_defs)
        old_api.finish()
        if not original_ok or any(snapshot[did] != original_snapshot[did] for did in owner_defs):
            reasons.append("h10 old protected owner fields differ from original failedhealthy raw history")
        srcs = sorted({h.get("config", {}).get("sync", {}).get("query_params", {}).get("src") for h in all_handles if h.get("kind") == "source" and h.get("ok")})
        if any(src not in engine.SRC_WINDOWS for src in srcs):
            reasons.append("h10 after source scope invalid")
        count = 3 * n + 3 * len(all_defs) + len(srcs) + 1
        if len(raw_after) < count:
            raise ValueError("h10 after raw suffix incomplete")
        instance.rec = types.SimpleNamespace(handles=all_handles)
        api = _H10ReplayAPI(raw_after[-count:])
        after_snapshot, after_ok = instance._h10_snapshot(api, expected_defs)
        after_rosters = instance.final_rosters(api)
        quiet = instance.final_quiet(api, srcs)
        api.finish()
        if not after_ok or after_snapshot != after["snapshot"] or after_snapshot != snapshot or after_rosters != after["rosters"] or quiet != after["quiet"] or quiet.get("ok") is not True:
            reasons.append("h10 after protected snapshot/rosters/quiet differ from actual ordered raw replay")
        observed = after_rosters["owner_roster"]
        if after_rosters["problems"] or {triple(o) for o in observed} != {triple(o) for o in owners} | {triple(held)} or len(observed) != 3 or [triple(r) for r in after_rosters["run_roster"]] != [triple(held)]:
            reasons.append("h10 exact baseline2+held1 owner / held-only nonterminal roster mismatch")
        for owner in owners:
            if not any(key(r) == key(owner) for r in observed):
                reasons.append("h10 old failed owner business state changed")
        known = json.loads(Path(report["tail_known_binding_file"]).read_text())
        expected_known = {"h10-held": {k: held[k] for k in ("def_id", "source_id", "run_id")}}
        expected_retained = []
        for owner in owners:
            tag = "failedhealthy-h13-" + owner["def_id"]
            expected_known[tag] = {k: owner[k] for k in ("def_id", "source_id", "run_id")}
            expected_retained.append({"def_tag": tag, "run_ref": "terminal-failed", "blocked_reason": owner["blocked_reason"], "origin": "failedhealthy", "source_summary": str(original_path), "source_summary_sha256": sha(original_path)})
        canonical = lambda rows: sorted(json.dumps(r, sort_keys=True) for r in rows)
        actual_retained = [dict(r, source_summary=str(Path(r["source_summary"]))) for r in known["retained_owner_rows"]]
        if known["known_bindings"] != expected_known or canonical(actual_retained) != canonical(expected_retained) or known.get("baseline_sha256") != sha(bp) or known.get("baseline_after_sha256") != sha(ap):
            reasons.append("h10 known3 / retained failedhealthy origin source SHA map mismatch")
    except Exception as exc:
        reasons.append(f"h10 independent baseline raw replay failed: {type(exc).__name__}: {exc}")
    return reasons


def v_restart(step_dir, ctx):
    """Base3 restart and the separate real auxiliary h10 crash evidence."""
    import builtins
    import os
    import types
    all_services = {"connector", "dataset", "pipeline", "connector2", "connector3", "dataset-proxy", "p25fixture", "p25-proxy", "p25connector"}
    reasons, imported = _v_images_record(ctx, "restart-before-running.json", all_services)
    after_reasons, after_imported = _v_images_record(ctx, "restart-running.json", all_services)
    reasons += after_reasons
    imported.update(after_imported)
    try:
        before = json.loads((ctx["ev"] / "restart-before-running.json").read_text())["services"]
        after = json.loads((ctx["ev"] / "restart-running.json").read_text())["services"]
        for name in all_services:
            b, a = before[name]["started_at"], after[name]["started_at"]
            if name in ("connector", "dataset", "pipeline"):
                if not RFC3339_RE.fullmatch(str(b)) or not RFC3339_RE.fullmatch(str(a)) or a <= b:
                    reasons.append(f"restart-regression: {name} actual boot did not advance")
            elif a != b:
                reasons.append(f"restart-regression: unrelated auxiliary {name} boot changed")
        rp = step_dir / "h10-report.json"
        report = json.loads(rp.read_text())
        crash_path = Path(report["crash_record"])
        original_report = crash_path.parent / "result.json"
        if rp.read_bytes() != original_report.read_bytes() or report.get("schema") != "p25-h10-runner-v1" or report.get("ok") is not True:
            reasons.append("restart-regression: actual h10 helper report original/verdict mismatch")
        reasons += _v_h10_command_records(report, crash_path.parent)
        prior_path = crash_path.parent / "prior.json"
        prior = json.loads(prior_path.read_text())
        crash = json.loads(crash_path.read_text())
        engine = loadmod_sibling("smoke_p25.py")
        # Rebind only file reads for the pure HTTP crash checker. Its raw
        # paths remain byte-preserved in the bundle's source documents.
        engine.open = lambda path, *a, **kw: builtins.open(Path(path), *a, **kw)
        engine._file_sha256 = lambda path: sha(Path(path))
        engine.os = types.SimpleNamespace(path=types.SimpleNamespace(
            isabs=os.path.isabs, isfile=lambda path: Path(path).is_file(), exists=lambda path: Path(path).exists()))
        r18 = json.loads((HERE / "build-candidate-bindings-r18.json").read_text())
        reasons += engine._validate_crash_record(crash, prior["defs"], prior["held_def"], prior["snapshot"], str(prior_path), r18["service_images"]["p25connector"]["image_id"])
        post_path = Path(report["post_summary"])
        post = json.loads(post_path.read_text())
        if post.get("failed") != [] or (post.get("quiet") or {}).get("ok") is not True or len(post.get("scenarios", [])) != 1 or post["scenarios"][0].get("id") != "h10" or post["scenarios"][0].get("failed") is not False or any(g.get("ok") is not True for g in post["scenarios"][0].get("gates", [])):
            reasons.append("restart-regression: actual h10 post scenario gates/quiet refused")
        h10_cases = post.get("scenarios") or []
        if len(h10_cases) == 1 and "G_RETAIN_UNKNOWN" not in {g.get("gate") for g in h10_cases[0].get("gates", [])}:
            reasons.append("restart-regression: mandatory h10 retention gate missing")
        reasons += _v_h10_baseline(report, ctx, engine)
        known_path = Path(report["tail_known_binding_file"])
        known_doc = json.loads(known_path.read_text())
        if known_doc.get("post_summary_sha256") != sha(post_path) or any(((known_doc.get("known_bindings") or {}).get("h10-held") or {}).get(k) != (crash.get("held") or {}).get(k) for k in ("def_id", "source_id", "run_id")):
            reasons.append("restart-regression: actual h10 known binding/post summary SHA mismatch")
        for q in (rp, original_report, prior_path, crash_path, post_path):
            imported["input:" + str(q)] = sha(q)
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as exc:
        reasons.append(f"restart-regression: actual base3/h10 raw artifacts unavailable: {exc}")
    return reasons, imported


def _file_json(step_dir, *patterns):
    """Prefer a SPECIFIC content artifact (abc raw / persist manifest /
    tail declaration) over the stage's stdout verdict object — the
    stdout shadow must never substitute for the real record."""
    for pat in patterns:
        for p in sorted(step_dir.rglob(pat)):
            if not p.is_file():
                continue
            try:
                obj = json.loads(p.read_text())
            except ValueError:
                continue
            if isinstance(obj, dict):
                return obj
    return None


def v_pretail(step_dir, ctx):
    p = ctx["ev"] / "tail-declaration.json"
    if not p.is_file():
        return ["pre-tail-gates: actual semantic declaration missing"], {}
    obj = json.loads(p.read_text())
    spec = obj.get("spec")
    reasons = []
    if obj.get("schema") != "p25-tail-roster-v1" or obj.get("ok") is not True or not isinstance(spec, dict):
        return ["pre-tail-gates: actual declaration schema/spec/verdict refused"], {}
    if obj.get("declaration_sha256") != hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest():
        reasons.append("pre-tail-gates: canonical spec SHA mismatch")
    tags = set()
    for kind in ("owner_rows", "run_rows"):
        entries = spec.get(kind)
        if not isinstance(entries, list):
            reasons.append(f"pre-tail-gates: {kind} missing")
            continue
        for row in entries:
            tag = row.get("def_tag")
            key = (kind, tag)
            if not isinstance(tag, str) or not tag or key in tags:
                reasons.append("pre-tail-gates: invalid/duplicate semantic tag")
            tags.add(key)
    for tag, ids in (obj.get("known_bindings") or {}).items():
        if not any(key[1] == tag for key in tags) or any(not UUID_RE.fullmatch(str(ids.get(k) or "")) for k in ("def_id", "source_id", "run_id")):
            reasons.append(f"pre-tail-gates: invalid known existing binding {tag}")
    # Existing known identities must come from actual healthy/h10 output;
    # future tail owners deliberately have no UUID before execution.
    try:
        ev = ctx["ev"]
        known = set()
        healthy_path = ev / ("window-aggregate.json" if (ev / "window-aggregate.json").is_file() else "window-result.json")
        healthy = json.loads(healthy_path.read_text())
        known.update((r.get("def_id"), r.get("source_id"), r.get("run_id")) for r in healthy.get("final_owner_roster", []))
        selection = json.loads((ev / "selected-index.json").read_text())["selections"]["restart-regression"]
        h10_report = json.loads((ev / selection["attempt_dir"] / "h10-report.json").read_text())
        binding_file = Path(h10_report["tail_known_binding_file"])
        h10 = json.loads(binding_file.read_text())
        known.update((r.get("def_id"), r.get("source_id"), r.get("run_id")) for r in h10["known_bindings"].values())
        declared = {(r.get("def_id"), r.get("source_id"), r.get("run_id")) for r in (obj.get("known_bindings") or {}).values()}
        if declared != known:
            reasons.append("pre-tail-gates: known identity set differs from actual healthy retained owners + h10 held identity")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        reasons.append(f"pre-tail-gates: existing identity parent evidence unavailable: {exc}")
    return reasons, {"input:" + str(p): sha(p)}


def v_tail(step_dir, ctx):
    ev = ctx["ev"]
    derived_path = ev / "tail-aggregate.json"
    reasons, imported = ([], {}) if derived_path.is_file() else _v_http_phase(step_dir, ctx, "tail")
    try:
        sp = derived_path if derived_path.is_file() else ev / "tail-result.json"
        bp = ev / "tail-selected-bindings.json" if derived_path.is_file() else Path(json.loads(sp.read_text())["tail_bindings_file"])
        pf = loadmod_sibling("verify_postfacts_p25.py")
        selected = pf.load_verified_tail(sp, bp, logical_path_class=ctx.get("logical_path_class"))
        reasons += selected["problems"]
        summary, bd = selected["summary"], selected["bindings"]
        if derived_path.is_file() and selected["derived"] is not True:
            reasons.append("tail: selected aggregate is not the verified typed h14 branch")
        imported["input:" + str(sp)] = sha(sp)
        decl = json.loads((ev / "tail-declaration.json").read_text())
        bound = bd.get("bindings")
        if bd.get("schema") != "p25-tail-bindings-v1" or bd.get("problems") != [] or bd.get("declaration_sha256") != decl.get("declaration_sha256") or bd.get("bindings_sha256") != hashlib.sha256(json.dumps(bound, sort_keys=True).encode()).hexdigest() or bd.get("summary_sha256") != sha(sp) or summary.get("tail_bindings") != bound or (summary.get("tail_adjudication") or {}).get("ok") is not True:
            reasons.append("tail: actual bindings/spec/summary byteSHA/verdict mismatch")
        owners = [dict(r, sync_def_id=r.get("def_id")) for r in summary["final_owner_roster"]]
        runs = [dict(r, id=r.get("run_id")) for r in summary["final_run_roster"]]
        for r in runs:
            if r.get("def_id") != r.get("sync_def_id"):
                reasons.append("tail: actual nonterminal run parent mismatch")
        reasons += pf._tail_gate(decl, runs, owners, bound, None)
        imported["input:" + str(bp)] = sha(bp)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        reasons.append(f"tail: actual final dual rosters/bindings unavailable: {exc}")
    return reasons, imported


def v_auxdown(step_dir, ctx):
    reasons = []
    proxy_f = None
    for p in step_dir.rglob("proxy-quiet-proof.stdout.log"):
        proxy_f = p
    fx_f = None
    for p in step_dir.rglob("fixture-quiet-proof.stdout.log"):
        fx_f = p
    if proxy_f is None:
        reasons.append("aux-down: archived proxy quiet RAW missing")
    else:
        try:
            s = json.loads(proxy_f.read_text())
        except ValueError:
            s = None
        if not isinstance(s, dict):
            reasons.append("aux-down: proxy quiet raw not JSON")
        else:
            for k in ("commit_started", "commit_finished",
                      "commit_inflight"):
                if not _si(s.get(k)):
                    reasons.append(f"aux-down: proxy {k} not strict "
                                   "non-bool int")
            if _si(s.get("commit_inflight")) and s["commit_inflight"]:
                reasons.append("aux-down: proxy inflight != 0")
            if _si(s.get("commit_started")) and _si(
                    s.get("commit_finished")) and \
                    s["commit_started"] != s["commit_finished"]:
                reasons.append("aux-down: proxy started != finished")
    if fx_f is None:
        reasons.append("aux-down: archived fixture quiet RAW missing")
    else:
        try:
            fx = json.loads(fx_f.read_text())
        except ValueError:
            fx = None
        if not (isinstance(fx, list) and fx):
            reasons.append("aux-down: fixture quiet raw not a "
                           "non-empty list")
        else:
            for i, body in enumerate(fx):
                if not (isinstance(body, dict) and _si(body.get(
                        "active")) and body["active"] == 0):
                    reasons.append(f"aux-down: fixture raw[{i}] "
                                   "active not strict int 0")
    return reasons, {}


def _r18(step_dir):
    for p in sorted(step_dir.rglob("*build-candidate-bindings*.json")):
        try:
            return json.loads(p.read_text()), p
        except ValueError:
            continue
    p = HERE / "build-candidate-bindings-r18.json"
    if p.is_file():
        return json.loads(p.read_text()), p
    return None, None


def _v_images_record(ctx, filename, present):
    """Replay Runtime collector bytes against accepted r18."""
    reasons, imported = [], {}
    bp = HERE / "build-candidate-bindings-r18.json"
    if sha(bp) != "a4e5774152c4ea9146f34dcc5968e987921e8c3e6f310c855137726acac9feef":
        return ["final-images: accepted r18 SHA mismatch"], imported
    b = json.loads(bp.read_text())
    rp = ctx["ev"] / filename
    if not rp.is_file():
        return ["final-images: Runtime final-running.json missing"], imported
    obj = json.loads(rp.read_text())
    if obj.get("schema") != "p25-running-evidence-v1" or obj.get("project") != "openfoundry-platform-p1-q3v9" or obj.get("env_dir") != "/private/tmp/openfoundry-platform-p1-q3v9":
        reasons.append("final-images: collector schema/project/target mismatch")
    svc = obj.get("services") or {}
    base = set(present)
    absent = {"connector", "dataset", "pipeline", "p25fixture", "p25-proxy", "p25connector", "connector2", "connector3", "dataset-proxy"} - base
    if set(svc) != base | absent:
        reasons.append("final-images: exact service set mismatch")
    rawdir = Path(str(rp) + ".raw")
    for name in sorted(base | absent):
        entry = svc.get(name) or {}
        ps = rawdir / f"{name}.ps-aq.raw"
        rc = rawdir / f"{name}.ps-aq.rc"
        if not ps.is_file() or not rc.is_file() or rc.read_text().strip() != "0" or entry.get("ps_rc") != 0:
            reasons.append(f"final-images: {name} missing/failed original ps")
            continue
        ids = ps.read_text().splitlines()
        expected_n = 1 if name in base else 0
        if len(ids) != expected_n or len(set(ids)) != len(ids) or not _si(entry.get("containers")) or entry["containers"] != expected_n or entry.get("ps_raw") != ps.read_text():
            reasons.append(f"final-images: {name} actual ps identity/count differs")
        if name in base:
            ins = rawdir / f"{name}.inspect.raw"
            irc = rawdir / f"{name}.inspect.rc"
            if not ins.is_file() or not irc.is_file() or irc.read_text().strip() != "0" or entry.get("inspect_rc") != 0:
                reasons.append(f"final-images: {name} original inspect failed/missing")
                continue
            parts = ins.read_text().strip().split("|")
            want = b["service_images"][name]["image_id"] if name in b["service_images"] else b["cache_only_images"]["python:3.12-alpine"]["image_id"]
            if len(parts) != 3 or parts[1:] != [want, "true"] or not RFC3339_RE.match(parts[0]):
                reasons.append(f"final-images: {name} raw boot/image/running mismatch")
            elif entry.get("started_at") != parts[0] or entry.get("image") != parts[1] or entry.get("running") is not True:
                reasons.append(f"final-images: {name} summary differs from raw inspect")
        elif entry.get("running") is not False:
            reasons.append(f"final-images: {name} absence state mismatch")
    for q in [rp, bp, *sorted(rawdir.rglob("*"))]:
        if q.is_file():
            imported["input:" + str(q)] = sha(q)
    return reasons, imported


def v_final_images(step_dir, ctx):
    return _v_images_record(ctx, "final-running.json", {"connector", "dataset", "pipeline"})


REPO_ROOT = HERE.parents[2]
SOURCE_DIRS = ("services/connector-management-service",
               "services/dataset-versioning-service",
               "services/pipeline-build-service",
               "libs/scheduling-cron")


def source_tree_hash(repo_root: Path, path_prefix=None) -> str:
    """EXACT recompute of the PG/build source fingerprint (recipe
    mirrored from build_candidates_p25.sh source_tree_hash: go.mod +
    go.sum + sorted shasum of every *.go/*.sql under the four source
    roots; evidence dirs never involved)."""
    import subprocess
    parts = []
    for mod in ("go.mod", "go.sum"):
        parts.append(f"{mod}\n".encode())
        parts.append((repo_root / mod).read_bytes())
    parts.append(b"content-hashes\n")
    out = subprocess.run(
        ["find", *[str(repo_root / d) for d in SOURCE_DIRS],
         "-type", "f", "(", "-name", "*.go", "-o", "-name",
         "*.sql", ")", "-print0"],
        capture_output=True, check=True)
    files = sorted(out.stdout.split(b"\0")[:-1])
    for f in files:
        digest = hashlib.sha256(
            Path(f.decode()).read_bytes()).hexdigest()
        source_path = Path(f.decode())
        recorded_path = str(path_prefix).rstrip("/") + "/" + str(source_path.relative_to(repo_root)) if path_prefix else source_path
        parts.append(f"{digest}  {recorded_path}"
                     .encode())
        parts.append(b"\n")
    blob = b"".join(parts)
    return hashlib.sha256(blob).hexdigest()


def _historical_suffixes() -> set:
    try:
        fb = json.loads((HERE / "p24-frozen-baseline.json").read_text())
        names = fb["content_gates"]["protected_counters"]["expected"]
        out = set()
        for n in names:
            for part in str(n).split("-"):
                if SUFFIX_RE.match(part):
                    out.add(part)
        return out
    except Exception:                              # noqa: BLE001
        return set()


def v_p21abc(step_dir, ctx):
    obj = _file_json(step_dir, "abc-result*.json", "*result_p21*.json",
                     "*-result*.json")
    if obj is None:
        obj, _ = _stage_json(step_dir)
    if obj is None:
        return (["p21abc: raw result record missing"], {})
    reasons = []
    if "controlled_nonterminal_ids" in obj:
        reasons.append("p21abc: FORBIDDEN convenience field present")
    if obj.get("official") is not True or obj.get("partial") is not \
            False:
        reasons.append("p21abc: official/partial != true/false")
    counts = obj.get("counts") or {}
    if not (_si(counts.get("pass")) and counts["pass"] == 3
            and _si(counts.get("fail")) and counts["fail"] == 0):
        reasons.append("p21abc: counts != pass3/fail0")
    results = obj.get("results")
    if not isinstance(results, dict) or set(results) != P21_CASES:
        reasons.append("p21abc: results keyset != the real 3 cases")
    else:
        for case in P21_CASES:
            if (results.get(case) or {}).get("status") != "pass":
                reasons.append(f"p21abc: case {case} not pass")
    suffix = obj.get("suffix")
    if not (isinstance(suffix, str) and SUFFIX_RE.match(suffix)):
        reasons.append("p21abc: suffix not 8-hex")
    elif suffix in ctx["historical_suffixes"]:
        reasons.append(f"p21abc: suffix {suffix} COLLIDES with a "
                       "historical round suffix (identity)")
    return reasons, {}


def v_postfacts(step_dir, ctx):
    """Run the PURE INDEPENDENT verifier (verify_postfacts_p25.
    verify_attempt — archive-owned, no runtime) against the attempt
    the stage points at; the stage's own summary is NOT trusted."""
    obj = _file_json(step_dir, "postfacts-verdict.json")
    if obj is None:
        obj, _ = _stage_json(step_dir)
    if obj is None:
        return (["postfacts: verdict record missing"], {})
    adir = obj.get("attempt_dir")
    decl = obj.get("tail_declaration")
    if not (isinstance(adir, str) and isinstance(decl, str)):
        return (["postfacts: attempt_dir/tail_declaration pointers "
                 "missing"], {})
    ap, dp = Path(adir), Path(decl)
    if not ap.is_dir() or not dp.is_file():
        return (["postfacts: pointed attempt/declaration missing"],
                {})
    try:
        pf = loadmod_sibling("verify_postfacts_p25.py")
        verdict = pf.verify_attempt(ap, dp, Path(obj["tail_bindings"]), Path(obj["tail_summary"]), Path(obj["identity_catalog"]), logical_path_class=ctx.get("logical_path_class"))
    except Exception as exc:                      # noqa: BLE001
        return ([f"postfacts: independent verifier failed: "
                 f"{exc.__class__.__name__}: {exc}"], {})
    reasons = []
    if verdict.get("ok") is not True:
        reasons.append("postfacts: independent verifier NOT ok: "
                       + "; ".join(verdict.get("reasons", [])[:3]))
    a, b = obj.get("final_roster_size"), obj.get(
        "expected_roster_size")
    if not (_si(a) and _si(b) and a == b
            and verdict.get("final_roster_size") == a):
        reasons.append(f"postfacts: roster sizes {a!r} != {b!r} or "
                       "!= verifier's")
    abc = obj.get("abc") or {}
    for k in ("b_id", "c_id"):
        if not UUID_RE.match(str(abc.get(k) or "")):
            reasons.append(f"postfacts: abc.{k} not a UUID")
    return reasons, {"postfacts:verifier-run": "verify_attempt"}


def loadmod_sibling(name):
    spec = importlib.util.spec_from_file_location(
        name.replace(".py", "_fin"), HERE / name)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def v_persist(step_dir, ctx):
    """Run archive-owned pure raw replay; summary counts never substitute."""
    obj = _file_json(step_dir, "*persist-manifest*.json")
    if obj is None:
        obj, _ = _stage_json(step_dir)
    if not isinstance(obj, dict) or not isinstance(obj.get("attempt_dir"), str):
        return ["persist: attempt_dir pointer missing"], {}
    try:
        verifier = loadmod_sibling("collect_persist_state_p25.py")
        result = verifier.verify_attempt(Path(obj["attempt_dir"]))
    except Exception as exc:
        return [f"persist: independent raw replay failed: {exc}"], {}
    return result["problems"], {"persist:verifier-run": "verify_attempt"}


def _sealed_owned_ids():
    try:
        fb = json.loads(
            (HERE / "p24-frozen-baseline.json").read_text())
        return [d["id"] for d in
                fb["content_gates"]["owned_fullstate"]["defs"]]
    except Exception:                              # noqa: BLE001
        return []


def v_source_freeze(step_dir, ctx):
    try:
        before = json.loads((ctx["ev"] / "01-source-delta.json").read_text())
        obj = json.loads((ctx["ev"] / "14-source-delta-final.json").read_text())
    except (OSError, ValueError) as exc:
        return [f"source-freeze: actual before/final delta artifacts missing: {exc}"], {}
    if obj != before:
        return ["source-freeze: final source delta differs from actual initial delta"], {}
    if obj.get("removed") != []:
        return ["source-freeze: removed != []"], {}
    if any(not isinstance(obj.get(k), list) for k in ("changed", "added")):
        return ["source-freeze: actual changed/added lists missing"], {}
    # FINAL source fingerprint recompute (the PG/build recipe,
    # mirrored; must equal the root-accepted a1bff…ffd27)
    try:
        got = source_tree_hash(ctx["repo_root"])
    except Exception as exc:                        # noqa: BLE001
        return ([f"source-freeze: recompute failed: "
                 f"{exc.__class__.__name__}: {exc}"], {})
    if got != PG_GREEN_SOURCE:
        return ([f"source-freeze: recomputed source fingerprint "
                 f"{got[:12]}… != a1bff07f…ffd27"], {})
    return [], {"source-freeze:recomputed": got}


def capability_predecessor_replay(ev, bundle, meta):
    """Compare captured evidence, excluding only known lifecycle controls.

    The three root prepare outputs may be rewritten after this capability
    check; nested same-named evidence remains immutable. The final bundle
    separately includes/revalidates all of the final root control bytes.
    """
    reasons = []
    records = {k[len("ev/"):]: v for k, v in meta["files"].items() if k.startswith("ev/")}
    selected = json.loads((ev / "selected-index.json").read_text())["selections"]
    prior = json.loads((bundle / "ev/selected-index.json").read_text())["selections"]
    if {k: v for k, v in selected.items() if k != "standalone"} != {k: v for k, v in prior.items() if k != "standalone"}:
        reasons.append("standalone: predecessor selections drifted since capability proof")
    own = selected["standalone"]["attempt_dir"].rstrip("/") + "/"
    root_outputs = {"manifest.json", "tools-sha.json", "prepare-lock.json"}
    for rel, rec in records.items():
        if rel in root_outputs or rel == "selected-index.json" or rel.startswith(own):
            continue
        original = ev / rel
        if rel == "manifest.jsonl":
            # The run_step completion receipt is appended only after its
            # bundle subprocess returns. Keep the entire old ledger prefix
            # immutable and permit exactly that selected completion row.
            try:
                raw = original.read_bytes()
                size = rec["original_bytes"]
                if not _si(size) or size < 0 or hashlib.sha256(raw[:size]).hexdigest() != rec["original"]:
                    raise ValueError("captured ledger prefix changed")
                suffix = raw[size:]
                if not suffix.endswith(b"\n") or len(suffix.splitlines()) != 1:
                    raise ValueError("ledger must append exactly one JSONL receipt")
                row = json.loads(suffix)
                sel = selected["standalone"]
                step = ev / sel["attempt_dir"]
                cmd = (step / "cmd.txt").read_text().strip()
                if cmd.startswith("CMD: "):
                    cmd = cmd[5:]
                if set(row) != {"step", "attempt", "rc", "seconds", "cmd", "dir"}:
                    raise ValueError("ledger receipt key set mismatch")
                if row["step"] != "standalone" or not _si(row["attempt"]) or row["attempt"] < 0 or row["attempt"] != sel["attempt"] or not _si(row["rc"]) or row["rc"] != 0 or not _si(row["seconds"]) or row["seconds"] < 0 or row["dir"] != sel["attempt_dir"] or row["cmd"] != cmd or (step / "rc").read_text().strip() != "0":
                    raise ValueError("ledger receipt differs from selected standalone command/attempt/rc")
            except (OSError, ValueError, KeyError, TypeError) as exc:
                reasons.append(f"standalone: completion ledger rejected: {exc}")
            continue
        if not original.is_file() or sha(original) != rec["original"]:
            reasons.append(f"standalone: captured predecessor input drift: {rel}")
    return reasons


def selected_standalone_bundle(ev, tools_dir=None):
    """Resolve only the selected successful CLI receipt, never a directory glob."""
    import shlex
    tools_dir = Path(tools_dir or HERE).resolve()
    ev = Path(ev).resolve()
    selection = json.loads((ev / "selected-index.json").read_text())["selections"]["standalone"]
    step = (ev / selection["attempt_dir"]).resolve()
    step.relative_to(ev)
    if (step / "rc").read_text().strip() != "0":
        raise ValueError("selected standalone original rc is not zero")
    command = (step / "cmd.txt").read_text().strip()
    if command.startswith("CMD: "):
        command = command[5:]
    argv = shlex.split(command)
    if len(argv) != 10 or not Path(argv[0]).name.startswith("python") or argv[1] != "-I" or Path(argv[2]).resolve() != tools_dir / "standalone_p25.py" or argv[3] != "bundle":
        raise ValueError("selected standalone command is not the exact bundle CLI")
    def flag(name):
        if argv.count(name) != 1 or argv.index(name) + 1 >= len(argv):
            raise ValueError("selected standalone missing/ambiguous " + name)
        return Path(argv[argv.index(name) + 1]).resolve()
    if flag("--src-ev") != ev or flag("--tools-dir") != tools_dir:
        raise ValueError("selected standalone source EV/tools mismatch")
    bundle = flag("--bundle-dir")
    if bundle == ev or ev in bundle.parents:
        raise ValueError("selected standalone destination must be outside EV")
    receipt = json.loads((step / "stdout.log").read_text())
    manifest = bundle / "bundle-hash-manifest.json"
    meta = json.loads((bundle / "standalone-meta.json").read_text())
    if receipt.get("ok") is not True or receipt.get("scope") != "preseal-capability" or receipt.get("bundle_manifest_sha256") != sha(manifest):
        raise ValueError("selected standalone receipt/manifest SHA mismatch")
    if meta.get("scope") != "preseal-capability" or Path(meta.get("src_ev") or "").resolve() != ev or Path(meta.get("tools_dir") or "").resolve() != tools_dir:
        raise ValueError("selected standalone bundle source metadata mismatch")
    return bundle


def v_standalone(step_dir, ctx):
    """Offline RE-VERIFICATION of the bundle by actually RUNNING the
    self-contained offline verifier (pure CLI subprocess), plus the
    external-dependency presence the bundle must carry."""
    import subprocess
    reasons = []
    try:
        bundle = selected_standalone_bundle(ctx["ev"], ctx.get("tools_dir", HERE))
    except (OSError, ValueError, KeyError, IndexError) as exc:
        return [f"standalone: selected CLI binding refused: {exc}"], {}
    verifier = bundle / "offline_verify_p25.py"
    if not verifier.is_file():
        return (["standalone: offline verifier missing from the "
                 "bundle"], {})
    try:
        proc = subprocess.run(
            [sys.executable, "-I", str(verifier), "--bundle",
             str(bundle)], capture_output=True, text=True, timeout=180)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return ([f"standalone: offline verifier failed to run: "
                 f"{exc}"], {})
    imported = {"standalone:offline-rc": str(proc.returncode)}
    try:
        meta = json.loads((bundle / "standalone-meta.json").read_text())
        if meta.get("scope") != "preseal-capability":
            reasons.append("standalone: expected explicit preseal-capability scope")
        reasons += capability_predecessor_replay(ctx["ev"], bundle, meta)
    except (OSError, ValueError, KeyError) as exc:
        reasons.append(f"standalone: predecessor proof replay failed: {exc}")
    if proc.returncode != 0:
        reasons.append("standalone: offline verifier rc="
                       f"{proc.returncode}: "
                       f"{proc.stdout.strip()[:200]}")
    for rel in ("externals/pg-candidate-bindings-r17.json",
                "externals/build-candidate-bindings-r18.json",
                "externals/preflight-accepted.json",
                "externals/p24-hash-manifest.json",
                "externals/p24-16-tools-sha-final.json"):
        p = bundle / rel
        if not p.is_file() or p.stat().st_size == 0:
            reasons.append(f"standalone: bundle external dependency "
                           f"missing: {rel}")
        else:
            imported[f"standalone:{rel}"] = sha(p)
    return reasons, imported


# stage_id (== EXACT selected-index key) -> validator
CANONICAL_STAGES = {
    "preflight": v_preflight,
    "unit": v_unit,
    "pg-isolated": v_pg,
    "build": v_build,
    "swap": v_swap,
    "window": v_window,
    "regressions-p1pre9": _v_count(9, "regressions-p1pre9"),
    "regressions-p23": _v_count(8, "regressions-p23"),
    "regressions-p22": _v_count(11, "regressions-p22"),
    "regressions-p24": _v_count(7, "regressions-p24"),
    "reread-artifacts": v_reread,
    "p1post9": _v_count(9, "p1post9"),
    "restart-regression": v_restart,
    "pre-tail-gates": v_pretail,
    "tail": v_tail,
    "aux-down": v_auxdown,
    "final-images": v_final_images,
    "p21abc": v_p21abc,
    "postfacts": v_postfacts,
    "persist": v_persist,
    "source-freeze": v_source_freeze,
    "standalone": v_standalone,
}


def load_selected_index(ev: Path, required: list, deferred_stages=()):
    """Strict selected-index validation. Canonical stages match EXACT
    keys only; every canonical stage is REQUIRED (caller cannot waive
    or shrink); extras are recorded unadjudicated (never green)."""
    reasons = []
    path = ev / "selected-index.json"
    if not path.is_file():
        return None, ["selected-index.json MISSING — the driver must "
                      "record the explicit selected-success index"]
    try:
        idx = json.loads(path.read_text())
    except ValueError as exc:
        return None, [f"selected-index.json not JSON: {exc}"]
    if not isinstance(idx, dict) or idx.get("schema") != \
            "p25-selected-index-v1":
        return None, ["selected-index.json schema != "
                      "p25-selected-index-v1"]
    if set(idx) != {"schema", "selections"}:
        return None, [f"selected-index.json key set {sorted(idx)} != "
                      "[schema, selections]"]
    sel = idx.get("selections")
    if not isinstance(sel, dict) or not sel:
        return None, ["selected-index.json selections missing/empty"]
    for step, entry in sel.items():
        if step in deferred_stages:
            continue  # Offline pre-seal bundle: only its own standalone CLI is in flight.
        if not isinstance(entry, dict) or \
                set(entry) - {"attempt_dir", "attempt", "note"} or \
                "attempt_dir" not in entry:
            reasons.append(f"selection '{step}': entry schema invalid")
            continue
        d = (ev / entry["attempt_dir"]).resolve()
        try:
            d.relative_to(ev.resolve())
        except ValueError:
            reasons.append(f"selection '{step}': attempt_dir escapes "
                           "EV")
            continue
        if not d.is_dir():
            reasons.append(f"selection '{step}': {entry['attempt_dir']}"
                           " is not a directory")
            continue
        cmd = d / "cmd.txt"
        if not cmd.is_file() or not cmd.read_text().strip():
            reasons.append(f"selection '{step}': cmd.txt missing/empty")
        for need in ("stdout.log", "stderr.log"):
            if not (d / need).is_file():
                reasons.append(f"selection '{step}': {need} missing")
        rc = d / "rc"
        informational = any(t in step for t in INFORMATIONAL)
        if not rc.is_file():
            reasons.append(f"selection '{step}': rc file missing")
        elif rc.read_text().strip() != "0" and not informational:
            reasons.append(f"selection '{step}': selected rc="
                           f"{rc.read_text().strip()!r} != 0")
        if informational:
            continue                    # never substitutes a required
        verdicts = []
        so = d / "stdout.log"
        if so.is_file():
            lines = [l for l in so.read_text(errors="ignore")
                     .splitlines() if l.strip()]
            if lines:
                try:
                    verdicts.append(json.loads(lines[-1]))
                except ValueError:
                    pass
        for pat in ("*verdict*.json", "*report*.json",
                    "*manifest*.json"):
            for p in d.rglob(pat):
                try:
                    verdicts.append(json.loads(p.read_text()))
                except ValueError:
                    verdicts.append({"all_ok": "not-json"})
        # Canonical PASS comes only from its actual semantic validator in
        # prepare(); actual frozen-suite/HTTP schemas have no generic ok key.
        # Original command/stdout/stderr/rc requirements above remain mandatory.
    return sel, reasons


def old_archive_reverify() -> tuple:
    """Prepare-internal MANDATORY (root: 'old859+27 …各自绑定'): re-hash
    the frozen P24 final archive (859 files) and the 27 tools against
    their manifests. No selection can waive this."""
    reasons, summary = [], {}
    try:
        spec = importlib.util.spec_from_file_location(
            "p25_derive_fin", HERE / "p25_cohort_derive.py")
        derive = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(derive)
        final = Path(derive.SEAL_P24_FINAL_DEFAULT)
    except Exception as exc:                        # noqa: BLE001
        return ([f"old-archive: seal chain unreadable: {exc}"], summary)
    hm_path = final / "hash-manifest.json"
    tools_path = final / "16-tools-sha-final.json"
    try:
        hm = json.loads(hm_path.read_text())
        ts = json.loads(tools_path.read_text())
    except (OSError, ValueError) as exc:
        return ([f"old-archive: manifest unreadable: {exc}"], summary)
    if sha(hm_path) != derive.EXPECTED_HASH_MANIFEST_SHA:
        reasons.append("old-archive: ROOT fixed P24 seal SHA mismatch")
    files = hm.get("files") or {}
    disk = {str(p.relative_to(final)) for p in final.rglob("*") if p.is_file() and p != hm_path}
    if disk != set(files):
        reasons.append("old-archive: frozen859 file set NOT bidirectional")
    if len(files) != 859:
        reasons.append(f"old-archive: 859-manifest lists {len(files)} "
                       "files")
    bad = [rel for rel, h in files.items()
           if not (final / rel).is_file()
           or sha(final / rel) != h]
    if bad:
        reasons.append(f"old-archive: {len(bad)} files drifted vs the "
                       f"859 manifest (first: {bad[0]})")
    tfiles = ts.get("files") or {}
    if ts.get("count") != 27 or len(tfiles) != 27 \
            or ts.get("missing") != []:
        reasons.append("old-archive: 27-tools record != count27/"
                       "missing[]")
    tools_base = final.parent.parent          # the frozen P24 tools dir
    tbad = [name for name, h in tfiles.items()
            if not (tools_base / name).is_file()
            or sha(tools_base / name) != h]
    if tbad:
        reasons.append(f"old-archive: {len(tbad)} tools drifted (first:"
                       f" {tbad[0]})")
    summary = {"archive_files": len(files), "archive_drift": len(bad),
               "tools": len(tfiles), "tools_drift": len(tbad),
               "hash_manifest_sha256": sha(hm_path),
               "tools_sha_final_sha256": sha(tools_path)}
    return reasons, summary


def tools_tree(tools_dir: Path) -> dict:
    tools = {}
    for p in sorted(tools_dir.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(tools_dir)
        if rel.parts and rel.parts[0] in TOOLS_EXCLUDE_DIRS:
            continue
        if p.suffix == ".pyc":
            continue
        tools[str(rel)] = sha(p)
    return tools


def frozen_import_shas() -> dict:
    try:
        spec = importlib.util.spec_from_file_location(
            "p25_derive_fin2", HERE / "p25_cohort_derive.py")
        derive = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(derive)
        final = Path(derive.SEAL_P24_FINAL_DEFAULT)
        return {"p24_final_hash_manifest": sha(
                    final / "hash-manifest.json"),
                "p24_tools_sha_final": sha(
                    final / "16-tools-sha-final.json")}
    except Exception as exc:                        # noqa: BLE001
        return {"error": f"{exc.__class__.__name__}: {exc}"}


def prepare(ev: Path, tools_dir: Path, required: list) -> dict:
    """Phase 1: canonical stage adjudication + content gates + the
    prepare-internal old-859/27 re-hash; settles manifest.json +
    tools-sha.json INSIDE the EV (still open)."""
    reasons = []
    sel, sel_reasons = load_selected_index(ev, required)
    reasons += sel_reasons
    ctx = {"ev": ev, "evidence_root": EVIDENCE_ROOT,
           "preflight_base": PREFLIGHT_BASE,
           "preflight_accepted": PREFLIGHT_ACCEPTED,
           "repo_root": REPO_ROOT, "tools_dir": tools_dir,
           "historical_suffixes": _historical_suffixes()}
    imported = {}
    stage_status = {}
    if sel is not None:
        for stage_id, validator in CANONICAL_STAGES.items():
            if stage_id not in sel:
                reasons.append(f"canonical stage MISSING from the "
                               f"selected index: '{stage_id}'")
                stage_status[stage_id] = "missing"
                continue
            d = (ev / sel[stage_id]["attempt_dir"]).resolve()
            if not d.is_dir():
                stage_status[stage_id] = "no-dir"
                continue
            s_reasons, s_imported = validator(d, ctx)
            reasons += [f"{stage_id}: {r}" for r in s_reasons]
            imported.update(s_imported)
            stage_status[stage_id] = "ok" if not s_reasons else "refused"
        extras = sorted(k for k in sel
                        if k not in CANONICAL_STAGES
                        and not any(t in k for t in INFORMATIONAL))
    else:
        extras = []
        for stage_id in CANONICAL_STAGES:
            stage_status[stage_id] = "missing"
            reasons.append(f"canonical stage MISSING from the selected "
                           f"index: '{stage_id}'")
    if sel is not None:
        for name in required:
            if name not in sel:
                reasons.append(f"--require '{name}' matches NO "
                               "selection key (EXACT matching only; "
                               "the canonical set is always enforced "
                               "regardless)")
    old_reasons, old_summary = old_archive_reverify()
    reasons += old_reasons
    hits = scan_secrets(ev)
    if hits:
        reasons.append(f"secret scan hits: {len(hits)} (leaks "
                       f"{len({h['kind'] for h in hits})}) — must be "
                       "0/0")
    frozen = frozen_import_shas()
    if "error" in frozen:
        reasons.append(f"frozen import chain unreadable: "
                       f"{frozen['error']}")

    manifest = {"schema": "p25-final-v3",
                "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
                "required_stages": sorted(CANONICAL_STAGES),
                "extra_required": required,
                "stage_status": stage_status,
                "unadjudicated_selections": extras,
                "imported_green": imported,
                "old_archive_reverify": old_summary,
                "secret_hits": len(hits),
                "secret_leaks": len({h["kind"] for h in hits}),
                "final_ok": not reasons,
                "problems": reasons}
    (ev / "manifest.json").write_text(json.dumps(
        manifest, indent=1, sort_keys=True) + "\n")
    if reasons:
        return manifest
    tools = {"tools_dir": str(tools_dir), "files": tools_tree(tools_dir),
             "frozen_imports": frozen}
    (ev / "tools-sha.json").write_text(json.dumps(
        tools, indent=1, sort_keys=True) + "\n")
    lock = {"ev": _snapshot(ev), "external": _external_snapshot(ev, tools_dir),
            "source_sha256": source_tree_hash(REPO_ROOT)}
    lock["ev"].pop("prepare-lock.json", None)
    (ev / "prepare-lock.json").write_text(json.dumps(lock, indent=1, sort_keys=True) + "\n")
    return manifest


def _external_snapshot(ev=None, tools_dir=None):
    paths = set(p for p in EVIDENCE_ROOT.rglob("*") if p.is_file())
    paths.update(p for p in PREFLIGHT_BASE.parent.rglob("*") if p.is_file())
    if ev is not None:
        bundle = selected_standalone_bundle(ev, tools_dir)
        paths.update(p for p in bundle.rglob("*") if p.is_file())
    for p in (PREFLIGHT_ACCEPTED, HERE / "ARCHIVE-PG-INPUTS.json"):
        paths.add(p)
    return {str(p): sha(p) for p in sorted(paths)}


def _snapshot(ev: Path) -> dict:
    snap = {}
    for p in sorted(ev.rglob("*")):
        if p.is_file():
            st = p.stat()
            snap[str(p.relative_to(ev))] = {
                "size": st.st_size, "mtime_ns": st.st_mtime_ns,
                "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
    return snap


def seal(ev: Path, meta_dir: Path) -> dict:
    """Phase 2 (the LAST EV write): LOCKS prepare success (manifest
    final_ok must be TRUE — a failed prepare is not sealable) and
    INPUT NON-DRIFT (every tools-sha entry re-hashed against disk;
    the P24 seal inputs re-hashed against the frozen-import record)
    → snapshot → seal-time secret scan → hash-manifest.json → proof
    seal wrote ONLY the manifest → bidirectional readback."""
    if meta_dir.resolve() == ev.resolve() or ev.resolve() in meta_dir.resolve().parents:
        return {"ok": False, "problems": ["seal metadata directory must be outside EV"]}
    meta_dir.mkdir(parents=True, exist_ok=True)
    problems = []
    for need in ("manifest.json", "tools-sha.json"):
        if not (ev / need).is_file():
            problems.append(f"{need} missing — run `prepare` first")
    if problems:
        return {"ok": False, "problems": problems}
    if (ev / HASH_MANIFEST).is_file():
        problems.append("hash-manifest.json already exists — EV is "
                        "sealed; use `reverify`")
        return {"ok": False, "problems": problems}
    try:
        manifest = json.loads((ev / "manifest.json").read_text())
    except ValueError:
        return {"ok": False, "problems": ["manifest.json not JSON"]}
    if manifest.get("final_ok") is not True:
        problems.append("prepare did NOT succeed (manifest final_ok "
                        "!= true) — a failed prepare is not sealable")
    try:
        tools = json.loads((ev / "tools-sha.json").read_text())
    except ValueError:
        return {"ok": False, "problems": ["tools-sha.json not JSON"]}
    tools_dir = Path(tools.get("tools_dir") or HERE)
    for rel, want in sorted((tools.get("files") or {}).items()):
        p = tools_dir / rel
        if not p.is_file() or sha(p) != want:
            problems.append(f"input drift: tools {rel} changed since "
                            "prepare")
    if tools_tree(tools_dir) != tools.get("files"):
        problems.append("input drift: tools file set/content changed since prepare")
    frozen = frozen_import_shas()
    for key, want in sorted((frozen or {}).items()):
        got = (tools.get("frozen_imports") or {}).get(key)
        if got != want:
            problems.append(f"input drift: frozen import {key} "
                            "changed since prepare")
    try:
        lock = json.loads((ev / "prepare-lock.json").read_text())
        now = _snapshot(ev)
        now.pop("prepare-lock.json", None)
        if {k: (v["size"], v["sha256"]) for k, v in lock.get("ev", {}).items()} != {k: (v["size"], v["sha256"]) for k, v in now.items()}:
            problems.append("input drift: EV bytes/set changed since prepare")
        if lock.get("external") != _external_snapshot(ev, tools_dir):
            problems.append("input drift: external raw inputs changed since prepare")
        if lock.get("source_sha256") != source_tree_hash(REPO_ROOT):
            problems.append("input drift: candidate source changed since prepare")
    except (OSError, ValueError) as exc:
        problems.append(f"prepare input lock missing/unreadable: {exc}")
    old_problems, _ = old_archive_reverify()
    problems += old_problems
    if meta_dir.resolve() == ev.resolve() or ev.resolve() in meta_dir.resolve().parents:
        problems.append("seal metadata directory must be outside EV")
    if problems:
        report = {"ok": False, "problems": problems}
        (meta_dir / "seal-report.json").write_text(json.dumps(
            report, indent=1, sort_keys=True) + "\n")
        return report
    pre = _snapshot(ev)
    (meta_dir / "preseal-snapshot.json").write_text(json.dumps(
        pre, indent=1, sort_keys=True) + "\n")
    late_hits = scan_secrets(ev)
    if late_hits:
        problems.append(f"seal-time secret scan hits: {len(late_hits)} "
                        f"(first: {late_hits[0]}) — EV content changed "
                        "after prepare or prepare missed it")
        report = {"ok": False, "problems": problems}
        (meta_dir / "seal-report.json").write_text(json.dumps(
            report, indent=1, sort_keys=True) + "\n")
        return report
    files = {rel: e["sha256"] for rel, e in pre.items()}
    (ev / HASH_MANIFEST).write_text(json.dumps(
        {"client_date": time.strftime("%Y-%m-%d"),
         "generated": time.strftime("%Y-%m-%d %H:%M:%S"),
         "files": files}, indent=1, sort_keys=True) + "\n")
    post = _snapshot(ev)
    extra = {k for k in post if k != HASH_MANIFEST and k not in pre}
    touched = [k for k in post if k != HASH_MANIFEST and k in pre and
               (post[k]["mtime_ns"] != pre[k]["mtime_ns"]
                or post[k]["sha256"] != pre[k]["sha256"])]
    if extra:
        problems.append(f"seal wrote EXTRA EV files: {sorted(extra)}")
    if touched:
        problems.append(f"seal MODIFIED other EV files: {touched}")
    written = json.loads((ev / HASH_MANIFEST).read_text())
    disk = {str(p.relative_to(ev)) for p in ev.rglob("*")
            if p.is_file() and p != ev / HASH_MANIFEST}
    if disk != set(written["files"]):
        problems.append("seal coverage is not bidirectional "
                        f"(disk={len(disk)}, manifest="
                        f"{len(written['files'])})")
    for rel, h in written["files"].items():
        if sha(ev / rel) != h:
            problems.append(f"seal readback mismatch: {rel}")
    report = {"ok": not problems, "problems": problems,
              "sealed_files": len(written["files"]),
              "seal_mtime_ns": (ev / HASH_MANIFEST).stat().st_mtime_ns,
              "seal_sha256": sha(ev / HASH_MANIFEST),
              "zero_extra_writes": not (extra or touched)}
    (meta_dir / "seal-report.json").write_text(json.dumps(
        report, indent=1, sort_keys=True) + "\n")
    return report


def reverify(ev: Path, meta_dir: Path) -> dict:
    """Post-seal byte-content/file-set readback (same-byte touches are
    outside this contract). No EV writes are performed by this verifier."""
    problems = []
    pre_path = meta_dir / "preseal-snapshot.json"
    hm = ev / HASH_MANIFEST
    if not hm.is_file():
        return {"ok": False, "problems": ["EV not sealed "
                                          "(hash-manifest.json absent)"]}
    if not pre_path.is_file():
        problems.append("preseal-snapshot.json missing in meta dir")
        return {"ok": False, "problems": problems}
    pre = json.loads(pre_path.read_text())
    seal_mtime = hm.stat().st_mtime_ns
    now = _snapshot(ev)
    for rel, e in now.items():
        if rel == HASH_MANIFEST:
            continue
        if rel not in pre:
            problems.append(f"post-seal write detected: NEW {rel}")
        elif e["sha256"] != pre[rel]["sha256"]:
            problems.append(f"content drift vs pre-seal snapshot: {rel}")
    for rel in pre:
        if rel not in now and rel != HASH_MANIFEST:
            problems.append(f"post-seal REMOVAL detected: {rel}")
    written = json.loads(hm.read_text())["files"]
    for rel, h in written.items():
        if sha(ev / rel) != h:
            problems.append(f"seal readback mismatch: {rel}")
    disk = {r for r in now if r != HASH_MANIFEST}
    if disk != set(written):
        problems.append("seal coverage is not bidirectional")
    report = {"ok": not problems, "problems": problems,
              "post_seal_writes": [p for p in problems
                                   if "post-seal" in p]}
    (meta_dir / "reverify-report.json").write_text(json.dumps(
        report, indent=1, sort_keys=True) + "\n")
    return report


def package_final(ev: Path, tools_dir: Path, meta_dir: Path) -> dict:
    """After hashLAST and bare readback, package full sealed EV outside EV."""
    import subprocess
    if meta_dir.resolve() == ev.resolve() or ev.resolve() in meta_dir.resolve().parents:
        return {"ok": False, "problems": ["final package metadata must be outside EV"]}
    meta_dir.mkdir(parents=True, exist_ok=True)
    checked = reverify(ev, meta_dir)
    if checked.get("ok") is not True:
        return {"ok": False, "problems": checked.get("problems", [])}
    before = _snapshot(ev)
    root = meta_dir / "final-bundle"
    root.mkdir(exist_ok=True)
    n = 1
    while (root / f"attempt-{n}").exists():
        n += 1
    bundle = root / f"attempt-{n}"
    problems = []
    try:
        sa = loadmod_sibling("standalone_p25.py")
        meta = sa.build_bundle(ev, tools_dir, bundle)
        ev_records = {k[len("ev/"):]: v for k, v in meta["files"].items() if k.startswith("ev/")}
        replay = sa.replay_compare(ev, bundle / "ev", {"files": ev_records})
        problems += replay["reasons"]
        argv = [sys.executable, "-I", str(bundle / "offline_verify_p25.py"), "--bundle", str(bundle)]
        prefix = meta_dir / f"final-bundle-attempt-{n}"
        Path(str(prefix) + ".cmd.json").write_text(json.dumps(argv) + "\n")
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=180)
        Path(str(prefix) + ".stdout").write_text(proc.stdout)
        Path(str(prefix) + ".stderr").write_text(proc.stderr)
        Path(str(prefix) + ".rc").write_text(str(proc.returncode) + "\n")
        if proc.returncode != 0:
            problems.append("final sealed standalone offline verifier refused")
        if meta.get("scope") != "sealed-final":
            problems.append("final standalone missing sealed-final scope")
    except Exception as exc:
        problems.append(f"final standalone package failed: {type(exc).__name__}: {exc}")
    if _snapshot(ev) != before:
        problems.append("final standalone packaging modified sealed EV")
    report = {"ok": not problems, "problems": problems, "bundle": str(bundle),
              "scope": "sealed-final", "ev_unchanged": _snapshot(ev) == before}
    (meta_dir / f"final-bundle-attempt-{n}.receipt.json").write_text(json.dumps(report, indent=1, sort_keys=True) + "\n")
    return report


def finalize(ev: Path, tools_dir: Path, required: list) -> dict:
    """Compat single-shot: prepare → seal (meta = sibling
    `<EV>-seal-meta`)."""
    m = prepare(ev, tools_dir, required)
    if not m["final_ok"]:
        return m
    s = seal(ev, ev.parent / f"{ev.name}-seal-meta")
    out = dict(m)
    out["seal_verified"] = s.get("ok")
    out["problems"] = m["problems"] + s.get("problems", [])
    if not s.get("ok"):
        out["final_ok"] = False
    else:
        packaged = package_final(ev, tools_dir, ev.parent / f"{ev.name}-seal-meta")
        out["final_bundle"] = packaged
        out["final_ok"] = packaged.get("ok") is True
        out["problems"] += packaged.get("problems", [])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", nargs="?", default="finalize",
                    choices=["prepare", "seal", "reverify", "package-final", "finalize"])
    ap.add_argument("--ev", required=True)
    ap.add_argument("--tools-dir")
    ap.add_argument("--require", action="append", default=[],
                    help="EXTRA exact-key assertions beyond the "
                         "canonical set (which is always enforced)")
    ap.add_argument("--meta-dir")
    args = ap.parse_args()
    ev = Path(args.ev)
    if args.mode == "package-final":
        report = package_final(ev, Path(args.tools_dir or HERE), Path(args.meta_dir or str(ev) + "-seal-meta"))
        print(json.dumps(report, indent=1, sort_keys=True))
        return 0 if report["ok"] else 1
    if args.mode == "prepare":
        m = prepare(ev, Path(args.tools_dir), args.require)
        print(json.dumps({"FINAL_OK": m["final_ok"],
                          "problems": m["problems"][:8],
                          "stages_refused": [
                              k for k, v in
                              m["stage_status"].items()
                              if v != "ok"]}, indent=1))
        return 0 if m["final_ok"] else 1
    meta = Path(args.meta_dir) if args.meta_dir else \
        ev.parent / f"{ev.name}-seal-meta"
    if args.mode == "seal":
        r = seal(ev, meta)
        print(json.dumps(r, indent=1, sort_keys=True))
        return 0 if r["ok"] else 1
    if args.mode == "reverify":
        r = reverify(ev, meta)
        print(json.dumps(r, indent=1, sort_keys=True))
        return 0 if r["ok"] else 1
    m = finalize(ev, Path(args.tools_dir), args.require)
    print(json.dumps({"FINAL_OK": m["final_ok"],
                      "seal_verified": m.get("seal_verified"),
                      "problems": m["problems"][:8]}, indent=1))
    return 0 if m["final_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

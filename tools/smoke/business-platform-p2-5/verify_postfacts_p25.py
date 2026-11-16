#!/usr/bin/env python3
"""P25 POSTFACTS verifier — seal-anchored readonly-tail adjudication.

review-archive-tools-round2 #4/#6 closures (ACTUAL shapes, no toy
contracts):

  * the ABC-round ids are DERIVED from the RAW P21 result using its
    REAL schema: official/partial/counts + the exact 3-case keyset
    {heartbeat_progress, lease_loss_fencing, terminal_commit_fault}
    each status=='pass'; NO convenience id field is accepted. B has NO
    direct UUID anywhere in the result — it is derived through the
    actual suffix→fixture_src→Source/Def/Run parent walk over the
    COLLECTED controlled rows (fixture_src p21b-<suffix> / p21c-<suffix>,
    each matching EXACTLY one nonterminal row); C is cross-checked
    against evidence.terminal_commit_fault.logical_path
    (`sync-<uuid>.ndjson`). Final controlled set is EXACTLY old6 +
    ABC2 + declared tail k, unique, parents-bound.
  * counters are adjudicated from READ-TAGGED RAW bodies (HTTP 200,
    body self-identifies name==src, strict non-bool ints, active==0,
    read1==read2 full-field, request_count == sealed expectation).
  * aux quiet is consumed from the ARCHIVED pre-removal RAW proof
    bound to the aux attempt (proxy commit_started/finished/inflight
    strict non-bool ints, inflight==0, started==finished; fixture
    per-src active==0) — the proxy/fixture are GONE by then; no
    post-hoc HTTP, no caller booleans.
  * the nonterminal RUN roster and the CHECKPOINT OWNER roster are
    adjudicated SEPARATELY (owners may reference terminal failed runs;
    never cleaned/adopted).

Formal entry: --attempt-dir ONLY (plus --tail-declaration, hash-bound
into the verdict). Everything else is rederived from the fixed P24
seal; the attempt's consumption ledger is re-hashed both ways; caller
JSON facts are NOT accepted as evidence (facts are REPLAYED from the
attempt's raw artifacts by replay_facts_from_attempt()).
"""

import argparse
import hashlib
import importlib.util
import json
import re
import sys
sys.dont_write_bytecode = True  # Pure validation must not mutate copied evidence/tools.
from pathlib import Path

HERE = Path(__file__).resolve().parent

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                     r"[0-9a-f]{4}-[0-9a-f]{12}$")
SUFFIX_RE = re.compile(r"^[0-9a-f]{8}$")
P21_CASES = frozenset({"heartbeat_progress", "lease_loss_fencing",
                       "terminal_commit_fault"})
NONTERMINAL = ("pending", "running")


def loadmod(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _si(v):
    """Strict non-bool int."""
    return isinstance(v, int) and not isinstance(v, bool)


def derive_abc_ids(raw, controlled_rows):
    """(#4, REAL schema) Derive the ABC-round nonterminal pair from the
    raw P21 result + the collected controlled rows. Returns
    {ok, reasons, b_id, c_id, suffix}. Refuses anything ambiguous or
    convenience-fielded instead of inventing ids."""
    out = {"ok": False, "reasons": [], "b_id": None, "c_id": None,
           "suffix": None}
    if not isinstance(raw, dict):
        out["reasons"].append("abc raw not an object")
        return out
    if "controlled_nonterminal_ids" in raw:
        out["reasons"].append("abc raw carries the FORBIDDEN "
                              "convenience field "
                              "controlled_nonterminal_ids — ids must be "
                              "DERIVED, never declared")
        return out
    if raw.get("official") is not True:
        out["reasons"].append(f"abc raw official={raw.get('official')!r} "
                              "is not True")
    if raw.get("partial") is not False:
        out["reasons"].append(f"abc raw partial={raw.get('partial')!r} "
                              "is not False")
    counts = raw.get("counts") or {}
    if not (_si(counts.get("pass")) and counts["pass"] == 3
            and _si(counts.get("fail")) and counts["fail"] == 0):
        out["reasons"].append(f"abc counts {counts!r} != pass3/fail0")
    results = raw.get("results")
    if not isinstance(results, dict) or set(results) != P21_CASES:
        got = sorted(results or [])
        out["reasons"].append(f"abc results keyset {got} != the real "
                              f"3-case keyset {sorted(P21_CASES)}")
    else:
        for case in sorted(P21_CASES):
            st = (results.get(case) or {}).get("status")
            if st != "pass":
                out["reasons"].append(f"abc case {case} status "
                                      f"{st!r} != pass")
    suffix = raw.get("suffix")
    if not (isinstance(suffix, str) and SUFFIX_RE.match(suffix)):
        out["reasons"].append(f"abc suffix {suffix!r} not 8-hex")
    if out["reasons"]:
        return out
    out["suffix"] = suffix
    # C id: the ONLY uuid the raw result carries is inside the
    # terminal-commit-fault logical path.
    ev = (raw.get("evidence") or {}).get("terminal_commit_fault") or {}
    lp = ev.get("logical_path")
    m = re.match(r"^sync-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                 r"[0-9a-f]{4}-[0-9a-f]{12})\.ndjson$", str(lp or ""))
    if not m:
        out["reasons"].append(f"abc C logical_path {lp!r} does not "
                              "match sync-<uuid>.ndjson")
        return out
    c_from_path = m.group(1)
    # B/C rows: actual suffix→fixture_src→Source/Def/Run parents walk
    # over the COLLECTED rows.
    if not isinstance(controlled_rows, list):
        out["reasons"].append("controlled rows missing for the parent "
                              "walk")
        return out
    rows = controlled_rows
    found = {}
    for tag, src in (("b", f"p21b-{suffix}"), ("c", f"p21c-{suffix}")):
        hits = [r for r in rows if r.get("fixture_src") == src]
        if len(hits) != 1:
            out["reasons"].append(
                f"fixture_src {src} matched {len(hits)} controlled "
                "rows (need EXACTLY one — the suffix→source→def→run "
                "walk is ambiguous or broken)")
            continue
        r = hits[0]
        if r.get("status") not in NONTERMINAL:
            out["reasons"].append(
                f"fixture_src {src} row {r.get('id')} status "
                f"{r.get('status')!r} not nonterminal (ABC rows stay "
                "pending/running)")
            continue
        for parent in ("sync_def_id", "source_id", "output_dataset_id",
                       "source_name"):
            if not r.get(parent):
                out["reasons"].append(
                    f"fixture_src {src} row lacks parent field "
                    f"{parent}")
        found[tag] = r.get("id")
    if out["reasons"]:
        return out
    if not UUID_RE.match(str(found.get("b") or "")):
        out["reasons"].append("derived B id not a UUID")
        return out
    if found.get("c") != c_from_path:
        out["reasons"].append(
            f"derived C row {found.get('c')} != logical_path uuid "
            f"{c_from_path} (crosscheck failed)")
        return out
    if found["b"] == found["c"]:
        out["reasons"].append("B and C resolved to the SAME run id")
        return out
    out["b_id"], out["c_id"] = found["b"], found["c"]
    out["ok"] = True
    return out


def _counter_gate(facts, frozen):
    """(#6) typed counters from READ-TAGGED raw bodies."""
    reasons = []
    expected = frozen["content_gates"]["protected_counters"]["expected"]
    counters = facts.get("counters") or {}
    for name in sorted(expected):
        want = expected[name]
        for read in ("read1", "read2"):
            entry = (counters.get(read) or {}).get(name)
            if not isinstance(entry, dict):
                reasons.append(f"counter {name} {read} raw missing")
                continue
            if entry.get("http_status") != 200:
                reasons.append(f"counter {name} {read} HTTP "
                               f"{entry.get('http_status')!r} != 200")
                continue
            body = entry.get("body")
            if not isinstance(body, dict):
                reasons.append(f"counter {name} {read} body not JSON "
                               "object")
                continue
            if body.get("name") != name:
                reasons.append(f"counter {name} {read} body self-"
                               f"identifies as {body.get('name')!r}")
            for k in ("request_count", "inventory_started",
                      "inventory_finished", "active"):
                if not _si(body.get(k)):
                    reasons.append(f"counter {name} {read}.{k} not a "
                                   "strict non-bool int "
                                   f"({body.get(k)!r})")
            if _si(body.get("active")) and body["active"] != 0:
                reasons.append(f"counter {name} {read} active="
                               f"{body['active']} not quiet 0")
            if _si(body.get("inventory_started")) and \
                    _si(body.get("inventory_finished")) and \
                    body["inventory_started"] != \
                    body["inventory_finished"]:
                reasons.append(f"counter {name} {read} inventory "
                               "started!=finished (in-flight)")
        r1 = (counters.get("read1") or {}).get(name) or {}
        r2 = (counters.get("read2") or {}).get(name) or {}
        if isinstance(r1.get("body"), dict) and \
                isinstance(r2.get("body"), dict) and \
                r1["body"] != r2["body"]:
            reasons.append(f"counter {name} UNSTABLE across reads")
        if isinstance(r1.get("body"), dict) and \
                r1["body"].get("request_count") != want["request_count"]:
            reasons.append(f"counter {name} request_count "
                           f"{r1['body'].get('request_count')!r} != "
                           f"sealed {want['request_count']} (P25 "
                           "window must not move protected cohorts)")
    return reasons


def _quiet_gates(facts):
    """(#6) archived pre-removal quiet RAW consumption — strict ints,
    never booleans, never post-hoc HTTP."""
    reasons = []
    proxy = facts.get("proxy_quiet_raw")
    if not isinstance(proxy, dict):
        reasons.append("archived proxy quiet raw missing/not an object "
                       "(aux removed before ABC — the pre-removal "
                       "proof MUST be consumed from the archive)")
    else:
        for k in ("commit_started", "commit_finished",
                  "commit_inflight"):
            if not _si(proxy.get(k)):
                reasons.append(f"proxy quiet raw {k} not a strict "
                               f"non-bool int ({proxy.get(k)!r})")
        if _si(proxy.get("commit_inflight")) and \
                proxy["commit_inflight"] != 0:
            reasons.append(f"proxy quiet raw commit_inflight="
                           f"{proxy['commit_inflight']} != 0")
        if _si(proxy.get("commit_started")) and \
                _si(proxy.get("commit_finished")) and \
                proxy["commit_started"] != proxy["commit_finished"]:
            reasons.append("proxy quiet raw started!=finished "
                           "(not true-quiet)")
    fx = facts.get("fixture_quiet_raw")
    if not isinstance(fx, list) or not fx:
        reasons.append("archived fixture quiet raw missing/empty "
                       "(per-src pre-removal counters required)")
    else:
        for i, body in enumerate(fx):
            if not isinstance(body, dict):
                reasons.append(f"fixture quiet raw[{i}] not an object")
                continue
            if not _si(body.get("active")):
                reasons.append(f"fixture quiet raw[{i}].active not a "
                               f"strict non-bool int "
                               f"({body.get('active')!r})")
            elif body["active"] != 0:
                reasons.append(f"fixture quiet raw[{i}] "
                               f"({body.get('name')}) active="
                               f"{body['active']} not quiet 0")
    return reasons


def _tail_gate(tail_decl, tail_rows, owner_rows, bound, schema27):
    """Independent exact tag/identity/state/parent matching for both rosters."""
    reasons = []
    if not isinstance(tail_decl, dict) or tail_decl.get("schema") != "p25-tail-roster-v1" or tail_decl.get("ok") is not True:
        return ["tail declaration schema mismatch"]
    spec = tail_decl.get("spec") or {}
    spec_sha = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    if tail_decl.get("declaration_sha256") != spec_sha:
        reasons.append("tail declaration spec SHA mismatch")
    runs, owners = spec.get("run_rows"), spec.get("owner_rows")
    if not isinstance(runs, list) or not isinstance(owners, list):
        return reasons + ["tail declaration dual lists missing"]
    expected = {}
    for kind, entries in (("owner", owners), ("run", runs)):
        for ent in entries:
            tag = ent.get("def_tag")
            key = ("run/" if kind == "run" else "") + str(tag)
            if not tag or key in expected or (kind == "run" and ent.get("count") != 1):
                reasons.append(f"invalid/duplicate declared tag/count: {key}")
            if any(k in ent for k in ("def_id", "source_id", "run_id")):
                reasons.append(f"{key}: future UUID embedded in semantic declaration")
            if kind == "owner" and (ent.get("run_ref") not in ("terminal-failed", "same") or ent.get("blocked_reason") not in ("", "requires_reconciliation")):
                reasons.append(f"{key}: invalid owner business state declaration")
            if kind == "run" and ent.get("semantics") != "p25-crash-held-prepublish":
                reasons.append(f"{key}: semantics does not describe actual prepublish crash")
            expected[key] = (kind, ent)
    if not isinstance(bound, dict) or set(bound) != set(expected):
        return reasons + ["actual bindings != exact declared owner/run tag set"]
    if len(tail_rows) != len(runs) or len(owner_rows) != len(owners):
        reasons.append("actual dual roster sizes differ from declaration")
    for row in tail_rows:
        if not isinstance(row, dict) or (schema27 is not None and set(row) != schema27):
            reasons.append("tail run does not have exact safe27 schema")
    actual_owner = {(r.get("sync_def_id"), r.get("source_id"), r.get("run_id")) for r in owner_rows}
    actual_run = {(r.get("sync_def_id"), r.get("source_id"), r.get("id")) for r in tail_rows}
    if len(actual_owner) != len(owner_rows) or len(actual_run) != len(tail_rows):
        reasons.append("duplicate actual owner/run identity")
    wanted = {"owner": set(), "run": set()}
    for key, (kind, ent) in expected.items():
        ids = bound[key]
        triple = tuple(ids.get(k) for k in ("def_id", "source_id", "run_id"))
        if any(not UUID_RE.match(str(v or "")) for v in triple) or ids.get("kind") != kind:
            reasons.append(f"{key}: invalid UUID triple/kind")
        if triple in wanted[kind] or any(t[2] == triple[2] for t in wanted[kind]):
            reasons.append(f"{key}: one actual identity used by multiple tags")
        wanted[kind].add(triple)
        semantic_field = "mode" if kind == "owner" else "semantics"
        if ent.get(semantic_field) is not None and ids.get(semantic_field) != ent[semantic_field]:
            reasons.append(f"{key}: actual binding mode/semantics differs from declaration")
        if kind == "owner":
            hit = [r for r in owner_rows if (r.get("sync_def_id"), r.get("source_id"), r.get("run_id")) == triple]
            for row in hit:
                terminal = ent.get("run_ref") == "terminal-failed"
                statuses = ("failed", "aborted", "error") if terminal else ("pending", "running")
                expected_reason = ent.get("blocked_reason", "requires_reconciliation" if terminal else "")
                if row.get("run_status") not in statuses or row.get("blocked_reason") != expected_reason:
                    reasons.append(f"{key}: actual owner status/reason differs from declaration")
        else:
            for row in tail_rows:
                if row.get("id") == triple[2]:
                    if row.get("status") not in ("pending", "running"):
                        reasons.append(f"{key}: run is not nonterminal")
                    if ent.get("semantics") == "p25-crash-held-prepublish":
                        prepublish = row.get("gate_entered") == "false" if schema27 is not None else ("publish_entered_at" in row and row["publish_entered_at"] is None)
                        if row.get("control_mode") != "controlled" or not prepublish:
                            reasons.append(f"{key}: actual controlled/prepublish run semantics mismatch")
        known = (tail_decl.get("known_bindings") or {}).get(ent.get("def_tag"))
        if known and any(known.get(k) != ids.get(k) for k in ("def_id", "source_id", "run_id")):
            reasons.append(f"{key}: identity differs from pre-existing known binding")
    if wanted["owner"] != actual_owner:
        reasons.append("exact owner roster identity/parent set mismatch")
    if wanted["run"] != actual_run:
        reasons.append("exact nonterminal run identity/parent set mismatch")
    return reasons


def adjudicate_postfacts(facts, frozen, guard_mod, roster, tail_decl,
                         abc_raw):
    """PURE adjudication over RAW-DERIVED facts (schema
    p25-postfacts-facts-v2 — produced by replay_facts_from_attempt;
    v1 caller-dumps JSON is refused)."""
    reasons = []
    if facts.get("schema") != "p25-postfacts-facts-v2":
        return {"ok": False, "reasons": [
            f"facts schema {facts.get('schema')!r} != v2 raw-replay — "
            "caller JSON is not execution evidence"], "abc": {}}
    # 1) old6 EXACT (seal-derived roster equality)
    rows = facts.get("controlled_rows") or []
    by_id = {}
    for i, r in enumerate(rows):
        rid = (r or {}).get("id")
        if rid in by_id:
            reasons.append(f"duplicate controlled row {rid}")
        by_id[rid] = r
    six = list(roster.GROUP1) + list(roster.GROUP2)
    for want in six:
        got = by_id.get(want["id"])
        if got is None:
            reasons.append(f"old6 row {want['id']} MISSING after ABC — "
                           "must never be touched")
            continue
        for k, v in want.items():
            if got.get(k) != v:
                reasons.append(f"old6 row {want['id']} DRIFT on {k}")
    # 2) ABC-round 2 derived from the RAW result + rows (#4)
    d = derive_abc_ids(abc_raw, rows)
    abc_ids = {d["b_id"], d["c_id"]} if d["ok"] else set()
    if not d["ok"]:
        reasons += [f"abc-derive: {r}" for r in d["reasons"]]
    for rid in abc_ids:
        if rid in {w["id"] for w in six}:
            reasons.append(f"ABC row {rid} COLLIDES with old6")
    # 3) declared tail k; the tail gate sees ONLY this round's tail
    #    rows (old6/ABC are bound by their own sections above)
    six_ids = {w["id"] for w in six}
    tail_rows = [r for r in rows if r.get("id") not in six_ids
                 and r.get("id") not in abc_ids]
    owner_rows = facts.get("owner_rows") or []
    reasons += _tail_gate(tail_decl, tail_rows, owner_rows,
                          facts.get("tail_bound_identities"),
                          set(guard_mod.expected_schema()))
    declared_k = sum(r["count"] for r in
                     (tail_decl.get("spec") or {}).get("run_rows") or [])
    expected_total = 6 + len(abc_ids) + declared_k
    if len(rows) != expected_total:
        reasons.append(f"final controlled set {len(rows)} != exact "
                       f"6+ABC2+{declared_k}={expected_total}")
    # 4) counters x2 stable/quiet/typed from RAW bodies
    reasons += _counter_gate(facts, frozen)
    # 5) archived pre-removal quiet RAWs
    reasons += _quiet_gates(facts)
    # 6) owner roster EXACT adjudication (separate from the run roster;
    #    terminal-failed owner refs are VALID and must be RETAINED)
    for o in owner_rows:
        rid = o.get("run_id")
        if not UUID_RE.match(str(rid or "")):
            reasons.append(f"owner row run_id {rid!r} not a UUID")
        if o.get("run_status") not in ("running", "pending", "failed",
                                       "aborted", "error"):
            reasons.append(f"owner row {str(rid)[:8]} run_status "
                           f"{o.get('run_status')!r} unknown")
        if o.get("run_status") in ("failed", "aborted", "error") and \
                rid not in {r.get("id") for r in rows}:
            # terminal-failed owner: its run is NOT in the nonterminal
            # set BY DESIGN — it must simply remain retained here.
            pass
    return {"ok": not reasons, "reasons": reasons,
            "final_roster_size": len(rows),
            "expected_roster_size": expected_total,
            "abc": {"b_id": d["b_id"], "c_id": d["c_id"],
                    "suffix": d["suffix"]},
            "owner_roster_size": len(owner_rows)}


def _load_sealed(here: Path):
    spec = importlib.util.spec_from_file_location(
        "p25_derive_pf", here / "p25_cohort_derive.py")
    derive = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(derive)
    data = derive.derive(Path(derive.SEAL_P24_FINAL_DEFAULT),
                         Path(derive.SEAL_P24_TOOLS_DEFAULT))
    cohort6_sha = hashlib.sha256(
        (here / "p25_cohort6.py").read_bytes()).hexdigest()
    return derive.frozen_baseline_dict(data, cohort6_sha)


def replay_facts_from_attempt(attempt_dir: Path, roster=None) -> dict:
    """REPLAY the facts from the attempt's RAW artifacts (never from
    caller JSON): ledger re-hash both ways + parse raws."""
    d = Path(attempt_dir)
    reasons = []
    try:
        summary = json.loads((d / "attempt-summary.json").read_text())
    except (OSError, ValueError) as exc:
        return {"schema": None, "_reasons": [
            f"attempt-summary unreadable: {exc}"]}
    ledger = summary.get("artifacts") or {}
    # A dynamic self-declared ledger cannot replace the canonical collector plan.
    here = Path(__file__).resolve().parent
    coll = loadmod(here / "collect_postfacts_p25.py", "canonical_pf_replay")
    plan = coll.canonical_plan_dict(_load_sealed(here), roster=roster)
    required = {"plan.json"}
    for st in plan["steps"]:
        sid = st["id"]
        if st["kind"] in ("psql", "cli", "compose_ps", "inspect"):
            for stem in st["artifacts"]:
                base = f"{sid}.{stem}"
                required.update(base + suf for suf in (".cmd", ".rc", ".stdout", ".stderr"))
                cmd = d / (base + ".cmd")
                argv = list(st["argv"])
                if st.get("needs_cid_from"):
                    pp = d / (st["needs_cid_from"] + ".stdout")
                    cids = pp.read_text().splitlines() if pp.is_file() else []
                    if len(cids) != 1 or not re.fullmatch(r"[0-9a-f]{64}", cids[0].strip()):
                        reasons.append("original old fixture ps identity is invalid")
                    else:
                        argv[argv.index("<CID>")] = cids[0].strip()
                if not cmd.is_file() or cmd.read_text() != " ".join(argv) + "\n":
                    reasons.append(f"canonical command/target mismatch: {base}")
        elif st["kind"] == "http_get":
            required.add(f"{sid}.{st['read']}.derived")
            for url in st["urls"]:
                base = f"{sid}.{st['read']}.{url.rsplit('/', 1)[-1]}"
                required.update(base + suf for suf in (".url", ".http_status", ".body"))
                up = d / (base + ".url")
                if not up.is_file() or up.read_text() != f"GET {url}\n":
                    reasons.append(f"canonical HTTP target mismatch: {base}")
        else:
            for stem in st["artifacts"]:
                base = f"{sid}.{stem}"
                required.update((base, base + ".cmd", base + ".rc"))
                cp = d / (base + ".cmd")
                if not cp.is_file() or not cp.read_text().startswith(f"ARCHIVED-RAW {st['src']} <- "):
                    reasons.append(f"canonical archived identity mismatch: {base}")
    if set(ledger) != required:
        reasons.append("canonical raw member set mismatch")
    for rel in sorted(required):
        pp = d / rel
        if not pp.is_file():
            reasons.append(f"canonical raw missing: {rel}")
        elif rel.endswith(".rc") and (pp.read_text().strip() != "0" or str((ledger.get(rel) or {}).get("original_rc")) != "0"):
            reasons.append(f"original rc/ledger rc not zero: {rel}")
    try:
        if json.loads((d / "plan.json").read_text()) != plan:
            reasons.append("recorded plan != canonical plan")
    except (OSError, ValueError):
        reasons.append("recorded canonical plan missing/unreadable")
    # membership + re-hash both ways
    disk = {str(p.relative_to(d)) for p in d.rglob("*") if p.is_file()
            and p.name != "attempt-summary.json"}
    if disk != set(ledger):
        reasons.append(f"ledger membership NOT bidirectional "
                       f"(disk={len(disk)}, ledger={len(ledger)})")
    # canonical plan/cmd/rc/steps enforcement: the recorded steps must
    # be EXACTLY the canonical base set (two-way) and the attempt must
    # have ZERO failures
    expected_steps = []
    for st in plan["steps"]:
        if st["kind"] == "http_get":
            expected_steps.append(f"{st['id']}.{st['read']}")
        else:
            expected_steps.extend(f"{st['id']}.{stem}" for stem in st["artifacts"])
    if sorted(summary.get("steps_recorded") or []) != \
            sorted(expected_steps):
        reasons.append("steps_recorded != the canonical plan base "
                       "set (two-way)")
    if summary.get("failures") != []:
        reasons.append(f"attempt failures not empty: "
                       f"{str(summary.get('failures'))[:120]}")
    for name, entry in sorted(ledger.items()):
        p = d / name
        if not p.is_file():
            reasons.append(f"ledger artifact missing: {name}")
            continue
        raw = p.read_bytes()
        if len(raw) != entry.get("bytes") or hashlib.sha256(
                raw).hexdigest() != entry.get("sha256"):
            reasons.append(f"ledger re-hash mismatch: {name}")

    def _read(rel):
        p = d / rel
        return p.read_text() if p.is_file() else None

    def _rows_from_stdout(rel):
        text = _read(rel)
        if text is None:
            return None, f"{rel} missing"
        lines = [l for l in text.strip().splitlines() if l.strip()]
        if not lines:
            return None, f"{rel} empty"
        try:
            v = json.loads(lines[-1])
        except ValueError:
            return None, f"{rel} last line not JSON"
        return (v or []), None

    rows, err = _rows_from_stdout("01-controlled-rows.rows.json.stdout")
    if err:
        reasons.append(err)
    owners, err = _rows_from_stdout("02-cp-owners.owners.json.stdout")
    if err:
        reasons.append(err)
    counters = {"read1": {}, "read2": {}}
    for read in ("read1", "read2"):
        sid = "03-counters" if read == "read1" else "04-counters"
        derived = None
        dtext = _read(f"{sid}.{read}.derived")
        if dtext:
            try:
                derived = json.loads(dtext)
            except ValueError:
                reasons.append(f"{sid}.{read}.derived not JSON")
        for src in sorted((derived or {}).get("bodies_sha256") or {}):
            status = _read(f"{sid}.{read}.{src}.http_status")
            body_raw = d / f"{sid}.{read}.{src}.body"
            body = None
            if body_raw.is_file():
                try:
                    body = json.loads(body_raw.read_text())
                except ValueError:
                    body = None
            counters[read][src] = {
                "http_status": int(status.strip())
                if status and status.strip().isdigit() else status,
                "body": body}
    proxy_raw = _read("05-archived-proxy-quiet.stats.archived")
    proxy = None
    if proxy_raw:
        try:
            proxy = json.loads(proxy_raw)
        except ValueError:
            reasons.append("archived proxy quiet raw not JSON")
    fixture_raw = _read("06-archived-fixture-quiet.counters.archived")
    fixture = None
    if fixture_raw:
        try:
            v = json.loads(fixture_raw)
            fixture = v if isinstance(v, list) else None
            if fixture is None:
                reasons.append("archived fixture quiet raw not a list "
                               "of per-src counter bodies")
        except ValueError:
            reasons.append("archived fixture quiet raw not JSON")
    abc_raw = None
    abc_text = _read("07-abc-result.abc-result.raw.json")
    if abc_text:
        try:
            abc_raw = json.loads(abc_text)
        except ValueError:
            reasons.append("abc raw copy not JSON")
    return {"schema": "p25-postfacts-facts-v2",
            "_reasons": reasons,
            "controlled_rows": rows or [],
            "owner_rows": owners or [],
            "counters": counters,
            "proxy_quiet_raw": proxy,
            "fixture_quiet_raw": fixture,
            "abc_raw": abc_raw,
            "attempt_dir": str(d),
            "plan_sha256": summary.get("plan_sha256")}


def verify_final_safety(attempt_dir, frozen):
    d = Path(attempt_dir)
    cg = frozen["content_gates"]
    problems = []
    def text(base):
        p = d / (base + ".stdout")
        if not p.is_file() or not p.read_text().strip():
            problems.append(f"final safety raw missing/empty: {base}")
            return ""
        return p.read_text().strip()
    for base in ("05b-global-grants.count.txt", "08-fault-absent.function-count.txt", "08-fault-absent.trigger-count.txt"):
        value = text(base)
        if value.splitlines()[-1:] != ["0"]:
            problems.append(f"final safety count != 0: {base}")
    for label, expected in cg["legacy"]["expected_lines"].items():
        if text(f"06-legacy-bc.{label}.txt").splitlines()[-1:] != [expected]:
            problems.append(f"final legacy13 projection drift: {label}")
    volumes = text("10-volumes.names.txt").splitlines()
    if not set(cg["volumes_expected_names"]) <= set(volumes):
        problems.append("final expected Docker volume name missing")
    for name in ("q3-env-mode.txt", "pg-env-mode.txt"):
        if text("11-env-modes." + name) != "600":
            problems.append(f"final env mode != 600: {name}")
    fx = cg["fixture"]
    expected_state = f"{fx['boot']}|{fx['running']}|{fx['image']}|{fx['restart_policy']}"
    if text("12-fixture.state.txt") != expected_state or text("12-fixture.pid1.txt") != f"null|{fx['pid1']}":
        problems.append("final original fixture boot/image/state/PID1 drift")
    return problems


def verify_identity_catalog(catalog_path, attempt_dir, frozen):
    """Re-derive all allowed identities from actual reports/handles/responses.

    The catalog's defs are only a cached projection. The raw fixed-ID read
    and independent unfiltered global query must agree exactly; unknown
    global schedule rows are never filtered out.
    """
    cp, d = Path(catalog_path), Path(attempt_dir)
    ev = cp.parent.resolve()
    catalog = json.loads(cp.read_text())
    problems, supplied, parents, origins, handles = [], {}, {}, {}, set()
    if catalog.get("schema") != "p25-owned-identity-catalog-v1":
        return ["identity catalog schema mismatch"]
    def add(rid, source, path):
        if not UUID_RE.fullmatch(str(rid or "")):
            problems.append("catalog source artifact contains invalid def UUID")
            return
        handles.add(rid)
        origins.setdefault(rid, set()).add(str(path))
        if source:
            if not UUID_RE.fullmatch(str(source)) or (rid in parents and parents[rid] != source):
                problems.append(f"identity source parent drift: {rid}")
            parents[rid] = source
    def def_response(obj):
        if not isinstance(obj, dict) or obj.get("status") not in (200, 201):
            return None
        path = str(obj.get("path") or "")
        if not ((obj.get("method") == "POST" and path.endswith("/api/v1/data-connection/syncs")) or (obj.get("method") == "GET" and re.search(r"/api/v1/data-connection/syncs/[0-9a-f-]{36}$", path))):
            return None
        body = obj.get("resp_body")
        try:
            body = json.loads(body) if isinstance(body, str) else body
        except ValueError:
            return None
        return body if isinstance(body, dict) and body.get("id") and body.get("source_id") else None
    read_entries = []
    for entry in catalog.get("inputs") or []:
        p = Path(entry.get("path") or "").resolve()
        try:
            p.relative_to(ev)
        except ValueError:
            problems.append("catalog input outside this EV")
            continue
        if str(p) in supplied or not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest() != entry.get("sha256"):
            problems.append(f"catalog duplicate/missing/hash-drift input: {p}")
            continue
        kind = entry.get("kind")
        supplied[str(p)] = kind
        if kind == "catalog-def-read":
            read_entries.append((entry, p))
            continue
        obj = json.loads(p.read_text())
        if kind == "p24-result":
            for rid, row in (obj.get("owned_sync_ids") or {}).items():
                add(rid, row.get("source_id"), p)
        elif kind == "p25-handles":
            for row in obj.get("handles") or []:
                if row.get("kind") in ("def", "h10-def") and row.get("ref"):
                    add(row["ref"], row.get("source_id"), p)
        elif kind in ("p25-def-create-response", "p25-def-read-response"):
            body = def_response(obj)
            if body is None:
                problems.append(f"catalog def response lacks actual safe parent: {p}")
            else:
                add(body["id"], body["source_id"], p)
        else:
            problems.append(f"unknown identity catalog input kind: {kind}")
    # Enumerate real history independently: omitted failed-attempt reports,
    # handles, or observed successful def responses cannot shrink allowlist.
    required = {str(p.resolve()): "p24-result" for p in ev.rglob("p24-report.json") if p.is_file()}
    required.update({str(p.resolve()): "p25-handles" for p in ev.rglob("handles.json") if p.is_file()})
    for p in ev.rglob("*.json"):
        if p == cp or not p.is_file():
            continue
        try:
            obj = json.loads(p.read_text())
        except (ValueError, UnicodeDecodeError):
            continue
        if def_response(obj) is not None:
            required[str(p.resolve())] = "p25-def-create-response" if obj["method"] == "POST" else "p25-def-read-response"
    for path, kind in required.items():
        if supplied.get(path) != kind:
            problems.append(f"catalog omitted/mistyped actual identity history: {path}")
    if not handles or set(parents) != handles:
        problems.append("identity catalog whole union empty or missing actual source parents")
    if len(read_entries) != 1:
        return problems + ["catalog requires ONE original fixed-whole-union readonly def read"]
    entry, rp = read_entries[0]
    coll = loadmod(Path(__file__).resolve().parent / "collect_postfacts_p25.py", "catalog_fixed_sql")
    companion_ok = True
    for field in ("cmd", "rc", "stderr"):
        path = Path(entry.get(field + "_path") or "").resolve()
        try:
            path.relative_to(ev)
        except ValueError:
            problems.append(f"catalog original {field} artifact outside EV")
            companion_ok = False
            continue
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != entry.get(field + "_sha256"):
            problems.append(f"catalog original {field} artifact missing/hash drift")
            companion_ok = False
    if not companion_ok:
        return problems
    command = Path(entry.get("cmd_path") or "")
    rc = Path(entry.get("rc_path") or "")
    expected_cmd = " ".join(coll.psql_argv(Path(coll.ENV_DIR_DEFAULT), coll.identity_def_sql(handles))) + "\n" if handles else None
    if not command.is_file() or command.read_text() != expected_cmd or not rc.is_file() or rc.read_text().strip() != "0":
        problems.append("catalog read command/target/whole UUID set/original rc refused")
    text = rp.read_text().strip()
    rows = json.loads(text.splitlines()[-1]) if text else None
    if not isinstance(rows, list):
        return problems + ["catalog original fixed-ID read has no JSON row array"]
    by_id = {r.get("id"): r for r in rows}
    if set(by_id) != handles or len(rows) != len(by_id):
        problems.append("catalog readonly rows != whole actual identity union")
    expected_projection = {}
    for rid, row in by_id.items():
        if set(row) != {"id", "source_id", "cron", "enabled", "cursor_null"} or row.get("source_id") != parents.get(rid) or row.get("enabled") != "false" or row.get("cursor_null") != "true":
            problems.append(f"catalog final def parent/quiescence/raw schema refused: {rid}")
        expected_projection[rid] = {"def_id": rid, "source_id": row.get("source_id"), "cron": row.get("cron")}
    cached = {}
    for row in catalog.get("defs") or []:
        rid = row.get("def_id")
        if rid in cached:
            problems.append("duplicate cached identity def")
        cached[rid] = {k: row.get(k) for k in ("def_id", "source_id", "cron")}
        supplied_origins = {str(Path(p).resolve()) for p in row.get("derived_from", [])}
        if not origins.get(rid, set()) <= supplied_origins or not supplied_origins <= set(supplied):
            problems.append(f"catalog cached identity lacks actual derivation inputs: {rid}")
    if cached != expected_projection:
        problems.append("catalog defs cached projection differs from independently raw-derived identities")
    expected_global = {}
    cg = frozen["content_gates"]
    for row in cg["owned_fullstate"]["defs"]:
        if row["id"] in cg["global_catalog"]["nonempty_cron_ids"]:
            expected_global[row["id"]] = {k: row[k] for k in ("id", "source_id", "cron", "enabled", "cursor_null")}
    for rid, row in by_id.items():
        if row.get("cron"):
            expected_global[rid] = row
    def last_json(name):
        text = (d / name).read_text().strip()
        value = json.loads(text.splitlines()[-1]) if text else None
        return [] if value is None else value
    global_rows = last_json("05d-global-schedules.rows.json.stdout")
    global_by_id = {r.get("id"): r for r in global_rows}
    if global_by_id != expected_global or len(global_rows) != len(global_by_id):
        problems.append("unfiltered global nonempty cron full set/parents/quiescence differs from sealed+actual histories")
    global_ids = last_json("05c-global-nonempty-cron.ids.json.stdout")
    if sorted(r.get("id") for r in global_ids) != sorted(expected_global):
        problems.append("independent global cron ID roster mismatch (unknown rows never filtered)")
    return problems


def load_verified_tail(summary_path, bindings_path, logical_path_class=None):
    """Select an original tail or an independently rebuilt h14 supplement.

    Merely clearing failed/gate flags or rebinding a summary hash never
    selects a derived tail: the exact typed proof must pass its raw verifier.
    """
    import pathlib
    sp, bp = Path(summary_path), Path(bindings_path)
    summary = json.loads(sp.read_text())
    bindings = json.loads(bp.read_text())
    problems = []
    derived = summary.get("schema") == "p25-tail-aggregate-v1"
    if derived:
        if sp.name != "tail-aggregate.json" or bp != sp.parent / "tail-selected-bindings.json" or Path(summary.get("tail_bindings_file") or "") != bp:
            problems.append("typed tail aggregate selected artifact paths mismatch")
        saved = pathlib.Path
        try:
            if logical_path_class is not None:
                pathlib.Path = logical_path_class
            helper = loadmod(Path(__file__).resolve().parent / "verify_h14_supplement_p25.py", "h14_selected_tail_replay")
            verdict = helper.verify_supplement(sp, resolve_path=lambda value: Path(value))
            if verdict.get("ok") is not True:
                problems.append("typed h14 supplement raw replay refused: " + str(verdict.get("problems", [])))
        except Exception as exc:
            problems.append(f"typed h14 supplement verifier failed: {type(exc).__name__}: {exc}")
        finally:
            pathlib.Path = saved
    else:
        if sp.name != "tail-result.json" or "supplement" in summary or Path(summary.get("tail_bindings_file") or "") != bp:
            problems.append("original tail path/provenance mismatch; derived summaries require typed proof")
        # Normal path keeps actual case recorder bytes as its independent
        # source. The typed branch alone may replace the two known gates.
        cases = summary.get("scenarios") or []
        if sorted(c.get("id") for c in cases) != ["h11", "h14"]:
            problems.append("original tail exact scenario set mismatch")
        evidence = Path((summary.get("attempts") or {}).get("evidence_dir") or "")
        try:
            evidence.resolve().relative_to(sp.parent.resolve())
        except ValueError:
            problems.append("original tail recorder outside its EV")
        for case in cases:
            gates = case.get("gates") or []
            if case.get("failed") is not False or not gates or any(not isinstance(g, dict) or g.get("ok") is not True for g in gates):
                problems.append("original tail actual case/gates not PASS")
            matches = []
            for raw in evidence.glob(str(case.get("id")) + "-attempt-*.json"):
                try:
                    if json.loads(raw.read_text()) == case:
                        matches.append(raw)
                except ValueError:
                    continue
            if len(matches) != 1:
                problems.append("original tail actual scenario recorder missing/ambiguous")
    if summary.get("phase") != "tail" or summary.get("failed") != [] or (summary.get("quiet") or {}).get("ok") is not True:
        problems.append("selected tail phase/failure/quiet verdict refused")
    return {"summary": summary, "bindings": bindings, "derived": derived, "problems": problems}


def _load_relocated_roster(here, guard):
    """Retain guard checks while equating only the fixed archive path alias.

    Checks preserved: exact27 schema/types, group4+2 fixed IDs/disjointness,
    RFC anchors/full images, roster byteSHA baseline binding, and every
    seal-rederived group/anchor/image/counter/PROVENANCE value. Only the
    fixed logical archive string versus its resolved physical copy differs.
    """
    mod = loadmod(here / "p25_cohort6.py", "p25_relocated_roster")
    derive = loadmod(here / "p25_cohort_derive.py", "p25_relocated_roster_derive")
    schema = guard.expected_schema()
    problems = []
    if set(mod.SCHEMA_27) != schema or len(mod.GROUP1) != 4 or len(mod.GROUP2) != 2:
        problems.append("roster exact27/group4+2 shape mismatch")
    bad1, ids1 = guard.validate_roster_rows(mod.GROUP1, "GROUP1", schema)
    bad2, ids2 = guard.validate_roster_rows(mod.GROUP2, "GROUP2", schema)
    problems += bad1 + bad2
    if ids1 != set(guard.EXPECTED_GROUP1_IDS) or ids2 != set(guard.EXPECTED_GROUP2_IDS) or ids1 & ids2:
        problems.append("roster fixed group IDs/disjointness mismatch")
    for name in ("FINAL_ANCHOR", "HISTORICAL_ANCHOR_OLD4"):
        if not guard.RFC3339.match(str(getattr(mod, name) or "")):
            problems.append("roster anchor invalid: " + name)
    for name in ("FINAL_IMAGE", "DATASET_IMAGE", "PIPELINE_IMAGE"):
        try:
            guard.norm_image(getattr(mod, name), name)
        except ValueError as exc:
            problems.append(str(exc))
    if not isinstance(mod.PROVENANCE, dict) or not mod.PROVENANCE.get("hash_manifest_sha256"):
        problems.append("roster PROVENANCE SHA chain missing")
    sealed = derive.derive(Path(derive.SEAL_P24_FINAL_DEFAULT), Path(derive.SEAL_P24_TOOLS_DEFAULT))
    # Unknown archive strings are never normalized; both sides must name
    # exactly the original fixed archive and its explicit resolved copy.
    logical = derive.SEAL_P24_FINAL_DEFAULT
    if mod.PROVENANCE.get("archive") != logical or sealed["PROVENANCE"]["archive"] != str(Path(logical).resolve()):
        problems.append("roster archive alias is not the fixed logical/default physical copy")
    sealed["PROVENANCE"]["archive"] = logical
    for name in ("GROUP1", "GROUP2", "FINAL_ANCHOR", "FINAL_IMAGE", "DATASET_IMAGE", "PIPELINE_IMAGE", "AUX_ABSENT", "HISTORICAL_ANCHOR_OLD4", "FIXTURE_BOOT", "COHORT_COUNTER_SRCS", "PROVENANCE"):
        if getattr(mod, name) != sealed[name]:
            problems.append(name + " differs from fixed seal derivation")
    fb = json.loads((here / "p24-frozen-baseline.json").read_text())
    if fb.get("p25_cohort6_sha256") != hashlib.sha256((here / "p25_cohort6.py").read_bytes()).hexdigest():
        problems.append("roster byteSHA differs from frozen baseline binding")
    if problems:
        raise RuntimeError("relocated roster validation refused: " + "; ".join(problems))
    return mod


def verify_attempt(attempt_dir: Path, tail_decl_path: Path,
                   tail_bindings_path: Path = None, tail_summary_path: Path = None,
                   identity_catalog_path: Path = None, logical_path_class=None) -> dict:
    """FORMAL entry: rederive everything from the seal + the attempt's
    raw artifacts; hash-bind the tail declaration."""
    here = Path(__file__).resolve().parent
    reasons = []
    frozen = _load_sealed(here)
    guard = loadmod(here / "p25_cohort_guard.py", "g_pf")
    roster = _load_relocated_roster(here, guard) if logical_path_class is not None else guard.load_roster()
    # canonical plan sha rederivation (collector module owns the plan)
    coll = loadmod(here / "collect_postfacts_p25.py", "c_pf")
    plan = coll.canonical_plan_dict(frozen, roster=roster)
    plan_sha = hashlib.sha256(json.dumps(
        plan, indent=1, sort_keys=False).encode()).hexdigest()
    facts = replay_facts_from_attempt(attempt_dir, roster=roster)
    reasons += facts.pop("_reasons") or []
    reasons += verify_final_safety(attempt_dir, frozen)
    if identity_catalog_path is None:
        reasons.append("actual identity catalog input is mandatory")
    else:
        reasons += verify_identity_catalog(identity_catalog_path, attempt_dir, frozen)
    if facts.get("plan_sha256") != plan_sha:
        reasons.append(f"attempt plan sha {facts.get('plan_sha256')}"
                       f" != canonical {plan_sha}")
    tail_decl = json.loads(Path(tail_decl_path).read_text())
    decl_sha = hashlib.sha256(
        Path(tail_decl_path).read_bytes()).hexdigest()
    if tail_bindings_path is None or tail_summary_path is None:
        reasons.append("actual tail bindings/summary artifacts are mandatory")
    else:
        bp, sp = Path(tail_bindings_path), Path(tail_summary_path)
        selected = load_verified_tail(sp, bp, logical_path_class=logical_path_class)
        reasons += selected["problems"]
        bindings_doc, summary = selected["bindings"], selected["summary"]
        if sp.parent.resolve() != Path(tail_decl_path).parent.resolve():
            reasons.append("selected tail summary/declaration EV mismatch")
        actual = bindings_doc.get("bindings")
        if bindings_doc.get("schema") != "p25-tail-bindings-v1" or bindings_doc.get("problems") != []:
            reasons.append("tail actual-bindings schema/verdict mismatch")
        canonical_sha = hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest()
        if bindings_doc.get("bindings_sha256") != canonical_sha or bindings_doc.get("declaration_sha256") != tail_decl.get("declaration_sha256"):
            reasons.append("tail bindings/declaration SHA mismatch")
        if bindings_doc.get("summary_sha256") != hashlib.sha256(sp.read_bytes()).hexdigest() or summary.get("tail_bindings") != actual or summary.get("failed") != []:
            reasons.append("tail bindings summary final-byte SHA/content/verdict mismatch")
        def identities(rows, owner):
            return {(r.get("def_id") if owner else r.get("sync_def_id"), r.get("source_id"), r.get("run_id"), r.get("run_status") if owner else r.get("status")) for r in rows}
        raw_owner = {(r.get("sync_def_id"), r.get("source_id"), r.get("run_id"), r.get("run_status")) for r in facts.get("owner_rows", [])}
        raw_rows = facts.get("controlled_rows", [])
        raw_run_ids = {v.get("run_id") for k, v in (actual or {}).items() if k.startswith("run/")}
        raw_runs = {(r.get("sync_def_id"), r.get("source_id"), r.get("id"), r.get("status")) for r in raw_rows if r.get("id") in raw_run_ids}
        if identities(summary.get("final_owner_roster", []), True) != raw_owner or identities(summary.get("final_run_roster", []), False) != raw_runs:
            reasons.append("tail final-summary dual roster identities/status/parents differ from raw postfacts")
        facts["tail_bound_identities"] = actual
    abc_raw = facts.pop("abc_raw")
    if abc_raw is None:
        return {"ok": False, "reasons": reasons + [
            "abc raw copy missing from the attempt"]}
    verdict = adjudicate_postfacts(facts, frozen, guard, roster,
                                   tail_decl, abc_raw)
    verdict["reasons"] = verdict["reasons"] + reasons
    verdict["ok"] = verdict["ok"] and not reasons
    verdict["tail_declaration_sha256"] = decl_sha
    verdict["plan_sha256"] = plan_sha
    verdict["attempt_dir"] = str(Path(attempt_dir).resolve())
    verdict["tail_declaration"] = str(Path(tail_decl_path).resolve())
    verdict["tail_bindings"] = str(Path(tail_bindings_path).resolve()) if tail_bindings_path else None
    verdict["tail_summary"] = str(Path(tail_summary_path).resolve()) if tail_summary_path else None
    verdict["identity_catalog"] = str(Path(identity_catalog_path).resolve()) if identity_catalog_path else None
    return verdict


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--attempt-dir", required=True)
    ap.add_argument("--tail-declaration", required=True)
    ap.add_argument("--tail-bindings", required=True)
    ap.add_argument("--tail-summary", required=True)
    ap.add_argument("--identity-catalog", required=True)
    ap.add_argument("--out")
    args = ap.parse_args()
    verdict = verify_attempt(Path(args.attempt_dir),
                             Path(args.tail_declaration),
                             Path(args.tail_bindings), Path(args.tail_summary),
                             Path(args.identity_catalog))
    text = json.dumps(verdict, indent=1, sort_keys=True) + "\n"
    if args.out:
        Path(args.out).write_text(text)
    # ALWAYS print the verdict too — the run_step-captured stdout's
    # last line is the finalizer's verdict evidence (ARCHIVE-
    # INTERFACE §1/§4), with or without --out.
    print(json.dumps({"ok": verdict["ok"],
                      "reasons": verdict["reasons"][:5],
                      "final_roster_size": verdict.get(
                          "final_roster_size")}))
    return 0 if verdict["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""P2.5 readonly preflight VERIFIER (formal; seal-derived, pure).

Round-2 closures (review-tools-code-round2 items 4/5/6 + 2/3 gates):

  * The FORMAL CLI accepts ONLY an attempt dir. The canonical plan,
    frozen baseline and roster are REDERIVED from the fixed,
    root-reviewed P24 seal (archive + tool/provenance hash chain via
    p25_cohort_derive.derive); the sibling generated files must EXACTLY
    equal the derivation (key sets included) — double-editing baseline
    + cohort can no longer forge expectations. No --plan /
    --frozen-baseline CLI inputs exist; toy data is injectable ONLY
    into the internal verify_attempt() pure function by tests.
  * Consumption ledger crosscheck: every attempt artifact is hashed
    and byte-counted again; each must appear in attempt-summary with
    identical sha/bytes and its recorded ORIGINAL rc; unknown extras
    fail; attempt-summary.plan_sha256 must equal the canonical plan
    sha. File existence alone is never consumption proof.
  * Content gates now include: owned-fullstate EXACT sealed-17
    comparison (queries themselves seal-filtered), GLOBAL grants
    total==0 and global nonempty-cron EXACT sealed set, C txn FLAT
    shape with branch_name=='main' + id/dataset_id/status/lineage, C
    file-list ownership binding (file entry transaction_id==txn,
    logical_path carries the run id), local-sha EXACT three keys with
    no MISSING, and source-p24-baseline.txt sha256 == sealed combined
    8cd25d42… .
  * Counter discipline: request_count / inventory_started /
    inventory_finished must be PRESENT NON-BOOL ints; active must be
    the exact int 0 (bool False is rejected); started==finished; both
    reads complete and equal on every field.

PURE: reads files only; no subprocesses, no network.
"""

import argparse
import hashlib
import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def loadmod(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def norm(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    return str(v)


def norm_row(row):
    return {k: norm(v) for k, v in sorted(row.items())}


def norm_rows(rows):
    if rows is None:
        return []
    return sorted((norm_row(r) for r in rows),
                  key=lambda r: json.dumps(r, sort_keys=True))


def last_json(txt):
    """Last meaningful line: psql session SET tags are skipped; the
    remaining last non-empty line must be the JSON payload."""
    lines = [l.strip() for l in (txt or "").splitlines()
             if l.strip() and l.strip().upper() != "SET"]
    if not lines:
        raise ValueError("empty projection output")
    return json.loads(lines[-1])


def last_value(txt):
    lines = [l.strip() for l in (txt or "").splitlines()
             if l.strip() and l.strip().upper() != "SET"]
    return lines[-1] if lines else None


# --- membership + ledger crosscheck -----------------------------------

def expected_cmd_text(step, attempt):
    argv = list(step["argv"])
    if step.get("needs_cid_from"):
        stem = step["needs_cid_from"]
        ps = attempt / f"{stem}.stdout"
        if not ps.is_file():
            return None
        cids = [l for l in ps.read_text().splitlines() if l.strip()]
        if len(cids) != 1:
            return None
        argv[argv.index("<CID>")] = cids[0]
    return " ".join(argv) + "\n"


def adjudicate_membership(attempt: Path, plan: dict,
                          c_supplement: Path = None):
    reasons = []
    declared = set()
    for step in plan["steps"]:
        sid = step["id"]
        if c_supplement is not None and step["kind"] == "http_get" \
                and sid == "07-c-live":
            # (#2-narrow) the BASE url/cmd/raw membership checks are
            # PRESERVED even when the status is superseded — both the
            # base and the supplement URLs must be canonical.
            for stem, url in zip(step["artifacts"], step["urls"]):
                u = attempt / f"{sid}.{stem}.url"
                cmdb = attempt / f"{sid}.{stem}.cmd"
                if not u.is_file():
                    reasons.append(f"{sid}.{stem}.url MISSING (base)")
                elif u.read_text() != f"GET {url}\n":
                    reasons.append(f"{sid}.{stem}.url(base) != "
                                   "canonical plan URL")
                if cmdb.is_file() and cmdb.read_text() != \
                        "GET (http) " + ", ".join(step["urls"]) + "\n":
                    reasons.append(f"{sid}.{stem}.cmd(base) != "
                                   "canonical")
            if all(
                    (attempt / f"{sid}.{st}.http_status").is_file()
                    and (attempt / f"{sid}.{st}.http_status")
                    .read_text().strip() == "404"
                    for st in step["artifacts"]):
                # fresh C evidence lives in the supplement (root-ruled
                # same-scope actor GETs); the gate is NOT exempted — it
                # is ENFORCED from the fresh artifacts. The base's 07
                # files stay archived (declared so they are not
                # 'unknown extras'); their 404 statuses are the known
                # historical facts superseded same-target.
                for stem in step["artifacts"]:
                    for suffix in (".url", ".http_status", ".body"):
                        declared.add(f"{sid}.{stem}{suffix}")
                for stem, url in zip(step["artifacts"], step["urls"]):
                    st_f = c_supplement / f"{sid}.{stem}.http_status"
                    u_f = c_supplement / f"{sid}.{stem}.url"
                    b_f = c_supplement / f"{sid}.{stem}.body"
                    if not st_f.is_file() or not u_f.is_file() \
                            or not b_f.is_file():
                        reasons.append(f"supplement {stem} artifact "
                                       "missing")
                        continue
                    if u_f.read_text() != f"GET {url}\n":
                        reasons.append(f"supplement {stem}.url != "
                                       "canonical URL")
                    if st_f.read_text().strip() != "200":
                        reasons.append(f"supplement {stem} http_status "
                                       f"{st_f.read_text().strip()} != "
                                       "200")
                continue
            # NOT all-404 (base 500/corrupt/unknown): fall through to
            # the generic http_get membership checks — supersession is
            # impossible; everything must pass on its own.
        if step["kind"] in ("compose_ps", "inspect", "psql", "cli"):
            for stem in step["artifacts"]:
                base = f"{sid}.{stem}"
                declared.update({base + ".cmd", base + ".rc",
                                 base + ".stdout", base + ".stderr"})
                want_cmd = expected_cmd_text(step, attempt)
                if want_cmd is None:
                    reasons.append(f"{base}: cid source unusable")
                    continue
                got_cmd = attempt / f"{base}.cmd"
                if not got_cmd.is_file():
                    reasons.append(f"{base}.cmd MISSING")
                    continue
                if got_cmd.read_text() != want_cmd:
                    reasons.append(
                        f"{base}.cmd != plan-rendered argv — whitelist "
                        f"violation (got {got_cmd.read_text()[:80]!r})")
                rc = attempt / f"{base}.rc"
                if not rc.is_file():
                    reasons.append(f"{base}.rc MISSING")
                elif rc.read_text().strip() != "0":
                    reasons.append(f"{base}.rc="
                                   f"{rc.read_text().strip()!r} — original"
                                   " rc must be a real 0")
                for suf in (".stdout", ".stderr"):
                    if not (attempt / f"{base}{suf}").is_file():
                        reasons.append(f"{base}{suf} MISSING")
                if step.get("expect_empty"):
                    out = attempt / f"{base}.stdout"
                    if out.is_file() and out.read_text().strip() != "":
                        reasons.append(
                            f"{base}: expected EMPTY stdout (absence "
                            "proof incl stopped containers)")
        elif step["kind"] == "http_get":
            if step["id"] == "09-protected-counters":
                # (#3) EXACT canonical content — existence alone proves
                # nothing: url/cmd/kind texts and per-src raw retention
                # (.http_status + .body) are compared byte-exactly.
                stem = step["artifacts"][0]
                base = f"{sid}.{stem}"
                declared.update({base + ".cmd", base + ".derived",
                                 base + ".kind"})
                want_cmd = ("GET (http) "
                            + ", ".join(step["urls"]) + "\n")
                got_cmd = attempt / f"{base}.cmd"
                if not got_cmd.is_file():
                    reasons.append(f"{base}.cmd MISSING")
                elif got_cmd.read_text() != want_cmd:
                    reasons.append(f"{base}.cmd != canonical counter "
                                   "command text")
                kind = attempt / f"{base}.kind"
                if not kind.is_file():
                    reasons.append(f"{base}.kind MISSING")
                elif kind.read_text() != "http\n":
                    reasons.append(f"{base}.kind != 'http'")
                if not (attempt / f"{base}.derived").is_file():
                    reasons.append(f"{base}.derived MISSING")
                read = step["artifacts"][0].split(".")[0]
                for url in step["urls"]:
                    src = url.rsplit("/", 1)[-1]
                    base = f"{sid}.{read}.{src}"
                    for suffix in (".url", ".http_status", ".body"):
                        declared.add(base + suffix)
                    u = attempt / f"{base}.url"
                    if not u.is_file():
                        reasons.append(f"{base}.url MISSING")
                    elif u.read_text() != f"GET {url}\n":
                        reasons.append(f"{base}.url != canonical plan "
                                       "URL")
                    stf = attempt / f"{base}.http_status"
                    if not stf.is_file():
                        reasons.append(f"{base}.http_status MISSING "
                                       "(raw chain broken)")
                    elif stf.read_text().strip() != "200":
                        reasons.append(f"{base} raw http_status "
                                       f"{stf.read_text().strip()!r} "
                                       "!= 200")
                    if not (attempt / f"{base}.body").is_file():
                        reasons.append(f"{base}.body MISSING (raw "
                                       "chain broken)")
            else:
                for stem, url in zip(step["artifacts"], step["urls"]):
                    base = f"{sid}.{stem}"
                    declared.update({base + ".url",
                                     base + ".http_status",
                                     base + ".body"})
                    if not (attempt / f"{base}.url").is_file():
                        reasons.append(f"{base}.url MISSING")
                    elif (attempt / f"{base}.url").read_text() != \
                            f"GET {url}\n":
                        reasons.append(f"{base}.url != canonical plan "
                                       "URL")
                    st = attempt / f"{base}.http_status"
                    if not st.is_file():
                        reasons.append(f"{base}.http_status MISSING")
                    elif st.read_text().strip() != \
                            str(step.get("expect_status", 200)):
                        reasons.append(
                            f"{base}: http_status "
                            f"{st.read_text().strip()} != "
                            f"{step.get('expect_status', 200)}")
                    if not (attempt / f"{base}.body").is_file():
                        reasons.append(f"{base}.body MISSING")
        elif step["kind"] == "local_sha":
            stem = step["artifacts"][0]
            base = f"{sid}.{stem}"
            declared.update({base + ".kind", base + ".derived"})
            for suf in (".kind", ".derived"):
                if not (attempt / f"{base}{suf}").is_file():
                    reasons.append(f"{base}{suf} MISSING")
            if (attempt / f"{base}.exception").is_file():
                declared.add(base + ".exception")
                reasons.append(f"{base}: local derivation recorded an "
                               "exception — must be adjudicated")
    for p in sorted(attempt.iterdir()):
        if p.name in ("attempt-summary.json",) or p.name.startswith("."):
            continue
        if p.name not in declared:
            reasons.append(f"UNKNOWN artifact {p.name} — not declared "
                           "by the plan (no silent extras)")
    return (not reasons), reasons


def adjudicate_supplement(c_supp: Path, plan: dict, frozen: dict,
                          base_attempt: Path):
    """(#1) The supplement is a FIRST-CLASS evidence artifact, not a
    presence check: exact 3 canonical GET members; url/http_status/
    body original bytes + SHA/length vs ITS OWN ledger; exact schema +
    attempt_dir identity; supplements_base_attempt must equal the base
    path AND record the BASE SUMMARY sha256 (so a different base, or a
    base whose summary changed after minting, refuses); actor owner/
    org must equal the sealed C parents; target/txn/file refs must be
    the sealed values. Raw tamper without ledger update is rejected."""
    import hashlib as _h
    reasons = []
    summ_p = c_supp / "attempt-summary.json"
    if not summ_p.is_file():
        return False, ["supplement summary missing"], None
    summ = json.loads(summ_p.read_text())
    summ_sha = _h.sha256(summ_p.read_bytes()).hexdigest()

    if summ.get("schema") != "p25-c-supplement-v1":
        reasons.append("supplement schema wrong")
    if Path(summ.get("attempt_dir") or "?").resolve() != \
            c_supp.resolve():
        reasons.append("supplement attempt_dir mismatch")
    if Path(summ.get("supplements_base_attempt") or "?").resolve() \
            != base_attempt.resolve():
        reasons.append(f"supplement binds base "
                       f"{summ.get('supplements_base_attempt')!r} != "
                       f"actual {str(base_attempt)!r}")
    # base binding: from the summary itself OR the verifier-minted
    # sibling base-binding.json (originals immutable; the sibling is
    # hashed into the final formal seal). Mismatch always refuses.
    bind_p = c_supp / "base-binding.json"
    binding_sha = None
    base_summ_sha = summ.get("base_summary_sha256")
    bind = None
    if bind_p.is_file():
        bind = json.loads(bind_p.read_text())
        binding_sha = _h.sha256(bind_p.read_bytes()).hexdigest()
        # the sibling must be FULLY consistent: exact base path AND
        # BOTH summary shas (base + this supplement)
        if Path(str(bind.get("base_attempt") or "?")).resolve() != \
                base_attempt.resolve():
            reasons.append("base-binding.json base_attempt path "
                           "mismatch")
        if bind.get("supplement_summary_sha256") != summ_sha:
            reasons.append("base-binding.json supplement summary sha "
                           "mismatch")
        if not base_summ_sha:
            base_summ_sha = bind.get("base_summary_sha256")
    actual = _h.sha256((base_attempt / "attempt-summary.json")
                       .read_bytes()).hexdigest()
    if not base_summ_sha:
        reasons.append("supplement missing base_summary_sha256 "
                       "binding (summary or base-binding.json)")
    elif base_summ_sha != actual:
        reasons.append("supplement base_summary_sha256 != current "
                       "base summary (base changed after minting?)")

    cg = frozen["content_gates"]
    cl = cg["c_live_sealed"]
    actor = summ.get("actor_identity") or {}
    # canonical refs
    steps = sorted(summ.get("steps_recorded") or [])
    want_stems = ["07-c-live.file.bin", "07-c-live.files.json",
                  "07-c-live.txn.json"]
    if steps != want_stems:
        reasons.append(f"supplement steps {steps} != exact canonical "
                       f"three {want_stems}")

    # find the canonical 07 URLs from the plan
    url_by_stem = {}
    for step in plan["steps"]:
        if step["id"] == "07-c-live" and step["kind"] == "http_get":
            for stem, u in zip(step["artifacts"], step["urls"]):
                url_by_stem[stem] = u
    want_bare = {st[len("07-c-live."):] for st in want_stems}
    if set(url_by_stem) != want_bare:
        reasons.append("plan lacks the canonical 07 c-live steps")

    ledger = summ.get("artifacts")
    if not isinstance(ledger, dict) or not ledger:
        reasons.append("supplement ledger missing")
        ledger = ledger or {}
    supp_files = {p.name for p in c_supp.iterdir()
                  if p.name not in ("attempt-summary.json",
                                    "base-binding.json")}
    want_files = set()
    for stem in want_stems:
        for suffix in (".url", ".http_status", ".body"):
            want_files.add(f"{stem}{suffix}")
    for extra in sorted(supp_files - want_files):
        reasons.append(f"supplement UNKNOWN file {extra}")
    for miss in sorted(want_files - supp_files):
        reasons.append(f"supplement missing file {miss}")

    for stem, url in sorted(url_by_stem.items()):
        u = c_supp / f"{stem}.url"
        if u.is_file() and u.read_text() != f"GET {url}\n":
            reasons.append(f"supplement {stem}.url != canonical")
        # sealed refs must appear in the canonical URLs
    joined = " ".join(url_by_stem.values())
    if cl["target"] not in joined or cl["txn_id"] not in joined \
            or cl["bound_files"][0] not in joined:
        reasons.append("canonical supplement URLs do not carry the "
                       "sealed target/txn/file refs")

    # EXACT keyset: precisely the 9 url/status/body entries — a
    # deleted entry (or an extra inside the ledger) is itself a
    # failure; no optional-skip.
    if set(ledger) != want_files:
        for miss in sorted(want_files - set(ledger)):
            reasons.append(f"supplement ledger entry MISSING {miss}")
        for extra in sorted(set(ledger) - want_files):
            reasons.append(f"supplement ledger UNKNOWN entry {extra}")
    for name in sorted(want_files & set(ledger)):
        f = c_supp / name
        meta = ledger[name]
        if not isinstance(meta, dict):
            reasons.append(f"supplement ledger {name}: non-dict entry")
            continue
        for field in ("bytes", "sha256"):
            if field not in meta:
                reasons.append(f"supplement ledger {name}: field "
                               f"{field} missing")
        if not f.is_file():
            continue  # file-level miss already reported above
        actual = {"bytes": f.stat().st_size,
                  "sha256": _h.sha256(f.read_bytes()).hexdigest()}
        if meta.get("bytes") != actual["bytes"] \
                or meta.get("sha256") != actual["sha256"]:
            reasons.append(f"supplement {name}: ledger bytes/sha != "
                           "raw (tamper without ledger update)")
        if name.endswith(".http_status"):
            if "original_rc" not in meta:
                reasons.append(f"supplement ledger {name}: "
                               "original_rc missing")
            elif meta.get("original_rc") != f.read_text().strip():
                reasons.append(f"supplement {name}: ledger status != "
                               "raw status")
    return (not reasons), reasons, {"summary_sha256": summ_sha,
                                    "actor": actor,
                                    "ledger": ledger,
                                    "binding_sha256": binding_sha}


def adjudicate_ledger(attempt: Path, plan: dict, canonical_plan_sha: str,
                      c_supplement: Path = None):
    """Round-2 item 5: independent consumption proof — recompute every
    file's sha/bytes and require exact ledger membership + recorded
    original rc + canonical plan sha."""
    reasons = []
    summary_p = attempt / "attempt-summary.json"
    if not summary_p.is_file():
        return False, ["attempt-summary.json MISSING"]
    summary = json.loads(summary_p.read_text())
    if summary.get("plan_sha256") != canonical_plan_sha:
        reasons.append(f"attempt plan_sha256 "
                       f"{summary.get('plan_sha256')} != canonical "
                       f"{canonical_plan_sha}")
    ledger = summary.get("artifacts")
    if not isinstance(ledger, dict) or not ledger:
        reasons.append("attempt-summary consumption ledger missing")
        return (not reasons), reasons
    # (#3 residual) attempt_dir must be PRESENT and EXACTLY equal —
    # missing/null never passes
    if summary.get("attempt_dir") != str(attempt):
        reasons.append(f"summary.attempt_dir "
                       f"{summary.get('attempt_dir')!r} missing/null/"
                       "unequal — must exactly equal the dir under "
                       f"adjudication {str(attempt)!r}")
    # (#3) steps_recorded must be the COMPLETE canonical set (exact
    # base names the plan declares; counter per-src raws are ledger
    # artifacts, not steps)
    want_steps = set()
    for step in plan["steps"]:
        if step["kind"] == "http_get" \
                and step["id"] != "09-protected-counters":
            for stem in step["artifacts"]:      # c-live: every stem
                want_steps.add(f"{step['id']}.{stem}")
        else:
            want_steps.add(f"{step['id']}.{step['artifacts'][0]}")
    got_steps = set(summary.get("steps_recorded") or [])
    for missing in sorted(want_steps - got_steps):
        reasons.append(f"steps_recorded missing canonical step base "
                       f"{missing}")
    for extra in sorted(got_steps - want_steps):
        reasons.append(f"steps_recorded has non-canonical base "
                       f"{extra}")
    disk = {}
    for p in sorted(attempt.iterdir()):
        if p.name == "attempt-summary.json" or p.name.startswith(".") \
                or not p.is_file():
            continue
        disk[p.name] = {"bytes": p.stat().st_size,
                        "sha256": hashlib.sha256(
                            p.read_bytes()).hexdigest()}
    for name, meta in sorted(disk.items()):
        got = ledger.get(name)
        if got is None:
            reasons.append(f"artifact {name} on disk but NOT in the "
                           "consumption ledger")
            continue
        if got.get("sha256") != meta["sha256"] or \
                got.get("bytes") != meta["bytes"]:
            reasons.append(f"artifact {name} ledger sha/bytes mismatch "
                           f"(ledger {got} vs disk {meta})")
    for name in sorted(set(ledger) - set(disk)):
        reasons.append(f"ledger entry {name} has no disk file")
    if summary.get("failures"):
        reasons.append(f"attempt recorded {len(summary['failures'])} "
                       "collector-side failure(s): "
                       f"{summary['failures'][:3]}")
    canonical_07 = set()
    if c_supplement is not None:
        for step in plan["steps"]:
            if step["id"] == "07-c-live" and step["kind"] == "http_get":
                canonical_07 = {f"07-c-live.{st}" for st in
                                step["artifacts"]}
    for name, meta in sorted(ledger.items()):
        if c_supplement is not None \
                and name.rsplit(".", 1)[0] in canonical_07 \
                and name.endswith(".http_status") \
                and meta.get("original_rc") == "404":
            # EXACTLY the known canonical three service-JWT 404 facts
            # may be superseded by the same-target fresh GETs (gate
            # still enforced from the supplement). Everything else —
            # 500/corrupt/unknown/noncanonical — still fails below.
            continue
        rc = meta.get("original_rc")
        if name.endswith(".rc"):
            if rc is None:
                reasons.append(f"{name}: ledger original_rc MISSING")
            else:
                actual = (attempt / name).read_text().strip()
                if rc != actual:
                    reasons.append(f"{name}: ledger original_rc "
                                   f"{rc!r} != actual .rc {actual!r}")
                if actual != "0":
                    reasons.append(f"{name}: original rc {actual!r} "
                                   "!= 0")
        elif name.endswith(".http_status"):
            if rc is None:
                reasons.append(f"{name}: ledger status MISSING")
            else:
                actual = (attempt / name).read_text().strip()
                if rc != actual:
                    reasons.append(f"{name}: ledger status {rc!r} != "
                                   f"actual .http_status {actual!r}")
                if actual != "200":
                    reasons.append(f"{name}: http status {actual!r} "
                                   "!= 200")
    return (not reasons), reasons


# --- content gates ------------------------------------------------------

def _strict_int(v):
    return isinstance(v, int) and not isinstance(v, bool)


def adjudicate_content(attempt: Path, plan: dict, guard_mod, roster,
                       frozen: dict, c_supplement: Path = None):
    reasons = []
    cg = frozen["content_gates"]

    def out(base, suf=".stdout"):
        # root-ruled C supplement: the 07-c-live artifacts may come
        # from a FRESH c-only readonly attempt (c_live_reverify_p25);
        # base attempt files are NEVER modified or overwritten — the
        # resolver merely reads the supplement first for 07 prefixes.
        if c_supplement is not None and \
                base.startswith("07-c-live.") and \
                suf in (".body", ""):
            p = c_supplement / f"{base}.body"
            if p.is_file() and suf == ".body":
                return None  # body handled via direct path below
        p = attempt / f"{base}{suf}"
        return p.read_text() if p.is_file() else None

    line = (out("01-connector-boot-image.inspect.out") or "").strip()
    parts = line.split("|")
    boot, image = roster.FINAL_ANCHOR, roster.FINAL_IMAGE
    if len(parts) != 2:
        reasons.append("01: connector inspect line malformed")
    else:
        boot, image = parts[0], parts[1]
        if boot != roster.FINAL_ANCHOR:
            reasons.append(f"01: connector boot {boot} != sealed final "
                           f"anchor {roster.FINAL_ANCHOR}")
        if image != roster.FINAL_IMAGE:
            reasons.append(f"01: connector image {image} != sealed "
                           f"{roster.FINAL_IMAGE}")
    for sid, want in (("02-dataset-boot-image", roster.DATASET_IMAGE),
                      ("02b-pipeline-boot-image", roster.PIPELINE_IMAGE)):
        line = (out(f"{sid}.inspect.out") or "").strip()
        parts = line.split("|")
        if len(parts) != 2 or parts[1] != want:
            reasons.append(f"{sid}: image != sealed {want}")

    for svc in ("connector2", "connector3", "dataset-proxy"):
        if out(f"03-aux-absent.{svc}.out") is None:
            reasons.append(f"03: {svc} artifact missing")

    rows_txt = out("04-controlled-rows.rows.json")
    if rows_txt is None:
        reasons.append("04: controlled rows missing")
    else:
        try:
            rows = last_json(rows_txt) or []
        except ValueError:
            rows = None
            reasons.append("04: controlled rows not JSON")
        if rows is not None:
            ok, sub = guard_mod.adjudicate_first_transition(
                rows, roster, boot, image)
            if not ok:
                reasons += [f"04/first-transition: {r}" for r in sub]

    want = cg["owned_fullstate"]
    for stem, sealed, label in (
            ("owned-defs.json", want["defs"], "defs"),
            ("owned-ticks.json", want["ticks"], "ticks"),
            ("owned-runs.json", want["runs"], "runs"),
            ("owned-grants.json", want["grants"], "grants")):
        txt = out(f"05-owned-fullstate.{stem}")
        if txt is None:
            reasons.append(f"05: {stem} missing")
            continue
        stripped = [l for l in txt.splitlines()
                    if l.strip() and l.strip().upper() != "SET"]
        if not stripped:
            if label == "grants":
                continue   # json_agg NULL over zero rows: valid empty
            reasons.append(f"05: {stem} EMPTY — sealed expects "
                           f"{len(sealed)} rows; never a silent empty")
            continue
        try:
            live = last_json(txt)
        except ValueError:
            reasons.append(f"05: {stem} not JSON")
            continue
        if norm_rows(live) != norm_rows(sealed):
            reasons.append(f"05: live {label} != sealed fullstate "
                           f"(live {len(live or [])} vs sealed "
                           f"{len(sealed)})")
    gtxt = out("05b-global-grants.count.txt")
    if gtxt is None:
        reasons.append("05b: global grants count missing")
    else:
        gv = last_value(gtxt)
        if gv != "0":
            reasons.append(f"05b: GLOBAL grants total {gv!r} != 0")
    ctxt = out("05c-global-nonempty-cron.ids.json")
    if ctxt is None:
        reasons.append("05c: global nonempty-cron missing")
    else:
        try:
            live_ids = sorted({str(e["id"]) for e in
                               (last_json(ctxt) or [])})
        except (ValueError, KeyError, TypeError):
            live_ids = None
            reasons.append("05c: nonempty-cron output unparsable")
        if live_ids is not None and live_ids != \
                cg["global_catalog"]["nonempty_cron_ids"]:
            reasons.append("05c: global nonempty-cron ids != sealed "
                           "set")

    for label, expected_line in cg["legacy"]["expected_lines"].items():
        txt = out(f"06-legacy-bc.{label}.txt")
        if txt is None:
            reasons.append(f"06: {label} missing")
            continue
        lines = [l for l in txt.splitlines() if l.strip()]
        if lines[-1] != expected_line:
            reasons.append(f"06: legacy {label} projection line != "
                           f"sealed ({lines[-1]!r})")

    # 07 C live — flat txn + download bytes + files-list ownership
    cl = cg["c_live_sealed"]

    def cpath(name):
        if c_supplement is not None and \
                (c_supplement / name).is_file():
            return c_supplement / name
        return attempt / name

    txn_body = cpath("07-c-live.txn.json.body")
    file_body = cpath("07-c-live.file.bin.body")
    files_body = cpath("07-c-live.files.json.body")
    if not txn_body.is_file() or not file_body.is_file() \
            or not files_body.is_file():
        reasons.append("07: c-live bodies missing")
    else:
        try:
            txn = json.loads(txn_body.read_text())
        except ValueError:
            txn = None
            reasons.append("07: C txn body not JSON")
        if txn is not None:
            if str(txn.get("id", "")).lower() != str(cl["txn_id"]).lower():
                reasons.append("07: C txn id != sealed txn id")
            if str(txn.get("dataset_id", "")).lower() != \
                    str(cl["target"]).lower():
                reasons.append("07: C txn dataset_id != sealed target")
            if txn.get("branch_name") != "main":
                reasons.append(f"07: C txn branch_name "
                               f"{txn.get('branch_name')!r} != 'main'")
            if txn.get("status") != "COMMITTED":
                reasons.append(f"07: C txn status "
                               f"{txn.get('status')!r} != COMMITTED")
            meta = txn.get("metadata") or {}
            lineage = (meta.get("lineage") or {}).get("sync_run_id") \
                if isinstance(meta, dict) else None
            if lineage != cl.get("lineage_run"):
                reasons.append("07: C txn metadata.lineage.sync_run_id "
                               "!= sealed lineage run")
        blob = file_body.read_bytes()
        if len(blob) != cl["bytes"]:
            reasons.append(f"07: C file bytes {len(blob)} != sealed "
                           f"{cl['bytes']}")
        if hashlib.sha256(blob).hexdigest() != cl["sha256"]:
            reasons.append("07: C file sha256 != sealed artifact hash")
        try:
            flist = json.loads(files_body.read_text())
            entries = flist.get("files") if isinstance(flist, dict) \
                else flist
            entry = next((e for e in (entries or [])
                          if str(e.get("id")) == str(
                              cl["bound_files"][0])), None)
        except (ValueError, AttributeError, TypeError):
            entry = None
        if entry is None:
            reasons.append("07: C bound file not present in files list "
                           "(ownership unproven)")
        else:
            if str(entry.get("transaction_id", "")).lower() != \
                    str(cl["txn_id"]).lower():
                reasons.append("07: C file transaction_id != sealed txn "
                               "(ownership broken)")
            if str(cl.get("lineage_run")) not in json.dumps(entry):
                reasons.append("07: C file entry lacks the run lineage "
                               "(logical_path)")

    for stem in ("function-count.txt", "trigger-count.txt"):
        v = last_value(out(f"08-fault-absent.{stem}") or "")
        if v != "0":
            reasons.append(f"08: {stem} count {v!r} != 0 — old injected "
                           "fault function/trigger still present")

    # 09 counters — strict typing (round-2 #6) + RAW-CHAIN REPLAY
    # (round3 #4): the derived snapshot is re-derived from the RETAINED
    # raw bodies+statuses and must match; each body self-identifies
    # name==src; bodies_sha256 must equal sha256(raw bytes).
    exp = cg["protected_counters"]["expected"]
    reads = {}
    for read in ("read1", "read2"):
        p = attempt / f"09-protected-counters.{read}.json.derived"
        if not p.is_file():
            reasons.append(f"09: {read} derived missing")
            continue
        derived = json.loads(p.read_text())
        replay = {}
        read_tag = read  # read1|read2 from the loop variable
        for name in exp:
            body_f = attempt / \
                f"09-protected-counters.{read_tag}.{name}.body"
            st_f = attempt / \
                f"09-protected-counters.{read_tag}.{name}.http_status"
            if not body_f.is_file() or not st_f.is_file():
                reasons.append(f"09: {read} raw chain missing for "
                               f"{name}")
                replay[name] = None
                continue
            raw = body_f.read_bytes()
            try:
                parsed = json.loads(raw)
            except ValueError:
                parsed = None
                reasons.append(f"09: {read} raw body for {name} not "
                               "JSON")
            if isinstance(parsed, dict) \
                    and parsed.get("name") != name:
                reasons.append(f"09: {read} body name "
                               f"{parsed.get('name')!r} != src {name!r}")
            want_sha = (derived.get("bodies_sha256") or {}).get(name)
            if want_sha != hashlib.sha256(raw).hexdigest():
                reasons.append(f"09: {read} bodies_sha256[{name}] != "
                               "sha256(raw body) — raw/derived split")
            replay[name] = {"http_status":
                            int(st_f.read_text().strip() or 0),
                            "body": parsed}
        for name in exp:
            if derived.get(name) != replay.get(name):
                reasons.append(f"09: {read} derived[{name}] != replay "
                               "from raw — derived is not the full raw "
                               "chain")
        reads[read] = derived
    if len(reads) == 2:
        for name, want_snap in exp.items():
            a = reads["read1"].get(name)
            b = reads["read2"].get(name)
            for read, snap in (("read1", a), ("read2", b)):
                if not isinstance(snap, dict) or "error" in snap:
                    reasons.append(f"09: {name} {read} missing/error")
                    continue
                if snap.get("http_status") != 200:
                    reasons.append(f"09: {name} {read} http_status "
                                   f"{snap.get('http_status')} != 200")
                    continue
                body = snap.get("body")
                if not isinstance(body, dict):
                    reasons.append(f"09: {name} {read} body not a dict")
                    continue
                for f in ("request_count", "inventory_started",
                          "inventory_finished"):
                    v = body.get(f)
                    if not _strict_int(v):
                        reasons.append(
                            f"09: {name} {read} field {f} "
                            f"{v!r} is not a present non-bool int")
                if _strict_int(body.get("request_count")) and \
                        body["request_count"] != \
                        want_snap["request_count"]:
                    reasons.append(f"09: {name} {read} request_count "
                                   f"{body['request_count']} != sealed "
                                   f"{want_snap['request_count']}")
                av = body.get("active")
                if av is False or not _strict_int(av) or av != 0:
                    reasons.append(f"09: {name} {read} active {av!r} is "
                                   "not the exact int 0")
                iv = body.get("inventory_started")
                ivf = body.get("inventory_finished")
                if _strict_int(iv) and _strict_int(ivf) and iv != ivf:
                    reasons.append(f"09: {name} {read} inventory "
                                   "started != finished")
                for f in ("mode", "total"):
                    if body.get(f) != want_snap.get(f):
                        reasons.append(f"09: {name} {read} field {f} "
                                       f"{body.get(f)!r} != sealed "
                                       f"{want_snap.get(f)!r}")
            if isinstance(a, dict) and isinstance(b, dict) and \
                    "error" not in a and "error" not in b:
                if a != b:
                    reasons.append(f"09: {name} read1 != read2 "
                                   "(full-field stability)")
    else:
        reasons.append("09: counter reads incomplete — cannot "
                       "adjudicate")

    vnames = [l.strip() for l in
              (out("10-volumes.names.txt") or "").splitlines() if l.strip()]
    for want_name in cg["volumes_expected_names"]:
        if want_name not in vnames:
            reasons.append(f"10: expected volume {want_name} absent")

    for stem in ("q3-env-mode.txt", "pg-env-mode.txt"):
        v = (out(f"11-env-modes.{stem}") or "").strip()
        if v != "600":
            reasons.append(f"11: {stem} mode {v!r} != 600")

    fx = cg["fixture"]
    st = (out("12-fixture.state.txt") or "").strip()
    want_st = (f"{fx['boot']}|{fx['running']}|{fx['image']}|"
               f"{fx['restart_policy']}")
    if st != want_st:
        reasons.append(f"12: fixture state {st!r} != sealed {want_st!r}")
    p1 = (out("12-fixture.pid1.txt") or "").strip()
    if p1 != f"null|{fx['pid1']}":
        reasons.append(f"12: fixture PID1 {p1!r} != sealed")

    p = attempt / "13-local-shas.derived.json.derived"
    if not p.is_file():
        reasons.append("13: local shas missing")
    else:
        derived = json.loads(p.read_text())
        want_keys = {"p24-frozen-baseline.json", "p25_cohort6.py",
                     "source-p24-baseline.txt"}
        if set(derived) != want_keys:
            reasons.append(f"13: derived keys {sorted(derived)} != "
                           f"exact three {sorted(want_keys)}")
        for f, sha in derived.items():
            fp = HERE / f
            actual = hashlib.sha256(fp.read_bytes()).hexdigest() \
                if fp.is_file() else "MISSING"
            if sha == "MISSING" or actual == "MISSING":
                reasons.append(f"13: {f} MISSING — never MISSING==MISSING")
            elif sha != actual:
                reasons.append(f"13: {f} derived sha {sha} != actual "
                               f"{actual}")
        if derived.get("p25_cohort6.py") != frozen.get(
                "p25_cohort6_sha256"):
            reasons.append("13: cohort6 sha != frozen-baseline binding")
        baseline_sha = hashlib.sha256(
            (HERE / "source-p24-baseline.txt").read_bytes()).hexdigest() \
            if (HERE / "source-p24-baseline.txt").is_file() else None
        if baseline_sha != frozen["source_baseline"]["combined_sha256"]:
            reasons.append("13: source-p24-baseline.txt sha != sealed "
                           "combined 8cd25d42…")
    return (not reasons), reasons


def verify_attempt(attempt: Path, plan: dict, frozen: dict, guard_mod,
                   roster, canonical_plan_sha, c_supplement=None):
    """INTERNAL pure adjudication over injected (possibly toy) plan/
    frozen — the formal main never passes caller data, only seal-
    derived values (tests use this entry for toy injection)."""
    supp_info = None
    if c_supplement is not None:
        ok_s, r_s, supp_info = adjudicate_supplement(
            c_supplement, plan, frozen, attempt)
        # actor parents must equal the sealed C row (roster-sourced)
        c_row = next((r for r in roster.GROUP2
                      if r["id"] == frozen["content_gates"]
                      ["c_live_sealed"]["run_id"]), None)
        if c_row is not None:
            act = (supp_info or {}).get("actor") or {}
            if act.get("sub") != c_row["run_owner_id"] \
                    or act.get("org_id") != c_row["run_org_id"]:
                r_s = list(r_s) + [
                    "supplement actor != sealed C parents "
                    f"(sub={act.get('sub')!r} org={act.get('org_id')!r})"]
                ok_s = False
        if not ok_s:
            return {"all_ok": False, "membership_ok": False,
                    "ledger_ok": False, "content_ok": False,
                    "reasons": [f"supplement: {r}" for r in r_s],
                    "c_supplement_rejected": True}
    ok_m, r_m = adjudicate_membership(attempt, plan, c_supplement)
    ok_l, r_l = adjudicate_ledger(attempt, plan, canonical_plan_sha,
                                  c_supplement)
    ok_c, r_c = adjudicate_content(attempt, plan, guard_mod, roster,
                                   frozen, c_supplement)
    if supp_info is not None:
        r_c = list(r_c)
    return {"all_ok": bool(ok_m and ok_l and ok_c),
            "membership_ok": ok_m, "ledger_ok": ok_l,
            "content_ok": ok_c,
            "reasons": r_m + r_l + r_c}


def main():
    ap = argparse.ArgumentParser(
        description="FORMAL verifier: everything rederived from the "
                    "fixed P24 seal; no caller-provided expectations")
    ap.add_argument("--attempt-dir", required=True)
    ap.add_argument("--c-supplement",
                    help="root-ruled fresh C-only readonly attempt dir "
                         "(c_live_reverify_p25); satisfies the 07 gate "
                         "from fresh GETs; base files untouched")
    ap.add_argument("--out")
    args = ap.parse_args()

    derive = loadmod(HERE / "p25_cohort_derive.py", "p25_derive_v")
    guard = loadmod(HERE / "p25_cohort_guard.py", "p25_guard_v")
    data = derive.derive(Path(derive.SEAL_P24_FINAL_DEFAULT),
                         Path(derive.SEAL_P24_TOOLS_DEFAULT))

    # generated siblings must EXACTLY equal the seal derivation
    sib = json.loads((HERE / "p24-frozen-baseline.json").read_text())
    want_fb = derive.frozen_baseline_dict(
        data, hashlib.sha256(
            (HERE / "p25_cohort6.py").read_bytes()).hexdigest())
    if sib != want_fb:
        print("[p25-verify] REFUSE: sibling p24-frozen-baseline.json "
              "!= seal derivation — regenerate; never hand-edit",
              file=sys.stderr)
        return 2
    roster = guard.load_roster()          # itself seal-checked

    plan = collector_plan(data)           # canonical, seal-derived
    plan_sha = hashlib.sha256(
        json.dumps(plan, indent=1, sort_keys=False).encode()).hexdigest()

    attempt = Path(args.attempt_dir)
    c_supp = Path(args.c_supplement) if getattr(
        args, "c_supplement", None) else None
    if c_supp is not None and not (c_supp / "attempt-summary.json"
                                   ).is_file():
        print(f"[p25-verify] c-supplement invalid: {c_supp}",
              file=sys.stderr)
        return 2
    if not attempt.is_dir():
        verdict = {"all_ok": False, "membership_ok": False,
                   "ledger_ok": False, "content_ok": False,
                   "reasons": [f"attempt dir missing: {attempt}"]}
    else:
        verdict = verify_attempt(attempt, plan,
                                 json.loads(json.dumps(
                                     want_fb)), guard, roster, plan_sha,
                                 c_supplement=c_supp)
    if c_supp is not None and verdict.get("c_supplement_rejected") \
            is not True:
        supp = json.loads((c_supp / "attempt-summary.json").read_text())
        verdict["c_supplement"] = {
            "dir": str(c_supp),
            "summary_sha256": hashlib.sha256(
                (c_supp / "attempt-summary.json").read_bytes()
            ).hexdigest(),
            "binding_sha256": hashlib.sha256(
                (c_supp / "base-binding.json").read_bytes()
            ).hexdigest() if (c_supp / "base-binding.json").is_file()
            else None,
            "sha256_of_artifacts": supp.get("artifacts"),
            "supplements_base_attempt": supp.get(
                "supplements_base_attempt")}
    text = json.dumps(verdict, indent=1, sort_keys=True) + "\n"
    if args.out:
        Path(args.out).write_text(text)
    else:
        print(text, end="")
    return 0 if verdict["all_ok"] else 1


def collector_plan(data):
    """Build the canonical plan exactly as the collector would from the
    SAME seal-derived inputs (fixed default env dir)."""
    coll = loadmod(HERE / "readonly_preflight_p25.py", "p25_coll_v")
    roster_mod = loadmod(HERE / "p25_cohort6.py", "p25_roster_v")
    return coll.canonical_plan_dict(Path(coll.ENV_DIR_DEFAULT),
                                    roster_mod,
                                    {"content_gates": data["CONTENT_GATES"]})


if __name__ == "__main__":
    sys.exit(main())

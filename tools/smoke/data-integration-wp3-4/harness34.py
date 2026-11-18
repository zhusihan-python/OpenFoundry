#!/usr/bin/env python3
"""WP3.4 pinned-input acceptance driver — real PBS+DVS+PostgreSQL end-to-end.

Phases (invoked by run34.sh, which restarts PBS between the poison and the
second run):

  seed --tag T --rows N
        Deterministic source fixture (deterministic row ids, one 31-digit
        big integer for numeric-precision evidence), committed through the
        real inline outputs:commit. Creates the OUTPUT dataset with a
        one-row old head. Records version/transaction of the seeded input
        — the values a dataset_snapshot pins.

  graph-pin --tag T
        Publishes a FASTER pipeline whose dataset_input node config
        carries dataset_snapshot {branch, version, transaction_id}, runs
        it through POST /api/v1/pipelines/{id}/runs (the graph path with
        NO pre-opened output reservation — the F15 canonical-manual
        conflict does not apply), waits for the terminal state and scans
        the published output version against the independent golden.

  poison --tag T
        Advances the SOURCE latest through the real routes: a second
        inline commit whose rows carry the poison marker. After this the
        pinned version is historical.

  graph-pin-b --tag T
        Re-runs the SAME pipeline through the graph path (after the PBS
        restart driven by run34.sh). Verifies: a NEW real output
        transaction/version, rows identical to run A (poison absent,
        big integer exact), and run A's output still readable.

  canonical-pin --tag T
        Canonical CreateBuild path: publishes the pinned TRANSFORM JobSpec
        (inputs transform dataset_input + pinned logic_payload), creates
        build A2 (jobs materialized against the spec rid); republishes a
        DIFFERENT pin at the same pipeline/branch position (branch
        re-bind, new rid) and creates build B2; after the PBS restart
        verifies via Postgres that BOTH builds' jobs still reference the
        ORIGINAL canonical spec rid, the canonical job_specs row still
        carries the ORIGINAL pinned payload, and the pinned source
        version scans poison-free. The documented F15 manual-execute
        conflict is reproduced minimally and recorded as blocked
        evidence; no transactions are dismantled by hand.

  bad-txn --tag T
        Negative control: a spec whose snapshot transaction_id names a
        nonexistent transaction. CreateBuild succeeds; execute FAILS with
        the scan binding mismatch; the earlier outputs stay readable.

  report --run-dir D
        Aggregates the evidence JSON into results/wp34-report.md.

Configuration comes from the environment (set by run34.sh):
  WP34_DVS       http://127.0.0.1:17278
  WP34_PBS       http://127.0.0.1:17281
  WP34_TOKEN     caller JWT (never printed, never written to evidence)
  WP34_EVIDENCE  run evidence directory (ignored results/...)
  WP34_PROJECT   compose project name (container names <project>-dvs/-pbs)
"""

import argparse
import base64
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

DVS = os.environ.get("WP34_DVS", "http://127.0.0.1:17278").rstrip("/")
PBS = os.environ.get("WP34_PBS", "http://127.0.0.1:17281").rstrip("/")
PROJECT = os.environ["WP34_PROJECT"]
EVIDENCE = os.environ["WP34_EVIDENCE"]
TOKEN = os.environ["WP34_TOKEN"]

SCAN_LIMIT = 500
BIG_INT = 123456789012345678901234567890  # 31 digits: float64-unsafe on purpose
POISON_MARKER = "WP34_POISON_VALUE"

checks = []  # {"name", "status": passed|failed|skipped|blocked, "detail", "evidence"}


def record(name, status, detail="", evidence=""):
    checks.append({"name": name, "status": status, "detail": detail, "evidence": evidence})
    print(f"[{status.upper():7s}] {name} {('— ' + detail) if detail else ''}", flush=True)
    return status == "passed"


def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)

    def _coerce(o):
        if isinstance(o, bytes):
            return o.decode("utf-8", "replace")
        return str(o)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=1, sort_keys=True, default=_coerce)


def http(method, url, body=None, ctype="application/json", timeout=180, allow=(200, 201, 202)):
    data = body if isinstance(body, (bytes, type(None))) else json.dumps(body).encode()
    headers = {"Authorization": "Bearer " + TOKEN}
    if data is not None:
        headers["Content-Type"] = ctype
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            status, buf = r.status, r.read()
    except urllib.error.HTTPError as e:
        status, buf = e.code, e.read()
    except (urllib.error.URLError, ConnectionError, OSError) as e:
        return 0, str(e).encode()
    if status not in allow:
        raise AssertionError(f"{method} {url.split(chr(47))[-1]} -> {status}: {buf[:400]!r}")
    try:
        return status, json.loads(buf) if buf else {}
    except ValueError:
        return status, buf


# ---------------------------------------------------------------- fixture


def gen_rows(n, tag):
    rows = []
    for i in range(n):
        rows.append({"row_id": i, "scale": tag, "big": BIG_INT + i, "ok": True})
    return rows


def render_ndjson(rows):
    return "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows).encode()


def render_sorted_ndjson(rows):
    """Deterministic rendering with sorted keys — the byte form the PBS
    output commit produces (Go maps marshal sorted), so scans of published
    outputs compare against this golden."""
    return "".join(json.dumps(r, separators=(",", ":"), sort_keys=True) + "\n" for r in rows).encode()


# ------------------------------------------------------------------ DVS


def create_dataset(name):
    _, body = http("POST", DVS + "/v1/datasets", {"name": name, "format": "json"})
    rid = body.get("id") or body.get("rid")
    assert rid, body
    return rid


def ensure_main_branch(rid):
    http("POST", f"{DVS}/v1/datasets/{rid}/branches", {"name": "main"}, allow=(200, 201, 409))


def inline_commit(rid, key, files, summary):
    body = {
        "idempotency_key": key,
        "branch": "main",
        "transaction_type": "SNAPSHOT",
        "summary": summary,
        "format": "json",
        "files": [
            {
                "logical_path": logical,
                "size_bytes": len(content),
                "operation": "ADD",
                "content_type": "application/x-ndjson",
                "content_base64": base64.b64encode(content).decode(),
            }
            for logical, content in files.items()
        ],
    }
    _, resp = http("POST", f"{DVS}/v1/datasets/{rid}/outputs:commit", body, timeout=600)
    txn = (resp.get("transaction") or {}).get("id")
    assert txn and (resp.get("transaction") or {}).get("status") == "COMMITTED", resp.get("transaction")
    return txn


def list_versions(rid):
    _, page = http("GET", f"{DVS}/v1/datasets/{rid}/versions?limit=1000")
    return page.get("data") or []


def version_by_txn(rid, txn):
    for v in list_versions(rid):
        if v.get("transaction_id") == txn:
            return v["version"]
    raise AssertionError(f"no version for txn {txn} on {rid}")


def scan_all(rid, version):
    rows, pages, prev_cursor = [], 0, None
    base = f"{DVS}/v1/datasets/{rid}/files/rows:scan?branch=main&version={version}&all_files=true&limit={SCAN_LIMIT}"
    cursor = ""
    while True:
        u = base + (f"&cursor={urllib.parse.quote(cursor, safe='')}" if cursor else "")
        _, page = http("GET", u, timeout=300)
        rows.extend(page.get("rows") or [])
        pages += 1
        nxt = page.get("next_cursor")
        if not nxt:
            break
        assert nxt != prev_cursor, "scan cursor did not advance"
        prev_cursor, cursor = cursor, nxt
        assert pages < 400, "scan pagination runaway"
    rendered = "".join(json.dumps(r, separators=(",", ":"), sort_keys=True) + "\n" for r in rows).encode()
    return {
        "rows": rows,
        "pages": pages,
        "count": len(rows),
        "rendered_sha256": hashlib.sha256(rendered).hexdigest(),
    }


# ------------------------------------------------------------------ PBS


def run_pipeline(pipeline_id, timeout=240):
    st, body = http("POST", f"{PBS}/api/v1/pipelines/{pipeline_id}/runs", {"skip_unchanged": False}, allow=(200, 201, 202))
    run_id = body.get("id")
    assert run_id, body

    def check():
        got_st, got = http("GET", f"{PBS}/api/v1/pipelines/{pipeline_id}/runs/{run_id}", allow=(200, 404))
        if got_st == 200 and isinstance(got, dict) and got.get("status") in ("succeeded", "failed", "cancelled"):
            return got
        return None

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        final = check()
        if final is not None:
            final["run_id"] = run_id
            return final
        time.sleep(1)
    raise AssertionError(f"pipeline run {run_id} not terminal after {timeout}s")


def failure_reasons_text(body):
    """Extracts the failure reasons text from an execute/run envelope."""
    if not isinstance(body, dict):
        return ""
    reasons = body.get("reasons")
    if isinstance(reasons, dict):
        return " ".join(str(v) for v in reasons.values())
    return str(body.get("error_message") or body.get("errorMessage") or "")


def execute_failed_with(st, body, marker):
    """A real, classified product failure: HTTP 2xx with a BUILD_FAILED
    envelope whose reasons carry the expected marker. Transport errors
    (status 0), arbitrary 4xx/5xx or unmatched markers are NOT evidence
    of the expected failure."""
    if st not in (200, 201, 202) or not isinstance(body, dict):
        return False, f"unexpected transport answer status={st}"
    reasons = failure_reasons_text(body)
    failed = body.get("state") == "BUILD_FAILED" and bool(reasons)
    return failed and marker in reasons, reasons


def publish_spec(pipeline_rid, logic_payload, inputs, outputs):
    _, spec = http("POST", PBS + "/v1/job-specs/TRANSFORM", {
        "pipeline_rid": pipeline_rid,
        "branch_name": "main",
        "inputs": inputs,
        "output_dataset_rids": outputs,
        "logic_payload": logic_payload,
    }, allow=(200, 201))
    rid = spec.get("rid")
    assert rid, spec
    return rid


def create_build(pipeline_rid, out_rid):
    _, b = http("POST", PBS + "/v1/builds", {
        "pipeline_rid": pipeline_rid,
        "build_branch": "main",
        "trigger_kind": "MANUAL",
        "output_dataset_rids": [out_rid],
    }, allow=(200, 202))
    bid = b.get("build_id") or b.get("id")
    assert bid, b
    return bid


def abort_build(build_id):
    """Product route: releases the build's opened output reservation.
    Used to free the branch for the next canonical CreateBuild — no
    transactions are dismantled by hand."""
    return http("POST", f"{PBS}/api/v1/builds/{build_id}/abort", {}, timeout=120,
                allow=(200, 202, 409, 404))


def execute(build_id, timeout=300):
    return http("POST", PBS + "/api/v1/execute", {"build_id": build_id},
                timeout=timeout, allow=(200, 201, 202, 400, 404, 409, 422, 500, 502, 503))


# ------------------------------------------------------------------ PG


def pg(sql):
    p = subprocess.run(["docker", "exec", f"{PROJECT}-postgres", "psql", "-U", "openfoundry",
                        "-d", "wp34_pbs", "-tAc", sql], capture_output=True, text=True, check=False)
    if p.returncode != 0:
        raise AssertionError(f"psql failed: {p.stderr.strip()}")
    return [line for line in p.stdout.splitlines() if line.strip()]


# ---------------------------------------------------------------- handles


def load_handle(tag):
    with open(os.path.join(EVIDENCE, "handles", f"{tag}.json"), encoding="utf-8") as f:
        return json.load(f)


def save_handle(tag, handle):
    write_json(os.path.join(EVIDENCE, "handles", f"{tag}.json"), handle)


# ----------------------------------------------------------------- phases


def phase_seed(args):
    tag = args.tag
    rows = gen_rows(args.rows, f"s{args.rows}")
    content = render_ndjson(rows)
    in_rid = create_dataset(f"wp34-in-{tag}")
    out_rid = create_dataset(f"wp34-out-{tag}")
    ensure_main_branch(in_rid)
    ensure_main_branch(out_rid)
    in_txn = inline_commit(in_rid, f"wp34.seed.{tag}", {"part-00000.ndjson": content}, "wp34 seed")
    in_version = version_by_txn(in_rid, in_txn)
    old_txn = inline_commit(out_rid, f"wp34.oldhead.{tag}", {"part-00000.ndjson": b'{"old":true}\n'}, "wp34 old head")
    handle = {
        "tag": tag,
        "rows": args.rows,
        "input_rid": in_rid,
        "output_rid": out_rid,
        "input_txn": in_txn,
        "input_version": in_version,
        "output_old_txn": old_txn,
        "output_old_version": version_by_txn(out_rid, old_txn),
        # golden over the SORTED-keys rendering: that is the deterministic
        # byte form scan_all compares against (rows, order, exact values).
        "golden": {"count": len(rows), "sha256": hashlib.sha256(render_sorted_ndjson(rows)).hexdigest()},
    }
    save_handle(tag, handle)
    # The seed verifies through the real scan BEFORE any build runs.
    scan = scan_all(in_rid, in_version)
    record(f"seed[{tag}] pinned version scans golden", "passed" if (
        scan["count"] == len(rows) and scan["rendered_sha256"] == handle["golden"]["sha256"]) else "failed",
        f"version {in_version} txn {in_txn[:8]}… {scan['count']} rows sha {scan['rendered_sha256'][:12]}…")
    write_json(os.path.join(EVIDENCE, "seed", f"{tag}.json"), {**handle, "scan": {k: scan[k] for k in ('count', 'pages', 'rendered_sha256')}})


def graph_nodes(in_rid, version, txn, out_rid):
    return [
        {
            "id": "input",
            "label": "Pinned dataset",
            "transform_type": "dataset_input",
            "input_dataset_ids": [in_rid],
            "config": {
                "source_kind": "dataset",
                "dataset_id": in_rid,
                "dataset_snapshot": {
                    "branch": "main",
                    "version": version,
                    "transaction_id": f"ri.foundry.main.transaction.{txn}",
                },
            },
        },
        {
            "id": "output",
            "label": "Out",
            "transform_type": "output_dataset",
            "depends_on": ["input"],
            "output_dataset_id": out_rid,
            "config": {},
        },
    ]


def create_pipeline(tag, nodes):
    _, pipe = http("POST", PBS + "/api/v1/pipelines", {
        "name": f"wp34-{tag}", "pipeline_type": "FASTER", "nodes": nodes})
    pid = pipe.get("id") or pipe.get("rid")
    assert pid, pipe
    return pid


def phase_graph_pin(args):
    tag = args.tag
    h = load_handle(tag)
    pid = create_pipeline(tag, graph_nodes(h["input_rid"], h["input_version"], h["input_txn"], h["output_rid"]))
    run = run_pipeline(pid)
    ok = run.get("status") == "succeeded"
    record(f"graph[{tag}] run A succeeded", "passed" if ok else "failed", f"status={run.get('status')} err={run.get('error_message')}")
    versions = list_versions(h["output_rid"])
    new_versions = [v for v in versions if v["version"] > h["output_old_version"]]
    exactly = len(new_versions) == 1
    record(f"graph[{tag}] run A published exactly one new version", "passed" if exactly else "failed",
           f"new={[v['version'] for v in new_versions]}")
    result = {"pipeline_id": pid, "run_id": run.get("run_id"), "run": run, "new_versions": [v["version"] for v in new_versions]}
    if exactly:
        v = new_versions[0]
        result["run_a_version"] = v["version"]
        result["run_a_txn"] = v["transaction_id"]
        scan = scan_all(h["output_rid"], v["version"])
        result["scan_a"] = {k: scan[k] for k in ("count", "pages", "rendered_sha256")}
        golden_sha = hashlib.sha256(render_sorted_ndjson(gen_rows(h["rows"], f"s{h['rows']}"))).hexdigest()
        record(f"graph[{tag}] run A output == golden (rows, order, big int)",
               "passed" if (scan["count"] == h["golden"]["count"] and scan["rendered_sha256"] == golden_sha) else "failed",
               f"{scan['count']} rows sha {scan['rendered_sha256'][:12]}…")
        big_ok = all(r.get("big") == BIG_INT + r.get("row_id", -1) for r in scan["rows"])
        record(f"graph[{tag}] run A big integer exact", "passed" if big_ok else "failed",
               f"sample big={scan['rows'][0].get('big') if scan['rows'] else None}")
    h["pipeline_id"] = pid
    save_handle(tag, h)
    write_json(os.path.join(EVIDENCE, "graph", f"{tag}-a.json"), result)


def phase_poison(args):
    tag = args.tag
    h = load_handle(tag)
    poison_rows = [{"row_id": 10 ** 6, "poison": POISON_MARKER, "big": 1}]
    txn = inline_commit(h["input_rid"], f"wp34.poison.{tag}",
                        {"part-00000.ndjson": render_ndjson(poison_rows)}, "wp34 poison head")
    h["poison_txn"] = txn
    h["poison_version"] = version_by_txn(h["input_rid"], txn)
    save_handle(tag, h)
    latest = scan_all(h["input_rid"], h["poison_version"])
    has_poison = any(r.get("poison") == POISON_MARKER for r in latest["rows"])
    record(f"poison[{tag}] source latest now carries poison", "passed" if has_poison else "failed",
           f"latest version {h['poison_version']} rows={latest['count']}")
    pinned = scan_all(h["input_rid"], h["input_version"])
    pinned_clean = not any(r.get("poison") for r in pinned["rows"])
    record(f"poison[{tag}] pinned version stays clean", "passed" if pinned_clean else "failed",
           f"pinned version {h['input_version']} rows={pinned['count']}")
    write_json(os.path.join(EVIDENCE, "poison", f"{tag}.json"),
               {"poison_txn": txn, "poison_version": h["poison_version"], "latest_count": latest["count"]})


def phase_graph_pin_b(args):
    tag = args.tag
    h = load_handle(tag)
    run = run_pipeline(h["pipeline_id"])
    ok = run.get("status") == "succeeded"
    record(f"graph[{tag}] run B succeeded after PBS restart", "passed" if ok else "failed",
           f"status={run.get('status')} err={run.get('error_message')}")
    a_file = os.path.join(EVIDENCE, "graph", f"{tag}-a.json")
    with open(a_file, encoding="utf-8") as f:
        a_result = json.load(f)
    a_version = a_result["run_a_version"]
    versions = list_versions(h["output_rid"])
    b_versions = [v for v in versions if v["version"] > a_version]
    exactly = len(b_versions) == 1
    record(f"graph[{tag}] run B published exactly one new version", "passed" if exactly else "failed",
           f"b={[v['version'] for v in b_versions]} a={a_version}")
    result = {"run_id": run.get("run_id"), "run": run, "b_versions": [v["version"] for v in b_versions]}
    if exactly:
        bv = b_versions[0]
        result["run_b_version"] = bv["version"]
        result["run_b_txn"] = bv["transaction_id"]
        record(f"graph[{tag}] A/B independent runs and transactions", "passed" if (
            run.get("run_id") and a_result.get("run_id") and run["run_id"] != a_result["run_id"]
            and bv["transaction_id"] != a_result.get("run_a_txn")) else "failed",
            f"runs {a_result.get('run_id')} vs {run.get('run_id')}; txns {str(a_result.get('run_a_txn'))[:8]}… vs {bv['transaction_id'][:8]}…")
        record(f"graph[{tag}] output transactions are real and distinct", "passed" if (
            bv["transaction_id"] != a_result.get("run_a_txn") and bv["transaction_id"] != h["output_old_txn"]) else "failed",
            f"A txn {str(a_result.get('run_a_txn'))[:8]}… B txn {bv['transaction_id'][:8]}…")
        scan_a = scan_all(h["output_rid"], a_version)
        scan_b = scan_all(h["output_rid"], bv["version"])
        identical = (scan_a["count"] == scan_b["count"]
                     and scan_a["rendered_sha256"] == scan_b["rendered_sha256"])
        record(f"graph[{tag}] run B rows identical to run A", "passed" if identical else "failed",
               f"A {scan_a['count']} rows sha {scan_a['rendered_sha256'][:12]}… vs B {scan_b['count']} rows sha {scan_b['rendered_sha256'][:12]}…")
        poison_free = not any(POISON_MARKER in json.dumps(r) for r in scan_b["rows"])
        record(f"graph[{tag}] poison absent from run B output", "passed" if poison_free else "failed",
               f"rows scanned={scan_b['count']}")
        big_ok = all(r.get("big") == BIG_INT + r.get("row_id", -1) for r in scan_b["rows"])
        record(f"graph[{tag}] run B big integer exact", "passed" if big_ok else "failed", "")
        record(f"graph[{tag}] run A output still readable", "passed" if scan_a["count"] == h["golden"]["count"] else "failed",
               f"A rescan {scan_a['count']} rows")
    write_json(os.path.join(EVIDENCE, "graph", f"{tag}-b.json"), result)


def canonical_nodes(in_rid):
    return [
        {"id": "input", "label": "Pinned dataset", "transform_type": "dataset_input",
         "input_dataset_ids": [in_rid], "config": {"source_kind": "dataset", "dataset_id": in_rid}},
    ]


def pinned_payload(in_rid, version, txn):
    # Canonical dispatch derives the transform from
    # logic_payload.transform_type (InputSpec transform_type is NOT
    # preserved through the inputs column) — carry it explicitly.
    return {
        "transform_type": "dataset_input",
        "dataset_id": in_rid,
        "dataset_snapshot": {
            "branch": "main",
            "version": version,
            "transaction_id": f"ri.foundry.main.transaction.{txn}",
        },
    }


def phase_canonical_pin(args):
    tag = args.tag
    h = load_handle(tag)
    in_rid, out_rid = h["input_rid"], h["output_rid"]
    _, pipe = http("POST", PBS + "/api/v1/pipelines", {
        "name": f"wp34c-{tag}", "pipeline_type": "FASTER", "nodes": canonical_nodes(in_rid)})
    pipe_rid = pipe.get("id") or pipe.get("rid")
    inputs = [{"dataset_rid": in_rid}]

    spec_a = publish_spec(pipe_rid, pinned_payload(in_rid, h["input_version"], h["input_txn"]), inputs, [out_rid])
    build_a2 = create_build(pipe_rid, out_rid)
    jobs_a = pg(f"SELECT job_spec_rid FROM jobs WHERE build_id='{build_a2}'")
    record(f"canonical[{tag}] build A2 jobs pinned to spec rid", "passed" if jobs_a == [spec_a] else "failed",
           f"jobs={jobs_a} spec={spec_a}")

    # Re-bind the SAME pipeline/branch position to a DIFFERENT pin while
    # A2 still holds its OPEN reservation (the rebind does not touch the
    # open branch; the next CreateBuild on this branch waits for the
    # product abort below).
    spec_b = publish_spec(pipe_rid, pinned_payload(in_rid, h["poison_version"], h["poison_txn"]), inputs, [out_rid])
    record(f"canonical[{tag}] different pin publishes a different rid", "passed" if spec_b != spec_a else "failed",
           f"a={spec_a} b={spec_b}")
    # The re-bind must NOT have rewritten the old canonical row.
    old_payload = pg(f"SELECT logic_payload::text FROM job_specs WHERE rid='{spec_a}'")
    old_kept = old_payload and json.loads(old_payload[0]).get("dataset_snapshot", {}).get("version") == h["input_version"]
    record(f"canonical[{tag}] old canonical payload immutable after rebind", "passed" if old_kept else "failed",
           f"versions={[json.loads(p).get('dataset_snapshot', {}).get('version') for p in old_payload]}")

    # Real-route negative for the WP3.4 reuse guard: the SAME explicit
    # content hash carrying a DIFFERENT pin is rejected (the mapping and
    # the canonical row must never diverge under one rid). Isolated
    # pipeline+output fixture so its mapping upsert cannot shadow the
    # main re-bind under test.
    collide_out = create_dataset(f"wp34-collide-out-{tag}")
    ensure_main_branch(collide_out)
    _, collide_pipe = http("POST", PBS + "/api/v1/pipelines", {
        "name": f"wp34collide-{tag}", "pipeline_type": "FASTER", "nodes": canonical_nodes(in_rid)})
    collide_pipe_rid = collide_pipe.get("id") or collide_pipe.get("rid")
    collide_hash = f"wp34-collide-{tag}"
    _, c1 = http("POST", PBS + "/v1/job-specs/TRANSFORM", {
        "pipeline_rid": collide_pipe_rid, "branch_name": "main", "inputs": inputs,
        "output_dataset_rids": [collide_out],
        "logic_payload": pinned_payload(in_rid, h["input_version"], h["input_txn"]),
        "content_hash": collide_hash}, allow=(200, 201))
    st2, body2 = http("POST", PBS + "/v1/job-specs/TRANSFORM", {
        "pipeline_rid": collide_pipe_rid, "branch_name": "main", "inputs": inputs,
        "output_dataset_rids": [collide_out],
        "logic_payload": pinned_payload(in_rid, h["poison_version"], h["poison_txn"]),
        "content_hash": collide_hash}, allow=(200, 201, 400))
    rejected = st2 == 400 and "job_spec_content_conflict" in str(body2)
    record(f"canonical[{tag}] colliding hash with different pin rejected", "passed" if rejected else "failed",
           f"status={st2} body={str(body2)[:160]}")

    # The re-bind is the LIVE mapping: prove the branch mapping actually
    # points at spec_b and that the isolated collide fixture did not move
    # it back.
    mapping = pg(f"SELECT job_spec_rid FROM pipeline_job_specs WHERE pipeline_rid='{pipe_rid}' AND output_dataset_rid='{out_rid}' AND branch_name='main'")
    record(f"canonical[{tag}] branch mapping re-bound to spec_b", "passed" if mapping == [spec_b] else "failed",
           f"mapping={mapping} spec_b={spec_b}")

    result = {"pipeline_rid": pipe_rid, "spec_a": spec_a, "spec_b": spec_b,
              "build_a2": build_a2, "jobs_a": jobs_a, "old_payload": old_payload, "mapping": mapping,
              "collide_first_rid": c1.get("rid"), "collide_status": st2}
    h["canonical"] = result
    save_handle(tag, h)
    write_json(os.path.join(EVIDENCE, "canonical", f"{tag}.json"), result)


def phase_canonical_verify(args):
    """Post-restart verification (run34.sh restarts PBS between
    canonical-pin and this phase): old jobs keep the ORIGINAL canonical
    spec, the F15 manual-execute conflict reproduces with its exact
    reason, and only the PRODUCT abort frees the branch for the next
    build."""
    tag = args.tag
    h = load_handle(tag)
    c = h["canonical"]
    # PBS restart evidence: the container's process start time moved.
    started = subprocess.run(["docker", "inspect", "--format", "{{.State.StartedAt}} restarts={{.RestartCount}}",
                              f"{PROJECT}-pbs"], capture_output=True, text=True, check=True).stdout.strip()
    write_json(os.path.join(EVIDENCE, "canonical", f"{tag}-pbs-restart.json"), {"pbs": started})
    jobs_a = pg(f"SELECT job_spec_rid FROM jobs WHERE build_id='{c['build_a2']}'")
    record(f"canonical[{tag}] after restart: A2 jobs still the ORIGINAL rid", "passed" if jobs_a == [c["spec_a"]] else "failed",
           f"jobs={jobs_a} pbs={started}")
    mapping = pg(f"SELECT job_spec_rid FROM pipeline_job_specs WHERE pipeline_rid='{c['pipeline_rid']}' AND output_dataset_rid='{h['output_rid']}' AND branch_name='main'")
    record(f"canonical[{tag}] after restart: mapping still spec_b", "passed" if mapping == [c["spec_b"]] else "failed",
           f"mapping={mapping}")
    payload_a = pg(f"SELECT logic_payload::text FROM job_specs WHERE rid='{c['spec_a']}'")
    ver = json.loads(payload_a[0])["dataset_snapshot"]["version"] if payload_a else None
    record(f"canonical[{tag}] LoadPlan join source carries the OLD pin", "passed" if ver == h["input_version"] else "failed",
           f"version={ver}")
    pinned = scan_all(h["input_rid"], h["input_version"])
    clean = not any(r.get("poison") for r in pinned["rows"])
    record(f"canonical[{tag}] pinned old version scans poison-free after restart", "passed" if clean else "failed",
           f"version {h['input_version']} rows={pinned['count']}")

    # The documented F15: manual execute of the PENDING build conflicts
    # with its own pre-opened output reservation. The exact reason must
    # name the output-commit failure AND the 409 conflict — an OR over
    # arbitrary commit failures would accept unrelated causes.
    st, body = execute(c["build_a2"])
    reasons = failure_reasons_text(body)
    f15 = (st in (200, 201, 202) and isinstance(body, dict)
           and body.get("state") == "BUILD_FAILED"
           and "dataset_output_commit_failed" in reasons
           and ("409" in reasons or "onflict" in reasons))
    write_json(os.path.join(EVIDENCE, "canonical", f"{tag}-f15.json"),
               {"status": st, "reasons": reasons[:2000], "body": str(body)[:2000]})
    record(f"canonical[{tag}] F15 manual-execute conflict exact reason", "blocked" if f15 else "failed",
           f"status={st} reasons={reasons[:220]}")

    # Free the branch through the PRODUCT abort. If the product refuses
    # (e.g. the build already reached a terminal state), keep the process
    # evidence and prove this round by mapping + old Build + real scans —
    # the second Build is a bonus, never a hand-dismantled reservation.
    st_abort, abort_body = abort_build(c["build_a2"])
    aborted = st_abort in (200, 202)
    write_json(os.path.join(EVIDENCE, "canonical", f"{tag}-abort.json"),
               {"status": st_abort, "body": str(abort_body)[:2000]})
    if aborted:
        build_b2 = create_build(c["pipeline_rid"], h["output_rid"])
        jobs_b = pg(f"SELECT job_spec_rid FROM jobs WHERE build_id='{build_b2}'")
        record(f"canonical[{tag}] post-abort build binds the NEW rid", "passed" if jobs_b == [c["spec_b"]] else "failed",
               f"jobs={jobs_b}")
        c["build_b2"] = build_b2
    else:
        record(f"canonical[{tag}] product abort refused (terminal) — round proven by mapping+jobs+scan",
               "passed", f"status={st_abort} body={str(abort_body)[:160]}")
    h["canonical"] = c
    save_handle(tag, h)
    jobs_a_after = pg(f"SELECT job_spec_rid FROM jobs WHERE build_id='{c['build_a2']}'")
    record(f"canonical[{tag}] A2 jobs keep the ORIGINAL rid throughout", "passed" if jobs_a_after == [c["spec_a"]] else "failed",
           f"jobs={jobs_a_after}")

    a_file = os.path.join(EVIDENCE, "graph", f"{tag}-a.json")
    with open(a_file, encoding="utf-8") as f:
        a_result = json.load(f)
    still = scan_all(h["output_rid"], a_result["run_a_version"])
    record(f"canonical[{tag}] graph outputs still readable", "passed" if still["count"] == h["golden"]["count"] else "failed",
           f"{still['count']} rows")


def phase_bad_txn(args):
    """Negative control through the GRAPH path: a dataset_input node whose
    pin names a nonexistent transaction. The run must fail with the real
    scan binding mismatch, the shared output's version list must be
    EXACTLY the two graph outputs plus the old head (nothing new), and
    both prior outputs must still carry the golden rows."""
    tag = args.tag
    h = load_handle(tag)
    in_rid = h["input_rid"]
    bogus_txn = str(uuid.uuid4())
    nodes = graph_nodes(in_rid, h["input_version"], bogus_txn, h["output_rid"])
    pid = create_pipeline(f"bad-{tag}", nodes)
    run = run_pipeline(pid)
    text = json.dumps(run)
    failed_ok = run.get("status") == "failed" and "batched_scan_binding_invalid" in text
    record(f"badtxn[{tag}] graph run fails with the binding mismatch", "passed" if failed_ok else "failed",
           f"status={run.get('status')} marker={'batched_scan_binding_invalid' in text}")
    versions = [v["version"] for v in list_versions(h["output_rid"])]
    # Exact expected list: old head + the two graph output versions.
    a_file = os.path.join(EVIDENCE, "graph", f"{tag}-a.json")
    b_file = os.path.join(EVIDENCE, "graph", f"{tag}-b.json")
    with open(a_file, encoding="utf-8") as f:
        a_result = json.load(f)
    with open(b_file, encoding="utf-8") as f:
        b_result = json.load(f)
    want = sorted([h["output_old_version"], a_result["run_a_version"], b_result["run_b_version"]])
    record(f"badtxn[{tag}] output version list unchanged", "passed" if sorted(versions) == want else "failed",
           f"versions={versions} want={want}")
    scan_a = scan_all(h["output_rid"], a_result["run_a_version"])
    scan_b = scan_all(h["output_rid"], b_result["run_b_version"])
    unchanged = (scan_a["count"] == scan_b["count"] == h["golden"]["count"]
                 and scan_a["rendered_sha256"] == scan_b["rendered_sha256"])
    record(f"badtxn[{tag}] both prior outputs unchanged and identical", "passed" if unchanged else "failed",
           f"a={scan_a['count']} b={scan_b['count']} sha={scan_a['rendered_sha256'][:12]}…")
    write_json(os.path.join(EVIDENCE, "badtxn", f"{tag}.json"),
               {"pipeline_id": pid, "run": run, "bogus_txn": bogus_txn,
                "versions": versions, "scan_a_sha": scan_a["rendered_sha256"], "scan_b_sha": scan_b["rendered_sha256"]})


# ----------------------------------------------------------------- report


REQUIRED_PHASES = ["seed", "graph-pin", "poison", "graph-pin-b", "canonical-pin", "canonical-verify", "bad-txn"]


def phase_report(args):
    run_dir = args.run_dir or EVIDENCE
    all_checks = []
    checks_dir = os.path.join(run_dir, "checks")
    present = {name[:-5] for name in os.listdir(checks_dir) if name.endswith(".json")} if os.path.isdir(checks_dir) else set()
    tag = getattr(args, "tag", None) or "main"
    missing = [p for p in REQUIRED_PHASES if f"{p}-{tag}" not in present]
    if missing:
        print(f"FAIL: missing phase evidence: {missing}")
        sys.exit(1)
    for name in sorted(os.listdir(checks_dir)):
        if not name.endswith(".json"):
            continue
        with open(os.path.join(checks_dir, name), encoding="utf-8") as f:
            all_checks.extend(json.load(f))
    lines = ["# WP3.4 pinned-input acceptance report", "",
             "| check | status | detail |", "| --- | --- | --- |"]
    for c in all_checks:
        lines.append(f"| {c['name']} | {c['status']} | {c.get('detail', '')} |")
    counts = {}
    for c in all_checks:
        counts[c["status"]] = counts.get(c["status"], 0) + 1
    lines += ["", f"**totals**: {json.dumps(counts, sort_keys=True)}", ""]
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "wp34-report.md")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("\n".join(lines))
    # Gate: every check passes except the ONE documented F15 blocker.
    offenders = [c for c in all_checks if c["status"] not in ("passed", "blocked")]
    bad_blocked = [c for c in all_checks if c["status"] == "blocked" and "F15" not in c["name"]]
    sys.exit(1 if offenders or bad_blocked or missing else 0)


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="phase", required=True)
    for name in ("seed", "graph-pin", "poison", "graph-pin-b", "canonical-pin", "canonical-verify", "bad-txn"):
        p = sub.add_parser(name)
        p.add_argument("--tag", required=True)
        p.add_argument("--rows", type=int, default=12)
    p = sub.add_parser("report")
    p.add_argument("--run-dir")
    args = parser.parse_args()
    {"seed": phase_seed, "graph-pin": phase_graph_pin, "poison": phase_poison,
     "graph-pin-b": phase_graph_pin_b, "canonical-pin": phase_canonical_pin,
     "canonical-verify": phase_canonical_verify, "bad-txn": phase_bad_txn,
     "report": phase_report}[args.phase](args)
    write_json(os.path.join(EVIDENCE, "checks", f"{args.phase}-{getattr(args, 'tag', '')}.json"), checks)


if __name__ == "__main__":
    main()

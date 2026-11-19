#!/usr/bin/env python3
"""WP3.4 pinned-input acceptance driver — real PBS+DVS+PostgreSQL end-to-end.

Phases (invoked by run34.sh, which restarts PBS before graph-pin-b and
canonical-verify):

  seed --tag T --rows N
        Deterministic source fixture (deterministic row ids, one 31-digit
        big integer for numeric-precision evidence), committed through the
        real inline outputs:commit. Creates the OUTPUT dataset with a
        one-row old head. Records version/transaction of the seeded input
        — the values a dataset_snapshot pins.

  graph-pin --tag T
        Publishes a FASTER pipeline whose dataset_input node config
        carries dataset_snapshot {branch, version, transaction_id}, runs
        it through POST /api/v1/pipelines/{id}/runs, waits for the
        terminal state and scans the published version against the golden.

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
        different pin at the same pipeline/branch position while A2
        holds the output reservation. Checks that a same-hash spec
        with a different pin is rejected.

  canonical-verify --tag T
        After a PBS restart, executes A2 through POST /api/v1/execute.
        Checks that the original pin produces the golden rows, the
        reservation is finalized and replaced by one committed output,
        retry adds no version, and a subsequent CreateBuild selects B2's pin.

  bad-txn --tag T
        Negative control: a graph input's snapshot names a nonexistent
        transaction. The run fails with the scan binding mismatch and
        the output versions and rows are preserved.

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


def get_output_transaction(rid, transaction_rid):
    txn = str(uuid.UUID(transaction_rid.removeprefix("ri.foundry.main.transaction.")))
    _, body = http("GET", f"{DVS}/v1/datasets/{rid}/branches/main/transactions/{txn}")
    assert body.get("id") == txn, body
    return body


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


def execute(build_id, timeout=300):
    return http("POST", PBS + "/api/v1/execute", {"build_id": build_id},
                timeout=timeout, allow=(200, 201, 202, 400, 404, 409, 422, 500, 502, 503))


def execute_completed(status, body, build_id, spec_rid):
    return (status == 200 and isinstance(body, dict)
            and body.get("build_id") == build_id and body.get("state") == "BUILD_COMPLETED"
            and body.get("completed") == 1 and body.get("failed") == body.get("aborted") == 0
            and body.get("nodes") == {spec_rid: "COMPLETED"})


def pbs_start_state():
    return subprocess.run(["docker", "inspect", "--format", "{{.State.StartedAt}} restarts={{.RestartCount}}",
                           f"{PROJECT}-pbs"], capture_output=True, text=True, check=True).stdout.strip()


# ------------------------------------------------------------------ PG


def pg(sql):
    p = subprocess.run(["docker", "exec", f"{PROJECT}-postgres", "psql", "-U", "openfoundry",
                        "-d", "wp34_pbs", "-tAc", sql], capture_output=True, text=True, check=False)
    if p.returncode != 0:
        raise AssertionError(f"psql failed: {p.stderr.strip()}")
    return [line for line in p.stdout.splitlines() if line.strip()]


def build_output_state(build_id):
    build_id = str(uuid.UUID(build_id))
    [row] = pg(f"""
        SELECT jsonb_build_object(
            'build_state', b.state,
            'jobs', COALESCE((SELECT jsonb_agg(jsonb_build_object(
                'id', j.id, 'spec', j.job_spec_rid, 'state', j.state, 'attempt', j.attempt,
                'transactions', j.output_transaction_rids)) FROM jobs j WHERE j.build_id = b.id), '[]'),
            'outputs', COALESCE((SELECT jsonb_agg(jsonb_build_object(
                'job_id', o.job_id, 'dataset', o.output_dataset_rid, 'transaction', o.transaction_rid,
                'committed', o.committed, 'aborted', o.aborted)) FROM job_outputs o
                JOIN jobs j ON j.id = o.job_id WHERE j.build_id = b.id), '[]'),
            'pending', COALESCE((SELECT jsonb_agg(transaction_rid) FROM build_pending_output_txns
                WHERE build_id = b.id), '[]'),
            'locks', COALESCE((SELECT jsonb_agg(transaction_rid) FROM build_input_locks
                WHERE build_id = b.id), '[]'))
        FROM builds b WHERE b.id = '{build_id}'
    """)
    return json.loads(row)


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
    opened = build_output_state(build_a2)
    jobs_a = [job["spec"] for job in opened["jobs"]]
    record(f"canonical[{tag}] build A2 jobs pinned to spec rid", "passed" if jobs_a == [spec_a] else "failed",
           f"jobs={jobs_a} spec={spec_a}")

    # Re-bind the SAME pipeline/branch position to a DIFFERENT pin while
    # A2 still holds its OPEN reservation. A2 keeps its original spec.
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
              "pbs_before_restart": pbs_start_state(), "pending_before_restart": opened["pending"],
              "collide_first_rid": c1.get("rid"), "collide_status": st2}
    h["canonical"] = result
    save_handle(tag, h)
    write_json(os.path.join(EVIDENCE, "canonical", f"{tag}.json"), result)


def phase_canonical_verify(args):
    """Execute the original build and verify the published version after restart."""
    tag = args.tag
    h = load_handle(tag)
    c = h["canonical"]
    build_id, out_rid, spec_a = c["build_a2"], h["output_rid"], c["spec_a"]
    started = pbs_start_state()
    restart = {"before": c["pbs_before_restart"], "after": started}
    write_json(os.path.join(EVIDENCE, "canonical", f"{tag}-pbs-restart.json"), restart)
    record(f"canonical[{tag}] PBS restarted", "passed" if restart["before"] != started else "failed", str(restart))

    before = build_output_state(build_id)
    jobs_a = [job["spec"] for job in before["jobs"]]
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

    reservation_rid = before["pending"][0] if len(before["pending"]) == 1 else None
    reservation = get_output_transaction(out_rid, reservation_rid) if reservation_rid else {}
    reserved = (reservation.get("status") == "OPEN" and reservation.get("tx_type") == "UPDATE"
                and before["pending"] == c["pending_before_restart"] and before["locks"] == [reservation_rid]
                and len(before["outputs"]) == 1 and before["outputs"][0]["transaction"] == reservation_rid
                and before["outputs"][0]["dataset"] == out_rid)
    record(f"canonical[{tag}] A2 holds its exact OPEN output reservation", "passed" if reserved else "failed",
           f"reservation={reservation_rid} status={reservation.get('status')}")

    versions_before = list_versions(out_rid)
    status, body = execute(build_id)
    completed = execute_completed(status, body, build_id, spec_a)
    record(f"canonical[{tag}] F15 canonical execute completes", "passed" if completed else "failed",
           f"status={status} body={str(body)[:700]}")

    after = build_output_state(build_id)
    jobs = after["jobs"]
    outputs = after["outputs"]
    output_rid = outputs[0]["transaction"] if len(outputs) == 1 else None
    persisted = (after["build_state"] == "BUILD_COMPLETED" and len(jobs) == len(outputs) == 1
                 and jobs[0]["spec"] == spec_a and jobs[0]["state"] == "COMPLETED"
                 and outputs[0]["job_id"] == jobs[0]["id"] and outputs[0]["dataset"] == out_rid
                 and outputs[0]["committed"] and not outputs[0]["aborted"]
                 and jobs[0]["transactions"] == [output_rid]
                 and output_rid and output_rid != reservation_rid)
    record(f"canonical[{tag}] PBS stores the completed build, job and output RID", "passed" if persisted else "failed",
           f"build={after['build_state']} jobs={jobs} outputs={outputs}")

    reservation_after = get_output_transaction(out_rid, reservation_rid) if reservation_rid else {}
    released = reservation_after.get("status") == "ABORTED" and not after["pending"] and not after["locks"]
    record(f"canonical[{tag}] reservation aborted and PBS references cleared", "passed" if released else "failed",
           f"reservation={reservation_after.get('status')} pending={after['pending']} locks={after['locks']}")

    versions_after = list_versions(out_rid)
    published = get_output_transaction(out_rid, output_rid) if output_rid else {}
    new_versions = [v for v in versions_after if v not in versions_before]
    exactly = (len(new_versions) == 1 and len(versions_after) == len(versions_before) + 1
               and all(v in versions_after for v in versions_before)
               and published.get("status") == "COMMITTED" and published.get("tx_type") == "SNAPSHOT"
               and published.get("started_by") and published["started_by"] == reservation.get("started_by")
               and new_versions[0]["transaction_id"] == published["id"])
    record(f"canonical[{tag}] DVS publishes exactly one version at the job output RID", "passed" if exactly else "failed",
           f"new={new_versions} output={published.get('id')} status={published.get('status')}")

    a_file = os.path.join(EVIDENCE, "graph", f"{tag}-a.json")
    with open(a_file, encoding="utf-8") as f:
        a_result = json.load(f)
    still = scan_all(out_rid, a_result["run_a_version"])
    historical = (still["count"] == h["golden"]["count"]
                  and still["rendered_sha256"] == a_result["scan_a"]["rendered_sha256"])
    record(f"canonical[{tag}] historical graph output is unchanged", "passed" if historical else "failed",
           f"{still['count']} rows sha={still['rendered_sha256']}")
    published_scan = None
    if exactly:
        c["output_version"] = new_versions[0]["version"]
        scan = scan_all(out_rid, c["output_version"])
        published_scan = {k: scan[k] for k in ("count", "pages", "rendered_sha256")}
    golden = (published_scan is not None and published_scan["count"] == h["golden"]["count"]
              and published_scan["rendered_sha256"] == h["golden"]["sha256"] == still["rendered_sha256"])
    record(f"canonical[{tag}] published rows match the pinned graph and golden", "passed" if golden else "failed",
           f"scan={published_scan} golden={h['golden']}")

    result = {"status": status, "body": body, "reasons": failure_reasons_text(body),
              "pbs_before": before, "pbs_after": after, "reservation_before": reservation,
              "reservation_after": reservation_after, "published": published,
              "versions_before": versions_before, "versions_after": versions_after, "published_scan": published_scan}
    repeat_ok, next_build_ok = False, False
    repeat_detail = next_build_detail = "A2 publication did not pass its required checks"
    if completed and persisted and released and exactly:
        retry_status, retry_body = execute(build_id)
        retry_state, retry_versions = build_output_state(build_id), list_versions(out_rid)
        repeat_ok = (execute_completed(retry_status, retry_body, build_id, spec_a)
                     and retry_state == after and retry_versions == versions_after)
        repeat_detail = f"status={retry_status} versions={[v['version'] for v in retry_versions]}"
        result["retry"] = {"status": retry_status, "body": retry_body, "pbs": retry_state, "versions": retry_versions}

        try:
            build_b2 = create_build(c["pipeline_rid"], out_rid)
        except AssertionError as e:
            next_build_detail = str(e)
        else:
            after_b = build_output_state(build_b2)
            jobs_b = [job["spec"] for job in after_b["jobs"]]
            next_build_ok = build_b2 != build_id and jobs_b == [c["spec_b"]]
            next_build_detail = f"build={build_b2} jobs={jobs_b}"
            c["build_b2"] = build_b2
            result["next_build"] = after_b
    record(f"canonical[{tag}] repeating execute keeps the same output and versions",
           "passed" if repeat_ok else "failed", repeat_detail)
    record(f"canonical[{tag}] next build binds the new spec after A2 publication",
           "passed" if next_build_ok else "failed", next_build_detail)
    write_json(os.path.join(EVIDENCE, "canonical", f"{tag}-f15.json"), result)
    h["canonical"] = c
    save_handle(tag, h)
    jobs_a_after = pg(f"SELECT job_spec_rid FROM jobs WHERE build_id='{build_id}'")
    record(f"canonical[{tag}] A2 jobs keep the ORIGINAL rid throughout", "passed" if jobs_a_after == [c["spec_a"]] else "failed",
           f"jobs={jobs_a_after}")


def phase_bad_txn(args):
    """Negative control through the GRAPH path: a dataset_input node whose
    pin names a nonexistent transaction. The run must fail with the real
    scan binding mismatch and leave existing output versions and rows intact."""
    tag = args.tag
    h = load_handle(tag)
    in_rid = h["input_rid"]
    bogus_txn = str(uuid.uuid4())
    nodes = graph_nodes(in_rid, h["input_version"], bogus_txn, h["output_rid"])
    pid = create_pipeline(f"bad-{tag}", nodes)
    versions_before = list_versions(h["output_rid"])
    run = run_pipeline(pid)
    text = json.dumps(run)
    failed_ok = run.get("status") == "failed" and "batched_scan_binding_invalid" in text
    record(f"badtxn[{tag}] graph run fails with the binding mismatch", "passed" if failed_ok else "failed",
           f"status={run.get('status')} marker={'batched_scan_binding_invalid' in text}")
    versions_after = list_versions(h["output_rid"])
    record(f"badtxn[{tag}] output version list unchanged", "passed" if versions_after == versions_before else "failed",
           f"before={[v['version'] for v in versions_before]} after={[v['version'] for v in versions_after]}")
    a_file = os.path.join(EVIDENCE, "graph", f"{tag}-a.json")
    b_file = os.path.join(EVIDENCE, "graph", f"{tag}-b.json")
    with open(a_file, encoding="utf-8") as f:
        a_result = json.load(f)
    with open(b_file, encoding="utf-8") as f:
        b_result = json.load(f)
    expected_versions = {"graph_a": a_result["run_a_version"], "graph_b": b_result["run_b_version"]}
    if "output_version" in h["canonical"]:
        expected_versions["canonical"] = h["canonical"]["output_version"]
    scans = {}
    for label, version in expected_versions.items():
        scan = scan_all(h["output_rid"], version)
        scans[label] = {"version": version, "count": scan["count"], "sha256": scan["rendered_sha256"]}
    unchanged = all({"count": scan["count"], "sha256": scan["sha256"]} == h["golden"] for scan in scans.values())
    record(f"badtxn[{tag}] published pinned outputs still match the golden", "passed" if unchanged else "failed",
           f"scans={scans}")
    write_json(os.path.join(EVIDENCE, "badtxn", f"{tag}.json"),
               {"pipeline_id": pid, "run": run, "bogus_txn": bogus_txn,
                "versions_before": versions_before, "versions_after": versions_after, "scans": scans})


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
    sys.exit(1 if any(c["status"] != "passed" for c in all_checks) else 0)


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

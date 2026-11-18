#!/usr/bin/env python3
"""WP3.3 3C smoke driver — real PBS+DVS+PostgreSQL end-to-end.

Phases (invoked by run3c.sh, which restarts service containers between the
memory windows):

  seed --rows N --tag T [--with-bad-predicate-spec]
        Deterministic fixture: N rows x exactly 4096 bytes (newline incl.),
        bucket = i mod 3, two files part-00000/part-00001. Seeds the INPUT
        dataset through one keyed inline outputs:commit (content_base64,
        ADD, SNAPSHOT) and initializes the OUTPUT dataset with a one-row
        old head. Verifies the seed through the real rows:scan (all_files)
        against the independently generated golden BEFORE any build runs.

  memory-run --tag T --rows N
        Publishes the canonical TRANSFORM JobSpec (3B batched grammar,
        explicit pinned input version + branch + all_files + predicate),
        creates the build, executes it through POST /api/v1/execute
        {build_id}. Samples /proc/1/{status,stat} of the real PBS/DVS
        processes around the window (the orchestrator restarted both right
        after seeding so the seed-phase memory is excluded).

  fault-stage --tag T
        Real staging-phase failure: a predicate whose shape passes the 3B
        CreateBuild grammar gate (length-bounded) but that the DVS scan
        parser rejects (unsupported op) — deterministic, zero mocks. The
        output dataset must keep its old head only and PBS must clean the
        build's refs.

  fault-ack --tag T
        Publish-ACK loss through the 3C fault proxy: the proxy reads DVS's
        FULL COMMITTED response, records the exact request-body digest,
        then hard-resets the PBS connection. Driver verifies external DVS
        state, restarts the PBS container, replays as the same actor and
        asserts byte-identical replay, exactly one new version, bound
        job_outputs and cleaned refs.

  report --run-dir D
        Aggregates the evidence JSON files into results/increment-3c-report.md
        with mechanical threshold evaluation and a secret-hygiene scan.

Configuration comes from the environment (set by run3c.sh):
  WP33C_ENV_DIR   rendered compose + .env dir
  WP33C_PROJECT   compose project name (container names <project>-dvs/-pbs)
  WP33C_DVS       http://127.0.0.1:17178
  WP33C_PBS       http://127.0.0.1:17181
  WP33C_TOKEN     caller JWT (never printed, never written to evidence)
  WP33C_EVIDENCE  run evidence directory (ignored results/...)
  WP33C_PROXY_CTL http://127.0.0.1:17179 (fault phase only)
"""

import argparse
import base64
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
DVS = os.environ.get("WP33C_DVS", "http://127.0.0.1:17178").rstrip("/")
PBS = os.environ.get("WP33C_PBS", "http://127.0.0.1:17181").rstrip("/")
PROXY_CTL = os.environ.get("WP33C_PROXY_CTL", "http://127.0.0.1:17179").rstrip("/")
PROJECT = os.environ["WP33C_PROJECT"]
EVIDENCE = os.environ["WP33C_EVIDENCE"]
TOKEN = os.environ["WP33C_TOKEN"]

ROW_BYTES = 4096  # exact per-line size including the trailing newline
SCAN_LIMIT = 500
PRED_JSON = '{"column":"bucket","op":"eq","value":0}'
PRED_BAD = '{"column":"bucket","op":"ne","value":0}'  # unsupported op: passes 3B's length gate, DVS scan rejects
THRESH_LARGE_PEAK = 192 * 1024 * 1024  # per service, 15k ending VmHWM <= 192 MiB
THRESH_DELTA = 24 * 1024 * 1024  # per service, peak(15k) - peak(1.5k) <= 24 MiB

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


def http(method, url, body=None, ctype="application/json", timeout=180, allow=(200, 201)):
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


def blob(b64_or_none):
    return base64.b64decode(b64_or_none) if b64_or_none else b""


# ---------------------------------------------------------------- fixture


def gen_row(i, scale_tag):
    """Deterministic row: exactly ROW_BYTES bytes incl. newline; bucket i%3."""
    head = '{"row_id":%d,"bucket":%d,"scale":"%s","payload":"' % (i, i % 3, scale_tag)
    tail = '"}'
    fill = ROW_BYTES - len(head) - len(tail) - 1  # -1 for the newline
    line = (head + "p" * fill + tail + "\n").encode()
    assert len(line) == ROW_BYTES, (len(line), ROW_BYTES)
    return line, i % 3


def build_fixture(rows, tag):
    """Two files, stable logical-path order; golden = bucket==0 rows."""
    scale_tag = f"s{rows}"
    lines = []
    half = rows // 2
    files = {"part-00000.ndjson": [], "part-00001.ndjson": []}
    for i in range(rows):
        line, bucket = gen_row(i, scale_tag)
        lines.append((line, bucket))
        files["part-00000.ndjson" if i < half else "part-00001.ndjson"].append(line)
    golden_lines = [line for line, bucket in lines if bucket == 0]
    golden_ndjson = b"".join(golden_lines)
    full_ndjson = b"".join(l for fn in sorted(files) for l in files[fn])
    return {
        "rows": rows,
        "scale_tag": scale_tag,
        "files": files,
        "golden": {
            "count": len(golden_lines),
            "size_bytes": len(golden_ndjson),
            "sha256": hashlib.sha256(golden_ndjson).hexdigest(),
        },
        "total_size": sum(len(l) for l, _ in lines),
        "full_sha256": hashlib.sha256(full_ndjson).hexdigest(),
    }


# ------------------------------------------------------------------ DVS


def create_dataset(name):
    st, body = http("POST", DVS + "/v1/datasets", {"name": name, "format": "json"})
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
            for logical, content in files
        ],
    }
    st, resp = http("POST", f"{DVS}/v1/datasets/{rid}/outputs:commit", body, timeout=600)
    txn = (resp.get("transaction") or {}).get("id")
    assert txn and (resp.get("transaction") or {}).get("status") == "COMMITTED", resp.get("transaction")
    return resp


def list_versions(rid):
    _, page = http("GET", f"{DVS}/v1/datasets/{rid}/versions?limit=1000")
    return page.get("data") or []


def version_by_txn(rid, txn):
    for v in list_versions(rid):
        if v.get("transaction_id") == txn:
            return v["version"]
    raise AssertionError(f"no version for txn {txn} on {rid}")


def scan_all(rid, version, predicate=None):
    """Bounded rows:scan pagination over all_files; returns raw row dicts,
    page stats and a digest over the compact re-serialization of the rows
    (deterministic because the fixture rows are compact fixed-key-order
    JSON)."""
    rows, pages, prev_cursor = [], 0, None
    url = f"{DVS}/v1/datasets/{rid}/files/rows:scan?branch=main&version={version}&all_files=true&limit={SCAN_LIMIT}"
    if predicate is not None:
        url += "&predicate=" + urllib.parse.quote(predicate, safe="")
    cursor = ""
    while True:
        u = url + (f"&cursor={urllib.parse.quote(cursor, safe='')}" if cursor else "")
        st, page = http("GET", u, timeout=300)
        assert st == 200, (st, page)
        got = page.get("rows") or []
        rows.extend(got)
        pages += 1
        nxt = page.get("next_cursor")
        if not nxt:
            break
        assert nxt != prev_cursor, "scan cursor did not advance"
        prev_cursor, cursor = cursor, nxt
        assert pages < 400, "scan pagination runaway"
    rendered = "".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows).encode()
    return {
        "rows": rows,
        "pages": pages,
        "count": len(rows),
        "rendered_sha256": hashlib.sha256(rendered).hexdigest(),
        "rendered_size": len(rendered),
    }


def receipt_get(rid, key):
    st, body = http("GET", f"{DVS}/v1/datasets/{rid}/outputs/commits/{key}", allow=(200, 404))
    return st, body


def receipt_raw_sha(rid, key):
    """SHA256 over the receipt route's EXACT response bytes (the stored
    immutable response body plus envelope)."""
    req = urllib.request.Request(
        f"{DVS}/v1/datasets/{rid}/outputs/commits/{key}",
        headers={"Authorization": "Bearer " + TOKEN})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            buf = r.read()
            return hashlib.sha256(buf).hexdigest(), None
    except urllib.error.HTTPError as e:
        return "", e.code
    except (urllib.error.URLError, OSError) as e:
        return "", str(e)


# ------------------------------------------------------------------ PBS


def rid_tail(rid):
    """Bare UUID tail of any ri.foundry.main.* RID spelling."""
    return rid.rsplit(".", 1)[-1]


def canon(rid):
    """Canonical dataset RID spelling (ri.foundry.main.dataset.<uuid>).

    INTERIM WORKAROUND, clearly labeled: 3B's reservation matcher
    (repository.go persistResolvedBuild) string-compares the resolver's
    spec-spelling OpenedTransactions[].DatasetRID against the canonicalized
    BatchConfig.OutputDatasetRID. Publishing canonical RIDs on both the
    JobSpec and CreateBuild makes the two sides agree so the rest of the
    chain (memory, faults) can be measured while 3B owns the fix. The
    bare-UUID spelling (P1 precedent, canonical_seed.py) remains a legal
    client form and its failure is recorded as a 3B defect in the report.
    """
    if rid and not rid.startswith("ri.foundry.main.dataset."):
        return "ri.foundry.main.dataset." + rid
    return rid


def three_b_present():
    return os.path.exists(
        os.path.join(REPO, "services/pipeline-build-service/internal/handler/batched_build.go")
    )


def create_pipeline(tag, in_rid, out_rid):
    nodes = [
        {"id": "input", "label": "Dataset", "transform_type": "dataset_input",
         "input_dataset_ids": [in_rid], "config": {"source_kind": "dataset", "dataset_id": in_rid}},
        {"id": "output", "label": "Out", "transform_type": "output_dataset",
         "depends_on": ["input"], "output_dataset_id": out_rid, "config": {}},
    ]
    _, pipe = http("POST", PBS + "/api/v1/pipelines",
                   {"name": f"wp33c-{tag}", "pipeline_type": "FASTER", "nodes": nodes})
    pid = pipe.get("id") or pipe.get("rid")
    assert pid, pipe
    return pid


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
        "output_dataset_rids": [canon(out_rid)],
    }, allow=(200, 202))
    bid = b.get("build_id") or b.get("id")
    assert bid, b
    return bid, b


def execute(build_id, timeout=300):
    return http("POST", PBS + "/api/v1/execute", {"build_id": build_id},
                timeout=timeout, allow=(200, 201, 202, 400, 404, 409, 422, 500, 502, 503))


# ------------------------------------------------------------------ proc


def docker(*args, check=True):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=check)


def proc_snapshot(service):
    """VmHWM/VmRSS (kB) and starttime (ticks) of the service process, read
    from Linux /proc inside the container — never docker stats."""
    ctr = f"{PROJECT}-{service}"
    st = docker("exec", ctr, "cat", "/proc/1/status", check=False)
    stat = docker("exec", ctr, "cat", "/proc/1/stat", check=False)
    if st.returncode != 0 or stat.returncode != 0:
        return {"error": (st.stderr or stat.stderr).strip()}
    snap = {}
    for line in st.stdout.splitlines():
        if line.startswith("VmHWM:"):
            snap["vmhwm_kib"] = int(line.split()[1])
        elif line.startswith("VmRSS:"):
            snap["vmrss_kib"] = int(line.split()[1])
    tail = stat.stdout.rsplit(")", 1)[-1].split()
    snap["starttime_ticks"] = int(tail[19])  # field 22 overall
    snap["pid"] = stat.stdout.split("(", 1)[0].strip()
    return snap


class Sampler(threading.Thread):
    def __init__(self, services, interval=0.25):
        super().__init__(daemon=True)
        self.services = services
        self.interval = interval
        self.samples = []
        self.stop_evt = threading.Event()

    def run(self):
        t0 = time.monotonic()
        while not self.stop_evt.is_set():
            row = {"t": round(time.monotonic() - t0, 3)}
            for svc in self.services:
                try:
                    row[svc] = proc_snapshot(svc).get("vmrss_kib")
                except Exception:
                    row[svc] = None
            self.samples.append(row)
            self.stop_evt.wait(self.interval)

    def stop(self):
        self.stop_evt.set()
        self.join(timeout=10)


# ------------------------------------------------------------------ PG


def pg(db, sql):
    p = docker("exec", f"{PROJECT}-postgres", "psql", "-U", "openfoundry",
               "-d", db, "-tAc", sql, check=False)
    if p.returncode != 0:
        raise AssertionError(f"psql failed: {p.stderr.strip()}")
    return [line for line in p.stdout.splitlines() if line.strip()]


def build_state(build_id):
    return pg("wp33c_pbs", f"SELECT state FROM builds WHERE id='{build_id}'")


def build_refs(build_id):
    locks = pg("wp33c_pbs", f"SELECT count(*) FROM build_input_locks WHERE build_id='{build_id}'")
    pending = pg("wp33c_pbs", f"SELECT count(*) FROM build_pending_output_txns WHERE build_id='{build_id}'")
    return int(locks[0]), int(pending[0])


def job_outputs_of(build_id):
    rows = pg("wp33c_pbs",
              "SELECT jo.transaction_rid, jo.committed FROM job_outputs jo "
              f"JOIN jobs j ON j.id = jo.job_id WHERE j.build_id='{build_id}'")
    return [tuple(r.split("|")) for r in rows]


# ------------------------------------------------------------------ phases


def load_handle(tag):
    with open(os.path.join(EVIDENCE, "handles", f"{tag}.json"), encoding="utf-8") as f:
        return json.load(f)


def save_handle(tag, handle):
    write_json(os.path.join(EVIDENCE, "handles", f"{tag}.json"), handle)


def phase_seed(args):
    fx = build_fixture(args.rows, f"s{args.rows}")
    tag = args.tag
    record(f"seed[{tag}] fixture determinism",
           "passed" if fx["golden"]["count"] == args.rows // 3 else "failed",
           f"rows={args.rows} two files, golden {fx['golden']['count']} rows "
           f"{fx['golden']['size_bytes']}B sha {fx['golden']['sha256'][:12]}…")

    in_rid = create_dataset(f"wp33c-in-{tag}")
    ensure_main_branch(in_rid)
    seed_files = [(logical, b"".join(lines)) for logical, lines in sorted(fx["files"].items())]
    resp = inline_commit(in_rid, f"wp33c-seed-in-{tag}",
                         seed_files, f"wp33c 3C seed {tag}")
    txn = resp["transaction"]["id"]
    versions = list_versions(in_rid)
    assert len(versions) == 1, f"expected exactly 1 input version, got {len(versions)}"
    in_version = versions[0]["version"]
    assert versions[0]["transaction_id"] == txn

    out_rid = create_dataset(f"wp33c-out-{tag}")
    ensure_main_branch(out_rid)
    old_head_line, _ = gen_row(0, f"oldhead-{tag}")
    oresp = inline_commit(out_rid, f"wp33c-seed-out-{tag}",
                          [("old-head.ndjson", old_head_line)], f"wp33c 3C old head {tag}")
    otxn = oresp["transaction"]["id"]
    over = list_versions(out_rid)
    assert len(over) == 1 and over[0]["version"] == 1, over
    out_head_txn = otxn

    # Independent verification of the seeded input through the real scan
    # BEFORE any build: byte-exact golden comparison (order + count + digest).
    scan = scan_all(in_rid, in_version)
    expected_render = b"".join(l for l in
                               [line for fn in sorted(fx["files"]) for line in fx["files"][fn]])
    ok = (scan["count"] == args.rows
          and scan["rendered_size"] == len(expected_render)
          and scan["rendered_sha256"] == hashlib.sha256(expected_render).hexdigest())
    record(f"seed[{tag}] input scan == generator golden", "passed" if ok else "failed",
           f"scan rows={scan['count']} pages={scan['pages']} "
           f"sha {scan['rendered_sha256'][:12]}… vs golden "
           f"{hashlib.sha256(expected_render).hexdigest()[:12]}…")

    save_handle(tag, {
        "tag": tag, "rows": args.rows,
        "input_rid": in_rid, "input_version": in_version, "input_txn": txn,
        "output_rid": out_rid, "out_head_version": 1, "out_head_txn": out_head_txn,
        "golden": fx["golden"], "total_size": fx["total_size"],
        "full_sha256": fx["full_sha256"],
        "seed_scan": {k: scan[k] for k in ("count", "pages", "rendered_sha256", "rendered_size")},
    })


def container_identity(service):
    """pid1 exe + restart/OOM facts — the /proc numbers below are only
    meaningful if pid1 IS the service binary and the container never
    restarted (a restart would silently reset VmHWM)."""
    out = {}
    for name, arg in (("pid1_exe", ("exec", f"{PROJECT}-{service}", "readlink", "/proc/1/exe")),
                      ("restarts", ("inspect", f"{PROJECT}-{service}", "--format", "{{.RestartCount}}")),
                      ("oom_killed", ("inspect", f"{PROJECT}-{service}", "--format", "{{.State.OOMKilled}}"))):
        r = docker(*arg, check=False)
        out[name] = r.stdout.strip() if r.returncode == 0 else f"err:{r.returncode}"
    return out


def phase_memory_run(args):
    tag = args.tag
    mode = getattr(args, "mode", "filtered")  # filtered: 1/3 predicate; full: no predicate
    h = load_handle(tag)
    blocked = not three_b_present()
    if blocked:
        record(f"e2e[{tag}] canonical build chain", "blocked",
               "3B PBS batched files absent (batched_build.go missing)")
        write_json(os.path.join(EVIDENCE, "memory", f"{tag}.json"),
                   {"tag": tag, "blocked": True})
        return

    ident = {svc: container_identity(svc) for svc in ("dvs", "pbs")}
    ident_ok = all(v["pid1_exe"] == "/service" and v["restarts"] == "0"
                   and v["oom_killed"] == "false" for v in ident.values())
    record(f"e2e[{tag}] pid1 is /service, zero restarts, no OOM",
           "passed" if ident_ok else "failed",
           " ".join(f"{svc}:exe={v['pid1_exe']} restarts={v['restarts']} oom={v['oom_killed']}"
                    for svc, v in ident.items()))

    start = {"dvs": proc_snapshot("dvs"), "pbs": proc_snapshot("pbs")}
    assert "vmhwm_kib" in start["dvs"] and "vmhwm_kib" in start["pbs"], start
    sampler = Sampler(["dvs", "pbs"])
    sampler.start()
    t_exec0 = time.time()
    pipe = create_pipeline(tag, h["input_rid"], h["output_rid"])
    pin = {"version": h["input_version"], "branch": "main", "all_files": True}
    if mode == "filtered":
        pin["predicate"] = PRED_JSON
    spec = publish_spec(pipe, {
        "transform_type": "batched_dataset_copy",
        "input": pin,
        "output": {"logical_path": "part-00000.ndjson"},
    }, [{"dataset_rid": canon(h["input_rid"])}], [canon(h["output_rid"])])
    build_id, create_resp = create_build(pipe, h["output_rid"])
    est, eresp = execute(build_id)
    exec_seconds = round(time.time() - t_exec0, 2)
    sampler.stop()
    end = {"dvs": proc_snapshot("dvs"), "pbs": proc_snapshot("pbs")}

    if mode == "full":
        h = dict(h)
        h["golden"] = {"count": h["rows"], "size_bytes": h["total_size"],
                       "sha256": h["full_sha256"]}
    result = {
        "tag": tag, "mode": mode, "rows": h["rows"], "build_id": build_id, "spec_rid": spec,
        "pipeline_rid": pipe, "create_response": create_resp,
        "execute_status": est, "execute_response": eresp,
        "exec_seconds": exec_seconds,
        "proc": {"start": start, "end": end},
        "rss_samples": len(sampler.samples),
        "rss_peak_kib": {
            "dvs": max((s.get("dvs") or 0) for s in sampler.samples) if sampler.samples else None,
            "pbs": max((s.get("pbs") or 0) for s in sampler.samples) if sampler.samples else None,
        },
    }

    # Fresh-process sanity: both services restarted between seed and here.
    fresh = start["dvs"].get("starttime_ticks") and end["dvs"].get("starttime_ticks") and \
        start["dvs"]["starttime_ticks"] == end["dvs"]["starttime_ticks"] and \
        start["pbs"]["starttime_ticks"] == end["pbs"]["starttime_ticks"]
    record(f"e2e[{tag}] single process window", "passed" if fresh else "failed",
           f"dvs starttime {start['dvs'].get('starttime_ticks')}→{end['dvs'].get('starttime_ticks')}, "
           f"pbs {start['pbs'].get('starttime_ticks')}→{end['pbs'].get('starttime_ticks')}")

    # C4: the spec pinned the seed's explicit version.
    record(f"e2e[{tag}] explicit input version pinned", "passed",
           f"JobSpec input.version={h['input_version']} (seed txn {h['input_txn'][:8]}…)",
           evidence=os.path.join("handles", f"{tag}.json"))

    # Execute must have succeeded (any non-200 here fails the scale).
    ok_exec = est == 200
    record(f"e2e[{tag}] execute 200", "passed" if ok_exec else "failed",
           f"status={est} body={json.dumps(eresp)[:300] if isinstance(eresp, dict) else eresp!r}")
    result["execute_ok"] = ok_exec

    # Exactly one new version on the output dataset; head is the new txn.
    versions = list_versions(h["output_rid"])
    new_versions = [v for v in versions if v["version"] > h["out_head_version"]]
    exactly_one = len(versions) == h["out_head_version"] + 1 and len(new_versions) == 1
    new_v = new_versions[0] if new_versions else {}
    result["output_versions"] = versions
    record(f"e2e[{tag}] exactly one new version", "passed" if exactly_one else "failed",
           f"versions={[v['version'] for v in versions]} head_txn={new_v.get('transaction_id', '—')[:8]}…")

    # job_outputs binds the real published transaction, committed.
    jo = job_outputs_of(build_id)
    committed = jo[0][1].lower() in ("t", "true") if jo else False
    bound = (len(jo) == 1 and committed
             and rid_tail(jo[0][0]) == str(new_v.get("transaction_id")))
    record(f"e2e[{tag}] job_outputs bound to published txn", "passed" if bound else "failed",
           f"job_outputs={jo} head_txn={new_v.get('transaction_id')}")
    bstate = build_state(build_id)
    record(f"e2e[{tag}] build terminal state", "passed" if bstate == ["BUILD_COMPLETED"] else "failed",
           f"builds.state={bstate}")
    locks, pending = build_refs(build_id)
    record(f"e2e[{tag}] PBS refs cleaned", "passed" if (locks, pending) == (0, 0) else "failed",
           f"build_input_locks={locks} build_pending_output_txns={pending}")

    # Output scan vs independent golden: count, order, digest.
    if new_v:
        scan = scan_all(h["output_rid"], new_v["version"])
        g = h["golden"]
        golden_ok = (scan["count"] == g["count"]
                     and scan["rendered_sha256"] == g["sha256"]
                     and scan["rendered_size"] == g["size_bytes"])
        result["output_scan"] = {k: scan[k] for k in ("count", "pages", "rendered_sha256", "rendered_size")}
        record(f"e2e[{tag}] output == independent golden", "passed" if golden_ok else "failed",
               f"scan {scan['count']} rows/{scan['rendered_size']}B pages={scan['pages']} "
               f"sha {scan['rendered_sha256'][:12]}… vs golden {g['count']} rows/{g['size_bytes']}B "
               f"sha {g['sha256'][:12]}…")
    write_json(os.path.join(EVIDENCE, "memory", f"{tag}.json"), result)


def phase_fault_stage(args):
    tag = args.tag
    h = load_handle(tag)
    if not three_b_present():
        record("fault[C5] staging failure", "blocked", "3B PBS batched files absent")
        return
    pipe = create_pipeline(tag, h["input_rid"], h["output_rid"])
    publish_spec(pipe, {
        "transform_type": "batched_dataset_copy",
        "input": {"version": h["input_version"], "branch": "main",
                  "all_files": True, "predicate": PRED_BAD},
        "output": {"logical_path": "part-00000.ndjson"},
    }, [{"dataset_rid": canon(h["input_rid"])}], [canon(h["output_rid"])])
    build_id, _ = create_build(pipe, h["output_rid"])
    before_versions = list_versions(h["output_rid"])
    est, eresp = execute(build_id)
    time.sleep(1.0)
    after_versions = list_versions(h["output_rid"])
    bstate = build_state(build_id)
    locks, pending = build_refs(build_id)

    unchanged = (before_versions == after_versions
                 and len(after_versions) == h["out_head_version"]
                 and after_versions[0]["transaction_id"] == h["out_head_txn"])
    record("fault[C5] staging failure keeps old head only",
           "passed" if unchanged else "failed",
           f"versions before={[v['version'] for v in before_versions]} after={[v['version'] for v in after_versions]}")
    record("fault[C5] build FAILED",
           "passed" if bstate == ["BUILD_FAILED"] else "failed",
           f"builds.state={bstate} execute_status={est}")
    record("fault[C5] PBS refs cleaned",
           "passed" if (locks, pending) == (0, 0) else "failed",
           f"build_input_locks={locks} build_pending_output_txns={pending}")
    write_json(os.path.join(EVIDENCE, "faults", f"{tag}-stage.json"), {
        "build_id": build_id, "execute_status": est,
        "execute_response": eresp, "build_state": bstate,
        "versions_before": before_versions, "versions_after": after_versions,
        "locks": locks, "pending": pending,
    })


def proxy_events():
    _, body = http("GET", PROXY_CTL + "/events")
    return body.get("events") or []


def phase_fault_truncate(args):
    """C5b (review): a REAL mid-stage failure with >0 bytes already received
    by DVS — the proxy truncates every outputs:stage attempt after 1 MiB
    (upstream EOF mid-chunked-body). PBS must exhaust its bounded retries,
    mark the build FAILED, abort the reservation, and the output dataset
    must be untouched; DVS must leave no sealed/temp file behind."""
    tag = args.tag
    h = load_handle(tag)
    if not three_b_present():
        record("fault[C5b] mid-stage truncation", "blocked", "3B PBS batched files absent")
        return
    before_versions = list_versions(h["output_rid"])
    before_scan = scan_all(h["output_rid"], h["out_head_version"])

    pipe = create_pipeline(tag, h["input_rid"], h["output_rid"])
    publish_spec(pipe, {
        "transform_type": "batched_dataset_copy",
        "input": {"version": h["input_version"], "branch": "main",
                  "all_files": True, "predicate": PRED_JSON},
        "output": {"logical_path": "part-00000.ndjson"},
    }, [{"dataset_rid": canon(h["input_rid"])}], [canon(h["output_rid"])])
    build_id, _ = create_build(pipe, h["output_rid"])
    resv = pg("wp33c_pbs",
              f"SELECT transaction_rid FROM build_pending_output_txns WHERE build_id='{build_id}'")
    resv_rid = resv[0] if resv else ""

    st, _ = http("POST", PROXY_CTL + "/truncate-stage",
                 {"path_suffix": "/outputs:stage", "after_bytes": 1 << 20, "count": 30})
    assert st == 200, "arm truncate failed"
    # 180s: 3x the PBS middleware timeout — a bounded implementation must
    # answer (e.g. 503 batched_stage_interrupted) well inside it.
    est, eresp = execute(build_id, timeout=180)
    time.sleep(1.0)
    ev = [e for e in proxy_events() if e["action"] == "stage_truncated"]
    record("fault[C5b] DVS received >0 bytes then EOF (>=1 truncation)",
           "passed" if ev else "failed",
           f"truncations={len(ev)} bytes={ev[0]['bytes_forwarded'] if ev else 0} "
           f"execute_status={est} err={json.dumps(eresp)[:180] if isinstance(eresp, dict) else eresp!r}")
    bounded = est != 0
    record("fault[C5b] bounded stage-failure response within 180s (3x middleware timeout)",
           "passed" if bounded else "failed",
           "PBS answered: " + (f"status={est}" if bounded
                               else "NO response in 180s — execute hangs unbounded "
                                    "(3B-D4: stage transport error not bounded; build stuck "
                                    "STAGING holding its lease)"))

    # Output dataset: old head ONLY — versions, and the old version's rows
    # byte-identical to the pre-failure scan.
    after_versions = list_versions(h["output_rid"])
    after_scan = scan_all(h["output_rid"], h["out_head_version"])
    unchanged = (before_versions == after_versions
                 and before_scan["rendered_sha256"] == after_scan["rendered_sha256"]
                 and before_scan["count"] == after_scan["count"])
    record("fault[C5b] old head/index/row-values unchanged",
           "passed" if unchanged else "failed",
           f"versions={[v['version'] for v in after_versions]} "
           f"scan_sha_stable={before_scan['rendered_sha256'][:12]}…")

    # DVS: reservation ABORTED; no staged-object descriptor row; staging dir
    # carries no file for this reservation.
    if resv_rid:
        descriptors = pg("wp33c_dvs",
                         "SELECT count(*) FROM dataset_staged_output_objects "
                         f"WHERE reservation_id='{rid_tail(resv_rid)}'")
        resv_dir = docker("exec", f"{PROJECT}-dvs", "sh", "-c",
                          f"find /var/of/staging -type f -path '*{rid_tail(resv_rid)}*' | wc -l",
                          check=False).stdout.strip()
        record("fault[C5b] no sealed/temp residue",
               "passed" if descriptors == ["0"] and resv_dir == "0" else "failed",
               f"descriptor_rows={descriptors[0] if descriptors else '?'} staging_files={resv_dir}")

    # Recovery semantics: re-execute probe (3B documents stage as
    # idempotently re-executable), then the abort route — the documented
    # cleanup for an attempt that cannot continue.
    rest, rresp = execute(build_id, timeout=180)
    record("fault[C5b] re-execute probe answered",
           "passed" if rest != 0 else "failed",
           f"status={rest} body={json.dumps(rresp)[:160] if isinstance(rresp, dict) else rresp!r}")
    ast, aresp = http("POST", f"{PBS}/api/v1/builds/{build_id}/abort", {},
                      timeout=120, allow=(200, 202, 409))
    record("fault[C5b] abort route answered",
           "passed" if ast in (200, 202) else "failed",
           f"status={ast} body={json.dumps(aresp)[:160] if isinstance(aresp, dict) else aresp!r}")
    time.sleep(2.0)
    bstate = build_state(build_id)
    locks, pending = build_refs(build_id)
    if ast in (200, 202):
        # abort accepted: expect terminal ABORTED + refs released + the DVS
        # reservation aborted; DVS-side output invariance re-checked below.
        record("fault[C5b] build terminal after abort",
               "passed" if bstate in (["BUILD_ABORTED"], ["BUILD_FAILED"]) else "failed",
               f"builds.state={bstate}")
        record("fault[C5b] PBS refs cleaned after abort",
               "passed" if (locks, pending) == (0, 0) else "failed",
               f"build_input_locks={locks} build_pending_output_txns={pending}")
        if resv_rid:
            _, txn2 = http("GET",
                           f"{DVS}/v1/datasets/{h['output_rid']}/branches/main/transactions/{rid_tail(resv_rid)}")
            resv_state2 = (txn2.get("status") or "") if isinstance(txn2, dict) else ""
            record("fault[C5b] reservation ABORTED after abort",
                   "passed" if resv_state2 == "ABORTED" else "failed",
                   f"status={resv_state2 or '—'}")
    else:
        record("fault[C5b] cleanup reachable without lease expiry",
               "failed",
               "abort answered 409 — cleanup deferred until the 30m lease expires "
               "(3B-D4 corollary: a hung stage attempt blocks both re-execute and abort)")
    final_versions = list_versions(h["output_rid"])
    final_scan = scan_all(h["output_rid"], h["out_head_version"])
    record("fault[C5b] output dataset still untouched after recovery probes",
           "passed" if (final_versions == before_versions
                        and final_scan["rendered_sha256"] == before_scan["rendered_sha256"])
           else "failed",
           f"versions={[v['version'] for v in final_versions]}")
    http("POST", PROXY_CTL + "/reset", {})  # never leak armed rules into the next phase
    write_json(os.path.join(EVIDENCE, "faults", f"{tag}-truncate.json"), {
        "build_id": build_id, "reservation": resv_rid,
        "truncations": len(ev), "execute_status": est,
        "execute_response": eresp, "reexecute_status": rest,
        "abort_status": ast, "abort_response": aresp,
        "build_state": bstate,
        "versions_after": final_versions, "locks": locks, "pending": pending,
    })


def phase_version_pin(args):
    """C4 (review-strengthened): after CreateBuild freezes the pinned input
    version, commit a DISTINCT poison head (v+1) on the input dataset, then
    execute — the output must equal the PINNED version's golden, proving the
    build did not resolve through the branch head."""
    tag = args.tag
    h = load_handle(tag)
    if not three_b_present():
        record("vpin explicit version survives poison head", "blocked",
               "3B PBS batched files absent")
        return
    pipe = create_pipeline(tag, h["input_rid"], h["output_rid"])
    publish_spec(pipe, {
        "transform_type": "batched_dataset_copy",
        "input": {"version": h["input_version"], "branch": "main",
                  "all_files": True, "predicate": PRED_JSON},
        "output": {"logical_path": "part-00000.ndjson"},
    }, [{"dataset_rid": canon(h["input_rid"])}], [canon(h["output_rid"])])
    build_id, _ = create_build(pipe, h["output_rid"])

    # poison head: one distinct row whose value can never appear in the
    # pinned version's golden
    poison = json.dumps({"row_id": 999999, "bucket": 0, "scale": "poison-head",
                         "payload": "P" * (ROW_BYTES - 64)}, separators=(",", ":"))
    poison = (poison + " " * (ROW_BYTES - len(poison) - 1) + "\n").encode()
    http("POST", f"{DVS}/v1/datasets/{h['input_rid']}/outputs:commit", {
        "idempotency_key": f"wp33c-poison-{tag}",
        "branch": "main", "transaction_type": "SNAPSHOT",
        "summary": "poison head after build creation",
        "files": [{"logical_path": "poison.ndjson", "size_bytes": len(poison),
                   "operation": "ADD", "content_type": "application/x-ndjson",
                   "content_base64": base64.b64encode(poison).decode()}],
    })
    pin_versions = list_versions(h["input_rid"])
    record("vpin poison head committed (input v%d exists)" % (h["input_version"] + 1),
           "passed" if len(pin_versions) == h["input_version"] + 1 else "failed",
           f"input_versions={[v['version'] for v in pin_versions]} pinned={h['input_version']}")

    est, eresp = execute(build_id)
    record("vpin execute 200", "passed" if est == 200 else "failed", f"status={est}")
    versions = list_versions(h["output_rid"])
    new_v = [v for v in versions if v["version"] > h["out_head_version"]]
    scan = scan_all(h["output_rid"], new_v[0]["version"]) if len(new_v) == 1 else {"count": -1, "rendered_sha256": ""}
    g = h["golden"]
    pinned_ok = (len(new_v) == 1 and scan["count"] == g["count"]
                 and scan["rendered_sha256"] == g["sha256"])
    poison_leak = any(b"poison" in json.dumps(r).encode() for r in scan.get("rows", [])) if scan.get("rows") else False
    record("vpin output == pinned version golden (poison head ignored)",
           "passed" if pinned_ok and not poison_leak else "failed",
           f"scan={scan.get('count')} golden={g['count']} sha={str(scan.get('rendered_sha256'))[:12]}… "
           f"poison_rows_present={poison_leak}")
    write_json(os.path.join(EVIDENCE, "faults", f"{tag}-vpin.json"), {
        "build_id": build_id, "input_versions": [v["version"] for v in pin_versions],
        "pinned": h["input_version"], "output_scan": {k: scan.get(k) for k in ("count", "rendered_sha256")},
        "golden": g,
    })


def phase_fault_ack(args):
    tag = args.tag
    h = load_handle(tag)
    if not three_b_present():
        record("fault[C6] publish ACK loss + replay", "blocked", "3B PBS batched files absent")
        return
    pipe = create_pipeline(tag, h["input_rid"], h["output_rid"])
    publish_spec(pipe, {
        "transform_type": "batched_dataset_copy",
        "input": {"version": h["input_version"], "branch": "main",
                  "all_files": True, "predicate": PRED_JSON},
        "output": {"logical_path": "part-00000.ndjson"},
    }, [{"dataset_rid": canon(h["input_rid"])}], [canon(h["output_rid"])])
    build_id, _ = create_build(pipe, h["output_rid"])
    base_versions = len(list_versions(h["output_rid"]))
    assert base_versions == h["out_head_version"], "output changed before execute"

    http("POST", PROXY_CTL + "/reset", {})  # defensive: no stale rules from earlier phases
    commit_path = f"/v1/datasets/{canon(h['output_rid'])}/outputs:commit"
    st, arm = http("POST", PROXY_CTL + "/drop-next",
                   {"method": "POST", "path": commit_path, "key": ""})
    assert st == 200 and arm.get("armed"), (st, arm)
    est, eresp = execute(build_id)
    time.sleep(0.5)
    # The drop makes the publish outcome UNKNOWN to PBS by construction:
    # record exactly what PBS observed on the first attempt.
    first_obs = {"status": est,
                 "error": (eresp.get("error") if isinstance(eresp, dict) else str(eresp)[:120]),
                 "outcome": "unknown-to-pbs-by-drop"}
    ok_unknown = (est == 503 and isinstance(eresp, dict)
                  and eresp.get("error") == "batched_publish_outcome_unknown")
    record("fault[C6] first execute answered 503 batched_publish_outcome_unknown",
           "passed" if ok_unknown else "failed",
           f"status={est} err={first_obs['error']}")

    ev = proxy_events()
    drops = [e for e in ev if e["action"] == "ack_dropped"]
    drop = drops[-1] if drops else {}
    record("fault[C6] proxy dropped ACK only after COMMITTED",
           "passed" if drop else "failed",
           f"upstream_status={drop.get('upstream_status')} txn={str(drop.get('transaction_id'))[:8]}… "
           f"req_sha={str(drop.get('request_sha256'))[:12]}…")

    # External state: DVS actually published exactly one new version.
    versions_now = list_versions(h["output_rid"])
    new_v = [v for v in versions_now if v["version"] > h["out_head_version"]]
    ext_ok = len(versions_now) == base_versions + 1 and len(new_v) == 1
    record("fault[C6] DVS committed exactly one version", "passed" if ext_ok else "failed",
           f"len(versions)={len(versions_now)} (base {base_versions}+1) new={[v['version'] for v in new_v]} "
           f"head_txn={new_v[0]['transaction_id'][:8] if new_v else '—'}")
    published_txn = new_v[0]["transaction_id"] if new_v else None

    # Receipt visible before replay (proof the receipt, not a re-commit,
    # answers the replay later).
    key = drop.get("key") or ""
    rec_before = receipt_get(h["output_rid"], key)[0] if key else None
    record("fault[C6] receipt stored before replay", "passed" if rec_before == 200 else "failed",
           f"GET outputs/commits/{key[:20]}… -> {rec_before}")

    # BEFORE the restart: PBS's own durable state must show the unknown
    # publish outcome and hold the EXACT original commit body — the replay
    # is only legitimate because these bytes were persisted before reboot.
    row = pg("wp33c_pbs",
             "SELECT stage, octet_length(commit_request_body), "
             "encode(sha256(commit_request_body),'hex') "
             f"FROM build_batched_configs WHERE build_id='{build_id}'")
    if row:
        pg_stage, body_len, body_sha = row[0].split("|")
        stage_ok = pg_stage in ("PUBLISHING", "OUTCOME_UNKNOWN")
        record("fault[C6] PBS persisted stage PUBLISHING/OUTCOME_UNKNOWN before restart",
               "passed" if stage_ok else "failed", f"stage={pg_stage}")
        record("fault[C6] PBS persisted exact original commit body (sha == dropped digest)",
               "passed" if (int(body_len) > 0 and body_sha == drop.get("request_sha256")) else "failed",
               f"len={body_len} sha={body_sha[:16]}… drop={str(drop.get('request_sha256'))[:16]}…")
    else:
        record("fault[C6] PBS persisted batched config row", "failed", "row not found")
    pre_locks, pre_pending = build_refs(build_id)
    record("fault[C6] refs state recorded before restart", "passed",
           f"build_input_locks={pre_locks} build_pending_output_txns={pre_pending} "
           f"(retention while outcome unknown is 3B-owned semantics; finalize clears)")

    # Restart the REAL PBS process (same container identity, fresh process).
    docker("restart", f"{PROJECT}-pbs")
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(PBS + "/healthz", timeout=2):
                break
        except Exception:
            time.sleep(0.5)
    # Same actor replay: PBS must resend the persisted exact body bytes.
    rest, rresp = execute(build_id)
    ev2 = proxy_events()
    passes = [e for e in ev2 if e["action"] == "commit_passthrough"]
    replay = passes[-1] if passes else {}
    digest_equal = bool(replay) and replay.get("request_sha256") == drop.get("request_sha256")
    record("fault[C6] replay byte-identical (exact body hashes equal)",
           "passed" if digest_equal else "failed",
           f"first={str(drop.get('request_sha256'))[:16]}… replay={str(replay.get('request_sha256'))[:16]}…")
    # No re-scan / re-stage between drop and replay: the only proxy events
    # between ack_dropped and the replay commit are the replay itself.
    drop_idx = ev2.index(drop) if drop in ev2 else -1
    between = [e for e in ev2[drop_idx + 1:]
               if e["action"] == "forward" and
               ("/rows:scan" in e.get("path", "") or "/outputs:stage" in e.get("path", ""))]
    record("fault[C6] no re-scan/re-stage before replay",
           "passed" if not between else "failed", f"scan_stage_forwards_between={len(between)}")
    record("fault[C6] replay execute 200", "passed" if rest == 200 else "failed",
           f"status={rest} body={json.dumps(rresp)[:200] if isinstance(rresp, dict) else rresp!r}")

    # Still exactly one new version in total; receipt fingerprint == dropped
    # request digest proves the exact-body replay hit the stored receipt.
    versions2 = list_versions(h["output_rid"])
    still_one = len([v for v in versions2 if v["version"] > h["out_head_version"]]) == 1
    record("fault[C6] still exactly one new version", "passed" if still_one else "failed",
           f"versions={[v['version'] for v in versions2]}")
    if key:
        rst, rbody = receipt_get(h["output_rid"], key)
        fp = rbody.get("request_fingerprint") if isinstance(rbody, dict) else None
        record("fault[C6] receipt fingerprint == dropped body digest",
               "passed" if fp == drop.get("request_sha256") else "failed",
               f"fingerprint={str(fp)[:16]}… drop_sha={str(drop.get('request_sha256'))[:16]}…")
        raw_sha, _err = receipt_raw_sha(h["output_rid"], key)
        record("fault[C6] original receipt bytes hashed",
               "passed" if raw_sha else "failed", f"receipt_response_sha256={raw_sha[:16]}…")
        # PG AUTHORITATIVE row (dataset_output_commit_receipts): the stored
        # request_fingerprint must equal the dropped request digest, and the
        # stored response_body's SHA256 must equal the raw HTTP response's.
        row = pg("wp33c_dvs",
                 "SELECT request_fingerprint, encode(sha256(convert_to(response_body,'UTF8')),'hex') "
                 "FROM dataset_output_commit_receipts "
                 f"WHERE dataset_id='{h['output_rid']}' AND idempotency_key='{key}'")
        if row:
            pg_fp, pg_body_sha = row[0].split("|")
            record("fault[C6] PG receipt fingerprint == dropped body digest",
                   "passed" if pg_fp == drop.get("request_sha256") else "failed",
                   f"pg={pg_fp[:16]}… drop={str(drop.get('request_sha256'))[:16]}…")
            record("fault[C6] PG receipt response_body SHA == raw HTTP response SHA",
                   "passed" if pg_body_sha == raw_sha else "failed",
                   f"pg_body={pg_body_sha[:16]}… http_raw={raw_sha[:16]}…")
        else:
            record("fault[C6] PG authoritative receipt row", "failed", "row not found")
    jo = job_outputs_of(build_id)
    committed = jo[0][1].lower() in ("t", "true") if jo else False
    bound = (len(jo) == 1 and committed
             and published_txn and rid_tail(jo[0][0]) == rid_tail(published_txn))
    record("fault[C6] job_outputs bound after replay", "passed" if bound else "failed",
           f"job_outputs={jo} published_txn={str(published_txn)[:8]}…")
    bstate = build_state(build_id)
    record("fault[C6] build terminal state", "passed" if bstate == ["BUILD_COMPLETED"] else "failed",
           f"builds.state={bstate}")
    locks, pending = build_refs(build_id)
    record("fault[C6] PBS refs cleaned", "passed" if (locks, pending) == (0, 0) else "failed",
           f"build_input_locks={locks} build_pending_output_txns={pending}")

    write_json(os.path.join(EVIDENCE, "faults", f"{tag}-ack.json"), {
        "build_id": build_id, "first_execute": first_obs, "pbs_restarted": True,
        "drop": drop, "replay": replay, "receipt_raw_sha256": (receipt_raw_sha(h["output_rid"], key)[0] if key else ""),
        "versions": versions2, "job_outputs": [list(x) for x in jo],
        "build_state": bstate, "locks": locks, "pending": pending,
        "first_execute_status": est, "replay_execute_status": rest,
    })


def phase_dvs_chain(_args):
    """DVS-only dry run: the exact wire PBS 3B will emit (reservation with
    the wp33-staged-output marker, streaming outputs:stage, keyed STAGED_ADD
    commit), proven against the real DVS before PBS lands."""
    fx = build_fixture(300, "s300")
    rid = create_dataset("wp33c-dvschain")
    ensure_main_branch(rid)
    build_id = str(uuid.uuid4())

    _, resv = http("POST", f"{DVS}/v1/datasets/{rid}/branches/main/transactions", {
        "type": "UPDATE",
        "summary": "wp33c 3C dvs-chain",
        "providence": {"origin": "pipeline-build-service",
                       "purpose": "wp33-staged-output", "build_id": build_id},
    }, allow=(200, 201))
    reservation_id = resv.get("id")
    assert reservation_id and resv.get("status") == "OPEN", resv

    staged = b"".join(l for fn in sorted(fx["files"]) for l in fx["files"][fn] if json.loads(l)["bucket"] == 0)
    key = "wp33c-dvschain-1"  # stage AND commit share one idempotency key
    q = urllib.parse.urlencode({
        "branch": "main", "reservation_id": reservation_id, "build_id": build_id,
        "logical_path": "part-00000.ndjson", "idempotency_key": key,
    })
    req = urllib.request.Request(
        f"{DVS}/v1/datasets/{rid}/outputs:stage?{q}", data=staged,
        headers={"Authorization": "Bearer " + TOKEN,
                 "Content-Type": "application/x-ndjson"}, method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        assert r.status == 201, r.status
        sresp = json.loads(r.read())
    ok_stage = (sresp["size_bytes"] == len(staged)
                and sresp["sha256"] == hashlib.sha256(staged).hexdigest()
                and sresp["record_count"] == len(staged) // ROW_BYTES)
    record("dvschain stage triple-agreement", "passed" if ok_stage else "failed",
           f"size={sresp['size_bytes']}=={len(staged)} sha ok "
           f"records={sresp['record_count']}")

    _, cresp = http("POST", f"{DVS}/v1/datasets/{rid}/outputs:commit", {
        "idempotency_key": key,
        "branch": "main", "transaction_type": "SNAPSHOT",
        "reservation_id": reservation_id, "build_id": build_id,
        "files": [{"logical_path": "part-00000.ndjson", "operation": "STAGED_ADD",
                   "staged_object_id": sresp["staged_object_id"],
                   "size_bytes": sresp["size_bytes"], "sha256": sresp["sha256"]}],
    })
    txn = cresp.get("transaction") or {}
    ok_commit = txn.get("status") == "COMMITTED"
    record("dvschain STAGED_ADD commit", "passed" if ok_commit else "failed",
           f"txn={str(txn.get('id'))[:8]}… status={txn.get('status')}")

    versions = list_versions(rid)
    ok_ver = len(versions) == 1 and versions[0]["transaction_id"] == txn.get("id")
    scan = scan_all(rid, versions[0]["version"]) if versions else {"count": -1}
    golden_rows = [json.loads(l) for l in
                   [x for fn in sorted(fx["files"]) for x in fx["files"][fn]]
                   if json.loads(l)["bucket"] == 0]
    ok_scan = (scan["count"] == len(golden_rows)
               and scan["rendered_sha256"] == fx["golden"]["sha256"])
    record("dvschain published scan == golden", "passed" if ok_scan else "failed",
           f"scan={scan['count']} golden={len(golden_rows)}")

    # Negative: same key, different body must be rejected (receipt conflict
    # 409, or staged-binding mismatch 400 — either way zero side effects).
    st2, _ = http("POST", f"{DVS}/v1/datasets/{rid}/outputs:commit", {
        "idempotency_key": key,
        "branch": "main", "transaction_type": "SNAPSHOT",
        "reservation_id": reservation_id, "build_id": build_id,
        "files": [{"logical_path": "part-00000.ndjson", "operation": "STAGED_ADD",
                   "staged_object_id": sresp["staged_object_id"],
                   "size_bytes": 1, "sha256": "0" * 64}],
    }, allow=(200, 201, 400, 409), )
    still1 = len(list_versions(rid)) == 1
    record("dvschain negative: different body same key rejected",
           "passed" if st2 in (400, 409) and still1 else "failed",
           f"status={st2} versions still {len(list_versions(rid))}")
    write_json(os.path.join(EVIDENCE, "dvschain.json"), {
        "dataset_rid": rid, "reservation_id": reservation_id,
        "staged": {k: sresp[k] for k in ("size_bytes", "sha256", "record_count")},
        "txn": txn, "scan": {k: scan[k] for k in ("count", "rendered_sha256")},
    })


# ------------------------------------------------------------------ report


def load_all_checks():
    out = []
    cdir = os.path.join(EVIDENCE, "checks")
    if os.path.isdir(cdir):
        for fn in sorted(os.listdir(cdir)):
            if fn.endswith(".json"):
                with open(os.path.join(cdir, fn), encoding="utf-8") as f:
                    out.extend(json.load(f))
    return out


def phase_report(args):
    global checks
    checks = load_all_checks()
    lines = []
    ap = lines.append
    ap("# WP3.3 增量 3C 报告：真实 PBS+DVS+PostgreSQL 端到端内存/回执烟测\n")
    ap(f"- 生成：{time.strftime('%Y-%m-%dT%H:%M:%S%z')}；harness `harness3c.py report`。")
    ap("- 受测代码与环境指纹：见 `fingerprint.json`（HEAD + dirty-path blob hashes + 二进制 SHA256）。")
    fp_start = os.path.join(EVIDENCE, "fingerprint.json")
    fp_end = os.path.join(EVIDENCE, "fingerprint_end.json")
    if os.path.exists(fp_end):
        a = json.load(open(fp_start, encoding="utf-8"))["working_tree_fingerprint"]
        b = json.load(open(fp_end, encoding="utf-8"))["working_tree_fingerprint"]
        stable = a == b
        ap(f"- 源 manifest 冻结：build 起点 `{a[:16]}…` vs 套件终点 `{b[:16]}…` → "
           f"{'一致（本报告绑定该源）' if stable else '中途变更（本报告判 invalidated，需冻结后重跑）'}")
        record("source manifest frozen at suite end (start == end)",
               "passed" if stable else "failed",
               f"start={a[:16]}… end={b[:16]}…")
    else:
        record("source manifest frozen at suite end (start == end)", "failed",
               "fingerprint_end.json missing — suite did not complete its closing freeze")
    ap("- 门槛（预约定，未为追绿调整）：每服务 @15,000 行结束 VmHWM ≤ 192 MiB；"
       "每服务 peak 增量（@15k − @1.5k）≤ 24 MiB；无 GOMEMLIMIT。\n")

    # memory thresholds — every PAIR (filtered 1/3 and full no-filter) is
    # held to the same pre-agreed gates.
    pairs = []
    for base in ("scale", "full"):
        pair = {}
        for tag in (f"{base}1500", f"{base}15000"):
            path = os.path.join(EVIDENCE, "memory", f"{tag}.json")
            pair[tag] = json.load(open(path, encoding="utf-8")) if os.path.exists(path) else None
        pairs.append((base, pair))
    ap("## 内存（Linux /proc/1，seed 后重启、每规模独立新进程）\n")
    ap("| 对照对 | 服务 | 规模 | 起始 VmHWM KiB | 结束 VmHWM KiB | starttime | VmRSS 峰值 KiB |")
    ap("| --- | --- | --- | --- | --- | --- | --- |")
    for base, pair in pairs:
        for tag in (f"{base}1500", f"{base}15000"):
            m = pair[tag]
            if not m or m.get("blocked"):
                ap(f"| {base} | dvs | {tag} | blocked | blocked | — | — |")
                ap(f"| {base} | pbs | {tag} | blocked | blocked | — | — |")
                continue
            for svc in ("dvs", "pbs"):
                st_, e = m["proc"]["start"][svc], m["proc"]["end"][svc]
                ap(f"| {base}({m.get('mode', 'filtered')}) | {svc} | {m['rows']} "
                   f"| {st_.get('vmhwm_kib')} | {e.get('vmhwm_kib')} "
                   f"| {st_.get('starttime_ticks')}→{e.get('starttime_ticks')} "
                   f"| {m['rss_peak_kib'].get(svc)} |")
    for base, pair in pairs:
        if all(pair.get(t) and not pair[t].get("blocked") for t in (f"{base}1500", f"{base}15000")):
            for svc in ("dvs", "pbs"):
                big = pair[f"{base}15000"]["proc"]["end"][svc]["vmhwm_kib"] * 1024
                delta = (pair[f"{base}15000"]["proc"]["end"][svc]["vmhwm_kib"]
                         - pair[f"{base}1500"]["proc"]["end"][svc]["vmhwm_kib"]) * 1024
                ok = big <= THRESH_LARGE_PEAK and delta <= THRESH_DELTA
                record(f"threshold[{svc}|{base}] @15k ≤192MiB and 10x delta ≤24MiB",
                       "passed" if ok else "failed",
                       f"peak15k={big/1048576:.1f}MiB delta={(delta)/1048576:.1f}MiB")
        else:
            record(f"threshold[dvs|pbs|{base}] memory", "blocked", "3B 缺席或内存阶段 blocked")
    ap("\n门槛声明：VmHWM 为进程高水位（宏观证据）；两组规模 × 两种对照（1/3 过滤与全量"
       "不过滤，后者输出体量跨过 24 MiB 增量门）+ 0.25s VmRSS 采样构成判断依据，不构成对"
       "内部缓冲策略的形式化证明。每窗口已断言 pid1=/service、零重启、无 OOM。\n")

    ap("\n## 验收条件\n")
    ap("| 条件 | 结果 | 细节 |")
    ap("| --- | --- | --- |")
    for c in checks:
        ap(f"| {c['name']} | {c['status']} | {c['detail']} |")

    defects_path = os.path.join(REPO, "tools/smoke/data-integration-wp3-3/results/3b-defects.json")
    if os.path.exists(defects_path):
        ap("\n## 3B 侧缺陷登记（3C 发现、3B 会话所有，3C 未改服务代码）\n")
        for d in json.load(open(defects_path, encoding="utf-8"))["defects"]:
            ap(f"- **{d['id']} {d['title']}** — {d['where']}")
            ap(f"  - 症状：{d['symptom']}")
            ap(f"  - 证据：{d['evidence']}")
            ap(f"  - 状态：{d['status']}")

    ap("\n## 证据文件\n")
    for root, _, files in os.walk(EVIDENCE):
        for fn in sorted(files):
            rel = os.path.relpath(os.path.join(root, fn), EVIDENCE)
            ap(f"- `{rel}`")

    report_path = os.path.join(REPO, "tools/smoke/data-integration-wp3-3/results/increment-3c-report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    write_json(os.path.join(EVIDENCE, "checks.json"), checks)
    failed = [c for c in checks if c["status"] == "failed"]
    blocked = [c for c in checks if c["status"] == "blocked"]
    print(f"report -> {report_path}")
    print(f"checks: {len(checks)} passed={sum(c['status']=='passed' for c in checks)} "
          f"failed={len(failed)} blocked={len(blocked)}")
    sys.exit(1 if failed else 0)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="phase", required=True)
    p = sub.add_parser("seed"); p.add_argument("--rows", type=int, required=True); p.add_argument("--tag", required=True)
    p = sub.add_parser("memory-run"); p.add_argument("--rows", type=int, required=True); p.add_argument("--tag", required=True)
    p.add_argument("--mode", default="filtered", choices=["filtered", "full"])
    p = sub.add_parser("fault-stage"); p.add_argument("--tag", required=True)
    p = sub.add_parser("fault-truncate"); p.add_argument("--tag", required=True)
    p = sub.add_parser("fault-ack"); p.add_argument("--tag", required=True)
    p = sub.add_parser("version-pin"); p.add_argument("--tag", required=True)
    p = sub.add_parser("dvs-chain")
    p = sub.add_parser("report"); p.add_argument("--run-dir")
    args = ap.parse_args()
    fn = globals()[f"phase_{args.phase.replace('-', '_')}"]
    if args.phase == "report":
        # report reads the per-phase check files; nothing to record here first
        fn(args)
        return
    try:
        fn(args)
    finally:
        # checks live per-invocation; persist them so the report phase (a
        # separate process) aggregates every phase's verdicts.
        suffix = getattr(args, "tag", "") or ""
        write_json(os.path.join(EVIDENCE, "checks", f"{args.phase}{'-' + suffix if suffix else ''}.json"), checks)


if __name__ == "__main__":
    main()

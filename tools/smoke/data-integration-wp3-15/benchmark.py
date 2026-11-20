"""Compare paired real NDJSON and Parquet writes through PBS and DVS."""

from hashlib import sha256
import json
import os
from pathlib import Path
from statistics import median
import sys
from time import perf_counter_ns


HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE), str(HERE.with_name("data-integration-wp3-13"))]
import acceptance as writer
import fixture
import published


stack = writer.stack
expect = writer.expect
EVIDENCE = writer.EVIDENCE
SERVICES = ("pbs", "dvs")
PAIR_ORDERS = (("ndjson", "parquet"), ("parquet", "ndjson")) * 2
EXECUTE_TIMEOUT_SECONDS = 300
POISON_PATH = "new-head.ndjson"
POISON_BYTES = (
    b'{"order_id":-1,"amount":"0.0000","order_date":"2025-01-01",'
    b'"ordered_at":"2025-01-01T00:00:00.000000Z","source_time_zone":"UTC",'
    b'"note":"new head sentinel"}\n'
)


def checkpoint(report):
    path = EVIDENCE / "benchmark.json"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def seed_source(files, golden):
    expect("source row count", len(golden.rows), fixture.ROWS)
    expect("source file count", len(files), 4)
    paths = [name for name, _ in files]
    expect("distinct sorted paths in source order", paths, sorted(set(paths)))
    for name, raw in files:
        expect(f"{name} records", raw.count(b"\n"), fixture.ROWS // 4)
        expect(f"{name} ends at a record boundary", raw.endswith(b"\n"), True)

    rid = stack.create_dataset("wp315-common-input")
    stack.ensure_main_branch(rid)
    committed = stack.inline_commit(rid, "wp315-common-input", files, "WP3.15 shared pinned snapshot")
    transaction = committed["transaction"]["id"]
    version = stack.version_by_txn(rid, transaction)
    source = {"dataset": rid, "branch": "main", "version": version, "transaction": transaction}
    return source, snapshot_files(source, paths)


def snapshot_files(pin, logical_paths):
    actual = [file for file in writer.file_list(pin["dataset"])
              if file["transaction_id"] == pin["transaction"]]
    expect("snapshot registers precisely its expected files",
           sorted(file["logical_path"] for file in actual), sorted(logical_paths))
    return {file["logical_path"]: file for file in actual}


def verify_input_files(pin, registered, expected):
    proof = []
    for name, want in expected:
        file = registered[name]
        raw = writer.download(pin["dataset"], file)
        digest = sha256(raw).hexdigest()
        expect(f"{name} committed file digest", digest, sha256(want).hexdigest())
        expect(f"{name} registered and downloaded size", (file["size_bytes"], len(raw)), (len(want), len(want)))
        proof.append({
            "file_id": file["id"], "logical_path": name, "size_bytes": len(raw),
            "sha256": digest, "record_count": raw.count(b"\n"),
        })
    return proof


def advance_input_head(pinned):
    rid = pinned["dataset"]
    before = stack.list_versions(rid)
    reply = stack.inline_commit(rid, "wp315-new-head", [(POISON_PATH, POISON_BYTES)], "WP3.15 later input head")
    transaction = reply["transaction"]["id"]
    version = stack.version_by_txn(rid, transaction)
    expect("later input head advances one version", version, pinned["version"] + 1)
    expect("later input head is the replacement transaction",
           writer.get(writer.dvs(rid, "branches/main"))["head_transaction_id"], transaction)
    after = stack.list_versions(rid)
    expect("only one new input version", len(after), len(before) + 1)
    expect("original input versions survived the head change", all(item in after for item in before), True)
    pin = {"dataset": rid, "branch": "main", "version": version, "transaction": transaction}
    registered = snapshot_files(pin, [POISON_PATH])
    return {**pin, "files": verify_input_files(pin, registered, [(POISON_PATH, POISON_BYTES)])}


def prepare_trial(pair, fmt, source):
    trial = f"pair{pair}-{fmt}"
    logical = f"part-00000.{fmt}"
    rid, old_file, before = writer.seed_output(trial)
    pipeline = stack.create_pipeline(trial, source["dataset"], rid)
    output = {"logical_path": logical}
    if fmt == "parquet":
        output.update(format="parquet", schema_version=writer.SCHEMA)
    spec = stack.publish_spec(pipeline, {
        "transform_type": "batched_dataset_copy",
        "input": {"branch": source["branch"], "version": source["version"], "all_files": True},
        "output": output,
    }, [{"dataset_rid": stack.canon(source["dataset"])}], [stack.canon(rid)])
    build_id, _ = stack.create_build(pipeline, rid)
    ident = writer.id_tail(build_id)
    reservation, = stack.pg(
        "wp33c_pbs", "SELECT transaction_rid FROM build_pending_output_txns "
        f"WHERE build_id='{ident}'",
    )
    expect(f"{trial} reserved DVS output", writer.get(writer.transaction(rid, reservation))["status"], "OPEN")
    expect(f"{trial} held build references", stack.build_refs(build_id), (1, 1))
    config = stack.pg(
        "wp33c_pbs", "SELECT input_dataset_rid, input_branch, input_version, all_files, logical_path "
        f"FROM build_batched_configs WHERE build_id='{ident}'",
    )
    expect(f"{trial} frozen input version, all_files and output path", config,
           [f"{stack.canon(source['dataset'])}|{source['branch']}|{source['version']}|t|{logical}"])
    build = {
        "trial": trial, "build": build_id, "job_spec": spec, "pipeline": pipeline,
        "reservation": writer.id_tail(reservation), "source": source,
        "old_file": old_file, "logical_path": logical,
    }
    return rid, before, build


def container_labels(service):
    name = f"{stack.PROJECT}-{service}"
    fields = [
        '{{index .Config.Labels "com.docker.compose.project"}}',
        '{{index .Config.Labels "com.docker.compose.service"}}',
        "{{.Id}}", "{{.State.StartedAt}}", "{{.RestartCount}}", "{{.State.OOMKilled}}",
    ]
    raw = stack.docker("inspect", "--format", "|".join(fields), name).stdout
    project, label, container_id, started_at, restarts, oom_killed = raw.strip().split("|")
    expect(f"{service} benchmark project ownership", project, stack.PROJECT)
    expect(f"{service} Compose service identity", label, service)
    return {
        "container_id": container_id,
        "started_at": started_at,
        "restarts": int(restarts),
        "oom_killed": json.loads(oom_killed),
    }


def memory_snapshot():
    result = {}
    for service in SERVICES:
        name = f"{stack.PROJECT}-{service}"
        identity = container_labels(service)
        exe = stack.docker("exec", name, "readlink", "/proc/1/exe").stdout.strip()
        expect(f"{service} PID1 executable", exe, "/service")
        expect(f"{service} process had no OOM", identity["oom_killed"], False)
        proc = stack.proc_snapshot(service)
        expect(f"{service} sampled process PID", proc.get("pid"), "1")
        if min(proc.get(key, 0) for key in ("vmhwm_kib", "vmrss_kib", "starttime_ticks")) <= 0:
            raise AssertionError(f"missing valid memory or process data for {service}")
        result[service] = {**identity, **proc, "pid1_exe": exe}
    return result


def restart_services():
    before_restart = {service: container_labels(service) for service in SERVICES}
    stack.docker("restart", *(f"{stack.PROJECT}-{service}" for service in SERVICES))
    writer.wait_for_health(stack.DVS)
    writer.wait_for_health(stack.PBS)
    current = memory_snapshot()
    for service in SERVICES:
        expect(f"{service} restart preserved container", current[service]["container_id"],
               before_restart[service]["container_id"])
        if current[service]["started_at"] == before_restart[service]["started_at"]:
            raise AssertionError(f"{service} did not start a fresh process")
    return current


def measure_execute(build_id):
    before = restart_services()
    started = perf_counter_ns()
    result = stack.execute(build_id, timeout=EXECUTE_TIMEOUT_SECONDS)
    elapsed_ns = perf_counter_ns() - started
    after = memory_snapshot()
    if elapsed_ns <= 0:
        raise AssertionError("monotonic execute duration must be positive")
    for service in SERVICES:
        for key in ("container_id", "started_at", "restarts", "starttime_ticks", "pid"):
            expect(f"{service} {key} stable across timed request", after[service][key], before[service][key])
        if after[service]["vmhwm_kib"] < before[service]["vmhwm_kib"]:
            raise AssertionError(f"{service} VmHWM fell within a single-process window")
    memory = {service: {"before": before[service], "after": after[service]} for service in SERVICES}
    return result, elapsed_ns / 1_000_000_000, memory


def stats(values):
    return {"min": min(values), "median": median(values), "max": max(values)}


def summarize(pairs):
    summary = {}
    trials = [trial for pair in pairs for trial in pair["trials"]]
    for fmt in fixture.FORMATS:
        selected = [trial for trial in trials if trial["format"] == fmt]
        expect(f"{fmt} successful trials", len(selected), len(PAIR_ORDERS))
        summary[fmt] = {
            "elapsed_seconds": stats([trial["elapsed_seconds"] for trial in selected]),
            "size_bytes": stats([trial["publication"]["size_bytes"] for trial in selected]),
            "memory": {
                service: {
                    "peak_vmhwm_kib": stats([trial["memory"][service]["after"]["vmhwm_kib"] for trial in selected]),
                    "growth_vmhwm_kib": stats([
                        trial["memory"][service]["after"]["vmhwm_kib"] - trial["memory"][service]["before"]["vmhwm_kib"]
                        for trial in selected
                    ]),
                }
                for service in SERVICES
            },
        }
    return summary


def run(report):
    report["phase"] = "seed"
    files, golden = fixture.make_input()
    source, original_files = seed_source(files, golden)
    report["source"] = source
    checkpoint(report)

    report["phase"] = "prepare"
    prepared = [
        [prepare_trial(number, fmt, source) for fmt in order]
        for number, order in enumerate(PAIR_ORDERS, start=1)
    ]
    for idx, order in enumerate(PAIR_ORDERS, start=1):
        report["pairs"].append({"pair": idx, "order": list(order), "trials": []})
    build_ids = {build["build"] for pair in prepared for _, _, build in pair}
    targets = {rid for pair in prepared for rid, _, _ in pair}
    expect("eight distinct builds", len(build_ids), 8)
    expect("eight distinct output datasets", len(targets), 8)

    report["phase"] = "advance_input_head"
    later = advance_input_head(source)
    proof = verify_input_files(source, original_files, files)
    expect("source proven row count", sum(file["record_count"] for file in proof), fixture.ROWS)
    report["source"] = {
        **source, "mode": "all_files", "file_count": len(proof), "record_count": fixture.ROWS,
        "files": proof, "head_after_preparing_all_builds": later,
    }
    checkpoint(report)

    first_pair_publications = []
    first_pair_paths = {}
    for pair, prepared_pair in zip(report["pairs"], prepared, strict=True):
        for fmt, (rid, before, build) in zip(pair["order"], prepared_pair, strict=True):
            tag = build["trial"]
            report["phase"] = f"execute {tag}"
            checkpoint(report)
            execute, seconds, memory = measure_execute(build["build"])
            report["phase"] = f"verify {tag}"
            publication = published.verify_output(rid, before, build, fmt, execute)
            artifact_path = Path(publication["artifact_path"])
            artifact = fixture.validate_artifact(artifact_path, fmt, golden)
            if pair["pair"] == 1:
                first_pair_publications.append(publication)
                first_pair_paths[fmt] = artifact_path
            pair["trials"].append({
                "trial": tag, "format": fmt, "elapsed_seconds": seconds, "memory": memory,
                "build": {field: build[field] for field in ("build", "job_spec", "pipeline", "source")},
                "publication": {key: value for key, value in publication.items() if key != "artifact_path"},
                "artifact_verification": artifact,
            })
            checkpoint(report)
            print(f"PASS {tag}: {publication['record_count']} rows, {publication['size_bytes']} bytes, {seconds:.3f} s", flush=True)

    report["write_summary"] = summarize(report["pairs"])
    report["phase"] = "cross_org"
    report["cross_org"] = published.verify_isolation(first_pair_publications)
    checkpoint(report)
    report["phase"] = "queries"
    report["queries"] = fixture.compare_queries(first_pair_paths, golden)


def main():
    report = {
        "status": "running",
        "pairs": [],
        "method": {
            "publish_endpoint": "/api/v1/execute",
            "stage_timeout_seconds": int(os.environ["WP315_STAGE_TIMEOUT_SECONDS"]),
            "scan_page_rows": int(os.environ["WP315_SCAN_PAGE_ROWS"]),
            "execute_client_timeout_seconds": EXECUTE_TIMEOUT_SECONDS,
            "memory": "PID1 VmRSS and VmHWM immediately before and after execute, with no polling during the request",
            "write_time": "monotonic client wall time for execute only, including scan, encode, stage and commit",
        },
    }
    try:
        run(report)
    except Exception as err:
        report["status"] = "failed"
        report["failure_type"] = type(err).__name__
        checkpoint(report)
        raise
    report["status"] = "passed"
    report["phase"] = "complete"
    checkpoint(report)


if __name__ == "__main__":
    main()

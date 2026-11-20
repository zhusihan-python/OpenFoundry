"""Record scoped evidence for the clean WP3.15 format benchmark."""

from hashlib import file_digest
import json
from math import isfinite
import os
from pathlib import Path
import platform
import re
import subprocess
from statistics import median
import sys
import time
from urllib.request import urlopen

import duckdb

ROOT = Path(__file__).resolve().parents[3]
BINARIES = {"dvs": "dataset-versioning-service", "pbs": "pipeline-build-service"}
JWT = re.compile(rb"eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}")
ORDER = [
    ["ndjson", "parquet"], ["parquet", "ndjson"],
    ["ndjson", "parquet"], ["parquet", "ndjson"],
]


def require(condition, label):
    if not condition:
        raise AssertionError(label)


def digest(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def command(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def revision():
    head = command("git", "rev-parse", "HEAD")
    require(head == os.environ["WP315_EXPECTED_SHA"], "checkout does not match the requested benchmark SHA")
    dirty = command("git", "status", "--porcelain=v1", "--untracked-files=all")
    require(not dirty, "benchmark requires an exact clean checkout")
    return {"head": head, "tree": command("git", "rev-parse", "HEAD^{tree}"), "dirty": False}


def wait_healthy(base):
    deadline = time.monotonic() + 90
    while True:
        try:
            with urlopen(base + "/healthz", timeout=2) as response:
                require(response.status == 200, f"HTTP health response at {base}")
                return
        except OSError as exc:
            if time.monotonic() >= deadline:
                raise RuntimeError(f"service did not become healthy at {base}") from exc
        time.sleep(0.25)


def runtime():
    project = os.environ["WP33C_PROJECT"]
    for key in ("WP33C_DVS", "WP33C_PBS"):
        wait_healthy(os.environ[key])
    containers = {}
    fields = ("id", "image", "image_id", "started_at", "nano_cpus", "cpu_quota",
              "memory_limit_bytes", "oom_killed")
    for service in ("postgres", *BINARIES):
        target = f"{project}-{service}"
        settings = command(
            "docker", "inspect", "--format",
            "{{.Id}}|{{.Config.Image}}|{{.Image}}|{{.State.StartedAt}}|"
            "{{.HostConfig.NanoCpus}}|{{.HostConfig.CpuQuota}}|{{.HostConfig.Memory}}|{{.State.OOMKilled}}",
            target,
        ).split("|")
        container = dict(zip(fields, settings, strict=True))
        if service in BINARIES:
            container["binary_sha256"] = command("docker", "exec", target, "sha256sum", "/service").split()[0]
        containers[service] = container
    pbs_env = command(
        "docker", "exec", f"{project}-pbs", "printenv",
        "BATCHED_SCAN_PAGE_ROWS", "BATCHED_STAGE_TIMEOUT_SECONDS", "DATASET_SERVICE_URL",
        "SCHEDULER_CONSUMER_ENABLED",
    ).splitlines()
    expected = [os.environ["WP315_SCAN_PAGE_ROWS"], os.environ["WP315_STAGE_TIMEOUT_SECONDS"],
                "http://dvs:50078", "false"]
    require(pbs_env == expected, "PBS runtime does not use the pinned direct benchmark settings")
    with duckdb.connect() as conn:
        version, source_id, codename = conn.execute("PRAGMA version").fetchone()
    docker_host = json.loads(command(
        "docker", "info", "--format",
        '{"cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}},"kernel":{{json .KernelVersion}},'
        '"os":{{json .OperatingSystem}},"architecture":{{json .Architecture}},"storage":{{json .Driver}}}',
    ))
    return {
        "project": project,
        "services": containers,
        "build_image_id": command("docker", "image", "inspect", "--format", "{{.Id}}", "golang:1.26.3"),
        "postgres": command("docker", "exec", f"{project}-postgres", "psql", "-AtU", "openfoundry",
                            "-d", "wp33c_dvs", "-c", "SHOW server_version"),
        "reader": {"name": "DuckDB", "version": version, "source_id": source_id, "codename": codename},
        "host": {"platform": platform.platform(), "python": platform.python_version(), "docker_vm": docker_host},
        "docker": command("docker", "version", "--format", "{{.Server.Version}}"),
        "settings": {"scan_page_rows": int(pbs_env[0]), "stage_timeout_seconds": int(pbs_env[1]),
                     "dataset_service_url": pbs_env[2], "scheduler_enabled": False},
        "stubs": [],
    }


def positive(value):
    return type(value) in (float, int) and isfinite(value) and value > 0


def check_benchmark(evidence, result, deployed):
    require(result["status"] == "passed", "benchmark correctness gate failed")
    pairs = result["pairs"]
    require(len(pairs) == len(ORDER), "four complete pairs are required")
    source = result["source"]
    require(source["mode"] == "all_files" and source["file_count"] == len(source["files"]) == 4,
            "benchmark did not read the committed four-file source")
    require(source["record_count"] == 100_000 and all(f["record_count"] == 25_000 for f in source["files"]),
            "input fixture row count mismatch")
    require(source["head_after_preparing_all_builds"]["version"] == source["version"] + 1,
            "historical input control did not advance the head")
    starts = {service: set() for service in BINARIES}
    paths, output_datasets, exact_values = set(), set(), set()
    for n, pair in enumerate(pairs, 1):
        require(pair["pair"] == n and pair["order"] == ORDER[n - 1], f"invalid pair order at pair {n}")
        trials = pair["trials"]
        require([trial["format"] for trial in trials] == pair["order"], f"incomplete trial pair {n}")
        for trial in trials:
            label = trial["trial"]
            require(positive(trial["elapsed_seconds"]), f"{label} invalid wall time")
            publication = trial["publication"]
            require(publication["status"] == "passed" and publication["record_count"] == 100_000,
                    f"{label} publication or row-count check failed")
            require(publication["dataset"] not in output_datasets, f"{label} reused an earlier output dataset")
            output_datasets.add(publication["dataset"])
            artifact = (evidence / publication["artifact"]).resolve()
            require(artifact.is_relative_to(evidence) and artifact not in paths, f"{label} invalid artifact path")
            paths.add(artifact)
            require(publication["size_bytes"] > 0 and artifact.stat().st_size == publication["size_bytes"],
                    f"{label} signed artifact size mismatch")
            require(digest(artifact) == publication["sha256"], f"{label} signed artifact digest mismatch")
            independent = trial["artifact_verification"]
            require(independent["row_count"] == 100_000, f"{label} independent row-count mismatch")
            exact_values.add(independent["rows_sha256"])
            for service in BINARIES:
                before, after = (trial["memory"][service][when] for when in ("before", "after"))
                for snap in (before, after):
                    require(str(snap["pid"]) == "1" and snap["pid1_exe"] == "/service",
                            f"{label} wrong {service} PID1")
                    require(str(snap["restarts"]) == "0" and str(snap["oom_killed"]).lower() == "false",
                            f"{label} {service} restarted or was OOM-killed")
                    require(snap["container_id"] == deployed[service]["id"], f"{label} wrong {service} container")
                    require(all(positive(snap[k]) for k in ("vmhwm_kib", "vmrss_kib", "starttime_ticks")),
                            f"{label} malformed {service} proc measurements")
                start = before["starttime_ticks"]
                require(start not in starts[service] and start == after["starttime_ticks"] and
                        before["started_at"] == after["started_at"], f"{label} {service} process was not a fresh stable trial")
                starts[service].add(start)
                require(before["vmhwm_kib"] <= after["vmhwm_kib"], f"{label} invalid {service} high-water mark")
    require(len(exact_values) == 1, "independent value digests differ between trials")
    queries = result["queries"]
    require(queries["reader"] == {"duckdb": "v1.4.4", "threads": 1}
            and queries["warmups_per_format"] == 1 and queries["sample_order"] == ORDER[:2] * 5,
            "DuckDB run conditions differ from the paired plan")
    require(sum(row[1] for row in queries["store_profits"]) == 100_000, "store aggregate row counts mismatch")
    for fmt in ("ndjson", "parquet"):
        samples = queries["samples_seconds"][fmt]
        require(len(samples) == 10 and all(map(positive, samples)), f"invalid {fmt} DuckDB times")
        require(queries["median_seconds"][fmt] == median(samples), f"incorrect {fmt} query median")
    isolation = result["cross_org"]
    require(isolation["status"] == "passed" and isolation["same_subject"] and isolation["distinct_organizations"],
            "cross-organization proof did not validate both identities")
    require([p["format"] for p in isolation["denials"]] == ["ndjson", "parquet"],
            "cross-organization checks did not cover both formats")
    require(isolation["foreign_own_control"]["scan"] == isolation["foreign_own_control"]["download"] == 200,
            "foreign identity was not confirmed against its own data")
    for probe in isolation["denials"]:
        require(probe["pinned_scan"] == probe["cursor_scan"] == probe["download"] == 404 and
                probe["matches_missing"] and not probe["signed_url_returned"] and
                probe["before_state_sha256"] == probe["after_state_sha256"],
                "cross-organization response leaked information or changed the output")


def verify():
    evidence = Path(os.environ["WP33C_EVIDENCE"]).resolve()
    run = evidence.parent
    binaries_dir = Path(os.environ["WP315_BIN"])
    source = read_json(evidence / "source-before.json")
    require(source == revision() == read_json(evidence / "source-after.json"), "source changed across the run")
    require(read_json(run / "fingerprint.json")["head"] == source["head"], "Go build source mismatch")
    go_version = re.search(r"^go version (go1\.26\.3) linux/arm64$", (run / "build.log").read_text(), re.MULTILINE)
    require(go_version is not None, "missing actual pinned Go toolchain evidence")
    compiled = {Path(path).name: value for value, path in (
        line.split(maxsplit=1) for line in (run / "binary-sha256.txt").read_text().splitlines()
    )}
    binary_hashes = {name: digest(binaries_dir / name) for name in BINARIES.values()}
    require(compiled == binary_hashes, "built binary differs from build checksums")
    before = read_json(evidence / "runtime-before.json")
    after = read_json(evidence / "runtime-after.json")
    for key in ("docker", "build_image_id", "postgres", "host", "reader", "settings", "stubs", "project"):
        require(before[key] == after[key], f"{key} changed across benchmark measurements")
    require(after["reader"]["version"] == "v1.4.4", "benchmark reader version mismatch")
    for service in before["services"]:
        a, b = before["services"][service], after["services"][service]
        require(a["id"] == b["id"] and a["image_id"] == b["image_id"], f"{service} container swapped")
        require(a["oom_killed"] == b["oom_killed"] == "false", f"{service} OOM-killed")
        if service in BINARIES:
            require(a["binary_sha256"] == b["binary_sha256"] == binary_hashes[BINARIES[service]],
                    f"{service} deployed binary mismatch")
    result = read_json(evidence / "benchmark.json")
    require(all(result["method"][key] == after["settings"][key]
                for key in ("scan_page_rows", "stage_timeout_seconds")), "claimed benchmark settings differ from runtime")
    check_benchmark(evidence, result, after["services"])
    teardown = read_json(evidence / "teardown.json")
    require(teardown == {"project": after["project"], "containers": 0, "volumes": 0, "networks": 0},
            "isolated benchmark project was not fully removed")

    secrets = fixture_secrets()
    require(len(secrets) == 4 and all(secrets), "missing scoped fixture secrets for evidence hygiene")
    artifacts = {}
    build_evidence = [run / name for name in ("fingerprint.json", "binary-sha256.txt", "build.log")]
    for path in sorted([*build_evidence, *(p for p in evidence.rglob("*") if p.is_file())]):
        contents = path.read_bytes()
        require(not any(secret in contents for secret in secrets) and not JWT.search(contents),
                f"scoped job evidence contains a fixture credential: {path.name}")
        artifacts[os.path.relpath(path, evidence)] = digest(path)
    verdict = {
        "status": "passed", "source": source, "runtime": after, "go": go_version.group(1),
        "paired_repetitions": len(ORDER), "binary_sha256": binary_hashes, "evidence_sha256": artifacts,
    }
    (evidence / "verdict.json").write_text(json.dumps(verdict, indent=2, sort_keys=True) + "\n")


def fixture_secrets():
    envfile = Path(os.environ["OF33C_ENVDIR"]) / ".env"
    values = [value for key, value in (
        line.split("=", 1) for line in envfile.read_text().splitlines()
    ) if key in ("OF33C_PG_PASSWORD", "OF33C_JWT_SECRET")]
    values.extend(os.environ.get(key) for key in ("WP33C_TOKEN", "WP315_FOREIGN_TOKEN"))
    return [value.encode() for value in values if value]


def redact_logs():
    data = sys.stdin.buffer.read()
    for secret in fixture_secrets():
        data = data.replace(secret, b"[redacted fixture credential]")
    sys.stdout.buffer.write(JWT.sub(b"[redacted bearer]", data))


if __name__ == "__main__":
    entry = sys.argv[1:] if len(sys.argv) > 1 else []
    if entry == ["source"]:
        print(json.dumps(revision(), sort_keys=True, indent=2))
    elif entry == ["runtime"]:
        print(json.dumps(runtime(), sort_keys=True, indent=2))
    elif entry == ["verify"]:
        verify()
    elif entry == ["redact-logs"]:
        redact_logs()
    else:
        sys.exit("usage: evidence.py source|runtime|verify|redact-logs")

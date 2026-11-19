"""Summarize or compare two completed WP3.15 capacity runs."""
import argparse
from hashlib import sha256
import json
from pathlib import Path
import re
from statistics import median
from subprocess import check_output, CalledProcessError
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evidence import ROOT, command, digest, read_json, require, check_benchmark

WORKLOAD = ("benchmark.py", "fixture.py", "published.py", "run.sh")
PARENT_FILES = {"../fingerprint.json", "../binary-sha256.txt", "../build.log"}
RESOURCE_KEYS = ("image_id", "nano_cpus", "cpu_quota", "memory_limit_bytes")


def samples(values):
    return {"min": min(values), "median": median(values), "max": max(values), "samples": values}


def metrics(benchmark):
    formats = {}
    for fmt in ("ndjson", "parquet"):
        trials = [t for pair in benchmark["pairs"] for t in pair["trials"] if t["format"] == fmt]
        formats[fmt] = {
            "write_seconds": samples([t["elapsed_seconds"] for t in trials]),
            "artifact_bytes": samples([t["publication"]["size_bytes"] for t in trials]),
            "query_seconds": samples(benchmark["queries"]["samples_seconds"][fmt]),
            **{f"{service}_peak_rss_kib": samples([t["memory"][service]["after"]["vmhwm_kib"] for t in trials])
               for service in ("dvs", "pbs")},
        }
    return formats


def load_run(root):
    verdict = read_json(root / "verdict.json")
    source, runtime = verdict["source"], verdict["runtime"]
    require(verdict["status"] == "passed" and source["dirty"] is False, "run requires a clean passing verdict")
    hashes = verdict["evidence_sha256"]
    required = {"benchmark.json", "source-before.json", "source-after.json", "runtime-after.json"}
    require(required <= hashes.keys(), "verdict is missing benchmark provenance")
    for relative, expected in hashes.items():
        path = (root / relative).resolve()
        require(path.is_relative_to(root) or (relative in PARENT_FILES and path == root.parent / Path(relative).name),
                f"invalid evidence path: {relative}")
        require(digest(path) == expected, f"evidence digest mismatch: {relative}")

    revision = source["head"]
    require(re.fullmatch("[0-9a-f]{40}", revision) is not None, "invalid recorded commit")
    require(command("git", "-C", str(ROOT), "rev-parse", revision + "^{tree}") == source["tree"],
            "recorded tree does not match the source commit")
    require(source == read_json(root / "source-before.json") == read_json(root / "source-after.json"),
            "source provenance mismatch")
    require(runtime == read_json(root / "runtime-after.json"), "runtime provenance mismatch")
    benchmark = read_json(root / "benchmark.json")
    check_benchmark(root, benchmark, runtime["services"])

    folder = Path(__file__).resolve().parent.relative_to(ROOT)
    profile = {
        "workload_sha256": {f: sha256(check_output(["git", "-C", str(ROOT), "show", f"{revision}:{folder / f}"])).hexdigest()
                            for f in WORKLOAD},
        "input_files": [{k: f[k] for k in ("logical_path", "sha256", "size_bytes", "record_count")}
                        for f in benchmark["source"]["files"]],
        "method": benchmark["method"],
        "environment": {key: runtime[key] for key in ("host", "docker", "build_image_id", "postgres", "reader", "settings", "stubs")},
        "service_resources": {name: {k: service[k] for k in RESOURCE_KEYS}
                              for name, service in runtime["services"].items()},
        "go": verdict["go"],
    }
    return {"source": source, "binary_sha256": verdict["binary_sha256"], "profile": profile, "metrics": metrics(benchmark)}


def compare(base, run):
    differing = [key for key, value in base["profile"].items() if value != run["profile"][key]]
    result = {"baseline": base, "candidate": run, "comparable": not differing, "differing_profile_fields": differing}
    if not differing:
        result["candidate_median_change_pct"] = {
            fmt: {metric: 100 * (sample["median"] / base["metrics"][fmt][metric]["median"] - 1)
                  for metric, sample in values.items()}
            for fmt, values in run["metrics"].items()
        }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("candidate", type=Path, help="completed WP3.15 evidence directory")
    parser.add_argument("--baseline", type=Path, help="earlier completed evidence directory")
    args = parser.parse_args()
    try:
        run = load_run(args.candidate.resolve(strict=True))
        result = compare(load_run(args.baseline.resolve(strict=True)), run) if args.baseline else run
    except (AssertionError, OSError, ValueError, KeyError, TypeError, CalledProcessError) as err:
        parser.exit(1, f"invalid benchmark evidence: {err}\n")
    print(json.dumps(result, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()

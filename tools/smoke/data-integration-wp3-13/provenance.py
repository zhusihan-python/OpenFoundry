"""Bind the live acceptance verdict to source, deployed binaries and artifacts."""

from hashlib import sha256
import json
import os
from pathlib import Path
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
CASES = ("publish", "empty", "invalid-row", "truncated-stage", "ack-replay", "dvs-rejection")
BINARIES = {
    "dvs": "dataset-versioning-service",
    "pbs": "pipeline-build-service",
}


def expect(label, actual, expected):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def digest(path):
    return sha256(path.read_bytes()).hexdigest()


def source_snapshot():
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=ROOT).decode()

    sources = git("ls-files", "-z", "--", *[
        str(HERE.with_name(f"data-integration-wp3-{wp}").relative_to(ROOT))
        for wp in ("3", "12", "13")
    ]).split("\0")
    statuses = git("status", "--porcelain=v1", "-z", "--no-renames", "--untracked-files=all").split("\0")
    changed = {entry[3:]: entry[:2] for entry in statuses if entry}
    paths = sorted({path for path in sources if path} | changed.keys())
    entries = {
        path: {
            "git_status": changed.get(path, ""),
            "sha256": digest(ROOT / path) if (ROOT / path).is_file() else None,
        }
        for path in paths
    }
    for source in HERE.iterdir():
        if source.suffix in (".py", ".sh", ".md"):
            expect(f"fingerprinted harness source {source.name}", str(source.relative_to(ROOT)) in entries, True)
    return {
        "head": git("rev-parse", "HEAD").strip(),
        "dirty": bool(changed),
        "entries": entries,
    }


def verify(evidence):
    def read(name):
        return json.loads((evidence / name).read_text())

    source = read("source-before.json")
    expect("unchanged source during smoke", read("source-after.json"), source)
    expect("final source fingerprint", source_snapshot(), source)
    build_manifest = json.loads((evidence.parent / "fingerprint.json").read_text())
    expect("build HEAD", build_manifest["head"], source["head"])
    for entry in build_manifest["entries"]:
        expect(f"build source {entry['path']}", source["entries"][entry["path"]]["sha256"], entry["sha256"])

    runtime = read("runtime-before.json")
    ending = read("runtime-after.json")
    expected_hashes = {
        service: digest(evidence.parent / "bin" / binary)
        for service, binary in BINARIES.items()
    }
    build_hashes = {}
    for line in (evidence.parent / "binary-sha256.txt").read_text().splitlines():
        value, filename = line.split(maxsplit=1)
        build_hashes[Path(filename).name] = value
    expect("compiled binary hashes", build_hashes,
           {binary: expected_hashes[service] for service, binary in BINARIES.items()})
    for current in (runtime, ending):
        for service, expected_hash in expected_hashes.items():
            expect(f"running {service} binary digest", current["containers"][service]["binary_sha256"], expected_hash)
        expect("DuckDB engine", current["reader"]["version"], "v1.4.4")
    # The ACK scenario restarts the PBS process in its existing container.
    for service in ("dvs", "pbs", "postgres", "faultproxy"):
        expect(f"{service} container identity", ending["containers"][service]["id"], runtime["containers"][service]["id"])
    expect("recorded PBS process restart",
           runtime["containers"]["pbs"]["started_at"] != ending["containers"]["pbs"]["started_at"], True)
    artifacts = {}
    for case in CASES:
        result_path = f"{case}.json"
        result = read(result_path)
        expect(f"{case} acceptance", result["status"], "passed")
        artifacts[result_path] = digest(evidence / result_path)
        if case in ("publish", "empty", "ack-replay"):
            path = f"{case}.parquet"
            artifacts[path] = digest(evidence / path)
            expect(f"{case} evidence Parquet digest", artifacts[path], result["parquet"]["sha256"])

    # The smoke stack's auth and database credentials must stay in its own .env.
    env_path = Path(os.environ["OF33C_ENVDIR"]) / ".env"
    secrets = [
        value.encode() for key, value in (
            line.split("=", maxsplit=1) for line in env_path.read_text().splitlines()
        ) if key in ("OF33C_JWT_SECRET", "OF33C_PG_PASSWORD")
    ]
    expect("found both fixture secrets for evidence check", len(secrets), 2)
    for path in evidence.parent.rglob("*"):
        if path.is_file() and path.parent.name != "bin":
            raw = path.read_bytes()
            expect(f"evidence credential check for {path.name}",
                   any(secret in raw for secret in secrets)
                   or bool(re.search(rb"eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}", raw)),
                   False)

    for name in ("source-before.json", "source-after.json", "runtime-before.json",
                 "runtime-after.json", "proxy-selftest.log"):
        artifacts[name] = digest(evidence / name)
    for name in ("fingerprint.json", "binary-sha256.txt", "build.log", "fault-proxy/events.jsonl"):
        artifacts[f"../{name}"] = digest(evidence.parent / name)
    verdict = {
        "status": "passed",
        "source_revision": source["head"],
        "source_dirty": source["dirty"],
        "source_fingerprint": artifacts["source-before.json"],
        "binary_sha256": expected_hashes,
        "duckdb": runtime["reader"],
        "cases": list(CASES),
        "evidence_sha256": artifacts,
    }
    (evidence / "verdict.json").write_text(json.dumps(verdict, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    if sys.argv[1:] == ["snapshot"]:
        print(json.dumps(source_snapshot(), indent=2, sort_keys=True))
    elif sys.argv[1:] == ["verify"]:
        verify(Path(os.environ["WP33C_EVIDENCE"]))
    else:
        sys.exit("usage: provenance.py snapshot|verify")

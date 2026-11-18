"""Bind scheduled default-write acceptance to deployed service and tick binaries."""

from hashlib import file_digest
import json
import os
from pathlib import Path
import re
import subprocess
import sys

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
CASES = (
    "default-parquet", "replayed-parquet", "empty-parquet", "explicit-ndjson",
    "legacy-ndjson", "invalid-row", "dvs-rejection", "manual-graph",
)
BINARIES = {
    "dvs": "dataset-versioning-service",
    "pbs": "pipeline-build-service",
    "tick": "schedules-tick",
}


def digest(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def equal(label, got, want):
    if got != want:
        raise AssertionError(f"{label}: expected {want!r}, got {got!r}")


def snapshot():
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=ROOT, text=True)

    inherited = (
        "data-integration-wp3-2", "data-integration-wp3-3", "data-integration-wp3-12",
        "data-integration-wp3-13", HERE.name, "business-platform-p1/mint_token.py",
    )
    clean = {x for x in git("ls-files", "-z", "--", "libs/schedule-events/schema",
                            *[f"tools/smoke/{path}" for path in inherited]).split("\0") if x}
    changed = {x[3:]: x[:2] for x in git("status", "-z", "--porcelain=v1",
                                         "--untracked-files=all", "--no-renames").split("\0") if x}
    for p in HERE.iterdir():
        if p.is_file():
            equal(f"smoke source included {p.name}", str(p.relative_to(ROOT)) in (clean | changed.keys()), True)
    paths = clean | changed.keys()
    return {
        "head": git("rev-parse", "HEAD").strip(),
        "dirty": bool(changed),
        "files": {path: {"status": changed.get(path, ""),
                         "sha256": digest(ROOT / path) if (ROOT / path).is_file() else None}
                  for path in sorted(paths)},
    }


def verify():
    evidence = Path(os.environ["WP33C_EVIDENCE"])
    build = evidence.parent
    binaries_dir = Path(os.environ["WP314_BIN"])

    def read(path):
        return json.loads(path.read_text())

    source = read(evidence / "source-before.json")
    for label, observed in (("source after acceptance", read(evidence / "source-after.json")),
                            ("source at verdict", snapshot())):
        equal(f"{label} HEAD", observed["head"], source["head"])
        changes = sorted(name for name in observed["files"].keys() | source["files"].keys()
                         if observed["files"].get(name) != source["files"].get(name))
        equal(f"{label} changed paths", changes, [])
    manifest = read(build / "fingerprint.json")
    equal("service build HEAD", manifest["head"], source["head"])
    for entry in manifest["entries"]:
        equal(f"service build fingerprint {entry['path']}", entry["sha256"], source["files"][entry["path"]]["sha256"])
    start = read(evidence / "runtime-before.json")
    finish = read(evidence / "runtime-after.json")
    deployed = {service: digest(binaries_dir / name) for service, name in BINARIES.items()}
    equal("tick build hash", (evidence / "tick-build-sha256.txt").read_text().split()[0], deployed["tick"])
    for runtime in (start, finish):
        equal("deployed binary digests", runtime["binary_sha256"], deployed)
        equal("DuckDB reader version", runtime["reader"]["version"], "v1.4.4")
        equal("real schedule producer", runtime["trigger_source"], "schedules-tick -> PostgreSQL WAL -> Debezium -> Kafka")
    for service in start["services"]:
        equal(f"{service} container identity", start["services"][service]["id"], finish["services"][service]["id"])
    equal("PBS actually restarted", start["services"]["pbs"]["started_at"] != finish["services"]["pbs"]["started_at"], True)
    for case in CASES:
        result = read(evidence / f"{case}.json")
        equal(f"required case {case}", result["status"], "passed")
        if case in ("default-parquet", "replayed-parquet", "empty-parquet"):
            parquet = result["parquet"]
            equal(f"{case} archived Parquet hash", digest(evidence / parquet["path"]), parquet["sha256"])

    envfile = Path(os.environ["OF33C_ENVDIR"]) / ".env"
    secrets = [value.encode() for key, value in (
        line.split("=", 1) for line in envfile.read_text().splitlines()
    ) if key in ("OF33C_PG_PASSWORD", "OF33C_JWT_SECRET")]
    equal("number of fixture secrets", len(secrets), 2)
    secrets.extend(os.environ[key].encode() for key in ("WP33C_TOKEN", "WP314_CIPHER_KEY"))
    jwt = re.compile(rb"eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}")
    artifacts = {}
    for path in sorted(evidence.rglob("*")):
        if path.is_file():
            raw = path.read_bytes()
            equal(f"evidence hygiene for {path.name}", any(secret in raw for secret in secrets) or bool(jwt.search(raw)), False)
            artifacts[str(path.relative_to(evidence))] = digest(path)
    verdict = {
        "status": "passed", "source_revision": source["head"], "source_dirty": source["dirty"],
        "source_fingerprint": artifacts["source-before.json"], "binary_sha256": deployed,
        "cases": list(CASES), "runtime": finish, "evidence_sha256": artifacts,
    }
    (evidence / ".verified.json").write_text(json.dumps(verdict, sort_keys=True, indent=2) + "\n")


if __name__ == "__main__":
    if sys.argv[1:] == ["snapshot"]:
        print(json.dumps(snapshot(), sort_keys=True, indent=2))
    elif sys.argv[1:] == ["verify"]:
        verify()
    else:
        sys.exit("usage: evidence.py snapshot|verify")

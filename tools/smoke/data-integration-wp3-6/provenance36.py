"""Bind the WP3.6 acceptance to its source revision and deployed binaries, and
re-check its claims from the artifacts alone once the stack is gone."""

from hashlib import sha256
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import duckdb

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.extend(map(str, (HERE, HERE.with_name("data-integration-wp3-12"))))
import reader36
import verify as reference  # WP3.12 golden orders.v1

CASES = ("historical-schema", "bound-publish", "switched-input")
BUSINESS = ("schema", "scan", "stage", "commit")
SOURCES = ("tools/smoke/data-integration-wp3-3", "tools/smoke/data-integration-wp3-12",
           "tools/smoke/data-integration-wp3-13", "tools/smoke/data-integration-wp3-6",
           "tools/smoke/business-platform-p1/mint_token.py")
BINARIES = {"dvs": "dataset-versioning-service", "pbs": "pipeline-build-service"}


def expect(label, actual, expected):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def digest(path):
    return sha256(path.read_bytes()).hexdigest()


def source_snapshot():
    def git(*args):
        return subprocess.check_output(["git", *args], cwd=ROOT).decode()

    sources = git("ls-files", "-z", "--", *SOURCES).split("\0")
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
        if source.suffix in (".py", ".sh", ".md", ".go"):
            expect(f"fingerprinted harness source {source.name}", str(source.relative_to(ROOT)) in entries, True)
    return {"head": git("rev-parse", "HEAD").strip(), "dirty": bool(changed), "entries": entries}


def recheck(evidence, acceptance):
    """Re-derive the driver's claims from downloaded artifacts only."""
    orders = reader36.describe(evidence / "wp36-input.parquet")
    expect("orders.v1 physical schema", [tuple(column) for column in orders["schema"]], reference.PARQUET_SCHEMA)
    expect("bound expectation is the orders.v1 content hash",
           acceptance["expected_schema_hash"], orders["content_hash"])
    bad = reader36.describe(evidence / "fixture" / "orders-amount-string.parquet")
    expect("incompatible sample differs only in the amount type", bad["fields"],
           [{**field, "field_type": "STRING"} if field["name"] == "amount" else field for field in orders["fields"]])
    expect("incompatible content hash differs", bad["content_hash"] == orders["content_hash"], False)
    bound = reader36.describe(evidence / "wp36-bound-reader.parquet")
    expect("published schema equals the pinned input schema", bound["fields"], orders["fields"])
    expect("published rows are the pinned input rows", bound["rows"], orders["rows"])
    expect("published rows are the golden orders",
           sorted(bound["rows"], key=lambda row: row[0]),
           sorted((list(row) for row in reference.EXPECTED_ORDERS), key=lambda row: row[0]))
    digests = {name: digest(evidence / name) for name in
               ("wp36-bound.parquet", "wp36-bound-reader.parquet", "wp36-after-rejection.parquet")}
    expect("published, independently read and re-downloaded bytes are identical", len(set(digests.values())), 1)
    legacy = acceptance["legacy_schema_answers"]
    created = acceptance["bound_publish"]

    def added(routes, status):
        answers = sorted(answer for route, answer in routes if route == "schema")
        expect(f"exactly one schema precondition answered {status}", answers.count(status), 1)
        answers.remove(status)
        expect("the other schema reads are the resolver's current-schema lookups", answers, legacy[:len(answers)])

    created_routes = [route for route in created["create_events"] if route[0] in BUSINESS]
    added(created_routes, 200)
    expect("bound CreateBuild touched no scan, upload or commit route",
           [route for route, _ in created_routes if route != "schema"], [])
    executed = [route for route in created["execute_events"] if route[0] in BUSINESS]
    expect("execution proved the schema before any scan or upload", executed[0], ["schema", 200])
    expect("the bound run scanned, uploaded and committed",
           [route in [event[0] for event in executed] for route in ("scan", "stage", "commit")], [True, True, True])
    refused = acceptance["switched_input"]
    expect("switched input refused with HTTP 412 and the top-level code",
           (refused["http_status"], refused["error"]), (412, "batched_input_schema_mismatch"))
    added(refused["business_events"], 412)
    expect("no scan, upload or commit after the refusal",
           [route for route, _ in refused["business_events"] if route != "schema"], [])
    expect("output unchanged and re-download identical",
           (refused["output_unchanged"], refused["redownload"]["sha256"]),
           (True, digests["wp36-bound-reader.parquet"]))
    return {"orders_content_hash": orders["content_hash"], "incompatible_content_hash": bad["content_hash"],
            "artifact_sha256": digests}


def verify(evidence):
    def read(name):
        return json.loads((evidence / name).read_text())

    source = read("source-before.json")
    expect("clean checkout", source["dirty"], False)
    expect("unchanged source during the smoke", read("source-after.json"), source)
    expect("final source fingerprint", source_snapshot(), source)
    build = json.loads((evidence.parent / "fingerprint.json").read_text())
    expect("build HEAD", build["head"], source["head"])
    for entry in build["entries"]:
        expect(f"build source {entry['path']}", source["entries"][entry["path"]]["sha256"], entry["sha256"])
    hashes = {service: digest(evidence.parent / "bin" / binary) for service, binary in BINARIES.items()}
    compiled = {Path(name).name: value for value, name in
                (line.split(maxsplit=1) for line in (evidence.parent / "binary-sha256.txt").read_text().splitlines())}
    expect("compiled binary hashes", compiled, {binary: hashes[service] for service, binary in BINARIES.items()})
    runtime, ending = read("runtime-before.json"), read("runtime-after.json")
    for service, expected in hashes.items():
        for current in (runtime, ending):
            expect(f"running {service} binary digest", current["containers"][service]["binary_sha256"], expected)
    expect("DuckDB engine", (runtime["reader"]["version"], duckdb.__version__), ("v1.4.4", reference.DUCKDB_VERSION))
    for service in ("dvs", "pbs", "postgres", "faultproxy"):
        expect(f"{service} ran undisturbed", ending["containers"][service], runtime["containers"][service])
    acceptance = read("wp36-acceptance.json")
    expect("acceptance status", acceptance["status"], "passed")
    expect("case roster", tuple(acceptance["cases"]), CASES)
    for case in CASES:
        expect(f"{case} result", read(f"{case}.json")["status"], "passed")

    # The stack's credentials must stay in its own .env.
    env_path = Path(os.environ["OF33C_ENVDIR"]) / ".env"
    secrets = [value.encode() for key, value in
               (line.split("=", maxsplit=1) for line in env_path.read_text().splitlines())
               if key in ("OF33C_JWT_SECRET", "OF33C_PG_PASSWORD")]
    expect("found both fixture secrets for the evidence check", len(secrets), 2)
    artifacts = {}
    for path in sorted(evidence.parent.rglob("*")):
        if path.is_file() and path.parent.name != "bin":
            raw = path.read_bytes()
            expect(f"evidence credential check for {path.name}",
                   any(secret in raw for secret in secrets)
                   or bool(re.search(rb"eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}", raw)),
                   False)
            artifacts[str(path.relative_to(evidence.parent))] = digest(path)
    verdict = {
        "status": "passed", "source_revision": source["head"], "source_dirty": source["dirty"],
        "binary_sha256": hashes, "duckdb": runtime["reader"], "cases": list(CASES),
        "recheck": recheck(evidence, acceptance), "evidence_sha256": artifacts,
    }
    (evidence / "verdict.json").write_text(json.dumps(verdict, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    if sys.argv[1:] == ["snapshot"]:
        print(json.dumps(source_snapshot(), indent=2, sort_keys=True))
    elif sys.argv[1:] == ["verify"]:
        verify(Path(os.environ["WP33C_EVIDENCE"]))
    else:
        sys.exit("usage: provenance36.py snapshot|verify")

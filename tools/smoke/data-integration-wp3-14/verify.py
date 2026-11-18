"""Bind the real acceptance checks to a clean source tree and running binaries."""

from hashlib import file_digest
import json
from pathlib import Path
import re
import subprocess
import sys

from duckdb import __version__ as duckdb_version


def checksum(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def main():
    evidence, build, staging = map(Path, sys.argv[1:4])
    project, gateway, revision, tree = sys.argv[4:]
    fingerprint = read_json(build / "fingerprint.json")
    assert fingerprint["head"] == revision
    cases = ("committed-pages", "preview-and-empty", "rejections", "pbs-replay", "browser")
    for case in cases:
        assert read_json(evidence / f"{case}.json")["status"] == "passed", case
    scans = read_json(evidence / "committed-pages.json")
    assert scans["rows"] == 1028 and len(scans["pages"]) > 2
    browser = read_json(evidence / "browser.json")
    assert "Chrome/" in browser["browser"] and browser["space_id"] > 0
    assert len(browser["first"]["ids"]) > 0 and len(browser["next"]["ids"]) > 0
    assert not browser["empty"]["ids"]
    assert (evidence / "historical-preview.png").stat().st_size > 0
    assert duckdb_version == "1.4.4"

    binaries = {}
    for container, executable, path in (
        (f"{project}-dvs", "dataset-versioning-service", "/service"),
        (f"{project}-pbs", "pipeline-build-service", "/service"),
        (gateway, "edge-gateway-service", "/binaries/edge-gateway-service"),
    ):
        expected = checksum(build / "bin" / executable)
        observed = docker("exec", container, "sha256sum", path).split()[0]
        assert observed == expected, executable
        identity, image, image_id = docker("inspect", "--format", "{{.Id}}|{{.Config.Image}}|{{.Image}}", container).split("|")
        binaries[executable] = {"sha256": observed, "container": identity, "image": image, "image_id": image_id}
    secrets = [value.encode() for key, value in (
        line.split("=", 1) for line in (staging / ".env").read_text().splitlines()
    ) if key in ("OF33C_JWT_SECRET", "OF33C_PG_PASSWORD")]
    assert len(secrets) == 2 and all(secrets)
    secrets.append((staging / "browser-token").read_bytes().strip())
    jwt = re.compile(rb"eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}")
    fingerprints = {}
    for path in sorted(evidence.iterdir()):
        if path.is_file():
            raw = path.read_bytes()
            assert not any(secret in raw for secret in secrets) and not jwt.search(raw), path.name
            fingerprints[path.name] = checksum(path)
    postgres = docker("exec", f"{project}-postgres", "psql", "-AtU", "openfoundry", "-d", "wp33c_dvs", "-c", "SHOW server_version")
    verdict = {
        "status": "passed",
        "source_revision": revision,
        "source_tree": tree,
        "source_dirty": False,
        "build_fingerprint_sha256": checksum(build / "fingerprint.json"),
        "cases": list(cases),
        "postgres": postgres,
        "duckdb": duckdb_version,
        "docker": docker("version", "--format", "{{.Server.Version}}"),
        "node": subprocess.check_output(["node", "--version"], text=True).strip(),
        "binaries": binaries,
        "stubs": ["Vite identity bootstrap and profile only"],
        "evidence_sha256": fingerprints,
    }
    (staging / "verdict.json").write_text(json.dumps(verdict, sort_keys=True, indent=2) + "\n")


if __name__ == "__main__":
    main()

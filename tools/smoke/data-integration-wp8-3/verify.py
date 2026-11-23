"""Bind the real WP8.3 acceptance checks to a clean source tree and running binaries."""

from hashlib import file_digest, sha256
import json
from pathlib import Path
import re
import subprocess
import sys

from duckdb import __version__ as duckdb_version


MISSING_PREVIEW = (
    "dataset_file_bytes_missing: the versioned file has no readable backing bytes at its registered storage path"
)


def checksum(path):
    with path.open("rb") as stream:
        return file_digest(stream, "sha256").hexdigest()


def read_json(path):
    return json.loads(path.read_text())


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def verify_schema_diff(evidence, browser, schema):
    old, next_pin, tip = (schema[key] for key in ("old", "next", "tip"))
    pins = (old, next_pin, tip)
    assert len({pin["transaction"] for pin in pins}) == 3
    expected_fields = (
        [("order_id", "LONG"), ("amount", "LONG"), ("legacy_field", "STRING")],
        [("order_id", "LONG"), ("amount", "STRING"), ("added_field", "BOOLEAN")],
        [("order_id", "LONG"), ("amount", "DOUBLE"), ("head_only", "STRING")],
    )
    for version, (pin, fields, path) in enumerate(zip(pins, expected_fields, ("a.parquet", "b.parquet", "c.parquet"),
                                                     strict=True), start=1):
        assert (pin["dataset"], pin["branch"], pin["version"]) == (old["dataset"], "main", version)
        assert [(f["name"], f["field_type"]) for f in pin["fields"]] == fields
        assert all(type(f["nullable"]) is bool for f in pin["fields"])
        file = pin["file"]
        artifact = evidence / f"schema-diff-v{version}.parquet"
        assert file["artifact"] == artifact.name
        assert (file["transaction"], file["path"], file["size_bytes"], file["sha256"]) == (
            pin["transaction"], path, artifact.stat().st_size, checksum(artifact),
        )
    assert schema["restored_file_sha256"] == old["file"]["sha256"]
    before, after_head, missing, restored = (schema[key] for key in ("before", "after_head", "missing", "restored"))
    for side, state, pin in (("a", before, old), ("b", before, next_pin), ("head", after_head, tip)):
        assert state[side] == {
            "http_status": 200, "dataset_id": pin["dataset"], "branch": pin["branch"],
            "version": pin["version"], "transaction_id": pin["transaction"], "fields": pin["fields"],
        }
    assert before == restored == {side: after_head[side] for side in ("a", "b")}
    assert missing["b"] == before["b"]
    assert missing["a"] == {"http_status": 422, "error": MISSING_PREVIEW}

    views = browser["schema_diff"]
    services = schema["service_observed_unix_ms"]
    browser_observed = schema["browser_observed_unix_ms"]
    expected_rows = [["+", "added_field", "BOOLEAN"], ["−", "legacy_field", "STRING"],
                     ["~", "amount", "LONG → STRING"]]
    most_recent = 0
    for phase, screenshot in (
        ("before", "schema-diff-before.png"),
        ("after_head", "schema-diff-after-head.png"),
        ("missing", "schema-diff-unavailable.png"),
        ("restored", "schema-diff-restored.png"),
    ):
        view = views[phase]
        assert most_recent <= services[phase] <= view["navigation_unix_ms"], phase
        for side, pin in (("a", old), ("b", next_pin)):
            selector = view["selects"][side]
            assert selector["value"] == f"transaction:{pin['transaction']}", (phase, side)
            assert all(f"transaction:{p['transaction']}" in selector["options"] for p in pins[:2]), (phase, side)
            assert (f"transaction:{tip['transaction']}" in selector["options"]) == (phase != "before"), (phase, side)
            request = view["requests"][side]
            assert request["path"] == f"/api/v1/datasets/{pin['dataset']}/schema", (phase, side)
            assert (request["version"], request["branch"]) == (pin["version"], pin["branch"]), (phase, side)
            assert request["status"] == (422 if phase == "missing" and side == "a" else 200), (phase, side)
            assert request["transferred_bytes"] > 0, (phase, side)
            assert (view["navigation_unix_ms"] <= request["started_unix_ms"] < request["completed_unix_ms"]
                    <= view["captured_at_unix_ms"]), (phase, side)
        if phase == "missing":
            assert view["rows"] is None
            assert "Side A unavailable" in view["alert"] and MISSING_PREVIEW in view["alert"]
            assert "Side B unavailable" not in view["alert"]
        else:
            assert view["rows"] == expected_rows and view["alert"] is None, phase
        assert view["capture"] == screenshot, phase
        assert (evidence / screenshot).read_bytes().startswith(b"\x89PNG\r\n\x1a\n"), phase
        most_recent = view["captured_at_unix_ms"]
        if phase != "restored":
            assert most_recent == browser_observed[phase], phase


def verify_row_match(evidence, browser, match):
    base, target, tip, duplicate = (match[name] for name in ("base", "target", "tip", "duplicate"))
    assert [(pin["branch"], pin["version"]) for pin in (base, target, tip, duplicate)] == [
        ("main", 1), ("main", 2), ("main", 3), ("main", 1),
    ]
    assert len({pin["transaction_id"] for pin in (base, target, tip, duplicate)}) == 4
    files = match["files"]
    for name, pin, paths in (
        ("base", base, ["a.parquet", "b.parquet"]),
        ("target", target, ["a.parquet", "b.parquet"]),
        ("tip", tip, ["head.parquet"]),
        ("duplicate", duplicate, ["a.parquet", "b.parquet"]),
    ):
        assert [file["path"] for file in files[name]] == paths
        for file in files[name]:
            artifact = evidence / Path(file["artifact"]).name
            assert artifact.name == file["artifact"]
            assert (file["transaction"], file["size_bytes"], file["sha256"]) == (
                pin["transaction_id"], artifact.stat().st_size, checksum(artifact),
            )
    assert files["base"][0]["sha256"] == files["target"][0]["sha256"]
    assert files["base"][1]["sha256"] != files["target"][1]["sha256"]
    assert files["duplicate"][0]["sha256"] == files["duplicate"][1]["sha256"]

    expected = match["expected"]
    assert (expected["key"], expected["base"], expected["target"], expected["changes_truncated"]) == (
        "order_id", base, target, False,
    )
    assert expected["counts"] == {"added": 1, "removed": 1, "modified": 1, "unchanged": 130}
    assert [(row["key"], row["kind"]) for row in expected["changes"]] == [
        ("9007199254740992", "added"), ("9007199254740993", "modified"), ("9007199254740995", "removed"),
    ]
    added, modified, removed = expected["changes"]
    assert "before" not in added and added["after"]["order_id"] == added["key"]
    assert "after" not in removed and removed["before"]["order_id"] == removed["key"]
    assert modified["before"]["order_id"] == modified["after"]["order_id"] == modified["key"]
    assert (modified["before"]["amount"], modified["after"]["amount"]) == ("12345678901234.5000", "12345678901234.5001")
    assert match["before"] == match["after"] == {"http_status": 200, **expected}
    for name, status, prefix in (
        ("unknown_key", 400, "dataset_row_compare_key_invalid"),
        ("transaction_mismatch", 422, "dataset_row_compare_transaction_mismatch"),
        ("duplicate", 422, "dataset_row_compare_duplicate_key"),
    ):
        rejected = match["rejections"][name]
        assert set(rejected) == {"http_status", "error"}
        assert rejected["http_status"] == status and rejected["error"].split(":", 1)[0] == prefix

    views = browser["row_match"]
    before_at = views["before"]["captured_at_unix_ms"]
    after_at = views["after_head"]["captured_at_unix_ms"]
    assert match["browser_observed_unix_ms"] == {"before": before_at, "after_head": after_at}
    assert before_at <= match["service_observed_unix_ms"]["after_head"]
    for name, key, status, screenshot in (
        ("before", "order_id", 200, "row-match-before.png"),
        ("after_head", "order_id", 200, "row-match-after-head.png"),
        ("invalid_key", "wrong_key", 400, "row-match-invalid-key.png"),
    ):
        view = views[name]
        request = view["request"]
        assert request["path"] == f"/api/v1/datasets/{expected['dataset_id']}/compare/rows"
        assert request["params"] == {
            "key": key, "limit": "50", "base_branch": base["branch"], "base_version": str(base["version"]),
            "base_transaction": base["transaction_id"], "target_branch": target["branch"],
            "target_version": str(target["version"]), "target_transaction": target["transaction_id"],
        }
        assert request["status"] == status and request["transferred_bytes"] > 0
        assert (view["navigation_unix_ms"] <= request["started_unix_ms"] < request["completed_unix_ms"]
                <= view["captured_at_unix_ms"])
        if name != "invalid_key":
            assert match["service_observed_unix_ms"][name] <= view["navigation_unix_ms"]
        else:
            assert after_at <= request["started_unix_ms"]
        for which, pin in (("A", base), ("B", target)):
            selector = view["selectors"][which]
            assert selector["value"] == f"transaction:{pin['transaction_id']}"
            assert all(f"transaction:{pin['transaction_id']}" in selector["options"] for pin in (base, target))
            assert (f"transaction:{tip['transaction_id']}" in selector["options"]) == (name != "before")
        if status == 200:
            assert view["table"] and view["rows"] == expected["changes"] and view["error"] is None
            assert view["summary"] == "1 added · 1 removed · 1 modified · 130 unchanged"
        else:
            assert not view["table"] and view["rows"] == [] and view["summary"] is None
            assert "dataset_row_compare_key_invalid" in view["error"]
        assert view["capture"] == screenshot
        assert (evidence / screenshot).read_bytes().startswith(b"\x89PNG\r\n\x1a\n")


def verify_range_diff(evidence, browser, report):
    expected = report["expected"]
    base, target, tip = expected["base"], expected["target"], report["tip"]
    assert [(pin["branch"], pin["version"]) for pin in (base, target, tip)] == [("main", 1), ("main", 2), ("main", 3)]
    assert len({pin["transaction_id"] for pin in (base, target, tip)}) == 3
    assert report["rows_per_side"] == 1300
    files = report["files"]
    for side, pin, paths in (
        ("base", base, ["a.parquet", "b.parquet"]),
        ("target", target, ["a.parquet", "b.parquet"]),
        ("tip", tip, ["head.parquet"]),
    ):
        assert [file["path"] for file in files[side]] == paths
        for file in files[side]:
            artifact = evidence / Path(file["artifact"]).name
            assert artifact.name == file["artifact"]
            assert (file["transaction"], file["size_bytes"], file["sha256"]) == (
                pin["transaction_id"], artifact.stat().st_size, checksum(artifact),
            )
    assert len({file["sha256"] for side in ("base", "target") for file in files[side]}) == 4
    assert expected["counts"] == {"added": 60, "removed": 60, "modified": 60, "unchanged": 1180}
    assert expected["key"] == "order_id" and expected["changes_truncated"] is False
    changes = expected["changes"]
    keys = [row["key"] for row in changes]
    assert len(keys) == len(set(keys)) == 180 and keys == sorted(keys)
    for kind in ("added", "removed", "modified"):
        assert sum(row["kind"] == kind for row in changes) == expected["counts"][kind]
    added, modified = changes[-2:]
    assert (added["key"], added["kind"], added["after"]["order_id"]) == ("9007199254740992", "added", "9007199254740992")
    assert (modified["key"], modified["kind"]) == ("9007199254740993", "modified")
    assert modified["before"]["order_id"] == modified["after"]["order_id"] == modified["key"]
    assert (modified["before"]["amount"], modified["after"]["amount"]) == ("12345678901234.5000", "12345678901234.5001")

    limit = report["limit"]
    assert limit == 50
    service_pages, views = report["pages"], browser["range_diff"]
    assert len(service_pages) == len(views["pages"]) == 4
    assert service_pages[0]["completed_unix_ms"] <= report["ready_unix_ms"] <= report["browser_first_unix_ms"]
    assert report["browser_first_unix_ms"] <= report["head_unix_ms"] <= service_pages[1]["started_unix_ms"]
    params = {
        "key": expected["key"], "limit": limit, "base_branch": base["branch"], "base_version": base["version"],
        "base_transaction": base["transaction_id"], "target_branch": target["branch"],
        "target_version": target["version"], "target_transaction": target["transaction_id"],
    }
    cursor, seen = None, set()
    for number, page in enumerate(service_pages, start=1):
        start = (number - 1) * limit
        query = {**params, **({"cursor": cursor} if cursor else {})}
        assert page["query"] == query and page["status"] == 200
        assert page["started_unix_ms"] <= page["completed_unix_ms"]
        body = page["body"]
        assert {key: value for key, value in body.items() if key != "next_cursor"} == {
            **expected, "changes": changes[start:start + limit],
        }
        cursor = body.get("next_cursor")
        if number < len(service_pages):
            assert isinstance(cursor, str) and cursor and cursor not in seen
            seen.add(cursor)
        else:
            assert "next_cursor" not in body
    assert [change for page in service_pages for change in page["body"]["changes"]] == changes

    first_cursor = service_pages[0]["body"]["next_cursor"]
    for name, error, overrides in (
        ("malformed", "dataset_row_compare_cursor_invalid", {"cursor": "not/a/valid/cursor"}),
        ("key", "dataset_row_compare_cursor_mismatch", {"cursor": first_cursor, "key": "amount"}),
        ("limit", "dataset_row_compare_cursor_mismatch", {"cursor": first_cursor, "limit": 49}),
        ("version", "dataset_row_compare_cursor_mismatch",
         {"cursor": first_cursor, "target_version": tip["version"], "target_transaction": tip["transaction_id"]}),
    ):
        rejected = report["rejections"][name]
        assert rejected["query"] == {**params, **overrides} and rejected["status"] == 400
        assert set(rejected["body"]) == {"error"} and rejected["body"]["error"].split(":", 1)[0] == error

    def check_browser_page(number, view, cursor):
        offset = (number - 1) * limit
        assert view["page_number"] == number and view["table"] and view["error"] is None
        assert view["summary"] == "60 added · 60 removed · 60 modified · 1180 unchanged"
        assert view["previous_enabled"] is (number > 1)
        assert view["next_enabled"] is (number < len(views["pages"]))
        assert view["rows"] == changes[offset:offset + limit]
        for side, pin in (("A", base), ("B", target)):
            selector = view["selectors"][side]
            assert selector["value"] == f"transaction:{pin['transaction_id']}"
        request = view["request"]
        assert request["path"] == f"/api/v1/datasets/{expected['dataset_id']}/compare/rows"
        query = {key: str(value) for key, value in params.items()}
        if cursor:
            query["cursor"] = cursor
        assert (request["params"], request["status"]) == (query, 200) and request["transferred_bytes"] > 0
        assert (views["pages"][0]["navigation_unix_ms"] == view["navigation_unix_ms"]
                <= view["action_unix_ms"] <= request["started_unix_ms"] < request["completed_unix_ms"]
                <= view["captured_at_unix_ms"])
        if number > 1:
            assert request["started_unix_ms"] >= report["head_unix_ms"]

    browser_cursors = set()
    for number, view in enumerate(views["pages"], start=1):
        token = view["request"]["params"].get("cursor")
        assert bool(token) == (number > 1)
        assert token not in browser_cursors
        browser_cursors.add(token)
        check_browser_page(number, view, token)
    assert [change for view in views["pages"] for change in view["rows"]] == changes
    assert report["ready_unix_ms"] <= views["pages"][0]["navigation_unix_ms"]
    assert views["pages"][0]["captured_at_unix_ms"] == report["browser_first_unix_ms"]
    assert views["pages"][-1]["captured_at_unix_ms"] <= report["browser_traversed_unix_ms"]
    check_browser_page(3, views["previous"], views["pages"][2]["request"]["params"]["cursor"])
    assert views["previous"]["captured_at_unix_ms"] == report["browser_traversed_unix_ms"]
    for view, capture in (
        (views["pages"][0], "range-diff-first.png"),
        (views["pages"][-1], "range-diff-last.png"),
        (views["previous"], "range-diff-previous.png"),
    ):
        assert view["capture"] == capture
        assert (evidence / capture).read_bytes().startswith(b"\x89PNG\r\n\x1a\n")

    memory = report["memory"]
    prior, before, after = memory["prior"], memory["before"], memory["after"]
    for snap in (prior, before, after):
        assert snap["pid"] == "1" and snap["oom_killed"] is False and snap["starttime_ticks"] > 0
        assert 0 < snap["vmrss_kib"] <= snap["vmhwm_kib"]
    assert prior["container_id"] == before["container_id"]
    assert prior["started_at"] != before["started_at"] and prior["starttime_ticks"] != before["starttime_ticks"]
    for name in ("container_id", "pid", "started_at", "starttime_ticks", "restart_count"):
        assert before[name] == after[name]
    assert before["vmhwm_kib"] <= after["vmhwm_kib"]
    assert report["browser_traversed_unix_ms"] <= memory["started_unix_ms"] <= memory["completed_unix_ms"]
    assert memory["duration_ns"] > 0 and memory["page_count"] == len(service_pages)
    assert memory["payload_sha256"] == sha256(
        json.dumps(changes, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def main():
    evidence, build, staging = map(Path, sys.argv[1:4])
    project, gateway, revision, tree = sys.argv[4:]
    fingerprint = read_json(build / "fingerprint.json")
    assert fingerprint["head"] == revision
    cases = ("schema-diff", "row-match", "range-diff", "browser")
    for case in cases:
        assert read_json(evidence / f"{case}.json")["status"] == "passed", case
    browser = read_json(evidence / "browser.json")
    assert "Chrome/" in browser["browser"] and browser["space_id"] > 0
    verify_schema_diff(evidence, browser, read_json(evidence / "schema-diff.json"))
    verify_row_match(evidence, browser, read_json(evidence / "row-match.json"))
    range_diff = read_json(evidence / "range-diff.json")
    verify_range_diff(evidence, browser, range_diff)
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
    assert range_diff["memory"]["before"]["container_id"] == binaries["dataset-versioning-service"]["container"]
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

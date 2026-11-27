"""Replay the archived Amazon v3 transaction through the sync client against a real Cassandra store.

Regression for duplicate SettlementLine objects of the generic create route: replaying the
archived Amazon v3 transaction through ``POST /api/v1/ontology/types/SettlementLine/objects/``
stores every settlement line once per delivery, as it did for the ProductCost rows before
#90. This is the Amazon counterpart of ``ontology_sync_replay.py``, reached through it with
``--type SettlementLine``: the client, the stack and the evidence layout are that script's.
Business callers sync the transaction once with ``python3 ontology_sync.py ENDPOINT --type
SettlementLine``.

    python3 ontology_sync_replay.py --type SettlementLine --output FILE [--bin-dir DIR]
    python3 ontology_sync_replay.py --type SettlementLine --output FILE --evidence-root DIR
    python3 ontology_sync_replay.py --type SettlementLine --output FILE --endpoint URL

The transaction is delivered twice and every delivery is read back on its own through the
list route and then the detail route of each listed object. The first readback must hold
exactly the three archived lines; the second the same objects and nothing new. The third
line then keeps its source with ``total`` +7.44 and must raise SyncConflictError, and the
store is read again after that attempt. Amounts are exact decimals: the service's answers
are read with every fraction as ``Decimal``, and so is the artifact they are checked against.

With ``--evidence-root`` the archived bytes are checked first against SHA256SUMS and
against what the report records of its v3 build and of its download of that version.
The exit code is 0 only when every scenario passed.
"""

import contextlib
import hashlib
import json
import urllib.request
from decimal import Decimal
from pathlib import Path

from ontology_sync import (SETTLEMENT_LINE_KEY, OntologySyncClient, SyncConflictError, archived_transaction,
                           row_key, rows_of, stable_object_id, sync_settlement_lines)
from ontology_sync_replay import HERE, Stack, backend_is_cassandra

ROUTE = "/api/v1/ontology/types/SettlementLine/objects/"
# The settlement lines the archived transaction must hold, in file order.
EXPECTED_LINES = [
    {"settlement id": 1000000010, "type": "Order", "order id": "000-0000000-0000003", "sku": "DEMO-SKU-C3",
     "total": "59.57"},
    {"settlement id": 1000000011, "type": "Order", "order id": "000-0000000-0000004", "sku": "DEMO-SKU-D4",
     "total": "7.44"},
    {"settlement id": 1000000012, "type": "Refund", "order id": "000-0000000-0000004", "sku": "DEMO-SKU-D4",
     "total": "-7.44"},
]
# The exact decimal sum of their totals.
EXPECTED_TOTAL = "59.57"


def read(endpoint, path, tenant):
    """One answer of the object service with every fraction in it an exact Decimal."""
    request = urllib.request.Request(endpoint + path, headers={"x-of-tenant": tenant}, method="GET")
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response, parse_float=Decimal)


def archived_lines(raw, source):
    """The archived rows as ``rows_of`` returns them, with every fraction an exact Decimal."""
    return [{**json.loads(line, parse_float=Decimal), "_source": {**source, "line": n}}
            for n, line in enumerate(raw.splitlines(), 1)]


def same_values(left, right):
    """JSON equality with every number, ``Decimal`` or not, compared as an exact decimal."""
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(same_values(left[name], right[name]) for name in left)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(map(same_values, left, right))
    if isinstance(left, (int, Decimal)) and isinstance(right, (int, Decimal)):
        return left == right
    return type(left) is type(right) and left == right


def plain(value):
    """The value as JSON evidence: every Decimal as its exact digits."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {name: plain(item) for name, item in value.items()}
    if isinstance(value, list):
        return [plain(item) for item in value]
    return value


def readback(endpoint, tenant):
    """The SettlementLine objects of one tenant: the list route, then each listed object by id.

    Only the detail route's answer is kept for an object, keyed by its id. An object the
    list omits, or that the detail route does not answer for, is not held.
    """
    listed = read(endpoint, ROUTE + "?per_page=50", tenant)
    details = {}
    for item in listed["data"]:
        detail = read(endpoint, ROUTE + item["id"], tenant)
        assert (detail["id"], detail["object_type_id"]) == (item["id"], "SettlementLine"), detail
        details[item["id"]] = detail
    return {"total": listed["total"], "listed": [item["id"] for item in listed["data"]], "details": details}


def held_lines(held, archived):
    """One line per object of a readback, after checking that each is the archived row of its line.

    A line held twice or missing, and an object whose properties differ from its archived
    row, fail here. A line names its object id and what the acceptance checks per line.
    """
    assert held["total"] == len(held["listed"]) == len(held["details"]), held["total"]
    by_line = {}
    for detail in held["details"].values():
        line = detail["properties"]["_source"]["line"]
        assert line not in by_line, f"line {line} is held twice"
        by_line[line] = detail
    assert sorted(by_line) == list(range(1, len(archived) + 1)), sorted(by_line)
    lines = []
    for number, row in enumerate(archived, 1):
        properties = by_line[number]["properties"]
        assert same_values(properties, row), (number, properties)
        lines.append({"line": number, "object_id": by_line[number]["id"], "settlement id": row["settlement id"],
                      "type": row["type"], "order id": row["order id"], "sku": row["sku"],
                      "total": str(properties["total"])})
    return lines


def checked(lines):
    """Assert that the lines are the expected settlement lines; return their exact decimal total."""
    named = [{name: line[name] for name in EXPECTED_LINES[0]} for line in lines]
    assert named == EXPECTED_LINES, named
    second, third = lines[1:]
    assert (second["order id"], second["sku"]) == (third["order id"], third["sku"]), lines  # one order, one SKU
    assert second["object_id"] != third["object_id"], lines  # two objects
    total = sum((Decimal(line["total"]) for line in lines), Decimal(0))
    assert total == Decimal(EXPECTED_TOTAL), total
    return str(total)


def stable_id(tenant, row):
    return stable_object_id(tenant, "SettlementLine", row["_source"]["transaction"],
                            row_key(row, row["_source"], SETTLEMENT_LINE_KEY))


def store_starts_empty(endpoint, tenant):
    """The archived tenant holds no SettlementLine object before the replay."""
    held = readback(endpoint, tenant)
    assert held["total"] == 0 and not held["listed"], \
        f"tenant {tenant} already holds {held['total']} SettlementLine objects; use an empty store"
    return {"passed": True, "tenant": tenant, "count_before": 0}


def replay_twice(endpoint, tenant, rows, archived, deliveries=2):
    """Each delivery is read back on its own: the first holds exactly the archived lines, the next
    the same objects and nothing new.

    The object id of every line must be the one the sync returned for that row, so the
    mapping from original row to object is the same in every readback.
    """
    runs = []
    for delivery in range(1, deliveries + 1):
        results = sync_settlement_lines(endpoint, tenant, rows)
        held = readback(endpoint, tenant)
        lines = held_lines(held, archived)
        assert [line["object_id"] for line in lines] == [result.object_id for result in results], lines
        runs.append({"delivery": delivery, "count": len(lines), "created": sum(result.created for result in results),
                     "total": checked(lines), "lines": lines,
                     "results": [{"line": row["_source"]["line"], "settlement id": row["settlement id"],
                                  "object_id": result.object_id, "created": result.created}
                                 for row, result in zip(rows, results)],
                     "readback": plain(held)})
    first, *again = runs
    assert first["count"] == len(EXPECTED_LINES) and all(result["created"] for result in first["results"]), first
    for run in again:
        assert run["count"] == first["count"] and run["lines"] == first["lines"], run
        assert not any(result["created"] for result in run["results"]), run
    return {"passed": True, "tenant": tenant, "deliveries": runs}


def changed_total_is_rejected(endpoint, tenant, rows, archived):
    """The third line keeps its source with ``total`` +7.44: SyncConflictError, and nothing changes.

    The store is read before and after the attempt. ``lines`` and ``total`` come from the
    readback after it.
    """
    client = OntologySyncClient(endpoint, tenant, key_properties=SETTLEMENT_LINE_KEY)
    third = rows[2]
    changed = {**third, "total": 7.44}
    assert changed["_source"] == third["_source"]
    before = readback(endpoint, tenant)
    try:
        client.sync("SettlementLine", changed)
    except SyncConflictError as error:
        rejected = {"line": third["_source"]["line"], "changed": "total", "total": "7.44",
                    "exception": type(error).__name__, "error": str(error)}
    else:
        raise AssertionError("the third line with another total was accepted")
    after = readback(endpoint, tenant)
    assert after["total"] == before["total"] and after["details"] == before["details"], "the store changed"
    lines = held_lines(after, archived)
    assert [line["object_id"] for line in lines] == [stable_id(tenant, row) for row in rows], lines
    return {"passed": True, "tenant": tenant, "rejected": rejected, "count_after": len(lines), "lines": lines,
            "total": checked(lines), "readback": plain(after)}


def verify_archive(root, source, raw):
    """Check the archived bytes against the manifest, SHA256SUMS and the report; return what was checked.

    ``archived_transaction`` has already refused an artifact that does not hash to the
    sha256 of the manifest's v3 build. This checks what it did not: that the manifest's
    build is the report's build of version 3 and the report's download of that version,
    and the checksums SHA256SUMS records for the manifest, the report and the artifact.
    """
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_bytes())
    build, = [item for item in manifest["sources"]["amazon"]["builds"] if item["output_version"] == 3]
    artifact = "amazon/" + build["artifact"]
    files = {"manifest.json": (root / "manifest.json").read_bytes(),
             "amazon/amazon-report.json": (root / "amazon" / "amazon-report.json").read_bytes(), artifact: raw}
    report = json.loads(files["amazon/amazon-report.json"])
    recorded = {name: digest for digest, name in (line.split() for line in (root / "SHA256SUMS").read_text().splitlines()
                                                  if line.strip())}
    # A file SHA256SUMS does not name has no recorded digest and fails the comparison below.
    checksums = {name: {"recorded": recorded.get(name), "computed": hashlib.sha256(content).hexdigest()}
                 for name, content in files.items()}
    assert all(item["recorded"] == item["computed"] for item in checksums.values()), checksums
    assert build["bytes"] == len(raw), (build["bytes"], len(raw))
    built, = [item for item in report["pipeline"]["builds"] if item["output_version"] == 3]
    assert (build["output_transaction"], build["output_file_id"], build["artifact"], build["sha256"]) == \
        (built["output_transaction"], built["output_file_id"], built["artifact"], built["sha256"]), (build, built)
    downloaded, = [item for item in report["pipeline"]["history"] if item["version"] == 3]
    file, = downloaded["listed_files"]
    assert (downloaded["transaction"], file["file_id"], file["transaction_id"], downloaded["download_sha256"],
            downloaded["size_bytes"]) == (source["transaction"], source["file"], source["transaction"],
                                          source["sha256"], len(raw)), downloaded
    assert report["revision"] == manifest["revision"], (report["revision"], manifest["revision"])
    return {"root": str(root), "revision": report["revision"], "checksums": checksums, "bytes": len(raw),
            "rows": downloaded["row_count"], "output": source}


def replay(args):
    """The scenarios of ``ontology_sync_replay.py --type SettlementLine``, with the evidence of that script."""
    revision, tenant, source, raw = archived_transaction(args.evidence_root, "SettlementLine")
    rows = rows_of(raw, source)
    archived = archived_lines(raw, source)
    evidence = {"revision": revision, "tenant": tenant, "source": source, "rows": rows,
                "client_sha256": hashlib.sha256((HERE / "ontology_sync.py").read_bytes()).hexdigest(), "scenarios": {}}
    try:
        if args.evidence_root:
            evidence["archive"] = verify_archive(args.evidence_root, source, raw)
        else:
            evidence["archive"] = {"embedded": True, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}
        with contextlib.ExitStack() as scope:
            if args.endpoint:
                endpoint = args.endpoint.rstrip("/")
            else:
                stack = scope.enter_context(Stack(args))
                endpoint, evidence["stack"] = stack.endpoint, stack.describe()
            evidence["endpoint"] = endpoint
            scenarios = [
                ("backend_is_cassandra", lambda: backend_is_cassandra(endpoint, tenant)),
                ("store_starts_empty", lambda: store_starts_empty(endpoint, tenant)),
                ("replay_twice", lambda: replay_twice(endpoint, tenant, rows, archived)),
                ("changed_total_is_rejected", lambda: changed_total_is_rejected(endpoint, tenant, rows, archived)),
            ]
            for name, run in scenarios:
                try:
                    evidence["scenarios"][name] = run()
                except Exception as error:  # recorded, so that one failure does not hide the others
                    evidence["scenarios"][name] = {"passed": False, "error": f"{type(error).__name__}: {error}"}
    except Exception as error:
        evidence["error"] = f"{type(error).__name__}: {error}"
    evidence["passed"] = "error" not in evidence and all(s["passed"] for s in evidence["scenarios"].values())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2) + "\n")
    if "error" in evidence:
        print(f"FAIL replay: {evidence['error']}")
    for name, scenario in evidence["scenarios"].items():
        print(f"{'PASS' if scenario['passed'] else 'FAIL'} {name}" + ("" if scenario["passed"] else ": " + scenario["error"]))
    print(f"{'PASS' if evidence['passed'] else 'FAIL'} evidence {args.output}")
    return 0 if evidence["passed"] else 1

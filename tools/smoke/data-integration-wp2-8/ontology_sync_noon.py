"""Replay the archived noon v3 transaction through the sync client against a real Cassandra store.

Regression for duplicate OrderLinePrice objects of the generic create route: the same
tenant, source transaction and order line numbers delivered twice through
``POST /api/v1/ontology/types/OrderLinePrice/objects/`` would store every line again.
This is the noon counterpart of ``ontology_sync_replay.py``, reached through it with
``--type OrderLinePrice``: the client, the stack and the evidence layout are that
script's, with the order line number ``item_nr`` in place of ``sku``. Business callers
sync the transaction once with ``python3 ontology_sync.py ENDPOINT --type OrderLinePrice``.

    python3 ontology_sync_replay.py --type OrderLinePrice --output FILE [--bin-dir DIR]
    python3 ontology_sync_replay.py --type OrderLinePrice --output FILE --evidence-root DIR
    python3 ontology_sync_replay.py --type OrderLinePrice --output FILE --endpoint URL

The exit code is 0 only when every scenario passed.
"""

import contextlib
import hashlib
import json

from ontology_sync import (ORDER_LINE_PRICE_KEY, OntologySyncClient, SyncConflictError,
                           archived_transaction, rows_of, sync_order_line_prices)
from ontology_sync_replay import HERE, Stack, backend_is_cassandra, call, counted

ROUTE = "/api/v1/ontology/types/OrderLinePrice/objects/"
# The order lines the archived transaction must hold, each with its currency and unit price.
EXPECTED_VALUES = {"NRPL0001-1": ["AED", "49"], "NRPL0002-1": ["AED", "120"], "NRPL0003-1": ["AED", "30"]}


def snapshot(endpoint, tenant):
    """The OrderLinePrice objects of one tenant grouped by order line number, each one read back by id."""
    listed = call(endpoint, "GET", ROUTE + "?per_page=50", tenant)
    assert listed["total"] == len(listed["data"])
    groups = {}
    for item in listed["data"]:
        detail = call(endpoint, "GET", ROUTE + item["id"], tenant)
        assert (detail["id"], detail["object_type_id"]) == (item["id"], "OrderLinePrice")
        properties = detail["properties"]
        groups.setdefault(properties["item_nr"], []).append(
            {"id": detail["id"], "currency": properties["currency"], "unit_price": properties["unit_price"]})
    return {item_nr: sorted(items, key=lambda item: item["id"]) for item_nr, items in groups.items()}


def values(by_item_nr):
    """The [currency, unit price] of every object held for each order line number."""
    return {item_nr: [[item["currency"], item["unit_price"]] for item in items]
            for item_nr, items in by_item_nr.items()}


def store_starts_empty(endpoint, tenant):
    """The archived tenant holds no OrderLinePrice object before the replay."""
    held = snapshot(endpoint, tenant)
    assert not held, f"tenant {tenant} already holds {counted(held)} OrderLinePrice objects; use an empty store"
    return {"passed": True, "tenant": tenant, "count_before": 0}


def replay_twice(endpoint, tenant, rows, deliveries=2):
    """Each delivery is read back on its own: the first holds exactly the expected order lines, the next the same objects."""
    runs = []
    for delivery in range(1, deliveries + 1):
        results = sync_order_line_prices(endpoint, tenant, rows)
        by_item_nr = snapshot(endpoint, tenant)
        assert {item["id"] for items in by_item_nr.values() for item in items} == \
            {result.object_id for result in results}
        runs.append({"delivery": delivery, "count": counted(by_item_nr), "by_item_nr": by_item_nr,
                     "results": [{"item_nr": row["item_nr"], "object_id": result.object_id, "created": result.created}
                                 for row, result in zip(rows, results)]})
    first, *again = runs
    assert first["count"] == len(EXPECTED_VALUES) and all(item["created"] for item in first["results"]), first
    assert values(first["by_item_nr"]) == {item_nr: [value] for item_nr, value in EXPECTED_VALUES.items()}, first
    for run in again:
        assert run["count"] == len(EXPECTED_VALUES) and run["by_item_nr"] == first["by_item_nr"], run
        assert not any(item["created"] for item in run["results"]), run
    return {"passed": True, "tenant": tenant, "deliveries": runs}


def other_content_is_rejected(endpoint, tenant, rows):
    """A row that keeps its order line number but carries another unit price is rejected.

    The store is read again after the last attempt. ``count_after`` and ``after`` come
    from that readback, not from the snapshot taken before the attempts.
    """
    client = OntologySyncClient(endpoint, tenant, key_properties=ORDER_LINE_PRICE_KEY)
    before = snapshot(endpoint, tenant)
    rejected = []
    for row in rows:
        changed = {**row, "unit_price": "9.99"}
        try:
            client.sync("OrderLinePrice", changed)
        except SyncConflictError as error:
            rejected.append({"changed": "unit_price", "item_nr": row["item_nr"], "unit_price": changed["unit_price"],
                             "error": str(error)})
        else:
            raise AssertionError(f"order line {row['item_nr']} with another unit_price was accepted")
    after = snapshot(endpoint, tenant)
    assert after == before, after
    return {"passed": True, "tenant": tenant, "rejected": rejected, "count_after": counted(after), "after": after}


def replay(args):
    """The scenarios of ``ontology_sync_replay.py --type OrderLinePrice``, with the evidence of that script."""
    revision, tenant, source, raw = archived_transaction(args.evidence_root, "OrderLinePrice")
    rows = rows_of(raw, source)
    evidence = {"revision": revision, "tenant": tenant, "source": source, "rows": rows,
                "client_sha256": hashlib.sha256((HERE / "ontology_sync.py").read_bytes()).hexdigest(), "scenarios": {}}
    try:
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
                ("replay_twice", lambda: replay_twice(endpoint, tenant, rows)),
                ("other_content_is_rejected", lambda: other_content_is_rejected(endpoint, tenant, rows)),
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

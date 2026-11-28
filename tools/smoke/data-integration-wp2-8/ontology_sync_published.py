"""Deliver the v3 that a Pipeline run of this round published, located from its run receipt.

The copies embedded in ``ontology_sync.py`` are the v3 outputs of one earlier run, at
revision c48a709f9. They hold the same rows and SHA256 as the v3 of every later round, and so
does the v1 of each round, which replays the same input: neither the values nor the hash of the
bytes tells this round's v3 from the archived transaction or from another file. What names it is
the receipt of its own run. A read-only reader starts from the run receipt the smoke saved, finds
the one transaction of the output dataset whose ``metadata.transaction_rid`` repeats the
``transaction_rid`` of the committed resource of that run, the version that cut it and its one
file, downloads the file and hashes the bytes itself. The rows are delivered once through the
conditional put of ``ontology_sync.py`` and the tenant is read back through the list route and
then the detail route of each listed object.

One assertion accepts or refuses what a tenant holds. It accepts a tenant that holds exactly the
three objects whose ids the client derives from the tenant, the type, the transaction of the
receipt and each key of its file, each with the values of its row and an ``_source`` that
repeats the dataset, version, transaction, file and SHA256 of that read and the line of its key
in that file. The id of an object is the one derived for the key it holds, so two objects that
swapped their complete properties are refused. Given another source and its rows, the same
assertion tells whether a tenant holds exactly that source. That must hold before the refusal of
the three other sources counts: the archived transaction, and the v1 and v2 this round
published, of which v1 has the bytes and the hash of v3.

    python3 ontology_sync_published.py --state DIR --dataset-base URL --endpoint URL [--tenant ID] [--type TYPE]
    python3 ontology_sync_published.py --state DIR --endpoint URL --tenant ID --verify-only [--type TYPE]

``--state`` is the evidence directory the smoke of this round printed. The read-only account is
the bearer in ``WP34_TOKEN`` and ``--dataset-base`` defaults to ``WP34_DVS``. The first form is
the entry of a downstream consumer. Each run delivers once and writes ``<prefix>-source.json``,
the source the receipt names, and ``<prefix>-v3.ndjson``, the bytes it downloaded. The first run
creates the three objects; the same command run again, after a restart of the downstream process,
creates none and is accepted the same way, because a redelivery of the same key and content
succeeds by #90. The second form reads a tenant back and judges it against the source file:
nothing is synced and the Dataset service is not asked. Both print one JSON report with the
readback and return 0 only when the assertion accepts the tenant. ``run.sh lingxing``,
``run.sh noon`` and ``run.sh amazon`` run the first form twice, as two processes with the same
command, and the second on three tenants that received the other sources, against a Cassandra
store each smoke starts and stops itself.

``--type`` selects the synced type and with it the smoke whose report names the round, the key
and the values of its rows and the prefix of its files: ``ProductCost``, the default, is a SKU
and its cost under ``product-cost``, ``OrderLinePrice`` an order line number with its currency
and unit price under ``order-line-price``, and ``SettlementLine`` a settlement line, which has
no key property of its own, with its settlement id, type, order id, SKU and total under
``settlement-line``. ``Kind`` holds what differs between the types; every function below is
theirs in common.

A settlement line is identified as ``ontology_sync.py`` identifies it: by ``_source.file`` and
``_source.line`` next to the tenant, the type and the transaction, so the order and the refund of
one order id and one SKU hold two objects. Its amounts are exact decimals: the answers of the
object service are read with every fraction a ``Decimal``, and the totals the tenant holds are
compared with the ``EXPECTED_LINES`` of ``ontology_sync_amazon.py`` and, as a whole, by its
``checked``.
"""

import argparse
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import partial
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _sibling(name):
    """A module of this directory, also under ``python3 -I``, which puts no directory on sys.path."""
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, HERE / (name + ".py"))
        sys.modules[name] = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(sys.modules[name])
    return sys.modules[name]


_sibling("ontology_sync"), _sibling("ontology_sync_replay"), _sibling("ontology_sync_noon")
_sibling("ontology_sync_amazon")
from ontology_sync import (ENTRIES, SETTLEMENT_LINE_KEY, SOURCE_ATTRIBUTES, SyncError, archived_transaction,  # noqa: E402
                           rows_of, stable_object_id)
from ontology_sync_amazon import EXPECTED_LINES, EXPECTED_TOTAL, checked, plain, read  # noqa: E402
from ontology_sync_noon import EXPECTED_VALUES  # noqa: E402
from ontology_sync_replay import Stack, backend_is_cassandra, call  # noqa: E402

# The lingxing smoke publishes the costs of product v1, v2 and v1 again as output v1, v2 and v3,
# the noon smoke the order lines of its v1, v2 and v1 again.
VERSION = 3
TOKEN = "WP34_TOKEN"  # the bearer of the read-only account
DEFAULT = "ProductCost"  # the type of the commands that name none
# What the smoke recorded of one build, by the name of the source the reader located.
RECORDED = {"run_id": "pipeline_run_id", "pipeline_output_transaction_rid": "pipeline_output_transaction_rid",
            "content_hash": "pipeline_content_hash", "dataset": "output_dataset", "version": "output_version",
            "transaction": "output_transaction", "file": "output_file_id", "sha256": "sha256",
            "size_bytes": "size_bytes", "row_count": "row_count"}
# The other sources the assertion must refuse, each with the attributes in which it differs
# from this round: by the output version this round published, or None for the archived transaction.
OTHER_SOURCES = (("archived_transaction", None, ("dataset", "transaction", "file")),
                 ("first_output", 1, ("version", "transaction", "file")),
                 ("second_output", 2, ("version", "transaction", "file", "sha256")))


def _get(endpoint, path, tenant):
    """One GET as ``ontology_sync_replay.py`` makes it: every fraction of the answer a float."""
    return call(endpoint, "GET", path, tenant)


@dataclass(frozen=True)
class Kind:
    """One synced type: what differs between the ProductCost rows of lingxing, the OrderLinePrice
    rows of noon and the SettlementLine rows of amazon. Everything else in this module is theirs
    in common.

    A row of the file is named by ``key`` and known by the values ``values`` reads beside that
    name: a cost, or a currency and a unit price. ``expected`` holds them for each name of the v3
    this round published and ``value_name`` is how a problem names them. ``report`` is the report
    of that round, whose ``organization``, the publisher of the round, is the tenant of the
    delivery. The evidence of a run is written to ``<prefix>-<name>`` in its state directory.

    ``key_properties`` are the key of an object, named as ``ontology_sync.py`` names key
    properties. By default it is the one ``key`` names and the value of that property is the
    name of a row. When every one is an attribute of the row's ``_source``, as for a settlement
    line, the row is named by its line: ``_source.line`` is the name, and every other attribute
    comes from the source judged against, which names a file and holds no line. ``row`` reads one
    row of a file and ``get`` one answer of the object service; a settlement line reads the
    fractions of both as exact decimals. ``whole``, for a type whose rows are also checked as a
    whole, is the sentence the rows must satisfy and the function of the first readback that
    raises AssertionError when they do not.
    """

    type_id: str
    expected: dict
    values: Callable[[dict], object]
    value_name: str
    report: str
    prefix: str
    key_properties: tuple = ()
    row: Callable[[str], dict] = json.loads
    get: Callable[[str, str, str], dict] = _get
    whole: tuple | None = None

    @property
    def key(self):
        """The property that names a row of this type in the reports of its sync entry."""
        return ENTRIES[self.type_id][0]

    @property
    def keys(self):
        """The properties that are the key of one object of this type."""
        return self.key_properties or (self.key,)

    @property
    def by_line(self):
        """Whether a row is named by its line: every key property is an attribute of its ``_source``."""
        return all(name.startswith("_source.") for name in self.keys)

    @property
    def name_property(self):
        """The property whose value in a read-back object names the row it holds."""
        return "line" if self.by_line else self.key

    def name(self, number, row):
        """What names one row of a file: its line, or the value of the property that is its key."""
        return number if self.by_line else row[self.key]

    def held_name(self, properties):
        """The name of the row a read-back object claims to hold: its ``_source.line``, or its key property.
        An object whose ``_source`` is no object claims no line."""
        return (held_source(properties) or {}).get("line") if self.by_line else properties.get(self.key)

    def key_of(self, source, name):
        """The key of the object that holds the row ``name`` names in the source ``source`` names.

        ``source`` names a file and holds no line, so the line of the key is the name of the row,
        as the one property that names the rows of the other types is; every other attribute of
        the key comes from ``source``.
        """
        return {prop: source[prop[len("_source."):]] if prop.startswith("_source.") and prop != "_source.line" else name
                for prop in self.keys}

    @property
    def sync(self):
        """The entry of ``ontology_sync.py`` that delivers the rows of this type."""
        return ENTRIES[self.type_id][1]

    @property
    def route(self):
        """The ontology route of this type's objects: the list, and one object by its id."""
        return f"/api/v1/ontology/types/{self.type_id}/objects/"

    @property
    def source_file(self):
        return f"{self.prefix}-source.json"

    @property
    def downloaded_file(self):
        return f"{self.prefix}-v3.ndjson"

    @property
    def downstream_file(self):
        return f"{self.prefix}-downstream.json"


def _cost(row):
    """The values of a ProductCost row beside its SKU."""
    return row["cg_price"]


def _price(row):
    """The values of an OrderLinePrice row beside its order line number."""
    return [row["currency"], row["unit_price"]]


# What names a settlement line beside its total.
LINE_FIELDS = ("settlement id", "type", "order id", "sku")


def _exact(amount):
    """An amount as the exact decimal of its digits: the Decimal of a readback, or the digits its JSON
    evidence keeps. A float, a boolean or text that is no number is returned as it is, so that it
    equals no expected amount."""
    if isinstance(amount, Decimal):
        return amount
    if isinstance(amount, (str, int)) and not isinstance(amount, bool):
        try:
            return Decimal(amount)
        except InvalidOperation:
            return amount
    return amount


def _line(row):
    """The values of a SettlementLine row beside its line: its settlement id, type, order id, SKU and total."""
    return [*(row[name] for name in LINE_FIELDS), _exact(row["total"])]


def expected_lines():
    """What the settlement lines of this round's v3 hold, by their line in the file: the ``EXPECTED_LINES``
    of ``ontology_sync_amazon.py``, each total an exact decimal."""
    return {number: [*(line[name] for name in LINE_FIELDS), Decimal(line["total"])]
            for number, line in enumerate(EXPECTED_LINES, 1)}


def _lines_held(store):
    """``checked`` of ``ontology_sync_amazon.py`` over the settlement lines a readback holds, in file order."""
    lines = [{"line": obj["properties"]["_source"]["line"], "object_id": obj["id"],
              **{name: obj["properties"][name] for name in LINE_FIELDS},
              "total": str(obj["properties"]["total"])} for obj in store["objects"]]
    return checked(sorted(lines, key=lambda line: line["line"]))


LINES_HELD = ("the three settlement lines the tenant holds are the expected three in file order: the order and the "
              f"refund of one order and SKU hold different ids, and their totals are exact decimals adding up to "
              f"{EXPECTED_TOTAL}", _lines_held)

PRODUCT_COST = Kind(type_id="ProductCost", expected={"0007": "0.00", "W-2": "9.25", "D3": "17.40"},
                    values=_cost, value_name="cg_price", report="product-report.json", prefix="product-cost")
ORDER_LINE_PRICE = Kind(type_id="OrderLinePrice", expected=EXPECTED_VALUES, values=_price,
                        value_name="currency and unit_price", report="noon-report.json", prefix="order-line-price")
SETTLEMENT_LINE = Kind(type_id="SettlementLine", expected=expected_lines(), values=_line,
                       value_name="settlement id, type, order id, sku and total", report="amazon-report.json",
                       prefix="settlement-line", key_properties=SETTLEMENT_LINE_KEY,
                       row=partial(json.loads, parse_float=Decimal), get=read, whole=LINES_HELD)
KINDS = {kind.type_id: kind for kind in (PRODUCT_COST, ORDER_LINE_PRICE, SETTLEMENT_LINE)}
# The lingxing evidence keeps the names of #95: what this round's v3 holds, each a string, and its files.
EXPECTED_COSTS = PRODUCT_COST.expected
SOURCE_FILE = PRODUCT_COST.source_file
DOWNLOADED_FILE = PRODUCT_COST.downloaded_file
DOWNSTREAM_FILE = PRODUCT_COST.downstream_file


class ReceiptError(Exception):
    """The run receipt does not name one committed output, or the Dataset service does not answer
    with the one transaction, version and file that repeat it. Nothing was synced."""


class _SignedAddressOnly(urllib.request.HTTPRedirectHandler):
    """A download is answered with 302 to the signed address of the file. Stop at that answer."""

    def http_error_302(self, req, fp, code, msg, headers):
        raise urllib.error.HTTPError(req.full_url, code, msg, headers, fp)

    http_error_301 = http_error_303 = http_error_307 = http_error_308 = http_error_302


_OPENER = urllib.request.build_opener(_SignedAddressOnly)


class DatasetReader:
    """The read-only account of the Dataset service. Every request it sends is a GET.

    ``opener`` replaces the transport in tests: it receives the request and the timeout
    and returns the status, the headers and the body.
    """

    def __init__(self, base, token, *, timeout=30, opener=None):
        self.base = base.rstrip("/")
        self.token = token
        self.timeout = timeout
        self._opener = opener

    def _send(self, url, bearer):
        request = urllib.request.Request(url, headers={"Authorization": "Bearer " + self.token} if bearer else {})
        if self._opener is not None:
            return self._opener(request, self.timeout)
        try:
            with _OPENER.open(request, timeout=self.timeout) as response:
                return response.status, response.headers, response.read()
        except urllib.error.HTTPError as error:
            with error:
                return error.code, error.headers, error.read()
        except OSError as error:
            raise ReceiptError(f"GET {url} failed: {error!r}") from error

    def get(self, path):
        """The JSON body of one authenticated GET."""
        status, _, body = self._send(self.base + path, True)
        if status != 200:
            raise ReceiptError(f"GET {path} returned {status}: {body[:200]!r}")
        try:
            return json.loads(body)
        except ValueError as error:
            raise ReceiptError(f"GET {path} returned invalid JSON") from error

    def download(self, dataset, file_id):
        """The bytes of one active file.

        The service answers with the signed address of the file. That address is requested
        on its own, so the bearer stays at the service.
        """
        path = f"/v1/datasets/{dataset}/files/{file_id}/download"
        status, headers, body = self._send(self.base + path, True)
        if status != 302 or not headers.get("Location"):
            raise ReceiptError(f"GET {path} returned {status}, expected 302 to the signed address of the file")
        status, _, body = self._send(headers["Location"], False)
        if status != 200:
            raise ReceiptError(f"the signed address of file {file_id} returned {status}")
        return body


def source_of(binding):
    """The ``_source`` of the rows of one source, without the line."""
    return {name: binding[name] for name in SOURCE_ATTRIBUTES if name != "line"}


def locate(reader, receipt, kind=PRODUCT_COST):
    """The source one Pipeline run published, as the read-only reader finds it from its run receipt.

    Returns the source and the bytes of its file. The receipt is the answer the Pipeline
    service gave for a succeeded run. Its one committed resource names the output dataset
    and, in ``transaction_rid``, the handle the Pipeline gave the transaction. One
    transaction of the output dataset repeats that handle in ``metadata.transaction_rid``:
    the version that cut it holds one file, whose bytes are downloaded and hashed here.
    ``lines`` of the source is the line of each key of ``kind`` in that file.
    """
    run_id, pipeline_id = receipt.get("id"), receipt.get("pipeline_id")

    def refuse(problem):
        raise ReceiptError(f"run {run_id}: {problem}")

    if receipt.get("status") != "succeeded" or receipt.get("run_id") != run_id:
        refuse(f"is {receipt.get('status')!r} as run {receipt.get('run_id')!r}, expected a succeeded run {run_id!r}")
    committed = [(node, resource) for node in receipt["node_results"]
                 for resource in node.get("output_resources") or [] if resource["status"] == "committed"]
    if len(committed) != 1:
        refuse(f"committed {len(committed)} resources, expected one")
    node, resource = committed[0]
    handle = f"pipeline-run:{run_id}:output"
    if resource["kind"] != "dataset" or resource["transaction_rid"] != handle:
        refuse(f"committed a {resource['kind']} as {resource['transaction_rid']!r}, expected a dataset as {handle!r}")
    dataset = resource["rid"].removeprefix("ri.foundry.main.dataset.")
    content_hash = node["output"]["content_hash"]
    page = reader.get(f"/v1/datasets/{dataset}/versions?limit=1000")
    if page.get("has_more"):
        refuse(f"dataset {dataset} has more versions than one page lists")
    found = []
    for version in page["data"]:
        if not version.get("transaction_id"):
            continue
        transaction = reader.get(f"/v1/datasets/{dataset}/branches/main/transactions/{version['transaction_id']}")
        if (transaction.get("metadata") or {}).get("transaction_rid") == handle:
            found.append((version, transaction))
    if len(found) != 1:
        refuse(f"{len(found)} transactions of dataset {dataset} repeat {handle!r}, expected one")
    version, transaction = found[0]
    metadata = transaction["metadata"]
    if (transaction["id"], transaction["status"], transaction["dataset_id"], transaction["branch_name"]) != (
            version["transaction_id"], "COMMITTED", dataset, resource["branch"]):
        refuse(f"transaction {version['transaction_id']} is {transaction['status']!r} on "
               f"{transaction['branch_name']!r} of dataset {transaction['dataset_id']}")
    if (metadata.get("pipeline_rid"), metadata.get("pipeline_output"), metadata.get("output_content_hash")) != (
            pipeline_id, True, content_hash):
        refuse(f"transaction {transaction['id']} names pipeline {metadata.get('pipeline_rid')!r} and output hash "
               f"{metadata.get('output_content_hash')!r}, expected {pipeline_id!r} and {content_hash!r}")
    listing = reader.get(f"/v1/datasets/{dataset}/files?branch=main&transaction_id={transaction['id']}")
    if listing["total"] != 1 or len(listing["files"]) != 1 or listing["files"][0]["transaction_id"] != transaction["id"]:
        refuse(f"transaction {transaction['id']} must hold exactly its one file, not {len(listing['files'])}")
    file, = listing["files"]
    raw = reader.download(dataset, file["id"])
    try:
        rows = [json.loads(line) for line in raw.splitlines()]
        preview = [dict(zip(metadata["preview_columns"], values)) for values in metadata["preview_rows"]]
        lines, _ = lines_and_values(rows, kind)
    except (ValueError, KeyError, TypeError) as error:
        refuse(f"file {file['id']} or its transaction does not parse: {error!r}")
    if (metadata["size_bytes"], metadata["row_count"]) != (len(raw), len(rows)) or preview != rows:
        refuse(f"file {file['id']} holds {len(rows)} rows in {len(raw)} bytes; its transaction records "
               f"{metadata['row_count']} rows in {metadata['size_bytes']} bytes and another preview")
    return {"version": version["version"], "dataset": dataset, "transaction": transaction["id"], "file": file["id"],
            "sha256": hashlib.sha256(raw).hexdigest(), "listed_size_bytes": file["size_bytes"],
            "run_id": run_id, "pipeline_id": pipeline_id, "pipeline_output_transaction_rid": handle,
            "content_hash": content_hash, "size_bytes": len(raw), "row_count": len(rows), "lines": lines}, raw


def read_back(endpoint, tenant, kind=PRODUCT_COST):
    """The objects of one type that one tenant holds: the list, then each listed object by id."""
    route = kind.route
    listed = kind.get(endpoint, route + "?per_page=50", tenant)
    return {"total": listed["total"],
            "objects": [kind.get(endpoint, route + item["id"], tenant) for item in listed["data"]]}


def _same(left, right):
    """Equality of two scalars where a boolean never equals a number."""
    return type(left) is type(right) and left == right


def lines_and_values(rows, kind=PRODUCT_COST):
    """The line and the values of each name in one file, by name."""
    return ({kind.name(number, row): number for number, row in enumerate(rows, 1)},
            {kind.name(number, row): kind.values(row) for number, row in enumerate(rows, 1)})


def _lenient(properties):
    """The properties of a read-back object, read as None where the object lacks one."""
    return defaultdict(lambda: None, properties)


def held_source(properties):
    """The ``_source`` a read-back object holds, or None when it holds none or holds one that is no object."""
    source = properties.get("_source")
    return source if isinstance(source, dict) else None


def row_differences(source, lines, values, tenant, store, kind=PRODUCT_COST):
    """Where the objects a tenant holds differ from the rows of one file.

    ``source`` names the dataset, version, transaction, file and SHA256 of the file, ``lines``
    the line of each name in it and ``values`` the values of each name. The tenant holds exactly
    that file when it holds one object per name and nothing else, when the id of each object is
    the id the client derives from the tenant, the type, the transaction of the file and the key
    of the name it holds, and when the object keeps the values of its row and an ``_source`` that
    repeats the source and the line of its name. An empty list accepts the tenant. An object
    whose ``_source`` is no object is refused for that, whatever else it holds.
    """
    problems = []
    type_id = kind.type_id
    ids = sorted(obj["id"] for obj in store["objects"])
    expected = sorted(stable_object_id(tenant, type_id, source["transaction"], kind.key_of(source, name))
                      for name in values)
    if store["total"] != len(values) or ids != expected:
        problems.append(f"{store['total']} listed and {len(ids)} read back, expected the {len(values)} objects of "
                        f"transaction {source['transaction']}: {ids} is not {expected}")
    for obj in store["objects"]:
        properties = obj["properties"]
        where = f"object {obj['id']}"
        if obj["object_type_id"] != type_id:
            problems.append(f"{where} is a {obj['object_type_id']}")
        got = properties.get("_source")
        if kind.by_line and not isinstance(got, dict):
            # The line that names the row is read from its _source: an object whose _source is no object names no row.
            problems.append(f"{where} _source: {got!r}, expected an object naming its line")
            continue
        name = kind.held_name(properties)
        # A key property is a string or an integer, as stable_object_id requires; a boolean is neither.
        if isinstance(name, bool) or not isinstance(name, (str, int)) or name not in values:
            problems.append(f"{where}: {kind.name_property} {name!r} is not one of {sorted(values)}")
            continue
        # The row agrees with the name it claims, so the id is what places it: without this a
        # tenant that swapped the complete properties of two objects would be accepted.
        if obj["id"] != stable_object_id(tenant, type_id, source["transaction"], kind.key_of(source, name)):
            problems.append(f"{where}: is the id of another row, expected the id of {kind.name_property} {name!r}")
        held = kind.values(_lenient(properties))
        if held != values[name]:
            problems.append(f"{where}: {kind.value_name} {held!r}, expected {values[name]!r}")
        want = {**source, "line": name if kind.by_line else lines[name]}
        for attribute in SOURCE_ATTRIBUTES:
            if not isinstance(got, dict) or not _same(got.get(attribute), want[attribute]):
                problems.append(f"{where} _source.{attribute}: {got.get(attribute) if isinstance(got, dict) else got!r}, "
                                f"its file reads {want[attribute]!r}")
    return problems


def differences(binding, tenant, store, kind=PRODUCT_COST):
    """Where the objects a tenant holds differ from the rows of the source ``binding`` names.

    This is the one acceptance of a delivery: ``row_differences`` for the values this
    round's v3 holds. It is run against the store after each delivery of this round's v3 and
    against a store that received another source. An empty list accepts the tenant.
    """
    return row_differences(source_of(binding), binding["lines"], kind.expected, tenant, store, kind)


def deliver(endpoint, tenant, source, raw, kind=PRODUCT_COST):
    """The rows of one source, delivered once through the sync client."""
    rows = rows_of(raw, source)
    results = kind.sync(endpoint, tenant, rows)
    return {"created": sum(result.created for result in results),
            "results": [{"line": row["_source"]["line"], kind.key: row[kind.key], "object_id": result.object_id,
                         "created": result.created} for row, result in zip(rows, results)]}


def build_of(report, version):
    """The build the smoke recorded for one output version."""
    build, = [build for build in report["pipeline"]["builds"] if build["output_version"] == version]
    return build


def this_round(state, kind=PRODUCT_COST):
    """The report of this round and the saved run receipt of the v3 it published."""
    report = json.loads((state / kind.report).read_text())
    return report, json.loads((state / f"pipeline-{build_of(report, VERSION)['ordinal']}-run.json").read_text())


def receipt_differs(binding, report, build):
    """Where the source the reader located differs from what the smoke recorded of that build."""
    recorded = {name: build[key] for name, key in RECORDED.items()}
    recorded["pipeline_id"] = report["pipeline"]["pipeline_id"]
    return [f"{name}: the receipt names {binding[name]!r}, the smoke recorded {recorded[name]!r}"
            for name in recorded if binding[name] != recorded[name]]


def _json(text):
    """The JSON a text holds, or None when it holds none."""
    try:
        return json.loads(text)
    except (OSError, ValueError):
        return None


def _held(store, kind=PRODUCT_COST):
    """What the objects of a readback hold: their count, the ``_source`` they share and each name's values.

    An object whose ``_source`` is no object holds none of its attributes.
    """
    objects = store.get("objects") or []
    sources = {json.dumps({key: value for key, value in source.items() if key != "line"}, sort_keys=True)
               for source in (held_source(obj["properties"]) or {} for obj in objects)}
    source = _json(next(iter(sources))) if len(sources) == 1 else None
    return {"count": len(objects), "source": source if isinstance(source, dict) else None,
            "values": {kind.held_name(obj["properties"]): kind.values(_lenient(obj["properties"])) for obj in objects}}


def main(argv=None):
    """The command that delivers this round's v3 once, or reads a tenant back against it.

    It prints one JSON report: the source the receipt names, what the delivery created
    when it delivered, and the tenant as it was read back afterwards. The exit code is 0
    only when the assertion accepts that readback.
    """
    parser = argparse.ArgumentParser(
        description="Deliver the v3 a Pipeline run of this round published, located from its run receipt.")
    parser.add_argument("--state", type=Path, required=True, help="evidence directory the smoke of this round printed")
    parser.add_argument("--dataset-base", default=os.environ.get("WP34_DVS"),
                        help="Dataset service the read-only account reads; default: WP34_DVS")
    parser.add_argument("--endpoint", required=True, help="object service base URL")
    parser.add_argument("--tenant", help="tenant to sync into or to read back; default: the organization of this round")
    parser.add_argument("--type", choices=tuple(KINDS), default=DEFAULT,
                        help="synced object type, which also selects the report of its smoke and the prefix of its "
                             "files; default: ProductCost")
    parser.add_argument("--verify-only", action="store_true",
                        help="read the tenant back against <prefix>-source.json; nothing is synced and the Dataset "
                             "service is not asked")
    args = parser.parse_args(argv)
    kind = KINDS[args.type]
    try:
        report, receipt = this_round(args.state, kind)
        tenant = args.tenant or report["organization"]
        if args.verify_only:
            binding = json.loads((args.state / kind.source_file).read_text())
            delivered = None
        else:
            if not args.dataset_base or not os.environ.get(TOKEN):
                parser.error(f"a delivery needs --dataset-base and the bearer of a read-only account in {TOKEN}")
            binding, raw = locate(DatasetReader(args.dataset_base, os.environ[TOKEN]), receipt, kind)
            (args.state / kind.source_file).write_text(json.dumps(binding, indent=2) + "\n")
            (args.state / kind.downloaded_file).write_bytes(raw)
            delivered = deliver(args.endpoint, tenant, source_of(binding), raw, kind)
        store = read_back(args.endpoint, tenant, kind)
    except (ReceiptError, SyncError, OSError, ValueError, KeyError, TypeError, IndexError) as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1
    problems = differences(binding, tenant, store, kind)
    print(json.dumps({"revision": report["revision"], "tenant": tenant, "source": source_of(binding), "lines": binding["lines"],
                      "receipt": {name: binding[name] for name in binding if name not in SOURCE_ATTRIBUTES and name != "lines"},
                      "delivered": delivered, "store": plain(store), "problems": problems, "accepted": not problems}, indent=2))
    return 0 if not problems else 1


def run(state, report, *, dataset, reader, bin_dir, pull="missing", type_id=DEFAULT):
    """This round's v3 delivered to a Cassandra store, and three other sources refused by the same assertion.

    Called by the lingxing smoke, by the noon smoke with ``type_id="OrderLinePrice"`` or by the
    amazon smoke with ``type_id="SettlementLine"``, while its Dataset service is up: ``dataset``
    is the base URL of that service and ``reader`` the bearer of its read-only account. The store
    is the Compose stack of ``ontology_sync_replay.py``, started with the object service binary
    of ``bin_dir`` and stopped before returning. The delivery entry is run twice, by two
    processes with the same command, as a downstream consumer runs it again after a restart:
    the first creates the three objects, the second none, and each reads the tenant back on its
    own. Each process prints a report; each report is kept as its own file and the whole evidence
    is written to ``<prefix>-downstream.json``. The returned summary is what the smoke's report
    keeps. An AssertionError is raised when a check failed, after the evidence was written.
    """
    kind = KINDS[type_id]
    state = Path(state)
    build = build_of(report, VERSION)
    tenant = report["organization"]
    env = {**os.environ, TOKEN: reader}
    evidence = {"revision": report["revision"], "tenant": tenant,
                "client_sha256": hashlib.sha256((HERE / "ontology_sync.py").read_bytes()).hexdigest(),
                "entry_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "checks": []}

    def check(label, condition, detail=""):
        print(f"{'PASS' if condition else 'FAIL'} {label}" + ("" if condition else ": " + detail), flush=True)
        evidence["checks"].append({"name": label, "passed": bool(condition), "detail": detail})

    def command(script, name, *arguments):
        """One entry of this directory run as a process. Its report is kept as <prefix>-<name>.json."""
        argv = [sys.executable, "-I", str(HERE / script), *map(str, arguments)]
        done = subprocess.run(argv, capture_output=True, text=True, env=env)
        printed = _json(done.stdout)
        if printed is not None:
            (state / f"{kind.prefix}-{name}.json").write_text(json.dumps(printed, indent=2) + "\n")
        return {"command": argv, "exit": done.returncode, "stderr": done.stderr.strip(), "report": printed,
                "evidence": f"{kind.prefix}-{name}.json" if printed is not None else None}

    try:
        args = argparse.Namespace(bin_dir=bin_dir, project=f"of-ontology-sync-{os.getpid()}", pull=pull, startup_timeout=240)
        with Stack(args) as stack:
            endpoint = evidence["endpoint"] = stack.endpoint
            evidence["stack"], evidence["backend"] = stack.describe(), backend_is_cassandra(endpoint, tenant)

            # The default type is named by no argument, so the commands of #95 are unchanged.
            typed = () if kind is PRODUCT_COST else ("--type", kind.type_id)
            # This round's v3, delivered by the entry: the same command, run by two processes.
            delivery_command = ("--state", state, "--dataset-base", dataset, "--endpoint", endpoint, *typed)
            first = command("ontology_sync_published.py", "delivery-1", *delivery_command)
            second = command("ontology_sync_published.py", "delivery-2", *delivery_command)
            deliveries = evidence["deliveries"] = [first, second]
            binding = _json((state / kind.source_file).read_text())
            if not (first["report"] and second["report"] and binding):
                raise AssertionError("a delivery printed no report:\n" + first["stderr"] + second["stderr"])
            check("this round's v3 is delivered by the entry, which creates three objects and is accepted, exit 0",
                  first["exit"] == 0 and first["report"]["accepted"] and first["report"]["delivered"]["created"] == len(kind.expected),
                  first["stderr"] + json.dumps(first["report"]["problems"]))
            check("the same command run again by another process creates nothing, reads the same three objects back "
                  "and is accepted, exit 0",
                  second["exit"] == 0 and second["report"]["accepted"] and second["report"]["delivered"]["created"] == 0
                  and not any(item["created"] for item in second["report"]["delivered"]["results"])
                  and second["report"]["store"] == first["report"]["store"],
                  second["stderr"] + json.dumps(second["report"]["problems"]))
            check("the two processes ran the same command and each located the same source",
                  first["command"] == second["command"] and first["report"]["source"] == second["report"]["source"]
                  and [item["object_id"] for item in first["report"]["delivered"]["results"]]
                  == [item["object_id"] for item in second["report"]["delivered"]["results"]])
            if kind.whole is not None:
                # The rows as a whole, as the kind states them; its function raises AssertionError.
                sentence, judge = kind.whole
                try:
                    judge(first["report"]["store"])
                    detail = ""
                except (AssertionError, KeyError, TypeError) as wrong:
                    detail = str(wrong)
                check(sentence, not detail, detail)
            differing = receipt_differs(binding, report, build)
            check("the run receipt the entry followed names the v3 the smoke recorded", not differing, "; ".join(differing))
            downloaded = (state / kind.downloaded_file).read_bytes()
            check("the read-only account downloaded the bytes the smoke's reader downloaded, and the hash is of those bytes",
                  downloaded == (state / build["artifact"]).read_bytes()
                  and hashlib.sha256(downloaded).hexdigest() == binding["sha256"] == build["sha256"],
                  f"{len(downloaded)} bytes, hash {hashlib.sha256(downloaded).hexdigest()}")

            # Three other sources, each delivered the same way into a tenant of its own and
            # read back through the same assertion. A refusal counts only after the tenant is
            # shown to hold exactly that source: delivered without error, one object per row of
            # its file with its identity, file and SHA256, and nothing else.
            expected = source_of(binding)
            refused = evidence["refused"] = {}
            for name, version, differs in OTHER_SOURCES:
                label = name.replace("_", " ")
                other_tenant = str(uuid.uuid4())
                entry = refused[name] = {"tenant": other_tenant, "expected_differs_in": list(differs)}
                if version is None:
                    # The default call of ontology_sync.py delivers the embedded copy of the archived v3.
                    _, _, intended, raw = archived_transaction(None, kind.type_id)
                    done = command("ontology_sync.py", f"refused-{name}-delivered", endpoint, "--tenant", other_tenant, *typed)
                    default = done["report"] or {}
                    entry["delivered"] = {"command": done["command"], "exit": done["exit"], "evidence": done["evidence"],
                                          "source": default.get("source"), "created": default.get("created")}
                else:
                    intended, raw = locate(DatasetReader(dataset, reader),
                                           json.loads((state / f"pipeline-{build_of(report, version)['ordinal']}-run.json").read_text()),
                                           kind)
                    results = kind.sync(endpoint, other_tenant, rows_of(raw, source_of(intended)))
                    entry["delivered"] = {"source": source_of(intended), "created": sum(result.created for result in results)}
                lines, values = lines_and_values([kind.row(line) for line in raw.splitlines()], kind)
                entry["intended"] = {"source": source_of(intended), "rows": len(values), "values": plain(values)}
                judged = entry["judged"] = command("ontology_sync_published.py", f"refused-{name}", "--state", state, "--endpoint",
                                                   endpoint, "--tenant", other_tenant, "--verify-only", *typed)
                verdict = judged["report"] or {}
                store = verdict.get("store") or {}
                held = _held(store, kind)
                entry["held"] = plain(held)
                own = row_differences(source_of(intended), lines, values, other_tenant, store, kind)
                entry["holds_its_own_source"] = not own
                # The archived source is delivered by a process of its own, the two others in this one.
                delivered_ok = version is not None or entry["delivered"]["exit"] == 0
                check(f"the {label} is delivered and its tenant holds exactly its {len(values)} objects of that source, "
                      "each with its file and SHA256, and nothing else",
                      delivered_ok and entry["delivered"]["created"] == len(values) and not own, "; ".join(own))
                entry["same_as_this_round"] = {attribute: (held["source"] or {}).get(attribute) == expected[attribute]
                                               for attribute in expected}
                entry["differs_in"] = [attribute for attribute, same in entry["same_as_this_round"].items() if not same]
                entry.update(expected="refused, exit 1", problems=verdict.get("problems") or [],
                             same_values_as_this_round=held["values"] == kind.expected)
                # The refusal must be for the identity alone: the assertion names each attribute that
                # differs and nothing else of the _source, and the id set. The archived transaction and
                # the v1 hold the values of this round; only the v2 holds others.
                named = {problem.split(" _source.")[1].split(":")[0] for problem in entry["problems"] if " _source." in problem}
                check(f"the {label} delivered into a tenant of its own is refused by the same assertion "
                      f"because {', '.join(differs)} differ, exit 1",
                      judged["exit"] == 1 and not verdict.get("accepted") and tuple(entry["differs_in"]) == differs
                      and named == set(differs) and any("is not" in problem for problem in entry["problems"])
                      and entry["same_values_as_this_round"] == (version in (None, 1)),
                      judged["stderr"] + json.dumps(verdict.get("store")))

            again = evidence["readback"] = command("ontology_sync_published.py", "readback", "--state", state, "--endpoint",
                                                   endpoint, "--tenant", tenant, "--verify-only", *typed)
            check("this round's tenant still holds the same three objects after the other sources were delivered, exit 0",
                  again["exit"] == 0 and (again["report"] or {}).get("store") == first["report"]["store"], again["stderr"])
    except Exception as error:  # recorded, so that the evidence is written whatever happened
        evidence["error"] = f"{type(error).__name__}: {error}"
    evidence["passed"] = "error" not in evidence and all(check["passed"] for check in evidence["checks"])
    (state / kind.downstream_file).write_text(json.dumps(evidence, indent=2) + "\n")
    failed = [check["name"] + ": " + check["detail"] for check in evidence["checks"] if not check["passed"]]
    assert evidence["passed"], "; ".join([evidence.get("error", "")] + failed)
    return {"evidence": kind.downstream_file, "source": first["report"]["source"],
            "deliveries": [{"exit": delivery["exit"], "created": delivery["report"]["delivered"]["created"],
                            "count": delivery["report"]["store"]["total"], "evidence": delivery["evidence"]}
                           for delivery in (first, second)],
            "refused": {name: {"held": entry["held"]["count"], "holds_its_own_source": entry["holds_its_own_source"],
                               "exit": entry["judged"]["exit"], "differs_in": entry["differs_in"],
                               "same_as_this_round": entry["same_as_this_round"]}
                        for name, entry in evidence["refused"].items()}}


if __name__ == "__main__":
    raise SystemExit(main())

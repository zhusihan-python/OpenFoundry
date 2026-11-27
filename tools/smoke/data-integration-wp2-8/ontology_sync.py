"""Idempotent delivery of archived source rows as ontology objects.

The generic create route (``POST /api/v1/ontology/types/{type}/objects``) mints a
new TimeUUID per request, so replaying one source transaction stores every row
again. This client is a separate sync entry for callers that replay transactions.
It derives one stable object id per source key and creates the object through the
object service's existing conditional put (``INSERT ... IF NOT EXISTS``); when the
object already exists it reads the object back and accepts it only if it is the
same row.

One synced row is identified by the tenant, the object type, ``_source.transaction``
and the caller's key properties (for ProductCost: ``sku``). The ``_source`` map must
carry all of ``version``, ``dataset``, ``transaction``, ``file``, ``sha256`` and
``line``. An existing object is accepted only when its tenant, type, complete
``_source`` and every other property equal the request; any difference raises
``SyncConflictError`` and nothing is written.

Only a Cassandra-backed service is supported. Its conditional put never overwrites
an existing object, which is what makes concurrent and retried deliveries of one
key converge on a single object.

Sync the archived ProductCost transaction once into a running object service:

    python3 ontology_sync.py ENDPOINT [--evidence-root DIR] [--tenant ID]

The lingxing v3 transaction of revision c48a709f9 is embedded below;
``--evidence-root`` reads it from an evidence directory instead. The command
syncs each row through this client and prints one JSON report. A failing row
stops the delivery with exit code 1; running it again then confirms the rows
that were already stored instead of duplicating them.

``--type OrderLinePrice`` syncs the archived noon v3 transaction of the same
revision the same way; the order line number ``item_nr`` is its key property.

``--type SettlementLine`` syncs the archived Amazon v3 transaction of the same
revision the same way. A settlement line has no key property of its own: its second
and third line are an order and the refund of one order id and one SKU, and only
their place in the file tells them apart. A SettlementLine row is identified by
``_source.file`` and ``_source.line`` next to the tenant, the type and the
transaction.
"""

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path

SOURCE_ATTRIBUTES = ("version", "dataset", "transaction", "file", "sha256", "line")
# object-database-service stores this owner on objects created by unauthenticated callers.
DEFAULT_OWNER = "00000000-0000-1000-8000-000000000001"
# The properties that identify one ProductCost row inside its source transaction.
PRODUCT_COST_KEY = ("sku",)
# The properties that identify one OrderLinePrice row inside its source transaction.
ORDER_LINE_PRICE_KEY = ("item_nr",)
# What identifies one SettlementLine row inside its source transaction: two lines of one order
# and SKU differ only by their place in the source file.
SETTLEMENT_LINE_KEY = ("_source.file", "_source.line")

_STATUS_PATH = "/api/v1/object-database/status"
_OBJECT_PATH = "/api/v1/object-database/objects/{tenant}/{object_id}"
# 100ns intervals between 1582-10-15 and 1970-01-01. Adding it keeps a derived
# timestamp inside 1970..3797 instead of spreading over the whole TimeUUID range.
_UNIX_OFFSET = 0x01B21DD213814000


class SyncError(Exception):
    """Base class for sync failures."""


class InvalidSourceError(SyncError):
    """The row has no usable source identity: an incomplete ``_source``, a missing key
    property, or an artifact that does not hash to ``_source.sha256``. Nothing was sent."""


class SyncConflictError(SyncError):
    """An object already holds the key with a different type, tenant or content."""


class SyncBackendError(SyncError):
    """The object service is not a ready Cassandra store or answered unexpectedly."""


class _Transient(Exception):
    """The request may be repeated: transport failure, 5xx or 429."""


@dataclass(frozen=True)
class SyncResult:
    """``created`` is true when the last evaluated put inserted the object."""

    object_id: str
    created: bool


def stable_object_id(tenant, type_id, transaction, key):
    """Return the TimeUUID (version 1) of the object that holds one source key.

    ``key`` maps each key property to its value; property order does not matter.
    121 bits of SHA-256 fill the timestamp, clock sequence and node fields. The
    other 7 bits are the version and variant of a legal TimeUUID, which the
    Cassandra ``object_id timeuuid`` column requires.
    """
    for name, value in key.items():
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise InvalidSourceError(f"key property {name} must be a string or integer")
    material = json.dumps(
        [tenant, type_id, transaction, [[name, key[name]] for name in sorted(key)]],
        separators=(",", ":"),
    )
    digest = hashlib.sha256(material.encode("utf-8")).digest()
    timestamp = _UNIX_OFFSET + (int.from_bytes(digest[:8], "big") & ((1 << 59) - 1))
    raw = bytearray(16)
    raw[0:4] = (timestamp & 0xFFFFFFFF).to_bytes(4, "big")
    raw[4:6] = ((timestamp >> 32) & 0xFFFF).to_bytes(2, "big")
    raw[6:8] = (((timestamp >> 48) & 0x0FFF) | (1 << 12)).to_bytes(2, "big")
    raw[8] = 0x80 | (digest[8] & 0x3F)
    raw[9] = digest[9]
    raw[10:16] = digest[10:16]
    return str(uuid.UUID(bytes=bytes(raw)))


def source_attributes(properties):
    """Return the complete ``_source`` map of a row or raise InvalidSourceError."""
    source = properties.get("_source")
    if not isinstance(source, dict):
        raise InvalidSourceError("_source must be an object with " + ", ".join(SOURCE_ATTRIBUTES))
    missing = [name for name in SOURCE_ATTRIBUTES if name not in source]
    if missing:
        raise InvalidSourceError("_source is missing " + ", ".join(missing))
    for name in ("version", "line"):
        value = source[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise InvalidSourceError(f"_source.{name} must be a positive integer")
    for name in ("dataset", "transaction", "file", "sha256"):
        if not isinstance(source[name], str) or not source[name]:
            raise InvalidSourceError(f"_source.{name} must be a non-empty string")
    return source


# The lingxing v3 transaction that first showed the duplicate ProductCost objects,
# at revision c48a709f9. It is the default input of sync_product_costs and of the
# command above.
ARCHIVED = {
    "revision": "c48a709f91408a1182b4d72cac03d21cdfa01a0b",
    "tenant": "84b2d642-c992-4cc3-af83-7b28062b1d37",
    "source": {
        "version": 3,
        "dataset": "5a2150df-b480-47b7-939b-288025692fea",
        "transaction": "56ea4fbc-99e4-4a07-98b8-de12f1f272c7",
        "file": "96bdec4a-2fa1-4be3-b0fc-ce79e762c593",
        "sha256": "846c2b4dbd3314a84f67c36031a06547f7c60341970fde512aa6afd975c0d6e8",
    },
    "artifact": b'{"cg_price":"0.00","sku":"0007"}\n{"cg_price":"9.25","sku":"W-2"}\n'
                b'{"cg_price":"17.40","sku":"D3"}\n',
}

# The noon v3 transaction of the same revision, three order lines. It is the input of
# sync_order_line_prices and of the command with ``--type OrderLinePrice``. The noon
# report names no organization; the tenant is the one of its archived refusal run.
ARCHIVED_ORDER_LINE_PRICES = {
    "revision": "c48a709f91408a1182b4d72cac03d21cdfa01a0b",
    "tenant": "015cae6e-495e-4027-aa74-5e5acc8b09b2",
    "source": {
        "version": 3,
        "dataset": "016dbd77-fe2e-4d60-a434-f34647e95e28",
        "transaction": "1f600730-d3f1-4ffe-8cd0-c4058d27ced0",
        "file": "a155352f-77b2-4e7c-af98-8e988f9e1b9b",
        "sha256": "2ce181d593a204b707822233a69835875ac0aa74acce5e9176393b18e43529fd",
    },
    "artifact": b'{"currency":"AED","item_nr":"NRPL0001-1","market":"AE",'
                b'"order_nr":"NRPL0001","store":"RPL01-NAE","unit_price":"49"}\n'
                b'{"currency":"AED","item_nr":"NRPL0002-1","market":"AE",'
                b'"order_nr":"NRPL0002","store":"RPL01-NAE","unit_price":"120"}\n'
                b'{"currency":"AED","item_nr":"NRPL0003-1","market":"AE",'
                b'"order_nr":"NRPL0003","store":"RPL01-NAE","unit_price":"30"}\n',
}

# The Amazon v3 transaction of the same revision, three settlement lines: an order, and the
# order and refund of one SKU that differ only by their line. It is the input of
# sync_settlement_lines and of the command with ``--type SettlementLine``. The artifact holds
# the names as UTF-8; the escapes below write the same bytes.
ARCHIVED_SETTLEMENT_LINES = {
    "revision": "c48a709f91408a1182b4d72cac03d21cdfa01a0b",
    "tenant": "518e9aa4-3c4d-488f-a372-c1d5bf1b6e3c",
    "source": {
        "version": 3,
        "dataset": "e5cd197b-4337-4d42-b8d2-410b3174e41e",
        "transaction": "18b8eb56-d952-4feb-9a61-d4d06f79f296",
        "file": "8bfb76ec-9f57-4e49-9ba5-f2f3cffdfdf3",
        "sha256": "2adc3f19f70041f150364fa81f33d275c19667204e68efabd28389551e2ce464",
    },
    "artifact": (
        '{"date/time":"Oct 2, 2026 08:05:00 AM PDT","description":"Sample item C","fba fees":0,"gift wrap '
        'credits":3.50,"giftwrap credits tax":0.28,"marketplace withheld tax":0,"order '
        'id":"000-0000000-0000003","other":0,"other transaction fees":0,"product sales":59.99,"product sales '
        'tax":4.80,"promotional rebates":0,"promotional rebates tax":0,"quantity":1,"regulatory '
        'fee":0,"selling fees":-9.00,"settlement id":1000000010,"shipping credits":0,"shipping credits '
        'tax":0,"sku":"DEMO-SKU-C3","tax on regulatory fee":0,"total":59.57,"type":"Order"}\n'
        '{"date/time":"Oct 12, 2026 11:20:00 AM PDT","description":"Caf\u00e9 sampler \u00e0 la carte","fba '
        'fees":-4.10,"gift wrap credits":0,"giftwrap credits tax":0,"marketplace withheld tax":-1.20,"order '
        'id":"000-0000000-0000004","other":0,"other transaction fees":0,"product sales":14.99,"product sales '
        'tax":0,"promotional rebates":0,"promotional rebates tax":0,"quantity":1,"regulatory fee":0,"selling '
        'fees":-2.25,"settlement id":1000000011,"shipping credits":0,"shipping credits '
        'tax":0,"sku":"DEMO-SKU-D4","tax on regulatory fee":0,"total":7.44,"type":"Order"}\n'
        '{"date/time":"Oct 25, 2026 04:00:00 PM PDT","description":"Caf\u00e9 sampler \u00e0 la carte","fba '
        'fees":4.10,"gift wrap credits":0,"giftwrap credits tax":0,"marketplace withheld tax":1.20,"order '
        'id":"000-0000000-0000004","other":0,"other transaction fees":0,"product sales":-14.99,"product '
        'sales tax":0,"promotional rebates":0,"promotional rebates tax":0,"quantity":-1,"regulatory '
        'fee":0,"selling fees":2.25,"settlement id":1000000012,"shipping credits":0,"shipping credits '
        'tax":0,"sku":"DEMO-SKU-D4","tax on regulatory fee":0,"total":-7.44,"type":"Refund"}\n'
    ).encode("utf-8"),
}

# The archived transaction of each synced type and, below an evidence root, the directory
# and report that hold it.
ARCHIVED_TRANSACTIONS = {
    "ProductCost": (ARCHIVED, "lingxing", "product-report.json"),
    "OrderLinePrice": (ARCHIVED_ORDER_LINE_PRICES, "noon", "noon-report.json"),
    "SettlementLine": (ARCHIVED_SETTLEMENT_LINES, "amazon", "amazon-report.json"),
}


def archived_transaction(root=None, type_id="ProductCost"):
    """Return (revision, tenant, source, raw v3 artifact) of the archived transaction of ``type_id``.

    ``type_id`` is ProductCost, OrderLinePrice or SettlementLine. Without ``root`` the
    embedded copy is returned. ``root`` is an evidence directory holding manifest.json, the
    report of the type's source and the v3 artifact.
    """
    archived, directory, report_name = ARCHIVED_TRANSACTIONS[type_id]
    if root is None:
        return archived["revision"], archived["tenant"], archived["source"], archived["artifact"]
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text())
    report = json.loads((root / directory / report_name).read_text())
    if report["revision"] != manifest["revision"]:
        raise InvalidSourceError(
            f"manifest revision {manifest['revision']} differs from the report revision {report['revision']}"
        )
    build, = [item for item in manifest["sources"][directory]["builds"] if item["output_version"] == 3]
    source = {"version": 3, "dataset": report["pipeline"]["output_dataset"], "transaction": build["output_transaction"],
              "file": build["output_file_id"], "sha256": build["sha256"]}
    if type_id != "OrderLinePrice":
        tenant = report["organization"]
    else:  # the noon report names no organization; the tenant is the one of its archived refusal run
        tenant = report["pipeline"]["refusals"]["crossed_transaction"]["tenant"]
    return report["revision"], tenant, source, (root / directory / build["artifact"]).read_bytes()


def rows_of(raw, source):
    """The archived rows, each carrying its complete _source."""
    digest = hashlib.sha256(raw).hexdigest()
    if digest != source["sha256"]:
        raise InvalidSourceError(f"the archived artifact hashes to {digest}, not to _source.sha256 {source['sha256']}")
    return [{**json.loads(line), "_source": {**source, "line": n}} for n, line in enumerate(raw.splitlines(), 1)]


def sync_product_costs(endpoint, tenant, rows):
    """Sync archived ProductCost rows once, in order. Returns one SyncResult per row."""
    client = OntologySyncClient(endpoint, tenant, key_properties=PRODUCT_COST_KEY)
    return [client.sync("ProductCost", row) for row in rows]


def sync_order_line_prices(endpoint, tenant, rows):
    """Sync archived OrderLinePrice rows once, in order. Returns one SyncResult per row."""
    client = OntologySyncClient(endpoint, tenant, key_properties=ORDER_LINE_PRICE_KEY)
    return [client.sync("OrderLinePrice", row) for row in rows]


def sync_settlement_lines(endpoint, tenant, rows):
    """Sync archived SettlementLine rows once, in order. Returns one SyncResult per row."""
    client = OntologySyncClient(endpoint, tenant, key_properties=SETTLEMENT_LINE_KEY)
    return [client.sync("SettlementLine", row) for row in rows]


# What --type selects: the property that identifies a row in the report and the entry that syncs it.
# For SettlementLine that is the settlement id; the object is keyed by _source.file and _source.line.
ENTRIES = {"ProductCost": ("sku", sync_product_costs), "OrderLinePrice": ("item_nr", sync_order_line_prices),
           "SettlementLine": ("settlement id", sync_settlement_lines)}


def row_key(properties, source, key_properties):
    """Return the key of one row: the value of each key property, by name.

    A key property named ``_source.<attribute>`` is read from the row's ``_source``,
    which ``source_attributes`` has checked to be complete; any other name is a
    property of the row.
    """
    missing = [name for name in key_properties if not name.startswith("_source.") and name not in properties]
    if missing:
        raise InvalidSourceError("row is missing key properties " + ", ".join(missing))
    return {name: source[name[len("_source."):]] if name.startswith("_source.") else properties[name]
            for name in key_properties}


def _same_json(left, right):
    """JSON equality; booleans never equal numbers."""
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_same_json(left[k], right[k]) for k in left)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(map(_same_json, left, right))
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return left == right
    return type(left) is type(right) and left == right


def _open(request, timeout):
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


class OntologySyncClient:
    """Sync rows of one tenant through the object service's conditional put and get.

    ``key_properties`` names the properties that identify a row inside one source
    transaction; a name of the form ``_source.<attribute>`` names an attribute of the
    row's ``_source``. ``opener`` replaces the transport in tests: it receives the
    ``urllib.request.Request`` and the timeout and returns the response body.
    """

    def __init__(self, endpoint, tenant, *, key_properties, owner=DEFAULT_OWNER, timeout=10,
                 attempts=4, backoff=0.2, opener=None):
        self.endpoint = endpoint.rstrip("/")
        self.tenant = tenant
        self.key_properties = tuple(key_properties)
        self.owner = owner
        self.timeout = timeout
        self.attempts = attempts
        self.backoff = backoff
        self._open = _open if opener is None else opener
        self._cassandra = False

    def require_cassandra_backend(self):
        """Refuse to sync unless the service stores objects in a ready Cassandra store."""
        if self._cassandra:
            return
        status = self._retry(lambda: self._json("GET", _STATUS_PATH), "object service status")
        if status.get("backend") != "cassandra" or status.get("ready") is not True:
            raise SyncBackendError(
                f"object service backend is {status.get('backend')!r}, not a ready Cassandra store"
            )
        self._cassandra = True

    def sync(self, type_id, properties):
        """Create the object of one row, or confirm that the identical object exists."""
        if not isinstance(properties, dict):
            raise InvalidSourceError("properties must be an object")
        source = source_attributes(properties)
        key = row_key(properties, source, self.key_properties)
        object_id = stable_object_id(self.tenant, type_id, source["transaction"], key)
        path = _OBJECT_PATH.format(tenant=urllib.parse.quote(self.tenant, safe=""), object_id=object_id)
        self.require_cassandra_backend()
        body = {"type_id": type_id, "version": 1, "payload": properties, "owner": self.owner}
        return self._retry(lambda: self._attempt(path, object_id, body, type_id, properties, source), f"sync {path}")

    def _attempt(self, path, object_id, body, type_id, properties, source):
        outcome = self._json("PUT", path, body)
        kind = outcome.get("outcome")
        if kind == "inserted":
            return SyncResult(object_id, True)
        if kind != "version_conflict":
            raise SyncBackendError(f"unexpected put outcome {outcome!r}")
        existing = self._json("GET", path)
        self._verify(existing, object_id, type_id, properties, source)
        return SyncResult(object_id, False)

    def _verify(self, existing, object_id, type_id, properties, source):
        """Reject an object that holds the key with any other type, tenant or content."""
        if (existing.get("tenant"), existing.get("type_id")) != (self.tenant, type_id):
            raise SyncConflictError(
                f"object {object_id} belongs to tenant {existing.get('tenant')!r} and type "
                f"{existing.get('type_id')!r}, not tenant {self.tenant!r} and type {type_id!r}"
            )
        stored = existing.get("payload")
        stored_source = stored.get("_source") if isinstance(stored, dict) else None
        for name in SOURCE_ATTRIBUTES:
            if not isinstance(stored_source, dict) or not _same_json(stored_source.get(name), source[name]):
                raise SyncConflictError(f"object {object_id} was synced with a different _source.{name}")
        if not _same_json(stored, properties):
            raise SyncConflictError(f"object {object_id} holds the same key with different properties")

    def _retry(self, operation, what):
        last = None
        for attempt in range(1, self.attempts + 1):
            try:
                return operation()
            except _Transient as error:
                last = error
                if attempt < self.attempts:
                    time.sleep(self.backoff * attempt)
        raise SyncBackendError(f"{what}: {last}; gave up after {self.attempts} attempts") from last

    def _json(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(self.endpoint + path, data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            raw = self._open(request, self.timeout)
        except urllib.error.HTTPError as error:
            with error:  # the error is the response; read it before it is closed
                detail = error.read()
            if error.code < 500 and error.code != 429:
                raise SyncBackendError(f"{method} {path} returned {error.code}: {detail!r}") from error
            raise _Transient(f"{method} {path} returned {error.code}") from error
        except OSError as error:
            raise _Transient(f"{method} {path} failed: {error!r}") from error
        try:
            return json.loads(raw)
        except ValueError as error:
            raise SyncBackendError(f"{method} {path} returned invalid JSON") from error


def main(argv=None):
    """The command that syncs one archived transaction once."""
    parser = argparse.ArgumentParser(
        description="Sync the archived lingxing ProductCost transaction once, or the noon OrderLinePrice "
                    "or Amazon SettlementLine transaction with --type.")
    parser.add_argument("endpoint", help="object service base URL, for example http://127.0.0.1:50125")
    parser.add_argument("--evidence-root", type=Path,
                        help="directory holding manifest.json and the report of the archived transaction's source")
    parser.add_argument("--tenant", help="tenant to sync into; default: the tenant of the archived report")
    parser.add_argument("--type", choices=tuple(ENTRIES), default="ProductCost",
                        help="object type of the archived transaction; default: ProductCost")
    args = parser.parse_args(argv)
    key, sync = ENTRIES[args.type]
    try:
        revision, tenant, source, raw = archived_transaction(args.evidence_root, args.type)
        tenant = args.tenant or tenant
        rows = rows_of(raw, source)
        results = sync(args.endpoint.rstrip("/"), tenant, rows)
    except (SyncError, OSError, ValueError) as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1
    created = sum(result.created for result in results)
    print(json.dumps({
        "revision": revision, "tenant": tenant, "source": source,
        "results": [{"line": row["_source"]["line"], key: row[key], "object_id": result.object_id,
                     "created": result.created} for row, result in zip(rows, results)],
        "created": created, "existing": len(results) - created,
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

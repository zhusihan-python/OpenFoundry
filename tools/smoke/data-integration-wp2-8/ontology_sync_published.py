"""Deliver the v3 that a Pipeline run of this round published, located from its run receipt.

The copies embedded in ``ontology_sync.py`` are the v3 outputs of one earlier run, at
revision c48a709f9. They hold the same SKUs, costs and SHA256 as the v3 of every later
round, and so does the v1 of each round, which replays the same input: neither the values
nor the hash of the bytes tells this round's v3 from the archived transaction or from
another file. What names it is the receipt of its own run. A read-only reader starts from
the run receipt the lingxing smoke saved, finds the one transaction of the output dataset
whose ``metadata.transaction_rid`` repeats the ``transaction_rid`` of the committed
resource of that run, the version that cut it and its one file, downloads the file and
hashes the bytes itself. The rows are delivered once through the conditional put of
``ontology_sync.py`` and the tenant is read back through the list route and then the
detail route of each listed object.

One assertion accepts or refuses what a tenant holds. It accepts a tenant that holds
exactly the three objects whose ids the client derives from the tenant, the type, the
transaction of the receipt and each SKU, each with the cost of its row and an ``_source``
that repeats the dataset, version, transaction, file and SHA256 of that read and the line
of its SKU in that file. The same assertion refuses three other sources delivered the same
way: the archived transaction, and the v1 and v2 this round published, of which v1 has the
bytes and the hash of v3.

    python3 ontology_sync_published.py --state DIR --dataset-base URL --endpoint URL [--tenant ID]
    python3 ontology_sync_published.py --state DIR --endpoint URL --tenant ID --verify-only

``--state`` is the evidence directory the lingxing smoke of this round printed. The
read-only account is the bearer in ``WP34_TOKEN`` and ``--dataset-base`` defaults to
``WP34_DVS``. The first form is the entry of a downstream consumer. Each run delivers once
and writes product-cost-source.json, the source the receipt names, and product-cost-v3.ndjson,
the bytes it downloaded. The first run creates the three objects; the same command run again,
after a restart of the downstream process, creates none and is accepted the same way, because
a redelivery of the same key and content succeeds by #90. The second form reads a tenant back
and judges it against product-cost-source.json: nothing is synced and the Dataset service is
not asked. Both print one JSON report with the readback and return 0 only when the assertion
accepts the tenant. ``run.sh lingxing`` runs the first form twice, as two processes with the
same command, and the second on three tenants that received the other sources, against a
Cassandra store it starts and stops itself.
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
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _sibling(name):
    """A module of this directory, also under ``python3 -I``, which puts no directory on sys.path."""
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, HERE / (name + ".py"))
        sys.modules[name] = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(sys.modules[name])
    return sys.modules[name]


_sibling("ontology_sync"), _sibling("ontology_sync_replay")
from ontology_sync import SOURCE_ATTRIBUTES, SyncError, rows_of, stable_object_id, sync_product_costs  # noqa: E402
from ontology_sync_replay import ROUTE, Stack, backend_is_cassandra, call  # noqa: E402

# The costs this round's v3 holds, each a string: SKU and cost.
EXPECTED_COSTS = {"0007": "0.00", "W-2": "9.25", "D3": "17.40"}
# The lingxing smoke publishes the costs of product v1, v2 and v1 again as output v1, v2 and v3.
VERSION = 3
TOKEN = "WP34_TOKEN"  # the bearer of the read-only account
SOURCE_FILE = "product-cost-source.json"
DOWNLOADED_FILE = "product-cost-v3.ndjson"
DOWNSTREAM_FILE = "product-cost-downstream.json"
# What the smoke recorded of one build, by the name of the source the reader located.
RECORDED = {"run_id": "pipeline_run_id", "pipeline_output_transaction_rid": "pipeline_output_transaction_rid",
            "content_hash": "pipeline_content_hash", "dataset": "output_dataset", "version": "output_version",
            "transaction": "output_transaction", "file": "output_file_id", "sha256": "sha256",
            "size_bytes": "size_bytes", "row_count": "row_count"}
# The other sources the assertion must refuse: by the output version this round published,
# or None for the archived transaction.
OTHER_SOURCES = (("archived_transaction", None), ("first_output", 1), ("second_output", 2))


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


def locate(reader, receipt):
    """The source one Pipeline run published, as the read-only reader finds it from its run receipt.

    Returns the source and the bytes of its file. The receipt is the answer the Pipeline
    service gave for a succeeded run. Its one committed resource names the output dataset
    and, in ``transaction_rid``, the handle the Pipeline gave the transaction. One
    transaction of the output dataset repeats that handle in ``metadata.transaction_rid``:
    the version that cut it holds one file, whose bytes are downloaded and hashed here.
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
        lines = {row["sku"]: number for number, row in enumerate(rows, 1)}
    except (ValueError, KeyError, TypeError) as error:
        refuse(f"file {file['id']} or its transaction does not parse: {error!r}")
    if (metadata["size_bytes"], metadata["row_count"]) != (len(raw), len(rows)) or preview != rows:
        refuse(f"file {file['id']} holds {len(rows)} rows in {len(raw)} bytes; its transaction records "
               f"{metadata['row_count']} rows in {metadata['size_bytes']} bytes and another preview")
    return {"version": version["version"], "dataset": dataset, "transaction": transaction["id"], "file": file["id"],
            "sha256": hashlib.sha256(raw).hexdigest(), "listed_size_bytes": file["size_bytes"],
            "run_id": run_id, "pipeline_id": pipeline_id, "pipeline_output_transaction_rid": handle,
            "content_hash": content_hash, "size_bytes": len(raw), "row_count": len(rows), "lines": lines}, raw


def read_back(endpoint, tenant):
    """The ProductCost objects of one tenant: the list, then each listed object by id."""
    listed = call(endpoint, "GET", ROUTE + "?per_page=50", tenant)
    return {"total": listed["total"],
            "objects": [call(endpoint, "GET", ROUTE + item["id"], tenant) for item in listed["data"]]}


def _same(left, right):
    """Equality of two scalars where a boolean never equals a number."""
    return type(left) is type(right) and left == right


def differences(binding, tenant, store):
    """Where the ProductCost objects a tenant holds differ from the source ``binding`` names.

    This is the one acceptance of a delivery. It is run against the store after each
    delivery of this round's v3 and against a store that received another source. An
    empty list accepts the tenant.
    """
    source = source_of(binding)
    problems = []
    ids = sorted(obj["id"] for obj in store["objects"])
    expected = sorted(stable_object_id(tenant, "ProductCost", source["transaction"], {"sku": sku}) for sku in EXPECTED_COSTS)
    if store["total"] != len(EXPECTED_COSTS) or ids != expected:
        problems.append(f"{store['total']} listed and {len(ids)} read back, expected the {len(EXPECTED_COSTS)} objects of "
                        f"transaction {source['transaction']}: {ids} is not {expected}")
    for obj in store["objects"]:
        properties = obj["properties"]
        where = f"object {obj['id']}"
        if obj["object_type_id"] != "ProductCost":
            problems.append(f"{where} is a {obj['object_type_id']}")
        sku = properties.get("sku")
        if sku not in EXPECTED_COSTS or sku not in binding["lines"]:
            problems.append(f"{where}: sku {sku!r} is not one of {sorted(EXPECTED_COSTS)}")
            continue
        if properties.get("cg_price") != EXPECTED_COSTS[sku]:
            problems.append(f"{where}: cg_price {properties.get('cg_price')!r}, expected {EXPECTED_COSTS[sku]!r}")
        want = {**source, "line": binding["lines"][sku]}
        got = properties.get("_source")
        for name in SOURCE_ATTRIBUTES:
            if not isinstance(got, dict) or not _same(got.get(name), want[name]):
                problems.append(f"{where} _source.{name}: {got.get(name) if isinstance(got, dict) else got!r}, "
                                f"this round's file reads {want[name]!r}")
    return problems


def deliver(endpoint, tenant, source, raw):
    """The rows of one source, delivered once through the sync client."""
    rows = rows_of(raw, source)
    results = sync_product_costs(endpoint, tenant, rows)
    return {"created": sum(result.created for result in results),
            "results": [{"line": row["_source"]["line"], "sku": row["sku"], "object_id": result.object_id,
                         "created": result.created} for row, result in zip(rows, results)]}


def build_of(report, version):
    """The build the smoke recorded for one output version."""
    build, = [build for build in report["pipeline"]["builds"] if build["output_version"] == version]
    return build


def this_round(state):
    """The report of this round and the saved run receipt of the v3 it published."""
    report = json.loads((state / "product-report.json").read_text())
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


def _held(store):
    """What the objects of a readback hold: their count, the ``_source`` they share and each SKU's cost."""
    objects = store.get("objects") or []
    sources = {json.dumps({key: value for key, value in (obj["properties"].get("_source") or {}).items() if key != "line"},
                          sort_keys=True) for obj in objects}
    return {"count": len(objects), "source": _json(next(iter(sources))) if len(sources) == 1 else None,
            "values": {obj["properties"].get("sku"): obj["properties"].get("cg_price") for obj in objects}}


def main(argv=None):
    """The command that delivers this round's v3 once, or reads a tenant back against it.

    It prints one JSON report: the source the receipt names, what the delivery created
    when it delivered, and the tenant as it was read back afterwards. The exit code is 0
    only when the assertion accepts that readback.
    """
    parser = argparse.ArgumentParser(
        description="Deliver the v3 a Pipeline run of this round published, located from its run receipt.")
    parser.add_argument("--state", type=Path, required=True, help="evidence directory the lingxing smoke of this round printed")
    parser.add_argument("--dataset-base", default=os.environ.get("WP34_DVS"),
                        help="Dataset service the read-only account reads; default: WP34_DVS")
    parser.add_argument("--endpoint", required=True, help="object service base URL")
    parser.add_argument("--tenant", help="tenant to sync into or to read back; default: the organization of this round")
    parser.add_argument("--verify-only", action="store_true",
                        help="read the tenant back against product-cost-source.json; nothing is synced and the Dataset "
                             "service is not asked")
    args = parser.parse_args(argv)
    try:
        report, receipt = this_round(args.state)
        tenant = args.tenant or report["organization"]
        if args.verify_only:
            binding = json.loads((args.state / SOURCE_FILE).read_text())
            delivered = None
        else:
            if not args.dataset_base or not os.environ.get(TOKEN):
                parser.error(f"a delivery needs --dataset-base and the bearer of a read-only account in {TOKEN}")
            binding, raw = locate(DatasetReader(args.dataset_base, os.environ[TOKEN]), receipt)
            (args.state / SOURCE_FILE).write_text(json.dumps(binding, indent=2) + "\n")
            (args.state / DOWNLOADED_FILE).write_bytes(raw)
            delivered = deliver(args.endpoint, tenant, source_of(binding), raw)
        store = read_back(args.endpoint, tenant)
    except (ReceiptError, SyncError, OSError, ValueError, KeyError, TypeError, IndexError) as error:
        print(f"{type(error).__name__}: {error}", file=sys.stderr)
        return 1
    problems = differences(binding, tenant, store)
    print(json.dumps({"revision": report["revision"], "tenant": tenant, "source": source_of(binding), "lines": binding["lines"],
                      "receipt": {name: binding[name] for name in binding if name not in SOURCE_ATTRIBUTES and name != "lines"},
                      "delivered": delivered, "store": store, "problems": problems, "accepted": not problems}, indent=2))
    return 0 if not problems else 1


def run(state, report, *, dataset, reader, bin_dir, pull="missing"):
    """This round's v3 delivered to a Cassandra store, and three other sources refused by the same assertion.

    Called by the lingxing smoke while its Dataset service is up: ``dataset`` is the base
    URL of that service and ``reader`` the bearer of its read-only account. The store is
    the Compose stack of ``ontology_sync_replay.py``, started with the object service
    binary of ``bin_dir`` and stopped before returning. The delivery entry is run twice,
    by two processes with the same command, as a downstream consumer runs it again after a
    restart: the first creates the three objects, the second none, and each reads the
    tenant back on its own. Each process prints a report; each report is kept as its own
    file and the whole evidence is written to product-cost-downstream.json. The returned
    summary is what the product report keeps. An AssertionError is raised when a check
    failed, after the evidence was written.
    """
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
        """One entry of this directory run as a process. Its report is kept as product-cost-<name>.json."""
        argv = [sys.executable, "-I", str(HERE / script), *map(str, arguments)]
        done = subprocess.run(argv, capture_output=True, text=True, env=env)
        printed = _json(done.stdout)
        if printed is not None:
            (state / f"product-cost-{name}.json").write_text(json.dumps(printed, indent=2) + "\n")
        return {"command": argv, "exit": done.returncode, "stderr": done.stderr.strip(), "report": printed,
                "evidence": f"product-cost-{name}.json" if printed is not None else None}

    try:
        args = argparse.Namespace(bin_dir=bin_dir, project=f"of-ontology-sync-{os.getpid()}", pull=pull, startup_timeout=240)
        with Stack(args) as stack:
            endpoint = evidence["endpoint"] = stack.endpoint
            evidence["stack"], evidence["backend"] = stack.describe(), backend_is_cassandra(endpoint, tenant)

            # This round's v3, delivered by the entry: the same command, run by two processes.
            delivery_command = ("--state", state, "--dataset-base", dataset, "--endpoint", endpoint)
            first = command("ontology_sync_published.py", "delivery-1", *delivery_command)
            second = command("ontology_sync_published.py", "delivery-2", *delivery_command)
            deliveries = evidence["deliveries"] = [first, second]
            binding = _json((state / SOURCE_FILE).read_text())
            if not (first["report"] and second["report"] and binding):
                raise AssertionError("a delivery printed no report:\n" + first["stderr"] + second["stderr"])
            check("this round's v3 is delivered by the entry, which creates three objects and is accepted, exit 0",
                  first["exit"] == 0 and first["report"]["accepted"] and first["report"]["delivered"]["created"] == len(EXPECTED_COSTS),
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
            differing = receipt_differs(binding, report, build)
            check("the run receipt the entry followed names the v3 the smoke recorded", not differing, "; ".join(differing))
            downloaded = (state / DOWNLOADED_FILE).read_bytes()
            check("the read-only account downloaded the bytes the smoke's reader downloaded, and the hash is of those bytes",
                  downloaded == (state / build["artifact"]).read_bytes()
                  and hashlib.sha256(downloaded).hexdigest() == binding["sha256"] == build["sha256"],
                  f"{len(downloaded)} bytes, hash {hashlib.sha256(downloaded).hexdigest()}")

            # Three other sources, each delivered the same way into a tenant of its own
            # and read back through the same assertion.
            expected = source_of(binding)
            refused = evidence["refused"] = {}
            for name, version in OTHER_SOURCES:
                other_tenant = str(uuid.uuid4())
                entry = refused[name] = {"tenant": other_tenant}
                if version is None:
                    # The default call of ontology_sync.py delivers the embedded copy of the archived v3.
                    entry["delivered"] = command("ontology_sync.py", f"refused-{name}-delivered", endpoint, "--tenant", other_tenant)
                else:
                    other, raw = locate(DatasetReader(dataset, reader),
                                        json.loads((state / f"pipeline-{build_of(report, version)['ordinal']}-run.json").read_text()))
                    results = sync_product_costs(endpoint, other_tenant, rows_of(raw, source_of(other)))
                    entry["delivered"] = {"source": source_of(other), "created": sum(result.created for result in results)}
                judged = entry["judged"] = command("ontology_sync_published.py", f"refused-{name}", "--state", state, "--endpoint",
                                                   endpoint, "--tenant", other_tenant, "--verify-only")
                verdict = judged["report"] or {}
                entry["held"] = _held(verdict.get("store") or {})
                entry["same_as_this_round"] = {attribute: (entry["held"]["source"] or {}).get(attribute) == expected[attribute]
                                               for attribute in expected}
                entry["differs_in"] = [attribute for attribute, same in entry["same_as_this_round"].items() if not same]
                entry.update(expected="refused, exit 1", problems=verdict.get("problems"))
                check(f"the {name.replace('_', ' ')} delivered into a tenant of its own is refused by the same assertion, exit 1",
                      judged["exit"] == 1 and not verdict.get("accepted") and bool(verdict.get("problems")),
                      judged["stderr"] + json.dumps(verdict.get("store")))

            again = evidence["readback"] = command("ontology_sync_published.py", "readback", "--state", state, "--endpoint",
                                                   endpoint, "--tenant", tenant, "--verify-only")
            check("this round's tenant still holds the same three objects after the other sources were delivered, exit 0",
                  again["exit"] == 0 and (again["report"] or {}).get("store") == first["report"]["store"], again["stderr"])
    except Exception as error:  # recorded, so that the evidence is written whatever happened
        evidence["error"] = f"{type(error).__name__}: {error}"
    evidence["passed"] = "error" not in evidence and all(check["passed"] for check in evidence["checks"])
    (state / DOWNSTREAM_FILE).write_text(json.dumps(evidence, indent=2) + "\n")
    failed = [check["name"] + ": " + check["detail"] for check in evidence["checks"] if not check["passed"]]
    assert evidence["passed"], "; ".join([evidence.get("error", "")] + failed)
    return {"evidence": DOWNSTREAM_FILE, "source": first["report"]["source"],
            "deliveries": [{"exit": delivery["exit"], "created": delivery["report"]["delivered"]["created"],
                            "count": delivery["report"]["store"]["total"], "evidence": delivery["evidence"]}
                           for delivery in (first, second)],
            "refused": {name: {"exit": entry["judged"]["exit"], "differs_in": entry["differs_in"],
                               "same_as_this_round": entry["same_as_this_round"]}
                        for name, entry in evidence["refused"].items()}}


if __name__ == "__main__":
    raise SystemExit(main())

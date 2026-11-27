import contextlib
import hashlib
import io
import json
import os
import re
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock
from urllib.parse import parse_qs, urlsplit

import ontology_sync_published as entry
from ontology_sync import archived_transaction, rows_of, stable_object_id, sync_product_costs
from ontology_sync_published import (DOWNLOADED_FILE, EXPECTED_COSTS, SOURCE_FILE, DatasetReader, ReceiptError,
                                     differences, lines_and_values, locate, main, receipt_differs, row_differences,
                                     source_of)
from test_ontology_sync import ARCHIVED, FakeObjectService, Handler

BASE = "http://dataset.test"
# The list route of an ontology type and, after it, one object by its id.
ONTOLOGY = re.compile(r"^/api/v1/ontology/types/([^/]+)/objects/?([^/]*)$")
# The v1 and the v3 of one round: the same bytes and the same hash. V2 is another snapshot.
V1 = V3 = b'{"cg_price":"0.00","sku":"0007"}\n{"cg_price":"9.25","sku":"W-2"}\n{"cg_price":"17.40","sku":"D3"}\n'
V2 = b'{"cg_price":"2.50","sku":"0007"}\n{"cg_price":"10.75","sku":"N4"}\n'


def published(raw=V3, version=3, handle=None, dataset=None, pipeline=None, **changes):
    """One output as the Dataset service answers for a run: its receipt, version, transaction and file.

    ``dataset`` and ``pipeline`` place the output in a dataset or a pipeline that other
    outputs name. ``handle`` is the transaction_rid the receipt's resource and the
    transaction both name; by default the Pipeline's own handle of the run. ``changes``
    replace fields of the transaction body after it was built.
    """
    run, fresh_pipeline, fresh_dataset, transaction, file = (str(uuid.uuid4()) for _ in range(5))
    pipeline, dataset = pipeline or fresh_pipeline, dataset or fresh_dataset
    handle = handle or f"pipeline-run:{run}:output"
    content_hash = "sha256:" + hashlib.sha256(raw).hexdigest()
    rows = [json.loads(line) for line in raw.splitlines()]
    columns = sorted(rows[0])
    receipt = {"id": run, "run_id": run, "pipeline_id": pipeline, "status": "succeeded", "node_results": [
        {"node_id": "output", "status": "succeeded", "output": {"content_hash": content_hash},
         "output_resources": [{"rid": dataset, "kind": "dataset", "branch": "main", "status": "committed",
                               "transaction_rid": handle}]}]}
    body = {"id": transaction, "dataset_id": dataset, "branch_name": "main", "status": "COMMITTED", "metadata": {
        "transaction_rid": handle, "pipeline_rid": pipeline, "pipeline_output": True, "output_content_hash": content_hash,
        "row_count": len(rows), "size_bytes": len(raw), "preview_columns": columns,
        "preview_rows": [[row[column] for column in columns] for row in rows]}}
    body.update(changes)
    return receipt, {"version": {"version": version, "transaction_id": transaction}, "transaction": body,
                     "file": {"id": file, "transaction_id": transaction, "size_bytes": len(rows)}, "raw": raw}


class FakeDataset:
    """The GET routes the read-only reader asks: the versions of a dataset, one transaction, the files
    of that transaction and the signed address of a file."""

    def __init__(self, *outputs):
        self.requests = []
        self.outputs = []
        self.files = None  # what the files of a transaction list to, instead of the files it holds
        for output in outputs:
            self.publish(output)

    def publish(self, output):
        self.outputs.append(output)
        return output

    def __call__(self, request, timeout):
        url = urlsplit(request.full_url)
        self.requests.append((request.get_method(), request.full_url, request.get_header("Authorization")))
        path, query = url.path, parse_qs(url.query)
        body, status, headers = b"", 200, {}
        if path.endswith("/versions"):
            dataset = path.split("/")[3]
            body = {"data": [o["version"] for o in self.outputs if o["transaction"]["dataset_id"] == dataset],
                    "has_more": False, "next_cursor": None}
        elif "/transactions/" in path:
            found = [o["transaction"] for o in self.outputs if (o["transaction"]["dataset_id"], o["transaction"]["id"])
                     == (path.split("/")[3], path.split("/")[-1])]
            body, status = (found[0], 200) if found else ({"error": "not found"}, 404)
        elif path.endswith("/files"):
            files = self.files if self.files is not None else [
                o["file"] for o in self.outputs if o["file"]["transaction_id"] == query["transaction_id"][0]
                and o["transaction"]["dataset_id"] == path.split("/")[3]]
            body = {"branch": "main", "total": len(files), "files": files}
        elif path.endswith("/download"):
            status, headers = 302, {"Location": BASE + "/signed" + path.removesuffix("/download")}
        elif path.startswith("/signed/"):
            body = next(o["raw"] for o in self.outputs if o["file"]["id"] == path.split("/")[-1])
        else:
            status = 404
        return status, headers, json.dumps(body).encode() if isinstance(body, (dict, list)) else body


def reader_of(*outputs):
    """A read-only reader over a Dataset service that serves the given outputs."""
    dataset = FakeDataset(*outputs)
    return DatasetReader(BASE, "reader-bearer", opener=dataset), dataset


def located(*pairs):
    """What ``locate`` returns for each (receipt, output) pair, from one Dataset service."""
    reader, _ = reader_of(*(output for _, output in pairs))
    return [locate(reader, receipt)[0] for receipt, _ in pairs]


def stored(tenant, raw, source, kind=entry.PRODUCT_COST):
    """The readback of a tenant that holds one object per line of ``raw``, each with its complete ``_source``."""
    objects = []
    for number, line in enumerate(raw.splitlines(), 1):
        row = kind.row(line)
        objects.append({"id": stable_object_id(tenant, kind.type_id, source["transaction"],
                                               kind.key_of(source, kind.name(number, row))),
                        "object_type_id": kind.type_id, "properties": {**row, "_source": {**source, "line": number}}})
    return {"total": len(objects), "objects": objects}


def named(problems, prefix):
    """The attribute each problem names after ``prefix``, such as ``transaction`` after `` _source.``."""
    return {problem.split(prefix)[1].split(":")[0] for problem in problems if prefix in problem}


def wire(obj):
    """An object as the ontology routes of the service answer it."""
    return {"id": obj["id"], "object_type_id": obj["type_id"], "properties": obj["payload"]}


class StoreHandler(Handler):
    """The object service with the list and the detail route of each ontology type the readback uses."""

    def do_GET(self):
        match = ONTOLOGY.match(urlsplit(self.path).path)
        if not match:
            return super().do_GET()
        self.server.requests.append(("GET", self.path))
        type_id, object_id = match.groups()
        tenant = self.headers.get("x-of-tenant")
        with self.server.lock:
            held = sorted((key[1], obj) for key, obj in self.server.objects.items()
                          if key[0] == tenant and obj["type_id"] == type_id)
        if object_id:
            found = [obj for _, obj in held if obj["id"] == object_id]
            return self.reply(wire(found[0]) if found else {"error": "not found"}, 200 if found else 404)
        self.reply({"data": [wire(obj) for _, obj in held], "total": len(held)})


class FakeStore(FakeObjectService):
    """The object service of test_ontology_sync, answering the ontology routes too."""

    def __init__(self):
        super().__init__()
        self.RequestHandlerClass = StoreHandler


class ReaderTests(unittest.TestCase):
    """The errors of the read-only account's transport."""

    def reader_answering(self, *answers):
        calls = []

        def opener(request, timeout):
            calls.append((request.full_url, request.get_header("Authorization")))
            return answers[min(len(calls), len(answers)) - 1]

        return DatasetReader(BASE, "reader-bearer", opener=opener), calls

    def test_an_answer_other_than_200_to_a_read_is_refused(self):
        reader, _ = self.reader_answering((404, {}, b'{"error":"nope"}'))
        with self.assertRaisesRegex(ReceiptError, re.escape("GET /v1/datasets returned 404: ")):
            reader.get("/v1/datasets")
        reader, _ = self.reader_answering((200, {}, b"not json"))
        with self.assertRaisesRegex(ReceiptError, "returned invalid JSON"):
            reader.get("/v1/datasets")

    def test_a_download_that_is_not_answered_with_a_signed_address_is_refused(self):
        for answer in ((200, {}, V3), (302, {}, b"")):
            with self.subTest(answer=answer):
                reader, _ = self.reader_answering(answer)
                with self.assertRaisesRegex(ReceiptError, "expected 302 to the signed address"):
                    reader.download("dataset-d", "f")

    def test_the_signed_address_is_asked_for_the_bytes_and_its_failure_is_refused(self):
        reader, calls = self.reader_answering((302, {"Location": "http://signed.test/f"}, b""), (503, {}, b""))
        with self.assertRaisesRegex(ReceiptError, "signed address of file f returned 503"):
            reader.download("dataset-d", "f")
        self.assertEqual(calls, [(BASE + "/v1/datasets/dataset-d/files/f/download", "Bearer reader-bearer"),
                                 ("http://signed.test/f", None)])


class LocateTests(unittest.TestCase):
    def test_the_receipt_leads_to_one_transaction_its_version_and_its_file_and_the_bytes_are_hashed(self):
        first = published(V1, version=1)
        second = published(V2, version=2, dataset=first[1]["transaction"]["dataset_id"])
        third = published(V3, version=3, dataset=first[1]["transaction"]["dataset_id"])
        reader, _ = reader_of(first[1], second[1], third[1])
        binding, raw = locate(reader, third[0])
        transaction, file = third[1]["transaction"], third[1]["file"]
        self.assertEqual(raw, V3)
        self.assertEqual(source_of(binding), {"version": 3, "dataset": transaction["dataset_id"], "transaction": transaction["id"],
                                              "file": file["id"], "sha256": hashlib.sha256(V3).hexdigest()})
        self.assertEqual({name: binding[name] for name in ("run_id", "pipeline_id", "pipeline_output_transaction_rid", "content_hash",
                                                           "size_bytes", "row_count", "listed_size_bytes", "lines")}, {
            "run_id": third[0]["id"], "pipeline_id": third[0]["pipeline_id"],
            "pipeline_output_transaction_rid": f"pipeline-run:{third[0]['id']}:output",
            "content_hash": third[0]["node_results"][0]["output"]["content_hash"], "size_bytes": len(V3), "row_count": 3,
            "listed_size_bytes": 3, "lines": {"0007": 1, "W-2": 2, "D3": 3}})

    def test_the_reader_sends_get_requests_only_and_leaves_the_bearer_at_the_service(self):
        receipt, output = published()
        reader, dataset = reader_of(output)
        locate(reader, receipt)
        self.assertEqual({method for method, _, _ in dataset.requests}, {"GET"})
        self.assertEqual([(method, url.removeprefix(BASE), bearer) for method, url, bearer in dataset.requests], [
            ("GET", f"/v1/datasets/{output['transaction']['dataset_id']}/versions?limit=1000", "Bearer reader-bearer"),
            ("GET", f"/v1/datasets/{output['transaction']['dataset_id']}/branches/main/transactions/"
                    f"{output['transaction']['id']}", "Bearer reader-bearer"),
            ("GET", f"/v1/datasets/{output['transaction']['dataset_id']}/files?branch=main&transaction_id="
                    f"{output['transaction']['id']}", "Bearer reader-bearer"),
            ("GET", f"/v1/datasets/{output['transaction']['dataset_id']}/files/{output['file']['id']}/download",
             "Bearer reader-bearer"),
            ("GET", f"/signed/v1/datasets/{output['transaction']['dataset_id']}/files/{output['file']['id']}", None)])

    def test_v1_and_v3_of_one_dataset_are_told_apart_by_their_receipt_not_by_bytes_or_hash(self):
        first = published(V1, version=1)
        third = published(V3, version=3, dataset=first[1]["transaction"]["dataset_id"])
        one, three = located(first, third)
        self.assertEqual([name for name in source_of(one) if one[name] != three[name]], ["version", "transaction", "file"])
        self.assertEqual((one["content_hash"], one["sha256"]), (three["content_hash"], three["sha256"]))

    def test_a_transaction_that_does_not_repeat_the_handle_names_no_source(self):
        receipt, output = published()
        output["transaction"]["metadata"]["transaction_rid"] = f"pipeline-run:{uuid.uuid4()}:output"
        reader, _ = reader_of(output)
        with self.assertRaisesRegex(ReceiptError, "0 transactions of dataset .* repeat 'pipeline-run:"):
            locate(reader, receipt)

    def test_two_transactions_of_one_handle_name_no_source(self):
        receipt, output = published()
        _, other = published(V3, version=4, dataset=output["transaction"]["dataset_id"])
        other["transaction"]["metadata"]["transaction_rid"] = output["transaction"]["metadata"]["transaction_rid"]
        reader, _ = reader_of(output, other)
        with self.assertRaisesRegex(ReceiptError, "2 transactions of dataset .* repeat"):
            locate(reader, receipt)

    def test_a_transaction_of_another_pipeline_or_another_output_hash_is_refused(self):
        for change in ("pipeline_rid", "output_content_hash", "pipeline_output"):
            with self.subTest(change=change):
                receipt, output = published()
                output["transaction"]["metadata"][change] = "another"
                reader, _ = reader_of(output)
                with self.assertRaisesRegex(ReceiptError, "names pipeline"):
                    locate(reader, receipt)

    def test_a_transaction_that_is_not_committed_on_main_of_the_output_dataset_is_refused(self):
        for change, message in (({"status": "OPEN"}, "is 'OPEN' on 'main'"),
                                ({"branch_name": "dev"}, "is 'COMMITTED' on 'dev'"),
                                ({"dataset_id": str(uuid.uuid4())}, "of dataset")):
            with self.subTest(change=change):
                receipt, output = published(**change)
                reader, _ = reader_of(output)
                with self.assertRaisesRegex(ReceiptError, message):
                    locate(reader, receipt)

    def test_a_transaction_that_holds_other_or_more_files_is_refused(self):
        receipt, output = published()
        for files, message in (([output["file"], {**output["file"], "id": str(uuid.uuid4())}], "its one file, not 2"),
                               ([], "its one file, not 0"),
                               ([{**output["file"], "transaction_id": str(uuid.uuid4())}], "its one file, not 1")):
            with self.subTest(message=message):
                reader, dataset = reader_of(output)
                dataset.files = files
                with self.assertRaisesRegex(ReceiptError, message):
                    locate(reader, receipt)

    def test_bytes_that_differ_from_what_the_transaction_records_are_refused(self):
        for change in ("size_bytes", "row_count", "preview_rows"):
            with self.subTest(change=change):
                receipt, output = published()
                metadata = output["transaction"]["metadata"]
                metadata[change] = metadata[change] - 1 if change != "preview_rows" else []
                reader, _ = reader_of(output)
                with self.assertRaisesRegex(ReceiptError, "its transaction records"):
                    locate(reader, receipt)

    def test_a_failed_run_or_two_committed_resources_name_no_source(self):
        receipt, output = published()
        reader, _ = reader_of(output)
        with self.assertRaisesRegex(ReceiptError, "is 'failed' as run"):
            locate(reader, {**receipt, "status": "failed"})
        with self.assertRaisesRegex(ReceiptError, "committed 2 resources"):
            locate(reader, {**receipt, "node_results": receipt["node_results"] * 2})
        # A resource whose transaction_rid is not the handle of this run is not followed.
        other_receipt, other_output = published(handle="another-handle")
        reader, _ = reader_of(other_output)
        with self.assertRaisesRegex(ReceiptError, "expected a dataset as 'pipeline-run:"):
            locate(reader, other_receipt)


class AcceptanceTests(unittest.TestCase):
    """``differences`` is the one acceptance. It is fed the readback of a tenant."""

    def setUp(self):
        self.tenant = "tenant-round"
        self.receipt, self.output = published()
        reader, _ = reader_of(self.output)
        self.binding, _ = locate(reader, self.receipt)
        self.source = source_of(self.binding)

    def problems(self, store):
        return differences(self.binding, self.tenant, store)

    def with_first_object(self, store, **properties):
        """The store with the properties of its first object changed or added."""
        first = {**store["objects"][0], "properties": {**store["objects"][0]["properties"], **properties}}
        return {**store, "objects": [first, *store["objects"][1:]]}

    def test_the_three_objects_of_this_round_are_accepted(self):
        self.assertEqual(self.problems(stored(self.tenant, V3, self.source)), [])

    def test_the_archived_transaction_is_refused_though_it_shares_values_and_hash(self):
        _, _, source, raw = archived_transaction()
        self.assertEqual((source["sha256"], raw), (self.source["sha256"], V3))
        problems = self.problems(stored(self.tenant, raw, source))
        self.assertEqual(named(problems, " _source."), {"dataset", "transaction", "file"})
        # Beside the id set, the id of each of the three rows is that of another transaction.
        others = [problem for problem in problems if " _source." not in problem]
        self.assertEqual(len(others), 4, problems)
        self.assertIn("is not", others[0])
        self.assertEqual([problem.rsplit("sku ", 1)[1] for problem in others[1:]], ["'0007'", "'W-2'", "'D3'"])
        self.assertFalse(any("cg_price" in problem or "is not one of" in problem for problem in problems), problems)
        self.assertEqual(len([problem for problem in problems if " _source." in problem]), 9)

    def test_the_v1_file_of_this_round_is_refused_though_it_shares_dataset_bytes_and_hash(self):
        receipt, output = published(V1, version=1, dataset=self.source["dataset"])
        one, = located((receipt, output))
        self.assertEqual([name for name in ("dataset", "sha256") if one[name] != self.source[name]], [])
        problems = self.problems(stored(self.tenant, V1, source_of(one)))
        self.assertEqual(named(problems, " _source."), {"version", "transaction", "file"})
        self.assertFalse(any("cg_price" in problem for problem in problems))

    def test_the_v2_file_of_this_round_is_refused_by_identity_and_values(self):
        two, = located(published(V2, version=2, dataset=self.source["dataset"]))
        problems = self.problems(stored(self.tenant, V2, source_of(two)))
        self.assertEqual(named(problems, " _source."), {"version", "transaction", "file", "sha256"})
        self.assertTrue(any("sku 'N4' is not one of" in problem for problem in problems), problems)
        self.assertTrue(any("cg_price '2.50', expected '0.00'" in problem for problem in problems), problems)

    def test_another_count_or_another_id_set_is_refused(self):
        three = stored(self.tenant, V3, self.source)
        self.assertTrue(any("4 listed and 4 read back, expected the 3 objects" in problem for problem in self.problems(
            {**three, "total": 4, "objects": three["objects"] + [three["objects"][0]]})))
        self.assertTrue(any("2 listed and 2 read back, expected the 3 objects" in problem for problem in self.problems(
            {**three, "total": 2, "objects": three["objects"][1:]})))
        self.assertTrue(any("3 listed and 2 read back, expected the 3 objects" in problem for problem in self.problems(
            {**three, "objects": three["objects"][:2]})))

    def test_an_object_with_the_right_properties_but_the_id_of_another_transaction_is_refused(self):
        three = stored(self.tenant, V3, self.source)
        foreign = {**three["objects"][0], "id": stable_object_id(self.tenant, "ProductCost", str(uuid.uuid4()), {"sku": "0007"})}
        problems = self.problems({**three, "objects": [foreign, *three["objects"][1:]]})
        self.assertEqual(len(problems), 2, problems)  # the id set, and the id of that row
        self.assertIn("is not", problems[0])
        self.assertTrue(problems[1].endswith("is the id of another row, expected the id of sku '0007'"), problems[1])

    def test_two_objects_that_swapped_their_complete_properties_are_refused(self):
        # The id set is still the right one and each row agrees with the SKU it names, so only
        # the id each object holds for that SKU can tell that the two rows changed places.
        three = stored(self.tenant, V3, self.source)
        first, second, third = three["objects"]
        swapped = {**three, "objects": [{**first, "properties": second["properties"]},
                                        {**second, "properties": first["properties"]}, third]}
        problems = self.problems(swapped)
        self.assertEqual(len(problems), 2, problems)
        self.assertEqual([problem.split(": ")[1].split(",")[0] for problem in problems],
                         ["is the id of another row", "is the id of another row"])
        self.assertEqual({problem.rsplit("sku ", 1)[1] for problem in problems}, {"'0007'", "'W-2'"})

    def test_another_cost_a_wrong_line_or_a_boolean_line_is_refused(self):
        three = stored(self.tenant, V3, self.source)
        for change, problem in (({"cg_price": "9.99"}, "cg_price '9.99', expected '0.00'"),
                                ({"_source": {**self.source, "line": 2}}, "_source.line: 2, its file reads 1"),
                                ({"_source": {**self.source, "line": True}}, "_source.line: True, its file reads 1")):
            with self.subTest(change=change):
                problems = self.problems(self.with_first_object(three, **change))
                self.assertEqual(len(problems), 1, problems)
                self.assertTrue(problems[0].endswith(problem), problems[0])

    def test_a_missing_source_or_another_type_is_refused(self):
        three = stored(self.tenant, V3, self.source)
        without = self.with_first_object(three, _source=None)
        problems = self.problems(without)
        self.assertEqual(named(problems, " _source."), {"version", "dataset", "transaction", "file", "sha256", "line"})
        other = self.problems({**three, "objects": [{**three["objects"][0], "object_type_id": "OtherCost"}, *three["objects"][1:]]})
        self.assertTrue(any("is a OtherCost" in problem for problem in other), other)

    def test_a_tenant_is_judged_against_another_source_by_the_rows_of_that_source(self):
        # What lets a refusal of another source count: the same rows, judged as that source, are accepted.
        _, _, source, raw = archived_transaction()
        own = stored(self.tenant, raw, source)
        self.assertEqual(row_differences(source, *lines_and_values([json.loads(line) for line in raw.splitlines()]),
                                         self.tenant, own), [])
        two, = located(published(V2, version=2))
        second = {"total": 2, "objects": [
            {"id": stable_object_id(self.tenant, "ProductCost", two["transaction"], {"sku": row["sku"]}),
             "object_type_id": "ProductCost", "properties": {**row, "_source": {**source_of(two), "line": number}}}
            for number, row in enumerate((json.loads(line) for line in V2.splitlines()), 1)]}
        self.assertEqual(row_differences(source_of(two), *lines_and_values([json.loads(line) for line in V2.splitlines()]),
                                         self.tenant, second), [])
        for not_held in ({"total": 0, "objects": []}, {**own, "total": 2, "objects": own["objects"][:2]}):
            self.assertTrue(row_differences(source, *lines_and_values([json.loads(line) for line in raw.splitlines()]),
                                            self.tenant, not_held), not_held)
        # Judged as this round, the same tenant that holds its own source is refused.
        self.assertTrue(self.problems(own))

    def test_what_the_smoke_recorded_of_the_build_is_compared_by_name(self):
        build = {"pipeline_run_id": self.binding["run_id"],
                 "pipeline_output_transaction_rid": self.binding["pipeline_output_transaction_rid"],
                 "pipeline_content_hash": self.binding["content_hash"], "output_dataset": self.binding["dataset"],
                 "output_version": 3, "output_transaction": self.binding["transaction"], "output_file_id": self.binding["file"],
                 "sha256": self.binding["sha256"], "size_bytes": self.binding["size_bytes"], "row_count": 3}
        report = {"pipeline": {"pipeline_id": self.binding["pipeline_id"]}}
        self.assertEqual(receipt_differs(self.binding, report, build), [])
        differing = receipt_differs(self.binding, report, {**build, "output_file_id": "another", "sha256": "0" * 64})
        self.assertEqual([problem.split(":")[0] for problem in differing], ["file", "sha256"])
        self.assertEqual(differing, ["file: the receipt names '" + self.binding["file"] + "', the smoke recorded 'another'",
                                     "sha256: the receipt names '" + self.binding["sha256"] + "', the smoke recorded '" + "0" * 64 + "'"])


class CommandTests(unittest.TestCase):
    def setUp(self):
        self.store = FakeStore()
        self.addCleanup(self.store.server_close)
        self.addCleanup(self.store.shutdown)
        self.endpoint = f"http://127.0.0.1:{self.store.server_port}"
        self.receipt, self.output = published()
        self.dataset = FakeDataset(self.output)
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.state = Path(directory.name)
        self.write_round(self.receipt)
        for patcher in (mock.patch.object(entry, "DatasetReader",
                                          lambda base, token: DatasetReader(base, token, opener=self.dataset)),
                        mock.patch.dict(os.environ, {"WP34_TOKEN": "reader-bearer"})):
            patcher.start()
            self.addCleanup(patcher.stop)

    def write_round(self, receipt):
        (self.state / "product-report.json").write_text(json.dumps(
            {"revision": "r1", "organization": "tenant-round",
             "pipeline": {"pipeline_id": receipt["pipeline_id"], "builds": [{"ordinal": 3, "output_version": 3}]}}))
        (self.state / "pipeline-3-run.json").write_text(json.dumps(receipt))

    def command(self, *arguments):
        """Run the entry once. Return its exit code and the report it printed."""
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(["--state", str(self.state), "--dataset-base", BASE, "--endpoint", self.endpoint, *arguments])
        return code, json.loads(out.getvalue()) if out.getvalue() else None, err.getvalue()

    def source(self):
        return json.loads((self.state / SOURCE_FILE).read_text())

    def values(self, report):
        return {obj["properties"]["sku"]: obj["properties"]["cg_price"] for obj in report["store"]["objects"]}

    def test_the_entry_locates_this_round_s_v3_delivers_it_once_and_is_accepted_with_exit_0(self):
        code, report, err = self.command()
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(report["source"], source_of(self.source()))
        self.assertEqual(report["source"]["sha256"], hashlib.sha256(V3).hexdigest())
        self.assertEqual((self.state / DOWNLOADED_FILE).read_bytes(), V3)
        self.assertEqual(report["delivered"]["created"], 3)
        self.assertEqual([(item["line"], item["sku"], item["created"]) for item in report["delivered"]["results"]],
                         [(1, "0007", True), (2, "W-2", True), (3, "D3", True)])
        self.assertEqual((report["store"]["total"], len(report["store"]["objects"])), (3, 3))
        self.assertEqual(self.values(report), EXPECTED_COSTS)
        self.assertEqual({obj["properties"]["_source"]["line"] for obj in report["store"]["objects"]}, {1, 2, 3})
        self.assertEqual(sorted(obj["id"] for obj in report["store"]["objects"]),
                         sorted(item["object_id"] for item in report["delivered"]["results"]))
        self.assertEqual((report["problems"], report["accepted"]), ([], True))
        self.assertEqual(len(self.store.objects), 3)

    def test_the_same_command_run_again_exits_0_creates_nothing_and_reads_the_same_objects_back(self):
        self.assertEqual(self.command()[0], 0)
        code, again, err = self.command()
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(again["delivered"]["created"], 0)
        self.assertEqual([item["created"] for item in again["delivered"]["results"]], [False] * 3)
        self.assertEqual(again["store"], self.command("--tenant", "tenant-round", "--verify-only")[1]["store"])
        self.assertEqual(len(self.store.objects), 3)

    def test_an_object_of_the_same_key_with_another_content_stops_the_delivery_with_exit_1(self):
        self.assertEqual(self.command()[0], 0)
        source = self.source()
        object_id = stable_object_id("tenant-round", "ProductCost", source["transaction"], {"sku": "0007"})
        stored = self.store.objects[("tenant-round", object_id)]
        self.store.objects[("tenant-round", object_id)] = {**stored, "payload": {**stored["payload"], "cg_price": "9.99"}}
        code, report, err = self.command()
        self.assertEqual((code, report), (1, None))
        self.assertIn("SyncConflictError", err)
        self.assertEqual(self.store.objects[("tenant-round", object_id)]["payload"]["cg_price"], "9.99")

    def test_a_tenant_that_also_holds_another_source_is_refused_after_the_delivery_with_exit_1(self):
        _, _, source, raw = archived_transaction()
        sync_product_costs(self.endpoint, "tenant-round", rows_of(raw, source))
        code, report, err = self.command()
        self.assertEqual((code, err), (1, ""))
        self.assertEqual(report["delivered"]["created"], 3)
        self.assertEqual(report["store"]["total"], 6)
        self.assertTrue(any("6 listed and 6 read back, expected the 3 objects" in problem for problem in report["problems"]), report["problems"])
        self.assertFalse(report["accepted"])

    def test_a_tenant_that_received_the_archived_transaction_is_refused_on_readback_with_exit_1(self):
        self.command()
        _, tenant, source, raw = archived_transaction()
        sync_product_costs(self.endpoint, tenant, rows_of(raw, source))
        code, report, err = self.command("--tenant", tenant, "--verify-only")
        self.assertEqual((code, err), (1, ""))
        self.assertEqual(report["delivered"], None)
        self.assertEqual(source["sha256"], self.source()["sha256"])
        self.assertEqual(self.values(report), EXPECTED_COSTS)
        self.assertEqual(named(report["problems"], " _source."), {"dataset", "transaction", "file"})
        self.assertIn("is not", [problem for problem in report["problems"] if " _source." not in problem][0])
        self.assertFalse(report["accepted"])

    def test_a_tenant_that_received_this_round_s_v1_file_is_refused_on_readback_with_exit_1(self):
        self.command()
        receipt, output = published(V1, version=1, dataset=self.source()["dataset"])
        self.dataset.publish(output)
        one, raw = locate(DatasetReader(BASE, "reader-bearer", opener=self.dataset), receipt)
        sync_product_costs(self.endpoint, "tenant-first", rows_of(raw, source_of(one)))
        code, report, _ = self.command("--tenant", "tenant-first", "--verify-only")
        self.assertEqual(code, 1)
        self.assertEqual(named(report["problems"], " _source."), {"version", "transaction", "file"})

    def test_a_run_whose_output_is_not_located_syncs_nothing(self):
        self.dataset.outputs[0]["transaction"]["metadata"]["transaction_rid"] = f"pipeline-run:{uuid.uuid4()}:output"
        code, report, err = self.command()
        self.assertEqual((code, report), (1, None))
        self.assertIn("ReceiptError", err)
        self.assertEqual(self.store.objects, {})

    def test_a_readback_without_a_source_file_fails_before_it_reads_the_store(self):
        with mock.patch.object(entry, "read_back") as read:
            code, _, err = self.command("--tenant", "tenant-round", "--verify-only")
        self.assertEqual((code, read.call_count), (1, 0))
        self.assertIn("FileNotFoundError", err)


if __name__ == "__main__":
    unittest.main()

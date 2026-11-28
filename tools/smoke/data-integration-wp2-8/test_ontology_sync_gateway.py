"""The object adapter's boundary at the edge gateway: ``ontology_sync_gateway`` for the three kinds.

The gateway of these tests is a stand-in that keeps the contract the real one is held to in the
scenarios: it binds a request to the organization of a bearer its key signed, refuses a write
without ``ontology:write`` and a bearer without an organization, and forwards the rest to the
object service with that tenant alone. Its faults are switches. It proves that the scenarios judge
what they claim to judge; the real gateway is accepted by ``run.sh <source>-gateway``, which needs
Docker. How the real one is started and removed is tested against a docker that only records.
"""

import argparse
import base64
import contextlib
import hashlib
import hmac
import http.client
import io
import json
import re
import secrets
import subprocess
import tempfile
import threading
import unittest
import uuid
from collections import Counter
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest import mock
from urllib.parse import urlsplit

import ontology_sync_gateway as subject
import ontology_sync_published as entry
from ontology_sync import archived_transaction
from ontology_sync_amazon import plain
from ontology_sync_gateway import LABEL, ORGANIZATION_REQUIRED, WRITE_PERMISSION, WRITE_REFUSED, ZERO_ORG
from test_ontology_sync import FakeObjectService
from test_ontology_sync_published import ONTOLOGY, StoreHandler, wire

KINDS = list(entry.KINDS.values())
A1 = "A1 through the gateway"
A2 = "A2 the tenant's reader is refused"
A3 = "A3 another organization's reader"
A4_LIST = "A4 after the delete the list holds"
A5 = "A5 the reader of the all-zero organization"
CONTROL = "the direct read holds the three objects"
LEFT = "the gateway container is removed"


class WriteHandler(StoreHandler):
    """The object service with the six routes of the adapter, each answering the tenant of ``x-of-tenant``
    alone. ``keeps_deleted`` leaves a deleted object in the list that its detail no longer fetches;
    ``repeats_rows`` answers every row of a list twice."""

    def held(self, type_id):
        with self.server.lock:
            return sorted((key[1], obj) for key, obj in self.server.objects.items()
                          if key[0] == self.headers.get("x-of-tenant") and obj["type_id"] == type_id)

    def listing(self, type_id):
        rows = [wire(obj) for _, obj in self.held(type_id) if not obj.get("deleted") or self.server.keeps_deleted]
        if self.server.repeats_rows:
            rows = [row for item in rows for row in (item, item)]
        self.reply({"data": rows, "page": 1, "per_page": 50, "total": len(rows)})

    def do_GET(self):
        match = ONTOLOGY.match(urlsplit(self.path).path)
        if match and not match.group(2):  # the list goes through listing(), where the faults of the store are
            self.server.requests.append(("GET", self.path))
            return self.listing(match.group(1))
        if match and any(obj["id"] == match.group(2) and obj.get("deleted") for _, obj in self.held(match.group(1))):
            self.server.requests.append(("GET", self.path))
            return self.reply({"error": "not found"}, 404)
        return super().do_GET()

    def do_POST(self):
        self.server.requests.append(("POST", self.path))
        type_id, rest = ONTOLOGY.match(urlsplit(self.path).path).groups()
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))  # read even for a query
        if rest == "query":
            return self.listing(type_id)
        properties = body["properties"]
        obj = {"tenant": self.headers.get("x-of-tenant"), "id": str(uuid.uuid4()), "type_id": type_id, "version": 1,
               "payload": properties, "owner": "gateway", "created_at_ms": 1, "updated_at_ms": 1}
        with self.server.lock:
            self.server.objects[(obj["tenant"], obj["id"])] = obj
        self.reply(wire(obj), 201)

    def do_PATCH(self):
        self.server.requests.append(("PATCH", self.path))
        type_id, object_id = ONTOLOGY.match(urlsplit(self.path).path).groups()
        found = [obj for _, obj in self.held(type_id) if obj["id"] == object_id and not obj.get("deleted")]
        if not found:
            return self.reply({"error": "not found"}, 404)
        properties = json.loads(self.rfile.read(int(self.headers["Content-Length"])))["properties"]
        found[0]["payload"] = {**found[0]["payload"], **properties}
        self.reply(wire(found[0]))

    def do_DELETE(self):
        self.server.requests.append(("DELETE", self.path))
        type_id, object_id = ONTOLOGY.match(urlsplit(self.path).path).groups()
        found = [obj for _, obj in self.held(type_id) if obj["id"] == object_id and not obj.get("deleted")]
        if not found:
            return self.reply({"error": "not found"}, 404)
        with self.server.lock:
            if self.server.keeps_deleted:
                found[0]["deleted"] = True
            else:
                del self.server.objects[(self.headers.get("x-of-tenant"), object_id)]
        self.send_response(204)
        self.end_headers()


class WriteStore(FakeObjectService):
    def __init__(self):
        super().__init__()
        self.RequestHandlerClass = WriteHandler
        self.keeps_deleted = self.repeats_rows = False


def claims_of(token, secret):
    """The claims of a token the key signed, or None for any other."""
    try:
        header, payload, signature = token.split(".")
        digest = hmac.new(secret.encode(), f"{header}.{payload}".encode(), hashlib.sha256).digest()
        expected = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
        if not hmac.compare_digest(signature, expected):
            return None
        return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    except (AttributeError, ValueError):
        return None


def total_as_text(value):
    """The answer with the ``total`` of every object's properties written as text, as a gateway that
    loses the number would answer it."""
    if isinstance(value, dict):
        value = {name: total_as_text(item) for name, item in value.items()}
        if isinstance(value.get("properties"), dict) and "total" in value["properties"]:
            value["properties"]["total"] = str(value["properties"]["total"])
    return value


class GatewayHandler(BaseHTTPRequestHandler):
    """The contract the real gateway is held to, with the faults of the stand-in it serves."""

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.proxy()

    do_POST = do_PATCH = do_DELETE = do_GET

    def fail(self, status, code):
        self.reply(status, json.dumps({"error": {"code": code, "message": code}}).encode())

    def reply(self, status, raw, content_type="application/json"):
        if status == 204:
            self.send_response(204)
            return self.end_headers()
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def proxy(self):
        server = self.server.gateway
        url = urlsplit(self.path)
        if url.path == "/healthz":
            return self.reply(200, b'{"status":"ok"}')
        claims = claims_of(self.headers.get("Authorization", "").removeprefix("Bearer "), server.secret)
        if claims is None:
            return self.fail(401, "unauthorized")
        organization = claims.get("org_id")
        if not organization or ("refuses the zero organization" in server.faults and organization == ZERO_ORG):
            return self.fail(403, ORGANIZATION_REQUIRED)
        matched = ONTOLOGY.match(url.path)
        if not matched:
            return self.fail(404, "unknown_service_route")
        type_id, rest = matched.groups()
        read = self.command == "GET" or (self.command == "POST" and rest == "query")
        if (not read and WRITE_PERMISSION not in claims.get("permissions", [])
                and "lets a reader write" not in server.faults):
            return self.fail(403, WRITE_REFUSED)
        if "shares this tenant" in server.faults and organization != server.tenant:
            organization = server.tenant
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None
        target = urlsplit(server.endpoint)
        connection = http.client.HTTPConnection(target.hostname, target.port, timeout=15)
        connection.request(self.command, self.path, body, {"x-of-tenant": organization})
        answer = connection.getresponse()
        raw, content_type = answer.read(), answer.getheader("Content-Type")
        connection.close()
        # The bytes of the store are forwarded as they are, the exact decimals of a settlement line
        # included; only a fault decodes an answer, to change it.
        shaping = server.faults & {"repeats rows", "wrong total", "answers the total as text"}
        if answer.status == 200 and read and shaping:
            data = json.loads(raw, parse_float=Decimal)
            if "repeats rows" in shaping and rest in ("", "query"):
                data["data"] = [row for item in data["data"] for row in (item, item)]
            if "wrong total" in shaping and rest in ("", "query"):
                data["total"] += 1
            if "answers the total as text" in shaping:
                data = total_as_text(data)
            raw = json.dumps(data, default=Decimal.__str__).encode()
        self.reply(answer.status, raw, content_type)


class FakeGateway:
    """The stand-in: ``faults`` names what it does wrong, ``tenant`` is the tenant of the round it may
    share. ``secret`` is the key its identities are minted with, as that of the real one."""

    def __init__(self, endpoint, tenant, *faults):
        self.endpoint, self.tenant, self.faults = endpoint.rstrip("/"), tenant, set(faults)
        self.secret = secrets.token_hex(32)

    def describe(self):
        return {"stand-in": True, "faults": sorted(self.faults)}

    @property
    def base(self):
        return f"http://127.0.0.1:{self.server.server_port}"

    def __enter__(self):
        if "does not start" in self.faults:
            token = subject.minter(self.secret)(org=self.tenant, permissions=[])
            raise subject.GatewayError(f"docker failed: the key {self.secret} and a bearer {token} in its message")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), GatewayHandler)
        self.server.gateway = self
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True)
        self.thread.start()
        return self

    def __exit__(self, *exception):
        if "does not start" not in self.faults:
            self.server.shutdown()
            self.server.server_close()
        return False

    def left(self):
        return ["still serving"] if "is left running" in self.faults else []


class AcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.store = WriteStore()
        self.addCleanup(self.store.server_close)
        self.addCleanup(self.store.shutdown)
        self.endpoint = f"http://127.0.0.1:{self.store.server_port}"
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.output = Path(directory.name) / "gateway.json"
        self.gateways = []

    def accept(self, kind, *faults):
        """Accept one delivered round of ``kind`` through a stand-in with ``faults``. Returns the evidence,
        what was printed, the round's raw file and the stand-in."""
        tenant = str(uuid.uuid4())
        _, _, source, raw = archived_transaction(None, kind.type_id)
        # The report of the first delivery as the entry prints it and ``run`` hands it on.
        delivered = {"delivered": entry.deliver(self.endpoint, tenant, source, raw, kind),
                     "store": plain(entry.read_back(self.endpoint, tenant, kind))}

        def start():
            self.gateways.append(FakeGateway(self.endpoint, tenant, *faults))
            return self.gateways[-1]

        printed = io.StringIO()
        with contextlib.redirect_stdout(printed):
            evidence = subject.accept(self.endpoint, kind, tenant, delivered, output=self.output, start=start,
                                      sleep=lambda seconds: None)
        return evidence, printed.getvalue(), raw

    def failed(self, evidence):
        return [item["name"] for item in evidence["checks"] if not item["passed"]]

    def test_a_gateway_that_keeps_the_contract_passes_the_fifteen_checks_for_each_kind(self):
        held = len(self.store.objects)
        for kind in KINDS:
            with self.subTest(kind=kind.type_id):
                evidence, printed, raw = self.accept(kind)
                self.assertEqual(self.failed(evidence), [])
                self.assertTrue(evidence["passed"])
                self.assertEqual(len(evidence["checks"]), 15)
                self.assertEqual((printed.count("PASS "), printed.count("FAIL "), printed.count("ERROR")), (15, 0, 0))
                requests = evidence["gateway_requests"]
                self.assertEqual(len(requests), 26)
                self.assertEqual(Counter(r["method"] for r in requests), {"GET": 18, "POST": 4, "PATCH": 2, "DELETE": 2})
                self.assertEqual(Counter(r["status"] for r in requests), {200: 16, 201: 2, 204: 1, 403: 3, 404: 4})
                self.assertNotIn("error", evidence)
                # A1 by row, named as the kind names a row: the SKU, the order line number, the line of the file.
                self.assertEqual(list(evidence["a1_by_row"]), list(kind.expected))
                for row in evidence["a1_by_row"].values():
                    self.assertTrue(subject.same(row["list"], row["direct"]) and subject.same(row["detail"], row["direct"])
                                    and subject.same(row["query"], row["direct"]), row["id"])
                    self.assertEqual(sorted(row["direct"]["_source"]), sorted(("version", "dataset", "transaction", "file",
                                                                                "sha256", "line")))
                self.assertEqual(list(kind.expected.values()),
                                 [kind.values(row["direct"]) for row in evidence["a1_by_row"].values()])
                self.assertEqual({tuple(v["ids"]) for v in evidence["after_delete"]["list"]},
                                 {tuple(sorted(evidence["expected_ids"].values()))})
        # The temporary objects are gone: the round's objects and the other organization's one remain.
        self.assertEqual(len(self.store.objects) - held, sum(len(kind.expected) + 1 for kind in KINDS))

    def test_the_evidence_and_what_was_printed_hold_no_key_and_no_token(self):
        evidence, printed = None, ""
        for kind in KINDS:
            evidence, printed, _ = self.accept(kind)
        text = self.output.read_text()
        for gateway in self.gateways:
            self.assertNotIn(gateway.secret, text + printed)
        self.assertEqual(subject.JWT.findall(text + printed), [])
        self.assertEqual(json.loads(text)["passed"], True)
        self.assertEqual(Counter(r["request_headers"] for r in evidence["gateway_requests"]),
                         {"Authorization: Bearer <redacted>": 20,
                          "Authorization: Bearer <redacted>; Content-Type: application/json": 6})

    def test_the_evidence_of_amazon_keeps_each_total_the_exact_number_the_gateway_answered(self):
        evidence, _, raw = self.accept(entry.SETTLEMENT_LINE)
        totals = [str(json.loads(line)["total"]) for line in raw.splitlines()]
        text = self.output.read_text()
        for total in totals:
            self.assertIn(f'"total": {total}', text)
        self.assertEqual([row["direct"]["total"] for row in evidence["a1_by_row"].values()],
                         [Decimal(total) for total in totals])

    def test_a_gateway_that_lets_a_reader_write_fails_a2_and_sees_the_tenant_change(self):
        for kind in KINDS:
            with self.subTest(kind=kind.type_id):
                evidence, printed, _ = self.accept(kind, "lets a reader write")
                self.assertFalse(evidence["passed"])
                failed = self.failed(evidence)
                self.assertTrue(any(name.startswith(A2) for name in failed), failed)
                reader_writes = [r for r in evidence["gateway_requests"] if r["identity"] == "reader of the tenant"
                                 and r["method"] in ("POST", "PATCH", "DELETE") and not r["path"].endswith("/query")]
                self.assertEqual([r["status"] for r in reader_writes], [201, 200, 204])
                self.assertIn("FAIL ", printed)

    def test_a_gateway_that_shares_this_tenant_with_another_organization_fails_a3(self):
        for kind in KINDS:
            with self.subTest(kind=kind.type_id):
                evidence, _, _ = self.accept(kind, "shares this tenant")
                failed = self.failed(evidence)
                self.assertTrue(any(name.startswith(A3) for name in failed), failed)
                self.assertFalse(evidence["passed"])

    def test_a_gateway_that_refuses_the_all_zero_organization_fails_a5(self):
        evidence, _, _ = self.accept(entry.PRODUCT_COST, "refuses the zero organization")
        self.assertTrue(any(name.startswith(A5) for name in self.failed(evidence)))
        self.assertFalse(evidence["passed"])

    def test_a_list_or_query_that_repeats_a_row_fails_a1_and_the_list_after_the_delete(self):
        for kind in KINDS:
            with self.subTest(kind=kind.type_id):
                evidence, _, _ = self.accept(kind, "repeats rows")
                failed = self.failed(evidence)
                self.assertTrue(any(name.startswith(A1) for name in failed), failed)
                self.assertTrue(any(name.startswith(A4_LIST) for name in failed), failed)
                self.assertFalse(evidence["verdicts"]["A1"]["list"] or evidence["verdicts"]["A1"]["query"])
                self.assertFalse(evidence["verdicts"]["A4"]["three again"])
                # Keyed by id the repeated row folds into one: three objects where six rows were answered.
                listing = evidence["gateway_requests"][0]
                self.assertEqual((len(listing["body"]["data"]), len(subject.held(listing))), (6, 3))

    def test_a_total_that_disagrees_with_the_rows_fails_a1(self):
        evidence, _, _ = self.accept(entry.ORDER_LINE_PRICE, "wrong total")
        self.assertTrue(any(name.startswith(A1) for name in self.failed(evidence)))
        self.assertFalse(evidence["passed"])

    def test_a_direct_read_whose_list_repeats_a_row_is_no_control(self):
        self.store.repeats_rows = True
        evidence, _, _ = self.accept(entry.PRODUCT_COST)
        self.assertTrue(any(name.startswith(CONTROL) for name in self.failed(evidence)), self.failed(evidence))
        self.assertFalse(evidence["passed"])

    def test_a_total_the_gateway_answers_as_text_fails_a1_for_amazon_and_no_other_kind(self):
        for kind in KINDS:
            with self.subTest(kind=kind.type_id):
                evidence, _, _ = self.accept(kind, "answers the total as text")
                self.assertEqual(any(name.startswith(A1) for name in self.failed(evidence)),
                                 kind is entry.SETTLEMENT_LINE)
                self.assertEqual(evidence["passed"], kind is not entry.SETTLEMENT_LINE)

    def test_a_list_that_still_holds_the_deleted_object_fails_a4(self):
        self.store.keeps_deleted = True
        evidence, _, _ = self.accept(entry.ORDER_LINE_PRICE)
        failed = self.failed(evidence)
        self.assertTrue(any(name.startswith(A4_LIST) for name in failed), failed)
        self.assertIn("A4 the three objects of the control are still held, each by id, as the direct read held them",
                      failed)
        after = evidence["after_delete"]
        self.assertEqual((len(after["direct"]["objects"]), after["get_deleted_id"]), (4, 404))

    def test_a_gateway_that_cannot_start_is_an_error_without_the_key_in_the_evidence_or_the_output(self):
        evidence, printed, _ = self.accept(entry.PRODUCT_COST, "does not start")
        self.assertFalse(evidence["passed"])
        self.assertIn("GatewayError", evidence["error"])
        self.assertEqual(len(evidence["checks"]), 1)  # nothing was asked
        self.assertEqual(evidence["gateway_requests"], [])
        text = self.output.read_text()
        self.assertNotIn(self.gateways[-1].secret, text + printed + evidence["error"])
        self.assertEqual(subject.JWT.findall(text + printed + evidence["error"]), [])
        self.assertIn("<secret redacted>", evidence["error"])
        self.assertIn("<jwt redacted>", evidence["error"])

    def test_a_gateway_that_is_left_running_is_not_reported_as_removed(self):
        evidence, _, _ = self.accept(entry.PRODUCT_COST, "is left running")
        self.assertEqual([name[:len(LEFT)] for name in self.failed(evidence)], [LEFT])


class Judgements(unittest.TestCase):
    control = {"a": {"x": "1"}, "b": {"x": "2"}, "c": {"x": "3"}}

    def answer(self, rows, total=3, status=200):
        return {"status": status, "body": {"data": rows, "total": total}}

    def test_same_holds_the_type_a_number_is_never_text_and_a_fraction_never_a_float(self):
        self.assertTrue(subject.same(Decimal("59.57"), Decimal("59.57")))
        self.assertTrue(subject.same({"total": Decimal("-9"), "n": 1}, {"total": Decimal("-9"), "n": 1}))
        for left, right in ((Decimal("59.57"), "59.57"), (Decimal("59.57"), 59.57), (1, 1.0), (1, True),
                            ({"a": 1}, {"a": 1, "b": 2})):
            self.assertFalse(subject.same(left, right), (left, right))

    def test_lists_judges_the_rows_before_they_are_keyed(self):
        rows = [{"id": name, "properties": value} for name, value in self.control.items()]
        self.assertTrue(subject.lists(self.answer(rows), self.control))
        self.assertFalse(subject.lists(self.answer(rows + rows[:1]), self.control))  # a row repeated
        self.assertFalse(subject.lists(self.answer(rows, total=4), self.control))  # a total that disagrees
        self.assertFalse(subject.lists(self.answer(rows[:2], total=2), self.control))  # a row missing
        self.assertFalse(subject.lists(self.answer(rows, status=403), self.control))
        other = [dict(row) for row in rows]
        other[0]["properties"] = {"x": "9"}
        self.assertFalse(subject.lists(self.answer(other), self.control))
        self.assertFalse(subject.lists({"status": 200, "body": "404 page not found"}, self.control))
        self.assertFalse(subject.lists(self.answer([{"id": ["a"], "properties": {}}], total=1), {"a": {}}))

    def test_exact_json_writes_a_decimal_as_the_number_and_a_string_as_the_string(self):
        text = subject.exact_json({"total": Decimal("59.57"), "text": "59.57", "count": 1})
        self.assertEqual(text.count('"total": 59.57'), 1)
        self.assertEqual(text.count('"text": "59.57"'), 1)
        self.assertEqual(text.count('"count": 1'), 1)
        self.assertEqual(re.findall(r"exact-decimal", text), [])
        self.assertEqual(set(json.loads(text)), {"total", "text", "count"})

    def test_scrub_takes_out_the_key_and_every_token(self):
        token = "eyJ" + "a" * 20 + "." + "b" * 20 + "." + "c" * 20
        self.assertEqual(subject.scrubbed({"a": [f"Bearer {token}", "key 0badf00d"], "n": Decimal("1.5")}, "0badf00d"),
                         {"a": ["Bearer <jwt redacted>", "key <secret redacted>"], "n": Decimal("1.5")})
        self.assertEqual(subject.scrub("nothing", ""), "nothing")


class GatewayContainer(unittest.TestCase):
    """How the real gateway is started and removed, with docker replaced by a recorder."""

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.binary = Path(directory.name) / "edge-gateway-service"
        self.binary.write_bytes(b"the binary")
        self.args = argparse.Namespace(project="of-ontology-sync-1")
        self.calls, self.answers = [], {"run": ("c0ffee" + "0" * 57 + "\n", 0), "port": ("127.0.0.1:49153\n", 0),
                                        "ps": [("", 0)], "image": ("sha256:abc\n", 0), "rm": ("", 0)}

        def docker(*words, environment=None, check=True):
            self.calls.append((words, environment))
            stdout, code = self.answers[words[0]] if words[0] != "ps" else self.answers["ps"].pop(0)
            if check and code:
                raise subprocess.CalledProcessError(code, ["docker", *words], stderr="docker: boom\n", output=stdout)
            return subprocess.CompletedProcess(["docker", *words], code, stdout, "")

        patcher = mock.patch.object(subject, "docker", docker)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_the_key_is_no_argument_the_port_is_docker_s_and_what_is_removed_is_listed_by_this_run_s_label(self):
        gateway = subject.Gateway(self.args, self.binary.parent)
        self.answers["ps"] = [("deadbeef\n", 0), ("", 0)]  # what is owned before and after the removal
        with gateway as started:
            self.assertEqual(started.base, "http://127.0.0.1:49153")
            self.assertTrue(gateway.describe()["container_id"].startswith("c0ffee"))
        words = [call[0] for call in self.calls]
        run = next(call for call in words if call[0] == "run")
        self.assertEqual(run[run.index("-p") + 1], "127.0.0.1::8080")
        # The key is named without its value: the next word after its name is the option after it.
        self.assertEqual(list(run[run.index("OF_JWT__SECRET") - 1:run.index("OF_JWT__SECRET") + 2]),
                         ["-e", "OF_JWT__SECRET", "-e"])
        self.assertIn(f"{LABEL}={gateway.token}", run)
        self.assertNotIn(gateway.secret, json.dumps(words))
        environment = next(call[1] for call in self.calls if call[0][0] == "run")
        self.assertEqual(environment, {"OF_JWT__SECRET": gateway.secret})
        self.assertEqual(words[-2:], [("ps", "-aq", "--filter", f"label={LABEL}={gateway.token}"), ("rm", "-f", "deadbeef")])
        self.assertEqual(gateway.left(), [])

    def test_a_container_docker_created_and_could_not_start_is_removed_by_id_and_the_error_carries_no_key(self):
        self.answers.update(run=("", 125), ps=[("deadbeef\n", 0)])
        gateway = subject.Gateway(self.args, self.binary.parent)
        with self.assertRaises(subject.GatewayError) as caught:
            gateway.__enter__()
        self.assertIn("docker: boom", str(caught.exception))
        self.assertNotIn(gateway.secret, str(caught.exception))
        self.assertEqual(self.calls[-1][0], ("rm", "-f", "deadbeef"))

    def test_a_gateway_docker_published_no_port_for_is_removed(self):
        self.answers.update(port=("", 0), ps=[("c0ffee" + "0" * 57 + "\n", 0)])
        gateway = subject.Gateway(self.args, self.binary.parent)
        with self.assertRaisesRegex(subject.GatewayError, "published no loopback port"):
            gateway.__enter__()
        self.assertEqual(self.calls[-1][0][:2], ("rm", "-f"))

    def test_two_runs_own_different_containers_and_a_listing_docker_failed_is_not_empty(self):
        first, second = (subject.Gateway(self.args, self.binary.parent) for _ in range(2))
        self.assertNotEqual(first.token, second.token)
        self.answers["ps"] = [("", 1)]
        self.assertEqual(first.left(), ["not listed: docker ps exited 1"])

    def test_a_missing_binary_is_refused_before_docker_is_asked(self):
        self.binary.unlink()
        with self.assertRaisesRegex(subject.GatewayError, "is missing"):
            subject.Gateway(self.args, self.binary.parent).__enter__()
        self.assertEqual(self.calls, [])


if __name__ == "__main__":
    unittest.main()

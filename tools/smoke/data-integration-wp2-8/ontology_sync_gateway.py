"""The object adapter's boundary at the edge gateway: this round's three objects of one type.

``ontology_sync_published.run`` calls ``accept`` while the round's own stack is still up, when
``run.sh`` names a ``-gateway`` source. Nothing is delivered again and no id is typed in: the
tenant, the transaction, the file and the three object ids are the round's own, those of the
report of its first delivery, and the objects are the ones the round delivered, in the Cassandra
store and the object service that are still running. Next to them one edge gateway is started,
built from the checkout by ``run.sh``, that routes to that object service. ``Kind`` makes it one
scenario for the ProductCost rows of lingxing, the OrderLinePrice rows of noon and the
SettlementLine rows of amazon: the route, the direct read, the name of a row and what it holds are
the kind's.

Every identity is an ordinary account minted by the smoke fixture of business-platform-p1 with the
gateway's own random key of this run, which is never written, and asks the gateway only what an
ordinary caller asks, on the adapter's six routes:

  A1  the tenant's reader lists, fetches each of the three ids and queries: the three objects, as
      the direct read holds them (GET and POST query are reads);
  A2  the same reader tries the three writes: the gateway refuses each one and the direct read
      holds what it held before;
  A3  a reader and a writer of another synthetic organization list, ask for this tenant's ids and
      write: nothing of this tenant is answered to them, their write lands in their own partition
      and this tenant's direct read holds what it held;
  A5  a reader of the all-zero organization lists: answered, not refused for a missing organization;
  A4  the tenant's writer creates, updates and deletes one object: 201, 200, 204, each seen by the
      tenant's reader, and 404 once it is deleted. The list afterwards is judged on its own.

A list or a query is judged on its rows as they are, before anything is keyed by id: as many rows
as objects, as many distinct ids, the same ``total`` and no id the control does not hold, so a row
the gateway repeats is not folded into one. A fraction of a gateway answer is read as the exact
``Decimal`` the gateway answered, so a settlement line's total is held to the direct read digit for
digit, and ``same`` asks the type as well: a number is never a string. The temporary objects are
created with the properties ``WRITES`` holds for the type and a ``note`` that names the scenario; a
settlement line's total is a whole number, as a fraction sent as a float would not be the
``Decimal`` its answer is read as.

Every check says whose it is: "gateway" is the candidate's boundary, "object store" is the object
service behind it, "setup" is this run. No tenant header of a client's own is sent, no identity is
forged and no path outside the six routes is asked. The evidence keeps every request and its answer
with no token and no key in it, an error included. ``passed`` is true only when every check passed.
"""

import hashlib
import importlib.util
import json
import os
import re
import secrets
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from decimal import Decimal
from pathlib import Path

from ontology_sync_amazon import plain

HERE = Path(__file__).resolve().parent
IMAGE = "golang:1.26.11"  # the image run.sh builds the services with; here it only carries the static binary
UPSTREAM = "http://objects:50125"  # the object service of ontology_sync.compose.yaml, as the stack's network names it
HEALTH_TIMEOUT = 60  # seconds the gateway is given to answer its health check
LABEL = "of.smoke.gateway"  # marks the gateway container of one run with a token of that run
ZERO_ORG = "00000000-0000-0000-0000-000000000000"  # the organization docs/poc-online-retail/README.md issues
WRITE_PERMISSION = "ontology:write"  # what the gateway's object adapter asks for a write
ORGANIZATION_REQUIRED, WRITE_REFUSED = "organization_required", "object_write_forbidden"  # the gateway's own error codes
# The properties of the temporary object each write scenario creates, by type; each adds a note of its own.
WRITES = {"ProductCost": {"sku": "GATEWAY-WRITE-CHECK", "cg_price": "1.25"},
          "OrderLinePrice": {"item_nr": "GATEWAY-WRITE-CHECK", "currency": "AED", "unit_price": "1.25"},
          "SettlementLine": {"settlement id": 1000000097, "type": "Order", "order id": "GATEWAY-WRITE-CHECK",
                             "sku": "DEMO-SKU-GW", "total": 1}}
JWT = re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}")
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


class GatewayError(Exception):
    """The gateway of the checkout could not be started."""


def docker(*words, environment=None, check=True):
    """One docker command. ``environment`` adds to this process's environment what the command is to
    pass on by name, so that no value of it is an argument."""
    return subprocess.run(["docker", *words], capture_output=True, text=True, check=check,
                          env={**os.environ, **environment} if environment else None)


def same(left, right):
    """Whether two answers hold the same values, type for type: a number is never a string, a count is
    never a fraction, and two fractions are the same exact decimal."""
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(same(left[name], right[name]) for name in left)
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(map(same, left, right))
    return type(left) is type(right) and left == right


def scrub(text, secret):
    """The text with no key and no token of this run in it."""
    return JWT.sub("<jwt redacted>", text.replace(secret, "<secret redacted>") if secret else text)


def scrubbed(value, secret):
    """The value with every string of it scrubbed and its other values as they are: a fraction stays the
    Decimal the gateway answered it as."""
    if isinstance(value, str):
        return scrub(value, secret)
    if isinstance(value, dict):
        return {name: scrubbed(item, secret) for name, item in value.items()}
    if isinstance(value, list):
        return [scrubbed(item, secret) for item in value]
    return value


def exact_json(value):
    """The evidence as JSON text, every Decimal as the exact digits the gateway answered it with: a
    number, 59.57 and not "59.57". json.dumps writes a type it does not know as the text its default
    returns, in quotes; the marker is what takes the quotes off again."""
    marker = "@exact-decimal:" + secrets.token_hex(16)
    text = json.dumps(value, indent=2, default=lambda number: f"{marker}{number}{marker}")
    return text.replace(f'"{marker}', "").replace(f'{marker}"', "")


class Gateway:
    """The edge gateway of the checkout: one container on the network of the round's stack, routing to
    its object service and removed again on exit.

    Docker picks the loopback port it is published on. The key its identities are minted with
    reaches the container by name, from the environment of this process and not from the argument
    list, so neither an error of docker nor a process listing carries it. What ``stop`` removes is
    what this run created and nothing else: the containers that carry this run's label, by id, the
    gateway among them and any that docker created and could not start.
    """

    def __init__(self, args, bin_dir):
        self.binary = Path(bin_dir).resolve() / "edge-gateway-service"
        self.project, self.name = args.project, args.project + "-gateway"
        self.token, self.secret = secrets.token_hex(8), secrets.token_hex(32)
        self.id, self.base = None, None

    def describe(self):
        return {"container": self.name, "container_id": self.id, "label": f"{LABEL}={self.token}",
                "network": self.project + "_default", "upstream": UPSTREAM, "image": IMAGE,
                "image_id": docker("image", "inspect", IMAGE, "--format", "{{.Id}}").stdout.strip(),
                "binary_sha256": hashlib.sha256(self.binary.read_bytes()).hexdigest()}

    def owned(self):
        """The ids of the containers that carry this run's label."""
        return docker("ps", "-aq", "--filter", f"label={LABEL}={self.token}").stdout.split()

    def stop(self):
        """Remove what this run created, by id."""
        ids = self.owned()
        if ids:
            docker("rm", "-f", *ids, check=False)

    def left(self):
        """The containers of this run that are still there, or why they could not be listed."""
        try:
            return self.owned()
        except subprocess.CalledProcessError as error:
            return [f"not listed: docker ps exited {error.returncode}"]

    def __enter__(self):
        if not self.binary.is_file():
            raise GatewayError(f"{self.binary} is missing; run.sh builds it for a -gateway source")
        try:
            self.id = docker("run", "-d", "--name", self.name, "--label", f"{LABEL}={self.token}", "--network",
                             self.project + "_default", "-v", f"{self.binary.parent}:/binaries:ro",
                             "-p", "127.0.0.1::8080",  # Docker picks a free loopback port
                             "-e", "OF_SERVER__PORT=8080", "-e", "OF_JWT__SECRET",  # by name: its value is no argument
                             "-e", f"OF_UPSTREAM__OBJECT_DATABASE_SERVICE_URL={UPSTREAM}",
                             "--entrypoint", "/binaries/edge-gateway-service", IMAGE,
                             environment={"OF_JWT__SECRET": self.secret}).stdout.strip()
            published = re.search(r"127\.0\.0\.1:\d+", docker("port", self.id, "8080").stdout)
            if published is None:
                raise GatewayError(f"container {self.id} published no loopback port for 8080")
            self.base = "http://" + published[0]
        except Exception as error:
            self.stop()  # a container this run created and could not start is this run's to remove
            if isinstance(error, subprocess.CalledProcessError):
                raise GatewayError("docker failed: " + (error.stderr or error.stdout).strip()[-400:]) from None
            raise
        return self

    def __exit__(self, *exception):
        self.stop()
        return False


def minter(secret):
    """``mint_token`` of the smoke fixture of business-platform-p1, signing with the key of this run."""
    spec = importlib.util.spec_from_file_location("wp23_p1", HERE.parent / "business-platform-p1/smoke/smoke.py")
    p1 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(p1)
    p1.SECRET_BYTES = secret.encode()
    return p1.mint_token


def refused(answer, status, code):
    """The gateway's own envelope: an error of this code."""
    body = answer.get("body")
    return answer.get("status") == status and isinstance(body, dict) and body.get("error", {}).get("code") == code


def body_of(answer):
    """The JSON object a gateway answer holds, or {} when it holds none."""
    body = answer.get("body")
    return body if isinstance(body, dict) else {}


def held(answer):
    """{id: properties} of the objects a list or a query answered. Evidence only: a repeated row folds
    into one here, which is why ``lists`` judges the rows before they are keyed."""
    rows = body_of(answer).get("data")
    if not isinstance(rows, list):
        return None
    return {row["id"]: row.get("properties") for row in rows if isinstance(row, dict) and isinstance(row.get("id"), str)}


def ids_of(answer):
    """The id of each row a list or a query answered, a repeated row twice."""
    return [row.get("id") for row in body_of(answer).get("data") or [] if isinstance(row, dict)]


def lists(answer, control):
    """Whether a list or a query answer holds exactly the objects of ``control``, {id: properties}: as
    many rows and as many distinct ids as ``control`` has objects, the same ``total``, no other id,
    and each row the properties ``control`` holds for its id. The rows are counted as they are, so a
    row the gateway repeats, a ``total`` that disagrees or a missing row is refused."""
    body = body_of(answer)
    rows = body.get("data")
    if answer.get("status") != 200 or not isinstance(rows, list) or not all(
            isinstance(row, dict) and isinstance(row.get("id"), str) for row in rows):
        return False
    ids = [row["id"] for row in rows]
    return (len(rows) == len(set(ids)) == len(control) == body.get("total") and set(ids) == set(control)
            and all(same(row.get("properties"), control[row["id"]]) for row in rows))


def total(answer):
    return body_of(answer).get("total")


def empty(answer):
    return (answer.get("status") == 200 and isinstance(answer.get("body"), dict)
            and answer["body"].get("data") == [] and answer["body"].get("total") == 0)


def brief(answers):
    return "; ".join(f"{a.get('method')} {a.get('path', '').split('/objects')[-1] or '/'} -> {a.get('status')} "
                     f"{str(a.get('body', a.get('error')))[:90]}" for a in answers)


def accept(endpoint, kind, tenant, delivered, *, output, args=None, bin_dir=None, start=None, sleep=time.sleep):
    """A1 to A5 through an edge gateway, against the object service at ``endpoint``. Returns the evidence.

    ``delivered`` is the report of the round's first delivery: under ``delivered`` its results, the
    object id of each line, and under ``store`` what it read back. ``args`` is that of the round's
    stack and ``bin_dir`` the directory run.sh built the services into; ``start`` replaces the
    gateway in tests with a context manager that yields its base URL. The evidence is written to
    ``output`` whatever happened.
    """
    evidence = {"tenant": tenant, "checks": [], "gateway_requests": []}
    gateway, accounts = None, {}
    other_org = str(uuid.uuid4())  # the other synthetic organization of this run

    def key():
        """The key of this run, once there is one: what nothing kept may hold."""
        return gateway.secret if gateway is not None else ""

    def check(label, ok, detail="", scope="gateway"):
        """One judgement, kept and printed without the key. The scope says whose behaviour it is."""
        detail = scrub(str(detail), key())
        evidence["checks"].append({"name": label, "passed": bool(ok), "detail": detail, "scope": scope})
        print(f"{'PASS' if ok else 'FAIL'} [{scope}] {label}" + ("" if ok else f": {detail}"), flush=True)

    def identity(label, organization, permissions):
        token = mint(org=organization, permissions=permissions)
        accounts[token] = {"label": label, "organization": organization, "permissions": permissions}
        return token

    def ask(token, method, path, body=None):
        """One request through the gateway, as it was asked and as it was answered — a 4xx is an answer too."""
        data = None if body is None else json.dumps(body).encode()
        headers = {"Authorization": f"Bearer {token}"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(gateway.base + path, data=data, headers=headers, method=method)
        record = {"identity": accounts[token]["label"], "method": method, "path": path, "request_body": body,
                  "request_headers": "Authorization: Bearer <redacted>"
                  + ("; Content-Type: application/json" if data is not None else "")}
        try:
            try:
                with urllib.request.urlopen(request, timeout=15) as response:
                    status, head, raw = response.status, dict(response.headers), response.read()
            except urllib.error.HTTPError as error:
                status, head, raw = error.code, dict(error.headers), error.read()
            try:
                parsed = json.loads(raw, parse_float=Decimal)  # the exact digits the gateway answered
            except ValueError:
                parsed = raw[:500].decode("utf-8", "replace")
            record.update(status=status,
                          response_headers={k: v for k, v in head.items() if k.lower() in ("content-type", "content-length")},
                          body=parsed)
        except OSError as error:  # no answer at all
            record["error"] = repr(error)
        record = scrubbed(record, gateway.secret)
        evidence["gateway_requests"].append(record)
        return record

    def direct(organization=tenant):
        """The direct read of one tenant as the downstream consumer reads it: its list, then every row the
        list holds. An id the list names that is no longer fetched is kept with the status it was answered."""
        listed = kind.get(endpoint, kind.route + "?per_page=50", organization)
        objects = []
        for item in listed["data"]:
            try:
                objects.append(kind.get(endpoint, kind.route + item["id"], organization))
            except urllib.error.HTTPError as error:
                objects.append({"id": item["id"], "status": error.code})
        return {"total": listed["total"], "objects": sorted(objects, key=lambda obj: obj["id"])}

    def created_id(answer, status, properties):
        """The id of the object a create answered: ``status``, a well-formed id and the properties sent."""
        body = body_of(answer)
        if answer.get("status") != status or not isinstance(body.get("id"), str) or not UUID.fullmatch(body["id"]):
            return None
        return body["id"] if same(body.get("properties"), properties) else None

    def is_object(answer, object_id, properties):
        body = body_of(answer)
        return answer.get("status") == 200 and body.get("id") == object_id and same(body.get("properties"), properties)

    try:
        gateway = start() if start else Gateway(args, bin_dir)
        mint = minter(gateway.secret)
        with gateway:
            evidence["gateway"] = gateway.describe()
            deadline, status = time.monotonic() + HEALTH_TIMEOUT, None
            while True:  # the health check carries no identity
                try:
                    with urllib.request.urlopen(gateway.base + "/healthz", timeout=5) as response:
                        status = response.status
                except OSError as error:
                    status = repr(error)
                if status == 200 or time.monotonic() > deadline:
                    break
                sleep(1)
            check("the edge gateway of this build answers its health check", status == 200, status, scope="setup")

            reader = identity("reader of the tenant", tenant, [])
            writer = identity("writer of the tenant", tenant, [WRITE_PERMISSION])
            other_reader = identity("reader of another organization", other_org, [])
            other_writer = identity("writer of another organization", other_org, [WRITE_PERMISSION])
            zero_reader = identity("reader of the all-zero organization", ZERO_ORG, [])
            evidence["identities"] = {"key": "generated for this run, never written", "accounts": list(accounts.values())}

            # The control: the direct read of the downstream consumer, with the tenant of the round.
            expected = {item["line"]: item["object_id"] for item in delivered["delivered"]["results"]}
            lines = sorted(expected.items())  # (line of the file, object id), in file order
            evidence["expected_ids"] = expected
            before = direct()
            control = {obj["id"]: obj["properties"] for obj in before["objects"]}
            evidence["control"] = before
            check("the direct read holds the three objects of this round's first delivery: one row and one id per "
                  "line of the file, the total 3, the ids its report gave and, object by object, what it read back",
                  before["total"] == len(before["objects"]) == len(control) == len(lines) == 3
                  and set(control) == set(expected.values())
                  and plain(control) == {obj["id"]: obj["properties"] for obj in delivered["store"]["objects"]},
                  scope="setup")

            base, write = kind.route.rstrip("/"), WRITES[kind.type_id]
            row_of = {object_id: kind.held_name(control[object_id]) for _, object_id in lines}

            # ---- A1: the tenant's reader reads, through the gateway.
            listing = ask(reader, "GET", base + "?per_page=50")
            details = {line: ask(reader, "GET", f"{base}/{object_id}") for line, object_id in lines}
            query = ask(reader, "POST", base + "/query", {"per_page": 50})
            a1 = {"list": lists(listing, control),
                  "details": all(is_object(details[line], object_id, control[object_id]) for line, object_id in lines),
                  "query": lists(query, control)}
            check("A1 through the gateway the tenant's reader is answered the list, each of the three ids and "
                  "the query as the direct read holds them: three rows, three ids, total 3",
                  all(a1.values()), brief([listing, query]) + " | " + brief(list(details.values())))
            # A1 by row, as evidence: for each row its id, and what the direct read, the list, the detail and
            # the query each hold for that id.
            evidence["a1_by_row"] = {row_of[object_id]: {"id": object_id, "direct": control[object_id],
                                                         "list": (held(listing) or {}).get(object_id),
                                                         "detail": body_of(details[line]).get("properties"),
                                                         "query": (held(query) or {}).get(object_id)}
                                     for line, object_id in lines}

            # ---- A2: the same reader tries the three writes.
            one = expected[1]
            attempts = {"create": ask(reader, "POST", base, {"properties": {**write, "note": "reader"}}),
                        "update": ask(reader, "PATCH", f"{base}/{one}", {"properties": {"note": "reader"}}),
                        "delete": ask(reader, "DELETE", f"{base}/{one}")}
            a2 = {"refused": all(refused(a, 403, WRITE_REFUSED) for a in attempts.values()), "unchanged": direct() == before}
            check("A2 the tenant's reader is refused each of the three writes by the gateway (403 object_write_forbidden)",
                  a2["refused"], brief(attempts.values()))
            check("A2 after the reader's attempts the direct read holds what it held before", a2["unchanged"])

            # ---- A3: another synthetic organization's ordinary requests.
            other_listing = ask(other_reader, "GET", base + "?per_page=50")
            other_details = {line: ask(other_reader, "GET", f"{base}/{object_id}") for line, object_id in lines}
            sent_other = {**write, "note": "another organization"}
            other_created = ask(other_writer, "POST", base, {"properties": sent_other})
            other_id = created_id(other_created, 201, sent_other)
            other_own = ask(other_reader, "GET", f"{base}/{other_id}") if other_id else {"method": "GET", "status": None}
            other_store = direct(other_org) if other_id else {"total": 0, "objects": []}
            evidence["other_partition"] = other_store
            a3 = {"list": empty(other_listing),
                  "details": all(d.get("status") == 404 for d in other_details.values()),
                  "write": other_id is not None,
                  "own": is_object(other_own, other_id, sent_other),
                  "partition": other_store["total"] == len(other_store["objects"]) == 1
                  and other_store["objects"][0]["id"] == other_id,
                  "tenant": direct() == before}
            check("A3 another organization's reader is answered an empty list and 404 for each of this tenant's three ids",
                  a3["list"] and a3["details"], brief([other_listing]) + " | " + brief(list(other_details.values())))
            check("A3 another organization's writer creates an object (201) that its reader fetches and that the direct "
                  "read holds in its own partition alone",
                  a3["write"] and a3["own"] and a3["partition"], brief([other_created, other_own])
                  + f" | its partition holds {other_store['total']}")
            check("A3 after the other organization's requests the tenant's direct read holds what it held before",
                  a3["tenant"])

            # ---- A5: the all-zero organization is an organization.
            zero_listing = ask(zero_reader, "GET", base + "?per_page=50")
            a5 = {"answered": zero_listing.get("status") == 200,
                  "not missing": not refused(zero_listing, 403, ORGANIZATION_REQUIRED), "empty": empty(zero_listing)}
            check("A5 the reader of the all-zero organization is answered its empty list (200), not refused for a "
                  "missing organization", all(a5.values()), brief([zero_listing]))

            # ---- A4: the tenant's writer writes — last, because the object store keeps what it lists afterwards.
            sent = {**write, "note": "created"}
            patched = {**sent, "note": "patched"}
            created = ask(writer, "POST", base, {"properties": sent})
            new_id = created_id(created, 201, sent)
            if new_id:
                fetched = ask(reader, "GET", f"{base}/{new_id}")
                update = ask(writer, "PATCH", f"{base}/{new_id}", {"properties": {"note": "patched"}})
                seen = ask(reader, "GET", f"{base}/{new_id}")
                delete = ask(writer, "DELETE", f"{base}/{new_id}")
                gone = ask(reader, "GET", f"{base}/{new_id}")
            else:
                fetched = update = seen = delete = gone = {"method": "-", "status": None}

            after = direct()
            evidence["after_delete"] = {"direct": after, "list": []}
            for second in range(5):  # the list right after the delete, then once a second
                poll = ask(reader, "GET", base + "?per_page=50")
                evidence["after_delete"]["list"].append({"seconds": second, "status": poll.get("status"),
                                                         "total": total(poll), "ids": ids_of(poll)})
                if second < 4:
                    sleep(1)
            again = poll
            gone_direct = None  # the deleted object, asked for by id: the status it was answered
            if new_id:
                try:
                    kind.get(endpoint, kind.route + new_id, tenant)
                    gone_direct = 200
                except urllib.error.HTTPError as error:
                    gone_direct = error.code
            evidence["after_delete"]["get_deleted_id"] = gone_direct

            a4 = {"create": new_id is not None,
                  "reader sees it": is_object(fetched, new_id, sent),
                  "update": is_object(update, new_id, patched),
                  "reader sees the update": is_object(seen, new_id, patched),
                  "delete": delete.get("status") == 204,
                  "gone": gone.get("status") == 404,
                  "control intact": (after["total"] == len(after["objects"]) == 3 and after == before
                                     and gone_direct == 404),
                  "three again": lists(again, control)}
            check("A4 the tenant's writer creates an object (201) and the tenant's reader fetches it",
                  a4["create"] and a4["reader sees it"], brief([created, fetched]))
            check("A4 the writer updates it (200) and the reader sees the update",
                  a4["update"] and a4["reader sees the update"], brief([update, seen]))
            check("A4 the writer deletes it (204) and the gateway answers 404 for it afterwards",
                  a4["delete"] and a4["gone"], brief([delete, gone]))
            check("A4 the three objects of the control are still held, each by id, as the direct read held them",
                  a4["control intact"], scope="object store")
            check("A4 after the delete the list holds the three objects of the control again: three rows, three ids, "
                  "total 3", a4["three again"], f"total {total(again)}, ids {ids_of(again)}", scope="object store")
            evidence["verdicts"] = {"A1": a1, "A2": a2, "A3": a3, "A4": a4, "A5": a5}
    except Exception as error:  # recorded without the key, so that the evidence is still written and the gateway stopped
        evidence["error"] = scrub(f"{type(error).__name__}: {error}", key())
        print(f"ERROR {evidence['error']}", flush=True)
    left = gateway.left() if gateway is not None else ["the gateway was not started"]
    check("the gateway container is removed and nothing of this run's is left", not left, left, scope="setup")
    evidence["passed"] = "error" not in evidence and all(item["passed"] for item in evidence["checks"])
    Path(output).write_text(exact_json(evidence) + "\n")
    return evidence

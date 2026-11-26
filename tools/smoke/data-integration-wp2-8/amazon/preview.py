"""#84's Amazon monthly reports through the WP8.3 version comparison, in a real browser.

One stack runs the #84 handoff of this directory: three registered reports and a
negative fourth one are published as four versions of the report dataset, one
Pipeline builds three versions of an output dataset, and a fourth run is refused
for the schema of its input. The independent reader of that organization then
works on the real Compare page behind the run's real edge gateway:

- report v2 with v3: two files of one month, different data under one field
  structure;
- report v3 with v4: v4 is the input of the refused run, and the page states the
  one change that refused it;
- output v3, located in the version list after the refusal and downloaded there
  through the page's own control.

The page's row comparison is no part of this case: it reads Parquet and noon
order-line NDJSON snapshots and refuses these CSV reports.
"""
from decimal import Decimal
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.request
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("amazon_handoff", HERE / "smoke.py")
amazon = importlib.util.module_from_spec(spec)
spec.loader.exec_module(amazon)
s3, transactions = amazon.s3, amazon.transactions
# The reader's request, the hash and binding of a committed schema and the judgment of a Schema
# diff panel are those of the noon case, which reads the same page.
noon = s3.module("noon_preview", HERE.with_name("noon") / "preview.py")
request, schema_hash, committed_schema, states_schema = noon.request, noon.schema_hash, noon.committed_schema, noon.states_schema

BRANCH = "main"
# Stated by the handoff, independent of every reader: the first two October
# files differ in their data and declare the same fields; the negative report
# changes one field. Both pairs are selected in one page session.
GROUPS = (("v2-v3", 2, 3), ("v3-v4", 3, 4))
EXPECTED_SCHEMA = {"v2-v3": [], "v3-v4": [dict(sign="~", name="total", hint="DOUBLE → STRING")]}
REFUSED_FIELD = dict(name="total", changed=["field_type"], before="DOUBLE", after="STRING")
NO_CHANGE = dict(order_kept=True, added=[], removed=[], modified=[])
EXPECTED_CHANGES = {"v2-v3": NO_CHANGE, "v3-v4": dict(NO_CHANGE, modified=[REFUSED_FIELD])}
# The dev stub profile the isolated web configuration answers; the page shows it.
ANONYMIZE = ("P31 Admin", "p31-admin@example.com")
BROWSER_DOWNLOAD = "browser-download-output-v3.ndjson"


def schema_changes(base, target):
    """How two field lists differ, read field by field.

    A field is compared whole, so a change in anything but its type counts too.
    The names keep their order or they do not, and a name that one list lacks
    is added or removed.
    """
    before = {field["name"]: field for field in base}
    after = {field["name"]: field for field in target}
    modified = []
    for name, field in before.items():
        if name not in after or field == after[name]:
            continue
        changed = field.keys() | after[name].keys()
        modified.append(dict(name=name, changed=sorted(k for k in changed if field.get(k) != after[name].get(k)),
                             before=field["field_type"], after=after[name]["field_type"]))
    return dict(order_kept=[field["name"] for field in base] == [field["name"] for field in target],
                added=[name for name in after if name not in before],
                removed=[name for name in before if name not in after], modified=modified)


def download_record(kept):
    """The browser's record of the download, by the names the browser script writes its fields.
    A field that is not there is named before any check reads it."""
    missing = [field for field in ("saved_as", "version", "option", "selector", "files", "controls",
                                    "listing_request", "request", "event", "captures", "clicked_unix_ms") if field not in kept]
    if missing:
        raise KeyError(f"the browser's record of the download does not hold {missing}")
    return kept


def listening(table):
    """The listening sockets of one /proc/net/tcp table, each as (address, port).

    An IPv4 address is written there as four little-endian bytes in hexadecimal.
    An IPv6 one is kept as its 32 hexadecimal digits.
    """
    sockets = []
    for line in table.splitlines()[1:]:
        parts = line.split()
        if parts[3] != "0A":
            continue
        address, port = parts[1].split(":")
        if len(address) == 8:
            address = ".".join(str(int(address[index:index + 2], 16)) for index in (6, 4, 2, 0))
        sockets.append((address, int(port, 16)))
    return sockets


class KeptRedirect(urllib.request.HTTPRedirectHandler):
    """The redirect itself is the answer: it is not followed."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def redirect_address(path, token, base):
    """The status a route answers, and the host and path of the address it
    redirects to. The query of that address is not kept: it carries the
    signature that makes the address worth having.
    """
    req = Request(base + path, headers={"Authorization": "Bearer " + token}, method="GET")
    try:
        with urllib.request.build_opener(KeptRedirect).open(req, timeout=30) as response:
            return response.status, None
    except HTTPError as error:
        address = urlsplit(error.headers.get("Location", ""))
        return error.code, (address.netloc, address.path)


def container_env(name, variable):
    settings = json.loads(subprocess.check_output(["docker", "inspect", name], text=True))[0]
    return next(value.split("=", 1)[1] for value in settings["Config"]["Env"] if value.startswith(variable + "="))


def download_path(dataset, file_id):
    return f"/api/v1/datasets/{dataset}/files/{file_id}/download"


def report_bytes(name):
    return (HERE / "fixtures" / f"transaction_view-{name}.csv").read_bytes()


def main(state):
    handles = amazon.main(state)
    handoff = json.loads((state / "amazon-report.json").read_text())
    reader, other, p1 = handles["reader"], handles["other"], handles["p1"]
    dataset, pipeline, gate = handoff["report_dataset"], handoff["pipeline"], handoff["schema_gate"]
    down, output_api = pipeline["output_dataset"], f"/api/v1/datasets/{pipeline['output_dataset']}"
    api = f"/api/v1/datasets/{dataset}"
    gateway_netloc = urlsplit(os.environ["WP23_GATEWAY"]).netloc
    service_netloc = urlsplit(os.environ["WP23_DVS"]).netloc
    report = dict(status="failed", revision=handoff["revision"], dataset=dataset, output_dataset=down,
                  identity=dict(label="independent reader of the first organization", permissions=[], roles=[],
                                stubs=["Vite identity bootstrap and profile only"]),
                  versions={}, groups={}, download={}, checks=[])

    def check(label, ok, reply=None):
        if not ok:
            raise AssertionError(label + (": " + repr(dict(status=reply["status"], redirects=reply["redirects"],
                                                             body=reply["body"][:300])) if reply else ""))
        report["checks"].append(label)
        print("PASS", label, flush=True)

    # The gateway, and the forwarder that answers the signed loopback address inside the gateway's
    # network namespace: each runs the binary built from this checkout.
    binaries = Path(os.environ["BIN_DIR"])
    gateway_container, forwarder_container = os.environ["WP23_GATEWAY_CONTAINER"], os.environ["WP23_FORWARDER_CONTAINER"]

    def running_hash(container, binary):
        return subprocess.check_output(["docker", "exec", container, "sha256sum", "/binaries/" + binary], text=True).split()[0]

    gateway = binaries / "edge-gateway-service"
    report["gateway"] = dict(binary_sha256=hashlib.sha256(gateway.read_bytes()).hexdigest(),
                             dataset_upstream=container_env(gateway_container, "OF_UPSTREAM__DATASET_VERSIONING_SERVICE_URL"))
    check("gateway runs the binary built from this checkout",
          running_hash(gateway_container, "edge-gateway-service") == report["gateway"]["binary_sha256"])

    forwarder = binaries / "loopback-forwarder"
    forward = json.loads(subprocess.check_output(["docker", "inspect", forwarder_container], text=True))[0]
    port = os.environ["WP23_DVS_PORT"]
    sockets = [socket for table in ("tcp", "tcp6")
               for socket in listening(subprocess.check_output(
                   ["docker", "exec", forwarder_container, "cat", f"/proc/net/{table}"], text=True))
               if socket[1] == int(port)]
    gateway_id = subprocess.check_output(["docker", "inspect", "-f", "{{.Id}}", gateway_container], text=True).strip()
    report["forwarder"] = dict(binary_sha256=hashlib.sha256(forwarder.read_bytes()).hexdigest(),
                               network_mode=forward["HostConfig"]["NetworkMode"], command=forward["Config"]["Cmd"],
                               listening=sockets)
    check("the forwarder runs the binary built from this checkout, in the gateway's network namespace, "
          "on the port the dataset service is published on",
          running_hash(forwarder_container, "loopback-forwarder") == report["forwarder"]["binary_sha256"]
          and forward["State"]["Running"] and forward["HostConfig"]["NetworkMode"] == "container:" + gateway_id
          and forward["Config"]["Cmd"] == ["-listen-port", port, "-target", "dataset:50078"])
    check("the forwarder listens on the loopback interface alone: 127.0.0.1, and no other address",
          sockets == [("127.0.0.1", int(port))])

    # The report versions the page compares, as the reader sees them: asked of the gateway, which
    # is the request the page makes, and of the dataset service bound to the version and transaction.
    transactions_of = {entry["version"]: entry["transaction"] for entry in handoff["files"][1:]}
    transactions_of[4] = gate["negative"]["transaction"]
    versions = {}
    for version, transaction in sorted(transactions_of.items()):
        gateway_answer = request("GET", f"{api}/schema?" + urlencode(dict(branch=BRANCH, version=version)), reader)
        check(f"gateway answers the schema of report v{version} for the reader",
              gateway_answer["status"] == 200 and gateway_answer["answered"] == gateway_netloc, gateway_answer)
        direct = request("GET", f"/v1/datasets/{dataset}/schema?" + urlencode(
            dict(branch=BRANCH, version=version, transaction_id=transaction)), reader, base=os.environ["WP23_DVS"])
        check(f"dataset service answers the schema of report v{version} bound to its version and transaction",
              direct["status"] == 200 and direct["answered"] == service_netloc
              and direct["body"] == gateway_answer["body"] == (state / f"schema-v{version}.json").read_bytes(), direct)
        fields = committed_schema(json.loads(gateway_answer["body"]), (dataset, BRANCH, version, transaction))
        (state / f"compare-schema-v{version}.gateway.json").write_bytes(gateway_answer["body"])
        versions[version] = dict(transaction=transaction, fields=fields, content_hash=schema_hash(fields))

    # v2 and v3 are different files of one month that declare the same fields. The data is read
    # from the bytes the handoff published, by the header of each report and no service.
    first, second = handoff["files"][1:3]
    rows = {entry["version"]: transactions.expected((state / f"published-{entry['file']}.csv").read_bytes())
            for entry in (first, second)}
    settlement = {version: [row["settlement id"] for row in held] for version, held in rows.items()}
    nets = {version: transactions.net(held) for version, held in rows.items()}
    check("report v2 and v3 hold different data: other bytes, three and two transactions that share no "
          "settlement id, and the totals 59.57 and -1.36",
          first["sha256"] != second["sha256"] and (len(rows[2]), len(rows[3])) == (3, 2)
          and not set(settlement[2]) & set(settlement[3])
          and nets == {2: Decimal("59.57"), 3: Decimal("-1.36")}
          and all(nets[version] == transactions.NET_TOTAL[entry["file"]] for version, entry in ((2, first), (3, second))))
    check("report v2 and v3 answer the same fields and the same schema hash",
          versions[2]["fields"] == versions[3]["fields"] and versions[2]["content_hash"] == versions[3]["content_hash"])

    # v4 is the negative report. It differs from v3 in one field, and its hash is another one.
    changes = {name: schema_changes(versions[base]["fields"], versions[target]["fields"]) for name, base, target in GROUPS}
    report["changes"] = changes
    check("read field by field, v3 and v4 differ in total alone: DOUBLE to STRING, every other field the same, "
          "no field added, removed or moved",
          changes == EXPECTED_CHANGES
          and versions[4]["content_hash"] not in (versions[3]["content_hash"], versions[2]["content_hash"]))
    check("the content hashes the handoff recorded are the ones the reader asked for",
          gate["expected_schema_hash"] == versions[3]["content_hash"]
          and gate["committed_schema_hash"] == versions[4]["content_hash"])

    # The receipt names the refused run. The handoff saved that run and the graph it was saved
    # with: the pin its input node was bound to, and the input node's own error.
    refused = json.loads((state / "pipeline-4-run.json").read_text())
    saved = json.loads((state / "pipeline-4-graph.json").read_text())
    config = {node["id"]: node["config"] for node in saved["dag"]["nodes"]}["input"]
    pin = config["dataset_snapshot"]
    results = refused.get("node_results") or []
    input_error = next((node.get("error") or "" for node in results if node["node_id"] == "input"), "")
    reason = 'field "total" is STRING in the committed schema and DOUBLE in the expected schema'
    refusal = report["refusal"] = dict(
        run_id=refused["id"], status=refused["status"], pipeline_id=refused["pipeline_id"], dataset=config["dataset_id"],
        branch=pin["branch"], version=pin["version"], transaction=pin["transaction_id"].removeprefix("ri.foundry.main.transaction."),
        expected_schema_hash=pin["expected_schema_hash"],
        nodes={node["node_id"]: node["status"] for node in results}, input_error=input_error)
    check("the compared v4 is the input of the refused run: the same dataset, branch, version and transaction, "
          "pinned to the fields and hash of v3",
          refused["id"] == gate["refused_run_id"] and refusal["status"] == "failed"
          and refusal["pipeline_id"] == pipeline["pipeline_id"]
          and (refusal["dataset"], refusal["branch"], refusal["version"]) == (dataset, BRANCH, 4)
          and refusal["transaction"] == versions[4]["transaction"]
          and refusal["expected_schema_hash"] == versions[3]["content_hash"] and pin["expected_schema"] == versions[3]["fields"]
          and refusal["nodes"] == {"input": "failed", "project-transactions": "cancelled", "output": "cancelled"})
    check("the input node's own error, written by the Pipeline service, names that dataset, the two refusals "
          "and the one change: total STRING in the committed schema and DOUBLE in the expected schema",
          f"failed to load dataset {dataset}:" in input_error and "dataset_input_schema_rejected" in input_error
          and "dataset_schema_content_hash_mismatch" in input_error and reason in input_error)

    # Output v3 as the reader finds it after the refusal: in the version list, then the files of
    # the transaction that cut it. That is the listing the page makes for a selected version.
    replay, last_good = pipeline["builds"][2], gate["last_good_output"]
    listed = request("GET", f"{output_api}/versions?limit=1000", reader)
    version_list = {item["version"]: item["transaction_id"] for item in json.loads(listed["body"]).get("data") or []} \
        if listed["status"] == 200 else {}
    check("the reader lists output v3 at the transaction of the third run, the head after the refusal",
          listed["status"] == 200 and listed["answered"] == gateway_netloc
          and version_list.get(3) == last_good["transaction_id"] == replay["output_transaction"] == gate["output_head"]["after"])
    listing_query = dict(branch=BRANCH, transaction_id=last_good["transaction_id"])
    listing = request("GET", f"{output_api}/files?" + urlencode(listing_query), reader)
    files = json.loads(listing["body"]).get("files") or [] if listing["status"] == 200 else []
    check("the files of that transaction alone are one active file, the one downloads accept",
          listing["status"] == 200 and listing["answered"] == gateway_netloc and len(files) == 1
          and files[0]["transaction_id"] == last_good["transaction_id"] and files[0]["status"] == "active"
          and files[0]["id"] == last_good["file_id"] == replay["output_file_id"])
    path = download_path(down, files[0]["id"])
    served = request("GET", path, reader)
    check("the gateway serves the reader the download of that file: the bytes of the third run",
          served["status"] == 200 and served["answered"] == gateway_netloc and not served["redirects"]
          and hashlib.sha256(served["body"]).hexdigest() == replay["sha256"] == transactions.OUTPUT_SHA256["2026-10"]
          and served["body"] == (state / "pipeline-history-output-v3.ndjson").read_bytes(), served)
    # The gateway follows the dataset service's redirect itself. The address it follows is on the
    # loopback the forwarder answers, under a host other than the service's own: a client leaves a
    # bearer credential behind on a redirect to another host.
    status, signed = redirect_address(path.replace("/api/v1", "/v1", 1), reader, os.environ["WP23_DVS"])
    report["signed_address"] = dict(status=status, netloc=signed and signed[0], path=signed and signed[1])
    check("the gateway reaches the dataset service under another host than the signed address it follows",
          status == 302 and signed[0] == f"127.0.0.1:{port}"
          and urlsplit(report["gateway"]["dataset_upstream"]).netloc != signed[0])
    check("the other tenant is refused the listing, the download and the schemas",
          all(reply["status"] in p1.DENY and b"total" not in reply["body"]
              for reply in (request("GET", f"{output_api}/files?" + urlencode(listing_query), other),
                            request("GET", path, other),
                            request("GET", f"{api}/schema?" + urlencode(dict(branch=BRANCH, version=4)), other))))
    committed = request("POST", f"{output_api}/outputs:commit", reader, payload=dict(
        idempotency_key=f"amazon-preview:{down}", branch=BRANCH, transaction_type="SNAPSHOT", format="json",
        files=[dict(logical_path="reader.ndjson", operation="ADD", content_type="application/x-ndjson",
                    size_bytes=2, content_base64="e30=")]))
    check("reader cannot publish to the downloaded dataset", committed["status"] == 403, committed)

    s3.write_json(state / "compare-ready.json", dict(
        dataset=dataset, output_dataset=down, anonymize=list(ANONYMIZE),
        groups=[dict(name=name, expected=EXPECTED_SCHEMA[name],
                     base=dict(branch=BRANCH, version=base, transaction_id=transactions_of[base]),
                     target=dict(branch=BRANCH, version=target, transaction_id=transactions_of[target]))
                for name, base, target in GROUPS],
        download=dict(dataset=down, version=last_good["version"], branch=BRANCH,
                      transaction_id=last_good["transaction_id"], logical_path=files[0]["logical_path"],
                      size_bytes=files[0]["size_bytes"], saved_as=BROWSER_DOWNLOAD)))
    token_file = state / "browser.jwt"
    try:
        token_file.write_text(reader)
        subprocess.run(["bash", str(HERE.with_name("noon") / "preview-ui.sh"), str(state), str(HERE)], check=True)
    finally:
        token_file.unlink(missing_ok=True)
    browser = json.loads((state / "browser.json").read_text())
    check("browser compared both pairs and saved the download of output v3", browser["status"] == "passed")

    for ordinal, (name, base, target) in enumerate(GROUPS):
        view = browser["schema"][name]
        chosen = {"A": versions[base], "B": versions[target]}
        # A side selected afresh asks for its schema again: both sides of the first pair, and each
        # side whose selection the next pair changed.
        earlier = {} if ordinal == 0 else {"A": versions[GROUPS[ordinal - 1][1]], "B": versions[GROUPS[ordinal - 1][2]]}
        asked_again = [side for side in chosen if earlier.get(side) != chosen[side]]
        sent = view["requests"]
        check(f"page shows the {name} schema changes of the committed declarations, counted item by item",
              view["panel"] is True and states_schema(view, EXPECTED_SCHEMA[name])
              and all(view["sides"][side]["fields"] == len(answer["fields"]) == 30 for side, answer in chosen.items()))
        check(f"page asked the gateway for the selected {name} report versions",
              view["navigation_unix_ms"] <= view["step_started_unix_ms"] <= view["read_unix_ms"]
              and all(sent[side]["path"] == f"{api}/schema"
                      and sent[side]["params"] == dict(branch=BRANCH, version=str(base if side == "A" else target))
                      and sent[side]["status"] == 200 and sent[side]["transferred_bytes"] > 0
                      and view["navigation_unix_ms"] <= sent[side]["started_unix_ms"] < sent[side]["completed_unix_ms"] <= view["read_unix_ms"]
                      and (side not in asked_again or view["step_started_unix_ms"] <= sent[side]["started_unix_ms"])
                      for side in chosen))
        check(f"page selects the {name} report versions",
              all(view["selectors"][side]["value"] == f"transaction:{answer['transaction']}"
                  and view["selectors"][side]["label"] == f"v{version} · {BRANCH} · {answer['transaction'][:8]}"
                  and {f"transaction:{answer['transaction']}" for answer in versions.values()}
                  <= set(view["selectors"][side]["options"])
                  for side, answer, version in (("A", chosen["A"], base), ("B", chosen["B"], target))))
        check(f"page keeps the {name} report versions selected with their schemas",
              view["alerts"] == [] and view["unavailable_labels"] == 0 and view["schema_notices"] == 0
              and view["files_panel"] is False)
        report["groups"][name] = dict(base=dict(branch=BRANCH, version=base, transaction_id=chosen["A"]["transaction"]),
                                      target=dict(branch=BRANCH, version=target, transaction_id=chosen["B"]["transaction"]),
                                      expected=EXPECTED_SCHEMA[name], asked_again=asked_again, page=view)
    first_view, second_view = (browser["schema"][name] for name in ("v2-v3", "v3-v4"))
    check("page states the v2-v3 schemas and then the v3-v4 schemas in one page session",
          second_view["navigation_unix_ms"] == first_view["navigation_unix_ms"]
          and second_view["step_started_unix_ms"] >= first_view["read_unix_ms"])
    # The page read v4 on the side of the run's own input, under the transaction the run was bound
    # to, and stated the field and the two types that the run's error names.
    check("the v3-v4 the page compared is the refusal of this run: its input's dataset, version and transaction, "
          "and the field and types its error names",
          report["dataset"] == refusal["dataset"] and report["groups"]["v3-v4"]["target"]["transaction_id"] == refusal["transaction"]
          and report["groups"]["v3-v4"]["base"]["transaction_id"] == versions[3]["transaction"]
          and [(row["name"], row["hint"].split(" → ")) for row in second_view["rows"]]
          == [(REFUSED_FIELD["name"], [REFUSED_FIELD["before"], REFUSED_FIELD["after"]])]
          and f'field "{REFUSED_FIELD["name"]}" is {REFUSED_FIELD["after"]} in the committed schema and '
              f'{REFUSED_FIELD["before"]} in the expected schema' in refusal["input_error"])

    # Every capture reads five panels the page never shows with the code that reads its own: each
    # statement alone, and each next to what it excludes. The page's own panel is then judged by
    # the same rule, here and as captured.
    change_with_notice, change_with_other, change, notice, notice_with_other = browser["schema"]["v2-v3"]["control"]
    check("the page reading knows each statement alone and refuses a change next to the empty notice",
          states_schema(change, change["rows"]) and states_schema(notice, [])
          and not any(states_schema(mixed, expected) for mixed in (change_with_notice, change_with_other, notice_with_other)
                      for expected in ([], EXPECTED_SCHEMA["v3-v4"])))

    # The download the browser saved, read from its own bytes: no service, and nothing the page
    # or a build recorded, is asked.
    kept = download_record(browser["download"])
    saved_bytes = (state / kept["saved_as"]).read_bytes()
    digest = hashlib.sha256(saved_bytes).hexdigest()
    held = transactions.read(saved_bytes)
    report["download"] = dict(
        version=kept["version"], transaction_id=last_good["transaction_id"], option=kept["option"], selector=kept["selector"],
        listed=kept["files"], listing_request=kept["listing_request"], request=kept["request"], event=kept["event"],
        captures=kept["captures"], saved_as=kept["saved_as"], size_bytes=len(saved_bytes), sha256=digest,
        net_total=str(transactions.net(held)), rows=len(held))
    check("browser saved the download of output v3 with the SHA-256 #84 pins for the first October report",
          digest == transactions.OUTPUT_SHA256["2026-10"] == replay["sha256"] == last_good["sha256"]
          and saved_bytes == (state / "pipeline-history-output-v3.ndjson").read_bytes())
    check("the downloaded v3 sums to the net total USD 59.57 #84 pins, as decimals over its total column",
          transactions.net(held) == Decimal("59.57") == transactions.NET_TOTAL["2026-10"] == Decimal(replay["net_total"]))
    check("the downloaded v3 holds the transactions of the first October report, read from its own bytes, "
          "and none of the second",
          not transactions.value_diffs(saved_bytes, transactions.expected(report_bytes("2026-10"), transactions.COLUMNS))
          and transactions.holds(held, transactions.expected(report_bytes("2026-10"), transactions.COLUMNS),
                                 transactions.expected(report_bytes("2026-10-2"), ("settlement id",))))
    check("page located output v3 in its version list and selected it by the transaction the reader listed",
          kept["option"]["value"] == f"transaction:{last_good['transaction_id']}"
          and kept["option"]["label"] == f"v3 · {BRANCH} · {last_good['transaction_id'][:8]}"
          and kept["selector"] == {"value": kept["option"]["value"], "label": kept["option"]["label"]})
    # The size is compared twice, each where it can be: the page's text against its own locale's
    # formatting of the listed size, in the browser; the listed size against the length of what
    # the browser saved, here. No text is parsed back into a number.
    shown = next(file for file in kept["files"] if file["path"] == files[0]["logical_path"])
    check("page listed the files of that transaction alone: the one file the reader listed, by path, "
          "with one download control for it",
          kept["listing_request"]["path"] == f"{output_api}/files" and kept["listing_request"]["params"] == listing_query
          and kept["listing_request"]["status"] == 200 and kept["listing_request"]["transferred_bytes"] > 0
          and [file["path"] for file in kept["files"]] == [files[0]["logical_path"]]
          and kept["controls"] == [f"Download {files[0]['logical_path']}"])
    check("page wrote the listed size as its own locale formats it",
          shown["size_matches"] is True and shown["size_text"] == shown["size_shown_as"])
    check("the size the reader listed is the length of what the browser saved",
          files[0]["size_bytes"] == len(saved_bytes))
    check("page asked the gateway for the download of that file once its control was clicked, and the browser saved it",
          kept["request"]["path"] == path and kept["request"]["params"] == {} and kept["request"]["status"] == 200
          and kept["request"]["transferred_bytes"] > 0
          and kept["clicked_unix_ms"] <= kept["request"]["started_unix_ms"] < kept["request"]["completed_unix_ms"]
          <= kept["event"]["saved_unix_ms"]
          and kept["event"]["url"].startswith(f"blob:{browser['origin']}/")
          and kept["event"]["suggested_filename"] == files[0]["logical_path"] and kept["event"]["status"] == "saved")
    s3.write_json(state / "preview-report.json", dict(report, status="passed"))

    secrets = [value.encode() for name, value in os.environ.items()
               if name in ("JWT_SECRET", "DB_PASSWORD", "STORE_ACCESS", "STORE_SECRET", "CREDENTIAL_KEY")]
    secrets += [token.encode() for token in p1.TOKENS]
    jwt = re.compile(rb"eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}")
    leaked = sorted(path.name for path in state.iterdir() if path.is_file()
                    and (any(secret in path.read_bytes() for secret in secrets) or jwt.search(path.read_bytes())))
    if leaked:
        s3.write_json(state / "preview-report.json", dict(report, status="failed", leaked=leaked))
    check("evidence holds no token or secret", not leaked)


if __name__ == "__main__":
    main(Path(sys.argv[1]))

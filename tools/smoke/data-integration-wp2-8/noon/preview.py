"""#76's published noon order lines through the WP8.3 row comparison.

One stack runs the #76 checks, which publish three output versions of one
Pipeline dataset. The same independent reader then compares them by item
number through the run's real gateway API and real frontend page. A key match
computed here over the NDJSON downloaded from each version must agree with both.
The same reader then compares the schemas of the published order lines, the
input dataset, on that page.
"""
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import urllib.request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("noon_handoff", HERE / "smoke.py")
noon = importlib.util.module_from_spec(spec)
spec.loader.exec_module(noon)

KEY, LIMIT, BRANCH = "item_nr", 50, "main"
# Stated by the handoff, independently of every reader: the second batch adds
# items four and five and reprices the first; the replay republishes the first.
EXPECTED = {
    "v1-v2": dict(counts=dict(added=2, removed=0, modified=1, unchanged=2),
                  changes=[("NRPL0001-1", "modified"), ("NRPL0004-1", "added"), ("NRPL0005-1", "added")]),
    "v1-v3": dict(counts=dict(added=0, removed=0, modified=0, unchanged=3), changes=[]),
}
# The dev stub profile the isolated web configuration answers; the page shows it.
ANONYMIZE = ("P31 Admin", "p31-admin@example.com")
# Stated by the handoff, independent of every reader: v2 changed rows and a
# price under the same seven-column declaration; v3 declares the unit price as a
# number. The first pair is selected again while v3 is the head of the branch.
SCHEMA_GROUPS = (("v1-v2", 1, 2), ("v2-v3", 2, 3), ("v1-v2-again", 1, 2))
EXPECTED_SCHEMA = {
    "v1-v2": [], "v2-v3": [dict(sign="~", name="unit_price", hint="STRING → DOUBLE")], "v1-v2-again": [],
}
PUBLISHED = {1: "published-v1.ndjson", 2: "published-v2.ndjson", 3: "published-v3-numeric.ndjson"}
FIRST_READ = {1: "schema-v1-first.json", 2: "schema-v2-first.json", 3: "schema-v3-numeric.json"}


def index(records, key=KEY):
    rows = {}
    for record in records:
        if record[key] in rows:
            raise ValueError("duplicate key in NDJSON")
        rows[record[key]] = record
    return rows


def compare_records(base, target, key=KEY):
    """Independent key match over decoded NDJSON records; no service code is involved."""
    before, after = index(base, key), index(target, key)
    counts = dict(added=0, removed=0, modified=0, unchanged=0)
    changes = []
    for item in sorted(before.keys() | after.keys()):
        change = dict(key=item)
        if item not in before:
            change.update(kind="added", after=after[item])
        elif item not in after:
            change.update(kind="removed", before=before[item])
        elif before[item] != after[item]:
            change.update(kind="modified", before=before[item], after=after[item])
        else:
            counts["unchanged"] += 1
            continue
        counts[change["kind"]] += 1
        changes.append(change)
    return dict(key=key, counts=counts, changes=changes, changes_truncated=False)


def stated(oracle):
    return oracle["counts"], [(change["key"], change["kind"]) for change in oracle["changes"]]


def summary(counts):
    return " · ".join(f"{counts[kind]} {kind}" for kind in ("added", "removed", "modified", "unchanged"))


def decode(data):
    return [json.loads(line) for line in data.splitlines() if line.strip()]


def rows_and_price_changed(before, after):
    """Order line v2 read against v1: rows were added and a price changed."""
    oracle = compare_records(before, after)
    return stated(oracle) == (EXPECTED["v1-v2"]["counts"], EXPECTED["v1-v2"]["changes"]) \
        and [(c["before"]["unit_price"], c["after"]["unit_price"]) for c in oracle["changes"] if c["kind"] == "modified"] == [("49", "49.25")]


def only_unit_price_changed(before, after):
    """Order line v3 read against v2: every row kept, each differing in the unit price alone."""
    oracle = compare_records(before, after)
    return oracle["counts"] == dict(added=0, removed=0, modified=len(before), unchanged=0) and all(
        [name for name in c["before"] if c["before"][name] != c["after"][name]] == ["unit_price"]
        and float(c["before"]["unit_price"]) == c["after"]["unit_price"] for c in oracle["changes"])


def schema_hash(fields):
    """The content hash the dataset service seals a committed declaration with."""
    return hashlib.sha256(json.dumps(fields, separators=(",", ":")).encode()).hexdigest()


def committed_schema(body, binding):
    """The declared fields of one versioned schema answer.

    The answer must be bound to the given dataset, branch, version and
    transaction, hold at least one field and carry the hash of exactly those
    fields. Anything else is no schema to compare: it can never read as "no
    changes".
    """
    fields = body.get("fields")
    if ((body.get("dataset_id"), body.get("branch"), body.get("version"), body.get("transaction_id")) != binding
            or not fields or body.get("content_hash") != schema_hash(fields)):
        raise ValueError(f"no committed schema bound to {binding}")
    return fields


def states_schema(view, expected):
    """Whether the page's Schema diff panel states exactly the expected changes.

    Every item the panel holds is counted: a change, the one empty notice, or
    anything else. With changes there are that many and no empty notice;
    without any, one empty notice and no change. A panel that holds a change
    next to the empty notice, or anything unaccounted for, states nothing.
    """
    changes = len(expected)
    return (view["items"] == changes + (0 if changes else 1) and view["empty_notices"] == (0 if changes else 1)
            and view["rows"] == expected and view["other"] == [])


class LocalRedirects(urllib.request.HTTPRedirectHandler):
    """A redirect is followed only to this run's own loopback endpoints."""

    def __init__(self):
        self.hops = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        endpoints = {urlsplit(os.environ[name]).netloc for name in ("WP23_GATEWAY", "WP23_DVS")}
        if urlsplit(newurl).netloc not in endpoints:
            raise URLError(f"refused redirect to {urlsplit(newurl).netloc}")
        self.hops.append((code, urlsplit(newurl).netloc))
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def request(method, path, token, payload=None, base=None):
    """One request to the run's real edge gateway, or to another service of
    the stack, as the given identity.

    The bearer credential stays with the service it was presented to: it is
    never carried across a redirect, and a redirect is followed only to this
    run's own loopback endpoints. A download is authorized by the dataset
    service and its bytes come from that service's signed URL.
    """
    headers, data = {}, None
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = Request((base or os.environ["WP23_GATEWAY"]) + path, data=data, headers=headers, method=method)
    req.add_unredirected_header("Authorization", "Bearer " + token)
    redirects = LocalRedirects()
    try:
        with urllib.request.build_opener(redirects).open(req, timeout=30) as response:
            return dict(status=response.status, body=response.read(), answered=urlsplit(response.geturl()).netloc,
                        redirects=redirects.hops)
    except HTTPError as error:
        return dict(status=error.code, body=error.read(), answered=None, redirects=[])


def comparison_query(base, target, key=KEY):
    return dict(key=key, limit=str(LIMIT), base_branch=base["branch"], base_version=str(base["version"]),
                base_transaction=base["transaction_id"], target_branch=target["branch"],
                target_version=str(target["version"]), target_transaction=target["transaction_id"])


def pin(build):
    """The version and transaction one published output is selected by."""
    return dict(branch=BRANCH, version=build["output_version"], transaction_id=build["output_transaction"])


def input_schemas(handoff, handles, state, check):
    """The order lines the runs above read, as the independent reader sees them.

    The reader asks the schema route of each version through the gateway, which
    is the request the page makes, and of the dataset service itself, bound to
    the version and transaction. Both answer the bytes the handoff read for that
    version when it was published. The rows it downloaded then differ as the
    handoff states.
    """
    dataset, gate, reader = handoff["dataset"], handoff["schema_gate"], handles["reader"]
    transactions = {batch["version"]: batch["transaction"] for batch in handoff["batches"]}
    transactions[3] = gate["third"]["transaction"]
    versions = {}
    for version, transaction in sorted(transactions.items()):
        gateway = request("GET", f"/api/v1/datasets/{dataset}/schema?" + urlencode(dict(branch=BRANCH, version=version)), reader)
        check(f"gateway answers the schema of order line v{version} for the reader",
              gateway["status"] == 200 and gateway["answered"] == urlsplit(os.environ["WP23_GATEWAY"]).netloc, gateway)
        direct = request("GET", noon.schema_route(dataset, version, transaction_id=transaction), reader, base=os.environ["WP23_DVS"])
        check(f"dataset service answers the schema of order line v{version} bound to its version and transaction",
              direct["status"] == 200 and direct["answered"] == urlsplit(os.environ["WP23_DVS"]).netloc
              and direct["body"] == gateway["body"] == (state / FIRST_READ[version]).read_bytes(), direct)
        fields = committed_schema(json.loads(gateway["body"]), (dataset, BRANCH, version, transaction))
        (state / f"compare-schema-v{version}.gateway.json").write_bytes(gateway["body"])
        (state / f"compare-schema-v{version}.dvs.json").write_bytes(direct["body"])
        versions[version] = dict(transaction=transaction, fields=fields, content_hash=schema_hash(fields))

    declared = {version: [dict(name=name, field_type="DOUBLE" if version == 3 and name == "unit_price" else "STRING", nullable=False)
                          for name in noon.ORDER_LINE_COLUMNS] for version in transactions}
    check("order line v1 and v2 declare the same seven columns and v3 changes only the unit price to DOUBLE",
          {version: answer["fields"] for version, answer in versions.items()} == declared
          and versions[1]["content_hash"] == versions[2]["content_hash"] == gate["expected_schema_hash"]
          and versions[3]["content_hash"] == gate["third"]["content_hash"] != gate["expected_schema_hash"])

    # The receipt names the refused run, its pipeline and, in the input node's
    # own error, the dataset. The handoff saved that run's input binding.
    refused = json.loads((state / "schema-gate-refused-run.json").read_text())
    snapshot = gate["refused"]["saved_input"]["dataset_snapshot"]
    input_error = next((node.get("error") or "" for node in refused["node_results"] if node["node_id"] == "input"), "")
    refusal = dict(run_id=refused["run_id"], status=refused["status"], pipeline_id=refused["pipeline_id"],
                   dataset=gate["refused"]["saved_input"]["dataset_id"], branch=snapshot["branch"], version=snapshot["version"],
                   transaction=transactions[3], expected_schema_hash=snapshot["expected_schema_hash"],
                   content_hash=versions[3]["content_hash"], input_error=input_error)
    check("the compared v3 is the input the same run refused for the pinned schema hash",
          refused["run_id"] == gate["refused"]["run_id"] and refused["pipeline_id"] == gate["pipeline_id"]
          and refusal["status"] == "failed"
          and (refusal["dataset"], refusal["branch"], refusal["version"]) == (dataset, BRANCH, 3)
          and snapshot["transaction_id"] == "ri.foundry.main.transaction." + transactions[3]
          and refusal["expected_schema_hash"] == gate["expected_schema_hash"] == versions[1]["content_hash"]
          and refusal["content_hash"] != refusal["expected_schema_hash"]
          and f"failed to load dataset {dataset}:" in input_error
          and "dataset_input_schema_rejected" in input_error and "dataset_schema_content_hash_mismatch" in input_error)

    lines = {version: decode((state / name).read_bytes()) for version, name in PUBLISHED.items()}
    rows = {"v1-v2": compare_records(lines[1], lines[2]), "v2-v3": compare_records(lines[2], lines[3])}
    check("order line v2 added rows and changed a price under the same declaration", rows_and_price_changed(lines[1], lines[2]))
    check("order line v3 kept every row and changed only the unit price", only_unit_price_changed(lines[2], lines[3]))

    head = handles["p1"].api("dataset", "ds_branch_get", token=reader, path_args={"id": dataset, "branch": BRANCH})
    check("v3 is the head of the order lines while v1 and v2 answer their committed schemas",
          head.status == 200 and head.body["head_transaction_id"] == transactions[3]
          and {v["version"]: v["transaction_id"] for v in handles["graph"].list_versions(dataset)} == transactions)

    downloads = {batch["version"]: batch["sha256"] for batch in handoff["batches"]}
    downloads[3] = gate["third"]["sha256"]
    return dict(
        dataset=dataset, key=KEY,
        versions={str(version): dict(answer, rows=len(lines[version]), sha256=downloads[version],
                                     gateway=f"compare-schema-v{version}.gateway.json", dvs=f"compare-schema-v{version}.dvs.json",
                                     first_read=FIRST_READ[version])
                  for version, answer in versions.items()},
        rows=rows, refusal=refusal,
        groups=[dict(name=name, expected=EXPECTED_SCHEMA[name], base=dict(branch=BRANCH, version=base, transaction_id=transactions[base]),
                     target=dict(branch=BRANCH, version=target, transaction_id=transactions[target]))
                for name, base, target in SCHEMA_GROUPS])


def main(state):
    handles = noon.main(state)
    handoff = json.loads((state / "noon-report.json").read_text())
    builds, dataset = handoff["pipeline"]["builds"], handoff["pipeline"]["output_dataset"]
    inputs = {batch["version"]: batch for batch in handoff["batches"]}
    reader, other = handles["reader"], handles["other"]
    api = f"/api/v1/datasets/{dataset}"
    report = dict(status="failed", revision=handoff["revision"], dataset=dataset, key=KEY, limit=LIMIT,
                  identity=dict(label="independent reader of the first organization", permissions=[], roles=[],
                                stubs=["Vite identity bootstrap and profile only"]),
                  versions={}, groups={}, checks=[])

    def check(label, ok, reply=None):
        if not ok:
            raise AssertionError(label + (": " + repr(dict(status=reply["status"], redirects=reply["redirects"],
                                                             body=reply["body"][:300])) if reply else ""))
        report["checks"].append(label)
        print("PASS", label, flush=True)

    gateway = Path(os.environ["BIN_DIR"]) / "edge-gateway-service"
    running = subprocess.check_output(["docker", "exec", os.environ["WP23_GATEWAY_CONTAINER"], "sha256sum",
                                       "/binaries/edge-gateway-service"], text=True).split()[0]
    report["gateway"] = dict(binary_sha256=hashlib.sha256(gateway.read_bytes()).hexdigest())
    check("gateway runs the binary built from this checkout", running == report["gateway"]["binary_sha256"])

    lines = {}
    for build in builds:
        version = build["output_version"]
        path = f"{api}/files/{build['output_file_id']}/download"
        # The gateway follows the dataset service's redirect itself, to a signed
        # loopback URL it cannot reach. Its answer is recorded, never asserted.
        gateway_reply = request("GET", path, reader)
        reply = request("GET", path, reader, base=os.environ["WP23_DVS"])
        body, digest = reply["body"], hashlib.sha256(reply["body"]).hexdigest()
        check(f"reader's dataset service download returns pipeline output v{version}",
              reply["status"] == 200 and digest == build["sha256"] and body == (state / build["artifact"]).read_bytes(),
              reply)
        lines[version] = decode(body)
        report["versions"][str(version)] = dict(
            pipeline_run_id=build["pipeline_run_id"], input_version=build["input_version"],
            input_transaction=build["input_transaction"], input_sha256=inputs[build["input_version"]]["sha256"],
            output_transaction=build["output_transaction"], output_file_id=build["output_file_id"],
            output_sha256=digest, rows=len(lines[version]), download=dict(
                entry="dataset service download, as the handoff's reader checks",
                requested=urlsplit(os.environ["WP23_DVS"]).netloc, answered=reply["answered"],
                redirects=reply["redirects"], gateway=dict(
                    status=gateway_reply["status"],
                    error=json.loads(gateway_reply["body"]).get("error", {}).get("code") if gateway_reply["status"] != 200 else None)))

    groups = []
    for name, first, second in (("v1-v2", 0, 1), ("v1-v3", 0, 2)):
        base, target = pin(builds[first]), pin(builds[second])
        oracle = compare_records(lines[base["version"]], lines[target["version"]])
        check(f"NDJSON key match states the {name} changes of the handoff",
              stated(oracle) == (EXPECTED[name]["counts"], EXPECTED[name]["changes"])
              and [(c.get("before", {}).get("unit_price"), c.get("after", {}).get("unit_price"))
                   for c in oracle["changes"] if c["kind"] == "modified"]
              == ([("49", "49.25")] if name == "v1-v2" else []))
        path = f"{api}/compare/rows?" + urlencode(comparison_query(base, target))
        reply = request("GET", path, reader)
        body = json.loads(reply["body"])
        check(f"gateway API compares {name} by item number for the reader",
              reply["status"] == 200 and reply["answered"] == urlsplit(os.environ["WP23_GATEWAY"]).netloc
              and body == {**oracle, "dataset_id": dataset, "base": base, "target": target}, reply)
        noon.s3.write_json(state / f"compare-{name}.api.json", body)
        groups.append(dict(name=name, base=base, target=target, summary=summary(oracle["counts"]),
                           rows=oracle["changes"], query=comparison_query(base, target)))
        report["groups"][name] = dict(ndjson=oracle, gateway=dict(http_status=reply["status"], **body))

        denied = request("GET", path, other)
        check(f"other tenant cannot compare {name}", denied["status"] == 404 and b'"counts"' not in denied["body"],
              denied)
    versions = len(handles["graph"].list_versions(dataset))
    refused = request("POST", api + "/outputs:commit", reader, payload=dict(
        idempotency_key=f"noon-preview:{dataset}", branch=BRANCH, transaction_type="SNAPSHOT", format="json",
        files=[dict(logical_path="order-lines.ndjson", operation="ADD", content_type="application/x-ndjson",
                    size_bytes=len(noon.encode(lines[1])),
                    content_base64=base64.b64encode(noon.encode(lines[1])).decode())]))
    check("reader cannot publish to the compared dataset", refused["status"] == 403
          and len(handles["graph"].list_versions(dataset)) == versions, refused)

    schema = input_schemas(handoff, handles, state, check)
    report["schema_diff"] = {name: schema[name] for name in ("dataset", "key", "versions", "rows", "refusal")}
    noon.s3.write_json(state / "compare-ready.json", dict(dataset=dataset, key=KEY, limit=LIMIT, groups=groups,
                                                          schema=dict(dataset=schema["dataset"], groups=schema["groups"]),
                                                          anonymize=list(ANONYMIZE)))
    token_file = state / "browser.jwt"
    try:
        token_file.write_text(reader)
        subprocess.run(["bash", str(HERE / "preview-ui.sh"), str(state)], check=True)
    finally:
        token_file.unlink(missing_ok=True)
    browser = json.loads((state / "browser.json").read_text())
    check("browser compared both groups", browser["status"] == "passed")

    for group in groups:
        name, base, target = group["name"], group["base"], group["target"]
        view = browser["groups"][name]
        sent = view["request"]
        check(f"page shows the {name} changes of the gateway API and the NDJSON key match",
              view["summary"] == group["summary"] and view["rows"] == group["rows"] == report["groups"][name]["gateway"]["changes"]
              and view["error"] is None and view["table"] is (len(group["rows"]) > 0))
        check(f"page asked the gateway for the selected {name} versions and key",
              sent["path"] == f"{api}/compare/rows" and sent["params"] == group["query"]
              and sent["status"] == 200 and sent["transferred_bytes"] > 0
              and view["navigation_unix_ms"] <= sent["started_unix_ms"] < sent["completed_unix_ms"]
              <= view["captured_at_unix_ms"])
        check(f"page selects the {name} versions",
              all(view["selectors"][side]["value"] == f"transaction:{chosen['transaction_id']}"
                  and view["selectors"][side]["label"] == f"v{chosen['version']} · {BRANCH} · {chosen['transaction_id'][:8]}"
                  for side, chosen in (("A", base), ("B", target)))
              and all(f"transaction:{builds[2]['output_transaction']}" in view["selectors"][side]["options"]
                      for side in ("A", "B")))
        report["groups"][name]["page"] = view
        check(f"page keeps the {name} versions selected and readable without a schema preview",
              view["alerts"] == [] and view["unavailable_labels"] == 0 and view["schema_notices"] == 2)

    route = f"/api/v1/datasets/{schema['dataset']}/schema"
    for ordinal, (group, (name, _, _)) in enumerate(zip(schema["groups"], SCHEMA_GROUPS)):
        view = browser["schema"][name]
        chosen = dict(A=group["base"], B=group["target"])
        # A side selected afresh asks for its schema again: both sides of the
        # first selection, and each side whose selection this one changed.
        earlier = {} if ordinal == 0 else dict(A=schema["groups"][ordinal - 1]["base"], B=schema["groups"][ordinal - 1]["target"])
        asked_again = [side for side, pin in chosen.items() if earlier.get(side) != pin]
        sent = {side: view["requests"][side] for side in chosen}
        check(f"page shows the {name} schema changes of the committed order line declarations",
              view["panel"] is True and states_schema(view, EXPECTED_SCHEMA[name])
              and all(view["sides"][side]["fields"] == len(schema["versions"][str(pin["version"])]["fields"]) == len(noon.ORDER_LINE_COLUMNS)
                      for side, pin in chosen.items()))
        check(f"page asked the gateway for the selected {name} order line versions",
              view["navigation_unix_ms"] <= view["step_started_unix_ms"] <= view["read_unix_ms"]
              and all(sent[side]["path"] == route and sent[side]["params"] == dict(branch=BRANCH, version=str(chosen[side]["version"]))
                  and sent[side]["status"] == 200 and sent[side]["transferred_bytes"] > 0
                  and view["navigation_unix_ms"] <= sent[side]["started_unix_ms"] < sent[side]["completed_unix_ms"] <= view["read_unix_ms"]
                  and (side not in asked_again or view["step_started_unix_ms"] <= sent[side]["started_unix_ms"])
                  for side in chosen))
        check(f"page selects the {name} order line versions",
              all(view["selectors"][side]["value"] == f"transaction:{pin['transaction_id']}"
                  and view["selectors"][side]["label"] == f"v{pin['version']} · {BRANCH} · {pin['transaction_id'][:8]}"
                  and {f"transaction:{answer['transaction']}" for answer in schema["versions"].values()}
                  <= set(view["selectors"][side]["options"])
                  for side, pin in chosen.items()))
        check(f"page keeps the {name} order line versions selected with their schemas",
              view["alerts"] == [] and view["unavailable_labels"] == 0 and view["schema_notices"] == 0 and view["files_panel"] is False)
        report["schema_diff"].setdefault("groups", {})[name] = dict(base=group["base"], target=group["target"],
                                                                  asked_again=asked_again, page=view)
    # Every capture reads five panels the page never shows with the code that
    # reads its own: each statement alone, and each next to what it excludes.
    # The page's own panel is then judged by the same rule, here and as captured.
    change_with_notice, change_with_other, change, notice, notice_with_other = browser["schema"]["v1-v2"]["control"]
    check("the page reading knows each statement alone and refuses a change next to the empty notice",
          states_schema(change, change["rows"]) and states_schema(notice, [])
          and not any(states_schema(mixed, expected) for mixed in (change_with_notice, change_with_other, notice_with_other)
                      for expected in ([], EXPECTED_SCHEMA["v2-v3"])))
    first, again = (browser["schema"][name] for name in ("v1-v2", "v1-v2-again"))
    check("page states the v1-v2 order line schemas again in the same session while v3 is the head",
          again["navigation_unix_ms"] == first["navigation_unix_ms"] and again["step_started_unix_ms"] >= first["read_unix_ms"]
          and all(again[read] == first[read]
                  for read in ("selectors", "sides", "items", "rows", "empty_notices", "other", "panel", "files_panel")))
    noon.s3.write_json(state / "preview-report.json", dict(report, status="passed"))

    secrets = [value.encode() for name, value in os.environ.items()
               if name in ("JWT_SECRET", "DB_PASSWORD", "STORE_ACCESS", "STORE_SECRET", "CREDENTIAL_KEY")]
    secrets += [token.encode() for token in handles["p1"].TOKENS]
    jwt = re.compile(rb"eyJ[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}")
    leaked = sorted(path.name for path in state.iterdir() if path.is_file()
                    and (any(secret in path.read_bytes() for secret in secrets) or jwt.search(path.read_bytes())))
    if leaked:
        noon.s3.write_json(state / "preview-report.json", dict(report, status="failed", leaked=leaked))
    check("evidence holds no token or secret", not leaked)


if __name__ == "__main__":
    main(Path(sys.argv[1]))

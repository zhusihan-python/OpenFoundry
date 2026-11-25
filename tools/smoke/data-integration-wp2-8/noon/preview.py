"""#76's published noon order lines through the WP8.3 row comparison.

One stack runs the #76 checks, which publish three output versions of one
Pipeline dataset. The same independent reader then compares them by item
number through the run's real gateway API and real frontend page. A key match
computed here over the NDJSON downloaded from each version must agree with both.
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

    noon.s3.write_json(state / "compare-ready.json", dict(dataset=dataset, key=KEY, limit=LIMIT, groups=groups,
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

"""DVS publication and tenant-boundary evidence outside timed WP3.15 windows."""

import base64
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sys
from urllib.error import HTTPError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, build_opener, urlopen
from uuid import uuid4

HERE = Path(__file__).resolve().parent
sys.path.append(str(HERE.with_name("data-integration-wp3-13")))
import acceptance as writer

stack = writer.stack
expect = writer.expect
ROW_COUNT = 100_000
MIME_TYPES = {"ndjson": "application/x-ndjson", "parquet": writer.MIME}


def _download_product(rid, file, trial, fmt, size, digest):
    """Fetch the authenticated redirect, then stream the signed data anonymously."""
    evidence = Path(stack.EVIDENCE).resolve()
    if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", trial):
        raise AssertionError("invalid benchmark trial name")
    relative = Path("products") / f"{trial}.{fmt}"
    target = (evidence / relative).resolve()
    if not target.is_relative_to(evidence):
        raise AssertionError("download artifact path is outside evidence directory")
    target.parent.mkdir(parents=True, exist_ok=True)

    request = Request(writer.dvs(rid, f"files/{file['id']}/download"),
                      headers={"Authorization": f"Bearer {stack.TOKEN}"})
    try:
        with build_opener(writer.NoRedirect()).open(request, timeout=30):
            raise AssertionError("file authorization route did not return a redirect")
    except HTTPError as response:
        with response:
            expect("file download redirect", response.code, 302)
            location = response.headers["Location"]
    signed, service = urlsplit(location), urlsplit(stack.DVS)
    expect("signed download origin", (signed.scheme, signed.netloc), (service.scheme, service.netloc))
    expect("signed local download path", signed.path.startswith("/v1/_internal/local-fs/"), True)

    hashed = sha256()
    received = 0
    temporary = target.with_suffix(target.suffix + ".partial")
    with urlopen(location, timeout=60) as response:
        expect("signed download status", response.status, 200)
        transport_type = response.headers.get_content_type()
        expect("signed local bytes transport type", transport_type, "application/octet-stream")
        with temporary.open("xb") as artifact:
            while block := response.read(256 * 1024):
                received += len(block)
                if received > size:
                    raise AssertionError("signed download exceeded the committed artifact size")
                artifact.write(block)
                hashed.update(block)
    expect("signed artifact size", received, size)
    expect("signed artifact SHA-256", hashed.hexdigest(), digest)
    if target.exists():
        raise AssertionError("benchmark product artifact already exists")
    temporary.rename(target)
    return {"artifact": relative.as_posix(), "artifact_path": str(target),
            "download_http_status": 200, "download_content_type": transport_type}


def _scan_url(rid, version, cursor=None):
    params = {"branch": "main", "version": version, "all_files": "true", "limit": 2}
    if cursor is not None:
        params["cursor"] = cursor
    return writer.dvs(rid, "files/rows:scan?" + urlencode(params))


def _checked_first_page(rid, version, txn, fmt):
    page = writer.get(_scan_url(rid, version))
    expect("scan dataset/branch/version/transaction",
           (page["dataset_id"], page["branch"], page["version"], page["transaction_id"]),
           (writer.id_tail(rid), "main", version, txn))
    expect("scan format", page["format"], fmt)
    expect("scan mode", page["mode"], "all_files")
    if fmt == "parquet":
        expect("scan schema version", page["schema_version"], writer.SCHEMA)
    return page


def verify_output(rid, before, build, fmt, execute):
    """Cross-check the actual published object after the timed execute call.

    artifact_path is the absolute local filename for DuckDB. Persist artifact
    (the evidence-relative filename) rather than this host-specific path.
    """
    mime = MIME_TYPES[fmt]
    status, response = execute
    expect("execute HTTP status", status, 200)
    expect("execute build", response["build_id"], build["build"])
    expect("execute build terminal state", response["state"], "BUILD_COMPLETED")
    detail = response["batched"]
    expect("first publish ACK", detail.get("replay", False), False)
    expect("PBS output dataset and branch", (detail["dataset_rid"], detail["branch"]),
           (stack.canon(rid), "main"))

    version = writer.new_version(rid, before)
    txn = writer.id_tail(version["transaction_id"])
    reservation = writer.id_tail(build["reservation"])
    expect("PBS ACK names the published transaction", writer.id_tail(detail["published_transaction_rid"]), txn)
    expect("PBS persisted completed build", stack.build_state(build["build"]), ["BUILD_COMPLETED"])
    expect("PBS persisted one committed job output",
           [(writer.id_tail(ref), committed) for ref, committed in stack.job_outputs_of(build["build"])],
           [(txn, "t")])
    expect("PBS reservation and input references released", stack.build_refs(build["build"]), (0, 0))
    expect("DVS reservation finalized", writer.get(writer.transaction(rid, reservation))["status"], "ABORTED")
    tx = writer.get(writer.transaction(rid, txn))
    expect("DVS committed SNAPSHOT identity",
           (tx["id"], tx["dataset_id"], tx["branch_name"], tx["status"], tx["tx_type"]),
           (txn, writer.id_tail(rid), "main", "COMMITTED", "SNAPSHOT"))
    key = detail["idempotency_key"]
    receipt = writer.get(writer.dvs(rid, "outputs/commits/" + quote(key, safe="")))
    expect("DVS keyed receipt names the committed transaction and branch",
           (receipt["transaction"]["id"], receipt["idempotency_key"], receipt["branch"]), (txn, key, "main"))

    file, = [item for item in writer.file_list(rid) if item["transaction_id"] == txn]
    logical = build.get("logical_path", f"part-00000.{fmt}")
    expect("committed logical file", file["logical_path"], logical)
    index, = writer.get(writer.dvs(rid, "files/index"))
    expect("file index has the current logical file", index["path"], logical)
    expect("indexed MIME for committed format", index["content_type"], mime)
    expect("serving file and index reference the same object", index["storage_path"], file["physical_uri"])
    physical_uri = urlsplit(file["physical_uri"])
    expect("server transaction-owned local storage key",
           (physical_uri.scheme, physical_uri.netloc,
            physical_uri.path.endswith(f"/datasets/{writer.id_tail(rid)}/transactions/{txn}/{logical}")),
           ("local", "", True))
    obj, size, digest = writer.id_tail(detail["staged_object_id"]), detail["size_bytes"], detail["sha256"]
    expect("positive nonempty committed artifact size", type(size) is int and size > 0, True)
    expect("coherent published file sizes", (file["size_bytes"], index["size_bytes"]), (size, size))
    expect("PBS sealed row count", detail["record_count"], ROW_COUNT)
    expected_meta = {
        "dataset_id": writer.id_tail(rid), "transaction_id": txn, "build_id": build["build"],
        "staged_object_id": obj, "record_count": ROW_COUNT, "sha256": digest,
    }
    if fmt == "parquet":
        expected_meta.update(format=fmt, schema_version=writer.SCHEMA)
    meta = index["metadata"]
    expect("server-stamped file index identity and digest",
           {name: meta[name] for name in expected_meta}, expected_meta)
    if file.get("sha256") is not None:
        expect("optional file projection digest", file["sha256"], digest)

    descriptor = stack.pg("wp33c_dvs", f"""
        SELECT id,status,consumed_by_transaction,logical_path,size_bytes,sha256,record_count
        FROM dataset_staged_output_objects
        WHERE reservation_id='{reservation}'
    """)
    expect("DVS exact sealed descriptor consumed",
           descriptor, [f"{obj}|CONSUMED|{txn}|{logical}|{size}|{digest}|{ROW_COUNT}"])
    owner_file = build["old_file"]
    expect("prior published bytes unchanged", sha256(writer.download(rid, owner_file)).hexdigest(),
           before["old_file_sha256"])
    download = _download_product(rid, file, build["trial"], fmt, size, digest)
    page = _checked_first_page(rid, version["version"], txn, fmt)
    expect("two committed rows on the pinned test page", (page["row_count"], len(page["rows"])), (2, 2))
    expect("pinned test page has remaining source rows", bool(page["next_cursor"]), True)

    return {
        "status": "passed", "dataset": writer.id_tail(rid), "branch": "main", "version": version["version"],
        "transaction": txn, "file_id": file["id"], "logical_path": logical,
        "format": fmt, "mime": mime, "record_count": ROW_COUNT, "size_bytes": size, "sha256": digest,
        "staged_object_id": obj, "reservation": reservation,
        "scan": {"status": 200, "format": page["format"], "row_count": page["row_count"],
                 "schema_version": page.get("schema_version"), "next_page": True},
        **download,
    }


def _fixture_claims(token):
    payload = token.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


def _foreign_download_denial(rid, file_id):
    request = Request(writer.dvs(rid, f"files/{file_id}/download"),
                      headers={"Authorization": f"Bearer {stack.TOKEN}"})
    try:
        with build_opener(writer.NoRedirect()).open(request, timeout=30):
            raise AssertionError("foreign file authorization unexpectedly succeeded")
    except HTTPError as response:
        with response:
            expect("foreign download denied", response.code, 404)
            expect("foreign download returned no signed URL", "Location" in response.headers, False)
            return json.loads(response.read())


def _state_digest(published):
    rid = published["dataset"]
    file, = [f for f in writer.file_list(rid) if f["id"] == published["file_id"]]
    state = writer.output_state(rid, file)
    expect("owner readback still matches published SHA-256", state["old_file_sha256"], published["sha256"])
    return sha256(json.dumps(state, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def verify_isolation(publications):
    """Run authenticated foreign-org probes on the first measured output pair."""
    products = list(publications)
    expect("tenant probe covers both formats", sorted(p["format"] for p in products), ["ndjson", "parquet"])
    owner, foreign = stack.TOKEN, os.environ["WP315_FOREIGN_TOKEN"]
    own_claims, foreign_claims = _fixture_claims(owner), _fixture_claims(foreign)
    expect("tenant probe has one subject", own_claims["sub"] == foreign_claims["sub"], True)
    expect("tenant probe has two explicit distinct organizations",
           bool(own_claims["org_id"] and foreign_claims["org_id"] and own_claims["org_id"] != foreign_claims["org_id"]), True)

    before = {}
    for product in products:
        rid = product["dataset"]
        page = _checked_first_page(rid, product["version"], product["transaction"], product["format"])
        expect("owner issued a continuation cursor", bool(page["next_cursor"]), True)
        before[rid] = (_state_digest(product), page["next_cursor"])

    denials = []
    absent = str(uuid4())
    try:
        stack.TOKEN = foreign
        control = stack.create_dataset("wp315-foreign-control")
        stack.ensure_main_branch(control)
        raw = b'{"foreign_control":true}\n'
        own_receipt = stack.inline_commit(control, "wp315-foreign-control", [("control.ndjson", raw)], "foreign own scope")
        txn = own_receipt["transaction"]["id"]
        version = stack.version_by_txn(control, txn)
        own_page = _checked_first_page(control, version, txn, "ndjson")
        expect("foreign identity can read its own committed row", own_page["rows"], [{"foreign_control": True}])
        own_file, = [f for f in writer.file_list(control) if f["transaction_id"] == txn]
        expect("foreign identity can download from its own organization", writer.download(control, own_file), raw)

        for product in products:
            rid, file_id, v = product["dataset"], product["file_id"], product["version"]
            cursor = before[rid][1]
            for request_cursor in (None, cursor):
                denied_status, denied = stack.http("GET", _scan_url(rid, v, request_cursor), allow=(404,))
                missing_status, missing = stack.http("GET", _scan_url(absent, v, request_cursor), allow=(404,))
                expect("foreign dataset and missing dataset both return HTTP 404", (denied_status, missing_status), (404, 404))
                expect("foreign pinned scan equals missing dataset response", denied, missing)
            expect("foreign download equals missing dataset response",
                   _foreign_download_denial(rid, file_id), _foreign_download_denial(absent, file_id))
            denials.append({"format": product["format"], "dataset": rid, "version": v,
                            "pinned_scan": 404, "cursor_scan": 404, "download": 404,
                            "matches_missing": True, "signed_url_returned": False})
    finally:
        stack.TOKEN = owner

    for denied, product in zip(denials, products, strict=True):
        previous = before[product["dataset"]][0]
        after = _state_digest(product)
        expect("owner-visible state and signed bytes unchanged by foreign reads", after, previous)
        denied.update(before_state_sha256=previous, after_state_sha256=after)
    return {"status": "passed", "same_subject": True, "distinct_organizations": True,
            "foreign_own_control": {"dataset": control, "version": version, "transaction": txn, "scan": 200, "download": 200},
            "denials": denials}

"""Amazon monthly report handoff: S3 envelope, receipt and registration checks, Dataset commit bodies.

Field names follow the pinned upstream bridge (unilume 6f3103e,
desktop/packs/ziniao-bridge/src/report-upload.ts:113-124, 155-177 and 278-292).
A delivered envelope carries what was submitted and the receipt the sender got for it:

- a file: registration {storeId, marketplace, reportKind, period, coverageStart, coverageEnd,
  completeness, sha256, byteSize, contentType} and receipt {status: "uploaded", fileId};
- a month without data: report {storeId, marketplace, reportKind, period, checkedAt} and
  receipt {status: "reported", requirementCode?}.

checkedAt is only the request time of a no-data report; the receipt says whether it was reported.
Neither a registration nor a report names a tenant: that comes from the signed caller.
"""
import base64
import binascii
import calendar
from datetime import datetime
import hashlib
import json
import re
import uuid

# Simulated identifiers. No real store, account or order is referenced.
STORE = "5e2f0d8a-6b1c-4e3f-9a7d-2c4b6e8f0a12"
MARKETPLACE = "amazon-us"
REPORT_KIND = "transaction_view"
PERIOD = re.compile(r"\d{4}-(0[1-9]|1[0-2])")
CHECKED_AT = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z")
SCOPE = ("storeId", "marketplace", "reportKind", "period")


class RegistrationError(ValueError):
    """A delivered envelope is not a registered file or a reported no-data month."""


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def coverage(period):
    """transaction_view covers its whole month: first to last day, completeness full."""
    year, month = map(int, period.split("-"))
    return dict(coverageStart=f"{period}-01", coverageEnd=f"{period}-{calendar.monthrange(year, month)[1]:02d}",
                completeness="full")


def registration(data, period):
    return dict(storeId=STORE, marketplace=MARKETPLACE, reportKind=REPORT_KIND, period=period, **coverage(period),
                sha256=sha256(data), byteSize=len(data), contentType="text/csv")


def envelope(kind, **payload):
    """One NDJSON line. The S3 Connector keeps a line only if it is a JSON object and trims each
    line, so the staged bytes equal the committed bytes and the CSV survives inside base64."""
    return json.dumps(dict(kind=kind, **payload), sort_keys=True, separators=(",", ":")).encode() + b"\n"


def object_key(period, delivery):
    return f"reports/{STORE}/{MARKETPLACE}/{REPORT_KIND}/{period}/{delivery}.ndjson"


def _scope(record):
    identity = tuple(record.get(name) for name in SCOPE[:3])
    if identity != (STORE, MARKETPLACE, REPORT_KIND) or not PERIOD.fullmatch(record.get("period") or ""):
        raise RegistrationError("not for this store, marketplace, report kind or YYYY-MM period")


def _file_id(receipt):
    try:
        return str(uuid.UUID(receipt.get("fileId")))
    except (ValueError, AttributeError, TypeError):
        raise RegistrationError("uploaded receipt has no valid fileId") from None


def unpack(body, period):
    """Check a committed envelope routed for `period`.

    Returns (record, fileId, file bytes) for a registered file, or (report, None, None) for a
    reported no-data month. Anything else is refused: a receipt other than uploaded or reported,
    a file that is not what it is registered as, or a registration outside its period.
    """
    envelope = json.loads(body)
    kind = envelope.get("kind")
    if kind not in ("report", "no_data"):
        raise RegistrationError(f"unknown envelope kind {kind!r}")
    record = envelope.get("registration" if kind == "report" else "report") or {}
    _scope(record)
    if record["period"] != period:
        raise RegistrationError(f"envelope routed for {period} carries period {record['period']}")
    receipt = envelope.get("receipt") or {}
    if kind == "no_data":
        if receipt.get("status") != "reported":
            raise RegistrationError(f"no-data receipt is {receipt.get('status')!r}, not 'reported'")
        if not CHECKED_AT.fullmatch(record.get("checkedAt") or ""):
            raise RegistrationError("reported no-data has no checkedAt")
        report = {name: record[name] for name in SCOPE + ("checkedAt",)}
        if receipt.get("requirementCode") is not None:
            report["requirementCode"] = receipt["requirementCode"]
        return report, None, None
    if receipt.get("status") != "uploaded":
        raise RegistrationError(f"file receipt is {receipt.get('status')!r}, not 'uploaded'")
    file_id = _file_id(receipt)
    if {name: record.get(name) for name in coverage(period)} != coverage(period):
        raise RegistrationError("registered coverage is not the whole period")
    if record.get("contentType") != "text/csv":
        raise RegistrationError("registered contentType is not text/csv")
    try:
        data = base64.b64decode(envelope.get("content_base64") or "", validate=True)
    except (binascii.Error, ValueError):
        raise RegistrationError("envelope content is not base64") from None
    if len(data) != record.get("byteSize"):
        raise RegistrationError("file length differs from registered byteSize")
    if sha256(data) != record.get("sha256"):
        raise RegistrationError("file sha256 differs from registered sha256")
    return record, file_id, data


def _commit(record, key, path, content_type, data):
    return dict(idempotency_key=key, branch="main", transaction_type="SNAPSHOT",
                format="csv" if content_type == "text/csv" else "json",
                files=[dict(logical_path=path, operation="ADD", content_type=content_type, size_bytes=len(data),
                            content_base64=base64.b64encode(data).decode())])


def file_commit(record, file_id, data):
    """Publish the original CSV bytes of one registered file.

    The request depends only on the registration, its fileId and the bytes, so an identical
    redelivery rebuilds it and replays the first receipt. The key separates files of one month.
    """
    return dict(_commit(record, "amazon-report:" + ":".join([*(record[name] for name in SCOPE), file_id]),
                        f"{MARKETPLACE}/{REPORT_KIND}/{record['period']}/{file_id}.csv", "text/csv", data),
                metadata=dict(registration=record, file_id=file_id))


def no_data_commit(report):
    """Record one reported no-data month. It goes to a status dataset, never to the report files."""
    return dict(_commit(report, "amazon-report:" + ":".join(report[name] for name in SCOPE),
                        f"{MARKETPLACE}/{REPORT_KIND}/{report['period']}.no-data.json", "application/json",
                        json.dumps(report, sort_keys=True, separators=(",", ":")).encode()),
                metadata=dict(no_data=report))

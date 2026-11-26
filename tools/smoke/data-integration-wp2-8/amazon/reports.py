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
import csv
from datetime import datetime
import hashlib
import io
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
# A report opens with one preamble record, All amounts in USD. Its header is the next record.
HEADER_ROW = 2
# The business columns of the report kind. They name the type a column may hold; the cells of one
# file decide whether it holds (csv_schema). The same lists as transactions.AMOUNTS and COUNTS,
# written out again: the publication does not import what a reader expects of a report.
AMOUNTS = ["product sales", "product sales tax", "shipping credits", "shipping credits tax", "gift wrap credits",
           "giftwrap credits tax", "regulatory fee", "tax on regulatory fee", "promotional rebates",
           "promotional rebates tax", "marketplace withheld tax", "selling fees", "fba fees",
           "other transaction fees", "other", "total"]
COUNTS = ("settlement id", "quantity")
# The characters Go's strings.TrimSpace removes. They are named here, so the trim does not follow
# what the Python version strips.
GO_SPACE = "\t\n\v\f\r \u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"
NUMBER = re.compile(r"-?(0|[1-9][0-9]*)(\.[0-9]+)?")
# What the Dataset CSV reader reads at most in one record (datasetFileRowsMaxColumns, file_rows.go).
MAX_COLUMNS = 256


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


def reader_number(cell):
    r"""One cell as the Dataset CSV reader gives it (csvCellValue, file_rows.go).

    Returns the trimmed text of a number, None for a cell that is empty once trimmed, and False
    for a string. The reader trims whitespace and takes -?(0|[1-9][0-9]*)(\.[0-9]+)? as a number
    when it is an int64 or has at most 15 integer digits. Decimal would also parse 1e3, +1 and
    001.00, so whether a column is a number does not follow Decimal.
    """
    text = cell.strip(GO_SPACE)
    if not text:
        return None
    if not NUMBER.fullmatch(text):
        return False
    whole, _, fraction = text.partition(".")
    digits = len(whole.removeprefix("-"))
    if digits <= 15:
        return text
    if fraction or digits > 19:
        return False
    # The length is known before int() runs: Python refuses an integer string of thousands of digits,
    # and a cell that long is a string here as it is for the reader.
    return text if -(1 << 63) <= int(whole) <= (1 << 63) - 1 else False


def column_names(header):
    """The names the Dataset CSV reader gives the columns of a record (csvColumnNames, file_rows.go).

    Each header cell is trimmed. A cell that is empty once trimmed is column_<position>, counted
    from 1. A name that comes twice is refused as it is met, and a header with more than
    MAX_COLUMNS columns is refused after it: the reader refuses such a file, so a column of it
    cannot be read or typed.
    """
    names, seen = [], set()
    for position, cell in enumerate(header, 1):
        name = cell.strip(GO_SPACE) or f"column_{position}"
        if name in seen:
            raise ValueError(f"the report names the column {name!r} twice")
        seen.add(name)
        names.append(name)
    if len(names) > MAX_COLUMNS:
        raise ValueError(f"the report has {len(names)} columns, more than the {MAX_COLUMNS} the Dataset CSV reader reads")
    return names


def field_type(name, cells):
    """The type of one column: the one its business role names, if the cells of this file hold it."""
    if name not in AMOUNTS and name not in COUNTS:
        return "STRING"
    numbers = [reader_number(cell) for cell in cells]
    if any(number is False for number in numbers):
        return "STRING"
    if name in COUNTS and any("." in number for number in numbers if number is not None):
        return "STRING"
    return "LONG" if name in COUNTS else "DOUBLE"


def csv_schema(data):
    """The schema of a published report: one field per header column, in header order.

    A field is named as the Dataset CSV reader names the column (column_names), and the business
    role of a column follows that name: a header cell padded with spaces is still the column it
    names. The role names the type the column may hold and the cells of this file decide whether
    it holds: an amount is DOUBLE and a count is LONG while every cell that is not empty is a
    number the Dataset CSV reader gives as one. Any other column is STRING, and so is an amount
    or a count that holds text in this file. Every field is nullable, because an empty cell is
    null in any column. Nullability is not read from one file's cells, or two files of one month
    would carry two schemas.
    """
    records = [record for record in csv.reader(io.StringIO(data.decode("utf-8-sig"), newline="")) if record]
    if len(records) < HEADER_ROW:
        raise ValueError("the report has no header record")
    header, rows = column_names(records[HEADER_ROW - 1]), records[HEADER_ROW:]
    if any(len(row) != len(header) for row in rows):
        raise ValueError("a record of the report does not match its header")
    return [dict(name=name, type=field_type(name, [row[at] for row in rows]), nullable=True)
            for at, name in enumerate(header)]


def csv_layout(data):
    """Where the dataset service finds the header of a published report, and the schema of its columns.

    It reads the header of a CSV from its first record unless the publication says otherwise.
    A report carries its header on the second record, after the preamble. The schema is declared
    by the publication, from the committed bytes: the dataset service derives none of it, and its
    versioned schema route answers it to a reader that asks by version and transaction.
    """
    return {"file_format": "TEXT", "fields": csv_schema(data),
            "custom_metadata": {"csv": {"headerRow": HEADER_ROW, "charset": "UTF-8", "bom": "strip"}}}


def file_commit(record, file_id, data):
    """Publish the original CSV bytes of one registered file.

    The request depends only on the registration, its fileId and the bytes, so an identical
    redelivery rebuilds it and replays the first receipt. The key separates files of one month.
    The schema says where the header of the file is and the type of each column; the bytes are
    published as they are.
    """
    return dict(_commit(record, "amazon-report:" + ":".join([*(record[name] for name in SCOPE), file_id]),
                        f"{MARKETPLACE}/{REPORT_KIND}/{record['period']}/{file_id}.csv", "text/csv", data),
                schema=csv_layout(data), metadata=dict(registration=record, file_id=file_id))


def no_data_commit(report):
    """Record one reported no-data month. It goes to a status dataset, never to the report files."""
    return dict(_commit(report, "amazon-report:" + ":".join(report[name] for name in SCOPE),
                        f"{MARKETPLACE}/{REPORT_KIND}/{report['period']}.no-data.json", "application/json",
                        json.dumps(report, sort_keys=True, separators=(",", ":")).encode()),
                metadata=dict(no_data=report))

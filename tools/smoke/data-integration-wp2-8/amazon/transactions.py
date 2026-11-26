"""What one Pipeline publishes for the transactions of a published report, and how a reader reconciles it.

These helpers serve the Pipeline acceptance only. The publication path is reports.py.
"""
import copy
import csv
from decimal import Decimal
import io
import json
import re

# A transaction is named by these columns of its report. The sixteen amount columns of a report follow.
IDENTITY = ["date/time", "settlement id", "type", "order id", "sku", "description", "quantity"]
AMOUNTS = ["product sales", "product sales tax", "shipping credits", "shipping credits tax", "gift wrap credits",
           "giftwrap credits tax", "regulatory fee", "tax on regulatory fee", "promotional rebates",
           "promotional rebates tax", "marketplace withheld tax", "selling fees", "fba fees",
           "other transaction fees", "other", "total"]
COLUMNS = IDENTITY + AMOUNTS
COUNTS = ("settlement id", "quantity")
# The negative amount cells of each October file.
NEGATIVE_AMOUNTS = {"2026-10": 6, "2026-10-2": 3}
# SHA256 of the NDJSON a downstream consumer downloads for the transactions of each October report.
OUTPUT_SHA256 = {"2026-10": "2adc3f19f70041f150364fa81f33d275c19667204e68efabd28389551e2ce464",
                 "2026-10-2": "127b33a8a8523ba3d44479ad4b66480529c29423b71b89acd2d796a6ef5aa143"}
# The sum of total over the transactions of each October report.
NET_TOTAL = {"2026-10": Decimal("59.57"), "2026-10-2": Decimal("-1.36")}
# The end of a data row whose last cell is a number the Dataset CSV reader gives as one.
LAST_NUMBER = re.compile(rb",-?(0|[1-9][0-9]*)(\.[0-9]+)?(\r?)$")


def expected(data, columns=None):
    """The transactions of one report file as a reader expects them, in file order.

    Each value is read from its column by name. An empty cell is None, a settlement id and a
    quantity are whole numbers, an amount is a Decimal and every other column is a string.
    Without `columns` every column of the report is kept, in the order of its header.
    """
    records = [record for record in csv.reader(io.StringIO(data.decode("utf-8-sig"), newline="")) if record]
    header_at = next((index for index, record in enumerate(records) if record[0] == IDENTITY[0]), None)
    if header_at is None:
        raise ValueError("the report has no header record")
    header, rows = records[header_at], records[header_at + 1:]
    if any(len(row) != len(header) for row in rows):
        raise ValueError("a record of the report does not match its header")
    columns = header if columns is None else columns
    missing = [name for name in columns if name not in header]
    if missing:
        raise ValueError(f"the report has no column {missing[0]!r}")
    return [{name: value(name, dict(zip(header, row))[name].strip()) for name in columns} for row in rows]


def value(column, text):
    """One cell of a column, or None when it is empty."""
    if not text:
        return None
    if column in AMOUNTS:
        return Decimal(text)
    if column in COUNTS:
        return int(text)
    return text


def read(data):
    """The transactions of a downloaded file: one JSON object per line, every amount a Decimal."""
    return [json.loads(line, parse_float=Decimal) for line in data.splitlines()]


def lines(rows):
    """Rows that a JSON reader already parsed, as the file value_diffs reads."""
    return "".join(json.dumps(row) + "\n" for row in rows).encode()


def table(columns, rows):
    """The rows of a preview as the reader holds them: each value under its column name.

    A preview is a list of values per row. It is read only while every column name comes once
    and every row is as wide as the columns: a narrower row would lose values and a repeated
    name would overwrite one. None says that it is not.
    """
    if len(set(columns)) != len(columns) or any(len(row) != len(columns) for row in rows):
        return None
    return [dict(zip(columns, row)) for row in rows]


def agrees(actual, expected):
    """Whether a downloaded value is the expected one of its kind.

    A number must come as a number: a bool is not one, though Python has True == 1, and neither is a string.
    """
    if isinstance(expected, (int, Decimal)):
        return type(actual) in (int, Decimal) and actual == expected
    return type(actual) is type(expected) and actual == expected


def value_diffs(data, expected):
    """Name each line and value where a downloaded file differs from the expected transactions."""
    text = data.splitlines()
    diffs = [] if len(text) == len(expected) else [f"{len(text)} lines, expected {len(expected)}"]
    for number, (line, row) in enumerate(zip(text, expected), 1):
        try:
            value = json.loads(line, parse_float=Decimal)
        except ValueError:
            diffs.append(f"line {number}: not JSON")
            continue
        if not isinstance(value, dict):
            diffs.append(f"line {number}: not an object")
            continue
        for column in sorted(set(value) | set(row)):
            if column not in value or column not in row:
                diffs.append(f"line {number} {column}: present on one side only")
            elif not agrees(value[column], row[column]):
                diffs.append(f"line {number} {column}: {value[column]!r}, expected {row[column]!r}")
    return diffs


def holds(rows, wanted, other):
    """Whether downloaded rows are the expected transactions in file order, and none of those of `other`."""
    ids = [row["settlement id"] for row in rows]
    return ids == [row["settlement id"] for row in wanted] and set(ids).isdisjoint(row["settlement id"] for row in other)


def negative_amounts(rows):
    """The negative amounts of each row, by column name."""
    return [{name: row[name] for name in AMOUNTS if row[name] is not None and row[name] < 0} for row in rows]


def with_total(data, cell):
    """The report with the `total` cell of its last data row replaced by `cell`.

    This builds a test input: a report that a Pipeline must refuse for its schema. Only that cell
    changes, and it changes on the bytes, so the BOM, the line endings and every other cell stay
    as they are. total is the last column of a report, so the last data row must end with a comma
    and a number.
    """
    body = data.rstrip(b"\r\n")
    last = LAST_NUMBER.search(body)
    if last is None:
        raise ValueError("the last data row of the report does not end with a number")
    return b"".join((body[:last.start()], b",", cell.encode(), last.group(3), data[len(body):]))


def text_total(data):
    """The report with the total of its last data row as text, N/A. The test input of the refusal a Pipeline owes it."""
    return with_total(data, "N/A")


def net(rows):
    """The sum of `total` over the rows that hold a number, as a Decimal.

    A row whose total is empty or is text, as in the report with a text total, adds nothing.
    """
    return sum((row["total"] for row in rows if type(row.get("total")) in (int, Decimal)), Decimal(0))


def nodes(graph, source, out, published, expected_schema=None):
    """The graph of one Pipeline: a pinned report input, a select of COLUMNS and an output to another dataset.

    With the schema a reader asked of the input version (`expected_schema`, the answer of the
    versioned schema route) the pin also requires that schema: it carries the answer's content
    hash and a copy of its fields.
    """
    nodes = graph.graph_nodes(source, published["version"], published["transaction"], out)
    if expected_schema is not None:
        nodes[0]["config"]["dataset_snapshot"].update(expected_schema_hash=expected_schema["content_hash"],
                                                      expected_schema=copy.deepcopy(expected_schema["fields"]))
    nodes.insert(1, {"id": "project-transactions", "label": "Publish transaction amounts", "depends_on": ["input"],
                     "transform_type": "select", "config": {"columns": COLUMNS}})
    nodes[-1]["depends_on"] = ["project-transactions"]
    return nodes


def refused_at_input(outcome, *codes):
    """Whether a run was refused at its input node, its own error naming every code.

    The run failed, its input node failed with the codes, the two nodes after it were
    cancelled, and no node committed an output resource.
    """
    nodes = outcome["nodes"]
    error = next((n["error"] or "" for n in nodes if n["node_id"] == "input"), "")
    return (outcome["status"] == "failed"
            and {n["node_id"]: n["status"] for n in nodes}
            == {"input": "failed", "project-transactions": "cancelled", "output": "cancelled"}
            and all(code in error for code in ("dataset_input_load_failed", "dataset_input_schema_rejected", *codes))
            and not [status for n in nodes for status in n["output_resources"] if status == "committed"])

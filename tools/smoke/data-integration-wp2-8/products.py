"""Local-product contract and deterministic, batch-keyed Dataset request for the fixture."""
import base64
import hashlib
import json
from decimal import Decimal, InvalidOperation

# The typed column contract a product snapshot is published under, in one
# fixed order of its own: the product id as an integer, the SKU, the name and
# the purchase cost as strings, so leading zeroes and decimal costs reach a
# downstream reader unchanged. project() admits no null for any of the four.
PRODUCT_SCHEMA = [{"name": "cg_price", "type": "STRING", "nullable": False},
                  {"name": "id", "type": "LONG", "nullable": False},
                  {"name": "product_name", "type": "STRING", "nullable": False},
                  {"name": "sku", "type": "STRING", "nullable": False}]
PRODUCT_COLUMNS = {field["name"] for field in PRODUCT_SCHEMA}


def published_schema(rows):
    """The formal schema field of a product snapshot commit.

    Each row must hold exactly the contract columns with the contract types,
    whatever the order its keys come in, so every snapshot of the source
    declares the same schema and hash.
    """
    for row in rows:
        if set(row) != PRODUCT_COLUMNS or type(row["id"]) is not int or not all(
                isinstance(row[column], str) for column in ("sku", "product_name", "cg_price")):
            raise ValueError("published products must have the contract columns: an integer id and three strings")
    return {"file_format": "TEXT", "fields": [dict(field) for field in PRODUCT_SCHEMA]}


def project(row):
    if not isinstance(row, dict) or type(row.get("id")) is not int or row["id"] < 1:
        raise ValueError("invalid product id")
    for field in ("sku", "product_name"):
        if not isinstance(row.get(field), str) or not row[field].strip():
            raise ValueError(f"invalid {field}")
    raw_cost = row.get("cg_price")
    if type(raw_cost) not in (str, int, Decimal):
        raise ValueError("invalid purchase cost")
    try:
        cost = Decimal(raw_cost)
    except InvalidOperation as exc:
        raise ValueError("invalid purchase cost") from exc
    if not cost.is_finite():
        raise ValueError("invalid purchase cost")
    return {"id": row["id"], "sku": row["sku"], "product_name": row["product_name"],
            "cg_price": str(cost)}


def collect(fetch_page, length):
    """Validate the complete response before making any snapshot visible."""
    if type(length) is not int or not 1 <= length <= 1000:
        raise ValueError("invalid page size")
    rows = []
    seen = set()
    total = None
    while True:
        page = fetch_page(len(rows), length)
        if not isinstance(page, dict) or type(page.get("code")) is not int or page["code"] != 0:
            raise ValueError("source returned a business error")
        count = page.get("total")
        if type(count) is not int or count < 0 or (total is not None and count != total):
            raise ValueError("invalid or changing total")
        total = count
        data = page.get("data")
        if not isinstance(data, list) or len(data) > length or len(rows) + len(data) > total:
            raise ValueError("invalid page length")
        for item in data:
            row = project(item)
            if row["id"] in seen:
                raise ValueError("duplicate product id")
            seen.add(row["id"])
            rows.append(row)
        if len(rows) == total:
            return rows
        if not data:
            raise ValueError("empty page before declared total")


def ndjson(rows):
    return b"".join(json.dumps(r, ensure_ascii=False, separators=(",", ":"),
                               sort_keys=True).encode() + b"\n" for r in rows)


def commit_body(source_dataset, batch, data, schema=None):
    """Same upstream identity and bytes reconstruct the same DVS request on every delivery.

    schema is opt-in. Without it the request is the one every batch was sent
    with before the input contract existed, byte for byte, so a batch
    delivered then still replays to its original receipt. The Dataset service
    fingerprints the exact request: the same batch key sent with the schema
    is another request, which that key refuses.
    """
    if not data:
        raise ValueError("empty snapshot publication awaits product confirmation")
    body = {
        "idempotency_key": f"lingxing-product:{source_dataset}:{batch}",
        "branch": "main", "transaction_type": "SNAPSHOT", "format": "json",
        "metadata": {"source_dataset": source_dataset, "source_batch": batch,
                     "content_hash": hashlib.sha256(data).hexdigest()},
        "files": [{"logical_path": "products.ndjson", "operation": "ADD",
                   "content_type": "application/x-ndjson", "size_bytes": len(data),
                   "content_base64": base64.b64encode(data).decode()}],
    }
    if schema is not None:
        body["schema"] = schema
    return body

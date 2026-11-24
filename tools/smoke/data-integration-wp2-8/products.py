"""Local-product contract and deterministic, batch-keyed Dataset request for the fixture."""
import base64
import hashlib
import json
from decimal import Decimal, InvalidOperation


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


def commit_body(source_dataset, batch, data):
    """Same upstream identity and bytes reconstruct the same DVS request on every delivery."""
    if not data:
        raise ValueError("empty snapshot publication awaits product confirmation")
    return {
        "idempotency_key": f"lingxing-product:{source_dataset}:{batch}",
        "branch": "main", "transaction_type": "SNAPSHOT", "format": "json",
        "metadata": {"source_dataset": source_dataset, "source_batch": batch,
                     "content_hash": hashlib.sha256(data).hexdigest()},
        "files": [{"logical_path": "products.ndjson", "operation": "ADD",
                   "content_type": "application/x-ndjson", "size_bytes": len(data),
                   "content_base64": base64.b64encode(data).decode()}],
    }

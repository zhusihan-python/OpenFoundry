import base64
import csv
import io
import json
from pathlib import Path
import re
import unittest

from reports import (HEADER_ROW, STORE, RegistrationError, coverage, envelope, file_commit, no_data_commit, object_key,
                     registration, unpack)

HERE = Path(__file__).resolve().parent
# What the sender registered for each original file. A fixture edit changes these values.
FILES = {"2026-09": ("f8aadfc4ef82939a8e544635e0004e216463a7de1c678ea330e522e6478dc508", 1282),
         "2026-10": ("f2a78b053ea7370c113a961aba5b7286df3bd894bae54de59c48a96bb94ebb09", 1115),
         "2026-10-2": ("9e4dd967246cbfde61c9a1b508c8e698236eec33ef39528396e7e281f2a48c3d", 850)}
FILE_ID = "0b6b1f2c-3d4e-4f50-9a6b-7c8d9e0f1a2b"
CHECKED_AT = "2026-11-24T08:00:00.000Z"
MONTH = re.compile(r"\d{4}-(0[1-9]|1[0-2])")


def fixture(name="2026-09"):
    return (HERE / "fixtures" / f"transaction_view-{name}.csv").read_bytes()


def delivered(data=None, period="2026-09", file_id=FILE_ID, status="uploaded", **changes):
    """A file envelope as delivered: its registration and the receipt the sender kept."""
    data = fixture() if data is None else data
    record = dict(registration(data, period if MONTH.fullmatch(period) else "2026-09"), **changes)
    return envelope("report", registration=record, receipt=dict(status=status, fileId=file_id),
                    content_base64=base64.b64encode(data).decode())


def no_data(status="reported", period="2026-08", report_changes=(), **receipt):
    """A no-data envelope: the report that was sent and the receipt the sender kept."""
    report = dict(storeId=STORE, marketplace="amazon-us", reportKind="transaction_view", period=period,
                  checkedAt=CHECKED_AT)
    report.update(report_changes)
    return envelope("no_data", report=report, receipt={} if status is None else dict(status=status, **receipt))


class FixtureTest(unittest.TestCase):
    def test_each_file_is_registered_by_its_own_bytes(self):
        for name, (digest, size) in FILES.items():
            with self.subTest(file=name):
                data = fixture(name)
                self.assertEqual((registration(data, "2026-10")["sha256"], len(data)), (digest, size))
        self.assertEqual(len({fixture(name) for name in FILES}), 3)

    def test_registration_contract(self):
        self.assertEqual(registration(fixture(), "2026-09"), {
            "storeId": STORE, "marketplace": "amazon-us", "reportKind": "transaction_view", "period": "2026-09",
            "coverageStart": "2026-09-01", "coverageEnd": "2026-09-30", "completeness": "full",
            "sha256": FILES["2026-09"][0], "byteSize": 1282, "contentType": "text/csv"})

    def test_month_ends(self):
        self.assertEqual([coverage(m)["coverageEnd"] for m in ("2026-02", "2024-02", "2026-04")],
                         ["2026-02-28", "2024-02-29", "2026-04-30"])

    def test_envelope_is_one_trimmed_ascii_line(self):
        body = delivered()
        self.assertEqual(body.count(b"\n"), 1)
        line = body[:-1]
        self.assertEqual(line, line.strip())
        self.assertTrue(line.isascii())
        self.assertEqual(sorted(json.loads(line)), ["content_base64", "kind", "receipt", "registration"])
        self.assertEqual(object_key("2026-09", "again"),
                         f"reports/{STORE}/amazon-us/transaction_view/2026-09/again.ndjson")

    def test_each_file_opens_with_the_preamble_and_keeps_its_header_on_the_second_record(self):
        for name in FILES:
            with self.subTest(file=name):
                data = fixture(name)
                records = list(csv.reader(io.StringIO(data.decode("utf-8-sig"))))
                self.assertTrue(data.startswith(b"\xef\xbb\xbf"))
                self.assertEqual((records[0], records[1][0]), (["All amounts in USD"], "date/time"))
                self.assertTrue(all(len(record) == len(records[1]) for record in records[1:]))
        self.assertEqual(HEADER_ROW, 2)


class FileTest(unittest.TestCase):
    def test_registered_file_round_trips_its_bytes(self):
        data = fixture()
        record, file_id, published = unpack(delivered(), "2026-09")
        self.assertEqual((record, file_id, published), (registration(data, "2026-09"), FILE_ID, data))
        self.assertTrue(published.startswith(b"\xef\xbb\xbf"))

    def test_receipt_must_say_uploaded(self):
        for status in ("failed", "skipped", None, ""):
            with self.subTest(status=status), self.assertRaisesRegex(RegistrationError, "not 'uploaded'"):
                unpack(delivered(status=status), "2026-09")

    def test_file_id_must_be_a_uuid(self):
        for file_id in ("", "   ", None, "not-a-uuid", 12):
            with self.subTest(file_id=file_id), self.assertRaisesRegex(RegistrationError, "no valid fileId"):
                unpack(delivered(file_id=file_id), "2026-09")

    def test_scope_must_match_the_routed_period(self):
        for changes in ({"marketplace": "amazon-ca"}, {"storeId": "00000000-0000-4000-8000-000000000000"},
                        {"reportKind": "fba_storage_fees"}, {"period": "2026-13"}, {"period": "2026-9"}):
            with self.subTest(changes=changes), self.assertRaises(RegistrationError):
                unpack(delivered(**changes), changes.get("period", "2026-09"))
        with self.assertRaisesRegex(RegistrationError, "carries period 2026-09"):
            unpack(delivered(), "2026-10")

    def test_coverage_must_be_the_whole_period(self):
        for changes in ({"coverageEnd": "2026-09-31"}, {"coverageStart": "2026-08-31"},
                        {"completeness": "partial_snapshot"}):
            with self.subTest(changes=changes), self.assertRaisesRegex(RegistrationError, "coverage"):
                unpack(delivered(**changes), "2026-09")
        with self.assertRaisesRegex(RegistrationError, "contentType"):
            unpack(delivered(contentType="application/pdf"), "2026-09")

    def test_length_and_hash_must_describe_the_file(self):
        truncated = delivered()  # the file lost its last line after it was registered
        record, _, data = unpack(truncated, "2026-09")
        stale = envelope("report", registration=record, receipt={"status": "uploaded", "fileId": FILE_ID},
                         content_base64=base64.b64encode(data[:-40]).decode())
        with self.assertRaisesRegex(RegistrationError, "length"):
            unpack(stale, "2026-09")
        changed = data.replace(b"Demo City", b"Demo Town")  # same length, different file
        self.assertEqual(len(changed), len(data))
        stale = envelope("report", registration=record, receipt={"status": "uploaded", "fileId": FILE_ID},
                         content_base64=base64.b64encode(changed).decode())
        with self.assertRaisesRegex(RegistrationError, "sha256"):
            unpack(stale, "2026-09")

    def test_commit_publishes_the_original_bytes_under_scope_and_file_id(self):
        data = fixture()
        record, file_id, published = unpack(delivered(), "2026-09")
        body = file_commit(record, file_id, published)
        file = body["files"][0]
        self.assertEqual((file["operation"], file["content_type"], file["size_bytes"]), ("ADD", "text/csv", 1282))
        self.assertEqual(base64.b64decode(file["content_base64"]), data)
        self.assertEqual(file["logical_path"], f"amazon-us/transaction_view/2026-09/{FILE_ID}.csv")
        self.assertEqual(body["metadata"], {"registration": record, "file_id": FILE_ID})
        self.assertEqual(body["idempotency_key"], f"amazon-report:{STORE}:amazon-us:transaction_view:2026-09:{FILE_ID}")
        self.assertEqual(body, file_commit(record, file_id, published))

    def test_commit_says_where_the_header_of_the_file_is(self):
        record, file_id, published = unpack(delivered(), "2026-09")
        self.assertEqual(file_commit(record, file_id, published)["schema"], {
            "file_format": "TEXT", "fields": [],
            "custom_metadata": {"csv": {"headerRow": HEADER_ROW, "charset": "UTF-8", "bom": "strip"}}})
        self.assertNotIn("schema", no_data_commit(unpack(no_data(), "2026-08")[0]))

    def test_key_separates_the_files_of_one_month(self):
        keys = set()
        for name in FILES:
            # One month, three files: every registration has its own key.
            record, file_id, data = unpack(delivered(fixture(name), "2026-10", file_id=f"{len(keys) + 1:08x}-3d4e-4f50-9a6b-7c8d9e0f1a2b"), "2026-10")
            keys.add(file_commit(record, file_id, data)["idempotency_key"])
        self.assertEqual(len(keys), 3)


class NoDataTest(unittest.TestCase):
    def test_reported_month_keeps_the_request_fields_and_an_optional_code(self):
        report, file_id, data = unpack(no_data(requirementCode="transaction_view"), "2026-08")
        self.assertEqual((file_id, data), (None, None))
        self.assertEqual(report, {"storeId": STORE, "marketplace": "amazon-us", "reportKind": "transaction_view",
                                  "period": "2026-08", "checkedAt": CHECKED_AT, "requirementCode": "transaction_view"})
        expected = dict(report, requirementCode="transaction_view")
        self.assertEqual(report, expected)
        del expected["requirementCode"]
        self.assertEqual(unpack(no_data(), "2026-08")[0], expected)

    def test_checked_at_alone_is_not_a_report(self):
        with self.assertRaisesRegex(RegistrationError, "not 'reported'"):
            unpack(no_data(status=None), "2026-08")

    def test_failed_or_skipped_report_is_not_a_confirmation(self):
        for status in ("failed", "skipped"):
            with self.subTest(status=status), self.assertRaisesRegex(RegistrationError, "not 'reported'"):
                unpack(no_data(status, note="core returned HTTP 500"), "2026-08")

    def test_reported_month_needs_checked_at_and_its_own_scope(self):
        for bad in (no_data(report_changes={"checkedAt": ""}), no_data(report_changes={"checkedAt": "2026-11-24"}),
                    no_data(report_changes={"marketplace": "amazon-ca"}), no_data(period="2026-13")):
            with self.subTest(body=bad), self.assertRaises(RegistrationError):
                unpack(bad, "2026-08")
        with self.assertRaisesRegex(RegistrationError, "carries period 2026-08"):
            unpack(no_data(), "2026-07")

    def test_commit_records_the_report_and_names_no_file(self):
        report = unpack(no_data(requirementCode="transaction_view"), "2026-08")[0]
        body = no_data_commit(report)
        file = body["files"][0]
        self.assertEqual((file["content_type"], file["logical_path"]),
                         ("application/json", "amazon-us/transaction_view/2026-08.no-data.json"))
        self.assertEqual((json.loads(base64.b64decode(file["content_base64"])), body["metadata"]),
                         (report, {"no_data": report}))
        self.assertEqual(body["idempotency_key"], f"amazon-report:{STORE}:amazon-us:transaction_view:2026-08")


if __name__ == "__main__":
    unittest.main()

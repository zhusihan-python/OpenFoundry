import unittest

import preview

FIELDS = [dict(name="settlement id", field_type="LONG", nullable=True),
          dict(name="total", field_type="DOUBLE", nullable=True),
          dict(name="type", field_type="STRING", nullable=True)]


def total_as(field_type):
    return [dict(field, field_type=field_type) if field["name"] == "total" else dict(field) for field in FIELDS]


class FieldChanges(unittest.TestCase):
    def test_the_negative_report_changes_the_total_alone(self):
        self.assertEqual(preview.schema_changes(FIELDS, total_as("STRING")), preview.EXPECTED_CHANGES["v3-v4"])

    def test_two_files_of_one_month_declare_the_same_fields(self):
        self.assertEqual(preview.schema_changes(FIELDS, FIELDS), preview.EXPECTED_CHANGES["v2-v3"])

    def test_every_other_difference_counts_and_is_not_the_pinned_change(self):
        expected = preview.EXPECTED_CHANGES["v3-v4"]
        # A field that changes its nullability.
        nullable = [dict(field, nullable=False) if field["name"] == "total" else dict(field) for field in FIELDS]
        self.assertEqual(preview.schema_changes(FIELDS, nullable)["modified"],
                         [dict(name="total", changed=["nullable"], before="DOUBLE", after="DOUBLE")])
        # A field added, removed, or moved.
        for other in (FIELDS + [dict(name="other", field_type="STRING", nullable=True)], FIELDS[:2], FIELDS[1:] + FIELDS[:1]):
            self.assertNotEqual(preview.schema_changes(FIELDS, other), expected)
        self.assertEqual(preview.schema_changes(FIELDS, FIELDS[1:] + FIELDS[:1])["order_kept"], False)
        self.assertEqual(preview.schema_changes(FIELDS, FIELDS[:2])["removed"], ["type"])

    def test_a_change_outside_the_type_is_not_read_as_the_pinned_change(self):
        self.assertNotEqual(preview.schema_changes(FIELDS, total_as("STRING")[:-1]), preview.EXPECTED_CHANGES["v3-v4"])


class PanelJudgment(unittest.TestCase):
    """The record the browser script writes is what the judgment reads, key by key."""

    def test_the_recorded_panel_is_what_the_judgment_reads(self):
        change = dict(items=1, rows=[dict(sign="~", name="total", hint="DOUBLE → STRING")], empty_notices=0, other=[])
        notice = dict(items=1, rows=[], empty_notices=1, other=[])
        self.assertTrue(preview.states_schema(change, preview.EXPECTED_SCHEMA["v3-v4"]))
        self.assertTrue(preview.states_schema(notice, preview.EXPECTED_SCHEMA["v2-v3"]))
        self.assertFalse(preview.states_schema(change, preview.EXPECTED_SCHEMA["v2-v3"]))
        self.assertFalse(preview.states_schema(notice, preview.EXPECTED_SCHEMA["v3-v4"]))
        # A change next to the empty notice, or next to another sentence, states nothing.
        for mixed in (dict(change, empty_notices=1, items=2), dict(change, other=["total is now a string"], items=2),
                      dict(notice, other=["total is now a string"], items=2)):
            for expected in ([], preview.EXPECTED_SCHEMA["v3-v4"]):
                self.assertFalse(preview.states_schema(mixed, expected))


class DownloadRecord(unittest.TestCase):
    """The record the browser script writes of the download, as the case reads it."""

    def record(self):
        return dict(saved_as="browser-download-output-v3.ndjson", version=3,
                    option=dict(value="transaction:t", label="v3 · main · t"),
                    selector=dict(value="transaction:t", label="v3 · main · t"),
                    files=[dict(path="part-00000.ndjson", size_text="1.640 B", size_shown_as="1.640 B", size_matches=True,
                                control="Download part-00000.ndjson", disabled=False)],
                    controls=["Download part-00000.ndjson"], listing_request=dict(path="/api/v1/datasets/d/files"),
                    request=dict(path="/d"), event=dict(url="blob:http://127.0.0.1:1/x"), captures=dict(), clicked_unix_ms=1)

    def test_the_record_is_read_as_the_browser_wrote_it(self):
        kept = preview.download_record(self.record())
        self.assertEqual((kept["saved_as"], kept["files"][0]["size_text"], kept["files"][0]["size_matches"]),
                         ("browser-download-output-v3.ndjson", "1.640 B", True))

    def test_a_field_the_browser_did_not_write_is_named_before_any_check_reads_it(self):
        for field in ("saved_as", "files", "event", "request", "listing_request"):
            record = self.record()
            del record[field]
            with self.assertRaisesRegex(KeyError, field):
                preview.download_record(record)

    def test_no_size_is_parsed_from_the_page_text(self):
        record = self.record()
        record["files"][0].update(size_text="1\u202f640\u00a0B", size_shown_as="1\u202f640\u00a0B")
        kept = preview.download_record(record)
        self.assertEqual(kept["files"][0]["size_text"], "1\u202f640\u00a0B")


class LoopbackListener(unittest.TestCase):
    """A /proc/net/tcp table of the namespace the forwarder shares with the gateway."""

    TABLE = """  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:D4A5 00000000:0000 0A 00000000:00000000 00:00000000 00000000     0        0 12345 1
   1: 0B00007F:87F9 00000000:00000000 0A 00000000:00000000 00:00000000 00000000     0        0 12346 1
   2: 0100007F:D4A5 0B00007F:87F9 06 00000000:00000000 00:00000000 00000000     0        0 12347 2
"""
    PORT = 0xD4A5

    def test_listening_sockets_are_read_by_address_and_port(self):
        self.assertEqual(preview.listening(self.TABLE), [("127.0.0.1", self.PORT), ("127.0.0.11", 0x87F9)])

    def test_a_listener_on_every_interface_is_told_from_the_loopback_alone(self):
        every = self.TABLE + f"   3: 00000000:{self.PORT:04X} 00000000:0000 0A 00000000:00000000 00:00000000 00000000 0 0 12348 3\n"
        on_port = [socket for socket in preview.listening(every) if socket[1] == self.PORT]
        self.assertEqual(on_port, [("127.0.0.1", self.PORT), ("0.0.0.0", self.PORT)])
        self.assertNotEqual(on_port, [("127.0.0.1", self.PORT)])

    def test_an_ipv6_listener_keeps_its_address_as_written(self):
        table = "  sl  local_address                         ... st ...\n   0: 00000000000000000000000001000000:1F90 0000... 0A 0 0 0 1\n"
        self.assertEqual(preview.listening(table), [("00000000000000000000000001000000", 8080)])


if __name__ == "__main__":
    unittest.main()

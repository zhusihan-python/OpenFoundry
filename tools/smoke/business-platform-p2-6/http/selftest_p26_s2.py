#!/usr/bin/env python3
"""S2 contract pure tests: fake release/runner only, no auth or live calls."""
import copy
import base64
import os
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
import p26_owned_ops as ops
import run_p26_s2_actual as wrapper
from selftest_p26_owned_ops import AUX, ROSTER, SOURCE, DEF, RUN, PG


class S2Tests(unittest.TestCase):
    def release(self):
        aux = copy.deepcopy(AUX)
        for inst in aux:
            aux[inst]["name"] = ops.AUX_NAMES_S2[inst]
        return {"w7_aux": aux, "w7_aux_cohort": 2,
                "w7_auth_identity": {"actor":RUN, "org":SOURCE},
                "expected_candidate_images": {"connector": aux["a"]["image"]},
                "w7_forbidden_aux_cids": ["1"*64, "2"*64, "3"*64, "4"*64]}

    def test_exact_cohort_image_and_old_ids(self):
        doc = self.release()
        self.assertIsNone(ops.validate_release_aux(doc)[1])
        for field, bad in (("name", "p26connectora"), ("name", "p26connectora3"),
                           ("cid", "1"*64), ("cid", next(iter(ops.OLD_AUX_CIDS))),
                           ("image", "sha256:"+"e"*64)):
            changed = copy.deepcopy(doc)
            changed["w7_aux"]["a"][field] = bad
            self.assertIsNotNone(ops.validate_release_aux(changed)[1], (field, bad))
        for key, bad in (("w7_aux_cohort", True), ("w7_aux_cohort", 3),
                         ("w7_forbidden_aux_cids", []),
                         ("w7_forbidden_aux_cids", [{}, {}, {}, {}]),
                         ("expected_candidate_images", {})):
            changed = {**doc, key: bad}
            self.assertIsNotNone(ops.validate_release_aux(changed)[1])

    def test_live_gate_checks_current_root_facts_before_subprocess(self):
        doc = self.release()
        doc.update(released_for=["http-owned-ops"], expires_utc="20990101T000000Z")
        with tempfile.TemporaryDirectory() as tmp:
            release = Path(tmp)/"release.json"
            release.write_text(json.dumps(doc))
            adapter = ops.OwnedAuxOps(doc["w7_aux"], release_file=str(release), roster=ROSTER,
                                      auth_identity=doc["w7_auth_identity"])
            doc["expected_candidate_images"]["connector"] = "sha256:"+"e"*64
            release.write_text(json.dumps(doc))
            with patch.object(ops.subprocess, "run", side_effect=AssertionError("must not execute")):
                with self.assertRaisesRegex(ops.OwnedOpsError, "differ from Root"):
                    adapter.stop_connector()

    def test_scoped_sql_and_foreign_row_refuses_further_docker(self):
        doc = self.release()
        calls = []
        def runner(method, argv, timeout):
            calls.append((method, argv))
            return 0, "", ""  # WHERE excludes foreign/owner-drift row
        adapter = ops.OwnedAuxOps(doc["w7_aux"], runner=runner, roster=ROSTER,
                                  auth_identity=doc["w7_auth_identity"], pg_observer=PG)
        sql = adapter._lease_argv(adapter._chain(DEF, RUN))[-1]
        for column, value in (("r.org_id", SOURCE), ("d.org_id", SOURCE),
                              ("s.org_id", SOURCE), ("r.owner_id", RUN),
                              ("r.requested_by", RUN), ("s.owner_id", RUN)):
            self.assertIn(f"{column}='{value}'::uuid", sql)
        for expected in (DEF, SOURCE, RUN, "r.recovery_protocol='p26-v1'",
                         "default_transaction_read_only=on"):
            self.assertIn(expected, " ".join(adapter._lease_argv(adapter._chain(DEF, RUN))))
        with self.assertRaisesRegex(ops.OwnedOpsError, "one safe owned"):
            adapter.wait_lease_expired(DEF, RUN, 1)
        before = len(calls)
        with self.assertRaisesRegex(ops.OwnedOpsError, "scope failed"):
            adapter.stop_connector()
        self.assertEqual(len(calls), before)

    def test_wrong_writer_claims_zero_subprocess(self):
        doc = self.release()
        doc.update(released_for=["http-owned-ops"], expires_utc="20990101T000000Z")
        payload = base64.urlsafe_b64encode(json.dumps({"sub":DEF, "org_id":SOURCE}).encode()).rstrip(b"=").decode()
        token = "Bearer fake." + payload + ".fake"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"release.json"
            path.write_text(json.dumps(doc))
            adapter = ops.OwnedAuxOps(doc["w7_aux"], release_file=str(path),
                roster=ROSTER, auth_identity=doc["w7_auth_identity"])
            with patch.dict(os.environ, {"OPENFOUNDRY_SMOKE_JWT":token, "P26_W7_JWT_WRITER":token}), patch.object(ops.subprocess, "run", side_effect=AssertionError("must not execute")):
                with self.assertRaisesRegex(ops.OwnedOpsError, "claims differ"):
                    adapter.stop_connector()

    def test_wrapper_exact_case_source_ports_and_expiry(self):
        doc = self.release()
        doc.update(frozen_source_sha256="f"*64,
                   w7_cases=["healthy_snapshot_and_legacy"],
                   released_for=["http-live-scenarios", "http-owned-ops"],
                   expires_utc="20990101T000000Z",
                   w7_bases={"fixture":"http://127.0.0.1:17095",
                             "proxy_admin":"http://127.0.0.1:17096",
                             "dvs":"http://127.0.0.1:17078",
                             "connector":"http://127.0.0.1:17101",
                             "connector_b":"http://127.0.0.1:17102"},
                   reviewed_s2_tool_shas=wrapper.hashes(),
                   reviewed_s2_wrapper_sha256=hashlib.sha256(Path(wrapper.__file__).read_bytes()).hexdigest())
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"release.json"
            def validate(d, case="healthy_snapshot_and_legacy", source="f"*64):
                path.write_text(json.dumps(d))
                return wrapper.validate(d, source, case, str(path))
            self.assertTrue(validate(doc))
            with self.assertRaises(ValueError):
                validate(doc, source="e"*64)
            with self.assertRaises(ValueError):
                validate(doc, case="healthy_incremental_epoch_once")
            for changes in ({"expires_utc":"20000101T000000Z"},
                            {"reviewed_s2_tool_shas":{}},
                            {"w7_bases":{**doc["w7_bases"], "connector":"http://127.0.0.1:17097"}}):
                with self.assertRaises(ValueError):
                    validate({**doc, **changes})


if __name__ == "__main__":
    unittest.main(verbosity=2)

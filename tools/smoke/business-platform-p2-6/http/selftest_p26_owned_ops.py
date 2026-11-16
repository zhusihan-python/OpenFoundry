#!/usr/bin/env python3
"""Owned-ops pure mechanics tests. No sockets, Docker, PG, or live HTTP."""
import base64
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parent))
from p26_http_lib import EvidenceDir
from p26_owned_ops import OwnedAuxOps, OwnedOpsError, validate_aux_facts, IDENTITY_FORMAT

AUX = {i: {"cid": c*64, "name": "p26connector"+i,
           "labels": {"owner": "p26-http", "stage": "p2-6", "role": "connector_"+i},
           "image": "sha256:"+"c"*64} for i,c in (("a","a"),("b","b"))}
SOURCE, DEF, RUN = ["11111111-1111-4111-8111-"+str(i).zfill(12) for i in range(1,4)]
ROSTER = [{"kind":"source","id":SOURCE},
          {"kind":"sync_def","id":DEF,"source":SOURCE},
          {"kind":"run","id":RUN,"def":DEF}]
PG = {"cid":"d"*64,"user":"openfoundry","database":"connectors"}


def identity(instance="a", running=True):
    facts = AUX[instance]
    return {"cid":facts["cid"],"name":"/"+facts["name"],"image":facts["image"],
            **facts["labels"],"running":running,"started_at":"2026-11-15T01:01:01Z","pid":123 if running else 0}


class OwnedOpsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ev = EvidenceDir(Path(self.tmp.name), "pure-only")
        self.calls = []
        self.running = True
        self.observe = {"db_clock":"2026-11-15T01:01:01+00:00", "protocol":"p26-v1",
                        "status":"running","generation":2,"lease_expired":True}

    def tearDown(self):
        self.tmp.cleanup()

    def runner(self, method, argv, timeout_s):
        # This runner performs no process/network operations.
        self.assertTrue(list(self.ev.path.glob("*.cmd")), "cmd exists BEFORE runner")
        self.calls.append((method, argv, timeout_s))
        if method == "inspect-identity":
            return 0, json.dumps(identity("b" if argv[-1] == "b"*64 else "a", self.running)), ""
        if method == "kill":
            self.running = False
        if method == "start":
            self.running = True
        if method == "lease-observe":
            return 0, json.dumps(self.observe), ""
        return 0, "ok", ""

    def ops(self, **kwargs):
        args = dict(runner=self.runner, roster=copy.deepcopy(ROSTER), evidence=self.ev, pg_observer=PG)
        args.update(kwargs)
        return OwnedAuxOps(AUX, **args)

    def test_pregate_readonly_exact_safe_proof(self):
        proof = {"db_clock": "2026-11-15T01:01:01+00:00", "status": "running",
                 "protocol": "p26-v1", "generation": 1,
                 "gate_not_entered": True, "no_intent": True,
                 "send_attempts": 0, "query_attempts": 0, "cp_owned_epoch_zero": True}
        calls = []
        def runner(method, argv, timeout):
            calls.append(argv)
            return 0, json.dumps(proof), ""
        ops = self.ops(runner=runner)
        self.assertEqual(ops.observe_pregate(DEF, RUN), proof)
        sql = calls[-1][-1]
        for expected in ("sync_def_checkpoints", "cp.active_run_id=r.id", "cp.confirmed_epoch=0",
                         "r.publish_entered_at IS NULL", "NOT EXISTS", "r.sync_def_id=", SOURCE):
            self.assertIn(expected, sql)
        for forbidden in ("lease_token", "lease_holder", "publish_body", "checkpoint_value", "UPDATE "):
            self.assertNotIn(forbidden, sql)
        proof["no_intent"] = False
        with self.assertRaisesRegex(OwnedOpsError, "pre-gate DB proof failed"):
            ops.observe_pregate(DEF, RUN)

    def test_readonly_claim_shape_rejects_owner_shortcut(self):
        def fake_token(claims):
            # Fake payload for pure shape checks, no signing/minting service.
            payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
            return "Bearer fake." + payload + ".fake-signature"
        writer = {"sub": RUN, "org_id": SOURCE}
        reader = {"sub": DEF, "org_id": SOURCE, "roles": [],
                  "permissions": ["connections:read", "dataset:read"]}
        with patch.dict(os.environ, {"OPENFOUNDRY_SMOKE_JWT": fake_token(writer)}):
            for bad in ({**reader, "sub": RUN}, {**reader, "org_id": RUN},
                        {**reader, "roles": ["admin"]},
                        {**reader, "permissions": ["connections:read", "connections:write"]}):
                with patch.dict(os.environ, {"P26_W7_JWT_READONLY": fake_token(bad)}):
                    with self.assertRaises(OwnedOpsError):
                        self.ops().credential("readonly")
            with patch.dict(os.environ, {"P26_W7_JWT_READONLY": fake_token(reader)}):
                self.assertEqual(self.ops().credential("readonly"), fake_token(reader))

    def test_real_docker_label_template_keys(self):
        # Release facts are normalized, actual Docker labels are namespaced.
        # This catches a valid-looking normalized mock hiding wrong CLI keys.
        for key in ("owner", "stage", "role"):
            self.assertIn('index .Config.Labels "of-p26-' + key + '"', IDENTITY_FORMAT)
            self.assertNotIn('index .Config.Labels "' + key + '"', IDENTITY_FORMAT)
        self.assertNotIn("Config.Env", IDENTITY_FORMAT)

    def test_exact_schema_distinct_and_image(self):
        for field, value in (("cid","b"*64),("name","q3connector"),("image","sha256:"+"d"*64),
                             ("labels",{"owner":"p26-http","stage":"p2-6","role":"connector_b"})):
            aux = copy.deepcopy(AUX)
            aux["a"][field] = value
            self.assertFalse(validate_aux_facts(aux)[0], field)
        self.assertFalse(validate_aux_facts({"a":{"cid":"a"*64,"label":"x=y"}})[0])

    def test_unreleased_is_zero_subprocess(self):
        ops = OwnedAuxOps(AUX, release_file=str(Path(self.tmp.name)/"absent"))
        with patch("p26_owned_ops.subprocess.run", side_effect=AssertionError("must not run")):
            with self.assertRaisesRegex(OwnedOpsError, "refusing.*http-owned-ops"):
                ops.stop_connector()

    def test_unbound_roster_is_zero_runner(self):
        for roster in (None, [], [{"kind":"run","id":RUN,"def":DEF}],
                       ROSTER+[ROSTER[-1]], [{**r,"id":"not-uuid"} for r in ROSTER]):
            with self.assertRaises(OwnedOpsError):
                self.ops(roster=roster).stop_connector()
        self.assertEqual(self.calls, [])

    def test_identity_mismatch_prevents_mutation(self):
        for key, value in (("cid","e"*64),("name","/connector-1"),("image","sha256:"+"d"*64),
                           ("owner","other"),("stage","p2-5"),("role","connector_b")):
            obj = identity(); obj[key] = value
            ops = self.ops(runner=lambda *args: (0,json.dumps(obj),""))
            with self.assertRaisesRegex(OwnedOpsError,"identity mismatch"):
                ops.stop_connector()
        self.assertEqual(self.calls, [])

    def test_crash_restart_reinspect_and_boot(self):
        ops = self.ops()
        self.assertTrue(ops.stop_connector("b"))
        self.assertTrue(ops.wait_stopped(instance="b"))
        self.assertTrue(ops.restart_connector("b"))
        self.assertEqual(ops.connector_boot_id("b"), "2026-11-15T01:01:01Z|123")
        methods = [x[0] for x in self.calls]
        for mutate in ("kill","start"):
            idx = methods.index(mutate)
            self.assertEqual(methods[idx-1],"inspect-identity")
            self.assertEqual(self.calls[idx][1][-1], AUX["b"]["cid"])
        for _,argv,_ in self.calls:
            self.assertNotIn("{{json .}}",argv)
            self.assertNotIn("Config.Env"," ".join(argv))

    def test_lease_exact_chain_and_readonly_argv(self):
        ops = self.ops()
        self.assertTrue(ops.wait_lease_expired(DEF,RUN,1))
        method, argv, bound = self.calls[-1]
        self.assertEqual(method,"lease-observe")
        self.assertNotIn("-T",argv)
        self.assertEqual(argv[4],PG["cid"])
        self.assertIn("default_transaction_read_only=on",argv[3])
        self.assertIn("statement_timeout=5000",argv[3])
        sql = argv[-1]
        for clause in ("r.sync_def_id=", "JOIN batch_sync_defs", "JOIN connections",
                       "r.recovery_source_id=s.id", "clock_timestamp()",SOURCE,DEF,RUN):
            self.assertIn(clause,sql)
        for forbidden in ("lease_token", "lease_holder", "publish_body", "UPDATE ","DELETE ","INSERT "):
            self.assertNotIn(forbidden,sql)
        self.assertEqual(ops.last_lease_observation,self.observe)

    def test_lease_wrong_parent_and_missing_pg_refuse(self):
        with self.assertRaises(OwnedOpsError):
            self.ops().wait_lease_expired(SOURCE,RUN,1)
        with self.assertRaises(OwnedOpsError):
            self.ops(pg_observer=None).wait_lease_expired(DEF,RUN,1)
        self.assertEqual(self.calls,[])

    def test_lease_strict_safe_projection(self):
        good = copy.deepcopy(self.observe)
        for bad in ({}, [], {**good,"protocol":None}, {**good,"status":"succeeded"},
                    {**good,"generation":True},{**good,"lease_expired":"true"},
                    {**good,"db_clock":"2026-11-15"},{**good,"lease_token":"secret"}):
            self.observe = bad
            with self.assertRaises(OwnedOpsError):
                self.ops().wait_lease_expired(DEF,RUN,1)

    def test_full_stdout_stderr_redaction_original_rc(self):
        secret="opaque-admin-secret-for-pure-test"
        with patch.dict(os.environ,{"P26_HTTP_ADMIN_TOKEN":secret}):
            ops = self.ops(runner=lambda *args:(-9,"x"*5000+secret,"y"*5000+secret))
            with self.assertRaises(OwnedOpsError):
                ops.stop_connector()
        meta = json.loads(next(self.ev.path.glob("*.meta")).read_text())
        self.assertEqual(meta["original_rc"],-9)
        self.assertEqual(next(self.ev.path.glob("*.rc")).read_text(),"-9\n")
        self.assertTrue("x"*5000 in next(self.ev.path.glob("*.stdout.raw")).read_text())
        self.assertTrue("y"*5000 in next(self.ev.path.glob("*.stderr.raw")).read_text())
        self.assertNotIn(secret,"".join(p.read_text() for p in self.ev.path.iterdir()))

    def test_timeout_does_not_forge_child_rc(self):
        def timeout(*args):
            raise subprocess.TimeoutExpired("pure",1,output=b"partial",stderr=b"failure")
        with self.assertRaisesRegex(OwnedOpsError,"original rc unavailable"):
            self.ops(runner=timeout).stop_connector()
        meta=json.loads(next(self.ev.path.glob("*.meta")).read_text())
        self.assertIsNone(meta["original_rc"])
        self.assertEqual(meta["failure"],"timeout")
        self.assertEqual(next(self.ev.path.glob("*.rc")).read_text(),"unavailable\n")
        self.assertEqual(next(self.ev.path.glob("*.stdout.raw")).read_text(),"partial")
        self.assertEqual(next(self.ev.path.glob("*.stderr.raw")).read_text(),"failure")

    def test_bind_context_uses_current_typed_roster(self):
        roster=[]
        ops=self.ops(roster=None).bind_context(roster,self.ev)
        with self.assertRaises(OwnedOpsError):
            ops.connector_boot_id()
        roster.extend(copy.deepcopy(ROSTER))
        self.assertTrue(ops.connector_boot_id())

    def test_runtime_release_change_prevents_subprocess(self):
        path=Path(self.tmp.name)/"release.json"
        path.write_text(json.dumps({"released_for":["http-owned-ops"],"expires_utc":"20990101T000000Z",
                                    "w7_aux":{**AUX,"a":AUX["b"]}}))
        ops=self.ops(runner=None,release_file=str(path))
        with patch("p26_owned_ops.subprocess.run",side_effect=AssertionError("must not run")):
            with self.assertRaisesRegex(OwnedOpsError,"differ from Root release"):
                ops.stop_connector()


if __name__ == "__main__":
    unittest.main(verbosity=2)

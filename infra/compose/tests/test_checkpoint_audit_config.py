"""Exercise the same Compose include, credential injection and bind mount used at startup."""

import json
import os
from pathlib import Path
import secrets
import subprocess
import unittest
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[3]
SECRET = "OPENFOUNDRY_CHECKPOINT_AUDIT_CDC_PASSWORD"


def services(password=None):
    env = os.environ.copy()
    env.pop(SECRET, None)
    env.pop("OPENFOUNDRY_POSTGRES_EXTRA_DATABASES", None)
    if password:
        env[SECRET] = password
    command = ["docker", "compose", "--env-file", os.devnull, "--profile", "foundation",
               "-f", str(ROOT / "compose.yaml"), "config", "--format", "json"]
    result = subprocess.run(command, check=True, env=env, capture_output=True, text=True)
    return json.loads(result.stdout)["services"]


class CheckpointAuditConfiguration(unittest.TestCase):
    def test_all_foundation_dockerfiles_exist(self):
        builds = {name: svc["build"] for name, svc in services().items() if svc.get("build")}
        self.assertIn("authorization-policy-service", builds)
        for name, build in builds.items():
            with self.subTest(service=name):
                self.assertTrue((Path(build["context"]) / build["dockerfile"]).is_file())

    def test_fresh_default_skips_audit_credentials(self):
        stack = services()
        self.assertFalse(stack["postgres"]["environment"]["CHECKPOINT_AUDIT_CDC_PASSWORD"])
        self.assertFalse(stack["checkpoint-audit-connect-init"]["environment"]["CHECKPOINT_AUDIT_CDC_ENABLED"])

    def test_registrar_tracks_policy_source_and_injects_credentials(self):
        secret = secrets.token_hex(24)
        stack = services(secret)
        postgres = stack["postgres"]
        policy = stack["authorization-policy-service"]
        worker = stack["debezium-connect"]["environment"]
        registrar = stack["checkpoint-audit-connect-init"]
        mount = registrar["volumes"][0]
        connector = json.loads(Path(mount["source"]).read_text())
        dbname = urlparse(policy["environment"]["DATABASE_URL"]).path.lstrip("/")

        self.assertIn(dbname, postgres["environment"]["POSTGRES_MULTIPLE_DATABASES"].split(","))
        self.assertEqual(connector["database.dbname"], dbname)
        self.assertEqual(connector["database.password"], "${env:CHECKPOINT_AUDIT_CDC_PASSWORD}")
        self.assertTrue(postgres["environment"]["CHECKPOINT_AUDIT_CDC_PASSWORD"] == secret)
        self.assertTrue(worker["CHECKPOINT_AUDIT_CDC_PASSWORD"] == secret)
        self.assertEqual(registrar["environment"]["CHECKPOINT_AUDIT_CDC_ENABLED"], "1")
        self.assertTrue(mount["read_only"])
        self.assertIn("@" + mount["target"], registrar["command"][0])
        self.assertIn("/connectors/checkpoint-audit-outbox/config", registrar["command"][0])
        self.assertIn("wal_level=logical", postgres["command"])
        self.assertEqual(connector["transforms.outbox.route.topic.replacement"], "${routedByValue}")
        projected = connector["transforms.outbox.table.fields.additional.placement"].split(",")
        self.assertCountEqual(projected, ["headers:header", "created_at:header"])
        legacy_registration = stack["debezium-connect-init"]["entrypoint"][-1]
        self.assertIn("/connectors/pg-policy-outbox/config", legacy_registration)
        self.assertEqual(legacy_registration.count("${routedByValue}"), 3)


if __name__ == "__main__":
    unittest.main()

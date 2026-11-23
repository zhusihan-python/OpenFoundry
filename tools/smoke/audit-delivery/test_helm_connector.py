"""Render the checkpoint connector with Helm and compare its wire contract to Compose."""
import json
import subprocess
import unittest
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[3]
CHART = ROOT / "infra/helm/infra/debezium"
ENABLED = {"checkpointAudit.enabled": "true", "checkpointAudit.hostname": "policy-primary",
           "checkpointAudit.dbname": "policy", "checkpointAudit.passwordSecretName": "cdc-login"}


def render(values, pipeline=False):
    args = ["helm", "template", "debezium", str(CHART)]
    if pipeline:
        args.extend(["-f", str(CHART / "values-schedule-outbox.yaml")])
    for key, value in values.items():
        args.extend(["--set", f"{key}={value}"])
    return subprocess.run(args, capture_output=True, text=True)


class HelmConnectorTest(unittest.TestCase):
    def test_profiles(self):
        compose = json.loads((ROOT / "infra/compose/local/debezium/checkpoint-audit-outbox.json").read_text())
        connector_class = compose.pop("connector.class")
        tasks = int(compose.pop("tasks.max"))
        compose.update({"database.hostname": "policy-primary", "database.dbname": "policy"})
        for enabled in (False, True):
            for pipeline in (False, True):
                with self.subTest(checkpoint=enabled, pipeline=pipeline):
                    result = render(ENABLED if enabled else {}, pipeline)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    resources = {d["metadata"]["name"]: d for d in yaml.safe_load_all(result.stdout) if d}
                    worker = resources["debezium"]["spec"]
                    providers = worker["config"].get("config.providers", "").split(",")
                    self.assertEqual("secrets" in providers, pipeline)
                    self.assertEqual("env" in providers, enabled)
                    container = worker["template"].get("connectContainer", {})
                    self.assertEqual(bool(container.get("volumeMounts")), pipeline)
                    self.assertEqual("checkpoint-audit-outbox" in resources, enabled)
                    if enabled:
                        doc = resources["checkpoint-audit-outbox"]
                        self.assertEqual(doc["metadata"]["labels"]["strimzi.io/cluster"], "debezium")
                        self.assertNotIn("strimzi.io/pause-reconciliation", doc["metadata"].get("annotations", {}))
                        self.assertEqual(doc["spec"], {"class": connector_class, "tasksMax": tasks, "config": compose})
                        env = container["env"]
                        self.assertEqual(env, [{"name": "CHECKPOINT_AUDIT_CDC_PASSWORD",
                                                "valueFrom": {"secretKeyRef": {"name": "cdc-login", "key": "password"}}}])
                        self.assertEqual(worker["config"]["config.providers.env.param.allowlist.pattern"], env[0]["name"])
                    else:
                        self.assertNotIn("env", container)

    def test_missing_source_or_secret(self):
        for key in ("checkpointAudit.hostname", "checkpointAudit.dbname", "checkpointAudit.passwordSecretName"):
            with self.subTest(missing=key):
                result = render({k: v for k, v in ENABLED.items() if k != key})
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(key + " is required", result.stderr)


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Run authenticated checkpoint decisions through CDC, Kafka and a durable Iceberg table."""
import argparse
import base64
import hashlib
import hmac
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
import uuid

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
CONNECTOR = "checkpoint-audit-outbox"
TOPIC = "audit.events.v1"
GROUP = "audit-sink"
KIND = "purpose_checkpoint.evaluated"
BUILD_IMAGE = "golang:1.25.14"


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def run(*args, env=None, data=None, timeout=90):
    # Child stderr can contain connector config, DSNs or the audit payload.
    result = subprocess.run(args, input=data, text=True, capture_output=True,
                            cwd=REPO, env=env, timeout=timeout)
    check(result.returncode == 0, f"{Path(str(args[0])).name} exited {result.returncode}")
    return result.stdout.strip()


def until(label, probe, seconds=100):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        try:
            if value := probe():
                return value
        except (RuntimeError, OSError, URLError):
            pass
        time.sleep(1)
    raise RuntimeError(f"timed out waiting for {label}")


def http(url, method="GET", body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = Request(url, method=method, headers=headers,
                      data=json.dumps(body).encode() if body is not None else None)
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def token(secret, caller):
    def encode(value):
        return base64.urlsafe_b64encode(value).rstrip(b"=")
    now = int(time.time())
    claims = {"sub": caller, "iat": now, "exp": now + 1800, "jti": str(uuid.uuid4()),
              "roles": [], "permissions": ["purpose-checkpoint:enforce"], "auth_methods": ["service"]}
    unsigned = b".".join(encode(json.dumps(part).encode()) for part in ({"alg": "HS256", "typ": "JWT"}, claims))
    return (unsigned + b"." + encode(hmac.digest(secret.encode(), unsigned, "sha256"))).decode()


class Fixture:
    def __init__(self, root, python):
        self.root = root
        self.python = python
        self.project = "ofaudit-" + secrets.token_hex(6)
        self.table = root / "iceberg"
        self.table_process = None
        self.caller = str(uuid.uuid4())
        self.actor = str(uuid.uuid4())
        self.stage = "build services"
        self.overlay = root / "foundation.json"
        self.env = dict(os.environ, FIXTURE_BIN=str(root / "bin"),
                        OPENFOUNDRY_DEBEZIUM_IMAGE="quay.io/debezium/connect:2.7.2.Final",
                        OPENFOUNDRY_POSTGRES_IMAGE="postgres:16-alpine",
                        OPENFOUNDRY_CHECKPOINT_AUDIT_CDC_PASSWORD=secrets.token_hex(24),
                        FIXTURE_JWT_SECRET=secrets.token_hex(32), FIXTURE_PG_PASSWORD=secrets.token_hex(24),
                        FIXTURE_TABLE_URL="http://host.docker.internal:1")
        self.compose_base = ["docker", "compose", "--env-file", os.devnull, "-p", self.project,
                             "-f", HERE / "compose.yaml", "-f", self.overlay]

    def compose(self, *args, **kwargs):
        return run(*self.compose_base, *args, env=self.env, **kwargs)

    def sql(self, query):
        return self.compose("exec", "-T", "postgres", "psql", "-XAt", "-v", "ON_ERROR_STOP=1",
                            "-U", self.pg_user, "-d", self.database, data=query)

    def kafka(self, command, *args):
        return self.compose("exec", "-T", "kafka", f"/opt/kafka/bin/kafka-{command}.sh",
                            "--bootstrap-server", "kafka:9092", *args)

    def address(self, service, port):
        return "http://" + self.compose("port", service, str(port))

    def build(self):
        bindir = self.root / "bin"
        bindir.mkdir()
        mounts = ["-v", f"{REPO}:/work:ro", "-w", "/work", "-v", f"{bindir}:/out"]
        for variable, destination in (("AUDIT_GO_MOD_CACHE", "/go/pkg/mod"), ("AUDIT_GO_BUILD_CACHE", "/root/.cache/go-build")):
            if path := os.environ.get(variable):
                mounts.extend(["-v", f"{Path(path).resolve()}:{destination}"])
        run("docker", "run", "--rm", *mounts, "-e", "CGO_ENABLED=0", BUILD_IMAGE, "go", "build",
            "-trimpath", "-buildvcs=false", "-o", "/out/", "./services/audit-sink/cmd/audit-sink",
            "./services/authorization-policy-service/cmd/authorization-policy-service", timeout=600)

    def render(self):
        self.stage = "render foundation deployment"
        services = json.loads(run("docker", "compose", "--env-file", os.devnull, "-f", REPO / "infra/compose/docker-compose.yml",
                                  "--profile", "foundation", "config", "--format", "json", "--no-env-resolution",
                                  env=self.env))["services"]
        pg = services["postgres"].copy()
        worker = services["debezium-connect"].copy()
        init = services["checkpoint-audit-connect-init"]
        policy_env = services["authorization-policy-service"]["environment"]
        dsn = urlsplit(policy_env["DATABASE_URL"])
        self.pg_user = pg["environment"]["POSTGRES_USER"]
        self.database = dsn.path.lstrip("/")
        self.policy_port = policy_env["PORT"]
        self.env["FIXTURE_POLICY_PORT"] = str(self.policy_port)
        check(dsn.username == self.pg_user and dsn.hostname == "postgres", "unexpected policy database role or host")
        self.env["FIXTURE_DATABASE_URL"] = dsn._replace(
            netloc=f"{self.pg_user}:{self.env['FIXTURE_PG_PASSWORD']}@{dsn.hostname}:{dsn.port}").geturl()

        # Inherit the shipped bootstrap directory, WAL settings, worker env, init container
        # entrypoint and read-only connector mount. Only credentials and published ports change.
        pg["environment"]["POSTGRES_PASSWORD"] = self.env["FIXTURE_PG_PASSWORD"]
        pg.pop("ports", None)
        worker["ports"] = ["127.0.0.1::8083"]
        sources = [vol["source"] for vol in init["volumes"] if vol.get("type") == "bind" and vol.get("read_only")]
        check(len(sources) == 1, "missing checkpoint connector registration mount")
        self.config = json.loads(Path(sources[0]).read_text())
        check((self.config["database.hostname"], int(self.config["database.port"]), self.config["database.dbname"])
              == (dsn.hostname, dsn.port, self.database), "policy and checkpoint connector databases differ")
        volumes = {vol["source"]: {} for vol in pg["volumes"] if vol["type"] == "volume"}
        self.overlay.write_text(json.dumps({"services": {"postgres": pg, "debezium-connect": worker,
                                                         "checkpoint-audit-connect-init": init}, "volumes": volumes}))

    def start(self):
        self.stage = "start isolated services"
        self.table_process = subprocess.Popen([self.python, HERE / "iceberg_table.py", "serve", "--root", self.table],
                                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        until("Iceberg table server", lambda: (self.table / "port").exists())
        self.env["FIXTURE_TABLE_URL"] = "http://host.docker.internal:" + (self.table / "port").read_text()
        self.compose("up", "-d", "postgres", "kafka", "debezium-connect")
        until("PostgreSQL", lambda: self.sql("SELECT 1;") == "1")
        self.sql("SELECT pg_create_logical_replication_slot('audit_smoke_witness', 'test_decoding');")
        self.compose("up", "-d", "policy")
        self.policy = self.address("policy", self.policy_port)
        until("policy migrations and health", lambda: http(self.policy + "/healthz"))
        self.connect = self.address("debezium-connect", 8083)
        until("Kafka Connect", lambda: http(self.connect + "/"))
        self.kafka("topics", "--create", "--if-not-exists", "--topic", TOPIC, "--partitions", "1", "--replication-factor", "1")
        self.compose("up", "-d", "sink")

    def start_connector(self):
        self.stage = "register deployed checkpoint connector"
        self.compose("run", "--rm", "--no-deps", "checkpoint-audit-connect-init")
        endpoint = self.connect + "/connectors/" + CONNECTOR
        def streaming():
            status = http(endpoint + "/status")
            return (status["connector"]["state"] == "RUNNING" and len(status["tasks"]) == 1
                    and status["tasks"][0]["state"] == "RUNNING"
                    and self.sql(f"SELECT active FROM pg_replication_slots WHERE slot_name='{self.config['slot.name']}';") == "t")
        until("Debezium streaming checkpoint WAL", streaming)

    def submit(self, approved):
        status = "approved" if approved else "denied"
        scope = "local" if approved else "public"
        purpose = "Synthetic checkpoint purpose " + secrets.token_hex(16)
        body = {"interaction_type": "ai_chat_completion", "actor_id": self.actor, "purpose_justification": purpose,
                "requested_private_network": False, "requires_approval": False, "tags": [],
                "evidence": {"operation": "chat/completions", "network_scope": scope}}
        receipt = http(self.policy + "/api/v1/checkpoints/purpose/enforce", "POST", body,
                       token(self.env["FIXTURE_JWT_SECRET"], self.caller))
        check(receipt["approved"] is approved and receipt["status"] == status, "unexpected checkpoint decision")
        rid = str(uuid.UUID(receipt["record_id"]))
        ledger = self.sql(f"SELECT status,actor_id,enforced_by FROM purpose_records WHERE id='{rid}';")
        check(ledger == "|".join((status, self.actor, self.caller)), "policy ledger mismatch")

        return status, rid, purpose, scope

    def observe(self, decision, offset):
        status, rid, purpose, scope = decision
        native = self.kafka("console-consumer", "--topic", TOPIC, "--partition", "0", "--offset", str(offset),
                            "--max-messages", "1", "--timeout-ms", "45000", "--property", "print.partition=true",
                            "--property", "print.offset=true")
        prefix = f"Partition:0\tOffset:{offset}\t"
        check(native.startswith(prefix), "Kafka partition or offset mismatch")
        check(purpose not in native, "Kafka contained raw justification")
        event = json.loads(native[len(prefix):])
        eid = str(uuid.UUID(event["event_id"]))
        payload = event["payload"]
        check(event["kind"] == KIND and event["resource_rid"] == rid and event["actor_id"] == self.actor
              and event["source_service"] == "authorization-policy-service", "audit envelope mismatch")
        check((payload["record_id"], payload["enforced_by"], payload["actor_id"], payload["status"], payload["kind"])
              == (rid, self.caller, self.actor, status, KIND), "Kafka decision mismatch")
        check(payload["evidence"]["justification_sha256"] == hashlib.sha256(purpose.encode()).hexdigest()
              and payload["evidence"]["network_scope"] == scope, "audit evidence mismatch")

        # The product removes each outbox row in its source transaction. Inspect committed WAL,
        # including the ledger INSERT in the same transaction, using a separate read-only witness.
        wal = self.sql(f"""
            WITH changes AS (SELECT * FROM pg_logical_slot_peek_changes('audit_smoke_witness',NULL,NULL))
            SELECT count(*) FILTER (WHERE data LIKE 'table public.purpose_records: INSERT:%'),
                   count(*) FILTER (WHERE data LIKE 'table outbox.events: INSERT:%' AND position('{eid}' in data)>0),
                   count(*) FILTER (WHERE data LIKE 'table outbox.events: DELETE:%' AND position('{eid}' in data)>0),
                   count(DISTINCT xid::text)
            FROM changes WHERE position('{rid}' in data)>0
              AND (data LIKE 'table public.purpose_records: INSERT:%' OR data LIKE 'table outbox.events:%');""")
        check(wal == "1|1|1|1" and self.sql("SELECT count(*) FROM outbox.events;") == "0",
              "outbox and receipt did not share a committed transaction")
        return event, {"record_id": rid, "event_id": eid, "status": status, "partition": 0,
                       "offset": offset, "same_transaction_receipt_outbox_insert_delete": True}

    def offset(self):
        group = self.kafka("consumer-groups", "--group", GROUP, "--describe")
        for line in group.splitlines():
            columns = line.split()
            if columns[:3] == [GROUP, TOPIC, "0"] and columns[3].isdigit():
                return int(columns[3])
        return None

    def read_table(self, events):
        data = json.loads(run(self.python, HERE / "iceberg_table.py", "read", "--root", self.table))
        expected = [{"event_id": e["event_id"], "at": e["at"], "kind": e["kind"], "payload": e["payload"],
                     "correlation_id": e.get("correlation_id")} for e in events]
        def by_event(row):
            return row["event_id"]
        check(sorted(data["rows"], key=by_event) == sorted(expected, key=by_event), "independent Iceberg readback mismatch")
        check(data["snapshot_count"] == len(events) and len(data["data_files"]) == len(events),
              "unexpected snapshot or Parquet file count")
        return {"snapshot_id": data["snapshot_id"], "snapshots": data["snapshot_count"],
                "parquet_files": len(data["data_files"]), "row_count": len(data["rows"])}

    def verify(self):
        self.start_connector()
        self.stage = "verify allowed decision and first delivery"
        first, first_report = self.observe(self.submit(True), 0)
        until("allow commit at offset 1", lambda: self.offset() == 1)
        before = self.read_table([first])
        self.stage = "deny during failed append"
        (self.table / "reject-append").touch()
        second, second_report = self.observe(self.submit(False), 1)
        rejected = self.table / "append-rejected"
        until("HTTP append rejection", rejected.exists)
        rejected_ids = json.loads(rejected.read_text())
        check(rejected_ids == [second["event_id"]], "failed append event ID mismatch")
        sink = self.compose("ps", "-aq", "sink")
        def exit_status():
            status, code = run("docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", sink).split()
            return (int(code),) if status == "exited" else None
        code, = until("audit-sink exit", exit_status)
        check(code != 0, "sink returned success on failed append")
        check(self.offset() == 1, "Kafka group advanced during the failed append")
        failed = self.read_table([first])
        check(failed == before, "table advanced during the failed append")
        (self.table / "reject-append").unlink()
        self.stage = "verify recovered writer and replay"
        self.compose("start", "sink")
        until("replayed denial committed at offset 2", lambda: self.offset() == 2)
        after = self.read_table([first, second])
        check(after["snapshot_id"] != before["snapshot_id"], "missing replay snapshot")
        check(self.kafka("get-offsets", "--topic", TOPIC, "--time", "latest") == TOPIC + ":0:2",
              "unexpected additional Kafka records")
        binary_hashes = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                         for path in sorted((self.root / "bin").iterdir())}
        images = {"go_builder": BUILD_IMAGE + "@" + run("docker", "image", "inspect", "-f", "{{.Id}}", BUILD_IMAGE)}
        for service in ("kafka", "postgres", "debezium-connect", "policy", "sink"):
            images[service] = run("docker", "inspect", "-f", "{{.Config.Image}}@{{.Image}}", self.compose("ps", "-q", service))
        return {"events": [first_report, second_report], "topic": TOPIC, "consumer_group": GROUP,
                "service_binary_sha256": binary_hashes,
                "committed_next_offsets": {"before_failure": 1, "after_rejected_append": 1, "after_restart": 2},
                "connector_streaming_before_first_checkpoint": True,
                "injected_append_status": 503, "rejected_event_ids": rejected_ids, "failed_sink_exit_code": code,
                "table": {"name": "of_audit.events", "catalog": "local SQLite PyIceberg", "before": before,
                          "after_rejected_append": failed, "after_replay": after},
                "postgres_version": self.sql("SHOW server_version;"), "kafka_connect": http(self.connect + "/")["version"],
                "python_packages": json.loads(run(self.python, "-c", "import json,pyiceberg,pyarrow; "
                                                      "print(json.dumps({'pyiceberg':pyiceberg.__version__, "
                                                      "'pyarrow':pyarrow.__version__}))")), "images": images}

    def close(self):
        if self.table_process:
            self.table_process.terminate()
            self.table_process.wait(timeout=15)
        if self.overlay.exists():
            self.compose("down", "-v", "--remove-orphans")
            label = "label=com.docker.compose.project=" + self.project
            for command in (["ps", "-aq"], ["network", "ls", "-q"], ["volume", "ls", "-q"]):
                check(not run("docker", *command, "--filter", label), "fixture resources remain")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new JSON evidence file outside the checkout")
    parser.add_argument("--table-python", default=Path(sys.executable), type=Path, help="Python executable with requirements.txt installed")
    args = parser.parse_args()
    output = args.output.resolve()
    check(not output.is_relative_to(REPO), "store evidence outside the checkout")
    with output.open("x") as evidence, tempfile.TemporaryDirectory(prefix="ofaudit-") as temp:
        report = {"revision": run("git", "rev-parse", "HEAD"), "dirty": bool(run("git", "status", "--porcelain")),
                  "result": "failed"}
        fixture = Fixture(Path(temp).resolve(), args.table_python.absolute())
        try:
            fixture.build()
            fixture.render()
            fixture.start()
            report.update(fixture.verify())
        except BaseException:
            report["failed_stage"] = fixture.stage
            raise
        finally:
            try:
                fixture.close()
                report["cleaned_up"] = True
                if "failed_stage" not in report:
                    report["result"] = "passed"
            except BaseException:
                report["cleaned_up"] = False
                report["failed_stage"] = "cleanup"
                raise
            finally:
                json.dump(report, evidence, indent=2)
                evidence.write("\n")
    print(f"Audit delivery passed. Evidence saved to {output}")


if __name__ == "__main__":
    main()

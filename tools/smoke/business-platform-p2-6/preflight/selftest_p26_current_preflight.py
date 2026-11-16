#!/usr/bin/env python3
"""Pure selftest for p26_current_preflight (W8) — ZERO sockets, ZERO
subprocesses, ZERO docker/SQL. All live boundaries are exercised through
injectable fakes; every fixture is SYNTHETIC and labeled as such (a fake
pass here says NOTHING about the real q3 substrate — it only proves the
collector's mechanics, comparators, gates and redaction)."""

import hashlib
import json
import sys
import tempfile
import unittest
import uuid as uuidlib
from argparse import Namespace
from pathlib import Path
from unittest import mock

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))          # import the tool under test
sys.path.insert(1, str(HERE.parent))   # import common/cohort helpers

import p26_current_preflight as P  # noqa: E402
from common import p26_gates as G  # noqa: E402
from cohort import p26_cohort_cli as C  # noqa: E402


def uid():
    return str(uuidlib.uuid4())


def hex64(ch="a"):
    return ch * 64


def uuidish(prefix, i):
    return f"{prefix}{i:02d}-0000-4000-8000-00000000000{i % 10}"


# ---------------------------------------------------------------- fixtures
def make_row(i, org):
    """Synthetic controlled row carrying all 27 safe columns."""
    r = {k: "" for k in C.SCHEMA_27}
    r.update({
        "id": uuidish("000000", i), "sync_def_id": uuidish("100000", i),
        "source_id": uuidish("200000", i), "output_dataset_id":
        uuidish("300000", i), "source_name": f"src{i}", "fixture_src":
        f"p21b-0000000{i}", "owner_id": uuidish("400000", i),
        "run_owner_id": uuidish("400000", i), "requested_by":
        uuidish("500000", i), "conn_org_id": org, "def_org_id": org,
        "run_org_id": org, "org_id": org, "status": "running",
        "control_mode": "controlled", "has_holder": "true", "has_token":
        "true", "started_at": "2026-11-14T17:00:00.000000+00:00",
        "heartbeat_at": "", "lease_expires_at":
        "2026-11-14T18:00:00.000000+00:00", "cancel_requested": "false",
        "gate_entered": "false", "attempt": "1", "generation": "1",
        "has_hash": "true", "has_version": "false", "finished_at": "",
    })
    return r


def make_owner(i, org, def_id=None, run_id=None, run_status="failed"):
    """Synthetic CP owner row. Owner defs/runs are DISJOINT from the
    controlled-9 ids EXCEPT the H10 entry — the REAL sealed shape (verified
    against the actual seal): the h10-held roster owner IS the controlled
    p25h13 run, so the protected9/cp8 overlap is exactly H10."""
    return {"sync_def_id": def_id or uuidish("700000", i), "source_id":
            uuidish("800000", i), "output_dataset_id": uuidish("900000", i),
            "mode": "cursor", "confirmed_epoch": str(i), "run_id":
            run_id or uuidish("600000", i), "active_since":
            "2026-11-14T16:52:00.900477+00:00", "run_status": run_status,
            "blocked_reason": "requires_reconciliation"}


def synth_baseline():
    """Synthetic 9/8 baseline (SHAPES mirror the sealed originals,
    including the H10 owner overlap)."""
    org = "a792b91e-0000-4000-8000-00000000000f"
    rows = [make_row(i, org) for i in range(9)]
    protected9 = [{"run_id": r["id"], "def_id": r["sync_def_id"],
                   "source_id": r["source_id"], "output_dataset_id":
                   r["output_dataset_id"], "origin": "original6",
                   "fixture_src": r["fixture_src"],
                   "public_projection_27": r} for r in rows]
    # owner 0 IS H10: same def, same run (held running)
    owners = [make_owner(0, org, def_id=rows[0]["sync_def_id"],
                         run_id=rows[0]["id"], run_status="running")]
    owners += [make_owner(i, org) for i in range(1, 8)]
    cp8 = []
    for i, o in enumerate(owners):
        cp8.append({"def_id": o["sync_def_id"], "source_id":
                    o["source_id"], "run_id": o["run_id"], "origin":
                    "h10-held" if i == 0 else "intentional-tail",
                    "tag": "h10-held" if i == 0 else f"t{i}",
                    "cp_owner_row_full": o,
                    "quiesce_checkpoint_full": {
                        "mode": "cursor", "initialized": i != 0,
                        "confirmed_epoch": 0,
                        "owner": {"run_id": o["run_id"], "run_status":
                                  o["run_status"], "since":
                                  "2026-11-14T16:52:00.900477Z",
                                  "blocked_reason":
                                  "requires_reconciliation"}},
                    "matched_public_run": {"id": o["run_id"], "sync_def_id":
                                           o["sync_def_id"], "status":
                                           o["run_status"]}})
    return org, rows, protected9, owners, cp8


def synth_catalog():
    h14_body = b"hello-old-bytes"
    catalog = {
        "grants_argv": ["docker", "compose", "exec", "-T", "postgres",
                        "psql", "-c", "SELECT grants"],
        "counter_names": [], "counter_bodies_sha256": {},
        "h14_download_url": (f"{P.ORIGINS['dataset']}/v1/datasets/{uid()}"
                             f"/files/{uid()}/download"),
        "h14_download_body_sha256": hashlib.sha256(h14_body).hexdigest(),
        "h14_download_body_len": len(h14_body),
        "h14_txn_url": (f"{P.ORIGINS['dataset']}/v1/datasets/{uid()}"
                        f"/branches/main/transactions/{uid()}"),
        "h14_txn_projection": {
            "id": "11111111-0000-4000-8000-000000000001", "dataset_id":
            "22222222-0000-4000-8000-000000000002", "branch_name": "main",
            "status": "COMMITTED", "tx_type": "SNAPSHOT", "metadata_rows":
            24, "metadata_bytes": 558, "metadata_file_count": 1,
            "metadata_content_hash": hex64("0")},
        "base3_container_baseline": {
            svc: {"image": f"sha256:{hex64(svc[0])}", "started_at":
                  "2026-11-14T17:15:55.538776Z"}
            for svc in ("dataset", "pipeline", "connector")},
        "quiesce_before": {},
    }
    counter_bodies = {f"p21b-0000000{i}":
                      json.dumps({"name": f"p21b-0000000{i}",
                                  "content": 1,
                                  "total": 151}).encode() for i in range(9)}
    catalog["counter_names"] = sorted(counter_bodies)
    catalog["counter_bodies_sha256"] = {
        k: hashlib.sha256(v).hexdigest() for k, v in counter_bodies.items()}
    return catalog, counter_bodies, h14_body


class FakeTransport:
    """Canned, asserting transport. Refuses unexpected URLs; proves the
    platform JWT never goes to the fixture or the anonymous follow."""

    def __init__(self, routes):
        self.routes = routes            # url -> (status, body, headers)
        self.calls, self.auth_log = [], []

    def __call__(self, url, auth=None, **kw):
        self.calls.append(url)
        self.auth_log.append(auth)
        if url.startswith(P.ORIGINS["fixture"]):
            assert auth is None, "platform JWT must NEVER reach the fixture"
        if P.DOWNLOAD_REDIRECT_PREFIX in url:
            assert auth is None, "follow must be ANONYMOUS"
        if url not in self.routes:
            raise AssertionError(f"unexpected URL {url}")
        status, body, headers = self.routes[url]
        return P.HttpResponse(status, headers or {}, body, url, bool(auth))


class FakeRunner:
    """Canned argv runner; FULL-argv keys (prefix matching caused real
    collisions between the three SQL argv in an earlier draft)."""

    def __init__(self, routes):
        self.routes = {tuple(k): v for k, v in routes.items()}
        self.calls = []

    def __call__(self, argv):
        self.calls.append(list(argv))
        key = tuple(str(x) for x in argv)
        if key in self.routes:
            v = self.routes[key]
            return v(argv) if callable(v) else v
        raise AssertionError(f"unexpected argv {argv}")


def release_file(tmp, phases=("q3-readonly-preflight",)):
    p = Path(tmp) / "release.json"
    p.write_text(json.dumps({"released_for": list(phases),
                             "released_by": "root", "expires_utc":
                             "20991231T235959Z"}))
    return str(p)


def build_fake_world(catalog, counter_bodies, h14_body, rows, protected9,
                     cp8):
    """Canned transport outputs + inspect canned answers matching the
    synthetic baseline. Returns (http_routes, derived, inspect_route)."""

    def inspect_route(argv):
        container = argv[2]
        fmt = argv[argv.index("--format") + 1]
        if fmt.startswith("{{.Id}}"):
            if container == P.Q3_FIXTURE:
                cfg, started = ("python:3.12-alpine",
                                P.EXPECTED_FIXTURE_STARTED_AT)
                img = f"sha256:{hex64('f')}"
            elif container == P.Q3_POSTGRES:
                cfg, started = ("postgres:16-alpine", "2026-11-13T10:00:00Z")
                img = f"sha256:{hex64('p')}"
            else:
                svc = container.replace(f"{G.Q3_PROJECT}-", "").replace(
                    "-1", "")
                b = catalog["base3_container_baseline"][svc]
                img, cfg, started = (b["image"], "composed",
                                     b["started_at"])
            return (0, f"{img}\n{img}\n{cfg}\n{started}\ntrue\n0\n"
                    '["python3","-I","/srv/rest_source.py"]')
        if fmt.startswith("{{range .Mounts}}"):
            m = (f"{G.Q3_PROJECT}_dataset-files:/home/nonroot:volume "
                 if container == P.Q3_DATASET else "")
            return (0, m)
        if fmt.startswith("{{json .NetworkSettings.Ports}}"):
            return (0, '{"5432/tcp": null}\n{}')
        if fmt.startswith("{{if .State.Health}}"):
            healthy = container in (P.Q3_FIXTURE, P.Q3_POSTGRES)
            return (0, "healthy" if healthy else P.HEALTH_NONE)
        if fmt.startswith("{{range $k, $v := .NetworkSettings.Networks}}"):
            ip = P.EXPECTED_FIXTURE_IP if container == P.Q3_FIXTURE \
                else "172.28.0.11"
            net = f"{G.Q3_PROJECT}_p1net"
            return (0, f"{net}={ip} ")
        if fmt.startswith("{{range .Config.Env}}"):
            return (0, "HOST\nPORT\nJWT_SECRET\n")
        raise AssertionError(f"unexpected inspect format {fmt!r}")

    routes = {P.ORIGINS["fixture"] + "/healthz": (200, b"ok", {})}
    for svc in ("dataset", "pipeline", "connector"):
        routes[f"{P.ORIGINS[svc]}/healthz"] = (200, b"ok", {})
    for name, body in counter_bodies.items():
        routes[f"{P.ORIGINS['fixture']}/admin/src/{name}"] = (200, body, {})

    ORG = "a792b91e-0000-4000-8000-00000000000f"
    before = {}
    owner_parents = {"by_def": {}, "source_baseline": {}}
    parents_rows = []
    for i, e in enumerate(cp8):
        cp = json.loads(json.dumps(e["quiesce_checkpoint_full"]))
        runs = [dict(e["matched_public_run"], org_id=ORG,
                     owner_id=uuidish("b00000", i),
                     requested_by=uuidish("c00000", i))]
        before[e["def_id"]] = {"checkpoint": cp, "runs": runs}
        owner_parents["by_def"][e["def_id"]] = {
            "def_id": e["def_id"], "org_id": ORG,
            "source_id": e["source_id"],
            "output_dataset_id": e["cp_owner_row_full"]["output_dataset_id"]}
        # Root fix shape: EVERY owner parent carries the H10/P25-main
        # identity (conn_owner_id == H10 run's requested_by, org == H10 org)
        h10_actor = rows[0]["requested_by"]
        owner_parents["source_baseline"][e["source_id"]] = {
            "conn_id": e["source_id"], "conn_org_id": ORG,
            "conn_owner_id": h10_actor,
            "source_name": f"src-{i}", "connector_type": "rest"}
        parents_rows.append({
            "def_id": e["def_id"], "org_id": ORG, "source_id":
            e["source_id"], "output_dataset_id":
            e["cp_owner_row_full"]["output_dataset_id"], "conn_id":
            e["source_id"], "conn_org_id": ORG, "conn_owner_id":
            h10_actor, "source_name": f"src-{i}",
            "connector_type": "rest"})
    # cp8[0] IS the H10 owner (same def/run as protected9[0]) — the real
    # sealed overlap; no separate H10 def exists.
    h10 = protected9[0]
    assert h10["def_id"] == cp8[0]["def_id"]
    for did, view in before.items():
        routes[f"{P.ORIGINS['connector']}/api/v1/data-connection/syncs/"
               f"{did}/checkpoint"] = (200, json.dumps(
                   view["checkpoint"]).encode(), {})
        routes[f"{P.ORIGINS['connector']}/api/v1/data-connection/syncs/"
               f"{did}/runs"] = (200, json.dumps(
                   view["runs"]).encode(), {})

    txn = {"id": catalog["h14_txn_projection"]["id"], "dataset_id":
           catalog["h14_txn_projection"]["dataset_id"], "branch_name":
           "main", "status": "COMMITTED", "tx_type": "SNAPSHOT",
           "metadata": {"rows": 24, "bytes": 558, "file_count": 1,
                        "content_hash": hex64("0")}}
    routes[catalog["h14_txn_url"]] = (200, json.dumps(txn).encode(), {})
    follow_url = (f"{P.ORIGINS['dataset']}{P.DOWNLOAD_REDIRECT_PREFIX}"
                  f"cb5f/a6fef?expires=123&sig={hex64('e')}")
    routes[catalog["h14_download_url"]] = (302, b"", {"Location":
                                                      follow_url})
    routes[follow_url] = (200, h14_body, {})

    derived = {"protected9": protected9, "cp8": cp8,
               "h10_run_id": h10["run_id"],
               "quiesce_before_chain":
               [{"run_id": r["run_id"],
                 "in_quiesce_before": r["run_id"] == h10["run_id"],
                 **({"checkpoint": before[h10["def_id"]]["checkpoint"],
                     "def_id": h10["def_id"]}
                    if r["run_id"] == h10["run_id"] else {})}
                for r in protected9]}
    return routes, derived, inspect_route, before, owner_parents, \
        parents_rows


def patch_run(catalog, derived):
    substrate = {"compose_file": "x", "port_map": P.EXPECTED_PORT_MAP,
                 "compose_env_refs": ["OF_P1_JWT_SECRET"],
                 "dataset_mount": "dataset-files:/home/nonroot",
                 "env_file": "x", "env_file_present": True,
                 "env_file_mode": "0o600",
                 "env_file_key_names": ["OF_P1_JWT_SECRET"],
                 "fixture_boot_pin": P.EXPECTED_FIXTURE_STARTED_AT}
    return [
        mock.patch.object(P, "load_baseline",
                          lambda fd: ({"seal": {"ok": True},
                                       "derived": {"protected9": 9,
                                                   "cp8": 8}},
                                      dict(catalog), derived)),
        mock.patch.object(P, "load_substrate_facts", lambda: substrate),
        mock.patch.object(P, "read_jwt_secret", lambda: "s3cr3t-test"),
    ]


def make_fake_runner(catalog, rows, cp8, inspect_route, parents_rows):
    """FakeRunner subclass: docker inspect answered by inspect_route
    (argv vary per container/probe); the four SQL argv matched FULL-argv
    (they share their first tokens, so prefix matching would collide)."""
    ps_lines = "\n".join(f"abc{'d' * 8}\t{n}\timg\tUp 30 hours"
                         for n in sorted(G.Q3_CONTAINERS))
    routes = {
        ("docker", "ps", "--format",
         "{{.ID}}\t{{.Names}}\t{{.Image}}\t{{.Status}}"): (0, ps_lines),
        tuple(C.OLDPG_READONLY_ARGV["controlled_rows_27col"]):
            (0, json.dumps(rows)),
        tuple(C.OLDPG_READONLY_ARGV["cp_owners"]):
            (0, json.dumps([e["cp_owner_row_full"] for e in cp8])),
        tuple(P.build_parents_argv(catalog["owner_parents_def_ids"])):
            (0, json.dumps(parents_rows)),
        tuple(catalog["grants_argv"]): (0, "SET\nSET\n\n"),
    }

    class R(FakeRunner):
        def __call__(self, argv):
            self.calls.append(list(argv))
            if argv[:2] == ["docker", "inspect"]:
                return inspect_route(argv)
            key = tuple(str(x) for x in argv)
            if key in self.routes:
                v = self.routes[key]
                return v(argv) if callable(v) else v
            raise AssertionError(f"unexpected argv {argv}")

    return R(routes)


def w8_tmp():
    """Temp dir INSIDE p2-6 (Attempt refuses outside-p2-6 outputs — that
    guard is itself under test); scratch is removed automatically."""
    scratch = P.RESULTS_W8 / ".selftest-scratch"
    scratch.mkdir(parents=True, exist_ok=True)
    return tempfile.TemporaryDirectory(dir=scratch)


def run_cmd_run(tmp, transport, runner, execute=True, release=None):
    args = Namespace(mode="run", execute=execute, release_file=release,
                     out_dir=str(Path(tmp) / "w8"))
    return P.cmd_run(args, transport=transport, runner=runner)


# ===========================================================================
class Comparators(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.org, cls.rows, cls.p9, cls.owners, cls.cp8 = synth_baseline()

    def test_controlled9_happy(self):
        r = P.compare_controlled9(self.p9, json.dumps(self.rows))
        self.assertTrue(r["ok"], r["problems"])

    def test_controlled9_missing(self):
        r = P.compare_controlled9(self.p9, json.dumps(self.rows[:8]))
        self.assertFalse(r["ok"])
        self.assertTrue(any("MISSING" in p for p in r["problems"]))

    def test_controlled9_extra_never_filtered(self):
        extra = make_row(9, self.org)
        r = P.compare_controlled9(self.p9, json.dumps(self.rows + [extra]))
        self.assertFalse(r["ok"])
        self.assertTrue(any("EXTRA" in p for p in r["problems"]))

    def test_controlled9_duplicate(self):
        r = P.compare_controlled9(self.p9, json.dumps(self.rows +
                                                      [self.rows[0]]))
        self.assertFalse(r["ok"])
        self.assertTrue(any("duplicate" in p for p in r["problems"]))

    def test_controlled9_changed_field(self):
        cur = [dict(r) for r in self.rows]
        cur[3]["status"] = "failed"
        r = P.compare_controlled9(self.p9, json.dumps(cur))
        self.assertFalse(r["ok"])
        self.assertIn("status", " ".join(r["problems"]))

    def test_controlled9_empty_is_not_quiet(self):
        r = P.compare_controlled9(self.p9, "SET\nSET\n\n")
        self.assertFalse(r["ok"])

    def test_cp8_happy(self):
        text = json.dumps([e["cp_owner_row_full"] for e in self.cp8])
        r = P.compare_cp_owners(self.cp8, text)
        self.assertTrue(r["ok"], r["problems"])

    def test_cp8_extra_owner_no_protected9_exemption(self):
        intruder = dict(self.cp8[0]["cp_owner_row_full"])
        intruder["run_id"] = self.p9[8]["run_id"]      # a protected-9 run
        intruder["sync_def_id"] = self.p9[8]["def_id"]
        text = json.dumps([e["cp_owner_row_full"] for e in self.cp8] +
                          [intruder])
        r = P.compare_cp_owners(self.cp8, text)
        self.assertFalse(r["ok"])
        self.assertTrue(any("EXTRA" in p and "exemption" in p
                            for p in r["problems"]))

    def test_cp8_changed_blocked_reason(self):
        owners = [dict(e["cp_owner_row_full"]) for e in self.cp8]
        owners[2]["blocked_reason"] = ""
        r = P.compare_cp_owners(self.cp8, json.dumps(owners))
        self.assertFalse(r["ok"])
        self.assertIn("blocked_reason", " ".join(r["problems"]))

    def test_cp8_missing(self):
        r = P.compare_cp_owners(self.cp8, json.dumps(
            [e["cp_owner_row_full"] for e in self.cp8[:7]]))
        self.assertFalse(r["ok"])
        self.assertTrue(any("MISSING" in p for p in r["problems"]))

    def test_public_view_happy(self):
        e = self.cp8[0]
        runs = [dict(e["matched_public_run"])]
        r = P.compare_public_view(e["def_id"], e["quiesce_checkpoint_full"],
                                  runs, 200, e["quiesce_checkpoint_full"],
                                  200, runs)
        self.assertTrue(r["ok"], r["problems"])

    def test_public_view_extra_run(self):
        e = self.cp8[0]
        runs = [dict(e["matched_public_run"])]
        extra = dict(e["matched_public_run"], id=uid())
        r = P.compare_public_view(e["def_id"], e["quiesce_checkpoint_full"],
                                  runs, 200, e["quiesce_checkpoint_full"],
                                  200, runs + [extra])
        self.assertFalse(r["ok"])
        self.assertTrue(any("EXTRA public run" in p for p in r["problems"]))

    def test_public_view_missing_matched_run_expired_owner_retains(self):
        e = self.cp8[0]
        r = P.compare_public_view(e["def_id"], e["quiesce_checkpoint_full"],
                                  [e["matched_public_run"]], 200,
                                  e["quiesce_checkpoint_full"], 200, [])
        self.assertFalse(r["ok"])
        self.assertTrue(any("MISSING public run" in p
                            for p in r["problems"]))

    def test_public_view_checkpoint_404_owner_lost(self):
        e = self.cp8[0]
        r = P.compare_public_view(e["def_id"], e["quiesce_checkpoint_full"],
                                  [e["matched_public_run"]], 404, None,
                                  200, [])
        self.assertFalse(r["ok"])
        self.assertIn("owner lost", " ".join(r["problems"]))

    def test_public_view_changed_checkpoint_owner(self):
        e = self.cp8[0]
        drifted = json.loads(json.dumps(e["quiesce_checkpoint_full"]))
        drifted["owner"]["run_id"] = uid()
        r = P.compare_public_view(e["def_id"], e["quiesce_checkpoint_full"],
                                  [e["matched_public_run"]], 200, drifted,
                                  200, [e["matched_public_run"]])
        self.assertFalse(r["ok"])
        self.assertIn("checkpoint.owner:", " ".join(r["problems"]))

    def test_public_view_changed_run_field(self):
        e = self.cp8[0]
        cur = [dict(e["matched_public_run"], status="succeeded")]
        r = P.compare_public_view(e["def_id"], e["quiesce_checkpoint_full"],
                                  [e["matched_public_run"]], 200,
                                  e["quiesce_checkpoint_full"], 200, cur)
        self.assertFalse(r["ok"])
        self.assertIn("field status", " ".join(r["problems"]))

    def test_grants_zero_happy(self):
        self.assertTrue(P.compare_grants_stdout("SET\nSET\n\n")["ok"])

    def test_grants_nonzero_fails(self):
        r = P.compare_grants_stdout('SET\nSET\n\n[{"sync_def_id":"x"}]\n')
        self.assertFalse(r["ok"])
        self.assertIn("NOT zero", " ".join(r["problems"]))

    def test_grants_wrong_preamble(self):
        self.assertFalse(P.compare_grants_stdout("ERROR: boom\n")["ok"])

    def test_h14_txn_compare(self):
        sealed = {"id": uid(), "dataset_id": uid(), "branch_name": "main",
                  "status": "COMMITTED", "tx_type": "SNAPSHOT",
                  "metadata_rows": 24, "metadata_bytes": 558,
                  "metadata_file_count": 1, "metadata_content_hash":
                  hex64("0")}
        body = {"id": sealed["id"], "dataset_id": sealed["dataset_id"],
                "branch_name": "main", "status": "COMMITTED", "tx_type":
                "SNAPSHOT", "metadata": {"rows": 24, "bytes": 558,
                                         "file_count": 1, "content_hash":
                                         hex64("0")}}
        self.assertTrue(P.compare_h14_txn(sealed, 200, body)["ok"])
        bad = P.compare_h14_txn(sealed, 200, {"metadata": {"rows": 25}})
        self.assertFalse(bad["ok"])
        self.assertIn("metadata_rows", " ".join(bad["problems"]))

    def test_download_redirect_rules(self):
        good = (f"{P.ORIGINS['dataset']}{P.DOWNLOAD_REDIRECT_PREFIX}k"
                f"?expires=1&sig={hex64('e')}")
        self.assertEqual(P.validate_download_redirect(good), good)
        with self.assertRaises(P.PreflightRefusal):     # wrong origin
            P.validate_download_redirect(
                f"http://127.0.0.1:9999{P.DOWNLOAD_REDIRECT_PREFIX}k?sig=x")
        with self.assertRaises(P.PreflightRefusal):     # wrong prefix
            P.validate_download_redirect(
                f"{P.ORIGINS['dataset']}/v1/other/k?sig=x")
        with self.assertRaises(P.PreflightRefusal):     # no presigned query
            P.validate_download_redirect(
                f"{P.ORIGINS['dataset']}{P.DOWNLOAD_REDIRECT_PREFIX}k")


class Redaction(unittest.TestCase):
    def test_query_and_bearer_redaction(self):
        text = (f"GET /v1/_internal/local-fs/k?expires=1&sig={hex64('e')}"
                " Authorization: Bearer eyJabc.def.ghi")
        out, _ = P.redact_text(text, [])
        self.assertNotIn(hex64("e"), out)
        self.assertNotIn("eyJabc", out)
        self.assertIn("<redacted>", out)

    def test_safe_url_projection_never_keeps_query(self):
        proj = P.safe_url_projection(f"{P.ORIGINS['dataset']}/p?"
                                     f"sig={hex64('e')}")
        self.assertNotIn(hex64("e"), json.dumps(proj))
        self.assertTrue(proj["query_digest"].startswith("sha256-"))

    def test_secret_scrub_in_attempt_record(self):
        with w8_tmp() as tmp:
            att = P.Attempt(Path(tmp), "unit")
            att.add_secret("supersecret-value")
            att.record("x", {"argv": ["cmd", "supersecret-value"]},
                       "leak supersecret-value", 0, kind="local")
            for f in att.dir.iterdir():
                self.assertNotIn("supersecret-value", f.read_text())


class ReleaseGateAndPlan(unittest.TestCase):
    def test_run_refuses_without_release_zero_calls(self):
        with w8_tmp() as tmp:
            tr, rn = FakeTransport({}), FakeRunner({})
            rc = run_cmd_run(tmp, tr, rn, execute=True,
                             release=str(Path(tmp) / "missing.json"))
            self.assertEqual(rc, 2)
            self.assertEqual((tr.calls, rn.calls), ([], []))
            res = json.loads(sorted((Path(tmp) / "w8").glob(
                "run-*/RESULT.json"))[-1].read_text())
            self.assertEqual(res["verdict"], "refused")

    def test_run_refuses_when_phase_not_listed(self):
        with w8_tmp() as tmp:
            rel = release_file(tmp, phases=("other-phase",))
            tr, rn = FakeTransport({}), FakeRunner({})
            rc = run_cmd_run(tmp, tr, rn, execute=True, release=rel)
            self.assertEqual(rc, 2)
            self.assertEqual((tr.calls, rn.calls), ([], []))

    def test_plan_makes_zero_live_calls(self):
        calls = {"http": 0, "runner": 0}

        def counting_http(*a, **k):
            calls["http"] += 1
            raise AssertionError("plan must not call HTTP")

        def counting_runner(*a, **k):
            calls["runner"] += 1
            raise AssertionError("plan must not run subprocesses")

        with w8_tmp() as tmp:
            args = Namespace(mode="plan", execute=False, release_file="x",
                             out_dir=str(Path(tmp) / "w8"))
            with mock.patch.object(P, "http_call", counting_http), \
                    mock.patch.object(G, "real_runner", counting_runner):
                rc = P.cmd_plan(args)     # real seal facts: read-only FS
            self.assertEqual(rc, 0)
            self.assertEqual(calls, {"http": 0, "runner": 0})
            plans = sorted((Path(tmp) / "w8").glob("plan-*/plan.json"))
            self.assertEqual(len(plans), 1)
            body = json.loads(plans[-1].read_text())
            self.assertFalse(body["live"])
            self.assertTrue(body["steps"])
            self.assertIn(P.PHASE, json.dumps(body["release_schema"]))
            self.assertTrue(body["origin_allowlist"]["dataset"].endswith(
                "17078"))


class ParentsComparator(unittest.TestCase):
    def _world(self):
        org, rows, p9, owners, cp8 = synth_baseline()
        catalog, counter_bodies, h14_body = synth_catalog()
        _, derived, _, before, owner_parents, parents_rows = \
            build_fake_world(catalog, counter_bodies, h14_body, rows, p9,
                             cp8)
        rb = {}
        for e in cp8:
            for r in before[e["def_id"]]["runs"]:
                if r.get("id") == e["run_id"]:
                    rb[e["def_id"]] = {"org_id": r.get("org_id"),
                                       "owner_id": r.get("owner_id"),
                                       "requested_by":
                                       r.get("requested_by")}
        return owner_parents, cp8, rb, parents_rows

    def test_happy(self):
        bp, cp8, rb, rows = self._world()
        r = P.compare_parents(bp, cp8, rb, json.dumps(rows))
        self.assertTrue(r["ok"], r["problems"])
        self.assertFalse(r["coverage"]["baseline_absent"])

    def test_org_drift_fails(self):
        bp, cp8, rb, rows = self._world()
        rows[0] = dict(rows[0], org_id=uid())
        r = P.compare_parents(bp, cp8, rb, json.dumps(rows))
        self.assertFalse(r["ok"])
        self.assertIn("org_id", " ".join(r["problems"]))

    def test_def_source_ne_cp_source_fails(self):
        bp, cp8, rb, rows = self._world()
        rows[1] = dict(rows[1], source_id=uid())
        r = P.compare_parents(bp, cp8, rb, json.dumps(rows))
        self.assertFalse(r["ok"])
        self.assertIn("Def.source", " ".join(r["problems"]))

    def test_extra_and_missing_def_rows_fail(self):
        bp, cp8, rb, rows = self._world()
        r = P.compare_parents(bp, cp8, rb, json.dumps(rows + [
            dict(rows[0], def_id=uid())]))
        self.assertFalse(r["ok"])
        self.assertTrue(any("EXTRA" in p for p in r["problems"]))
        r = P.compare_parents(bp, cp8, rb, json.dumps(rows[:7]))
        self.assertFalse(r["ok"])
        self.assertTrue(any("MISSING" in p for p in r["problems"]))

    def test_baseline_absent_reported_not_guessed(self):
        bp, cp8, rb, rows = self._world()
        # sealed source response lacked source_name -> coverage entry,
        # current value recorded, never compared, never a failure
        first = cp8[0]["source_id"]
        del bp["source_baseline"][first]["source_name"]
        r = P.compare_parents(bp, cp8, rb, json.dumps(rows))
        self.assertTrue(r["ok"], r["problems"])
        self.assertIn(f"{cp8[0]['def_id']}.source_name",
                      r["coverage"]["baseline_absent"])

    def test_tenant_binding_current_mismatch_fails(self):
        bp, cp8, rb, rows = self._world()
        rows[2] = dict(rows[2], conn_org_id=uid())
        r = P.compare_parents(bp, cp8, rb, json.dumps(rows))
        self.assertFalse(r["ok"])
        self.assertIn("Conn.org", " ".join(r["problems"]))


class JwtIdentitySelection(unittest.TestCase):
    """Root fix (post attempt1): protected9[0] is a FOREIGN legacy P21
    tenant row; the JWT identity MUST come from the H10 run projection
    and be cross-asserted against the whole-8 sealed parents."""

    def _mini_world(self, *, tamper=None, absent=False):
        org = "a792b91e-dab5-4739-9344-2848c1e83487"
        foreign = "c7673177-0000-4000-8000-0000000000c7"
        sub = "1c0e238a-3657-4978-98dc-d71a342c3fa1"
        h10 = {"run_id": uuidish("000000", 6), "def_id":
               uuidish("100000", 6), "source_id": uuidish("200000", 6),
               "output_dataset_id": uuidish("300000", 6),
               "public_projection_27": {"requested_by": sub, "org_id": org}}
        first = {"run_id": uuidish("000000", 0), "def_id":
                 uuidish("100000", 0),
                 "public_projection_27": {"requested_by":
                                          uuidish("f00000", 0),
                                          "org_id": foreign}}
        # list order mirrors the real derive: original6 first (foreign
        # legacy tenant), H10 later — index 0 must NEVER win
        protected9 = [first] + [
            {"run_id": uuidish("000000", i), "def_id": uuidish("100000", i),
             "public_projection_27": {"requested_by":
                                      uuidish("f00000", i),
                                      "org_id": foreign}}
            for i in range(1, 6)] + [h10] + [
            {"run_id": uuidish("000000", i), "def_id":
             uuidish("100000", i),
             "public_projection_27": {"requested_by": sub, "org_id": org}}
            for i in (7, 8)]
        cp8 = []
        srcs = {}
        for i in range(8):
            sid = uuidish("700000", i)
            cp8.append({"def_id": uuidish("100000", i + 10) if i else
                        h10["def_id"], "source_id": sid, "run_id":
                        uuidish("600000", i), "origin": "x", "tag": "t",
                        "cp_owner_row_full": {}, "quiesce_checkpoint_full":
                        {}, "matched_public_run": {}})
            srcs[sid] = {"conn_id": sid, "conn_org_id": org,
                         "conn_owner_id": sub, "source_name": f"s{i}",
                         "connector_type": "rest"}
        if tamper:
            srcs[tamper]["conn_owner_id"] = uid()
        if absent:
            del srcs[next(iter(srcs))]["conn_org_id"]
        derived = {"protected9": protected9, "cp8": cp8,
                   "h10_run_id": h10["run_id"], "quiesce_before_chain": []}
        catalog = {"owner_parents": {"by_def": {}, "source_baseline": srcs}}
        return derived, catalog, sub, org

    def test_selects_h10_identity_not_first_row(self):
        derived, catalog, sub, org = self._mini_world()
        got_sub, got_org = P.jwt_identity_from_baseline(derived, catalog)
        self.assertEqual((got_sub, got_org), (sub, org))  # NOT the P21 row

    def test_cross_assert_mismatch_refuses(self):
        derived, catalog, _, _ = self._mini_world(
            tamper=uuidish("700000", 3))
        with self.assertRaises(P.PreflightRefusal) as cm:
            P.jwt_identity_from_baseline(derived, catalog)
        self.assertIn("conn_owner_id", str(cm.exception))

    def test_cross_assert_absent_refuses(self):
        derived, catalog, _, _ = self._mini_world(absent=True)
        with self.assertRaises(P.PreflightRefusal) as cm:
            P.jwt_identity_from_baseline(derived, catalog)
        self.assertIn("baseline absent", str(cm.exception))


class ParentsSqlShapeAndPostMode(unittest.TestCase):
    """R2: the parents SQL projection keys must EXACTLY match what
    compare_parents consumes (an earlier draft shipped `d.id::text`
    without AS def_id and would have failed the FIRST real run)."""

    def test_sql_projection_shape_matches_comparator(self):
        sql = P.build_parents_argv(["d" * 8 + "-0000-4000-8000-0" +
                                    "0" * 12])[-1]
        # aliased columns
        for alias in ("AS def_id", "AS conn_id", "AS conn_org_id",
                      "AS conn_owner_id", "AS source_name"):
            self.assertIn(alias, sql)
        # unaliased d./c. columns deliver their own names
        for col in ("d.org_id::text", "d.source_id::text",
                    "d.output_dataset_id::text", "c.connector_type"):
            self.assertIn(col, sql)
        # comparator consumes exactly these keys
        self.assertEqual(set(P.PARENTS_REQUIRED_KEYS), {
            "def_id", "org_id", "source_id", "output_dataset_id",
            "conn_id", "conn_org_id", "conn_owner_id", "source_name",
            "connector_type"})

    def test_missing_schema_key_structured_fail(self):
        bp, cp8, rb, rows = ParentsComparator()._world()
        broken = [{k: v for k, v in rows[0].items()
                   if k != "conn_owner_id"}] + rows[1:]
        r = P.compare_parents(bp, cp8, rb, json.dumps(broken))
        self.assertFalse(r["ok"])
        self.assertIn("missing schema keys",
                      " ".join(r["problems"]))
        self.assertIn("conn_owner_id", " ".join(r["problems"]))

    # --- R2 pre/post image expectations -------------------------------
    def test_post_switch_expect_replaces_old_started_pin(self):
        cat = {"base3_container_baseline": {
            "dataset": {"image": f"sha256:{hex64('d')}",
                        "started_at": "2026-11-14T17:15:55Z"}}}
        new_img = f"sha256:{hex64('n')}"
        raw = (f"{new_img}\n{new_img}\ncomposed\n"
               "2026-11-15T09:00:00Z\ntrue\n0\n"
               '["/svc"]')
        sid = f"inspect-{P.Q3_DATASET}-identity"
        # default (no expect): new boot violates the old pin
        ok, d = P.evaluate_inspect(sid, P.Q3_DATASET, raw, cat)
        self.assertFalse(ok)
        # with the Root-released expect: new boot legitimate
        ok, d = P.evaluate_inspect(sid, P.Q3_DATASET, raw, cat,
                                   expects={"dataset": new_img})
        self.assertTrue(ok, d)
        # but a DIFFERENT running image violates the expect pin
        other = f"sha256:{hex64('x')}"
        raw_bad = raw.replace(new_img, other)
        ok, d = P.evaluate_inspect(sid, P.Q3_DATASET, raw_bad, cat,
                                   expects={"dataset": new_img})
        self.assertFalse(ok)
        self.assertIn("expect", d)
        # restart count still enforced even when switched
        raw_restart = raw.replace("true\n0", "true\n2")
        ok, d = P.evaluate_inspect(sid, P.Q3_DATASET, raw_restart, cat,
                                   expects={"dataset": new_img})
        self.assertFalse(ok)
        self.assertIn("RestartCount", d)

    def test_pipeline_and_fixture_never_switched(self):
        cat = {"base3_container_baseline": {
            "pipeline": {"image": f"sha256:{hex64('p')}",
                         "started_at": "2026-11-14T17:15:56Z"}}}
        raw = (f"sha256:{hex64('q')}\ncomposed\ncomposed\n"
               "2026-11-15T09:00:00Z\ntrue\n0\n[]")
        sid = f"inspect-{P.Q3_PIPELINE}-identity"
        ok, d = P.evaluate_inspect(sid, P.Q3_PIPELINE, raw, cat,
                                   expects={"dataset": f"sha256:"
                                              + hex64('n')})
        self.assertFalse(ok)     # pipeline keeps the strict old pin

    def test_expect_image_format_validation(self):
        from argparse import Namespace
        good = Namespace(expect_dataset_image=f"sha256:{hex64('a')}",
                         expect_connector_image=None)
        self.assertEqual(P.expect_images_from_args(good),
                         {"dataset": f"sha256:{hex64('a')}"})
        for bad in ("abc", "sha256:xyz", f"sha256:{hex64('a')[:-1]}",
                    f"{hex64('a')}"):
            args = Namespace(expect_dataset_image=bad,
                             expect_connector_image=None)
            with self.assertRaises(P.PreflightRefusal):
                P.expect_images_from_args(args)


class DockerHealthNetwork(unittest.TestCase):
    def test_health_predicate(self):
        ok, d = P.evaluate_inspect("inspect-fixture-1-health",
                                   P.Q3_FIXTURE, "healthy", {})
        self.assertTrue(ok)
        ok, d = P.evaluate_inspect("inspect-fixture-1-health",
                                   P.Q3_FIXTURE, "unhealthy", {})
        self.assertFalse(ok)
        self.assertIn("health", d)
        ok, d = P.evaluate_inspect("inspect-dataset-1-health",
                                   P.Q3_DATASET, P.HEALTH_NONE, {})
        self.assertTrue(ok)

    def test_network_predicate(self):
        net = f"{G.Q3_PROJECT}_p1net"
        ok, d = P.evaluate_inspect("inspect-fixture-1-network",
                                   P.Q3_FIXTURE,
                                   f"{net}={P.EXPECTED_FIXTURE_IP} ", {})
        self.assertTrue(ok)
        ok, d = P.evaluate_inspect(
            "inspect-fixture-1-network", P.Q3_FIXTURE,
            f"{net}=172.28.9.8 ", {})
        self.assertFalse(ok)          # fixture IP drifted off compose pin
        ok, d = P.evaluate_inspect("inspect-dataset-1-network",
                                   P.Q3_DATASET, "bridge=172.17.0.2 ", {})
        self.assertFalse(ok)          # wrong network identity


class ExceptionEscape(unittest.TestCase):
    """R1 #3: an exception mid-request must still leave the SAFE cmd on
    disk first, archive the original failure, and redact everything."""

    def _run_with_boom(self, tmp, boom_after_jwt=True):
        org, rows, p9, owners, cp8 = synth_baseline()
        catalog, counter_bodies, h14_body = synth_catalog()
        routes, derived, inspect_route, before, owner_parents, \
            parents_rows = build_fake_world(catalog, counter_bodies,
                                            h14_body, rows, p9, cp8)
        catalog["quiesce_before"] = before
        catalog["owner_parents"] = owner_parents
        catalog["owner_parents_def_ids"] = sorted(owner_parents["by_def"])
        runner = make_fake_runner(catalog, rows, cp8, inspect_route,
                                  parents_rows)

        class Boom:
            def __init__(self, inner):
                self.inner, self.calls = inner, []

            def __call__(self, url, auth=None, **kw):
                self.calls.append(url)
                if url == f"{P.ORIGINS['dataset']}/healthz":
                    raise OSError("connection reset by peer "
                                  f"sig={hex64('z')} secret=s3cr3t-test")
                return self.inner(url, auth=auth, **kw)

        tr = Boom(FakeTransport(routes))
        patches = patch_run(catalog, derived)
        with patches[0], patches[1], patches[2]:
            args = Namespace(mode="run", execute=True,
                             release_file=release_file(tmp),
                             out_dir=str(Path(tmp) / "w8"))
            rc = P.cmd_run(args, transport=tr, runner=runner)
        return rc, sorted((Path(tmp) / "w8").glob("run-*"))[-1]

    def test_exception_archives_cmd_and_redacts(self):
        with w8_tmp() as tmp:
            rc, attempt = self._run_with_boom(tmp)
            self.assertEqual(rc, 1)
            self.assertTrue(list(attempt.glob(
                "*service-healthz-dataset.cmd.json")))
            raws = list(attempt.glob("*service-healthz-dataset.raw"))
            self.assertEqual(len(raws), 1)        # original failure kept
            self.assertIn("OSError", raws[0].read_text())
            res = json.loads((attempt / "RESULT.json").read_text())
            self.assertEqual(res["verdict"], "fail")
            # nothing secret in ANY file despite the exception text
            for f in attempt.rglob("*"):
                if f.is_file():
                    txt = f.read_text(errors="replace")
                    self.assertNotIn("s3cr3t-test", txt)
                    self.assertNotIn(hex64("z"), txt)


class FakeWorldRuns(unittest.TestCase):
    def _setup(self, tmp):
        org, rows, p9, owners, cp8 = synth_baseline()
        catalog, counter_bodies, h14_body = synth_catalog()
        routes, derived, inspect_route, before, owner_parents, \
            parents_rows = build_fake_world(
                catalog, counter_bodies, h14_body, rows, p9, cp8)
        catalog["quiesce_before"] = before
        catalog["owner_parents"] = owner_parents
        catalog["owner_parents_def_ids"] = sorted(owner_parents["by_def"])
        runner = make_fake_runner(catalog, rows, cp8, inspect_route,
                                  parents_rows)
        patches = patch_run(catalog, derived)
        return catalog, routes, runner, patches, parents_rows

    def test_full_happy_fake_run_passes_and_leaks_nothing(self):
        with w8_tmp() as tmp:
            catalog, routes, runner, patches, _ = self._setup(tmp)
            tr = FakeTransport(routes)
            with patches[0], patches[1], patches[2]:
                rc = run_cmd_run(tmp, tr, runner, execute=True,
                                 release=release_file(tmp))
            self.assertEqual(rc, 0)
            attempt = sorted((Path(tmp) / "w8").glob("run-*"))[-1]
            res = json.loads((attempt / "RESULT.json").read_text())
            self.assertEqual(res["verdict"], "pass")
            self.assertTrue(res["live"])
            # the download step (its own kind) must actually RUN — an
            # earlier draft silently skipped it and still passed
            self.assertIn(catalog["h14_download_url"], tr.calls)
            self.assertTrue(any(P.DOWNLOAD_REDIRECT_PREFIX in u
                                for u in tr.calls))
            self.assertTrue(list(attempt.glob("*h14-download-gate.gate.rc")))
            self.assertEqual(res["scope"], P.SCOPE)
            self.assertFalse(res["full_old_regressions_complete"])
            self.assertIn("build_job_all_history", res["deferred"])
            # R1 #2: full safe public objects archived, recomputable
            view_raw = list(attempt.glob("*view-cp-*.raw"))
            self.assertTrue(view_raw)
            for f in view_raw:
                txt = f.read_text()
                self.assertIn("safe_projection", txt)
                self.assertIn('"mode"', txt)   # actual object fields
            follow_raw = list(attempt.glob("*h14-download-follow.raw"))
            self.assertEqual(len(follow_raw), 1)
            self.assertIn("hello-old-bytes",
                          follow_raw[0].read_text())
            for url, auth in zip(tr.calls, tr.auth_log):
                if url.startswith(P.ORIGINS["fixture"]) or \
                        P.DOWNLOAD_REDIRECT_PREFIX in url:
                    self.assertIsNone(auth)
            jwt_values = [a for a in tr.auth_log if a]
            self.assertTrue(jwt_values)     # protected reads DID carry it
            for f in attempt.rglob("*"):
                if f.is_file():
                    txt = f.read_text(errors="replace")
                    self.assertNotIn("s3cr3t-test", txt)
                    for j in jwt_values:
                        self.assertNotIn(j, txt)
                    self.assertNotIn(hex64("e"), txt)   # presigned sig
            self.assertTrue(list(attempt.glob("*.rc")))

    def test_wrong_redirect_origin_fails_closed(self):
        with w8_tmp() as tmp:
            catalog, routes, runner, patches, _ = self._setup(tmp)
            evil = (f"http://127.0.0.1:9999{P.DOWNLOAD_REDIRECT_PREFIX}"
                    f"k?sig=x")
            routes[catalog["h14_download_url"]] = (302, b"",
                                                   {"Location": evil})
            with patches[0], patches[1], patches[2]:
                rc = run_cmd_run(tmp, FakeTransport(routes), runner,
                                 execute=True, release=release_file(tmp))
            self.assertEqual(rc, 1)
            attempt = sorted((Path(tmp) / "w8").glob("run-*"))[-1]
            res = json.loads((attempt / "RESULT.json").read_text())
            self.assertEqual(res["verdict"], "fail")
            self.assertIn("WRONG ORIGIN", res["first_error"]["detail"])

    def test_service_healthz_500_fails_closed(self):
        with w8_tmp() as tmp:
            catalog, routes, runner, patches, _ = self._setup(tmp)
            victim = f"{P.ORIGINS['connector']}/healthz"
            routes[victim] = (500, b"boom", {})
            tr = FakeTransport(routes)
            with patches[0], patches[1], patches[2]:
                rc = run_cmd_run(tmp, tr, runner, execute=True,
                                 release=release_file(tmp))
            self.assertEqual(rc, 1)
            attempt = sorted((Path(tmp) / "w8").glob("run-*"))[-1]
            res = json.loads((attempt / "RESULT.json").read_text())
            self.assertIn("service /healthz", res["first_error"]["detail"])

    def test_http_500_stops_first_error(self):
        with w8_tmp() as tmp:
            catalog, routes, runner, patches, _ = self._setup(tmp)
            victim = next(u for u in routes if u.endswith("/checkpoint"))
            routes[victim] = (500, b"boom", {})
            tr = FakeTransport(routes)
            with patches[0], patches[1], patches[2]:
                rc = run_cmd_run(tmp, tr, runner, execute=True,
                                 release=release_file(tmp))
            self.assertEqual(rc, 1)
            self.assertEqual(tr.calls[-1], victim)   # nothing after it

    def test_baseline_refusal_blocks_live_calls(self):
        with w8_tmp() as tmp:
            tr, rn = FakeTransport({}), FakeRunner({})

            def boom(fd):
                raise P.PreflightRefusal("seal broken")

            with mock.patch.object(P, "load_baseline", boom):
                rc = run_cmd_run(tmp, tr, rn, execute=True,
                                 release=release_file(tmp))
            self.assertEqual(rc, 2)
            self.assertEqual((tr.calls, rn.calls), ([], []))


if __name__ == "__main__":
    unittest.main(verbosity=2)

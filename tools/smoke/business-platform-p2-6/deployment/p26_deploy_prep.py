#!/usr/bin/env python3
"""P2.6 W6 deployment PREPARATION (plan/pure only — NO actual up/bind/
restart until Root's next phase file; Root gates: switch-dataset,
switch-connector, http-fixture-serve, http-proxy-serve, http-aux-start).

Facts pinned from read-only inspection + W7's proposal-only plan-live
output (2026-11-15). W8 owns the current-core-preservation preflight
(REQUIRED before first switch — my historical seal guard is NOT that
gate); W7 owns HTTP ops/scenarios; this file only prepares exact argv,
the image-only override, typed CID ledger schema and the new 0600 env
file PLAN (metadata only — no values, ever).
"""

import argparse
import json
import sys
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402

TOOL = "p26_deploy_prep"
OUT = G.RESULTS_W6 / "deployment"
Q3_NET = "openfoundry-platform-p1-q3v9_p1net"   # ACTUAL inspected net
Q3_COMPOSE = "/private/tmp/openfoundry-platform-p1-q3v9/compose.yaml"
Q3_ENV = "/private/tmp/openfoundry-platform-p1-q3v9/.env"
PY_FULL = ("sha256:c5ba3deef00c7758561a0f03c26f44f10589263d40e732b8748e0"
           "d1f38f6dd89")  # python:3.12-alpine cached (whitelist-verified)
S = "b94e6b2f33afe91187a569982a2763c27d04bbd8502fa92c3b5e186fe18ff14c"
CAND = {
 "dataset": {"image_full": "sha256:4217c40275b9fb7b28f8ef872a6b64870876"
              "ce6e6c599ff4714531dd919ef75b",
              "binary_sha256": "e3fa14ca388437eeaef737919f03d3eb260852cb"
              "79b28c9eaea09f08e3de10bc",
              "build_evidence": "results/w6/build/build-dataset_"
              "20261115T053954Z",
              "verified": "RESUME-VERIFIED 2026-11-15 "
                          "(build-resume-dataset gates 8/8 rc0: exact "
                          "fullID + safe-config==old + source S "
                          "before/after; see results/w6/build/"
                          "build-resume-dataset_20261115T055923Z)"},
 "connector": {"image_full": "sha256:f23f69161c6f86f7402a18dbd14e24fdcad"
               "581ecbd9968e7b964ef1baf11df34",
               "binary_sha256": "58c1728043b4632a65c89ca3b2922ef00ad92de"
               "0001359c1d2cc163c05fefbbf",
               "build_evidence": "results/w6/build/build-connector_"
               "20261115T054527Z",
               "verified": "RESUME-VERIFIED 2026-11-15 "
                           "(build-resume-connector gates 8/8 rc0; see "
                           "results/w6/build/build-resume-connector_"
                           "20261115T055933Z)"},
}
HTTP_LABELS = ["of-p26-owner=p26-http", "of-p26-stage=p2-6"]
ROLES = {"p26fixture": "fixture", "p26proxy": "proxy",
         "p26connectora": "connector_a", "p26connectorb": "connector_b"}
P26_ROOT = str(G.ROOT_P26)
ADMIN_ENV = "/private/tmp/openfoundry-p26-http-admin.env"
SMOKE_ENV = "/private/tmp/openfoundry-p26-http-smoke.env"
AUX_ENV = "/private/tmp/openfoundry-p26-aux.env"
RELEASE_JSON = "/private/tmp/openfoundry-p26-release.json"


def candidates():
    return {"frozen_S": S, "candidates": CAND,
            "old_full_ids_untouched": {
                "dataset": "dc4eca36295110f253af382f4cc89d2263cfff4b36671e87a9156a5f577c9264",
                "connector": "6b556702b34ba399f4ee434963c798faf86b12af294657ec3b144309c0097592",
                "pipeline": "d50858d8ddcf4b1cdc11eb0a071a180ade0bb4af43fd471dd8464885b7700520",
            },
            "note": "old q3 tags/images/containers preserved until switch; "
                    "old PG/Pipeline/fixture NEVER recreated"}


def override_yaml():
    return (f"# P26 image-only candidate override (W6-prepared; use with "
            f"BASE q3 compose + private env, unchanged)\n"
            f"services:\n"
            f"  dataset:\n    image: {CAND['dataset']['image_full']}\n"
            f"  connector:\n    image: {CAND['connector']['image_full']}\n"
            f"# NOTHING else overridden: named-files volume /home/nonroot,\n"
            f"# old PG volumes, network/bindings/security config all come\n"
            f"# from the BASE compose; candidate AUTO_RECOVERY default OFF,\n"
            f"# cron/sweeper 0 remain old cfg.\n")


def switch_plan():
    ov = "tools/smoke/business-platform-p2-6/deployment/" \
         "override-imageonly-p26.yaml"
    up = ["docker", "compose", "--env-file", Q3_ENV, "-f", Q3_COMPOSE,
          "-f", ov, "--project-name", G.Q3_PROJECT, "up", "-d",
          "--no-deps", "--no-build", "--pull=never"]
    return {
     "required_before_first_switch": [
        "W8 actual current_core_preservation GREEN (NOT the W6 "
        "historical seal guard)", "candidate 08/09 verify re-release "
        "green (null-Cmd fix)", "health check of old running service"],
     "phases": ["switch-dataset", "switch-connector"],
     "phase_sequencing": "Root releases switch-dataset FIRST as a single "
                         "phase; W8 does a mid-check (new Dataset pinned) "
                         "BEFORE Root releases switch-connector as the "
                         "second single phase. Main switch does NOT wait "
                         "for HTTP serve containers.",
     "steps": [
      {"phase": "switch-dataset", "argv": up + ["dataset"],
       "post": ["health", "full running Image == candidate fullID",
                "named-files volume present", "W8 readonly core check"]},
      {"phase": "switch-connector", "argv": up + ["connector"],
       "post": ["health", "full running Image == candidate fullID",
                "loops off / current grants none", "W8 after-check"]}],
     "untouched": ["pipeline", "old PG", "old fixture-1",
                   "no whole-stack up/down/restart"],
     "checkpoint_note": "Main image-only switch can CHECKPOINT after "
                        "dataset+connector health+W8 check — does NOT "
                        "need all new HTTP serve containers started; "
                        "serve phases are independent follow-ups",
     "rollback_prepared_manual_only": [
        "NOT docker tag (never mutate old tags). Instead: generate a "
        "SEPARATE old-image override file (same shape as the candidate "
        "override but pinning OLD fullIDs) then the SAME compose up -d "
        "--no-deps --no-build --pull=never <service> command. Old "
        "images are already present; compose re-creates the service "
        "container from the old fullID without retagging anything.",
        {"override_file": "override-imageonly-rollback-p26.yaml",
         "content_hint": "identical to override-imageonly-p26.yaml but "
                         "with OLD fullIDs substituted (from the "
                         "candidates() old_full_ids_untouched values)"},
        {"example_dataset": ["docker", "compose", "--env-file", Q3_ENV,
                             "-f", Q3_COMPOSE,
                             "-f", "override-imageonly-rollback-p26.yaml",
                             "--project-name", G.Q3_PROJECT,
                             "up", "-d", "--no-deps", "--no-build",
                             "--pull=never", "dataset"]},
        {"example_connector": ["docker", "compose", "--env-file", Q3_ENV,
                               "-f", Q3_COMPOSE,
                               "-f", "override-imageonly-rollback-p26.yaml",
                               "--project-name", G.Q3_PROJECT,
                               "up", "-d", "--no-deps", "--no-build",
                               "--pull=never", "connector"]},
        "NEVER auto-rollback after unknown writes — report, Root decides; "
        "rollback is manual-only via the separate override file"]}


def _run_argv(name, role, port, cport, image, cmd, extra_opts=None):
    """Docker argv order: OPTIONS → IMAGE → CMD."""
    opts = extra_opts or []
    return (["docker", "run", "-d", "--pull=never", "--name", name,
             "--network", Q3_NET, "--label", HTTP_LABELS[0],
             "--label", HTTP_LABELS[1], "--label",
             f"of-p26-role={role}",
             "-p", f"127.0.0.1:{port}:{cport}"] + opts +
            [image] + cmd)


def http_plan():
    """Exact argv; Docker order OPTIONS→IMAGE→CMD; paths match the
    actual P26 root mount at /srv/p26 (scripts at /srv/p26/fixture/)."""
    ro = ["-v", f"{P26_ROOT}:/srv/p26:ro",
          "-v", f"{RELEASE_JSON}:/run/p26-release.json:ro",
          "-e", "P26_RELEASE_FILE=/run/p26-release.json"]
    fix_cmd = ["python3", "-I",
               "/srv/p26/fixture/p26_rest_source.py",
               "serve", "0.0.0.0", "8090",
               "--admin-token-env", "P26_HTTP_ADMIN_TOKEN"]
    px_cmd = ["python3", "-I",
              "/srv/p26/fixture/p26_dataset_fault_proxy.py",
              "serve", "0.0.0.0", "8091", "http://dataset:50078",
              "--admin-token-env", "P26_HTTP_ADMIN_TOKEN"]
    # ALL production config.go exact env names (no invented vars):
    #  HOST=0.0.0.0 (config.go:83)  PORT=50088 (config.go:84)
    #  DATABASE_URL constructed in memory from q3 OF_P1_PG_PASSWORD (NOT
    #    a key copy; value never printed)
    #  JWT_SECRET from OF_P1_JWT_SECRET; CREDENTIAL_ENCRYPTION_KEY from
    #    OF_P1_CREDENTIAL_KEY (both in 0600 aux env file, memory only)
    #  OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS=0 (config.go:129)
    #  OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=0 (config.go:133)
    #  OPENFOUNDRY_SYNC_AUTO_RECOVERY_ENABLED=false (config.go:137)
    #  ALLOWED_EGRESS_HOSTS=fixture,p26fixture (config.go:112)
    aux_env = ["--env-file", AUX_ENV,
               "-e", "HOST=0.0.0.0",
               "-e", "PORT=50088",
               "-e", "DATASET_SERVICE_URL=http://p26proxy:8091",
               "-e", "ALLOWED_EGRESS_HOSTS=fixture,p26fixture",
               "-e", "ALLOW_PRIVATE_NETWORK_EGRESS=true",
               "-e", "OPENFOUNDRY_AUTO_REGISTRATION_INTERVAL_SECS=0",
               "-e", "OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=0",
               "-e", "OPENFOUNDRY_SYNC_AUTO_RECOVERY_ENABLED=false"]
    pyimg = "sha256:c5ba3deef00c7758561a0f03c26f44f10589263d40e732b8748e0d1f38f6dd89"
    cnimg = "sha256:f23f69161c6f86f7402a18dbd14e24fdcad581ecbd9968e7b964ef1baf11df34"
    fixture = _run_argv("p26fixture", "fixture", 17095, 8090, pyimg,
                        fix_cmd, ro + ["--env-file", ADMIN_ENV])
    proxy = _run_argv("p26proxy", "proxy", 17096, 8091, pyimg,
                      px_cmd, ro + ["--env-file", ADMIN_ENV])
    auxa = _run_argv("p26connectora", "connector_a", 17097, 50088,
                     cnimg, [], aux_env)
    auxb = _run_argv("p26connectorb", "connector_b", 17098, 50088,
                     cnimg, [], aux_env)
    return {
     "network": Q3_NET,
     "prechecks_before_any_bind": [
        # 4 EXACT container-name checks (docker ps -a covers stopped/
        #   unknown-label; any hit — running or dead — is a collision)
        ["docker", "ps", "-a", "--filter", "name=p26fixture",
         "--format", "{{.Names}}"],
        ["docker", "ps", "-a", "--filter", "name=p26proxy",
         "--format", "{{.Names}}"],
        ["docker", "ps", "-a", "--filter", "name=p26connectora",
         "--format", "{{.Names}}"],
        ["docker", "ps", "-a", "--filter", "name=p26connectorb",
         "--format", "{{.Names}}"],
        # ports must be free
        ["lsof", "-nP", "-iTCP:17095-17098", "-sTCP:LISTEN"],
        "ANY output from the 4 name filters or lsof => HALT preserve "
        "(stopped/unknown-label collision counts); NEVER rm; Root "
        "decides"],
     "phases": ["http-fixture-serve", "http-proxy-serve",
                "http-aux-start"],
     "serves": [
      {"phase": "http-fixture-serve", "argv": fixture,
       "notes": "/srv/p26/fixture/p26_rest_source.py (P26 root RO)"},
      {"phase": "http-proxy-serve", "argv": proxy,
       "notes": "upstream http://dataset:50078"},
      {"phase": "http-aux-start", "argv": [auxa, auxb],
       "notes": "ALLOWED_EGRESS_HOSTS=fixture,p26fixture; "
                "OPENFOUNDRY_SYNC_SCHEDULER_INTERVAL_SECS=0 (production "
                "name, 0=off per config.go:133); no invented vars"}],
     "typed_cid_ledger_schema": {
       "per_resource": ["cid", "name", "labels", "image_full_id",
                        "network", "host_port", "started_at"]},
     "old_never_restart": ["fixture-1 (17090)", "P25 aux (17092-94)"]}


def env_plan():
    return {"metadata_only_no_values": True, "files": [
      {"path": ADMIN_ENV, "mode": "0600",
       "key_names": ["P26_HTTP_ADMIN_TOKEN"],
       "gen": "python secrets.token_urlsafe — generated at release time; "
              "value never printed/archived; fixture/proxy/runner "
              "memory-only (X-P26-Admin-Token)"},
      {"path": SMOKE_ENV, "mode": "0600",
       "key_names": ["OPENFOUNDRY_SMOKE_JWT (platform org JWT — attaches "
                     "ONLY to dvs/connector clients)",
                     "P26_W7_JWT_WRITER (writer JWT)",
                     "P26_W7_JWT_READONLY (readonly same-scope-identity-"
                     "no-write JWT)",
                     "P26_W7_JWT_FOREIGN (foreign REAL-signed distinct-"
                     "tenant JWT)"],
       "gen": "W7 harness loads in memory from q3 OF_P1_JWT_SECRET + "
              "known fixture identities; JWTs are stateless — valid "
              "within expiry regardless of restarts. P26_HTTP_ADMIN_"
              "TOKEN is SEPARATE (admin env file, never here)."},
      {"path": AUX_ENV, "mode": "0600",
       "key_names": ["DATABASE_URL (CONSTRUCTED in memory from q3 "
                     "OF_P1_PG_PASSWORD — not a key copy)",
                     "JWT_SECRET (from OF_P1_JWT_SECRET)",
                     "CREDENTIAL_ENCRYPTION_KEY (from "
                     "OF_P1_CREDENTIAL_KEY)"],
       "gen": "values constructed in memory from q3 private file keys "
              "OF_P1_PG_PASSWORD / OF_P1_JWT_SECRET / "
              "OF_P1_CREDENTIAL_KEY (connector only); never print/"
              "archive DSN/JWT/signatures"}],
     "separation": "admin/env credentials NEVER ride platform JWT env; "
                   "platform JWT attaches ONLY to dvs/connector clients"}


def main(argv=None):
    ap = argparse.ArgumentParser(prog=TOOL)
    ap.add_argument("mode", choices=["candidates", "override",
                                     "switch-plan", "http-plan",
                                     "env-plan"])
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    OUT.mkdir(parents=True, exist_ok=True)
    data = {"candidates": candidates, "override": lambda: {
                "file": "override-imageonly-p26.yaml",
                "content": override_yaml()},
            "switch-plan": switch_plan,
            "http-plan": http_plan, "env-plan": env_plan}[args.mode]()
    G.write_json(Path(args.out), {"tool": TOOL, "utc": G.now_utc(),
                                  "mode": args.mode, "plan": data})
    if args.mode == "override":
        ovf = Path(__file__).parent / "override-imageonly-p26.yaml"
        ovf.write_text(override_yaml())
        print(f"[{TOOL}] override written -> {ovf}")
    print(f"[{TOOL}] {args.mode} -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

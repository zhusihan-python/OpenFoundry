#!/usr/bin/env python3
"""PURE self-test for deployment/p26_deploy_prep.py (plan shapes only)."""

import json
import sys
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from deployment import p26_deploy_prep as DP  # noqa: E402

RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond), detail))
    print(f"[deploy] {'PASS' if cond else 'FAIL'} {name}"
          + (f" — {detail}" if detail and not cond else ""))


def j(a):
    return " ".join(str(x) for x in a)


def main():
    hp = DP.http_plan()
    serves = {s["phase"]: s for s in hp["serves"]}
    fx = j(serves["http-fixture-serve"]["argv"])
    px = j(serves["http-proxy-serve"]["argv"])
    aux = serves["http-aux-start"]["argv"]
    check("net-is-q3-p1net", DP.Q3_NET in fx and "of-p26-pg-net" not in fx)
    check("pull-never-all",
          "--pull=never" in fx and "--pull=never" in px and
          all("--pull=never" in j(a) for a in aux))
    check("labels-trio-roles",
          "of-p26-owner=p26-http" in fx and "of-p26-stage=p2-6" in fx and
          "of-p26-role=fixture" in fx and "of-p26-role=proxy" in px and
          "of-p26-role=connector_a" in j(aux[0]) and
          "of-p26-role=connector_b" in j(aux[1]))
    check("ports-actual",
          "127.0.0.1:17095:8090" in fx and "127.0.0.1:17096:8091" in px and
          "127.0.0.1:17097:50088" in j(aux[0]) and
          "127.0.0.1:17098:50088" in j(aux[1]))
    check("ro-mounts-release-json",
          ":/srv/p26:ro" in fx and ":/srv/p26:ro" in px and
          "/run/p26-release.json:ro" in fx and "P26_RELEASE_FILE" in fx)
    check("upstream-50078-not-50117",
          "http://dataset:50078" in px and "50117" not in px)
    check("no-platform-jwt-fixture-proxy",
          "JWT" not in fx.upper().replace("P26_HTTP_ADMIN_TOKEN", "") or
          "OPENFOUNDRY_SMOKE_JWT" not in fx)
    check("aux-one-candidate-image",
          aux[0][-1] == aux[1][-1] and
          aux[0][-1].startswith("sha256:f23f6916"))
    check("aux-env-inmemory-shape",
          "DATASET_SERVICE_URL=http://p26proxy:8091" in j(aux[0]) and
          "fixture,p26fixture" in j(aux[0]))
    check("precheck-halt-never-rm",
          any("NEVER rm" in j(p) if isinstance(p, list) else "NEVER rm" in p
              for p in hp["prechecks_before_any_bind"]))
    check("ledger-typed-schema",
          set(hp["typed_cid_ledger_schema"]["per_resource"]) >=
          {"cid", "name", "image_full_id", "network", "host_port",
           "started_at"})
    sp = DP.switch_plan()
    up = sp["steps"][0]["argv"]
    check("switch-up-flags",
          "--no-deps" in up and "--no-build" in up and
          "--pull=never" in up and "--env-file" in up and
          up[up.index("--project-name") + 1] == "openfoundry-platform-p1-"
          "q3v9" and "dataset" == up[-1])
    check("switch-phases",
          sp["phases"] == ["switch-dataset", "switch-connector"])
    check("w8-gate-required-not-historical",
          any("W8" in p and "current_core_preservation" in p
              for p in sp["required_before_first_switch"]))
    check("rollback-manual-only",
          any("NEVER auto-rollback" in str(r)
              for r in sp["rollback_prepared_manual_only"]))
    ov = DP.override_yaml()
    body = "\n".join(ln for ln in ov.splitlines()
                     if not ln.strip().startswith("#"))
    check("override-image-only",
          "image: sha256:4217c402" in ov and "image: sha256:f23f6916" in ov
          and "volume" not in body and "ports" not in body and
          "environment" not in body and
          body.count("image:") == 2)
    c = DP.candidates()
    check("candidates-pin-S-and-resume-verified",
          c["frozen_S"] == DP.S and
          all(v["verified"].startswith("RESUME-VERIFIED") for v in
              c["candidates"].values()))
    e = DP.env_plan()
    check("env-metadata-only",
          e["metadata_only_no_values"] and
          all(f["mode"] == "0600" and not any("=" in k for k in
              f["key_names"]) for f in e["files"]))

    npass = sum(1 for _, ok, _ in RESULTS if ok)
    print(f"[deploy] {npass}/{len(RESULTS)} PASS")
    return 0 if npass == len(RESULTS) else 1


if __name__ == "__main__":
    sys.exit(main())

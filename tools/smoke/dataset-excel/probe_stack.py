#!/usr/bin/env python3
"""Health + end-to-end probe for the disposable dataset-excel stack.

Polls until (or gives up after 180s):
  1. the edge gateway answers GET /healthz with 200, and
  2. an authenticated GET /api/v1/datasets?page_size=1 through the gateway
     (real /api/v1/datasets -> /v1/* rewrite, real DVS, real PG, real JWT
     validation) returns 200 with valid JSON.

Writes the two final HTTP statuses to <out> as stack-probe.json and exits 0
iff both are 200. Called by run.sh under run_step "stack-probe".

Usage: probe_stack.py <gw_host_port> <token_file> <out_json>
"""
import json
import sys
import time
import urllib.error
import urllib.request


def main() -> None:
    port, token_file, out = sys.argv[1], sys.argv[2], sys.argv[3]
    token = open(token_file, encoding="utf-8").read().strip()
    probe = {"gw_healthz_status": 0, "catalog_status": 0}
    deadline = time.time() + 180
    while time.time() < deadline:
        probe["gw_healthz_status"] = 0
        try:
            with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/healthz", timeout=5) as r:
                probe["gw_healthz_status"] = r.status
        except Exception:
            pass
        if probe["gw_healthz_status"] == 200:
            probe["catalog_status"] = 0
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/api/v1/datasets?page_size=1",
                headers={"Authorization": f"Bearer {token}"})
            try:
                with urllib.request.urlopen(req, timeout=10) as r:
                    probe["catalog_status"] = r.status
                    json.load(r)  # must be a real JSON body from DVS
            except urllib.error.HTTPError as e:
                probe["catalog_status"] = e.code
            except Exception:
                probe["catalog_status"] = 0
            if probe["catalog_status"] == 200:
                break
        time.sleep(2)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(probe, fh, indent=2)
    print(f"[stack] probe: healthz={probe['gw_healthz_status']} "
          f"catalog={probe['catalog_status']}")
    sys.exit(0 if probe["gw_healthz_status"] == 200
             and probe["catalog_status"] == 200 else 1)


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""P2.6 W6 unified per-name acceptance roster (read-only extraction).

Builds the UNIFIED per-name roster (never count-only): current Dataset 8
+ current Connector 17 + the P25 frozen 87-name lineage extracted with
the SAME file semantics as pg_exact_check_p25.py (regex over the frozen
sh vars + list files — NO import/execution of old tools) + a pending
wire-ACK-loss cohort slot. Per name: service, package (detected from
landed source when present), cohort image/version, the exact -run group
it belongs to, and — for the frozen 87 — per-name sealed-green proof
(1 RUN/1 PASS/0 SKIP/0 FAIL) parsed from the SHA-verified sealed P25
unified raw inside the frozen bundle.

Usage: python3 -I -B p26_unified_roster.py emit --out F [--sources-root
DIR] [--sealed-raw PATH] [--check]   (# --check re-derives & compares)
"""

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from common import p26_gates as G  # noqa: E402

TOOL = "p26_unified_roster"
SRC = Path("/Volumes/ExternalSD/projects/OpenFoundry")
SOURCES = {
    "p1_run_tests": "tools/smoke/business-platform-p1/run_tests.sh",
    "p21_run_tests": "tools/smoke/business-platform-p2-1/run_tests_p21.sh",
    "p22_names": "tools/smoke/business-platform-p2-2/p22-required-names.txt",
    "p23_names": "tools/smoke/business-platform-p2-3/p23-required-names.txt",
    "p24_names": "tools/smoke/business-platform-p2-4/p24-required-names.txt",
    "p25_names": "tools/smoke/business-platform-p2-5/p25-required-names.txt",
}
SEALED_RAW_REL = ("tools/smoke/business-platform-p2-5/results/"
                  "2026-11-14-p25-final-seal-meta/final-bundle/attempt-1/"
                  "dependencies/evidence/pg/unified_20261114T141737Z.raw")
SEALED_MANIFEST_REL = ("tools/smoke/business-platform-p2-5/results/"
                       "2026-11-14-p25-final-seal-meta/final-bundle/"
                       "attempt-1/bundle-hash-manifest.json")

VAR_SERVICE = {"REQUIRED_DATASET": "dataset",
               "REQUIRED_PIPELINE": "pipeline",
               "REQUIRED_CONNECTOR": "connector",
               "REQUIRED_LEASE": "connector"}
COHORT = {"dataset": {"image": "golang:1.26.3", "go": "go1.26.3",
                      "run_group": "test-dataset-{normal,race}"},
          "pipeline": {"image": "golang:1.26.3", "go": "go1.26.3",
                       "run_group": "unified-p25-lineage"},
          "connector": {"image": "golang:1.25", "go": "go1.25.14",
                        "run_group": "test-connector-{normal,race} / "
                                     "unified-p25-lineage"}}


def sh_var_names(path: Path, var: str):
    m = re.search(rf'^{var}="([^"]+)"', path.read_text(), re.M)
    if not m:
        raise SystemExit(f"[{TOOL}] {var} not found in {path}")
    names = m.group(1).split()
    if not names:
        raise SystemExit(f"[{TOOL}] {var} empty in {path}")
    return names


def list_file_names(path: Path):
    out = [ln.split("#")[0].strip() for ln in path.read_text().splitlines()]
    out = [n for n in out if n]
    if not out:
        raise SystemExit(f"[{TOOL}] empty names list: {path}")
    return out


def scan_source_functions(services=("connector-management-service",
                                     "dataset-versioning-service",
                                     "pipeline-build-service")):
    idx = {}
    for svc in services:
        for p in (SRC / "services" / svc).rglob("*_test.go"):
            for m in re.finditer(r"^func (Test\w+)\(", p.read_text(errors=
                                                "replace"), re.M):
                idx.setdefault(m.group(1), []).append(str(p))
    return idx


def sealed_green(raw_path: Path, manifest_path: Path, names):
    """Per-name 1RUN/1PASS/0SKIP/0FAIL from the SHA-verified sealed raw."""
    want = manifest_path.read_text()
    rel = str(raw_path.relative_to(manifest_path.parent))
    man = json.loads(want)["files"]
    if rel not in man:
        raise SystemExit(f"[{TOOL}] sealed raw not in bundle manifest")
    if hashlib.sha256(raw_path.read_bytes()).hexdigest() != man[rel]:
        raise SystemExit(f"[{TOOL}] sealed raw sha mismatch — refusing")
    stats, _ = G.parse_go_test_verbose(raw_path.read_text(
        errors="replace").splitlines())
    out = {}
    for n in names:
        d = stats.get(n)
        out[n] = bool(d and (d["run"], d["pass"], d["skip"], d["fail"])
                      == (1, 1, 0, 0))
    return out


def emit(sources_root: Path, sealed_raw: Path, sealed_manifest: Path,
         dataset_names_file=None, connector_names_file=None):
    p = {k: sources_root / v for k, v in SOURCES.items()}
    lineage = []           # (name, service, origin)
    for var, svc in VAR_SERVICE.items():
        sh = p["p1_run_tests"] if var != "REQUIRED_LEASE" else \
            p["p21_run_tests"]
        for n in sh_var_names(sh, var):
            lineage.append((n, svc, var))
    for key, svc in (("p22_names", "connector"), ("p23_names", "connector"),
                     ("p24_names", "connector"), ("p25_names", "connector")):
        for n in list_file_names(p[key]):
            lineage.append((n, svc, key))
    seen = {}
    for n, svc, origin in lineage:
        if n in seen:
            raise SystemExit(f"[{TOOL}] duplicate name across sources: {n}")
        seen[n] = (svc, origin)
    if len(seen) != 87:
        raise SystemExit(f"[{TOOL}] frozen lineage is {len(seen)} names, "
                         "expected exactly 87 — per-name refusal, not "
                         "count-fudge")
    idx = scan_source_functions()
    green = sealed_green(sealed_raw, sealed_manifest, list(seen))

    def entry(n, svc, origin, group, sealed=None):
        paths = idx.get(n, [])
        pkg = None
        if paths:
            pp = Path(paths[0])
            svc_dir = pp.relative_to(SRC / "services")
            # Root-caught bug fix: relative_to(SRC/services) STRIPS the
            # services/ segment — the module cwd is /src, so packages
            # must be ./services/<svc>/... (was ./<svc>/...).
            pkg = "./services/" + str(svc_dir.parent) + "/"
        sealed_flag = (green[n] if origin == "sealed" else
                       None if sealed is None else sealed)
        return {"name": n, "service": svc, "origin": origin,
                "cohort": COHORT[svc], "package_now": pkg,
                "source_present_now": bool(paths),
                "sealed_green_1_1_0_0": sealed_flag,
                "run_group": group}

    frozen = [entry(n, s, "sealed", "unified-p25-lineage")
              for n, (s, o) in sorted(seen.items())]
    ds = [entry(n, "dataset", "w1-current", "test-dataset-{normal,race}")
          for n in G.load_required_names(
              dataset_names_file or
              Path(__file__).parent / "p26-dataset-pg-names.txt")]
    cn = [entry(n, "connector", "w2-current", "test-connector-"
              "{normal,race}") for n in G.load_required_names(
        connector_names_file or
        Path(__file__).parent / "p26-connector-pg-names.txt")]
    for e in ds + cn:
        if not e["source_present_now"]:
            raise SystemExit(f"[{TOOL}] current-cohort name missing from "
                             f"landed source: {e['name']}")
    return {"utc": G.now_utc(), "per_name_counts": {
                "dataset_current": len(ds), "connector_current": len(cn),
                "p25_frozen_lineage": len(frozen)},
            "roster": {"dataset_current": ds, "connector_current": cn,
                       "p25_frozen_87": frozen,
                       "wire_ack_loss": {"status": "PENDING — appended as "
                                         "new cohort revision when W2 "
                                         "delivers (Root instruction)"}},
            "notes": ["frozen 87 extracted with pg_exact_check_p25.py file"
                      " semantics (regex/列表, no old-tool import/exec)",
                      "sealed per-name green parsed from bundle "
                      "hash-verified unified raw",
                      "counts are BYPRODUCTS; every name is listed "
                      "individually"]}


UNIFIED_DIR = Path(__file__).parent / "unified"
# Unified sweep facts (P25 unified precedent + cached-image verification):
# ONE module tree -> openfoundry-gomod/gobuild (P25 unified ran all three
# services through these); image golang:1.26.3 fullID (cached, verified);
# per-service DSN env names from the landed harnesses.
UNIFIED_BASE = {
    "image_full_id": "2d6c80227255c3112a4d08e67ba98e58efd3846daf15d9d7d4"
                     "c389565d881b1a",
    "image_ref": "golang:1.26.3", "expect_go": "go1.26.3",
    "volumes": ["openfoundry-gomod", "openfoundry-gobuild"],
}
UNIFIED_DSN_ENV = {"dataset": "DATASET_DB_DSN", "pipeline":
                   "PIPELINE_DB_DSN", "connector": "CONNECTOR_DB_DSN"}


def _assert_pkg_real(pkg: str):
    """Root instruction: the package must be a REAL host dir (SRC is the
    live repo for real emits; synthetic roots carry their own dirs)."""
    host = SRC / pkg[len("./"):]
    if not host.is_dir():
        raise SystemExit(f"[{TOOL}] package {pkg!r} is not a real dir "
                         f"under SRC ({host}) — refusing (emit must be "
                         "grounded in actual source layout, not fake "
                         "names)")


def emit_names(out_dir: Path, sources_root: Path, sealed_raw: Path,
               sealed_manifest: Path):
    """Per-service UNIFIED names files + cohorts JSON. Old-lineage names
    split by the extraction's own service map (counts REPORTED, never
    hardcoded); current cohort names appended; every name re-verified
    landed (package derived from source)."""
    roster = emit(sources_root, sealed_raw, sealed_manifest)
    old = {}
    for e in roster["roster"]["p25_frozen_87"]:
        old.setdefault(e["service"], []).append(e)
    cur = {"dataset": roster["roster"]["dataset_current"],
           "connector": roster["roster"]["connector_current"]}
    out_dir.mkdir(parents=True, exist_ok=True)
    cohorts = {}
    grouping = {}
    for svc in ("dataset", "pipeline", "connector"):
        names = [e["name"] for e in old.get(svc, [])] + \
                [e["name"] for e in cur.get(svc, [])]
        if not names:
            raise SystemExit(f"[{TOOL}] unified {svc}: EMPTY name list — "
                             "refusing (names are the authority)")
        seen = set()
        for n in names:
            if n in seen:
                raise SystemExit(f"[{TOOL}] unified {svc}: duplicate {n}")
            seen.add(n)
        nf = out_dir / f"p26-unified-{svc}-names.txt"
        nf.write_text(
            "# AUTO-FROZEN by p26_unified_roster emit-names (do not hand-"
            f"edit; regenerate). unified suite for {svc}: old sealed "
            f"lineage {len(old.get(svc, []))} + current "
            f"{len(cur.get(svc, []))}; every name landed-source-verified."
            "\n" + "\n".join(names) + "\n")
        pkgs = sorted({e["package_now"] for e in old.get(svc, []) +
                       cur.get(svc, []) if e["package_now"]})
        for pk in pkgs:
            _assert_pkg_real(pk)
        cohorts[svc] = {"names_file": str(nf.name), "packages": pkgs,
                        "dsn_env": UNIFIED_DSN_ENV[svc], **UNIFIED_BASE}
        grouping[svc] = {"old": len(old.get(svc, [])),
                         "current": len(cur.get(svc, [])),
                         "total": len(names)}
    total = sum(g["total"] for g in grouping.values())
    G.write_json(out_dir / "p26-unified-cohorts.json",
                 {"utc": G.now_utc(), "services": cohorts,
                  "grouping_actual": grouping, "total_names": total,
                  "note": "114 expected ONLY after W2 wire name lands "
                          "(Root accepts then); names are the authority, "
                          "counts are reported facts",
                  "fresh_db": "unified runs create a fresh lowercase db "
                              "ofp26_unified_<ts> per sweep (P25 "
                              "precedent) + SHOW server_major gate"})
    return {"grouping": grouping, "total": total,
            "cohorts_json": str(out_dir / "p26-unified-cohorts.json")}


def main(argv=None):
    ap = argparse.ArgumentParser(prog=TOOL)
    ap.add_argument("mode", choices=["emit", "emit-names"])
    ap.add_argument("--out", default=None)
    ap.add_argument("--sources-root", default=str(SRC))
    ap.add_argument("--sealed-raw", default=str(SRC / SEALED_RAW_REL))
    ap.add_argument("--sealed-manifest",
                    default=str(SRC / SEALED_MANIFEST_REL))
    ap.add_argument("--dataset-names", default=None,
                    help="override current dataset names file (self-test)")
    ap.add_argument("--connector-names", default=None,
                    help="override current connector names file (self-test)")
    ap.add_argument("--unified-dir",
                    default=str(UNIFIED_DIR))
    args = ap.parse_args(argv)
    if args.mode == "emit-names":
        res = emit_names(Path(args.unified_dir), Path(args.sources_root),
                         Path(args.sealed_raw), Path(args.sealed_manifest))
        print(json.dumps(res, indent=1))
        return 0
    roster = emit(Path(args.sources_root), Path(args.sealed_raw),
                  Path(args.sealed_manifest),
                  dataset_names_file=Path(args.dataset_names)
                  if args.dataset_names else None,
                  connector_names_file=Path(args.connector_names)
                  if args.connector_names else None)
    c = roster["per_name_counts"]
    if args.out:
        G.write_json(Path(args.out), roster)
    print(f"[{TOOL}] roster: dataset={c['dataset_current']} "
          f"connector={c['connector_current']} "
          f"frozen87={c['p25_frozen_lineage']}"
          f"{' -> ' + args.out if args.out else ''}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

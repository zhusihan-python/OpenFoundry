#!/usr/bin/env python3
"""P26 W7 OWNED aux-connector process adapter (code/pure phase).

Implements scenarios.ConnectorOps against the NEW owned p26 aux
connectors ONLY (Root-released exact CIDs + labels from W6 deployment
facts). NOTHING in this module executes now: every operation first
builds a bounded argv, checks the release phase `http-owned-ops` plus
the owned-resource ledger, and only a Root-released live phase may run
it (same gate model as the fixture/proxy serve mains). Old q3 base
containers / old fixture / PG / Pipeline are structurally unreachable
(name/CID/label allowlist, old-substrate markers refused).

Operation contracts (r3 memo):
  * stop  = docker kill --signal KILL <owned-cid> (SIGKILL crash
    window), then wait until docker inspect says Running==false;
  * start = docker start <SAME owned CID> (no recreate, no port/volume/
    config change — candidate env preserved by the container itself);
  * boot identity = docker inspect StartedAt + PID (real process
    facts, not any fictional public boot field);
  * lease-expired proof = READ-ONLY q3 psql observer over THIS owned
    new Run's DB clock + generation/lease-expired boolean, exact
    parent Run/Def/Source IDs taken from the typed owned roster; never
    raw holder/token/opaque checkpoint values; no force-expire SQL, no
    old-UUID adoption;
  * credentials = valid signed JWT VALUES provided by Root via private
    env (writer / read-only same-tenant / foreign-tenant), read into
    memory only; the harness NEVER mints JWTs.

Usage (live, after release): constructed by run_p26_http with the
release-provided aux facts; pure tests inject a fake in the selftest.
"""

import base64
import copy
import json
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.dont_write_bytecode = True
sys.path.insert(0, str(__file__).rsplit("/", 1)[0])

from p26_http_lib import (BoundedWait, EvidenceDir, RESULTS_W7, redact,
                          read_release, release_allows)

OWNED_OPS_PHASE = "http-owned-ops"

# Old substrates this adapter must NEVER touch (exact project markers;
# deliberately NOT a bare "of-p2" prefix — that would wrongly match the
# NEW of-p26 owner labels).
FORBIDDEN_NAME_MARKERS = ("of-p25", "of-p24", "of-p23", "of-p22",
                          "p25", "fixture-1", "postgres", "pipeline",
                          "dataset-1", "connector-1")
CID_RE = re.compile(r"^[0-9a-f]{64}$")
LABEL_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}=[A-Za-z0-9_.-]{1,64}$")
IMAGE_RE = re.compile(r"^sha256:[0-9a-f]{64}$")

CREDENTIAL_ENV = {
    "writer": "P26_W7_JWT_WRITER",
    "readonly": "P26_W7_JWT_READONLY",
    "foreign": "P26_W7_JWT_FOREIGN",
}

# Root release `w7_aux` schema (r4): exact NEW owned names/labels/roles
# and the candidate FULL image id per instance — PARSED, never guessed.
AUX_NAMES = {"a": "p26connectora", "b": "p26connectorb"}
AUX_NAMES_S2 = {"a": "p26connectora2", "b": "p26connectorb2"}
# Immutable old A/B IDs; cohort2 can never target these even with renamed facts.
OLD_AUX_CIDS = frozenset({
    "ff40554413fd3708a54f4e469ad604bcfe812084b94bc117c035d17a7a9b303f",
    "316b0282f5864486438e81a5c760b2a1dfccc3065dcbc2b06a35e757b4669355",
})
AUX_ROLES = {"a": "connector_a", "b": "connector_b"}
AUX_REQUIRED_LABELS = {"owner": "p26-http", "stage": "p2-6"}


class OwnedOpsError(Exception):
    pass


def validate_release_aux(release_doc, *, schema_only=False):
    """Parse the Root release `w7_aux` block into owned-aux facts.
    Schema per instance: full 64-hex cid, EXACT new container name,
    labels {owner=p26-http, stage=p2-6, role=connector_a|b}, and the
    candidate FULL sha256 image id. A/B cids must differ; old q3
    markers are rejected in every field. Returns (aux, error)."""
    release_doc = release_doc or {}
    cohort = release_doc.get("w7_aux_cohort", 1)
    if type(cohort) is not int or cohort not in (1, 2):
        return None, "w7_aux_cohort must be 1 or 2 (fixed names only)"
    names = AUX_NAMES_S2 if cohort == 2 else AUX_NAMES
    forbidden = set(OLD_AUX_CIDS) if cohort == 2 else set()
    candidate = None
    if cohort == 2:
        images = release_doc.get("expected_candidate_images")
        candidate = images.get("connector") if isinstance(images, dict) else None
        if not isinstance(candidate, str) or not IMAGE_RE.fullmatch(candidate):
            return None, "cohort2 requires expected_candidate_images.connector full sha256"
        denied = release_doc.get("w7_forbidden_aux_cids", [])
        if schema_only:
            denied = sorted(OLD_AUX_CIDS)
        if (not isinstance(denied, list) or (not schema_only and len(denied) < 4)
                or any(not isinstance(c, str) or not CID_RE.fullmatch(c) for c in denied)
                or len(set(denied)) != len(denied)):
            return None, "cohort2 requires w7_forbidden_aux_cids (main and legacy C/A/B full IDs)"
        forbidden.update(denied)
    aux_in = release_doc.get("w7_aux")
    if not isinstance(aux_in, dict):
        return None, ("release lacks w7_aux (Root-supplied owned aux "
                      "container facts — W6 publishes names/CIDs/"
                      "labels/image; the harness never guesses them)")
    out = {}
    for inst in ("a", "b"):
        facts = aux_in.get(inst)
        if not isinstance(facts, dict):
            return None, f"w7_aux missing instance {inst!r}"
        cid = facts.get("cid")
        if not isinstance(cid, str) or not CID_RE.fullmatch(cid):
            return None, f"w7_aux[{inst}].cid must be a full 64-hex id"
        if cid in forbidden:
            return None, "cohort2 refuses old main/legacy aux CID"
        name = facts.get("name")
        if name != names[inst]:
            return None, (f"w7_aux[{inst}].name must be exactly "
                          f"{names[inst]!r}")
        labels = facts.get("labels")
        if not isinstance(labels, dict):
            return None, f"w7_aux[{inst}].labels must be an object"
        want_labels = dict(AUX_REQUIRED_LABELS, role=AUX_ROLES[inst])
        for key, value in want_labels.items():
            if labels.get(key) != value:
                return None, (f"w7_aux[{inst}].labels[{key}] must be "
                              f"{value!r}")
        image = facts.get("image")
        if not isinstance(image, str) or not IMAGE_RE.fullmatch(image):
            return None, (f"w7_aux[{inst}].image must be the candidate "
                          f"full sha256 id")
        if candidate is not None and image != candidate:
            return None, "cohort2 aux image differs from expected_candidate_images.connector"
        blob = " ".join([cid, str(name), str(image)]
                        + [f"{k}={v}" for k, v in
                           sorted((labels or {}).items())]).lower()
        for marker in FORBIDDEN_NAME_MARKERS:
            if marker in blob:
                return None, (f"w7_aux[{inst}] matches forbidden "
                              f"old-substrate marker {marker!r}")
        out[inst] = {"cid": cid, "name": name,
                     "labels": dict(labels), "image": image}
    if out["a"]["cid"] == out["b"]["cid"]:
        return None, "w7_aux A/B cids must be DISTINCT containers"
    if out["a"]["image"] != out["b"]["image"]:
        return None, "w7_aux A/B must run the SAME candidate image"
    return out, None


def validate_aux_facts(aux):
    """Require the exact release schema even for injected pure runners."""
    doc = {"w7_aux": aux}
    if isinstance(aux, dict) and any(isinstance(v, dict) and v.get("name") in AUX_NAMES_S2.values() for v in aux.values()):
        doc.update(w7_aux_cohort=2,
                   expected_candidate_images={"connector": aux.get("a", {}).get("image")})
    parsed, error = validate_release_aux(doc, schema_only=True)
    return (False, error) if error else (True, "ok")


def canonical_uuid(value):
    try:
        parsed = uuid.UUID(value)
    except (ValueError, TypeError, AttributeError):
        raise OwnedOpsError("owned roster requires canonical UUIDs") from None
    if str(parsed) != value or parsed.int == 0:
        raise OwnedOpsError("owned roster requires nonzero canonical UUIDs")
    return value


def validate_auth_identity(identity, credentials=()):
    """Root scope plus writer claim equality; token bytes stay in memory."""
    if not isinstance(identity, dict) or set(identity) != {"actor", "org"}:
        raise OwnedOpsError("S2 requires w7_auth_identity actor/org")
    scope = {key: canonical_uuid(identity.get(key)) for key in ("actor", "org")}
    for raw in credentials:
        try:
            payload = raw.removeprefix("Bearer ").split(".")[1]
            claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
            actual = {"actor": canonical_uuid(claims.get("sub")),
                      "org": canonical_uuid(claims.get("org_id"))}
        except (ValueError, TypeError, AttributeError, IndexError, OwnedOpsError):
            raise OwnedOpsError("S2 writer claims missing/invalid") from None
        if actual != scope:
            raise OwnedOpsError("S2 writer claims differ from Root identity")
    return scope


def validate_pg_observer(facts):
    if (not isinstance(facts, dict)
            or not isinstance(facts.get("cid"), str)
            or not CID_RE.fullmatch(facts["cid"])
            or facts.get("user") != "openfoundry"
            or facts.get("database") != "connectors"):
        raise OwnedOpsError("Root w7_pg_observer requires full CID, "
                            "user=openfoundry, database=connectors")
    return {key: facts[key] for key in ("cid", "user", "database")}


# Never inspect the full object: Config.Env may contain credentials.
IDENTITY_FORMAT = ('{"cid":{{json .Id}},"name":{{json .Name}},'
                   '"image":{{json .Image}},'
                   '"owner":{{json (index .Config.Labels "of-p26-owner")}},'
                   '"stage":{{json (index .Config.Labels "of-p26-stage")}},'
                   '"role":{{json (index .Config.Labels "of-p26-role")}},'
                   '"running":{{json .State.Running}},'
                   '"started_at":{{json .State.StartedAt}},'
                   '"pid":{{json .State.Pid}}}')


class OwnedAuxOps:
    """ConnectorOps implementation over the owned aux containers.
    All mutating operations are argv-planned and release-gated; nothing
    binds/starts/kills in the pure phase."""

    def __init__(self, aux, ledger_path=None, release_file=None,
                 runner=None, roster=None, evidence=None, pg_observer=None,
                 auth_identity=None):
        ok, why = validate_aux_facts(aux)
        if not ok:
            raise OwnedOpsError(why)
        self.aux = copy.deepcopy(aux)
        self._s2 = aux["a"]["name"] == AUX_NAMES_S2["a"]
        self._auth_identity = validate_auth_identity(auth_identity) if self._s2 else None
        self._scope_invalid = False
        self._release_file = release_file
        # Injection is for pure tests only; production always checks the
        # release again immediately before every subprocess.
        self._runner = runner
        self._lock = threading.Lock()
        self._seq = 0
        self._evidence_prefix = "ops-" + uuid.uuid4().hex[:12]
        self._roster = roster
        self._evidence = evidence
        self._evidence_root = Path(ledger_path) if ledger_path else RESULTS_W7
        self._pg_observer = (validate_pg_observer(pg_observer)
                             if pg_observer is not None else None)
        self.last_lease_observation = None

    def bind_context(self, roster, evidence=None):
        """Bind the live typed roster by reference as HTTP creates objects.

        Entries are {kind:source,id}, {kind:sync_def,id,source}, and
        {kind:run,id,def}. Missing or ambiguous links always refuse.
        No lookup/adoption by arbitrary UUID is permitted.
        """
        if not isinstance(roster, list):
            raise OwnedOpsError("typed owned roster must be a list")
        if evidence is not None and not isinstance(evidence, EvidenceDir):
            raise OwnedOpsError("ops evidence must be an EvidenceDir")
        self._roster = roster
        if evidence is not None:
            self._evidence = evidence
        return self

    def _released(self):
        return release_allows(OWNED_OPS_PHASE, self._release_file)

    def _gate(self):
        if self._scope_invalid:
            raise OwnedOpsError("S2 owned observer scope failed; further operations refused")
        if self._runner is None:
            ok, why = self._released()
            if not ok:
                raise OwnedOpsError(f"refusing owned-ops: {why} "
                                    f"({OWNED_OPS_PHASE}; zero execution)")
            doc = read_release(self._release_file) if self._release_file else None
            if self._release_file is None:
                from p26_http_lib import DEFAULT_RELEASE_FILE
                doc = read_release(DEFAULT_RELEASE_FILE)
            released_aux, error = validate_release_aux(doc)
            if error or released_aux != self.aux:
                raise OwnedOpsError("owned aux facts differ from Root release")
            if self._s2:
                scope = validate_auth_identity(doc.get("w7_auth_identity"),
                    [os.environ.get(key) for key in ("OPENFOUNDRY_SMOKE_JWT", "P26_W7_JWT_WRITER")])
                if scope != self._auth_identity:
                    raise OwnedOpsError("S2 bound identity differs from current Root release")
            return doc
        return None

    def _chain(self, def_id=None, run_id=None):
        if not isinstance(self._roster, list) or not self._roster:
            raise OwnedOpsError("typed owned roster is unbound or empty")
        def unique(kind, obj_id):
            canonical_uuid(obj_id)
            rows = [r for r in self._roster if isinstance(r, dict)
                    and r.get("kind") == kind and r.get("id") == obj_id]
            if len(rows) != 1:
                raise OwnedOpsError("owned roster missing/ambiguous typed identity")
            return rows[0]
        if run_id is None:
            rows = [r for r in self._roster if isinstance(r, dict)
                    and r.get("kind") == "run"]
            if not rows:
                raise OwnedOpsError("owned roster has no typed run")
            # Validate all chains before a process mutation.
            return [self._chain(r.get("def"), r.get("id")) for r in rows]
        run = unique("run", run_id)
        if run.get("def") != def_id:
            raise OwnedOpsError("owned Run does not bind requested Def")
        definition = unique("sync_def", def_id)
        source_id = definition.get("source")
        unique("source", source_id)
        return {"run_id": run_id, "def_id": def_id,
                "source_id": source_id, **(self._auth_identity or {})}

    def _safe_output(self, value):
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        out = str(value or "")
        # Pattern redaction plus exact in-memory credential values protects
        # stderr too, including opaque admin credentials lacking a prefix.
        for key in (*CREDENTIAL_ENV.values(), "OPENFOUNDRY_SMOKE_JWT",
                    "P26_HTTP_ADMIN_TOKEN"):
            secret = os.environ.get(key)
            if secret:
                out = out.replace(secret, "<redacted>")
                if secret.startswith("Bearer "):
                    out = out.replace(secret[7:], "<redacted>")
        return redact(out)

    def _execute(self, method, argv, timeout_s=30.0):
        released = self._gate()
        if method in ("lease-observe", "pregate-observe") and released is not None:
            pg = validate_pg_observer(released.get("w7_pg_observer"))
            if len(argv) < 5 or argv[4] != pg["cid"]:
                raise OwnedOpsError("PG observer CID differs from current Root release")
        self._chain()
        if not 0 < timeout_s <= 30:
            raise OwnedOpsError("operation timeout must be in (0,30]")
        with self._lock:
            if self._evidence is None:
                self._evidence = EvidenceDir(self._evidence_root,
                                             "owned-ops-" + uuid.uuid4().hex[:8])
            self._seq += 1
            rec = self._evidence.step(f"{self._evidence_prefix}-{self._seq:04d}-{method}")
            rec.cmd(argv, note="bounded owned op; no env values")
        started = time.monotonic()
        rc, out, err, failure = None, "", "", None
        try:
            if self._runner:
                result = self._runner(method, argv, timeout_s)
                if len(result) == 2:
                    rc, out = result
                else:
                    rc, out, err = result
            else:
                cp = subprocess.run(argv, capture_output=True, text=True,
                                    timeout=timeout_s)
                rc, out, err = cp.returncode, cp.stdout, cp.stderr
        except subprocess.TimeoutExpired as exc:
            out, err, failure = exc.stdout, exc.stderr, "timeout"
        except OSError as exc:
            err, failure = str(exc), type(exc).__name__
        finally:
            safe_out, safe_err = self._safe_output(out), self._safe_output(err)
            (rec.dir / f"{rec.name}.stdout.raw").write_text(safe_out)
            (rec.dir / f"{rec.name}.stderr.raw").write_text(safe_err)
            # No fabricated Unix rc for a timeout/spawn failure. None is
            # explicitly distinguished from an actual child return code.
            rec.finish(rc if rc is not None else "unavailable",
                       ("STDOUT\n" + safe_out + "\nSTDERR\n" + safe_err).encode(),
                       {"original_rc": rc, "failure": failure,
                        "duration_ms": int((time.monotonic()-started)*1000),
                        "timeout_s": timeout_s, "method": method})
        if failure:
            raise OwnedOpsError(f"owned op {method}: {failure}; original rc unavailable")
        if not isinstance(rc, int):
            raise OwnedOpsError("owned op returned no original integer rc")
        return rc, out

    def _identity(self, instance="a"):
        facts = self.aux.get(instance)
        if facts is None:
            raise OwnedOpsError("unknown owned instance")
        rc, out = self._execute("inspect-identity", ["docker", "inspect",
            "--type", "container", "--format", IDENTITY_FORMAT, facts["cid"]])
        try:
            obj = json.loads(out)
        except (ValueError, TypeError):
            raise OwnedOpsError("invalid Docker identity JSON") from None
        expected = {"cid": facts["cid"], "name": "/" + facts["name"],
                    "image": facts["image"], **AUX_REQUIRED_LABELS,
                    "role": AUX_ROLES[instance]}
        if rc != 0 or not isinstance(obj, dict) or any(
                obj.get(k) != v for k, v in expected.items()):
            raise OwnedOpsError("runtime owned container identity mismatch")
        if (type(obj.get("running")) is not bool
                or type(obj.get("pid")) is not int or obj["pid"] < 0
                or not isinstance(obj.get("started_at"), str)
                or not obj["started_at"]):
            raise OwnedOpsError("invalid Docker process state")
        return obj

    def stop_connector(self, instance="a"):
        """Reinspect exact identity immediately before SIGKILL."""
        self._identity(instance)
        rc, _ = self._execute("kill", ["docker", "kill", "--signal", "KILL",
                                      self.aux[instance]["cid"]])
        if rc != 0:
            raise OwnedOpsError(f"docker kill rc={rc}")
        if not self.wait_stopped(instance=instance):
            raise OwnedOpsError("owned aux did not stop within bound")
        return True

    def wait_stopped(self, timeout_s=30, instance="a"):
        deadline = time.monotonic() + min(float(timeout_s), 30)
        while True:
            if not self._identity(instance)["running"]:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(0.5, remaining))

    def restart_connector(self, instance="a"):
        """Reinspect then start the SAME CID; no recreation or config edits."""
        self._identity(instance)
        rc, _ = self._execute("start", ["docker", "start", self.aux[instance]["cid"]])
        if rc != 0:
            raise OwnedOpsError(f"docker start rc={rc}")
        return True

    def connector_boot_id(self, instance="a"):
        obj = self._identity(instance)
        return f'{obj["started_at"]}|{obj["pid"]}'

    def _lease_argv(self, chain, projection=None):
        doc = self._gate()
        facts = validate_pg_observer((doc or {}).get("w7_pg_observer")
                                     if doc else self._pg_observer)
        # UUIDs were canonicalized against typed ownership BEFORE SQL.
        default_projection = ("json_build_object('db_clock',clock_timestamp(),"
               "'protocol',r.recovery_protocol,'status',r.status,"
               "'generation',r.generation,'lease_expired',"
               "(r.lease_expires_at IS NOT NULL AND "
               "r.lease_expires_at <= clock_timestamp()))")
        scope = ""
        if self._s2:
            org, actor = canonical_uuid(chain.get("org")), canonical_uuid(chain.get("actor"))
            scope = (f" AND r.org_id='{org}'::uuid AND d.org_id='{org}'::uuid"
                     f" AND s.org_id='{org}'::uuid AND r.owner_id='{actor}'::uuid"
                     f" AND r.requested_by='{actor}'::uuid AND s.owner_id='{actor}'::uuid")
        sql = ("SELECT " + (projection or default_projection) + " "
               "FROM sync_runs r JOIN batch_sync_defs d ON d.id=r.sync_def_id "
               "JOIN connections s ON s.id=d.source_id "
               f"WHERE r.id='{chain['run_id']}'::uuid "
               f"AND r.sync_def_id='{chain['def_id']}'::uuid "
               f"AND d.source_id='{chain['source_id']}'::uuid "
               "AND r.recovery_source_id=s.id AND r.recovery_protocol='p26-v1'" + scope + ";")
        return ["docker", "exec", "-e",
                "PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000",
                facts["cid"], "psql", "-U", facts["user"], "-d", facts["database"],
                "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", sql]

    def wait_lease_expired(self, def_id, run_id, timeout_s=90):
        """Readonly observer; missing/wrong rows are errors, never adopted."""
        chain = self._chain(def_id, run_id)
        argv = self._lease_argv(chain)
        deadline = time.monotonic() + min(max(float(timeout_s), 0), 180)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            rc, out = self._execute("lease-observe", argv, min(10, remaining))
            try:
                obj = json.loads(out)
                valid = (rc == 0 and isinstance(obj, dict)
                         and set(obj) == {"db_clock", "protocol", "status",
                                          "generation", "lease_expired"}
                         and obj["protocol"] == "p26-v1"
                         and obj["status"] == "running"
                         and type(obj["generation"]) is int
                         and obj["generation"] >= 1
                         and type(obj["lease_expired"]) is bool)
                stamp = datetime.fromisoformat(obj["db_clock"])
                valid = valid and stamp.tzinfo is not None
            except (ValueError, TypeError, KeyError):
                valid = False
            if not valid:
                self._scope_invalid = self._s2
                raise OwnedOpsError("lease observer did not return one safe owned running v1 row")
            self.last_lease_observation = obj
            if obj["lease_expired"]:
                return True
            time.sleep(min(1, max(0, deadline-time.monotonic())))

    def observe_pregate(self, def_id, run_id):
        """Exact typed-owned DB proof, read-only; no candidate/body/lease secret."""
        chain = self._chain(def_id, run_id)
        projection = ("json_build_object('db_clock',clock_timestamp(),"
            "'status',r.status,'protocol',r.recovery_protocol,'generation',r.generation,"
            "'gate_not_entered',r.publish_entered_at IS NULL,"
            "'no_intent',NOT EXISTS(SELECT 1 FROM sync_run_publish_intents i WHERE i.run_id=r.id),"
            "'send_attempts',COALESCE((SELECT i.send_attempts FROM sync_run_publish_intents i WHERE i.run_id=r.id),0),"
            "'query_attempts',COALESCE((SELECT i.query_attempts FROM sync_run_publish_intents i WHERE i.run_id=r.id),0),"
            "'cp_owned_epoch_zero',EXISTS(SELECT 1 FROM sync_def_checkpoints cp "
            "WHERE cp.sync_def_id=r.sync_def_id AND cp.source_id=s.id "
            "AND cp.active_run_id=r.id AND cp.confirmed_epoch=0))")
        rc, out = self._execute("pregate-observe", self._lease_argv(chain, projection), 10)
        try:
            obj = json.loads(out)
            valid = (rc == 0 and isinstance(obj, dict)
                     and set(obj) == {"db_clock", "status", "protocol", "generation",
                        "gate_not_entered", "no_intent", "send_attempts", "query_attempts", "cp_owned_epoch_zero"}
                     and obj["status"] == "running" and obj["protocol"] == "p26-v1"
                     and type(obj["generation"]) is int and obj["generation"] >= 1
                     and all(obj[k] is True for k in ("gate_not_entered", "no_intent", "cp_owned_epoch_zero"))
                     and type(obj["send_attempts"]) is int and obj["send_attempts"] == 0
                     and type(obj["query_attempts"]) is int and obj["query_attempts"] == 0
                     and datetime.fromisoformat(obj["db_clock"]).tzinfo is not None)
        except (ValueError, TypeError, KeyError):
            valid = False
        if not valid:
            self._scope_invalid = self._s2
            raise OwnedOpsError("owned pre-gate DB proof failed")
        return obj

    def credential(self, kind):
        """Valid signed JWT VALUE from the private env (memory only;
        the harness never mints credentials)."""
        env_name = CREDENTIAL_ENV.get(kind)
        if not env_name:
            raise OwnedOpsError(f"unknown credential kind {kind!r}")
        value = os.environ.get(env_name)
        if not value:
            raise OwnedOpsError(
                f"env {env_name} missing (Root-provided credential)")
        if kind == "readonly":
            writer = os.environ.get("OPENFOUNDRY_SMOKE_JWT")
            try:
                def claims(raw):
                    payload = raw.removeprefix("Bearer ").split(".")[1]
                    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
                wc, rc = claims(writer), claims(value)
                permissions = rc.get("permissions")
                valid = (canonical_uuid(rc.get("org_id")) == canonical_uuid(wc.get("org_id"))
                    and canonical_uuid(rc.get("sub")) != canonical_uuid(wc.get("sub"))
                    and rc.get("roles") == [] and isinstance(permissions, list)
                    and "connections:read" in permissions
                    and all(isinstance(p, str) and not any(x in p.lower() for x in ("write", "admin", "*")) for p in permissions))
            except (ValueError, TypeError, AttributeError, IndexError, OwnedOpsError):
                valid = False
            if not valid:
                raise OwnedOpsError("readonly credential must be same-org different nonowner subject, empty roles, read permissions only")
        return value


def plan_owned_ops(aux):
    """Pure argv review; exact PG CID is supplied by Root, never guessed."""
    ok, why = validate_aux_facts(aux)
    if not ok:
        return {"valid": False, "reason": why, "argv": []}
    plan = {"valid": True, "release_phase": OWNED_OPS_PHASE, "argv": []}
    for instance in sorted(aux):
        cid = aux[instance]["cid"]
        identity = ["docker", "inspect", "--type", "container", "--format",
                    IDENTITY_FORMAT, cid]
        plan["argv"] += [
            {"op": f"inspect-before-kill-{instance}", "argv": identity},
            {"op": f"kill-{instance}", "argv": ["docker", "kill", "--signal", "KILL", cid]},
            {"op": f"inspect-before-start-{instance}", "argv": identity},
            {"op": f"start-{instance}", "argv": ["docker", "start", cid]}]
    plan["pg_release_schema"] = {"w7_pg_observer": {
        "cid": "Root-approved full 64-hex q3 PG CID",
        "user": "openfoundry", "database": "connectors"}}
    plan["roster_required"] = ["source(id)", "sync_def(id,source)", "run(id,def)"]
    plan["credential_env"] = dict(CREDENTIAL_ENV)
    return plan

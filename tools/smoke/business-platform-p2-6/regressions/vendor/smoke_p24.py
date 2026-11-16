#!/usr/bin/env python3
"""P2.4 HTTP acceptance scenarios — review-tools-round1 fixes applied
(2026-11-14, tools GLM); round-3 hardening (2026-11-14, primary GLM under
root's exclusive delegation; Tools GLM notified and no longer edits this
file). Still NOT released for HTTP execution (needs --confirm-http AND
root's formal release; the G4 restart probe additionally needs
--with-restart-probe). Pure-local modes: --list-gates / --contract-probe.

Round-3 fixes mapped:
  R1  G6 weak-owner boundary: the scheduled Run must terminate `failed`
      with a REAL dataset-versioning 403 attribution in run.error (never
      an arbitrary failed/aborted), and the target's FULL transaction set
      (paginated /v1/datasets/{id}/transactions — OPEN/ABORTED without
      files included) plus file IDs must be provably unchanged.
  R2  full_state no longer derives transactions from files: it reuses the
      FROZEN P23 admission_snapshots.enumerate_target_txns (strict
      models.Page envelope, bounded cursor following, duplicate-page
      rejection, pinned status vocabulary) over the real transactions
      endpoint.
  R3  run_all: a per-case cleanup exception can never abort the loop or
      skip the global finally; finally_owned now runs in the OUTERMOST
      finally (always attempted, own try/except), every per-owned step
      keeps its per-sync try/except, and the attempt result is always
      persisted.
  R4  G4 restart probe is REAL but flag-gated code. The slot is frozen
      with the FROZEN P23 interactive Source FOR UPDATE holder (_SourceRowLock
      — the exact NOWAIT lock every replica claim takes; NOT a disable, which
      would bump the revision and fake stale_revision instead of lost
      credentials). Root correction: Repo.Migrate replays ALTER TABLE
      connections at startup, so the lock is NEVER held across the start —
      the sequence is explicit stop → release → start: guard4 →
      owned-runs-terminal → probe sync + UNEXPIRED arm + prewarm
      enqueued→terminal → acquire Source lock → this Source's runs terminal
      → non-None tick/run/grant snapshot → guard4 AGAIN immediately pre-stop
      (lock held) → compose stop --timeout 120 + clean-state verify
      (Running=false/ExitCode=0/no OOM; raw docker inspect archived) →
      ACTIVE ROLLBACK release rc0 STRICTLY BEFORE start (aux holds no local
      generation; base gracefully stopped ⇒ the unlocked gap has no armed
      claimant) → compose start → health → DB grant identical +
      locally_executable=false → 5s zero-new-Run/tick window (both sets
      non-None) → auth_required(owner_stale) on a NEW tick → fresh arm NEW
      grant → enqueued resumes on NEW tick+run ids. ANY stop/guard/release
      failure still frees the lock (finally belt) and fails honestly — start
      is never attempted to mask an anomaly. Formal driver MUST pass
      --with-restart-probe and its final gate must actually run this probe
      (interface requirement handed to root). All BEFORE any P21 ABC phase.

Round-1 fixes mapped:
  1  owned Sync IDs registered IMMEDIATELY on create; global finally
     disables+GET-confirms every owned schedule and proves a bounded
     no-new-IDs window; cleanup failures gate the verdict (never hidden
     under an earlier PASS). Per-case best-effort disable at case end.
  2  G4 short JWT reuses the OWNER subject+org (isolates lifetime);
     G6 weak token reuses the SOURCE OWNER subject/org without write
     scopes (isolates the intended permission boundary); wrong-org is
     reported as tenant predicate, parent-binding proof left to PG.
  3  G3 revision path: cron-change +1, disable +1, re-enable +1 ⇒ rev0+3.
  4  runs/ticks helpers are STRICT: HTTP 200, real array (M.extract_items
     for the runs envelope), sync association validated; an error
     envelope can never silently become an empty list.
  5  G2/G7 require a REAL positive enqueued tick + associated Run awaited
     to terminal, persisted disable GET-confirmed, and ID-SET comparison
     over a bounded window (never 0==0, never saturating counts).
  6  G5 sets a REAL Source admission policy (max_inflight_runs=1 +
     min_interval_ms) on its own source, observes typed capacity and
     interval skips with zero extra Run/stamp/pull/artifact effects
     (fixture request_count + run-ID-set + admission-stamp snapshots),
     then proves another Source reaches a real succeeded+COMMITTED
     terminal. Local-full/Stop are evidence-BOUND (new unit tests +
     formal P22/ disable-proof phases), never placeholder HTTP PASS.
  7  G1 adds COMMITTED txn status, metadata.lineage.sync_run_id binding,
     and full expected_records(151) value equality (leading-zero IDs).
  8  Results are suffix/attempt-specific and collision-safe
     (TMP/result_p24-<suffix>.json, printed as RESULT_FILE=…); the
     driver copies per attempt and never overwrites TMP.
  9  Driver ordering: frozen P22 aux-up FIRST, then LAST up of the three
     connectors with base+P22+P24 overrides; whitelisted interval
     inspect + arm loop_running=true asserted (in formal_run_p24.sh).
  10 PG names: p24-required-names.txt is DRAFT from the real current
     schedule_integration_test.go (13 top names), root freezes later.
"""

import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONTRACT = HERE / "implementation-contract.md"
P1_SMOKE = HERE.parent / "business-platform-p1" / "smoke" / "smoke.py"
P23_SNAP = HERE.parent / "business-platform-p2-3" / "admission_snapshots.py"
LIVE_GUARD = HERE / "p24_live_guard.py"
TMP_DEFAULT = Path("/private/tmp/openfoundry-platform-p1-q3v9")
PROJ = "openfoundry-platform-p1-q3v9"
STAMP_COLUMN = "sync_last_admitted_at"      # P23 admission stamp (frozen)
ADMIT_KEY = "admission"                      # config.sync.admission

# ---------------------------------------------------------------------------
# Pinned from implementation-contract.md §2/§6 (2026-11-14 10:38).
# ---------------------------------------------------------------------------
ROUTES = {
    "sync_create": "/api/v1/data-connection/syncs",                        # POST
    "sync_patch": "/api/v1/data-connection/syncs/{id}",                    # PATCH
    "sync_get": "/api/v1/data-connection/syncs/{id}",                      # GET
    "sync_runs": "/api/v1/data-connection/syncs/{id}/runs",                # GET
    "sync_run": "/api/v1/data-connection/syncs/{id}/run",                  # POST
    "schedule_arm": "/api/v1/data-connection/syncs/{id}/schedule:arm",     # POST
    "schedule_revoke": "/api/v1/data-connection/syncs/{id}/schedule:arm",  # DELETE
    "schedule_ticks": "/api/v1/data-connection/syncs/{id}/schedule/ticks", # GET
}
FIELDS = {
    "cron": "schedule_cron",
    "timezone": "schedule_time_zone",
    "enabled": "schedule_enabled",
    "revision": "schedule_revision",
    "next_fire": "schedule_next_fire_at",
    "invalid_schedule_code": "invalid_schedule",
    "tick_outcome": "outcome",
    "tick_run_link": "run_id",
    "tick_scheduled_for": "scheduled_for",
    "skip_reason": "reason",
    "grant": "schedule_authorization",
    "auth_required": "auth_required",
    "arm_armed": "armed",
    "arm_grant": "grant_id",
    "arm_expires": "authorization_expires_at",
    "arm_loop": "scheduler_loop_running",
}
# 6 fields => Quartz6. NOTE: the repo-owned Foundry-parity parser REJECTS
# "?" in day-of-week (first real-HTTP round 1448507d89: every arm/create
# answered 400 invalid_schedule — a tool-constant bug, not a production
# bug); the every-second form the library actually accepts is six plain
# stars (same family as the already-green Go six-field "0 */5 * * * *").
EVERY_SECOND_QUARTZ6 = "* * * * * *"
EVERY_MINUTE_UNIX5 = "* * * * *"

GATES = {
    "G1": "automatic REST 151/hash/lineage/COMMITTED/full-values via armed schedule",
    "G2": "two instances, same slot <=1 Run, tick association (positive)",
    "G3": "enable/disable/modify/clear semantics (rev0+3 path)",
    "G4": "short-JWT(owner sub/org)/renew/cross-instance/restart auth_required",
    "G5": "REAL shared Source admission gates + zero side effects + B terminal",
    "G6": "tenant same-sub dual-Org/personal/write-scope isolation",
    "G7": "migration/restart old state; finally disable + ID-set stop proof",
    "G8": "evidence honesty (real rc/values/hash, failures preserved)",
}

M = None
P23S = None         # frozen P23 admission_snapshots module (R2)
ENV_DIR = None
EV = None
SUFFIX = None
OWNED = {}          # sid -> {"tag", "source_id", "target_id", "fixture_src"}
CUR_TAG = ""
CLEANUP = None      # global finally record (item 1)
RESTART_PROBE = False   # R4: only with --with-restart-probe (formal phase)


def load_helpers(env_dir):
    global M, ENV_DIR, P23S
    ENV_DIR = Path(env_dir)
    spec = importlib.util.spec_from_file_location("p1smoke", P1_SMOKE)
    M = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(M)
    M.ARGS = type("A", (), {"env_dir": str(env_dir)})()
    M.load_env(ENV_DIR)                    # in-memory only
    M.BASES["connector2"] = os.environ.get(
        "OF_P24_CONNECTOR2_BASE", "http://127.0.0.1:17089")
    # R2: reuse the FROZEN P23 snapshot module (read-only import, never a
    # copy) for the strict transactions pagination.
    spec23 = importlib.util.spec_from_file_location("p23snap", P23_SNAP)
    P23S = importlib.util.module_from_spec(spec23)
    spec23.loader.exec_module(P23S)


def expect(cond, label, detail=None):
    item = {"ok": bool(cond), "label": label,
            "detail": M.scrub(str(detail))[:400] if detail is not None
            else None}
    EV.append(item)
    print(f"[p24-smoke] {'ok  ' if cond else 'FAIL'} {label}"
          + (f" — {item['detail']}" if detail is not None and not cond
             else ""))
    return bool(cond)


def api(path, token, *, method=None, payload=None, base="connector"):
    return M.http(base, path, payload=payload, method=method, token=token)


# ------------------------------------------------- owned registry (item 1) --
def register_owned(sid, *, tag, source_id, target_id, fixture_src):
    OWNED[sid] = {"tag": tag, "source_id": source_id,
                  "target_id": target_id, "fixture_src": fixture_src}
    print(f"[p24-smoke] OWNED + {sid} (tag={tag})")


def disable_owned_by_tag(prefix):
    """Best-effort per-case disable (run at every case end). R3: NEVER
    raises — a per-sync failure is recorded and the loop CONTINUES, so a
    cleanup exception can neither abort the scenario loop nor skip the
    global finally (which still gates the verdict). Root wiring fix: the
    comparison is case-insensitive (both sides lowered) so a scenario
    name of any case always matches its tags — in particular the g4 case
    MUST disable its g4/g4x/g4r tags (the G4R probe ends with a FRESH
    armed generation; without the case-end disable it would keep firing
    into later cases)."""
    want = prefix.lower()
    for sid, info in sorted(OWNED.items()):
        if not info["tag"].lower().startswith(want):
            continue
        tok = CURRENT_TOKENS.get(info["tag"]) or _LAST_OWNER[0]
        if not tok:
            continue
        try:
            res = api(ROUTES["sync_patch"].format(id=sid), tok,
                      method="PATCH", payload={FIELDS["enabled"]: False})
            print(f"[p24-smoke] case-cleanup {sid} disable "
                  f"rc={res.status}")
        except Exception as exc:                          # noqa: BLE001
            print(f"[p24-smoke] case-cleanup {sid} disable ERROR "
                  f"(recorded, loop continues): {str(exc)[:120]}")


class _Bearer:
    def __init__(self, token):
        self.token = token


CURRENT_TOKENS = {}      # tag -> token (set by each scenario)
_LAST_OWNER = [None]


# ------------------------------------------------------- strict lists (4) --
def list_ticks(owner, sid, limit=200, base="connector"):
    """STRICT: 200 + bare array + every tick bound to THIS sync."""
    res = api(f"{ROUTES['schedule_ticks'].format(id=sid)}?limit={limit}",
              owner.token, method="GET", base=base)
    if not (res.status == 200 and isinstance(res.body, list)
            and all(t.get("sync_def_id") == sid for t in res.body)):
        expect(False, f"strict ticks list {sid[:8]}",
               {"status": res.status, "type": type(res.body).__name__,
                "body": M.scrub(str(res.body))[:200]})
        return None
    return res.body


def list_runs(owner, sid):
    """STRICT: 200 + real items (bare array or {items:[…]}); an error
    envelope can never become an empty list. The ACTUAL models.SyncRun
    always carries sync_def_id — require it present and == sid (no
    missing-key fallback); every item is a dict with a unique id."""
    res = api(ROUTES["sync_runs"].format(id=sid), owner.token, method="GET")
    ok = res.status == 200
    items = None
    if ok:
        if isinstance(res.body, list):
            items = res.body
        elif isinstance(res.body, dict) and isinstance(
                res.body.get("items"), list):
            items = res.body["items"]
    assoc = (items is not None
             and all(isinstance(t, dict) and t.get("sync_def_id") == sid
                     and t.get("id") for t in items))
    uniq = items is not None and len(
        {t["id"] for t in items}) == len(items)
    if not (ok and items is not None and assoc and uniq):
        expect(False, f"strict runs list {sid[:8]}",
               {"status": res.status, "type": type(res.body).__name__,
                "assoc": assoc, "uniq": uniq,
                "body": M.scrub(str(res.body))[:200]})
        return None
    return items


def tick_ids(owner, sid):
    ts = list_ticks(owner, sid)
    return None if ts is None else {t["id"] for t in ts}


def run_ids(owner, sid):
    rs = list_runs(owner, sid)
    return None if rs is None else {r["id"] for r in rs}


def get_sync(owner, sid, base="connector"):
    return api(ROUTES["sync_get"].format(id=sid), owner.token,
               method="GET", base=base)


def patch_sync(owner, sid, body):
    return api(ROUTES["sync_patch"].format(id=sid), owner.token,
               method="PATCH", payload=body)


def arm(owner, sid, base="connector", token=None):
    return api(ROUTES["schedule_arm"].format(id=sid),
               token or owner.token, method="POST", payload={}, base=base)


def revoke(owner, sid):
    return api(ROUTES["schedule_revoke"].format(id=sid), owner.token,
               method="DELETE")


def psql_read(sql):
    """Read-only psql via the stack postgres (SELECT-only callers)."""
    proc = subprocess.run(
        ["docker", "compose", "-f", str(ENV_DIR / "compose.yaml"),
         "--project-name", PROJ, "exec", "-T", "postgres", "psql",
         "-U", "openfoundry", "-d", "connectors", "-t", "-A", "-v",
         "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True, timeout=90)
    if proc.returncode != 0:
        raise RuntimeError(f"psql rc={proc.returncode}: "
                           f"{proc.stderr[:150]}")
    return proc.stdout


def stamp_of(source_id):
    """Admission stamp, STRICTLY parsed. PG's explicit `(bool)::text`
    renders 'true'/'false'. Semantics (review fix — the previous
    inversion returned None for REAL non-null stamps):
      flag == 'true'  ⇒ column IS NULL  ⇒ legal pre-admission NULL →
                        return None;
      flag == 'false' ⇒ column NOT NULL ⇒ MUST return the NON-EMPTY
                        stamp text (empty value ⇒ raise);
      anything else (missing row, unparseable) ⇒ raise — a failed read
      may NEVER masquerade as a legal NULL."""
    out = psql_read(f"SELECT COALESCE({STAMP_COLUMN}::text, '') || '|' ||"
                    f" ({STAMP_COLUMN} IS NULL)::text FROM connections"
                    f" WHERE id = '{source_id}'::uuid;")
    value, _, flag = out.strip().partition("|")
    if flag not in ("true", "false"):
        expect(False, f"stamp read invalid {source_id[:8]}", out[:80])
        raise RuntimeError(
            f"stamp read invalid for {source_id}: {out[:80]!r}")
    if flag == "true":
        return None                       # legal NULL (not admitted yet)
    if not value:
        expect(False, f"stamp non-null but empty {source_id[:8]}",
               out[:80])
        raise RuntimeError(
            f"stamp non-null but empty for {source_id}: {out[:80]!r}")
    return value


def fixture_request_count(src):
    snap = M.http("fixture", f"/admin/src/{src}")
    return (snap.body or {}).get("request_count") if snap.status == 200 \
        else None


# --------------------------------------------------------- sync factory ----
def make_sync(owner, *, tag, cron=EVERY_MINUTE_UNIX5, tz=None, enabled=True,
              page_size=50, total=151, admission=None, fixture_extra=None,
              request_timeout_ms=None):
    """Fresh fixture src + source(+optional admission) + target + sync.
    Registers the owned Sync ID IMMEDIATELY on create success (item 1).
    request_timeout_ms configures the REAL Source pull budget (root
    resumed-review: fixture delays must live INSIDE the configured
    Source timeout, not the 10s default)."""
    sfx = f"{tag}-{SUFFIX}-{uuid.uuid4().hex[:6]}"
    CURRENT_TOKENS[tag] = owner.token
    _LAST_OWNER[0] = owner.token
    patch = {"mode": "ok", "content": 1, "total": total,
             "page_size": page_size}
    if fixture_extra:
        patch.update(fixture_extra)
    M.fixture_admin(sfx, patch)
    payload = M.source_payload(f"p24-{sfx}", sfx)
    if request_timeout_ms is not None:
        payload["config"]["sync"]["limits"]["request_timeout_ms"] = \
            request_timeout_ms
    if admission is not None:
        payload["config"]["sync"][ADMIT_KEY] = admission
    res = M.api("connector", "src_create", token=owner.token, payload=payload)
    M.expect(res.status in (200, 201) and res.body.get("id"),
             "source create failed", {"status": res.status,
                                      "body": M.scrub(str(res.body))[:300]})
    source = res.body
    target = M.create_dataset(owner.token, f"p24-out-{sfx}", "json")
    body = {"source_id": source["id"], "output_dataset_id": target["id"],
            FIELDS["enabled"]: enabled}
    if cron is not None:
        body[FIELDS["cron"]] = cron
    if tz:
        body[FIELDS["timezone"]] = tz
    sync = api(ROUTES["sync_create"], owner.token, payload=body)
    if not (sync.status in (200, 201)
            and isinstance(sync.body, dict) and sync.body.get("id")):
        # Root fix (first real round): fail FAST with a clearly recorded
        # failure instead of a downstream KeyError on sync.body["id"]; the
        # per-scenario handler then records the error honestly. Owned
        # registration stays success-only (no def row ⇒ nothing to
        # disable/revoke — the global finally covers whatever registered).
        expect(False, "make_sync: sync create failed (fail-fast)",
               {"tag": tag, "status": sync.status,
                "body": M.scrub(str(sync.body))[:300]})
        raise RuntimeError(f"make_sync sync create failed tag={tag} "
                           f"status={sync.status}")
    register_owned(sync.body["id"], tag=tag, source_id=source["id"],
                   target_id=target["id"], fixture_src=sfx)
    return source, target, sync


def verify_published_artifact(owner, target_id, run, total=151):
    """G1/G5-B full P21C-caliber checks (item 7): the P1 helper already
    asserts version/file/txn binding + sha256(raw)==content_hash==
    txn.metadata.content_hash; we ADD COMMITTED, lineage run binding and
    full expected_records value equality."""
    rows, _file_meta, _raw = M.download_run_artifact(
        owner.token, target_id, run)
    txn = M.api("dataset", "ds_txn_get",
                path_args={"id": target_id, "branch": "main",
                           "txn": run["dataset_version_id"]},
                token=owner.token)
    expect(txn.status == 200, "artifact txn read 200",
           {"status": txn.status})
    body = txn.body or {}
    expect(body.get("status") == "COMMITTED", "txn status COMMITTED",
           {"status": body.get("status")})
    lineage = (body.get("metadata") or {}).get("lineage") or {}
    expect(lineage.get("sync_run_id") == run["id"],
           "txn lineage.sync_run_id bound to THIS run",
           {"lineage": lineage.get("sync_run_id"), "run": run["id"]})
    expect(rows == M.expected_records(total),
           f"artifact full-value equality vs expected_records({total})",
           {"rows": len(rows),
            "first": rows[0] if rows else None})


def wait_terminal_run(owner, sid, run_id, timeout=120):
    def find():
        rs = list_runs(owner, sid)
        return next((r for r in (rs or [])
                     if r["id"] == run_id and r["status"] == "succeeded"),
                    None)
    got = wait_for(find, timeout)
    expect(got is not None, f"run {run_id[:8]} reached succeeded", got)
    return got


def wait_for(pred, timeout, interval=0.5):
    deadline = time.monotonic() + timeout
    out = None
    while time.monotonic() < deadline:
        out = pred()
        if out:
            return out
        time.sleep(interval)
    return out


def wait_tick(owner, sid, outcome, reason=None, timeout=30):
    def find():
        ts = list_ticks(owner, sid)
        return next((t for t in (ts or [])
                     if t.get(FIELDS["tick_outcome"]) == outcome
                     and (reason is None
                          or t.get(FIELDS["skip_reason"]) == reason)), None)
    return wait_for(find, timeout)


def sync_view(owner, sid, source_id, base="connector"):
    """200 + real id + Source association FIRST (root resumed-review:
    legal Go DTOs omitempty-omit disabled-false and NULL cursor, so key
    presence can never be the validity check)."""
    res = get_sync(owner, sid, base)
    if not (res.status == 200 and isinstance(res.body, dict)
            and res.body.get("id") == sid
            and res.body.get("source_id") == source_id):
        expect(False, f"strict sync GET {sid[:8]}",
               {"status": res.status,
                "id_ok": isinstance(res.body, dict)
                and res.body.get("id") == sid,
                "body": M.scrub(str(res.body))[:200]})
        return None
    return res.body


def enabled_is_true(body):
    """omitempty: an ABSENT schedule_enabled means false."""
    return body.get(FIELDS["enabled"]) is True


def cursor_is_null(body):
    """omitempty: absent or null schedule_next_fire_at both mean NULL."""
    return not body.get(FIELDS["next_fire"])


def persisted_disabled(sid):
    """SELECT the exact owned def row (post-migration columns exist once
    the candidate is live): enabled must be false AND cursor NULL."""
    out = psql_read(
        f"SELECT (schedule_enabled)::text, (schedule_next_fire_at IS NULL)"
        f"::text FROM batch_sync_defs WHERE id = '{sid}'::uuid;")
    parts = out.strip().split("|")
    ok = parts == ["false", "true"]
    expect(ok, f"persisted disabled+NULL cursor {sid[:8]}", out[:80])
    return ok


def confirm_disabled(owner, sid, source_id=None):
    if source_id:
        body = sync_view(owner, sid, source_id)
    else:
        res = get_sync(owner, sid)
        body = res.body if res.status == 200 and isinstance(
            res.body, dict) and res.body.get("id") == sid else None
    ok = (body is not None and not enabled_is_true(body)
          and cursor_is_null(body))
    expect(ok, f"GET-confirm disabled {sid[:8]}",
           {"enabled": (body or {}).get(FIELDS["enabled"]),
            "next_fire": (body or {}).get(FIELDS["next_fire"])})
    if ok:
        ok = persisted_disabled(sid)      # persisted, not just DTO shape
    return ok


def runs_state(owner, sid):
    """{run_id: status} or None on any strict-list failure (cleanup and
    G5/G6 use this for terminal-state proof)."""
    rs = list_runs(owner, sid)
    return None if rs is None else {r["id"]: r.get("status") for r in rs}


TERMINAL_RUN_STATES = ("succeeded", "failed", "aborted")


def wait_run_status(owner, sid, run_id, statuses, timeout=60):
    def find():
        st = runs_state(owner, sid)
        return st and st.get(run_id) in statuses
    got = wait_for(find, timeout)
    expect(bool(got), f"run {run_id[:8]} reached {'|'.join(statuses)}",
           (runs_state(owner, sid) or {}).get(run_id))
    return got


def enumerate_target_txns(target_id, token):
    """R2: the target's FULL transaction set through the FROZEN P23
    strict pagination (models.Page envelope, bounded cursor following,
    duplicate-page rejection, pinned OPEN/COMMITTED/ABORTED vocabulary) —
    OPEN/ABORTED transactions without files can never be missed the way a
    files-derived derivation would. Raises on any envelope/read failure."""
    import urllib.request

    def fetch(cursor):
        url = f"{M.BASES['dataset']}/v1/datasets/{target_id}/transactions"
        if cursor:
            url += f"?cursor={cursor}"
        req = urllib.request.Request(url, headers={
            "Authorization": "Bearer " + token})
        with urllib.request.urlopen(req, timeout=30) as r:
            body = json.loads(r.read() or b"null")
        return r.status, body

    return P23S.enumerate_target_txns(fetch)


def full_state(owner, sid, source_id, target_id, fsrc):
    """Complete refusal-effect state (root resumed-review: BOTH G5
    windows must compare target file IDs, txnID→status, Source stamp,
    Run-ID set AND fixture counts; a normal occupier publish can never be
    mistaken for refusal effects). R2: txnID→status comes from the FULL
    paginated transactions enumeration (frozen P23 parser), NOT from
    files. Returns None on any read failure — callers must treat None as
    invalid, never as empty."""
    files = M.list_files(owner.token, target_id)
    if not isinstance(files, list):
        expect(False, f"full_state list_files {sid[:8]}", str(files)[:120])
        return None
    try:
        txn_status = enumerate_target_txns(target_id, owner.token)
    except Exception as exc:                              # noqa: BLE001
        expect(False, f"full_state txn enumeration {sid[:8]}",
               str(exc)[:200])
        return None
    runs = run_ids(owner, sid)
    if runs is None:
        return None
    fx = fixture_request_count(fsrc)
    if fx is None:
        expect(False, f"full_state fixture read {sid[:8]}", fsrc)
        return None
    try:
        stamp = stamp_of(source_id)      # None = legal NULL ONLY; a
    except RuntimeError:                 # failed read invalidates the
        expect(False, f"full_state stamp read {sid[:8]}",   # snapshot
               "stamp read failed")      # (never conflated with NULL)
        return None
    return {"files": sorted(f.get("id", "") for f in files),
            "txn_status": txn_status, "runs": sorted(runs),
            "stamp": stamp, "fx": fx}


# --------------------------------------------- R4: real restart probe ------
def _guard4(env_dir):
    """Strict guard4: run the approved P24 live guard (4 pinned pre-boot
    exceptions; unknown/missing/drift blocks). Returns (real_rc, full
    stdout+stderr) — the rc is never rewritten."""
    proc = subprocess.run(
        ["python3", "-I", str(LIVE_GUARD), str(env_dir), "connector"],
        capture_output=True, text=True, timeout=120)
    out = (proc.stdout + ("\n[stderr]\n" + proc.stderr if proc.stderr
                          else "")).strip()
    return proc.returncode, out


def _probe_dump(ev, name, rc, out):
    """FULL (untruncated) probe subprocess output to its own evidence
    file; the result JSON references path/rc/bytes only."""
    path = TMP_DEFAULT / f"result_p24-{SUFFIX}-g4r-{name}.txt"
    path.write_text((M.scrub(out) if out else "") + "\n")
    ev[f"g4r_{name}"] = {"rc": rc, "file": str(path),
                         "bytes": len(out or "")}


def _load_p23_smoke():
    """Import the FROZEN P23 smoke module (read-only reuse of its
    battle-tested interactive _SourceRowLock; module top level is pure
    constants). ARGS/PROJECT are injected so the holder's compose argv
    points at THIS environment."""
    spec = importlib.util.spec_from_file_location(
        "p23sm", HERE.parent / "business-platform-p2-3" / "smoke_p23.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.ARGS = type("A", (), {"env_dir": str(ENV_DIR)})()
    mod.PROJECT = PROJ
    return mod


def _connector_healthy(timeout=60):
    import urllib.request
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                    f"{M.BASES['connector']}/healthz", timeout=5) as r:
                if r.status == 200:
                    return True
        except Exception:                               # noqa: BLE001
            time.sleep(1.0)
    return False


def _grant_row(sid):
    """DB grant metadata (NONSECRET columns only) for the def; None when
    no row exists. Read-only psql."""
    out = psql_read(
        f"SELECT grant_id::text, boot_id::text, expires_at::text, "
        f"last_seen_at::text FROM sync_schedule_grants "
        f"WHERE sync_def_id = '{sid}'::uuid;")
    return out.strip() or None


def _grant_fields(row):
    """Parse a _grant_row line into {grant_id, boot_id, expires_at,
    last_seen_at} (None on a malformed row). last_seen_at legitimately
    advances via heartbeats while the service RUNS — callers compare the
    other three fields across a live window and the FULL row only across
    a stopped window."""
    if not row:
        return None
    parts = row.split("|")
    if len(parts) != 4 or not all(parts):
        return None
    return {"grant_id": parts[0], "boot_id": parts[1],
            "expires_at": parts[2], "last_seen_at": parts[3]}


def _compose(*argv, timeout=240):
    """Run a docker compose subcommand for THIS environment; returns the
    raw CompletedProcess (rc/stdout/stderr preserved by the caller)."""
    return subprocess.run(
        ["docker", "compose", "-f", str(ENV_DIR / "compose.yaml"),
         "--project-name", PROJ, *argv],
        capture_output=True, text=True, timeout=timeout)


def _pin_connector_cid():
    """Pin the compose connector container id BEFORE the stop (root G4
    blocker fix: `compose ps -q` lists only RUNNING containers — after a
    stop it is guaranteed empty, so the stopped-state verify and the
    finally belt could never see the container; verified against the
    official docker compose ps reference semantics by independent
    find-docs review). STRICTLY exactly one non-empty id, else None.
    compose start reuses the same stopped container, so the pin stays
    valid across the whole stop→release→start sequence."""
    ps = _compose("ps", "-q", "connector", timeout=30)
    if ps.returncode != 0:
        return None, (ps.stdout + ps.stderr)
    lines = [ln for ln in ps.stdout.splitlines() if ln.strip()]
    if len(lines) != 1:
        return None, (f"expected exactly 1 connector container, got "
                      f"{len(lines)}: {lines!r}")
    return lines[0].strip(), ps.stdout


def _connector_state(cid):
    """Whitelisted docker inspect STATE projection for the pinned
    container: `docker inspect --format {{json .State}} <cid>` — NEVER a
    full-container inspect (whose Config.Env carries secrets). Returns
    ({running, exit_code, oom_killed} parsed ONLY from those three keys,
    safe_raw_state_json, real_inspect_rc) or (None, output, rc)."""
    if not cid:
        return None, "no pinned container id", 1
    proc = subprocess.run(
        ["docker", "inspect", "--format", "{{json .State}}", cid],
        capture_output=True, text=True, timeout=30)
    raw = proc.stdout.strip()
    if proc.returncode != 0:
        return None, (raw + "\n" + proc.stderr.strip()), proc.returncode
    try:
        state = json.loads(raw)
        return {"running": bool(state.get("Running")),
                "exit_code": state.get("ExitCode"),
                "oom_killed": bool(state.get("OOMKilled"))}, raw, proc.returncode
    except ValueError:
        return None, raw, proc.returncode


def g4_restart_probe(ev):
    """R4: REAL authorization-restart proof on the ORIGINAL connector.
    Formal phase only (--with-restart-probe), always BEFORE any P21 ABC
    phase. The slot is FROZEN with the frozen P23 interactive Source FOR
    UPDATE holder (the exact NOWAIT lock every replica claim takes) — NOT
    by disabling (a disable would bump the revision and masquerade the
    observation as stale_revision instead of lost credentials).

    Root correction (restart-blocking): Repo.Migrate replays ALTER TABLE
    connections at startup and would DEADLOCK behind a held connections
    row lock — so the lock is NEVER held across the start. The explicit
    controlled sequence is stop → release → start:

      guard4 → owned-runs-terminal → probe sync + UNEXPIRED arm + prewarm
      enqueued→terminal → acquire Source FOR UPDATE (blocks all replica
      claims) → this Source's runs terminal → non-None tick/run/grant
      snapshot → guard4 AGAIN immediately pre-stop (lock held) →
      compose stop --timeout 120 + State verify (Running=false,
      ExitCode=0, OOMKilled=false; raw inspect archived) → ACTIVE
      ROLLBACK release with real rc 0 STRICTLY BEFORE start (the aux
      replica holds no local generation for this grant and the base is
      gracefully stopped, so the brief unlocked gap has no armed claimant)
      → compose start → health → DB grant identical +
      locally_executable=false → 5s zero-new-Run/tick window →
      auth_required(owner_stale) on a NEW tick → fresh arm NEW grant →
      enqueued resumes on NEW tick+run ids.

    Any failed guard/list/terminal/stop/release step ⇒ the lock is still
    released (finally belt) and the probe FAILS HONESTLY — it NEVER
    proceeds to start to mask an anomaly."""
    # 0. Strict guard4 before anything of ours is created.
    grc, gout = _guard4(ENV_DIR)
    _probe_dump(ev, "guard_pre", grc, gout)
    if not expect(grc == 0, "G4R strict guard4 (pre)", {"rc": grc}):
        return
    # 1. Precondition: every OWNED run so far already terminal.
    for osid in sorted(OWNED):
        tok = CURRENT_TOKENS.get(OWNED[osid]["tag"]) or _LAST_OWNER[0]
        st = runs_state(_Bearer(tok), osid) if tok else None
        if not expect(st is not None and all(
                s in TERMINAL_RUN_STATES for s in (st or {}).values()),
                      f"G4R owned runs terminal pre-restart {osid[:8]}",
                      {k: v for k, v in (st or {}).items()
                       if v not in TERMINAL_RUN_STATES}):
            return
    # 2. Dedicated probe sync + UNEXPIRED arm + prewarm firing.
    org = str(uuid.uuid4())
    owner = M.Identity("p24g4r-owner", org=org)
    source, target, sync = make_sync(owner, tag="g4r",
                                     cron=EVERY_SECOND_QUARTZ6)
    sid = sync.body["id"]
    arm_res = arm(owner, sid)
    if not expect(arm_res.status == 200
                  and arm_res.body.get(FIELDS["arm_armed"]) is True,
                  "G4R unexpired arm (owner, default TTL)", arm_res.body):
        return
    old_grant = arm_res.body.get(FIELDS["arm_grant"])
    enq0 = wait_tick(owner, sid, "enqueued", timeout=30)
    if not expect(bool(enq0 and enq0.get(FIELDS["tick_run_link"])),
                  "G4R prewarm enqueued tick proves live authorization",
                  enq0):
        return
    wait_run_status(owner, sid, enq0[FIELDS["tick_run_link"]],
                    TERMINAL_RUN_STATES, timeout=60)

    holder = _load_p23_smoke()._SourceRowLock(source["id"])
    started = [False]

    def release_lock():
        """Active ROLLBACK; idempotent (release() clears the session).
        Returns the real rc (None when there was nothing to release)."""
        hrc = holder.release()
        ev["g4r_lock_release_rc"] = hrc
        return hrc

    try:
        # 3. Freeze the slot: Source FOR UPDATE blocks every replica claim
        #    (all claim paths take this row NOWAIT ⇒ contention skip, no
        #    new ticks/runs for THIS source while held).
        pid = holder.acquire(timeout=15)
        if not expect(isinstance(pid, int) and pid > 0,
                      "G4R source row lock acquired (frozen P23 holder)",
                      pid):
            return
        ev["g4r_lock_pid"] = pid
        # 4. In-flight executions of THIS source finish; nothing new can
        #    be admitted while the lock is held.
        def source_terminal():
            st = runs_state(owner, sid)
            return st if (st is not None and all(
                s in TERMINAL_RUN_STATES for s in st.values())) else None
        if not expect(wait_for(source_terminal, 90) is not None,
                      "G4R probe-source runs all terminal under lock",
                      runs_state(owner, sid)):
            return
        before_ticks = tick_ids(owner, sid)
        before_runs = run_ids(owner, sid)
        if not expect(before_ticks is not None
                      and before_runs is not None,
                      "G4R pre-stop ID sets readable (non-None)",
                      {"ticks": before_ticks is not None,
                       "runs": before_runs is not None}):
            return
        pre_stop_row = _grant_row(sid)
        pre_stop_f = _grant_fields(pre_stop_row)
        if not expect(pre_stop_f is not None,
                      "G4R DB grant row present pre-stop", pre_stop_row):
            return
        # Identity from ARM time through the locked window: grant_id,
        # boot_id and expires_at may never drift (only last_seen_at may
        # legally advance via heartbeats while the service runs).
        expect(pre_stop_f["grant_id"] == old_grant,
               "G4R grant_id identical arm→pre-stop",
               {"arm": old_grant, "row": pre_stop_f["grant_id"]})
        # 5. Strict guard4 IMMEDIATELY before the stop, lock still held.
        grc2, gout2 = _guard4(ENV_DIR)
        _probe_dump(ev, "guard_pre_stop", grc2, gout2)
        if not expect(grc2 == 0, "G4R strict guard4 (immediately pre-stop, "
                      "lock held)", {"rc": grc2}):
            return
        # 6. Controlled STOP of the ORIGINAL connector (graceful SIGTERM
        #    window). The container id is PINNED while it still runs
        #    (compose ps -q is running-only), the state check uses the
        #    whitelisted {{json .State}} projection only, and BOTH the
        #    compose rc and the REAL inspect rc are archived with the safe
        #    raw State JSON.
        cid, cid_raw = _pin_connector_cid()
        ev["g4r_container"] = cid
        _probe_dump(ev, "pin_cid", 0 if cid else 1, cid_raw or "")
        if not expect(cid is not None,
                      "G4R connector container pinned (exactly 1)", cid_raw):
            return
        stop = _compose("stop", "--timeout", "120", "connector")
        _probe_dump(ev, "stop", stop.returncode,
                    stop.stdout + ("\n[stderr]\n" + stop.stderr
                                   if stop.stderr else ""))
        state, state_raw, inspect_rc = _connector_state(cid)
        _probe_dump(ev, "state_stopped", inspect_rc, state_raw or "")
        if not expect(stop.returncode == 0,
                      "G4R graceful connector stop (--timeout 120)",
                      {"rc": stop.returncode}):
            return
        if not expect(state is not None and state["running"] is False
                      and state["exit_code"] == 0
                      and state["oom_killed"] is False,
                      "G4R stopped cleanly (Running=false, ExitCode=0, "
                      "no OOM)", state):
            return
        # 6b. Grant-row BASELINE after the stop confirmation and before
        #     release/start: the service is DOWN, heartbeats have stopped,
        #     so the full row (last_seen_at included) is now frozen and a
        #     byte-identical post-start comparison is meaningful.
        stopped_row = _grant_row(sid)
        stopped_f = _grant_fields(stopped_row)
        if not expect(stopped_f is not None,
                      "G4R DB grant row present stopped", stopped_row):
            return
        expect(all(stopped_f[k] == pre_stop_f[k]
                   for k in ("grant_id", "boot_id", "expires_at")),
               "G4R grant_id/boot_id/expires_at identical arm→stop "
               "(only last_seen_at may have advanced)",
               {"pre_stop": pre_stop_f, "stopped": stopped_f})
        # 7. ACTIVE ROLLBACK — the lock is released STRICTLY BEFORE start
        #    (Repo.Migrate's startup ALTER TABLE connections must never
        #    queue behind our row lock). A nonzero release rc fails the
        #    probe: start is never attempted to mask it.
        hrc = release_lock()
        if not expect(hrc == 0,
                      "G4R source lock actively released rc0 BEFORE start",
                      hrc):
            return
        # 8. START the original connector back up; full output archived.
        startp = _compose("start", "connector")
        _probe_dump(ev, "start", startp.returncode,
                    startp.stdout + ("\n[stderr]\n" + startp.stderr
                                     if startp.stderr else ""))
        started[0] = True
        if not expect(startp.returncode == 0, "G4R connector start",
                      {"rc": startp.returncode}):
            return
        if not expect(_connector_healthy(),
                      "G4R connector healthy after start",
                      {"base": M.BASES["connector"]}):
            return
        # 9. Post-start invariants: DB grant metadata byte-identical to
        #    the STOPPED baseline (the row survived the unlocked gap
        #    untouched — no armed claimant existed, and with no local
        #    generation anywhere no heartbeat can have advanced it).
        after_row = _grant_row(sid)
        expect(after_row == stopped_row,
               "G4R DB grant metadata preserved across stop/release/start",
               {"stopped": stopped_row, "after": after_row})
        view = sync_view(owner, sid, source["id"])
        gv = (view or {}).get(FIELDS["grant"]) or {}
        expect((gv.get("db_grant") or {}).get("grant_id") == old_grant
               and gv.get("locally_executable") is False,
               "G4R start ⇒ db grant visible but locally_executable=false",
               gv)
    finally:
        # Belt: ANY failure path (guard/list/terminal/stop/release) still
        # frees the lock; an already-released session is a no-op. If the
        # connector was stopped but start never ran, say so honestly —
        # never leave the stack down silently.
        if holder.proc is not None:
            hrc = release_lock()
            expect(hrc == 0, "G4R finally-belt lock release rc0", hrc)
        if not started[0] and ev.get("g4r_container"):
            st, _raw, _rc = _connector_state(ev["g4r_container"])
            if st is not None and st.get("running") is False:
                expect(False, "G4R probe failed with connector STOPPED — "
                       "honest failure, start not attempted to mask it",
                       st)

    # 10. Post-start window: zero NEW runs and zero NEW ticks in 5s (no
    #    automatic re-run, no old-grant reuse); both ID sets non-None.
    time.sleep(5)
    after_ticks = tick_ids(owner, sid)
    after_runs = run_ids(owner, sid)
    if not expect(after_ticks is not None and after_runs is not None,
                  "G4R post-release ID sets readable (non-None)",
                  {"ticks": after_ticks is not None,
                   "runs": after_runs is not None}):
        return
    new_ticks = (after_ticks or set()) - (before_ticks or set())
    new_runs = (after_runs or set()) - (before_runs or set())
    expect(not new_ticks and not new_runs,
           "G4R 5s post-release window: zero new Run/tick (no auto "
           "re-run, no old-grant reuse)",
           {"new_ticks": sorted(new_ticks), "new_runs": sorted(new_runs)})

    # 11. Honest auth_required once the owner heartbeat lapses; the tick
    #     must be a NEW row, not the prewarm one.
    stale = wait_tick(owner, sid, FIELDS["auth_required"],
                      reason="owner_stale", timeout=150)
    expect(stale is not None and stale.get("id") not in before_ticks,
           "G4R post-start auth_required/owner_stale on a NEW tick",
           {"tick": (stale or {}).get("id"),
            "reason": (stale or {}).get(FIELDS["skip_reason"])})
    # 12. Fresh arm = NEW grant generation; firing resumes on NEW tick and
    #     NEW run ids (never the prewarm rows).
    again = arm(owner, sid)
    if not expect(again.status == 200
                  and again.body.get(FIELDS["arm_grant"]) not in
                  (None, old_grant),
                  "G4R fresh arm = NEW grant generation (old never reused)",
                  {"old": old_grant,
                   "new": again.body.get(FIELDS["arm_grant"])}):
        return

    def fresh_enqueued():
        for t in (list_ticks(owner, sid) or []):
            if (t.get(FIELDS["tick_outcome"]) == "enqueued"
                    and t.get("id") not in before_ticks
                    and t.get(FIELDS["tick_run_link"])
                    and t[FIELDS["tick_run_link"]] not in before_runs):
                return t
        return None
    resumed = wait_for(fresh_enqueued, 30)
    expect(resumed is not None,
           "G4R fresh arm resumes firing on a NEW tick with a NEW run",
           resumed)
    if resumed:
        wait_run_status(owner, sid, resumed[FIELDS["tick_run_link"]],
                        TERMINAL_RUN_STATES, timeout=60)
    ev["g4_restart"] = {
        "sync": sid, "source": source["id"], "old_grant": old_grant,
        "fresh_grant": again.body.get(FIELDS["arm_grant"]),
        "prewarm_tick": enq0.get("id"),
        "resumed_tick": (resumed or {}).get("id"),
        "before_ids": {"ticks": sorted(before_ticks or set()),
                       "runs": sorted(before_runs or set())}}


# ------------------------------------------------------------------ G1 -----
def scenario_g1(ev):
    org = str(uuid.uuid4())
    owner = M.Identity("p24g1-owner", org=org)
    source, target, sync = make_sync(owner, tag="g1",
                                     cron=EVERY_SECOND_QUARTZ6)
    expect(sync.status in (200, 201), "G1 sync create", sync.body)
    sid = sync.body["id"]
    body0 = sync_view(owner, sid, source["id"])
    expect(body0 is not None and enabled_is_true(body0)
           and body0.get(FIELDS["next_fire"]), "G1 enabled+next_fire",
           body0)
    arm_res = arm(owner, sid)
    expect(arm_res.status == 200 and arm_res.body.get(FIELDS["arm_armed"])
           is True and arm_res.body.get(FIELDS["arm_grant"]),
           "G1 arm", arm_res.body)
    expect(arm_res.body.get(FIELDS["arm_loop"]) is True,
           "G1 scheduler_loop_running (needs P24 env override)", arm_res.body)

    enq = wait_tick(owner, sid, "enqueued", timeout=30)
    ok_link = bool(enq and enq.get(FIELDS["tick_run_link"]))
    expect(ok_link, "G1 positive enqueued tick with run link", enq)
    patch_sync(owner, sid, {FIELDS["enabled"]: False})
    if ok_link:
        run = wait_terminal_run(owner, sid, enq[FIELDS["tick_run_link"]])
        if run:
            verify_published_artifact(owner, target["id"], run, total=151)
        ts = list_ticks(owner, sid) or []
        slots = [(t[FIELDS["tick_scheduled_for"]], t["schedule_revision"])
                 for t in ts]
        expect(len(slots) == len(set(slots)), "G1 no duplicate slot", slots)
        expect(all(int(t["schedule_revision"]) == 1 for t in ts),
               "G1 ticks revision stable while config unchanged", ts[:3])
    ev["g1"] = {"sync": sid}


# ------------------------------------------------------------------ G2 -----
def scenario_g2(ev):
    org = str(uuid.uuid4())
    owner = M.Identity("p24g2-owner", org=org)
    source, target, sync = make_sync(owner, tag="g2",
                                     cron=EVERY_SECOND_QUARTZ6)
    sid = sync.body["id"]
    arm2 = arm(owner, sid, base="connector2")
    expect(arm2.status == 200 and arm2.body.get(FIELDS["arm_armed"]) is True,
           "G2 arm on connector2 (aux up required)", arm2.body)
    expect(arm2.body.get(FIELDS["arm_loop"]) is True,
           "G2 aux instance loop_running=true (P24 override applied)",
           arm2.body)
    enq = wait_tick(owner, sid, "enqueued", timeout=30)
    expect(bool(enq and enq.get(FIELDS["tick_run_link"])),
           "G2 positive enqueued tick", enq)
    patch_sync(owner, sid, {FIELDS["enabled"]: False})
    if enq and enq.get(FIELDS["tick_run_link"]):
        wait_terminal_run(owner, sid, enq[FIELDS["tick_run_link"]])
    confirm_disabled(owner, sid)
    time.sleep(3)                                # bounded settle window
    ts = list_ticks(owner, sid) or []
    rs = list_runs(owner, sid) or []
    slots = [t[FIELDS["tick_scheduled_for"]] for t in ts]
    expect(len(slots) == len(set(slots)), "G2 one tick row per slot", slots)
    linked_ids = {t[FIELDS["tick_run_link"]] for t in ts
                  if t.get(FIELDS["tick_run_link"])}
    run_id_set = {r["id"] for r in rs}
    expect(len(linked_ids) == len([t for t in ts
                                   if t.get(FIELDS["tick_run_link"])]),
           "G2 tick->run association unique (ID set)", sorted(linked_ids))
    expect(linked_ids <= run_id_set,
           "G2 every enqueued tick's run exists in this sync's runs",
           {"linked": sorted(linked_ids), "runs": sorted(run_id_set)})
    expect(len(run_id_set) == len(linked_ids),
           "G2 runs ID set == enqueued tick run links (<=1 run/slot, 2 "
           "instances)", {"runs": len(run_id_set),
                          "enqueued": len(linked_ids)})
    ev["g2"] = {"sync": sid, "runs": sorted(run_id_set),
                "enqueued": sorted(linked_ids)}


# ------------------------------------------------------------------ G3 -----
def scenario_g3(ev):
    org = str(uuid.uuid4())
    owner = M.Identity("p24g3-owner", org=org)
    source, target, sync = make_sync(owner, tag="g3",
                                     cron=EVERY_MINUTE_UNIX5)
    sid = sync.body["id"]
    rev0 = get_sync(owner, sid).body[FIELDS["revision"]]
    arm_res = arm(owner, sid)
    expect(arm_res.status == 200, "G3 arm", arm_res.body)

    same = patch_sync(owner, sid, {FIELDS["cron"]: EVERY_MINUTE_UNIX5})
    body1 = get_sync(owner, sid).body
    expect(same.status in (200, 201)
           and body1[FIELDS["revision"]] == rev0,
           "G3 same-value PATCH keeps revision (separate no-bump case)",
           body1[FIELDS["revision"]])
    g1v = (body1.get(FIELDS["grant"]) or {}).get("db_grant") or {}
    expect(g1v.get("schedule_revision") == rev0 and g1v.get("live") is True,
           "G3 same-value PATCH keeps grant live", g1v)

    patch_sync(owner, sid, {FIELDS["cron"]: "*/2 * * * *"})       # change 1
    body2 = get_sync(owner, sid).body
    expect(body2[FIELDS["revision"]] == rev0 + 1, "G3 rev after cron change",
           body2[FIELDS["revision"]])
    g2v = (body2.get(FIELDS["grant"]) or {}).get("db_grant") or {}
    expect(g2v.get("live") is False, "G3 stale_revision invalidates grant",
           g2v)

    patch_sync(owner, sid, {FIELDS["enabled"]: False})            # change 2
    body3 = sync_view(owner, sid, source["id"])
    expect(body3 is not None and cursor_is_null(body3)
           and not enabled_is_true(body3),
           "G3 disable NULLs cursor (omitempty-safe)",
           {"next": (body3 or {}).get(FIELDS["next_fire"]),
            "rev": (body3 or {}).get(FIELDS["revision"])})

    reen = patch_sync(owner, sid, {FIELDS["enabled"]: True})      # change 3
    body4 = get_sync(owner, sid).body
    expect(reen.status in (200, 201) and bool(body4[FIELDS["next_fire"]])
           and body4[FIELDS["revision"]] == rev0 + 3,
           "G3 THREE actual changes ⇒ revision rev0+3 with future cursor",
           {"rev": body4[FIELDS["revision"]],
            "next": body4[FIELDS["next_fire"]]})

    for label, patch_body in (
            ("cron garbage", {FIELDS["cron"]: "bad cron"}),
            ("cron 7 fields", {FIELDS["cron"]: "1 2 3 4 5 6 7"}),
            ("cron too long", {FIELDS["cron"]: "*/1 " * 52}),
            ("tz bad", {FIELDS["timezone"]: "Not/AZone"})):
        res = patch_sync(owner, sid, patch_body)
        stable = get_sync(owner, sid).body
        expect(res.status == 400
               and res.body.get("error") == FIELDS["invalid_schedule_code"]
               and isinstance(res.body.get("detail"), str)
               and stable[FIELDS["revision"]] == rev0 + 3,
               f"G3 invalid PATCH rejected, no mutation ({label})",
               {"status": res.status, "body": res.body})
    clr = patch_sync(owner, sid, {FIELDS["cron"]: ""})
    body5 = sync_view(owner, sid, source["id"])
    expect(clr.status in (200, 201) and body5 is not None
           and not enabled_is_true(body5) and cursor_is_null(body5),
           "G3 empty cron clears+disables (omitempty-safe)",
           (body5 or {}).get(FIELDS["enabled"]))

    res = api(ROUTES["sync_create"], owner.token,
              payload={"source_id": source["id"],
                       "output_dataset_id": target["id"],
                       FIELDS["cron"]: "", FIELDS["enabled"]: True})
    expect(res.status == 400
           and res.body.get("error") == FIELDS["invalid_schedule_code"],
           "G3 create enabled-without-cron rejected", res.body)
    ev["g3"] = {"sync": sid, "rev_path": [rev0, body1[FIELDS["revision"]],
                                          body2[FIELDS["revision"]],
                                          body4[FIELDS["revision"]]]}


# ------------------------------------------------------------------ G4 -----
def scenario_g4(ev):
    org = str(uuid.uuid4())
    owner = M.Identity("p24g4-owner", org=org)
    source, target, sync = make_sync(owner, tag="g4",
                                     cron=EVERY_SECOND_QUARTZ6)
    sid = sync.body["id"]
    # item 2: SAME owner subject+org; only lifetime is short (ttl=8s)
    short_tok = M.mint_token(sub=owner.sub, org=org, ttl=8)
    res = arm(owner, sid, token=short_tok)
    exp = res.body.get(FIELDS["arm_expires"]) if res.status == 200 else None
    expect(res.status == 200 and exp is not None,
           "G4 short-JWT (owner sub/org) arm returns capped expiry",
           res.body)
    wait_tick(owner, sid, "enqueued", timeout=20)
    expired = wait_tick(owner, sid, FIELDS["auth_required"],
                        reason="grant_expired", timeout=40)
    expect(expired is not None,
           "G4 post-expiry tick auth_required/grant_expired", expired)
    patch_sync(owner, sid, {FIELDS["enabled"]: False})

    source2, target2, sync2 = make_sync(owner, tag="g4x",
                                        cron=EVERY_MINUTE_UNIX5)
    sid2 = sync2.body["id"]
    arm2 = arm(owner, sid2, base="connector2")
    expect(arm2.status == 200, "G4 cross arm on aux", arm2.body)
    expect(arm2.body.get(FIELDS["arm_loop"]) is True,
           "G4 aux loop_running=true", arm2.body)
    view_base = get_sync(owner, sid2).body
    grant_view = (view_base.get(FIELDS["grant"]) or {})
    expect((grant_view.get("db_grant") or {}).get("grant_id")
           == arm2.body.get(FIELDS["arm_grant"])
           and grant_view.get("locally_executable") is False,
           "G4 base sees db grant but locally_executable=false", grant_view)
    again = arm(owner, sid2)
    expect(again.status == 200
           and again.body.get(FIELDS["arm_grant"])
           != arm2.body.get(FIELDS["arm_grant"]),
           "G4 re-arm fresh grant_id",
           {"first": arm2.body.get("grant_id"),
            "second": again.body.get("grant_id")})
    rv = revoke(owner, sid2)
    expect(rv.status == 200, "G4 revoke", rv.body)
    # Root fake-green fix (twin of the finally_owned site): judge "no db
    # grant" ONLY through the strict view (200/dict/id/source) with a
    # valid authorization projection — a failed GET is a FAIL, never an
    # implicit "gone".
    view2 = sync_view(owner, sid2, source2["id"])
    proj2 = (view2 or {}).get(FIELDS["grant"])
    expect(view2 is not None and isinstance(proj2, dict)
           and proj2.get("db_grant") is None,
           "G4 revoked ⇒ strict view OK and no db grant",
           {"view_ok": view2 is not None, "projection": proj2})
    ev["g4"] = {"sync": sid, "cross_sync": sid2}
    # R4: the restart proof is REAL code but only executes in the formal
    # phase (explicit flag; root release). Without the flag this records
    # an honest skip — never a placeholder PASS.
    if RESTART_PROBE:
        g4_restart_probe(ev)
    else:
        ev["g4"]["restart_probe"] = (
            "SKIPPED honestly: requires --with-restart-probe (formal "
            "phase; strict guard4 + graceful restart + health are "
            "executed only under root release)")


# ------------------------------------------------------------------ G5 -----
def scenario_g5(ev):
    org = str(uuid.uuid4())
    owner = M.Identity("p24g5-owner", org=org)
    # REAL shared admission gates on OUR source: cap=1 + 5min interval.
    # Occupier = SHORT bounded delay (6s page-2 of a 2-row fixture) that
    # fits INSIDE the explicitly configured Source request timeout (20s)
    # and the P1 HTTP budget (30s) — no 151x delay, no unjoined thread
    # (root resumed-review).
    source, target, sync = make_sync(
        owner, tag="g5", cron=EVERY_SECOND_QUARTZ6, page_size=1, total=2,
        admission={"max_inflight_runs": 1, "min_interval_ms": 300000},
        fixture_extra={"delay_ms": 6000, "delay_from_page": 2},
        request_timeout_ms=20000)
    sid = sync.body["id"]
    fsrc = OWNED[sid]["fixture_src"]

    result = {"error": None, "status": None, "body": None}

    def occupy():
        try:
            res = api(ROUTES["sync_run"].format(id=sid), owner.token,
                      method="POST", payload={})
            result["status"], result["body"] = res.status, res.body
        except Exception as exc:                          # noqa: BLE001
            result["error"] = str(exc)[:200]

    th = threading.Thread(target=occupy)
    th.start()

    def occupier_inflight():
        if (fixture_request_count(fsrc) or 0) < 2:
            return None
        ids = run_ids(owner, sid)
        return ids or None

    inflight = wait_for(occupier_inflight, 20)
    expect(inflight is not None, "G5 short occupier in flight",
           {"fx": fixture_request_count(fsrc)})
    occupier_run = next(iter(inflight)) if inflight else None

    arm_res = arm(owner, sid)
    expect(arm_res.status == 200, "G5 arm", arm_res.body)

    # window 1 — capacity refusal while the single slot is HELD:
    # full-state equality (files/txn/stamp/runs/fx); None = read failure
    before = full_state(owner, sid, source["id"], target["id"], fsrc)
    expect(before is not None, "G5 capacity window pre-state readable",
           before)
    cap = wait_tick(owner, sid, "skipped_admission_capacity", timeout=6)
    expect(cap is not None,
           "G5 typed capacity skip while cap=1 occupied by manual run", cap)
    time.sleep(2)
    after = full_state(owner, sid, source["id"], target["id"], fsrc)
    expect(after is not None, "G5 capacity window post-state readable",
           after)
    if before is not None and after is not None:
        expect(after == before,
               "G5 capacity-refusal ZERO effects (files/txn/stamp/runs/fx)",
               {"delta": {k: (before.get(k), after.get(k))
                          for k in before if before.get(k) != after.get(k)}})

    # thread MUST be joined and its real result checked
    th.join(timeout=30)
    expect(not th.is_alive(), "G5 occupier thread joined (no leak)",
           {"alive": th.is_alive()})
    expect(result["error"] is None and result["status"] in (200, 201, 202),
           "G5 occupier HTTP result observed ok",
           {"error": result["error"], "status": result["status"]})
    if occupier_run:
        wait_run_status(owner, sid, occupier_run, ("succeeded",), timeout=60)
    else:
        expect(False, "G5 occupier run id unknown — terminal check "
               "skipped honestly", {"inflight": inflight is not None})

    # window 2 — interval refusal after the occupier's terminal commit:
    # same full-state equality INCLUDING the admission stamp
    before2 = full_state(owner, sid, source["id"], target["id"], fsrc)
    expect(before2 is not None, "G5 interval window pre-state readable",
           before2)
    itv = wait_tick(owner, sid, "skipped_admission_min_interval",
                    timeout=15)
    expect(itv is not None,
           "G5 typed interval skip after terminal (5min interval)", itv)
    time.sleep(2)
    after2 = full_state(owner, sid, source["id"], target["id"], fsrc)
    expect(after2 is not None, "G5 interval window post-state readable",
           after2)
    if before2 is not None and after2 is not None:
        expect(after2 == before2,
               "G5 interval-refusal ZERO effects incl. stamp "
               "(files/txn/stamp/runs/fx)",
               {"delta": {k: (before2.get(k), after2.get(k))
                          for k in before2
                          if before2.get(k) != after2.get(k)}})
    patch_sync(owner, sid, {FIELDS["enabled"]: False})

    # another Source reaches a REAL successful terminal + COMMITTED
    sB, tB, syncB = make_sync(owner, tag="g5b", total=1, cron=None,
                              enabled=False)
    sidB = syncB.body["id"]
    manual = api(ROUTES["sync_run"].format(id=sidB), owner.token,
                 method="POST", payload={})
    expect(manual.status in (200, 201, 202),
           "G5 other-Source manual run accepted", manual.body)
    rsB = list_runs(owner, sidB) or []
    expect(bool(rsB), "G5 other-Source run exists", len(rsB))
    if rsB:
        runB = wait_terminal_run(owner, sidB, rsB[0]["id"], timeout=60)
        if runB:
            verify_published_artifact(owner, tB["id"], runB, total=1)

    # local-full/Stop are BOUND to real evidence, not HTTP PASS
    ev["g5"] = {"sync": sid, "other_sync": sidB,
                "occupier_run": occupier_run,
                "evidence_bindings": [
                    {"criterion": "local_full skip",
                     "bound_to": "internal/handlers/schedule_loop_test.go "
                                 "(reviewed new unit tests: coord.Reserve "
                                 "nil ⇒ Skips[local_full])",
                     "http_claimed": False},
                    {"criterion": "Stop semantics",
                     "bound_to": "formal P22 10-p22-full local full/Stop "
                                 "phases + P24 driver disable-owned-proof "
                                 "+ authz.Stop unit tests",
                     "http_claimed": False}]}


# ------------------------------------------------------------------ G6 -----
def scenario_g6(ev):
    orgA, orgB = str(uuid.uuid4()), str(uuid.uuid4())
    shared_sub = str(uuid.uuid4())
    in_a = M.Identity("p24g6-a", sub=shared_sub, org=orgA)
    in_b = M.Identity("p24g6-b", sub=shared_sub, org=orgB)
    personal = M.Identity("p24g6-personal")
    source, target, sync = make_sync(in_a, tag="g6",
                                     cron=EVERY_MINUTE_UNIX5)
    sid = sync.body["id"]

    res = get_sync(in_b, sid)
    expect(res.status == 404, "G6 same-sub dual-Org cannot see A's sync",
           {"status": res.status})
    res = arm(in_b, sid)
    expect(res.status in (403, 404), "G6 same-sub dual-Org arm blocked",
           {"status": res.status})

    _psrc, _ptgt, psync = make_sync(personal, tag="g6p",
                                    cron=EVERY_MINUTE_UNIX5)
    psid = psync.body["id"]
    res = arm(in_a, psid)
    expect(res.status in (403, 404),
           "G6 org token cannot arm personal-domain sync", res.status)
    res = api(f"{ROUTES['schedule_ticks'].format(id=psid)}?limit=50",
              in_a.token, method="GET")
    expect(res.status in (403, 404),
           "G6 org token cannot read personal-domain ticks", res.status)

    other_owner = M.Identity("p24g6-other", org=orgB)
    res = arm(other_owner, sid)
    expect(res.status in (403, 404),
           "G6 cross-TENANT arm blocked (tenant predicate scope; true "
           "parent-ID binding proof belongs to PG integration tests)",
           {"status": res.status})

    # root resumed-review + R1: same-OWNER weak JWT legitimately ARMS 200
    # (precheck is a Dataset GET); the REAL boundary shows at publish:
    # the scheduled Run must terminate FAILED with the REAL dataset-
    # versioning 403 attribution in run.error (an arbitrary failed/aborted
    # can never stand in), and the target's FULL transaction set plus file
    # IDs must be provably unchanged. Source pull MAY happen before the
    # commit refusal — fixture counts are reported, never claimed zero.
    wsrc, wtgt, wsync = make_sync(in_a, tag="g6w",
                                  cron=EVERY_SECOND_QUARTZ6)
    wsid = wsync.body["id"]
    wfsrc = OWNED[wsid]["fixture_src"]
    weak_tok = M.mint_token(sub=in_a.sub, org=orgA, permissions=[])
    weak_before = full_state(in_a, wsid, wsrc["id"], wtgt["id"], wfsrc)
    expect(weak_before is not None, "G6 weak pre-state readable",
           weak_before)
    res = arm(in_a, wsid, token=weak_tok)
    expect(res.status == 200,
           "G6 same-owner weak JWT ARMS 200 (arm precheck is Dataset GET)",
           res.body)
    enq = wait_tick(in_a, wsid, "enqueued", timeout=30)
    weak_run = enq.get(FIELDS["tick_run_link"]) if enq else None
    expect(bool(weak_run), "G6 weak-owner scheduled run enqueued", enq)
    if weak_run:
        wait_run_status(in_a, wsid, weak_run, ("failed", "aborted"),
                        timeout=60)
        rs = list_runs(in_a, wsid) or []
        row = next((r for r in rs if r["id"] == weak_run), {})
        err = str(row.get("error") or "")
        expect(row.get("status") == "failed"
               and "dataset-versioning" in err and "403" in err,
               "G6 weak-owner run FAILED with REAL dataset-versioning 403 "
               "attribution in run.error",
               {"status": row.get("status"), "error": err[:200]})
        weak_after = full_state(in_a, wsid, wsrc["id"], wtgt["id"], wfsrc)
        expect(weak_after is not None, "G6 weak post-state readable",
               weak_after)
        if weak_before is not None and weak_after is not None:
            expect(weak_after["files"] == weak_before["files"]
                   and weak_after["txn_status"] == weak_before["txn_status"],
                   "G6 weak-owner publish denial ⇒ target FULL txn set + "
                   "file IDs unchanged (OPEN/ABORTED included)",
                   {"files_before": weak_before["files"],
                    "files_after": weak_after["files"],
                    "txn_before": weak_before["txn_status"],
                    "txn_after": weak_after["txn_status"]})
    patch_sync(in_a, wsid, {FIELDS["enabled"]: False})

    # SEPARATE boundary: a subject with NO Source read/write at all
    # (random sub, no grants) is refused at the arm chain itself
    no_src_tok = M.mint_token(sub=str(uuid.uuid4()), org=orgA,
                              permissions=[])
    res = arm(in_a, sid, token=no_src_tok)
    expect(res.status == 403,
           "G6 no-Source-permission subject arm 403 (distinct from the "
           "Dataset-write boundary above)", {"status": res.status})
    ev["g6"] = {"sync": sid, "personal_sync": psid, "weak_sync": wsid,
                "weak_run": weak_run,
                "parent_binding_proof": "deferred to PG "
                "schedule_integration_test.go (tenant/parent cases)"}


# ------------------------------------------------------------------ G7 -----
def scenario_g7(ev):
    org = str(uuid.uuid4())
    owner = M.Identity("p24g7-owner", org=org)
    _s1, _t1, sync1 = make_sync(owner, tag="g7", cron=None, enabled=False)
    sid = sync1.body["id"]
    body = sync_view(owner, sid, _s1["id"])
    expect(body is not None and not enabled_is_true(body)
           and cursor_is_null(body)
           and body.get(FIELDS["timezone"], "UTC") == "UTC",
           "G7 pre-existing def dormant (disabled, NULL cursor, UTC)",
           {k: (body or {}).get(k) for k in (FIELDS["enabled"],
                                             FIELDS["next_fire"],
                                             FIELDS["timezone"])})
    res = patch_sync(owner, sid, {"file_glob": "*.json"})
    expect(res.status in (200, 201), "G7 name-only legacy PATCH ok",
           {"status": res.status})

    # positive firing sync then REAL disable proof by ID SETS (item 5)
    _s2, _t2, sync2 = make_sync(owner, tag="g7f",
                                cron=EVERY_SECOND_QUARTZ6)
    sid2 = sync2.body["id"]
    arm(owner, sid2)
    enq = wait_tick(owner, sid2, "enqueued", timeout=30)
    expect(bool(enq and enq.get(FIELDS["tick_run_link"])),
           "G7 positive enqueued tick before disable", enq)
    if enq and enq.get(FIELDS["tick_run_link"]):
        wait_terminal_run(owner, sid2, enq[FIELDS["tick_run_link"]])
    patch_sync(owner, sid2, {FIELDS["enabled"]: False})
    confirm_disabled(owner, sid2)                    # persisted disable
    before = {"ticks": tick_ids(owner, sid2), "runs": run_ids(owner, sid2)}
    time.sleep(10)                                   # bounded window
    after = {"ticks": tick_ids(owner, sid2), "runs": run_ids(owner, sid2)}
    new = {k: sorted((after[k] or set()) - (before[k] or set()))
           for k in ("ticks", "runs")}
    expect(before["ticks"] is not None and after["ticks"] is not None
           and not any(new.values()),
           "G7 disabled ⇒ ID-SET proof of no further firing (positive "
           "history, not 0==0)", {"new": new,
                                  "ticks": len(before["ticks"] or ()),
                                  "runs": len(before["runs"] or ())})
    ev["g7"] = {"sync": sid, "firing_sync": sid2, "window_s": 10,
                "counts": {k: [len(before[k] or ()), len(after[k] or ())]
                           for k in ("ticks", "runs")},
                "new_ids": new,
                "pinned_defs_verified_by":
                    "p24_live_guard + verify_preflight A/B (def-level GET "
                    "of the 4 pinned rows is guard territory)"}


SCENARIOS = [("g1", scenario_g1), ("g2", scenario_g2), ("g3", scenario_g3),
             ("g4", scenario_g4), ("g5", scenario_g5), ("g6", scenario_g6),
             ("g7", scenario_g7)]


# --------------------------------------------- global finally (item 1) ------
def finally_owned(window_s=8):
    """Disable/revoke + confirm EVERY owned schedule, prove runs
    terminal, persisted disabled/NULL cursor, and a bounded no-new-IDs
    window — all with loops still ON. Runs even after scenario
    failures; every per-owned failure is CAUGHT, RECORDED, and the loop
    CONTINUES to the other owned syncs; anything invalid gates
    cleanup.ok (a failed strict list is NEVER coerced to [])."""
    rec = {"per_sync": {}, "window_s": window_s, "read_errors": []}
    token_for = {sid: (CURRENT_TOKENS.get(info["tag"])
                       or _LAST_OWNER[0])
                 for sid, info in OWNED.items()}
    for sid in sorted(OWNED):
        tok = token_for.get(sid)
        info = OWNED[sid]
        entry = {"tag": info["tag"], "source_id": info["source_id"],
                 "target_id": info["target_id"],
                 "fixture_src": info["fixture_src"], "suffix": SUFFIX}
        rec["per_sync"][sid] = entry
        if not tok:
            entry["error"] = "no owner token retained"
            continue
        b = _Bearer(tok)
        try:
            res = api(ROUTES["sync_patch"].format(id=sid), tok,
                      method="PATCH", payload={FIELDS["enabled"]: False})
            entry["disable_status"] = res.status
        except Exception as exc:                          # noqa: BLE001
            entry["disable_error"] = str(exc)[:150]
        try:
            view = get_sync(b, sid)
            body = view.body if view.status == 200 and isinstance(
                view.body, dict) and view.body.get("id") == sid else None
            entry["get_confirmed"] = bool(
                body is not None and not enabled_is_true(body)
                and cursor_is_null(body))
        except Exception as exc:                          # noqa: BLE001
            entry["get_error"] = str(exc)[:150]
        try:
            entry["persisted_confirmed"] = persisted_disabled(sid)
        except Exception as exc:                          # noqa: BLE001
            entry["persisted_error"] = str(exc)[:150]
        try:
            rv = revoke(b, sid)
            entry["revoke_status"] = rv.status
            # Root fake-green fix: judge "grant gone" ONLY through the
            # STRICT sync view (200/dict/id/source) AND a structurally
            # valid authorization projection. A failed or malformed GET is
            # an explicit error — never equal to "no grant".
            gv = sync_view(b, sid, info["source_id"])
            if gv is None:
                entry["grant_gone"] = False
                entry["grant_view_error"] = (
                    "strict GET failed after revoke (not 'no grant')")
            elif not isinstance(gv.get(FIELDS["grant"]), dict):
                entry["grant_gone"] = False
                entry["grant_view_error"] = (
                    "authorization projection missing/invalid after revoke")
            else:
                entry["grant_gone"] = (
                    gv[FIELDS["grant"]].get("db_grant") is None)
        except Exception as exc:                          # noqa: BLE001
            entry["revoke_error"] = str(exc)[:150]
        try:
            st = runs_state(b, sid)
            if st is None:
                entry["runs_state"] = "INVALID (strict list failed)"
            else:
                entry["runs_terminal"] = all(
                    s in TERMINAL_RUN_STATES for s in st.values())
                entry["runs_state_unresolved"] = {
                    k: v for k, v in st.items()
                    if v not in TERMINAL_RUN_STATES}
        except Exception as exc:                          # noqa: BLE001
            entry["runs_error"] = str(exc)[:150]

    def sets():
        out, valid = {}, True
        for sid in sorted(OWNED):
            tok = token_for.get(sid)
            if not tok:
                valid = False
                continue
            t = _Bearer(tok)
            ti, ri = tick_ids(t, sid), run_ids(t, sid)
            if ti is None or ri is None:
                valid = False          # invalid read is NOT an empty set
                rec["read_errors"].append(sid)
                continue
            out[sid] = {"ticks": ti, "runs": ri}
        return out, valid

    before, valid_before = sets()
    time.sleep(window_s)
    after, valid_after = sets()
    rec["before"] = {s: {k: sorted(v) for k, v in d.items()}
                     for s, d in before.items()}
    rec["after"] = {s: {k: sorted(v) for k, v in d.items()}
                    for s, d in after.items()}
    rec["new_ids"] = {}
    for s, d in after.items():
        bb = before.get(s)
        if bb is None:
            continue
        rec["new_ids"][s] = {
            "ticks": sorted(d["ticks"] - bb["ticks"]),
            "runs": sorted(d["runs"] - bb["runs"])}
    all_covered = set(before) == set(OWNED) and set(after) == set(OWNED)
    per = rec["per_sync"].values()
    confirms_ok = bool(OWNED) and all(
        e.get("disable_status") in (200, 201)
        and e.get("get_confirmed") is True
        and e.get("persisted_confirmed") is True
        and e.get("revoke_status") == 200
        and e.get("grant_gone") is True
        and e.get("runs_terminal") is True
        for e in per)
    no_new = not any(v for d in rec["new_ids"].values() for v in d.values())
    rec["ok"] = bool(confirms_ok and no_new and valid_before
                     and valid_after and all_covered and not
                     rec["read_errors"])
    return rec


def run_all(env_dir, official, window_s):
    global EV, SUFFIX, CLEANUP
    EV = []
    SUFFIX = time.strftime("%H%M%S") + uuid.uuid4().hex[:4]
    load_helpers(env_dir)
    per = {}
    failed = []
    rc = [1]
    try:
        for name, fn in SCENARIOS:
            start = len(EV)
            sub = {}
            try:
                fn(sub)
                sub["status"] = ("pass" if EV[start:] and
                                 all(i["ok"] for i in EV[start:]) else "fail")
            except Exception as exc:                     # noqa: BLE001
                sub["status"] = "error"
                sub["error"] = M.scrub(str(exc))[:300]
            finally:
                # R3: never raises (per-sync try/except inside) — a cleanup
                # exception cannot abort the loop or skip the global
                # finally below.
                disable_owned_by_tag(name)      # per-case best effort
            per[name] = sub
            if sub["status"] != "pass":
                failed.append(name)
    finally:
        # R3: the OUTERMOST finally ALWAYS attempts the global cleanup
        # first (own try/except so even a cleanup crash still leaves the
        # attempt result persisted), then writes the collision-safe
        # result. rc is computed HERE, after cleanup, on every path.
        try:
            CLEANUP = finally_owned(window_s)
        except Exception as exc:                          # noqa: BLE001
            CLEANUP = {"ok": False,
                       "error": (M.scrub(str(exc))[:200] if M
                                 else str(exc)[:200])}
        cleanup_ok = bool(CLEANUP and CLEANUP.get("ok"))
        # Root fix: official requires the COMPLETE scenario set — every
        # expected scenario actually ran AND passed — plus cleanup ok. An
        # accidentally truncated loop or early exit can never yield
        # official=true with a lucky empty `failed` list.
        expected = [name for name, _fn in SCENARIOS]
        scenarios_ok = (set(per) == set(expected)
                        and all(per.get(n, {}).get("status") == "pass"
                                for n in expected))
        rc[0] = 0 if (scenarios_ok and cleanup_ok) else 1
        result = {"client_date": time.strftime("%Y-%m-%d"),
                  "suffix": SUFFIX,
                  "official": bool(official and scenarios_ok and cleanup_ok),
                  "partial": (not scenarios_ok) or not cleanup_ok,
                  "counts": {"pass": sum(1 for v in per.values()
                                         if v["status"] == "pass"),
                             "fail": len(failed)},
                  # safe owned bindings — NEVER tokens/Authorization
                  "owned_sync_ids": {
                      sid: {"tag": info["tag"],
                            "source_id": info["source_id"],
                            "target_id": info["target_id"],
                            "fixture_src": info["fixture_src"],
                            "suffix": SUFFIX}
                      for sid, info in OWNED.items()},
                  "cleanup": CLEANUP,
                  "results": per,
                  "expectations": EV}
        out = TMP_DEFAULT / f"result_p24-{SUFFIX}.json"
        n = 0
        while out.exists():
            n += 1
            out = TMP_DEFAULT / f"result_p24-{SUFFIX}-r{n}.json"
        out.write_text(json.dumps(result, indent=1) + "\n")
        print(f"[p24-smoke] pass={result['counts']['pass']} "
              f"fail={result['counts']['fail']} cleanup_ok={cleanup_ok} "
              f"official={result['official']}")
        print(f"RESULT_FILE={out}")
    return rc[0]


def main():
    args = sys.argv[1:]
    if "--list-gates" in args:
        for k, v in GATES.items():
            print(f"{k}: {v}")
        return 0
    if "--contract-probe" in args:
        if not CONTRACT.exists():
            print(f"[p24-smoke] contract NOT landed: {CONTRACT}",
                  file=sys.stderr)
            return 3
        print(f"[p24-smoke] contract landed ({CONTRACT.name}); ROUTES "
              f"{sum(v is not None for v in ROUTES.values())}"
              f"/{len(ROUTES)} pinned; FIELDS "
              f"{sum(v is not None for v in FIELDS.values())}"
              f"/{len(FIELDS)} pinned")
        return 0
    env_dir = TMP_DEFAULT
    if "--env-dir" in args:
        env_dir = Path(args[args.index("--env-dir") + 1])
    window_s = 8
    if "--window" in args:
        window_s = int(args[args.index("--window") + 1])
    if "--confirm-http" not in args:
        print("[p24-smoke] refusing HTTP execution without --confirm-http "
              "(root release required; today is code + pure-selftest only)",
              file=sys.stderr)
        return 3
    global RESTART_PROBE
    RESTART_PROBE = "--with-restart-probe" in args
    return run_all(env_dir, official="--official" in args,
                   window_s=window_s)


if __name__ == "__main__":
    sys.exit(main())

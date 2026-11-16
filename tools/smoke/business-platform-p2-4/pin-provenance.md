# P2.4 live-guard pin provenance — for root review (2026-11-14)

Status: DRAFT. `p24_live_guard.py` stays `RELEASED_FOR_EXECUTION=False`
until root reviews this page AND the fresh preflight baseline. No guard
run against the live stack has happened; no switch has happened.

## 1. Authoritative inputs

* P2.3 completion: `/Users/allen/.codex/visualizations/2026/11/13/01a1d419-a586-76c2-b72b-8b773a80c7b6/handoffs/2026-11-14-p23/completion.md`
* P2.4 handoff: `…/handoffs/2026-11-14-p24/handoff.md`
* Frozen archive: `tools/smoke/business-platform-p2-3/results/2026-11-14-p23-final/`
  (its tools and results are frozen; this task only reads them).

## 2. Frozen-archive integrity (recomputed this session)

Command (read-only): recompute sha256 of every file under the archive
vs `hash-manifest.json`, and diff the file set (manifest excludes only
itself). Output: `entries: 695 / bad: 0 / missing: 0 /
actual_files_excl_manifest: 695 / unindexed: 0 / verify_rc=0`. Root has
independently recomputed the same 695/0 result (acknowledged in the
root review message of 2026-11-14).

## 3. The pin list is DERIVED, not transcribed

`derive_pins_p24.py` mechanically enforces, exiting non-zero on any
break (and, since the root round-1 fix, validating the generated module
with `ast.parse` + literal value equality before writing / in `--check`):

1. The four source files' SHA256 equal the frozen manifest:
   * `new-round-p21-rows/after-p21abc.json` — `9483aa590fd7729f9989d161965dd0d3b083fbc24d100a6a97e173d68a0c5b83`
   * `new-round-p21-rows/after-restart3-full.json` — `b0620203ce64c15b7d1c0a147028e21ada7ca410872baae4ed0f9733bda3000a`
   * `new-round-p21-rows/after-restart3-full-compare.json` — `437433819702d3f229c0a38005bd54175c0767e8411d3a135d4a5f3ae3ecc705`
   * `new-round-p21-rows/README.md` — `a08e7f7d13d99b6038a4012d21e9239feb64a4158d6b413f39372dd3f16a6880`
2. Both snapshots carry exactly the 4 handoff IDs and are identical on
   all 20 common fields; the archived compare verdict (rows=4,
   identical_on_common_set=true, diffs={}, field_count=20) is re-checked.
3. The two OLD rows equal the root-approved `p23_live_guard.py`
   `PINNED_EXCEPTIONS` on every overlapping field (imported, not copied).
4. Output `p24_pins.py`: 4 rows × 27 fields — the 24 frozen keys
   verbatim + `conn_org_id`/`def_org_id`/`run_org_id`.

## 4. The org triple is an INFERRED constraint (root wording kept)

The archived generating SQL for `org_id` was NOT preserved
(`run_owner_id`/`org_id` appear nowhere else in the archive), so which
of connections/batch_sync_defs/sync_runs the archived value came from
is unknown. The three extra pins are therefore NOT the three columns'
independently-archived historical values — they are a stricter
inferred-equality constraint (all three = archived org_id), **verified
by the fresh read-only 3-table collection on 2026-11-14** (see §6:
live `c.org_id = d.org_id = r.org_id = pinned org_id` for all 4 rows).
Any divergence blocks.

## 5. Per-row provenance and semantics

| row | id | generation | semantics | provenance caliber |
|---|---|---|---|---|
| old B | `74d62557-e4e4-4a1a-a5b9-d63f3415d626` | 96351e3b (P22-final round) | lease-loss fencing (`lease_expires_at` < `started_at`, gate false) | root-approved P23 guard pin + both frozen snapshots |
| old C | `c14b09f0-1341-4127-9e60-23abe7c36739` | 96351e3b | committed-unconfirmed (gate_entered true) | same |
| new B | `e08c0b07-4b08-4a88-ba6f-aad4f77eddf2` | 528455c2 (P23 round 10b) | lease-loss fencing | STRONG PARENT-CHAIN evidence — the ABC `result_p21.json` carries NO B run UUID; identity comes from the frozen full snapshots (creation + post-restart3, 20-field equal) |
| new C | `84055551-5d3b-4ba0-94a9-0bc8738d4335` | 528455c2 | committed-unconfirmed | EXPLICIT UUID: `logical_path "sync-84055551-….ndjson"`, `txn COMMITTED`, `lineage_run_match`, `artifact_sha256_matches_txn` in the same frozen result |

All four remain running/occupying Source quota; none may be cancelled,
terminalized, retriggered or rewritten to make a guard pass.

## 6. Fresh read-only live verification (2026-11-14, 00-preflight)

Root-authorized whitelist only (docker inspect/ps/volume-ls, stat .env
MODE, SELECT-only psql, fixture counter GET). Real rc + raw stdout +
stderr preserved per artifact, `-rN` collision-safe, failed attempts
kept. `verify_preflight_p24.py` → `all_ok=true` (rc=0):

* **A** controlled pending/running = EXACTLY the 4 pinned rows, every
  one of the 27 fields equal (incl. live org triple — closes §4).
* **B** real-datetime compare: all four `started_at` (latest
  `2026-11-13 22:02:50.746532+00`) < connector boot
  `2026-11-13T22:05:01.092139798Z` (restart3-era, image
  `sha256:a8200dd7…`), each parsed via `_parse_ts`, never raw strings.
* **C** fixture unchanged: StartedAt `2026-11-13T18:29:14.176785055Z`
  (exact pin), Running, PID1 `python3 -I /srv/rest_source.py 0.0.0.0
  8090` (Entrypoint null), RestartPolicy `no`.
* **D/E/F** base5 all running; the 3 named volumes present; `.env`
  mode 600 (values never read anywhere).
* **G** legacy b6ec49ab B/C (frozen P22 13-field projection, verbatim
  SQL) == frozen P23-final `00-baseline-manifest.json` values.
* **H** fixture counters: b6ec49ab trio == frozen baseline (0/0/0,
  mode ok); 96351e3b b=2/c=5 and 528455c2 b=2/c=5 request counts
  recorded for root review (not asserted — they postdate that baseline).
* **I** NO non-empty `schedule_cron` anywhere; the two interval columns
  (`virtual_table_sources_link.auto_register_interval_seconds`,
  `virtual_tables.update_detection_interval_seconds`) are all-NULL.

## 7. Boot-comparison semantics (the strict exception)

Adopted semantics (same as the root-approved P23 guard): each pinned
row's `started_at`, parsed as a REAL datetime, must be strictly BEFORE
the target container's docker-inspected `State.StartedAt` (strict
RFC3339). A literal "earlier than the ORIGINAL executing container's
boot" is unsatisfiable by construction (a run starts after its executor
boots); the live check subsumes it — every container alive at P24
switch time boots after all four `started_at`, so no live process can
still own any of them. Timeline: old B/C started 19:22 by the P22-era
connector (replaced at the P23 swap, baseline start 2026-11-13T21:09:27Z);
new B/C started 22:02 by the P23-swap connector (replaced at restart3,
boot 22:05:01Z, live-verified); P24 will boot its target later still.
The guard re-verifies against the live inspect at every run.
**Open question for root (确信度: 中): confirm this reading of
"实际原执行容器boot"; an alternative is additionally pinning the two
historical executor boots as frozen doc-level evidence.**

## 8. BLOCK matrix (guard exits 1, never forgives)

unknown row (incl. NULL started_at) / duplicate id / pinned row missing
(deleted or terminalized) / ANY of the 27 fields drifted (heartbeat,
lease, gate, status, identity, org, attempt, generation…) / started_at
NULL / started_at ≥ boot / unparseable timestamp / psql or inspect or
compose-ps not rc0 / compose ps returning ≠ exactly 1 container id.
No age/expiry exemption, no discovery, no auto-add, no writes; holder /
token VALUES are never selected (presence booleans only).

## 9. Non-goals

Legacy b6ec49ab B/C stay protected by the frozen P2.2 baseline
suites/projection re-run as P2.4 regressions — NOT by this guard. The
P23 guard and all frozen tools remain untouched (imported/invoked
only). Scheduling enablement, multi-instance dedup, missed-slot policy,
Source 429/local 503/cancel/Stop, parent-chain escalation scenarios all
await root's pinned contract (`contract-pinned.json`).

## 10. Session execution log (honest boundaries)

* Frozen-archive SHA recompute rc=0 (§2). Derive/selftest/py_compile
  rc=0 (first derive version had a broken generated-module assignment —
  root round-1 caught it; fixed, now ast-validated; the failed
  generation was console-only, not archived).
* Preflight attempt 1 preserved: fixture inspect template error
  (`.Config.Path` absent — container has Entrypoint/Cmd only) and two
  per-table SQL errors (assumed id+org_id columns) — raw `.txt`/`.err`/
  `.rc` kept, fixed in attempt 2 (column-probe per table). Attempt 2
  org_id mismatch exposed the missing `org_id` alias in guard ROW_SQL
  (root round-2); fixed with `r.org_id::text AS org_id` while keeping
  the three org pins. Attempt 3 all green.
* Guard strictness round-2 fixes applied: `compose ps` must be rc0 with
  EXACTLY one container id (multi-id refuses, no blind `[0]`); empty
  inspect stdout is a friendly BLOCK; same strictness applied to the
  preflight collector; selftest gained the matching small cases
  (19 ok, rc=0).
* NOT executed by this task: guard against live stack, any
  Docker/DB/HTTP mutation, tests, switch, restart, fixture touch.

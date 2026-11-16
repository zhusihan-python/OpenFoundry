# Root review input, CODE/PURE only

Readiness directory: `results/w10/Nsetup-P22-P23-ready-attempt1`.
Every `*-argv.json` is the actual executable argv. Candidate releases have
`root_accepted:false`, null expiry/authority and missing future deployment pins;
they cannot authorize resource calls. Root changes the release-file argv to its
actual private release and, for P22, changes both driver argv release paths to
that same file. Output paths remain exclusive and exact.

## N setup

Entry `regressions/adapters/p22_legacy_probe_entry.py --release-file ROOT --output EXACT`.
Root release must bind stage `p22-new-legacy-probe-setup`, suite `p22`, S, future
expiry, `allow_new_legacy_fixture_insert:true`, exact entry SHA, the exact13
`setup_tools_sha256` map, exclusive `setup_output`, accepted deployment record
and SHA, current typed roster and SHA, accepted historical BC baseline and SHA.

The initial typed roster can reuse actual post-ALL10 empty NEW-live proof. The
accepted BC baseline is `legacy-BC-safe27-2.json`, SHA
`81e4b14c363cd91cbd481ede2a837161d1879faff2b95df8d219d7611c6d12fa`.
The entry re-reads the whole legacy pending/running set, both historical safe27
rows, separate NULL protocols and full nine-field parents. It compares all
fields, including the original owner/requested_by distinctions. Extra rows fail.
Initial and final W10 guards independently protect old controlled9, CP8, full
parent/public views, actual source and every pinned container.

In memory it creates a fresh actor/org JWT, then exactly:

1. POST `http://127.0.0.1:17100/api/v1/data-connection/sources`, fresh name/query
   src, copied P1 rest_api config with `p26fixture:8090` URL/domain/egress allowlist.
   No fixture arm, trigger, restart or source fetch is issued.
2. POST `http://127.0.0.1:17078/v1/datasets`, fresh name, format json.
3. POST `http://127.0.0.1:17100/api/v1/data-connection/syncs`, those actual parent
   IDs and `schedule_enabled:false`; actual SQL confirms disabled/next-fire NULL.
4. Exact helper SQL INSERT of a fresh UUID, legacy mode, NULL recovery_protocol,
   NULL stage/lease columns, with atomic Def/source/tenant/actor and disabled
   schedule predicates. Registry entries must come from this invocation's
   successful HTTP receipts. It never adopts old C or converts a controlled Run.
5. Re-read whole legacy = exact historic2 + new1; actual full27, independent
   protocolNULL, unchanged full parents, disabled Def, public checkpoint404 and
   full authenticated Run list containing exactly that new Run.

Result contains actual-created IDs and evidence links. The typed candidate is
`setup_output/typed-new-N-candidate.json`: new full27 origin pointer, full parent,
actual public baseline and historic2 whole legacy baseline. Root accepts/pins
this file before P22. Partial failures preserve objects and INSERT attempt UUID
and original receipts; there is no auto-retry, cancel, clear or adoption.
Every actual command/request has a BEFORE receipt. Unix child rc is original;
spawn/timeout is unavailable with original_rc null. Secrets stay in memory and
all raw logs are scrubbed; public private-key leaks are rejected before body
persistence. Caller captures outer argv/stdout/stderr/original Unix rc once.

## P22 full11 and P23 full8

Use `p22-argv.json` / `p23-argv.json` exactly. NEW envdir is
`/private/tmp/openfoundry-p26-legacy-env`; main C17100, connector2 A17097,
connector3 B17098 and dataset proxy17099 are explicit. P23 includes real
`--allow-restart`. Guard-call counts are P22=0 and P23=1, in addition to the
adapter's actual initial guard. Candidate config matches workers1/queue64/
taskTTL300/authTTL300. P22 G requires B through NEW P22 proxy; C/A direct Dataset.

P22 Root `legacy_cancel_probe` is exactly the accepted new N run_id/def_id/
source_id/tenant/actor. Original N's owner resolution is scoped to this NEW
legacy row; original I/M historical read-only snapshots remain unchanged.
Root `deployment_record` must contain base3/oldfixture + fresh C/A/B/proxy actual
full CID/image/boot/running pins. P23 uses the actual accepted post-P22 identities,
including J's recreated A and I/M changed boots; it never adopts names.

P22 J Root fields: `released_for:[legacy-deadline-driver]`, current driver
path/SHA and exact `up-deadline:7`/`restore` argv, `legacy_env_dir`, exact
`driver_chain_file`, `w7_pg_observer:{cid:ACTUAL_FULL_PG_CID,user:openfoundry,
database:connectors}`, compose/deadline7 config SHA map, and predecessor path /
actual adapter SHA / evidence_root. I producer writes
`p26-w10-compose-predecessor-v1` at the Root exact predecessor file, binding
initialRootA → sameCID/image changedboot and exact C/A/B start once each; two
actual command/after-inspect receipt sets prove original rc0 and fourline state.
Driver verifies writer pin, initial anchor, receipt hashes and actual before
CID/image/labels/boot/config; J changes only A TTL7/auth300 and restores300 in
original finally. The Source timeout remains60000ms. A failure keeps its
original outcome and explicit driver restore proof/pending state; no name adoption.

Caller pins `active_tools_sha256` and actual source before/after, captures full
original stdout/stderr/Unixrc and archives suite env result/identities as NEW
results. P22 expects all D..N once11, P23 O..V once8; no green subset substitutes.
P23 real Source holder Popen and owned NOWAIT seams remain enabled; only actual
this-invocation Source IDs may be locked. No historical row is cancelled.

No actual N/P22/P23 stage has run from this package.

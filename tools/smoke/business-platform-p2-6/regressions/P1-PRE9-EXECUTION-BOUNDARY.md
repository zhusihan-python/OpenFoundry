# P1 pre9 minimum executable boundary

This stage is independent of P25 sidecars, N setup, and legacy A/B migration.
No legacy live run is included in this code/pure delivery.

```
python3 -I -B /Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/regressions/adapters/w10_suite_adapter.py \
  --stage p1pre9 --suite p1 --release-file ROOT_RELEASE_JSON \
  --candidate-source-file /Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w9/2026-11-15-w9-attempt1/04-s-parity.stdout \
  --env-dir /private/tmp/openfoundry-platform-p1-q3v9 --expect-guard-calls 0 \
  --record /Volumes/ExternalSD/projects/OpenFoundry/tools/smoke/business-platform-p2-6/results/w10/p1pre9-attempt1/adapter.json \
  -- --env-dir /private/tmp/openfoundry-platform-p1-q3v9
```

`ROOT_RELEASE_JSON` is the concrete Root-owned release file, not a shell token.
Required fields:

- `schema:p26-w10-release-v1`, `root_accepted:true`, `stage:p1pre9`, `suite:p1`,
  `expires_utc` future UTC, `candidate_source` the full frozen S.
- `suite_argv:["--env-dir","/private/tmp/openfoundry-platform-p1-q3v9"]`,
  `expect_guard_calls:0`, and an exclusive NEW `evidence_root` under results/w10.
- Actual `deployment_record` path/SHA and `typed_roster` path/SHA.
- `owned_sql_templates:["p1-new-target-check"]`, `serial_owned_window:true`.
  No aux/driver/N-probe fields are needed for P1.

Deployment record: `{source_sha256:S, containers:{actual_name:{cid:full64hex,
image:"sha256:full64hex",started_at:actualUTC}}}`. Base3 and the original q3
fixture are mandatory. Use actual post-HTTP10 images/boots; any included owned
A/B container must also still be Running at that pinned boot. The original
fixture boot remains 2026-11-13T18:29:14.176785055Z.

Typed input must be a real `p26-w10-typed-v1` projection of current post-HTTP10
unknown owners/runs, with actual full27, independent recovery_protocol, CP owner,
full safe parents, public checkpoint/Run baselines, and original evidence JSON
pointer/hash. Empty typed input is invalid once HTTP10 unknown rows exist. Guard
compares the actual whole old+NEW controlled/CP sets and rejects every shadow.
Root's accepted post-HTTP10 originals supply this input; no DB call is needed to
prepare its schema adaptation if those originals include all required columns.

The private existing q3 `.env` retains real `OF_P1_PG_PASSWORD`,
`OF_P1_JWT_SECRET`, `OF_P1_SERVICE_JWT`; values never go into release/results.
Resolved endpoints must be Dataset17078, Pipeline17081, MAIN Connector17088,
fixture17090. Original main cron/sweeper/recovery remain OFF. P1 creates fresh
suffix resources and never restarts the fixture or any service.

The nine original scenarios run from NEW copied `tree/business-platform-p1/smoke/smoke.py`.
No run.sh setup/up/down is called. A zero expected inner-guard count does not skip
the initial real current-source/CID/image/boot/whole-set guard.

The publish-failure CHECK uses only this invocation's actual HTTP-created target
UUID. Exact public schema/table/object absence and zero own ACTIVE work precede
install. Only after that proof and actual install attempt is bounded independent
outer-finally DROP authority established. Catalog failure/timeout/existing object
never permits DROP; expiry cannot strand the invocation's own installed CHECK.

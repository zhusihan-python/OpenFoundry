# OpenFoundry agent entrypoint

Read `CLAUDE.md` and the `CLAUDE.md` in each affected module before changing it.
Use the Makefile and checked-in workflows as executable command sources.

Before implementation, record the observable acceptance conditions, affected
paths, and required checks. `python3 tools/quality/verification_plan.py BASE`
prints a conservative check plan for changes since the merge base with BASE.
Unknown paths expand the plan; the output is not evidence that checks ran.

For ZCode tasks, use one shared task, issue, or PR record following
`palantir-research/knowledge-base/design/ZCode交付与验收模板.md` §1.
Codex records affected entry points, data, outputs, and how to verify each.
GLM-5.4 runs the checks via ZCode GUI and links evidence for the tested revision
in that record. Codex reviews the final diff, has GLM verify any newly affected
paths, and records the verdict there.

For completion, attach evidence for the final code revision. Distinguish
passed, failed, skipped and blocked. UI changes need browser verification;
mocked UI tests do not prove backend integration. Required real-backend suites
that skip remain incomplete. Record deployed versions and explicit stubs.

When correcting recurring mistakes, add a regression test, executable boundary,
or improve the API. Do not disable checks or broaden exceptions to hide errors.
Changes to validation rules or baseline entries need independent human review.

Use `.github/pull_request_template.md`. The author must not approve their own
sensitive change. Identity, authorization, migrations and public contracts keep
the review requirements in CLAUDE.md and CONTRIBUTING.md. No automatic merging
is introduced by this policy.

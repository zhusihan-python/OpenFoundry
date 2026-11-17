# OpenFoundry agent entrypoint

Read `CLAUDE.md` and the `CLAUDE.md` in each affected module before changing it.
Use the Makefile and checked-in workflows as executable command sources.

Before implementation, record the observable acceptance conditions, affected
paths, and required checks. `python3 tools/quality/verification_plan.py BASE`
prints a conservative check plan for changes since the merge base with BASE.
Unknown paths expand the plan; the output is not evidence that checks ran.

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

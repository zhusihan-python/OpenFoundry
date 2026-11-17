# Verification and review policy

Start at AGENTS.md. Existing CLAUDE.md files remain the module conventions;
this policy adds acceptance evidence rather than duplicating architecture.

Run `python3 tools/quality/verification_plan.py BASE_REF` before implementation.
The plan includes committed, working-tree and untracked files. Deleted and
renamed paths must still influence selection. Unknown/shared changes expand to
all categories. The result means `not-run`, never `passed`.

| Category | Existing execution entrypoint |
| --- | --- |
| go-unit | `make test` |
| go-integration | `make test-integration` (Docker required) |
| contracts | `make contracts-check` and `make capabilities-check` |
| frontend | frontend lint, check, test:unit, build and relevant Playwright tests |
| real-excel | real suite and prerequisites in apps/web/e2e/fixtures/README.md |
| infra-review | affected Helm/Terraform workflow plus deployment review |
| docs-links | build the docs and check changed links |
| human-review | actual reviewer; cannot be satisfied by an agent's self-report |

`features/dataset-excel-import.json` is the first Feature Map. It links the
observable workflow to existing code and tests; capability catalog snapshots
remain API inventories, not proof of runtime correctness.

Use the PR template's acceptance-to-evidence table. Evidence needs final code
SHA, dirty state, actual runtime versions, environment and explicit mocks.
Skipped required tests and unavailable environments are incomplete validation.
Validation changes need negative controls proving both rejection and acceptance.

Frontend validation entrypoints require `human-review`: engineering tools under
`apps/web/scripts/`, `apps/web/package.json` (which can change verification
commands), and root-level `apps/web/{playwright,vite,vitest}[.<variant>].config.*`
files with JavaScript/TypeScript extensions (`js`, `mjs`, `cjs`, `ts`, `mts`,
`cts`). This includes real-suite and E2E server variants such as
`playwright.ofexcel.config.ts` and `vite.p31-e2e.config.ts`. Ordinary product
source and individual E2E specs do not gain a review requirement merely by
living under `apps/web/`; existing sensitive-path rules still apply.
Removing or renaming an entrypoint retains the review requirement through its
old path. A selected `human-review` category requests an actual independent
reviewer; the plan neither records an approval nor executes any check.

CODEOWNERS now routes to the repository owner's real account — every entry,
because GitHub assigns no code owner for nonexistent teams and the last
matching pattern wins without falling back to the catch-all. This does not
configure GitHub rules or prove an approval exists. Before enabling auto-merge,
configure required checks and independent review with actual collaborators;
a PR authored by the owner still needs another eligible reviewer where approval
is required. Identity, authorization, migrations and contracts remain human-reviewed.

Pilot metrics: repeated defect classes, human review time, post-merge rework and
rollbacks. Expand the policy after repeatable clean-environment runs, not on PR
volume alone. No automatic merges or new production verification endpoints are
introduced here.

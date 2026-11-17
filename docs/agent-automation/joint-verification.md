# Joint verification of the quality pilot

The five draft PRs remain independently reviewable. Each original workflow keeps its PR triggers and original checks, and also accepts `workflow_call`. On a complete combined tree, `.github/workflows/quality-integration.yml` calls those five exact workflows from the caller's commit. It adds contract/SDK drift checking, a documentation build without deployment, source provenance, and an aggregate of every automatic result.

The joint entry runs on pushes to `codex/quality-integration-*` and `codex/dataset-pipeline-file-io`, without path filters. It does not depend on `workflow_dispatch` being present on the default branch. Do not push the standalone policy PR as a combined verification candidate: that branch intentionally does not contain the architecture or Excel workflow. Its existing policy self-tests remain independent. The joint workflow requires the complete combined tree.

## Construct and verify a candidate

Record each source PR SHA and its current review status before constructing a validation candidate. Validation may run before independent human approval; successful automation does not supply that approval. In a separate clone or worktree, start at the common pilot base and merge the recorded source commits in order: architecture (#1), Go (#4), frontend (#3), Excel (#5), policy (#2). Use `git merge --no-ff` for this audit candidate. Record each resulting merge parent, and verify every recorded source SHA is an ancestor of the candidate using `git merge-base --is-ancestor SOURCE_SHA HEAD`. This ancestry audit belongs to the current candidate's report; the permanent workflow does not hard-code old PR commit IDs or require a future target merge strategy.

Run `git status --porcelain=v1`, record `git rev-parse HEAD HEAD^{tree}`, and push the resulting clean candidate to a fresh `codex/quality-integration-LABEL` branch. Do not update the target branch or merge the draft PRs to obtain this evidence. Find the resulting `Joint quality verification` run for the exact candidate SHA and wait for all jobs to finish. A successful source-branch run from a different SHA is not combined evidence.

The reviewed pilot lint baseline is `8d8543437da7f14b168b3667f1eb784892c629fc`. Preflight requires that exact full, nonzero commit to be an ancestor of the candidate and different from HEAD. The caller passes it explicitly to the Go workflow. The Go reusable entry independently rejects empty, malformed, nonexistent, unrelated, all-zero, or HEAD baselines; it never falls back to an event base for a present-but-invalid reusable input. Standalone Go PR/push runs retain their real event base. Updating this pilot baseline requires human review; do not substitute a moving target branch or a merge base that has advanced to HEAD.

## What success means

Every execution job verifies its checkout equals `GITHUB_SHA` and records the source tree hash and tool versions in its logs. The joint preflight also uploads source provenance. The callee workflow references are local, so all original jobs operate on the same caller commit. Existing race checks, frontend checks, Playwright assertions/skips, real-backend Excel evidence guards, architecture boundaries, and policy tests are retained.

The aggregate requires success from preflight, all five workflow calls, contracts, and documentation. Any failure, cancellation, skip, missing result, or unexpected result fails closed. A cancelled workflow may itself prevent final execution; cancellation is never a successful combined result. Human review and repository approvals remain separate: neither the helper nor the aggregate manufactures an approval, and successful automation does not authorize merge or release.

SQLC remains explicitly `not-integrated`: the Go policy gate verifies configuration/inventory consistency, while generation is not applicable. Its green result is not evidence of production SQLC query coverage.

## Excel acceptance and fallback

On a complete combined tree, prefer `bash tools/smoke/dataset-excel/run.sh` for local real-backend acceptance. It builds a disposable PostgreSQL, dataset-service, and gateway stack and verifies both real-backend cases with structured evidence. The original manual command and prerequisites remain in `docs/agent-automation/features/dataset-excel-import.json` under `real_check`; standalone PRs without the runner must use that fallback. The feature map is metadata and does not automatically execute a missing combined-tree runner.

For each run preserve the candidate SHA/tree, five validated source SHAs and their review status and ancestry results, lint baseline, workflow URL, every job conclusion, tool versions, structured Excel evidence, and any documented skips or N/A states. Report package-level Go success as package-level evidence when the command does not print individual case names. Keep credentials and raw browser traces out of published evidence.

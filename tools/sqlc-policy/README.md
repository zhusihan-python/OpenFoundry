# sqlc-policy — honest status gate for SQLC integration

SQLC is **not integrated** in this repository. This tool keeps that status
explicit and prevents the repo or CI from claiming sqlc validation it does
not have. It adds no policy powers beyond configuration honesty; it does not
verify query correctness, SQL ownership, or data residency.

## Usage (single entry for local and CI)

```sh
go run ./tools/sqlc-policy             # policy gate (default mode)
go run ./tools/sqlc-policy --generate  # generation entry (`make gen-sqlc`)
```

## Declaration

`sqlc-policy.yaml` at the repo root carries exactly three non-empty string
(`!!str`) keys: `status` (`not-integrated` | `integrated`), `reason`, and
`enable_when`. Unknown keys, duplicate keys, missing or non-string values,
or an unknown status fail the gate.

## What the policy gate checks (default mode)

- `sqlc.yaml` parses and is strictly shaped: single YAML document only
  (a second or trailing document is rejected instead of silently
  ignored), no duplicate keys at the top level or within a sql entry
  (a trailing `sql: []` cannot mask an earlier real block; duplicates
  nested deeper, inside `gen.*`, are left to real sqlc to reject once
  integrated), `version` must be the string `"2"`, and only the
  top-level keys `version` and `sql` are supported. sqlc 1.27's v2
  config also defines `cloud`, `servers`, `overrides`, `plugins`,
  `rules` and `options` — the gate rejects those as unsupported rather
  than accepting shapes it has not validated. `sql` must be a list; a
  missing, null, mapping, or scalar `sql` is rejected — it is never
  treated as a zero-entry list.
- While `not-integrated`: `sql` must be empty; the file inventory (git
  tracked + untracked, honoring .gitignore — node_modules and friends are
  never walked; paths are read NUL-separated via `ls-files -z` so space
  or non-ASCII names survive intact) may contain exactly one sqlc config,
  the root `sqlc.yaml`; and no `.go` file may start with a real sqlc
  generated header. A nested `sqlc.{yaml,yml,json}` or a new generated
  file fails the gate. The header scan is a constraint on the claimed
  status, not a complete proof of generation provenance.
- While `integrated`: every `sql` entry must carry `engine`, `schema`,
  `queries`, and a `gen.<language>.out`; generation is then enforced by
  the `--generate` entry.

## Modes and exit codes

- Default: `0` policy pass (status stated in the message — "generation
  not-applicable" makes no generation claim), `1` policy violations,
  `2` operational errors (missing files, unparseable YAML, multiple YAML
  documents, non-git root, git failure). Fail-closed: the gate cannot
  degrade to a silent PASS.
- `--generate`: funnels through the same policy gate first —
  `make gen-sqlc` cannot bypass it, and a contradictory or corrupt
  config fails here too. While `not-integrated` it prints an explicit
  `generation not-applicable` line (and appends it to the GitHub step
  summary when `$GITHUB_STEP_SUMMARY` is set) and exits 0; while
  `integrated` it runs a real `sqlc generate` and propagates failure —
  the CLI reports a single exit 1, not sqlc's original exit code. It
  deliberately performs **no drift check**: updating generated
  files is the point, and `make gen` must be able to fix drift. Drift
  verification is CI's separate `git status` step, which stays vacuously
  clean while not integrated.

## Verification performed when this gate landed

Real `sqlc` v1.27.0 against a minimal sqlite fixture: one successful
generate, a second generate leaving the tree unchanged (no drift), and
stub-binary tests proving generation failures propagate instead of
fake-passing. See the unit tests for the full negative-control list.

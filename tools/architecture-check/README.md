# Dataset architecture pilot

Run `go test ./tools/architecture-check` and `go run ./tools/architecture-check`
from the repository root. No services or databases are started.

The Go AST scanner checks every production `.go` file under
`services/dataset-versioning-service`, including files behind build tags and
OS/arch suffixes (`parser.ImportsOnly` ignores constraints by design). It rejects:

- raw broker clients (`github.com/nats-io/*`, `github.com/segmentio/kafka-go`,
  `github.com/confluentinc/*`, `github.com/IBM/sarama`) and the data bus
  (`libs/event-bus-data`) anywhere in the service;
- imports of any *other* service's packages, public or internal;
- in `internal/domain` (including nested subpackages): `database/sql`, `pgx`,
  and this service's own `internal/repo` and `internal/handlers`.

Shared contracts (`libs/proto-gen`), the control bus, and persistence inside
the repo layer remain allowed. Import paths match at package-element
boundaries: a rule for `kafka-go` covers `kafka-go/sasl` but not the unrelated
package `kafka-go-metrics`, and `pgx` does not cover `pgtype`.

Exit codes: `0` pass, `1` boundary violations listed, `2` operational error —
missing or empty scope (zero production `.go` files scanned), unreadable
directory, or parse failure. The scanner therefore cannot degrade to a silent
PASS when the tree is moved or renamed; it only reports `0` after scanning at
least one file.

## Relationship to Go's own rules

The Go toolchain already refuses to compile code outside
`services/dataset-versioning-service` that imports its `internal/...` tree.
This pilot adds what the compiler does not express: other services' *public*
packages are also off-limits (use versioned contracts instead), in-service
layering (`domain` must not know `repo`/`handlers`) is invisible to `internal`,
and raw-broker bans are a product policy, not a language rule.

## What this is not

An import-boundary pilot, not proof of data residency, runtime SQL ownership,
or transitive dependency isolation: a banned package can still reach dataset
code through an allowed import's own dependencies, and nothing here inspects
SQL text or storage locations. Test files are outside the production boundary.
There are currently no exceptions: adding one requires a narrow scope,
documented reason, responsible reviewer and negative-control coverage.

## Expanding to the next domain

Copy the pattern only after inventorying that domain's actual dependency paths
(imports, build tags, test layout) and writing negative controls for its
real aliases and near-miss package names. Scope guard first: the new domain
must fail on missing or empty scope before any rule is trusted.

package postgres

import (
	"context"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/openfoundry/openfoundry-go/services/pipeline-build-service/internal/handler"
)

// This file is the single place where the P1.1 tenant boundary is turned
// into SQL. Every public repository query funnels its row visibility
// through these helpers so list endpoints cannot scan foreign tenants and
// id-based lookups cannot bypass the boundary.
//
// Rules (see handler.TenantScope):
//
//   - Organization request (TenantID set): rows must carry the same
//     tenant_id. Admin claims do NOT widen this.
//   - Personal request (TenantID nil): rows must have tenant_id NULL and
//     belong to the requesting subject (legacy pre-tenant data).
//   - No scope in context (internal callers, unauthenticated paths): the
//     legacy unfiltered SQL runs unchanged. Authenticated HTTP routes
//     always carry a scope because the auth middleware guarantees claims.

// scopeFromContext resolves the effective tenant scope for a repository
// call. An explicit scope (background workers) wins over claims.
func scopeFromContext(ctx context.Context) *handler.TenantScope {
	return handler.TenantScopeFromContext(ctx)
}

// ErrDatasetTenantBoundaryUnsupported lives in the handler package (the
// postgres package imports handler; the reverse would be an import
// cycle). Scoped callers that reach a dataset-schema helper without the
// org columns fail closed with it instead of running unfiltered SQL.

// sqlBuilder assembles WHERE clauses with positional placeholders while
// keeping argument order in lockstep with the generated SQL.
type sqlBuilder struct {
	clauses []string
	args    []any
}

func (b *sqlBuilder) placeholder(value any) string {
	b.args = append(b.args, value)
	return "$" + strconv.Itoa(len(b.args))
}

func (b *sqlBuilder) and(clause string) {
	if clause != "" {
		b.clauses = append(b.clauses, clause)
	}
}

func (b *sqlBuilder) whereSQL() string {
	if len(b.clauses) == 0 {
		return ""
	}
	return " AND " + strings.Join(b.clauses, " AND ")
}

// ownerColumn names the column that identifies the owning subject of a
// tenant-NULL (personal-domain) row for each tenanted table.
const (
	ownerPipelines       = "owner_id"     // pipelines.owner_id UUID
	ownerBuilds          = "requested_by" // builds.requested_by TEXT (subject uuid string)
	ownerJobSpecs        = "created_by"   // job_specs.created_by TEXT
	ownerPipelineSpecs   = "published_by" // pipeline_job_specs.published_by UUID
	ownerSubmissionsUUID = "created_by"   // pipeline_run_submissions.created_by UUID
)

// tenantComparison emits the boundary comparison for `ref` (table or
// alias) and records its argument on the builder, so it can be embedded
// both in plain WHERE clauses and inside EXISTS subqueries.
func tenantComparison(b *sqlBuilder, scope *handler.TenantScope, ref, ownerCol string, ownerIsText bool) string {
	if scope.TenantID != nil {
		return ref + ".tenant_id = " + b.placeholder(*scope.TenantID)
	}
	var subject any = scope.Subject
	if ownerIsText {
		subject = scope.Subject.String()
	}
	return "(" + ref + ".tenant_id IS NULL AND " + ref + "." + ownerCol + " = " + b.placeholder(subject) + ")"
}

// appendTenantClause adds the tenant boundary for `table` (optionally with
// an alias, e.g. "builds b") to the builder and reports whether a clause
// was added.
func appendTenantClause(b *sqlBuilder, scope *handler.TenantScope, table string) bool {
	if scope == nil {
		return false
	}
	ref := table
	if idx := strings.LastIndex(table, " "); idx >= 0 { // "pipelines p" → alias "p"
		ref = strings.TrimSpace(table[idx+1:])
	}
	switch {
	case strings.Contains(table, "pipeline_run_submissions"):
		b.and(tenantComparison(b, scope, ref, ownerSubmissionsUUID, false))
	case strings.Contains(table, "pipeline_job_specs"):
		b.and(tenantComparison(b, scope, ref, ownerPipelineSpecs, false))
	case strings.Contains(table, "job_specs"):
		b.and(tenantComparison(b, scope, ref, ownerJobSpecs, true))
	case strings.Contains(table, "builds"):
		b.and(tenantComparison(b, scope, ref, ownerBuilds, true))
	default:
		b.and(tenantComparison(b, scope, ref, ownerPipelines, false))
	}
	return true
}

// appendPipelineParentClause bounds rows that hang off the pipelines table
// through fkExpr (e.g. "pipeline_runs.pipeline_id") by their parent
// pipeline's tenant. Used by pipeline_runs / pipeline_versions queries.
func appendPipelineParentClause(b *sqlBuilder, scope *handler.TenantScope, fkExpr string) bool {
	if scope == nil {
		return false
	}
	comparison := tenantComparison(b, scope, "p", ownerPipelines, false)
	b.and("EXISTS (SELECT 1 FROM pipelines p WHERE p.id = " + fkExpr + " AND " + comparison + ")")
	return true
}

// appendBuildParentClause bounds jobs rows by their owning build's tenant.
// Used by jobs / job_logs queries (job_logs resolve through jobs).
func appendBuildParentClause(b *sqlBuilder, scope *handler.TenantScope, jobsAlias string) bool {
	if scope == nil {
		return false
	}
	comparison := tenantComparison(b, scope, "b", ownerBuilds, true)
	b.and("EXISTS (SELECT 1 FROM builds b WHERE b.id = " + jobsAlias + ".build_id AND " + comparison + ")")
	return true
}

// appendDatasetOrgClause bounds queries that read the co-located
// dataset-versioning tables (datasets/dataset_branches) by the dataset's
// owning domain. The dataset service owns that schema and names the
// columns org_id / owner_id (see its 20261113000000_dataset_org_tenancy
// migration); the caller must have verified those columns exist before
// embedding this clause (see Repository.datasetOrgCapable).
func appendDatasetOrgClause(b *sqlBuilder, scope *handler.TenantScope) bool {
	if scope == nil {
		return false
	}
	if scope.TenantID != nil {
		b.and("d.org_id = " + b.placeholder(*scope.TenantID))
		return true
	}
	b.and("(d.org_id IS NULL AND d.owner_id = " + b.placeholder(scope.Subject) + ")")
	return true
}

// tenantValueForInsert returns the tenant_id to persist for a row created
// by the caller behind ctx. A nil scope keeps NULL (legacy callers only;
// authenticated HTTP routes always carry a scope).
func tenantValueForInsert(ctx context.Context) *uuid.UUID {
	if scope := scopeFromContext(ctx); scope != nil {
		return scope.TenantID
	}
	return nil
}

// submitterForInsert returns the creating subject for pipeline_run_submissions.
func submitterForInsert(ctx context.Context) *uuid.UUID {
	if scope := scopeFromContext(ctx); scope != nil {
		return &scope.Subject
	}
	return nil
}

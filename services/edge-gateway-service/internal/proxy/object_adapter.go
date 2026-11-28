package proxy

import (
	"net/http"
	"strings"

	"github.com/google/uuid"

	authmw "github.com/openfoundry/openfoundry-go/libs/auth-middleware"
	"github.com/openfoundry/openfoundry-go/services/edge-gateway-service/internal/errs"
)

// objectWritePermission is what a subject needs to create, update or delete
// ontology objects: the key the project already uses
// (docs/poc-online-retail/README.md, libs/auth-middleware/rbac_test.go).
// [authmw.Claims.HasPermissionKey] decides it, admin and `ontology:*` included.
const objectWritePermission = "ontology:write"

// The object adapter's routes sit under the collection `objects` of an
// ontology type; POST to the collection's `query` child is a read.
const (
	objectAdapterPrefix = "/api/v1/ontology/types/"
	objectCollection    = "objects"
	objectQuery         = "query"
)

// objectAccess is what a request forwarded to the object adapter does to the
// objects of an organization.
type objectAccess int

const (
	// notObjectAdapter marks a request that is not forwarded to the object adapter.
	notObjectAdapter objectAccess = iota
	// objectRead lists, fetches or queries objects.
	objectRead
	// objectWrite creates, updates or deletes them.
	objectWrite
)

// objectAccessOf classifies a request by the path the gateway forwards and
// its HTTP method.
//
// The object adapter (object-database-service, internal/server/server.go)
// mounts these routes behind the gateway, which forwards the path unchanged:
//
//	GET    /api/v1/ontology/types/{type_id}/objects/             list
//	POST   /api/v1/ontology/types/{type_id}/objects/             create
//	POST   /api/v1/ontology/types/{type_id}/objects/query        query
//	GET    /api/v1/ontology/types/{type_id}/objects/{object_id}  fetch
//	PATCH  /api/v1/ontology/types/{type_id}/objects/{object_id}  update
//	DELETE /api/v1/ontology/types/{type_id}/objects/{object_id}  delete
//
// Every path under the collection is classified, deeper ones included, and
// GET and POST …/query are the only reads. So nothing the adapter dispatches
// falls outside the classification, and no method it could dispatch to a
// write is taken for a read.
func objectAccessOf(path, method string) objectAccess {
	typeAndBelow, ok := strings.CutPrefix(path, objectAdapterPrefix)
	if !ok {
		return notObjectAdapter
	}
	_, belowType, _ := strings.Cut(typeAndBelow, "/")
	if belowType != objectCollection && !strings.HasPrefix(belowType, objectCollection+"/") {
		return notObjectAdapter
	}
	if method == http.MethodGet ||
		(method == http.MethodPost && belowType == objectCollection+"/"+objectQuery) {
		return objectRead
	}
	return objectWrite
}

// authorizeObjectAdapter guards the boundary where the gateway forwards
// ontology objects to the object adapter, and returns the organization the
// request is bound to.
//
// objectAdapter is the route table's answer that the request goes down the
// object adapter's branch ([selectRoute]), whatever addresses the upstreams
// are given. Of the paths that branch takes, the adapter's own routes alone
// are gated: objectAccessOf tells them from the rest and says whether the
// request reads or writes.
//
// The request gets through only with a verified identity that names an
// organization — the all-zero UUID included — which its session scope allows.
// That organization is the partition the adapter reads and writes in; a
// tenant of the client's own is never used. Reading takes no permission,
// writing takes ontology:write.
//
// The second result is true when the gateway denied the request and wrote
// the answer itself. Both results are zero for a request that is not the
// object adapter's.
func authorizeObjectAdapter(w http.ResponseWriter, r *http.Request, claims *authmw.Claims, objectAdapter bool) (*uuid.UUID, bool) {
	if !objectAdapter {
		return nil, false
	}
	access := objectAccessOf(r.URL.Path, r.Method)
	if access == notObjectAdapter {
		return nil, false
	}
	if claims == nil {
		errs.Write(w, http.StatusUnauthorized, errs.CodeAuthenticationRequired,
			"object routes require a verified identity")
		return nil, true
	}
	if claims.OrgID == nil {
		errs.Write(w, http.StatusForbidden, errs.CodeOrganizationRequired,
			"object routes require an organization")
		return nil, true
	}
	if !claims.AllowsOrgID(claims.OrgID) {
		errs.Write(w, http.StatusForbidden, errs.CodeScopedSessionOrgDenied,
			"session scope does not allow this organization")
		return nil, true
	}
	if access == objectWrite && !claims.HasPermissionKey(objectWritePermission) {
		errs.Write(w, http.StatusForbidden, errs.CodeObjectWriteForbidden,
			"requires one of: "+objectWritePermission)
		return nil, true
	}
	return claims.OrgID, false
}

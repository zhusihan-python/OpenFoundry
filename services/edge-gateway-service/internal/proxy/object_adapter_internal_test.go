package proxy

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestObjectAccessOf(t *testing.T) {
	t.Parallel()
	const collection = "/api/v1/ontology/types/ProductCost/objects"
	cases := []struct {
		method, path string
		want         objectAccess
	}{
		// The six routes the object adapter mounts.
		{http.MethodGet, collection, objectRead},
		{http.MethodGet, collection + "/", objectRead},
		{http.MethodPost, collection, objectWrite},
		{http.MethodPost, collection + "/", objectWrite},
		{http.MethodPost, collection + "/query", objectRead},
		{http.MethodGet, collection + "/0196a1b2-0000-7000-8000-000000000001", objectRead},
		{http.MethodPatch, collection + "/0196a1b2-0000-7000-8000-000000000001", objectWrite},
		{http.MethodDelete, collection + "/0196a1b2-0000-7000-8000-000000000001", objectWrite},
		// Deeper in the collection, and with an empty type id: still the adapter's subtree.
		{http.MethodGet, collection + "/abc/links", objectRead},
		{http.MethodPost, collection + "/abc/links", objectWrite},
		{http.MethodPost, "/api/v1/ontology/types//objects", objectWrite},
		// Not the query route: an object id, so not a read.
		{http.MethodPost, collection + "/query/", objectWrite},
		{http.MethodGet, collection + "/query", objectRead},
		// Methods the adapter does not mount, and a lowercase one.
		{http.MethodPut, collection + "/abc", objectWrite},
		{http.MethodHead, collection, objectWrite},
		{"get", collection, objectWrite},
		// Not the object adapter's routes.
		{http.MethodGet, "/api/v1/ontology/types/ProductCost", notObjectAdapter},
		{http.MethodPost, "/api/v1/ontology/types/ProductCost/objects-links", notObjectAdapter},
		{http.MethodPost, "/api/v1/ontology/types/ProductCost/other/objects", notObjectAdapter},
		{http.MethodGet, "/api/v1/ontology/types-else/ProductCost/objects", notObjectAdapter},
		{http.MethodPost, "/api/v1/datasets/abc/objects", notObjectAdapter},
		{http.MethodPost, "/api/v1/ontology/objects/abc/rule-runs", notObjectAdapter},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, objectAccessOf(tc.path, tc.method), "%s %s", tc.method, tc.path)
	}
}

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/openfoundry/openfoundry-go/services/edge-gateway-service/internal/config"
)

func TestSelectRouteFlagsTheObjectAdapterBranch(t *testing.T) {
	t.Parallel()
	// Every ontology upstream behind one address, as services may be: the flag
	// still tells the object adapter's branch from the branches that only look
	// like it, and SelectUpstream keeps answering the upstream.
	const shared = "http://one-address:8080"
	u := config.UpstreamURLs{
		ObjectDatabase:     shared,
		OntologyActions:    shared,
		OntologyQuery:      shared,
		OntologyDefinition: shared,
		DatasetVersioning:  "http://datasets:8080",
	}
	cases := []struct {
		path     string
		upstream string
		want     bool
	}{
		// The object adapter's branch.
		{"/api/v1/ontology/types/X/objects", shared, true},
		{"/api/v1/ontology/types/X/objects/", shared, true},
		{"/api/v1/ontology/types/X/objects/abc", shared, true},
		{"/api/v1/ontology/types/X/objects/query", shared, true},
		// Wider than the adapter's own routes; objectAccessOf tells them apart.
		{"/api/v1/ontology/types/X/other/objects", shared, true},
		// Branches that only look like it.
		{"/api/v1/ontology/types/X/objects/knn", shared, false},
		{"/api/v1/ontology/types/X/objects/abc/inline-edit", shared, false},
		{"/api/v1/ontology/types/X/objects/abc/inline-edit/", shared, false},
		{"/api/v1/ontology/types/X/objects/inline-edit-batch", shared, false},
		{"/api/v1/ontology/types/X/rules", shared, false},
		// The object database's other route, and another service's.
		{"/api/v1/ontology/links/t/instances/abc", shared, false},
		{"/api/v1/ontology/types/X", shared, false},
		{"/api/v1/datasets/abc/objects", u.DatasetVersioning, false},
	}
	for _, tc := range cases {
		upstream, objectAdapter := selectRoute(tc.path, u)
		assert.Equal(t, tc.want, objectAdapter, tc.path)
		assert.Equal(t, tc.upstream, upstream, tc.path)
		assert.Equal(t, upstream, SelectUpstream(tc.path, u), "the entry point answers the upstream: %s", tc.path)
	}

	upstream, objectAdapter := selectRoute("/api/v1/totally-unknown", u)
	assert.Equal(t, "", upstream)
	assert.False(t, objectAdapter)
}

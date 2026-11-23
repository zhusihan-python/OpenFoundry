package models

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestS3SnapshotSource(t *testing.T) {
	cfg, err := ParseS3SnapshotSource(json.RawMessage(`{"url":"s3://orders-bucket/daily/","bucket":"orders-bucket","sync":{"object_key":"北京 1.ndjson"}}`))
	require.NoError(t, err)
	require.Equal(t, &S3SnapshotSource{Bucket: "orders-bucket", ObjectKey: "daily/北京 1.ndjson"}, cfg)

	legacy := json.RawMessage(`{"bucket":"orders-bucket","endpoint":"https://catalog.example","iceberg_tables":[]}`)
	cfg, err = ParseS3SnapshotSource(legacy)
	require.NoError(t, err)
	require.Nil(t, cfg)

	for _, raw := range []string{
		`{"bucket":"orders","sync":{"object_key":""}}`,
		`{"bucket":"orders","sync":{"object_key":"../private.ndjson"}}`,
		`{"url":"s3://first","bucket":"second","sync":{"object_key":"orders"}}`,
		`{"bucket":"orders","sync":{"object_key":"orders","incremental":{"enabled":true}}}`,
		`{"bucket":"orders","endpoint":"https://client-chosen.example","sync":{"object_key":"a"}}`,
		`{"bucket":"orders","secret_access_key":"redacted","sync":{"object_key":"a"}}`,
	} {
		_, err = ParseS3SnapshotSource(json.RawMessage(raw))
		require.Error(t, err)
	}
}

func TestGalleryAdvertisesOnlyImplementedNativeBatchSync(t *testing.T) {
	for _, configured := range []bool{false, true} {
		found := map[string]bool{}
		for _, c := range BuildGalleryCatalog(configured).Connectors {
			for _, capability := range c.Capabilities {
				if capability == "batch_sync" {
					found[c.Type] = true
				}
			}
		}
		want := map[string]bool{"rest_api": true}
		if configured {
			want["s3"] = true
		}
		require.Equal(t, want, found)
	}
}

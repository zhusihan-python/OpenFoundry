package models

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// S3SnapshotSource identifies one NDJSON object. Its endpoint and credentials
// are resolved by the service, never from the connection JSON.
type S3SnapshotSource struct {
	Bucket    string
	ObjectKey string
}

var s3BucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// ParseS3SnapshotSource returns nil for an existing catalog-only connection.
func ParseS3SnapshotSource(raw json.RawMessage) (*S3SnapshotSource, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil, nil // legacy catalog passthrough
	}
	if _, err := ParseSyncAdmissionPolicy(raw); err != nil {
		return nil, err
	}
	var sync map[string]json.RawMessage
	if block, ok := root["sync"]; ok {
		if err := json.Unmarshal(block, &sync); err != nil {
			return nil, nil // older adapters may own this field
		}
	}
	if _, selected := sync["object_key"]; !selected {
		return nil, nil
	}
	for field := range sync {
		if field != "object_key" && field != "admission" {
			return nil, fmt.Errorf("unsupported S3 sync setting %q", field)
		}
	}
	var cfg struct {
		Bucket string          `json:"bucket"`
		URL    string          `json:"url"`
		Sync   json.RawMessage `json:"sync"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, errors.New("S3 snapshot source accepts only bucket, url and sync; bind aws_keys through source credentials")
	}
	var key string
	if err := json.Unmarshal(sync["object_key"], &key); err != nil || key == "" || strings.HasPrefix(key, "/") {
		return nil, errors.New("S3 sync.object_key requires a relative object key")
	}
	if cfg.URL != "" {
		u, err := url.Parse(cfg.URL)
		if err != nil || u.Scheme != "s3" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (cfg.Bucket != "" && cfg.Bucket != u.Host) {
			return nil, errors.New("S3 source url requires s3://bucket[/prefix] with matching bucket")
		}
		cfg.Bucket = u.Host
		if prefix := strings.Trim(u.Path, "/"); prefix != "" {
			key = prefix + "/" + key
		}
	}
	if !s3BucketName.MatchString(cfg.Bucket) || strings.Contains(cfg.Bucket, "..") {
		return nil, errors.New("invalid S3 snapshot bucket name")
	}
	if len(key) > 1024 || strings.ContainsAny(key, "\x00\r\n") {
		return nil, errors.New("invalid S3 snapshot object key")
	}
	for _, segment := range strings.Split(key, "/") {
		if segment == "." || segment == ".." {
			return nil, errors.New("S3 snapshot key cannot contain dot segments")
		}
	}
	return &S3SnapshotSource{Bucket: cfg.Bucket, ObjectKey: key}, nil
}

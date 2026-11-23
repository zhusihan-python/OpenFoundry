package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
	"unicode/utf8"

	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/adapters/sigv4"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/domain/http_runtime"
)

type S3SnapshotOptions struct {
	Endpoint             string
	Region               string
	Bucket               string
	ObjectKey            string
	AccessKeyID          string
	SecretAccessKey      string
	SessionToken         string
	AllowedEgressHosts   []string
	AllowPrivateNetworks bool
	BeforePage           func(context.Context) error
}

const (
	maxS3ObjectBytes = 8 << 20
	maxS3ObjectRows  = 10000
)

// S3Endpoint validates the operator-configured origin and egress policy before
// the gallery advertises native reads and before each request is signed.
func S3Endpoint(endpoint string, allowedHosts []string, allowPrivate bool) (*url.URL, error) {
	target, err := url.Parse(endpoint)
	if err != nil || (target.Scheme != "http" && target.Scheme != "https") || target.Hostname() == "" || target.User != nil || target.RawQuery != "" || target.ForceQuery || target.Fragment != "" || (target.Path != "" && target.Path != "/") {
		return nil, errors.New("invalid S3 endpoint origin")
	}
	policy := http_runtime.EgressPolicy{AllowedHosts: allowedHosts, AllowPrivateNetworks: allowPrivate,
		AllowInsecureHTTP: allowPrivate && len(allowedHosts) > 0}
	if target.Scheme == "http" && !policy.AllowInsecureHTTP {
		return nil, errors.New("plain HTTP for S3 requires explicit private egress and an allowed host")
	}
	if err := http_runtime.ValidateURL(target, &policy); err != nil {
		return nil, err
	}
	return target, nil
}

// FetchS3Snapshot obtains exactly one signed object. No platform bearer, S3
// credential provider chain or upstream error body crosses this boundary.
func FetchS3Snapshot(ctx context.Context, opts S3SnapshotOptions) (*SnapshotResult, error) {
	target, err := S3Endpoint(opts.Endpoint, opts.AllowedEgressHosts, opts.AllowPrivateNetworks)
	if err != nil {
		return nil, err
	}
	if opts.Bucket == "" || opts.ObjectKey == "" || opts.Region == "" || opts.AccessKeyID == "" || opts.SecretAccessKey == "" {
		return nil, errors.New("missing S3 object location or signing credentials")
	}
	target.Path = "/" + opts.Bucket + "/" + opts.ObjectKey
	target.RawPath = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, errors.New("invalid S3 GetObject request")
	}
	if opts.BeforePage != nil {
		if err := opts.BeforePage(ctx); err != nil {
			return nil, err
		}
	}
	sigv4.SignRequest(req, nil, opts.AccessKeyID, opts.SecretAccessKey, opts.SessionToken, opts.Region, "s3", time.Now())
	req.Header.Set("Accept-Encoding", "identity")
	client := http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("S3 GetObject request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("S3 GetObject returned HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxS3ObjectBytes+1))
	if err != nil {
		return nil, errors.New("could not read complete S3 object")
	}
	if len(data) > maxS3ObjectBytes {
		return nil, errors.New("S3 object exceeds snapshot byte limit")
	}
	var out bytes.Buffer
	var rows int64
	for len(data) > 0 {
		line, rest, _ := bytes.Cut(data, []byte{'\n'})
		data = rest
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if line[0] != '{' || !utf8.Valid(line) || !json.Valid(line) {
			return nil, errors.New("invalid NDJSON object in S3 snapshot")
		}
		rows++
		if rows > maxS3ObjectRows {
			return nil, errors.New("S3 object exceeds snapshot row limit")
		}
		out.Write(line)
		out.WriteByte('\n')
	}
	b := out.Bytes()
	sum := sha256.Sum256(b)
	return &SnapshotResult{NDJSON: b, Rows: rows, Bytes: int64(len(b)), ContentHash: fmt.Sprintf("%x", sum), Pages: 1}, nil
}

package runtime

import (
	"net/url"
	"regexp"
)

var syncURLPattern = regexp.MustCompile(`https?://[^\s"'<>]+`)

// SanitizeSyncError removes URL query strings, fragments and userinfo from
// error text before it is persisted to run records or returned over HTTP.
//
// The whole query is dropped rather than filtering known-sensitive parameter
// names: source auth may place a credential behind ANY query parameter name
// (rest_api `api_key` with `query_param`), and timeout/connection errors carry
// the fully-encoded request URL, so both raw and percent-encoded credential
// forms must disappear.
func SanitizeSyncError(msg string) string {
	return syncURLPattern.ReplaceAllStringFunc(msg, func(candidate string) string {
		u, err := url.Parse(candidate)
		if err != nil || u.Host == "" {
			return candidate
		}
		u.User = nil
		u.RawQuery = ""
		u.ForceQuery = false
		u.Fragment = ""
		return u.String()
	})
}

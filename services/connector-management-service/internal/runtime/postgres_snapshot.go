package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/domain/http_runtime"
	"github.com/openfoundry/openfoundry-go/services/connector-management-service/internal/models"
)

// PostgresEndpoint accepts a single operator-owned TCP destination. Client credentials,
// pgx options, alternate hosts and local sockets cannot be supplied through this URL.
func PostgresEndpoint(raw string, allowed []string, private bool) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL endpoint")
	}
	database := strings.TrimPrefix(u.Path, "/")
	if u.Scheme != "postgresql" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.ContainsAny(u.Host, ",%/\\") || database == "" || strings.ContainsAny(database, "/\x00") || strings.HasSuffix(u.Host, ":") {
		return nil, errors.New("expected PostgreSQL endpoint postgresql://host[:port]/database")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return nil, errors.New("invalid PostgreSQL port")
		}
	}
	policy := http_runtime.EgressPolicy{AllowedHosts: allowed, AllowPrivateNetworks: private}
	if err := http_runtime.ValidateURL(u, &policy); err != nil {
		return nil, err
	}
	return u, nil
}

type PostgresSnapshotOptions struct {
	Endpoint             *url.URL
	Table                models.PostgresTable
	User, Password       string
	AllowPrivateNetworks bool
	BeforeRead           func(context.Context) error
}

// pgx resolves a host into individual numeric addresses before calling DialFunc.
func postgresDialAddressAllowed(network, address string, allowPrivate bool) bool {
	if network != "tcp" {
		return false
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	ip = ip.Unmap()
	if ip.IsMulticast() || ip.IsUnspecified() {
		return false
	}
	return allowPrivate || (ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast())
}

const primaryKeyQuery = `
SELECT c.relkind::text, array_agg(a.attname::text ORDER BY k.ord)
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
JOIN pg_catalog.pg_index i ON i.indrelid=c.oid AND i.indisprimary AND i.indisvalid
CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
JOIN pg_catalog.pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum
WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind IN ('r','p') AND k.ord<=i.indnkeyatts
GROUP BY c.relkind, i.indexrelid`

func FetchPostgresSnapshot(ctx context.Context, opts PostgresSnapshotOptions) (*SnapshotResult, error) {
	if opts.Endpoint == nil || opts.User == "" || opts.Password == "" {
		return nil, errors.New("missing PostgreSQL endpoint or source credential")
	}
	// Start from pgx's required parser, then set the connection identity and TLS
	// explicitly. Credentials must never fall back to a service account or .pgpass.
	cfg, err := pgx.ParseConfig("host=localhost port=5432 dbname=postgres user=unused password=unused sslmode=disable")
	if err != nil {
		return nil, errors.New("could not initialize PostgreSQL client")
	}
	u := opts.Endpoint
	cfg.Host = u.Hostname()
	if port := u.Port(); port != "" {
		p, err := strconv.ParseUint(port, 10, 16)
		if err != nil {
			return nil, errors.New("invalid PostgreSQL port")
		}
		cfg.Port = uint16(p)
	}
	cfg.Database = strings.TrimPrefix(u.Path, "/")
	cfg.User, cfg.Password = opts.User, opts.Password
	cfg.TLSConfig = &tls.Config{ServerName: cfg.Host, MinVersion: tls.VersionTLS12}
	cfg.Fallbacks = nil
	dial := cfg.DialFunc
	cfg.DialFunc = func(ctx context.Context, network, address string) (net.Conn, error) {
		if !postgresDialAddressAllowed(network, address, opts.AllowPrivateNetworks) {
			return nil, errors.New("PostgreSQL address blocked by egress policy")
		}
		return dial(ctx, network, address)
	}
	cfg.RuntimeParams = map[string]string{"client_encoding": "UTF8", "search_path": "pg_catalog", "timezone": "UTC", "DateStyle": "ISO, YMD"}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if opts.BeforeRead != nil {
		if err := opts.BeforeRead(ctx); err != nil {
			return nil, err
		}
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("PostgreSQL source connection failed")
	}
	defer func() { _ = conn.Close(ctx) }()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, errors.New("could not start read-only PostgreSQL snapshot")
	}
	defer func() { _ = tx.Rollback(ctx) }()
	table := pgx.Identifier{opts.Table.Schema, opts.Table.Name}.Sanitize()
	// Hold the relation identity across primary-key discovery and the ordered read.
	if _, err := tx.Exec(ctx, "LOCK TABLE ONLY "+table+" IN ACCESS SHARE MODE"); err != nil {
		return nil, errors.New("PostgreSQL source table is unavailable or unreadable")
	}
	var kind string
	var keys []string
	if err := tx.QueryRow(ctx, primaryKeyQuery, opts.Table.Schema, opts.Table.Name).Scan(&kind, &keys); err != nil || len(keys) == 0 {
		return nil, errors.New("PostgreSQL snapshot requires a table with a primary key")
	}
	order := make([]string, len(keys))
	for i, key := range keys {
		order[i] = pgx.Identifier{"src", key}.Sanitize()
	}
	// Bound both single-row transfer and total output. Read one extra row to
	// distinguish oversized tables from a complete snapshot at the limit.
	const maxRows = 10000
	const maxBytes = 8 << 20
	// A traditional inheritance child is a separate source whose keys need not
	// be unique in the parent. Declarative partitions belong to the logical table.
	from := table
	if kind == "r" {
		from = "ONLY " + table
	}
	q := fmt.Sprintf("SELECT CASE WHEN octet_length(row_to_json(src.*)::text) < $1 THEN row_to_json(src.*)::text END FROM %s AS src ORDER BY %s LIMIT $2", from, strings.Join(order, ", "))
	rows, err := tx.Query(ctx, q, maxBytes, maxRows+1)
	if err != nil {
		return nil, errors.New("PostgreSQL snapshot query failed")
	}
	defer rows.Close()
	var buf bytes.Buffer
	var count int64
	for rows.Next() {
		var row *string
		if err := rows.Scan(&row); err != nil {
			return nil, errors.New("could not read PostgreSQL snapshot row")
		}
		count++
		if count > maxRows || row == nil {
			return nil, errors.New("PostgreSQL snapshot exceeds row or byte limit")
		}
		if !utf8.ValidString(*row) {
			return nil, errors.New("invalid UTF-8 in PostgreSQL snapshot row")
		}
		if err := json.Compact(&buf, []byte(*row)); err != nil {
			return nil, errors.New("invalid PostgreSQL JSON row")
		}
		buf.WriteByte('\n')
		if buf.Len() > maxBytes {
			return nil, errors.New("PostgreSQL snapshot exceeds byte limit")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("PostgreSQL snapshot query failed")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, errors.New("could not complete PostgreSQL snapshot")
	}
	return &SnapshotResult{NDJSON: buf.Bytes(), Rows: count, Bytes: int64(buf.Len()), Pages: 1, ContentHash: fmt.Sprintf("%x", sha256.Sum256(buf.Bytes()))}, nil
}

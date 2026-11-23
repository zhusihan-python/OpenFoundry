package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
)

func mysqlIdentifier(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// mysqlDial resolves once and dials the validated numeric address. TLS still
// verifies the operator-provided hostname, including when it resolves privately.
func mysqlDial(private bool) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupHost(ctx, host)
		if err != nil {
			return nil, err
		}
		dialer := net.Dialer{}
		for _, ip := range ips {
			target := net.JoinHostPort(ip, port)
			if !databaseDialAddressAllowed(network, target, private) {
				continue
			}
			if conn, err := dialer.DialContext(ctx, network, target); err == nil {
				return conn, nil
			}
		}
		return nil, errors.New("no reachable permitted MySQL address")
	}
}

// FetchMySQLSnapshot publishes a complete InnoDB table or returns no data.
func FetchMySQLSnapshot(ctx context.Context, opts TableSnapshotOptions) (*SnapshotResult, error) {
	if opts.Endpoint == nil || opts.User == "" || opts.Password == "" {
		return nil, errors.New("missing MySQL endpoint or credential")
	}
	database := strings.TrimPrefix(opts.Endpoint.Path, "/")
	if opts.Table.Schema != database {
		return nil, errors.New("MySQL source schema must match the configured database")
	}
	cfg := mysql.NewConfig()
	port := opts.Endpoint.Port()
	if port == "" {
		port = "3306"
	}
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort(opts.Endpoint.Hostname(), port)
	cfg.DBName = database
	cfg.User = opts.User
	cfg.Passwd = opts.Password
	cfg.TLS = &tls.Config{ServerName: opts.Endpoint.Hostname(), MinVersion: tls.VersionTLS12}
	cfg.DialFunc = mysqlDial(opts.AllowPrivateNetworks)
	cfg.Collation = "utf8mb4_bin"
	cfg.Params = map[string]string{"time_zone": "'+00:00'"}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, errors.New("invalid MySQL source configuration")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if opts.BeforeRead != nil {
		if err := opts.BeforeRead(ctx); err != nil {
			return nil, err
		}
	}
	db := sql.OpenDB(connector)
	defer func() { _ = db.Close() }()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, errors.New("MySQL source connection failed")
	}
	defer func() { _ = tx.Rollback() }()
	table := mysqlIdentifier(database) + "." + mysqlIdentifier(opts.Table.Name)
	// The zero-row access requires SELECT and holds the metadata lock until
	// commit, so DDL cannot replace the table between discovery and reading.
	probe, err := tx.QueryContext(ctx, "SELECT * FROM "+table+" LIMIT 0")
	if err != nil {
		return nil, errors.New("MySQL source table is unavailable or unreadable")
	}
	if err = probe.Close(); err != nil {
		return nil, errors.New("MySQL source table probe failed")
	}
	var engine sql.NullString
	err = tx.QueryRowContext(ctx, "SELECT ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=?", database, opts.Table.Name).Scan(&engine)
	if err != nil || engine.String != "InnoDB" {
		return nil, errors.New("MySQL snapshot requires an InnoDB base table")
	}
	columns, err := mysqlNames(ctx, tx, "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=? ORDER BY ORDINAL_POSITION", database, opts.Table.Name)
	if err != nil || len(columns) == 0 {
		return nil, errors.New("MySQL source columns unavailable")
	}
	key, err := mysqlNames(ctx, tx, "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.STATISTICS WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=? AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX", database, opts.Table.Name)
	if err != nil || len(key) == 0 {
		return nil, errors.New("MySQL snapshot requires a primary key")
	}
	fields := make([]string, len(columns))
	args := make([]any, len(columns))
	for i, c := range columns {
		fields[i] = "?, " + mysqlIdentifier(c)
		args[i] = c
	}
	for i, c := range key {
		key[i] = mysqlIdentifier(c)
	}
	const maxBytes = 8 << 20
	const maxRows = 10000
	row := "CAST(JSON_OBJECT(" + strings.Join(fields, ", ") + ") AS CHAR CHARACTER SET utf8mb4)"
	query := fmt.Sprintf("SELECT IF(OCTET_LENGTH(%s)<%d,%s,NULL) FROM %s ORDER BY %s LIMIT %d", row, maxBytes, row, table, strings.Join(key, ", "), maxRows+1)
	rows, err := tx.QueryContext(ctx, query, append(args, args...)...)
	if err != nil {
		return nil, errors.New("MySQL snapshot query failed")
	}
	defer func() { _ = rows.Close() }()
	var result bytes.Buffer
	var count int64
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, errors.New("could not read MySQL snapshot row")
		}
		count++
		if count > maxRows || data == nil {
			return nil, errors.New("MySQL snapshot exceeds row or byte limit")
		}
		if !utf8.Valid(data) || json.Compact(&result, data) != nil {
			return nil, errors.New("invalid MySQL JSON row")
		}
		result.WriteByte('\n')
		if result.Len() > maxBytes {
			return nil, errors.New("MySQL snapshot exceeds byte limit")
		}
	}
	if rows.Err() != nil || tx.Commit() != nil {
		return nil, errors.New("could not complete MySQL snapshot")
	}
	return &SnapshotResult{NDJSON: result.Bytes(), Bytes: int64(result.Len()), Rows: count, Pages: 1, ContentHash: fmt.Sprintf("%x", sha256.Sum256(result.Bytes()))}, nil
}

func mysqlNames(ctx context.Context, tx *sql.Tx, query, schema, table string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, schema, table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

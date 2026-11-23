package models

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

type PostgresTable struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
}

// Only the dedicated postgres_table field opts a legacy connection into native reads.
func ParsePostgresSnapshotSource(raw json.RawMessage) (*PostgresTable, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if _, err := ParseSyncAdmissionPolicy(raw); err != nil {
		return nil, err
	}
	var probe struct {
		Sync struct {
			Table json.RawMessage `json:"postgres_table"`
		} `json:"sync"`
	}
	if json.Unmarshal(raw, &probe) != nil || len(probe.Sync.Table) == 0 {
		return nil, nil
	}
	var cfg struct {
		Sync struct {
			Table     PostgresTable   `json:"postgres_table"`
			Admission json.RawMessage `json:"admission"`
		} `json:"sync"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, errors.New("PostgreSQL snapshot accepts only sync.postgres_table and sync.admission; bind credentials separately")
	}
	table := cfg.Sync.Table
	for _, name := range []string{table.Schema, table.Name} {
		if strings.TrimSpace(name) == "" || len(name) > 63 || !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
			return nil, errors.New("PostgreSQL snapshot requires valid schema and table identifiers of at most 63 bytes")
		}
	}
	return &table, nil
}

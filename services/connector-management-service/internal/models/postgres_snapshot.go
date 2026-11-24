package models

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

type SnapshotTable struct {
	Schema string `json:"schema"`
	Name   string `json:"name"`
}

// ParsePostgresSnapshotSource opts in only configurations that set sync.postgres_table.
func ParsePostgresSnapshotSource(raw json.RawMessage) (*SnapshotTable, error) {
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
			Table     SnapshotTable   `json:"postgres_table"`
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

// ParseMySQLSnapshotSource opts in only configurations that set sync.mysql_table.
func ParseMySQLSnapshotSource(raw json.RawMessage) (*SnapshotTable, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if _, err := ParseSyncAdmissionPolicy(raw); err != nil {
		return nil, err
	}
	var probe struct {
		Sync struct {
			Table json.RawMessage `json:"mysql_table"`
		} `json:"sync"`
	}
	if json.Unmarshal(raw, &probe) != nil || len(probe.Sync.Table) == 0 {
		return nil, nil
	}
	// The admission gate reads exact JSON keys. Reject alternate spellings and
	// unknown nested settings before storing the original source config.
	var root map[string]json.RawMessage
	var sync map[string]json.RawMessage
	if json.Unmarshal(raw, &root) != nil || len(root) != 1 || json.Unmarshal(root["sync"], &sync) != nil || sync["mysql_table"] == nil {
		return nil, errors.New("expected sync.mysql_table")
	}
	for key := range sync {
		if key != "mysql_table" && key != "admission" {
			return nil, errors.New("unexpected MySQL snapshot setting")
		}
	}
	if admission, ok := sync["admission"]; ok {
		var policy SyncAdmissionPolicy
		decoder := json.NewDecoder(bytes.NewReader(admission))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&policy); err != nil {
			return nil, errors.New("invalid MySQL snapshot admission setting")
		}
	}
	var table SnapshotTable
	decoder := json.NewDecoder(bytes.NewReader(sync["mysql_table"]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&table); err != nil {
		return nil, errors.New("invalid MySQL snapshot table")
	}
	for _, name := range []string{table.Schema, table.Name} {
		if strings.TrimSpace(name) == "" || utf8.RuneCountInString(name) > 64 || strings.ContainsRune(name, 0) {
			return nil, errors.New("invalid MySQL table identifier")
		}
	}
	return &table, nil
}

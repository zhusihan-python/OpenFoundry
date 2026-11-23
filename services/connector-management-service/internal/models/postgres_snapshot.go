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
	var config struct {
		Sync struct {
			Table     SnapshotTable   `json:"mysql_table"`
			Admission json.RawMessage `json:"admission"`
		} `json:"sync"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, errors.New("MySQL snapshot accepts only sync.mysql_table and sync.admission; bind credentials separately")
	}
	table := config.Sync.Table
	for _, name := range []string{table.Schema, table.Name} {
		if strings.TrimSpace(name) == "" || utf8.RuneCountInString(name) > 64 || strings.ContainsRune(name, 0) {
			return nil, errors.New("invalid MySQL table identifier")
		}
	}
	return &table, nil
}

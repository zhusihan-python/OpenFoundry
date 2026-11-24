package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

type order struct {
	ID     string  `json:"order_id"`
	Amount *int64  `json:"amount_cents"`
	Batch  *string `json:"batch"`
}

type change struct {
	Op     string `json:"op"`
	Source struct {
		Connector string `json:"connector"`
		Name      string `json:"name"`
		Database  string `json:"db"`
		Schema    string `json:"schema"`
		Table     string `json:"table"`
		LSN       int64  `json:"lsn"`
		Snapshot  string `json:"snapshot"`
	} `json:"source"`
	Transaction *struct {
		ID    string `json:"id"`
		Order int64  `json:"total_order"`
	} `json:"transaction"`
	Before *order `json:"before"`
	After  *order `json:"after"`
}

// The orders contract requires all three columns and a non-null text key.
// Reject schema drift and unavailable TOAST values before advancing the checkpoint.
func (o *order) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	if len(fields) != 3 || fields["order_id"] == nil || fields["amount_cents"] == nil || fields["batch"] == nil {
		return errors.New("order schema changed")
	}
	type plain order
	if err := json.Unmarshal(raw, (*plain)(o)); err != nil {
		return err
	}
	if o.ID == "" || (o.Batch != nil && *o.Batch == "__debezium_unavailable_value") {
		return errors.New("incomplete order")
	}
	return nil
}

func parseChange(key, value []byte, prefix, database string) (change, string, string, error) {
	var c change
	var k struct {
		ID string `json:"order_id"`
	}
	if json.Unmarshal(key, &k) != nil || k.ID == "" || json.Unmarshal(value, &c) != nil {
		return c, "", "", errors.New("invalid order change or key")
	}
	if c.Source.Connector != "postgresql" || c.Source.Name != prefix || c.Source.Database != database || c.Source.Schema != "sales" || c.Source.Table != "orders" || c.Source.LSN <= 0 {
		return c, "", "", errors.New("unexpected CDC source or position")
	}
	if (c.Before != nil && c.Before.ID != k.ID) || (c.After != nil && c.After.ID != k.ID) {
		return c, "", "", errors.New("order key mismatch")
	}
	switch c.Op {
	case "r", "c":
		if c.After == nil || c.Before != nil {
			return c, "", "", errors.New("invalid create image")
		}
	case "u":
		if c.After == nil || c.Before == nil {
			return c, "", "", errors.New("update requires REPLICA IDENTITY FULL")
		}
	case "d":
		if c.After != nil || c.Before == nil {
			return c, "", "", errors.New("delete requires before image")
		}
	default:
		return c, "", "", errors.New("unsupported CDC operation")
	}
	if c.Op == "r" {
		return c, k.ID, fmt.Sprintf("snapshot:%d:%s", c.Source.LSN, k.ID), nil
	}
	if c.Transaction == nil || c.Transaction.ID == "" || c.Transaction.Order < 1 {
		return c, "", "", errors.New("missing Debezium transaction metadata")
	}
	return c, k.ID, fmt.Sprintf("tx:%s:%d", c.Transaction.ID, c.Transaction.Order), nil
}

func matches(a, b order) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}

func apply(rows map[string]order, key string, c change) ([]byte, error) {
	prior, exists := rows[key]
	if c.Before != nil {
		if !exists || !matches(prior, *c.Before) {
			return nil, errors.New("prior state differs from source before image")
		}
	} else if exists {
		return nil, errors.New("source created an existing order")
	}
	if c.Op == "d" {
		delete(rows, key)
	} else {
		rows[key] = *c.After
	}
	if len(rows) > 10000 {
		return nil, errors.New("order projection exceeds 10000 rows")
	}
	keys := make([]string, 0, len(rows))
	for k := range rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, k := range keys {
		if err := enc.Encode(rows[k]); err != nil {
			return nil, err
		}
		if buf.Len() > 1<<20 {
			return nil, errors.New("order projection exceeds 1 MiB")
		}
	}
	return buf.Bytes(), nil
}

func digest(b []byte) string { return fmt.Sprintf("%x", sha256.Sum256(b)) }

// Hold snapshot rows in the durable checkpoint until Debezium marks the last
// row. An initially empty table can begin directly with a streaming create.
func readyForPublish(c change, complete bool, previousOffset int64) (bool, error) {
	if c.Op != "r" {
		if !complete && previousOffset >= 0 {
			return false, errors.New("initial snapshot did not complete")
		}
		return true, nil
	}
	if complete {
		return false, errors.New("unexpected new snapshot on an active feed")
	}
	switch c.Source.Snapshot {
	case "first", "true":
		return false, nil
	case "last":
		// Only the final row closes this single-table bootstrap.
		return true, nil
	default:
		return false, errors.New("unsupported snapshot marker")
	}
}

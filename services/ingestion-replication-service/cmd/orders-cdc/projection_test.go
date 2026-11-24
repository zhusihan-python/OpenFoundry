package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIncompleteAndDriftingOrdersFailClosed(t *testing.T) {
	for _, raw := range []string{
		`{"order_id":"0007","amount_cents":1}`,
		`{"order_id":7,"amount_cents":1,"batch":"a"}`,
		`{"order_id":"0007","amount_cents":1.5,"batch":"a"}`,
		`{"order_id":"0007","amount_cents":1,"batch":"a","new":1}`,
		`{"order_id":"0007","amount_cents":1,"batch":"__debezium_unavailable_value"}`,
	} {
		var o order
		require.Error(t, json.Unmarshal([]byte(raw), &o))
	}
	keys := map[string]order{"0007": {ID: "0007"}}
	cents := int64(123)
	_, err := apply(keys, "0007", change{Op: "d", Before: &order{ID: "0007", Amount: &cents}})
	require.Error(t, err)
	require.Len(t, keys, 1)
}

func TestBootstrapBoundary(t *testing.T) {
	c := change{Op: "r"}
	c.Source.Snapshot = "first"
	ready, err := readyForPublish(c, false, -1)
	require.NoError(t, err)
	require.False(t, ready)
	_, err = readyForPublish(change{Op: "c"}, false, 0)
	require.Error(t, err)
	c.Source.Snapshot = "last"
	ready, err = readyForPublish(c, false, 0)
	require.NoError(t, err)
	require.True(t, ready)
	_, err = readyForPublish(c, true, 1)
	require.Error(t, err)
	ready, err = readyForPublish(change{Op: "c"}, false, -1)
	require.NoError(t, err)
	require.True(t, ready)
}

func TestEmptyTextPrimaryKey(t *testing.T) {
	const event = `{"op":"c","source":{"connector":"postgresql","name":"p","db":"orders","schema":"sales","table":"orders","lsn":123},"transaction":{"id":"42:123","total_order":1},"after":{"order_id":"","amount_cents":0,"batch":null}}`
	change, id, _, err := parseChange([]byte(`{"order_id":""}`), []byte(event), "p", "orders")
	require.NoError(t, err)
	require.Empty(t, id)
	rows := map[string]order{}
	_, err = apply(rows, id, change)
	require.NoError(t, err)
	require.Contains(t, rows, "")
	for _, key := range []string{`{"order_id":null}`, `{}`} {
		_, _, _, err = parseChange([]byte(key), []byte(event), "p", "orders")
		require.Error(t, err)
	}
}

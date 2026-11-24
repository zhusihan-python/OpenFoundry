// orders-cdc materializes a single-partition Debezium PostgreSQL order feed in DVS.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/openfoundry/openfoundry-go/services/ingestion-replication-service/internal/repo/orderscdc"
	"github.com/segmentio/kafka-go"
)

type worker struct {
	dataset                                      uuid.UUID
	prefix, database, topic, endpoint, tokenFile string
	db                                           *pgx.Conn
	client                                       *http.Client
}

func (w *worker) publish(ctx context.Context, event, orderID string, c change, offset int64, rows []byte) error {
	key := "wp15:" + digest([]byte(w.topic+"\x00"+event))
	if rows == nil {
		rows = []byte{}
	}
	body, err := json.Marshal(map[string]any{
		"idempotency_key":  key,
		"branch":           "main",
		"transaction_type": "SNAPSHOT",
		"format":           "json",
		"summary":          "PostgreSQL order change",
		"metadata": map[string]any{
			"topic": w.topic, "partition": 0, "offset": strconv.FormatInt(offset, 10), "lsn": strconv.FormatInt(c.Source.LSN, 10),
			"op": c.Op, "order_id": orderID, "source_event_id": event,
		},
		"files": []map[string]any{{
			"logical_path": "orders.ndjson", "operation": "ADD",
			"content_type": "application/x-ndjson", "size_bytes": len(rows), "content_base64": rows,
		}},
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.endpoint+"/api/v1/datasets/"+w.dataset.String()+"/outputs:commit", bytes.NewReader(body))
	if err != nil {
		return err
	}
	token, err := os.ReadFile(w.tokenFile)
	if err != nil {
		return errors.New("cannot read dataset writer token")
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return errors.New("dataset publish unavailable")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		return fmt.Errorf("dataset publish returned HTTP %d", resp.StatusCode)
	}
	var receipt struct {
		Dataset     uuid.UUID `json:"dataset_id"`
		Branch      string    `json:"branch"`
		Key         string    `json:"idempotency_key"`
		Hash        string    `json:"request_fingerprint"`
		Transaction struct {
			ID     uuid.UUID
			Status string
		}
	}
	if json.NewDecoder(resp.Body).Decode(&receipt) != nil || receipt.Dataset != w.dataset || receipt.Key != key || receipt.Branch != "main" || receipt.Transaction.ID == uuid.Nil || receipt.Transaction.Status != "COMMITTED" || receipt.Hash != digest(body) {
		return errors.New("invalid dataset publish receipt")
	}
	return nil
}

func (w *worker) process(ctx context.Context, m kafka.Message) error {
	if m.Topic != w.topic || m.Partition != 0 {
		return errors.New("expected single partition order topic")
	}
	c, key, event, err := parseChange(m.Key, m.Value, w.prefix, w.database)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(c)
	if err != nil {
		return err
	}
	fingerprint := digest(payload)
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var complete bool
	var last, snapshotLSN, lastLSN int64
	var data []byte
	var binding string
	err = tx.QueryRow(ctx, "SELECT last_offset,rows,source_binding,bootstrap_complete,bootstrap_lsn,last_source_lsn FROM orders_cdc_checkpoint WHERE dataset_id=$1 FOR UPDATE", w.dataset).Scan(&last, &data, &binding, &complete, &snapshotLSN, &lastLSN)
	if err != nil {
		return err
	}
	if binding != w.topic+"/"+w.database {
		return errors.New("checkpoint belongs to another source")
	}
	if m.Offset > last+1 {
		return errors.New("gap in order topic; checkpoint retained")
	}
	var saved string
	err = tx.QueryRow(ctx, "SELECT fingerprint FROM orders_cdc_applied WHERE dataset_id=$1 AND event_id=$2", w.dataset, event).Scan(&saved)
	switch {
	case err == nil:
		if saved != fingerprint {
			return errors.New("source event changed on replay")
		}
	case errors.Is(err, pgx.ErrNoRows):
		if m.Offset <= last {
			return errors.New("unrecognized event at acknowledged offset")
		}
		var orders map[string]order
		if err = json.Unmarshal(data, &orders); err != nil {
			return err
		}
		// A restarted initial snapshot has a new consistent point. Discard
		// unpublished rows collected at the abandoned point, including rows
		// that were deleted before the new snapshot began.
		if c.Op == "r" && c.Source.LSN != snapshotLSN {
			if c.Source.LSN < lastLSN {
				return errors.New("unexpected snapshot generation")
			}
			orders = make(map[string]order)
			snapshotLSN = c.Source.LSN
			complete = false
		}
		lastLSN = max(lastLSN, c.Source.LSN)
		output, err := apply(orders, key, c)
		if err != nil {
			return err
		}
		data, err = json.Marshal(orders)
		if err != nil {
			return err
		}
		complete, err = readyForPublish(c, complete, last)
		if err != nil {
			return err
		}
		if complete {
			if err = w.publish(ctx, event, key, c, m.Offset, output); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(ctx, "INSERT INTO orders_cdc_applied VALUES ($1,$2,$3)", w.dataset, event, fingerprint); err != nil {
			return err
		}
	default:
		return err
	}
	if m.Offset > last {
		if _, err = tx.Exec(ctx, "UPDATE orders_cdc_checkpoint SET last_offset=$2,rows=$3,bootstrap_complete=$4,bootstrap_lsn=$5,last_source_lsn=$6 WHERE dataset_id=$1", w.dataset, m.Offset, data, complete, snapshotLSN, lastLSN); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func run(ctx context.Context) error {
	for _, name := range []string{"CDC_DATASET_ID", "CDC_SOURCE_PREFIX", "CDC_SOURCE_DATABASE", "CDC_DATABASE_URL", "CDC_DVS_URL", "CDC_TOKEN_FILE", "CDC_KAFKA_BROKER"} {
		if os.Getenv(name) == "" {
			return fmt.Errorf("missing %s", name)
		}
	}
	id, err := uuid.Parse(os.Getenv("CDC_DATASET_ID"))
	if err != nil || id == uuid.Nil {
		return errors.New("invalid destination dataset")
	}
	endpoint, err := url.Parse(os.Getenv("CDC_DVS_URL"))
	if err != nil || endpoint.Host == "" || (endpoint.Scheme != "https" && endpoint.Scheme != "http") || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return errors.New("invalid dataset service URL")
	}
	db, err := pgx.Connect(ctx, os.Getenv("CDC_DATABASE_URL"))
	if err != nil {
		return errors.New("cannot connect to checkpoint database")
	}
	defer func() { _ = db.Close(ctx) }()
	if err = orderscdc.Migrate(ctx, db); err != nil {
		return err
	}
	prefix := os.Getenv("CDC_SOURCE_PREFIX")
	topic := prefix + ".sales.orders"
	w := worker{
		dataset: id, db: db, prefix: prefix, topic: topic,
		database: os.Getenv("CDC_SOURCE_DATABASE"), tokenFile: os.Getenv("CDC_TOKEN_FILE"),
		endpoint: strings.TrimRight(endpoint.String(), "/"),
		client:   &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
	if _, err = db.Exec(ctx, "INSERT INTO orders_cdc_checkpoint(dataset_id,source_binding) VALUES ($1,$2) ON CONFLICT DO NOTHING", id, topic+"/"+w.database); err != nil {
		return err
	}
	broker := os.Getenv("CDC_KAFKA_BROKER")
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	parts, err := conn.ReadPartitions(topic)
	_ = conn.Close()
	if err != nil || len(parts) != 1 || parts[0].ID != 0 {
		return errors.New("expected an existing single-partition order topic")
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker}, GroupID: "orders-cdc-" + id.String(), Topic: topic,
		StartOffset: kafka.FirstOffset, MaxWait: time.Second, MaxBytes: 4 << 20,
		WatchPartitionChanges: true, PartitionWatchInterval: 5 * time.Second,
	})
	defer func() { _ = r.Close() }()
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err = w.process(ctx, msg); err != nil {
			return fmt.Errorf("order partition %d offset %d: %w", msg.Partition, msg.Offset, err)
		}
		if err = r.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

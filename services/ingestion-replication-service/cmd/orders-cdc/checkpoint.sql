CREATE TABLE IF NOT EXISTS orders_cdc_checkpoint (
    dataset_id uuid PRIMARY KEY,
    source_binding text NOT NULL,
    bootstrap_complete boolean NOT NULL DEFAULT false,
    last_offset bigint NOT NULL DEFAULT -1,
    rows jsonb NOT NULL DEFAULT '{}'
);
CREATE TABLE IF NOT EXISTS orders_cdc_applied (
    dataset_id uuid NOT NULL REFERENCES orders_cdc_checkpoint(dataset_id),
    event_id text NOT NULL,
    fingerprint text NOT NULL,
    PRIMARY KEY (dataset_id, event_id)
);

ALTER TABLE builds
    ADD COLUMN IF NOT EXISTS manual_staged_group JSONB,
    ADD COLUMN IF NOT EXISTS manual_group_publish_body BYTEA;

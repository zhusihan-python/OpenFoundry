-- NULL preserves recovery for builds that started before the group intent
-- was introduced. A non-NULL value binds the exact ordered DVS request.
ALTER TABLE builds
    ADD COLUMN IF NOT EXISTS manual_group_publish_fingerprint TEXT;

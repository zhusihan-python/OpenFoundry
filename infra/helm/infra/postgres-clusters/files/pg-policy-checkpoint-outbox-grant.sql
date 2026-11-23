-- The outbox writer reads only the conflict key and tombstone predicate.
-- Run as the pg-policy outbox owner on clusters initialized before WP6.5.
GRANT SELECT (event_id) ON outbox.events TO svc_checkpoints_purpose;

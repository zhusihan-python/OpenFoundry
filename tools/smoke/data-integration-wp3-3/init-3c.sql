-- WP3.3 3C smoke: two independent databases on one PG16 instance.
-- Idempotent-by-fresh-volume only (the compose postgres-data volume starts
-- empty on every `run3c.sh up` after down-volumes).
CREATE DATABASE wp33c_dvs;
CREATE DATABASE wp33c_pbs;

-- P1 acceptance databases beyond POSTGRES_DB (datasets).
-- pipeline-build and connector-management own separate databases;
-- migrate_fresh is the empty target reset by run.sh migrate-tests
-- (dataset-versioning migration/tenancy integration tests). Connector
-- repo integration tests bootstrap their own Postgres via testcontainers
-- (docker socket mounted into the runner), so no database is needed here.
CREATE DATABASE pipelines OWNER openfoundry;
CREATE DATABASE connectors OWNER openfoundry;
CREATE DATABASE migrate_fresh OWNER openfoundry;

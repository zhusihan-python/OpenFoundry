#!/bin/sh
# The official image sources this after initializing the fixture database.
# Reject plaintext TCP even for a login with the correct password.
{ echo 'hostnossl all all all reject'; cat "$PGDATA/pg_hba.conf"; } > "$PGDATA/pg_hba.ssl"
mv "$PGDATA/pg_hba.ssl" "$PGDATA/pg_hba.conf"

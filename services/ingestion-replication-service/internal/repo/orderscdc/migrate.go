// Package orderscdc owns the dedicated orders worker checkpoint schema.
package orderscdc

import (
	"context"
	_ "embed"

	"github.com/jackc/pgx/v5"
)

//go:embed migrations/20261124000001_orders_checkpoint.sql
var checkpointMigration string

// Migrate serializes concurrent worker startups in the checkpoint database.
func Migrate(ctx context.Context, db *pgx.Conn) error {
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(817041527, 1)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS orders_cdc_migrations (version bigint PRIMARY KEY)"); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, "INSERT INTO orders_cdc_migrations VALUES (20261124000001) ON CONFLICT DO NOTHING")
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		if _, err = tx.Exec(ctx, checkpointMigration); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

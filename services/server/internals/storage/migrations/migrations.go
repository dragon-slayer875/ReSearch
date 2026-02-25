package migrations

import (
	"context"
	"embed"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"go.uber.org/zap"
)

//go:embed *.sql
var embedMigrations embed.FS

func ApplyMigrations(ctx context.Context, dbPool *pgxpool.Pool, logger *zap.Logger) error {
	db := stdlib.OpenDBFromPool(dbPool)

	postgresDriver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		return err
	}

	fsDriver, err := iofs.New(embedMigrations, ".")
	if err != nil {
		return err
	}

	m, err := migrate.NewWithInstance("iofs", fsDriver, "postgres", postgresDriver)
	if err != nil {
		return err
	}

	err = m.Up()
	if err != nil {
		return err
	}

	return nil
}

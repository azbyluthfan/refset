// Command dbrows feeds refset from an application's own database code: plain
// SQL that reads rows into structs, plus a one-row version counter the writer
// bumps in the same transaction. No refset tables, triggers or adapter. It
// uses Postgres here, but the pattern is the same for MySQL, SQLite or any
// database/sql driver.
//
//	docker compose -f examples/docker-compose.yml up -d --wait
//	REFSET_PG_URL=postgres://refset:refset@localhost:55432/refset?sslmode=disable go run ./dbrows
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azbyluthfan/refset"
)

type Currency struct {
	Code     string
	Decimals int
	RateUSD  float64
}

// Your existing data-access code: read rows into structs.
func loadCurrencies(ctx context.Context, pool *pgxpool.Pool) (uint64, []Currency, error) {
	// Read the version and the rows in one snapshot so they agree.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	version, err := readVersion(ctx, tx)
	if err != nil {
		return 0, nil, err
	}
	rows, err := tx.Query(ctx, `SELECT code, decimals, rate_usd FROM app_currencies`)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	var out []Currency
	for rows.Next() {
		var c Currency
		if err := rows.Scan(&c.Code, &c.Decimals, &c.RateUSD); err != nil {
			return 0, nil, err
		}
		out = append(out, c)
	}
	return version, out, rows.Err()
}

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func readVersion(ctx context.Context, q queryRower) (uint64, error) {
	var v int64
	err := q.QueryRow(ctx, `SELECT version FROM app_versions WHERE name = 'currencies'`).Scan(&v)
	return uint64(v), err //nolint:gosec // the app's version counter, never negative
}

// setRate is the writer: change the row and bump the version in one
// transaction. The version row's lock also orders concurrent writers.
func setRate(ctx context.Context, pool *pgxpool.Pool, code string, rate float64) (uint64, error) {
	var v int64
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE app_currencies SET rate_usd = $2 WHERE code = $1`, code, rate); err != nil {
			return err
		}
		return tx.QueryRow(ctx,
			`UPDATE app_versions SET version = version + 1 WHERE name = 'currencies' RETURNING version`).Scan(&v)
	})
	return uint64(v), err //nolint:gosec // the app's version counter, never negative
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	url := os.Getenv("REFSET_PG_URL")
	if url == "" {
		url = "postgres://refset:refset@localhost:55432/refset?sslmode=disable" //nolint:gosec // the local Docker database from examples/docker-compose.yml
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := setup(ctx, pool); err != nil {
		return err
	}

	currencies, err := refset.Open(ctx, &refset.ReloadSource[string, Currency]{
		Dataset: "currencies",
		Load:    func(ctx context.Context) (uint64, []Currency, error) { return loadCurrencies(ctx, pool) },
		Version: func(ctx context.Context) (uint64, error) { return readVersion(ctx, pool) },
		Key:     func(c Currency) string { return c.Code },
	}, refset.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = currencies.Close() }()

	idr, _ := currencies.Get("IDR")
	fmt.Printf("IDR rate from memory: %g (version %d)\n", idr.RateUSD, currencies.Version())

	v, err := setRate(ctx, pool, "IDR", 0.000061)
	if err != nil {
		return err
	}
	fmt.Printf("Writer updated IDR and bumped the version to %d\n", v)

	idr, _, err = currencies.GetAtLeast(ctx, "IDR", v)
	if err != nil {
		return err
	}
	fmt.Printf("IDR rate at version %d: %g\n", currencies.Version(), idr.RateUSD)
	return nil
}

func setup(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS app_currencies (code text PRIMARY KEY, decimals int NOT NULL, rate_usd double precision NOT NULL);
		CREATE TABLE IF NOT EXISTS app_versions (name text PRIMARY KEY, version bigint NOT NULL DEFAULT 0);
		INSERT INTO app_versions (name) VALUES ('currencies') ON CONFLICT DO NOTHING;
		INSERT INTO app_currencies VALUES ('IDR', 0, 0.000063), ('EUR', 2, 1.08)
			ON CONFLICT (code) DO UPDATE SET rate_usd = excluded.rate_usd;
		UPDATE app_versions SET version = version + 1 WHERE name = 'currencies';`)
	return err
}

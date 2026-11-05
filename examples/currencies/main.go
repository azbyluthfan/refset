// Command currencies is design §6.1: a currency table read on every request,
// served from memory and kept current from Postgres.
//
//	docker compose -f examples/docker-compose.yml up -d --wait
//	REFSET_PG_URL=postgres://refset:refset@localhost:55432/refset?sslmode=disable go run ./currencies
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/examples/pgsource"
)

// CurrenciesDataset is shared by the writer and the source.
const CurrenciesDataset = "currencies"

type Currency struct {
	Code     string  `db:"code"`
	Decimals int     `db:"decimals"`
	RateUSD  float64 `db:"rate_usd"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, envOr("REFSET_PG_URL", "postgres://refset:refset@localhost:55432/refset?sslmode=disable"))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := setup(ctx, pool); err != nil {
		return err
	}

	listener := pgsource.NewListener(pool, nil)
	defer listener.Close()
	src, err := pgsource.NewSource(pool, listener, pgsource.Table[string, Currency]{
		Dataset:     CurrenciesDataset,
		Query:       "SELECT code, decimals, rate_usd FROM currencies",
		ByKeysQuery: "SELECT code, decimals, rate_usd FROM currencies WHERE code = ANY($1)",
		Key:         func(c Currency) string { return c.Code },
	})
	if err != nil {
		return err
	}
	currencies, err := refset.Open(ctx, src, refset.Options{})
	if err != nil {
		return err // source unreachable: don't start with empty data
	}
	defer func() { _ = currencies.Close() }()

	convert := func(usd float64, code string) {
		c, ok := currencies.Get(code) // hot path: local, lock-free
		if !ok {
			fmt.Printf("  %s: unknown currency\n", code)
			return
		}
		fmt.Printf("  %.2f USD = %.*f %s   (dataset version %d)\n", usd, c.Decimals, usd/c.RateUSD, code, currencies.Version())
	}

	fmt.Println("Converting from memory:")
	convert(10, "IDR")
	convert(10, "EUR")

	fmt.Println("An admin updates the IDR rate in Postgres...")
	if _, err := pgsource.Write(ctx, pool, func(tx *pgsource.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE currencies SET rate_usd = 0.000061 WHERE code = 'IDR'`); err != nil {
			return err
		}
		return tx.LogUpsert(CurrenciesDataset, "IDR")
	}, CurrenciesDataset); err != nil {
		return err
	}
	time.Sleep(100 * time.Millisecond) // the notification arrives within milliseconds
	fmt.Println("Converting again, with no request-path database query:")
	convert(10, "IDR")
	return nil
}

func setup(ctx context.Context, pool *pgxpool.Pool) error {
	if err := pgsource.Migrate(ctx, pool); err != nil {
		return err
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS currencies (
		code text PRIMARY KEY, decimals int NOT NULL, rate_usd double precision NOT NULL)`); err != nil {
		return err
	}
	_, err := pgsource.Write(ctx, pool, func(tx *pgsource.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO currencies VALUES ('IDR', 0, 0.000063), ('EUR', 2, 1.08)
			ON CONFLICT (code) DO UPDATE SET rate_usd = excluded.rate_usd`); err != nil {
			return err
		}
		return tx.LogUpsert(CurrenciesDataset, "IDR", "EUR")
	}, CurrenciesDataset)
	return err
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

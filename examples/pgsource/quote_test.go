package pgsource

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

var literalInputs = []string{
	"plans",
	"it's",
	`back\slash`,
	`\'`,
	`'); DROP TABLE refset_versions; --`,
	`\'); DROP TABLE refset_versions; --`,
	"multi\nline",
	"",
}

func TestQuoteLiteral(t *testing.T) {
	for in, want := range map[string]string{
		"plans":  `'plans'`,
		"it's":   `'it''s'`,
		`a\b`:    ` E'a\\b'`,
		`\'`:     ` E'\\'''`,
		`'); --`: `'''); --'`,
	} {
		if got := quoteLiteral(in); got != want {
			t.Errorf("quoteLiteral(%q) = %s, want %s", in, got, want)
		}
	}
}

// Postgres must read every quoted literal back as the original string, with
// standard_conforming_strings on (the default) and off. Needs REFSET_PG_URL.
func TestQuoteLiteralRoundTripsInPostgres(t *testing.T) {
	url := os.Getenv("REFSET_PG_URL")
	if url == "" {
		t.Skip("REFSET_PG_URL not set")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, setting := range []string{"on", "off"} {
		if _, err := conn.Exec(ctx, "SET standard_conforming_strings = "+setting); err != nil {
			t.Fatal(err)
		}
		for _, in := range literalInputs {
			// Send the literal as written, as the DDL is, through the
			// low-level connection: pgx's own simple-protocol path refuses
			// to run while standard_conforming_strings is off.
			results, err := conn.PgConn().Exec(ctx, "SELECT "+quoteLiteral(in)).ReadAll()
			if err != nil {
				t.Fatalf("standard_conforming_strings=%s, input %q: %v", setting, in, err)
			}
			if len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 1 {
				t.Fatalf("standard_conforming_strings=%s, input %q: want one value, got %d statements", setting, in, len(results))
			}
			if got := string(results[0].Rows[0][0]); got != in {
				t.Fatalf("standard_conforming_strings=%s: %q read back as %q", setting, in, got)
			}
		}
	}
}

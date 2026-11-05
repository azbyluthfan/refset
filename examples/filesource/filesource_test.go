package filesource_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/examples/filesource"
	"github.com/azbyluthfan/refset/refsettest"
)

type rate struct {
	Code string  `json:"code"`
	USD  float64 `json:"usd"`
}

func TestConformance(t *testing.T) {
	refsettest.RunConformance(t, func(t *testing.T) refsettest.Harness[string, rate] {
		s, err := filesource.Open[string, rate]("currencies", t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return refsettest.Harness[string, rate]{
			Source: s,
			Apply: func(_ context.Context, ops ...refset.Op[string, rate]) (uint64, error) {
				return s.Apply(ops...)
			},
			Compact: func(context.Context) error { return s.Compact() },
			Key:     func(i int) string { return "C" + strconv.Itoa(i) },
			Value:   func(i int) rate { return rate{Code: "C" + strconv.Itoa(i), USD: float64(i) / 100} },
		}
	})
}

// A dataset over files picks up writes by polling and survives compaction.
func TestDatasetOverFiles(t *testing.T) {
	s, err := filesource.Open[string, rate]("currencies", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(refset.Op[string, rate]{Key: "IDR", Value: rate{"IDR", 0.000062}}); err != nil {
		t.Fatal(err)
	}
	d, err := refset.Open(context.Background(), s, refset.Options{PollInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close() }()

	v, _ := s.Apply(refset.Op[string, rate]{Key: "EUR", Value: rate{"EUR", 1.08}})
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	got, ok, err := d.GetAtLeast(context.Background(), "EUR", v)
	if err != nil || !ok || got.USD != 1.08 {
		t.Fatalf("EUR = %+v, %v, %v", got, ok, err)
	}
	if r, _ := d.Get("IDR"); r.USD != 0.000062 {
		t.Fatalf("IDR lost: %+v", r)
	}
}

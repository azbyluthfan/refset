package refset_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

type currency struct {
	Code    string
	RateUSD float64
}

// appStore stands in for an application's own data: structs plus a version
// it bumps on every change, both under one lock.
type appStore struct {
	mu       sync.Mutex
	version  uint64
	rows     map[string]currency
	loads    atomic.Int64
	versions atomic.Int64
}

func newAppStore() *appStore { return &appStore{rows: make(map[string]currency)} }

func (a *appStore) apply(ops ...refset.Op[string, currency]) uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, op := range ops {
		if op.Delete {
			delete(a.rows, op.Key)
		} else {
			a.rows[op.Key] = op.Value
		}
	}
	a.version++
	return a.version
}

func (a *appStore) load(context.Context) (uint64, []currency, error) {
	a.loads.Add(1)
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]currency, 0, len(a.rows))
	for _, c := range a.rows {
		out = append(out, c)
	}
	return a.version, out, nil
}

func (a *appStore) currentVersion(context.Context) (uint64, error) {
	a.versions.Add(1)
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.version, nil
}

func (a *appStore) source() *refset.ReloadSource[string, currency] {
	return &refset.ReloadSource[string, currency]{
		Dataset: "currencies",
		Load:    a.load,
		Version: a.currentVersion,
		Key:     func(c currency) string { return c.Code },
	}
}

func TestReloadSourceConformance(t *testing.T) {
	refsettest.RunConformance(t, func(*testing.T) refsettest.Harness[string, currency] {
		app := newAppStore()
		return refsettest.Harness[string, currency]{
			Source: app.source(),
			Apply: func(_ context.Context, ops ...refset.Op[string, currency]) (uint64, error) {
				// The key lives in the value; keep them consistent.
				for i := range ops {
					ops[i].Value.Code = ops[i].Key
				}
				return app.apply(ops...), nil
			},
			Key:   func(i int) string { return "C" + strconv.Itoa(i) },
			Value: func(i int) currency { return currency{RateUSD: float64(i)} },
			Equal: func(a, b currency) bool { return a.RateUSD == b.RateUSD },
		}
	})
}

func TestReloadSourceDataset(t *testing.T) {
	app := newAppStore()
	app.apply(refset.Op[string, currency]{Key: "IDR", Value: currency{"IDR", 0.000063}})
	d := open[string, currency](t, app.source(), refset.Options{PollInterval: time.Hour, WaitBound: time.Second})
	if c, _ := d.Get("IDR"); c.RateUSD != 0.000063 {
		t.Fatalf("initial load: %+v", c)
	}

	// The app changes its data and bumps the version; a read that needs the
	// new version triggers an immediate reload.
	v := app.apply(
		refset.Op[string, currency]{Key: "IDR", Value: currency{"IDR", 0.000061}},
		refset.Op[string, currency]{Key: "EUR", Value: currency{"EUR", 1.08}},
	)
	c, ok, err := d.GetAtLeast(context.Background(), "EUR", v)
	if err != nil || !ok || c.RateUSD != 1.08 {
		t.Fatalf("EUR = %+v, %v, %v", c, ok, err)
	}
	if c, _ := d.Get("IDR"); c.RateUSD != 0.000061 {
		t.Fatalf("IDR not reloaded: %+v", c)
	}
	if st := d.Stats(); st.Reloads != 1 || st.Entries != 2 {
		t.Fatalf("stats %+v", st)
	}

	// Polls with no change only read the version.
	loads := app.loads.Load()
	if _, _, err := d.GetAtLeast(context.Background(), "EUR", v); err != nil {
		t.Fatal(err)
	}
	if app.loads.Load() != loads {
		t.Fatal("an unchanged version caused a reload")
	}
}

func TestReloadSourceWithoutVersionFunc(t *testing.T) {
	app := newAppStore()
	app.apply(refset.Op[string, currency]{Key: "IDR", Value: currency{"IDR", 1}})
	src := app.source()
	src.Version = nil // every poll calls Load to learn the version
	d := open[string, currency](t, src, refset.Options{PollInterval: 10 * time.Millisecond})
	v := app.apply(refset.Op[string, currency]{Key: "IDR", Value: currency{"IDR", 2}})
	eventually(t, "reloaded", func() bool { return d.Version() == v })
	if c, _ := d.Get("IDR"); c.RateUSD != 2 {
		t.Fatalf("IDR = %+v", c)
	}
}

func TestReloadSourceNotify(t *testing.T) {
	app := newAppStore()
	notify := make(chan uint64, 1)
	src := app.source()
	src.Notify = func(ctx context.Context) (<-chan uint64, error) {
		out := make(chan uint64)
		go func() {
			defer close(out)
			for {
				select {
				case v := <-notify:
					select {
					case out <- v:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
		return out, nil
	}
	d := open[string, currency](t, src, refset.Options{PollInterval: time.Hour})
	notify <- app.apply(refset.Op[string, currency]{Key: "EUR", Value: currency{"EUR", 1.08}})
	eventually(t, "reloaded on notification", func() bool { _, ok := d.Get("EUR"); return ok })
}

func TestReloadSourceVersionErrors(t *testing.T) {
	app := newAppStore()
	src := app.source()
	boom := errors.New("version table unreachable")
	var fail atomic.Bool
	src.Version = func(ctx context.Context) (uint64, error) {
		if fail.Load() {
			return 0, boom
		}
		return app.currentVersion(ctx)
	}
	d := open[string, currency](t, src, refset.Options{PollInterval: 5 * time.Millisecond})
	fail.Store(true)
	eventually(t, "poll errors counted", func() bool { return d.Stats().PollErrors > 0 })
}

func TestReloadSourceValidation(t *testing.T) {
	bad := []*refset.ReloadSource[string, currency]{
		nil,
		{},
		{Dataset: "x", Key: func(c currency) string { return c.Code }},
		{Dataset: "x", Load: newAppStore().load},
	}
	for i, src := range bad {
		if _, err := refset.Open[string, currency](context.Background(), src, refset.Options{}); err == nil {
			t.Errorf("case %d: Open accepted an incomplete ReloadSource", i)
		}
	}
	src := newAppStore().source()
	if _, err := src.Watch(context.Background()); err == nil {
		t.Fatal("Watch without Notify should report that polling is used")
	}
	if src.Name() != "currencies" {
		t.Fatalf("Name = %q", src.Name())
	}
}

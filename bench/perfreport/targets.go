package main

import (
	"context"
	"sync"
	"time"

	"github.com/azbyluthfan/refset"
)

// target is a store under test: refset, or a map behind a sync.RWMutex.
type target interface {
	get(key string) (plan, bool)
	// write sets entry i and returns once the write is committed at the
	// source (refset) or in the map.
	write(i int, v plan)
	close()
}

// refsetTarget is a Dataset fed by a feed. With reload set, every change is
// a full reload, as with refset.ReloadSource.
type refsetTarget struct {
	f *feed[string, plan]
	d *refset.Dataset[string, plan]
}

func newRefset(keys []string, vals []plan, reload bool, opts refset.Options) *refsetTarget {
	f := newFeed(keys, vals, reload)
	d, err := refset.Open(context.Background(), f, opts)
	if err != nil {
		panic(err)
	}
	return &refsetTarget{f: f, d: d}
}

func (t *refsetTarget) get(k string) (plan, bool) { return t.d.Get(k) }
func (t *refsetTarget) write(i int, v plan)       { t.f.upsert(i, v) }
func (t *refsetTarget) close()                    { _ = t.d.Close() }

// mapTarget is the usual alternative: one map, a read-write lock. In reload
// mode a write rebuilds the whole map from the current values and swaps it
// in under the lock, which is what a periodic "reload everything" cache does.
type mapTarget struct {
	reload bool
	keys   []string
	vals   []plan // current values, for rebuilds; only the writer uses it

	mu sync.RWMutex
	m  map[string]plan
}

func newMap(keys []string, vals []plan, reload bool) *mapTarget {
	t := &mapTarget{reload: reload, keys: keys, vals: append([]plan(nil), vals...)}
	t.m = t.build()
	return t
}

func (t *mapTarget) build() map[string]plan {
	m := make(map[string]plan, len(t.keys))
	for i, k := range t.keys {
		m[k] = t.vals[i]
	}
	return m
}

func (t *mapTarget) get(k string) (plan, bool) {
	t.mu.RLock()
	v, ok := t.m[k]
	t.mu.RUnlock()
	return v, ok
}

func (t *mapTarget) write(i int, v plan) {
	t.vals[i] = v
	if t.reload {
		m := t.build()
		t.mu.Lock()
		t.m = m
		t.mu.Unlock()
		return
	}
	t.mu.Lock()
	t.m[t.keys[i]] = v
	t.mu.Unlock()
}

func (t *mapTarget) close() {}

// refsetOpts are the options every refset target uses: polling rarely, so
// notifications drive updates, with the shard count under test.
func refsetOpts(cfg config) refset.Options {
	return refset.Options{Shards: cfg.shards, PollInterval: time.Second}
}

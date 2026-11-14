package refset

import (
	"container/heap"
	"context"
	"errors"
	"maps"
	"reflect"
	"testing"
	"time"
	"unsafe"
)

func rootFrom(l layout, shards int, version uint64, m map[int]int) *root[int, int] {
	r := &root[int, int]{version: version, l: l, shards: emptyShards[int, int](shards)}
	for k, v := range m {
		r.shards[shardOf(l, k)][k] = v
		r.n++
	}
	return r
}

func contents(r *root[int, int]) map[int]int {
	out := make(map[int]int)
	for _, m := range r.shards {
		maps.Copy(out, m)
	}
	return out
}

// G1: copy-on-write copies only touched shards; the rest are shared.
func TestApplyCopiesOnlyTouchedShards(t *testing.T) {
	l := newLayout(16)
	initial := make(map[int]int)
	for i := range 1000 {
		initial[i] = i
	}
	cur := rootFrom(l, 16, 1, initial)
	next, _ := applyDeltas(cur, 2, []Delta[int, int]{{Version: 2, Ops: []Op[int, int]{{Key: 7, Value: 70}}}})

	changed := 0
	for i := range cur.shards {
		if reflect.ValueOf(cur.shards[i]).Pointer() != reflect.ValueOf(next.shards[i]).Pointer() {
			changed++
		}
	}
	if changed != 1 {
		t.Fatalf("copied %d shards, want 1", changed)
	}
	if v := cur.shards[shardOf(l, 7)][7]; v != 7 {
		t.Fatalf("old root modified: key 7 = %d", v)
	}
	if v := next.shards[shardOf(l, 7)][7]; v != 70 || next.version != 2 || next.n != 1000 {
		t.Fatalf("next: key 7 = %d, version %d, n %d", v, next.version, next.n)
	}
}

// G1: deltas at or below the applied version are ignored.
func TestApplySkipsDuplicateDeltas(t *testing.T) {
	l := newLayout(4)
	cur := rootFrom(l, 4, 5, map[int]int{1: 1})
	next, _ := applyDeltas(cur, 6, []Delta[int, int]{
		{Version: 4, Ops: []Op[int, int]{{Key: 1, Value: 999}}},
		{Version: 5, Ops: []Op[int, int]{{Key: 2, Value: 999}}},
		{Version: 6, Ops: []Op[int, int]{{Key: 3, Value: 3}, {Key: 1, Delete: true}}},
	})
	want := map[int]int{3: 3}
	if got := contents(next); !maps.Equal(got, want) || next.n != 1 {
		t.Fatalf("got %v (n=%d), want %v", got, next.n, want)
	}
}

func TestCheckChanges(t *testing.T) {
	d := func(v uint64) Delta[int, int] { return Delta[int, int]{Version: v} }
	tests := []struct {
		name    string
		applied uint64
		head    uint64
		deltas  []Delta[int, int]
		ok      bool
	}{
		{"empty", 3, 3, nil, true},
		{"ascending", 3, 6, []Delta[int, int]{d(4), d(5), d(6)}, true},
		{"collapsed at head", 3, 9, []Delta[int, int]{d(9)}, true},
		{"above head", 3, 5, []Delta[int, int]{d(6)}, false},
		{"out of order", 3, 6, []Delta[int, int]{d(5), d(4)}, false},
		{"head below applied", 7, 5, nil, false},
	}
	for _, tt := range tests {
		err := checkChanges("x", tt.applied, tt.head, tt.deltas)
		if (err == nil) != tt.ok {
			t.Errorf("%s: err = %v, want ok=%v", tt.name, err, tt.ok)
		}
	}
}

func TestOptionsResolve(t *testing.T) {
	c, err := Options{}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	if c.shards != 0 || c.batchWindow != 5*time.Millisecond || c.waitBound != 50*time.Millisecond ||
		c.pollInterval != time.Second || c.clock == nil || c.log == nil {
		t.Fatalf("defaults: %+v", c)
	}
	if c, err := (Options{WaitBound: -1}).resolve(); err != nil || c.waitBound != -1 {
		t.Fatalf("WaitBound -1: %v %v", c.waitBound, err)
	}
	bad := []Options{
		{Shards: 3},
		{Shards: -1},
		{Shards: 1 << 20},
		{BatchWindow: -1},
		{WaitBound: -2},
		{PollInterval: -1},
		{MaxStaleness: -1},
		{MaxStaleness: time.Second}, // equal to default PollInterval
		{PollInterval: 2 * time.Second, MaxStaleness: time.Second},
		{MaxConcurrentFallbacks: -2},
		{ReloadInterval: -1},
	}
	for _, o := range bad {
		if _, err := o.resolve(); err == nil {
			t.Errorf("%+v: expected error", o)
		}
	}
}

func TestElapsedUsesLargerClock(t *testing.T) {
	base := Reading{MonoNanos: 100, WallNanos: 1000}
	tests := []struct {
		name string
		now  Reading
		want time.Duration
	}{
		{"normal", Reading{MonoNanos: 150, WallNanos: 1050}, 50},
		{"suspend: wall jumps, mono stops", Reading{MonoNanos: 110, WallNanos: 5000}, 4000},
		{"wall stepped back", Reading{MonoNanos: 300, WallNanos: 0}, 200},
		{"both behind", Reading{MonoNanos: 100, WallNanos: 900}, 0},
	}
	for _, tt := range tests {
		if got := elapsed(base, tt.now); got != tt.want {
			t.Errorf("%s: got %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestWaiterHeap(t *testing.T) {
	var h waiterHeap
	mk := func(minV, need uint64) *waiter {
		w := &waiter{min: minV, needSeq: need, ch: make(chan error, 1)}
		heap.Push(&h, w)
		return w
	}
	w5, w3, w9, w7 := mk(5, 0), mk(3, 0), mk(9, 2), mk(7, 1)

	h.releaseApplied(5)
	for _, w := range []*waiter{w3, w5} {
		if err := <-w.ch; err != nil || w.index != -1 {
			t.Fatalf("waiter %d: err %v index %d", w.min, err, w.index)
		}
	}
	// Poll 1 saw head 6: w7 (needs poll 1) is ahead; w9 needs poll 2, not yet.
	h.releaseAhead(1, 6)
	if err := <-w7.ch; !errors.Is(err, ErrVersionAhead) {
		t.Fatalf("w7: %v", err)
	}
	if h.Len() != 1 || h[0] != w9 {
		t.Fatalf("heap = %v", h)
	}
	if !h.remove(w9) || h.remove(w9) {
		t.Fatal("remove should succeed once")
	}
	mk(1, 0)
	h.releaseAll(ErrClosed)
	if h.Len() != 0 {
		t.Fatal("releaseAll left waiters")
	}
}

func TestContextMinVersion(t *testing.T) {
	ctx := context.Background()
	if _, ok := MinVersion(ctx, "plans"); ok {
		t.Fatal("empty context has a version")
	}
	ctx = ContextWithMinVersion(ctx, "plans", 51)
	ctx2 := ContextWithMinVersion(ctx, "perms", 9)
	ctx3 := ContextWithMinVersion(ctx2, "plans", 40) // lower: kept at 51
	ctx4 := ContextWithMinVersion(ctx3, "plans", 60)
	check := func(c context.Context, ds string, want uint64) {
		t.Helper()
		if v, ok := MinVersion(c, ds); !ok || v != want {
			t.Fatalf("%s = %d,%v want %d", ds, v, ok, want)
		}
	}
	check(ctx, "plans", 51)
	check(ctx2, "perms", 9)
	check(ctx3, "plans", 51)
	check(ctx4, "plans", 60)
	check(ctx2, "plans", 51) // parent unchanged by child
	if _, ok := MinVersion(ctx, "perms"); ok {
		t.Fatal("parent context modified")
	}
}

func TestSnapshotZeroValue(t *testing.T) {
	var s Snapshot[string, int]
	if _, ok := s.Get("x"); ok || s.Len() != 0 || s.Version() != 0 {
		t.Fatal("zero Snapshot not empty")
	}
	for range s.All() {
		t.Fatal("zero Snapshot iterated")
	}
}

// FuzzApplyDeltas checks that applying any sequence of ops in any batching,
// with duplicates re-delivered, matches a plain map (G1, G2).
func FuzzApplyDeltas(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add([]byte{0x11, 0x91, 0x22, 0xa2, 0x33, 0xb3})
	f.Fuzz(func(t *testing.T, data []byte) {
		l := newLayout(4)
		cur := &root[int, int]{l: l, shards: emptyShards[int, int](4)}
		model := make(map[int]int)
		var all []Delta[int, int]
		var pending []Delta[int, int]
		version := uint64(0)
		for i := 0; i+1 < len(data); i += 2 {
			b, val := data[i], int(data[i+1])
			version++
			op := Op[int, int]{Key: int(b & 0x0f), Value: val, Delete: b&0x10 != 0}
			d := Delta[int, int]{Version: version, Ops: []Op[int, int]{op}}
			all = append(all, d)
			pending = append(pending, d)
			if op.Delete {
				delete(model, op.Key)
			} else {
				model[op.Key] = op.Value
			}
			if b&0x80 != 0 { // flush a batch, re-delivering everything seen so far
				cur, _ = applyDeltas(cur, version, all)
				pending = nil
				if got := contents(cur); !maps.Equal(got, model) || cur.n != len(model) {
					t.Fatalf("at v%d: got %v (n=%d), want %v", version, got, cur.n, model)
				}
			}
		}
		cur, _ = applyDeltas(cur, version, pending)
		if got := contents(cur); !maps.Equal(got, model) || cur.n != len(model) || cur.version != version {
			t.Fatalf("final: got %v (n=%d, v=%d), want %v (v=%d)", got, cur.n, cur.version, model, version)
		}
	})
}

// Rule R2 (design §4.11): the field groups of Dataset written by different
// goroutines never share a cache line, whatever the struct's base alignment.
func TestDatasetFieldGroupsOnSeparateCacheLines(t *testing.T) {
	var d Dataset[string, int]
	readHotEnd := unsafe.Offsetof(d.loopDone) + unsafe.Sizeof(d.loopDone)
	loopStart := unsafe.Offsetof(d.knownHead)
	loopEnd := unsafe.Offsetof(d.fallbacks) + unsafe.Sizeof(d.fallbacks)
	waitStart := unsafe.Offsetof(d.mu)
	if unsafe.Offsetof(d.root) != 0 {
		t.Errorf("root should lead the read-hot group, at offset %d", unsafe.Offsetof(d.root))
	}
	if gap := loopStart - readHotEnd; gap < cacheLineSize {
		t.Errorf("read-hot and loop-written groups only %d bytes apart", gap)
	}
	if gap := waitStart - loopEnd; gap < cacheLineSize {
		t.Errorf("loop-written and waiter groups only %d bytes apart", gap)
	}
}

package refsettest

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
)

// Harness gives RunConformance a fresh source and ways to write to it.
type Harness[K comparable, V any] struct {
	// Source is the source under test. It must start empty or at a stable
	// state; RunConformance reads it first.
	Source refset.Source[K, V]

	// Apply commits ops atomically as one new version and returns that
	// version. It must be safe for concurrent use.
	Apply func(ctx context.Context, ops ...refset.Op[K, V]) (uint64, error)

	// Compact discards the source's change history up to its current head.
	// Optional: when nil, requirement C4's compaction check is skipped.
	Compact func(ctx context.Context) error

	// Key and Value build distinct keys and values from small integers.
	Key   func(i int) K
	Value func(i int) V

	// Equal compares values. Optional: reflect.DeepEqual when nil.
	Equal func(a, b V) bool
}

// RunConformance checks a Source against the Source contract
// (docs/source-contract.md, requirements C1 to C7). newHarness is called once
// per subtest and must return a fresh, independent source.
//
// It runs these subtests:
//
//   - C7/name: Name is non-empty and stable.
//   - C1_C3/sequential: snapshots and changes from every earlier version match
//     a model, and versions are contiguous.
//   - C2/concurrent_writers: concurrent writes get unique, contiguous versions.
//   - C1_C3/during_writes: snapshots and changes taken while writes continue
//     match the model at the versions they report.
//   - C4/compaction (if Harness.Compact is set): changes from before a
//     compaction return ErrSnapshotRequired.
//   - C4/history_reset: changes after a version beyond the head return
//     ErrSnapshotRequired.
//   - C5/watch: notifications never exceed the head, and the channel closes on
//     cancellation.
//   - C6/key_loader (if the source implements refset.KeyLoader): LoadKey
//     returns the latest committed value and is safe for concurrent use.
func RunConformance[K comparable, V any](t *testing.T, newHarness func(t *testing.T) Harness[K, V]) {
	t.Helper()
	t.Run("C7/name", func(t *testing.T) { testName(t, newHarness(t)) })
	t.Run("C1_C3/sequential", func(t *testing.T) { testSequential(t, newHarness(t)) })
	t.Run("C2/concurrent_writers", func(t *testing.T) { testConcurrentWriters(t, newHarness(t)) })
	t.Run("C1_C3/during_writes", func(t *testing.T) { testDuringWrites(t, newHarness(t)) })
	t.Run("C4/compaction", func(t *testing.T) {
		h := newHarness(t)
		if h.Compact == nil {
			t.Skip("Harness.Compact not set")
		}
		testCompaction(t, h)
	})
	t.Run("C4/history_reset", func(t *testing.T) { testHistoryReset(t, newHarness(t)) })
	t.Run("C5/watch", func(t *testing.T) { testWatch(t, newHarness(t)) })
	t.Run("C6/key_loader", func(t *testing.T) {
		h := newHarness(t)
		if _, ok := h.Source.(refset.KeyLoader[K, V]); !ok {
			t.Skip("source does not implement refset.KeyLoader")
		}
		testKeyLoader(t, h)
	})
}

// model records the dataset's state at every version.
type model[K comparable, V any] struct {
	h      Harness[K, V]
	states map[uint64]map[K]V
	first  uint64
}

func (h Harness[K, V]) equal(a, b V) bool {
	if h.Equal != nil {
		return h.Equal(a, b)
	}
	return reflect.DeepEqual(a, b)
}

func (h Harness[K, V]) mapsEqual(a, b map[K]V) bool {
	return maps.EqualFunc(a, b, h.equal)
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// load reads a full snapshot, failing the test on error or duplicate keys.
func load[K comparable, V any](t *testing.T, ctx context.Context, src refset.Source[K, V]) (uint64, map[K]V) {
	t.Helper()
	m := make(map[K]V)
	dup := false
	v, err := src.LoadSnapshot(ctx, func(k K, val V) {
		if _, ok := m[k]; ok {
			dup = true
		}
		m[k] = val
	})
	if err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}
	if dup {
		t.Fatalf("C1: LoadSnapshot passed a key more than once")
	}
	return v, m
}

func newModel[K comparable, V any](t *testing.T, h Harness[K, V]) *model[K, V] {
	t.Helper()
	v, m := load(t, ctxT(t), h.Source)
	return &model[K, V]{h: h, states: map[uint64]map[K]V{v: m}, first: v}
}

// record adds the state after applying ops at version v onto version v-1.
func (m *model[K, V]) record(t *testing.T, v uint64, ops []refset.Op[K, V]) {
	t.Helper()
	prev, ok := m.states[v-1]
	if !ok {
		t.Fatalf("C2: version %d committed without version %d before it", v, v-1)
	}
	next := maps.Clone(prev)
	applyOps(next, ops)
	m.states[v] = next
}

// checkChanges verifies a Changes result against the model (C3).
func (m *model[K, V]) checkChanges(t *testing.T, after, head uint64, deltas []refset.Delta[K, V]) {
	t.Helper()
	base, ok := m.states[after]
	if !ok {
		t.Fatalf("model has no state at version %d", after)
	}
	want, ok := m.states[head]
	if !ok {
		t.Fatalf("C3: Changes(%d) reported head %d, which was never committed", after, head)
	}
	got := maps.Clone(base)
	var prev uint64
	for _, d := range deltas {
		if d.Version <= after || d.Version > head {
			t.Fatalf("C3: Changes(%d) returned a delta at version %d outside (%d, %d]", after, d.Version, after, head)
		}
		if d.Version < prev {
			t.Fatalf("C3: Changes(%d) returned deltas out of order (%d after %d)", after, d.Version, prev)
		}
		prev = d.Version
		applyOps(got, d.Ops)
	}
	if !m.h.mapsEqual(got, want) {
		t.Fatalf("C3: Changes(%d) up to head %d: applied result %v, want state at %d %v", after, head, got, head, want)
	}
}

// checkReload handles a source that answered Changes(after) with
// ErrSnapshotRequired instead of per-key changes, which the contract allows
// (C4; refset.ReloadSource always does). The full snapshot it asks the dataset
// to load must then be the state at the current head (C1, C3).
func (m *model[K, V]) checkReload(t *testing.T, ctx context.Context, after, head uint64) {
	t.Helper()
	v, snap := load(t, ctx, m.h.Source)
	if v != head || !m.h.mapsEqual(snap, m.states[head]) {
		t.Fatalf("C1/C3: Changes(%d) asked for a reload, but LoadSnapshot = version %d %v, want version %d %v",
			after, v, snap, head, m.states[head])
	}
}

func randomOps[K comparable, V any](h Harness[K, V], rng *rand.Rand) []refset.Op[K, V] {
	n := 1 + rng.IntN(3)
	ops := make([]refset.Op[K, V], n)
	for i := range ops {
		ops[i] = refset.Op[K, V]{Key: h.Key(rng.IntN(10)), Value: h.Value(rng.IntN(1000)), Delete: rng.IntN(4) == 0}
	}
	return ops
}

func testName[K comparable, V any](t *testing.T, h Harness[K, V]) {
	n := h.Source.Name()
	if n == "" {
		t.Fatal("C7: Name is empty")
	}
	for range 3 {
		if h.Source.Name() != n {
			t.Fatal("C7: Name changed between calls")
		}
	}
}

func testSequential[K comparable, V any](t *testing.T, h Harness[K, V]) {
	ctx := ctxT(t)
	m := newModel(t, h)
	rng := rand.New(rand.NewPCG(1, 1))
	head := m.first
	for range 50 {
		ops := randomOps(h, rng)
		v, err := h.Apply(ctx, ops...)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if v != head+1 {
			t.Fatalf("C2: Apply returned version %d after %d; versions must be contiguous", v, head)
		}
		m.record(t, v, ops)
		head = v
	}
	for after := m.first; after <= head; after++ {
		got, deltas, err := h.Source.Changes(ctx, after)
		if after < head && errors.Is(err, refset.ErrSnapshotRequired) {
			m.checkReload(t, ctx, after, head)
			continue
		}
		if err != nil {
			t.Fatalf("Changes(%d): %v", after, err)
		}
		if got != head {
			t.Fatalf("C3: Changes(%d) reported head %d, want %d", after, got, head)
		}
		m.checkChanges(t, after, got, deltas)
	}
	v, snap := load(t, ctx, h.Source)
	if v != head || !h.mapsEqual(snap, m.states[head]) {
		t.Fatalf("C1: LoadSnapshot = version %d %v, want version %d %v", v, snap, head, m.states[head])
	}
}

func testConcurrentWriters[K comparable, V any](t *testing.T, h Harness[K, V]) {
	ctx := ctxT(t)
	m := newModel(t, h)
	const writers, each = 8, 20
	var mu sync.Mutex
	ops := make(map[uint64][]refset.Op[K, V])
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				op := []refset.Op[K, V]{{Key: h.Key(100 + w*each + i), Value: h.Value(i)}}
				v, err := h.Apply(ctx, op...)
				if err != nil {
					errs <- err
					return
				}
				mu.Lock()
				_, dup := ops[v]
				ops[v] = op
				mu.Unlock()
				if dup {
					errs <- fmt.Errorf("C2: version %d returned to two writers", v)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	versions := slices.Sorted(maps.Keys(ops))
	for i, v := range versions {
		if v != m.first+uint64(i)+1 {
			t.Fatalf("C2: versions not contiguous: got %v...", versions[:min(len(versions), i+1)])
		}
		m.record(t, v, ops[v])
	}
	last := versions[len(versions)-1]
	head, deltas, err := h.Source.Changes(ctx, m.first)
	if errors.Is(err, refset.ErrSnapshotRequired) {
		m.checkReload(t, ctx, m.first, last)
		return
	}
	if err != nil {
		t.Fatalf("Changes: %v", err)
	}
	m.checkChanges(t, m.first, head, deltas)
}

func testDuringWrites[K comparable, V any](t *testing.T, h Harness[K, V]) {
	ctx := ctxT(t)
	m := newModel(t, h)
	var mu sync.Mutex
	ops := make(map[uint64][]refset.Op[K, V])
	stop := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		rng := rand.New(rand.NewPCG(2, 2))
		for {
			select {
			case <-stop:
				writerDone <- nil
				return
			default:
			}
			o := randomOps(h, rng)
			v, err := h.Apply(ctx, o...)
			if err != nil {
				writerDone <- err
				return
			}
			mu.Lock()
			ops[v] = o
			mu.Unlock()
		}
	}()

	type observation struct {
		snapV  uint64
		snap   map[K]V
		head   uint64
		deltas []refset.Delta[K, V]
		// For a source that answered ErrSnapshotRequired: the reload it asked for.
		reloaded   bool
		reloadV    uint64
		reloadSnap map[K]V
	}
	var obs []observation
	for range 20 {
		v, snap := load(t, ctx, h.Source)
		head, deltas, err := h.Source.Changes(ctx, v)
		if errors.Is(err, refset.ErrSnapshotRequired) {
			rv, rsnap := load(t, ctx, h.Source)
			if rv < v {
				t.Fatalf("C2: reload after ErrSnapshotRequired went from version %d back to %d", v, rv)
			}
			obs = append(obs, observation{snapV: v, snap: snap, reloaded: true, reloadV: rv, reloadSnap: rsnap})
			continue
		}
		if err != nil {
			t.Fatalf("Changes(%d): %v", v, err)
		}
		if head < v {
			t.Fatalf("C3: Changes(%d) reported head %d below the snapshot version", v, head)
		}
		obs = append(obs, observation{snapV: v, snap: snap, head: head, deltas: deltas})
	}
	close(stop)
	if err := <-writerDone; err != nil {
		t.Fatalf("Apply: %v", err)
	}
	for _, v := range slices.Sorted(maps.Keys(ops)) {
		m.record(t, v, ops[v])
	}
	for _, o := range obs {
		want, ok := m.states[o.snapV]
		if !ok || !h.mapsEqual(o.snap, want) {
			t.Fatalf("C1: snapshot at version %d = %v, want %v", o.snapV, o.snap, want)
		}
		if o.reloaded {
			want, ok := m.states[o.reloadV]
			if !ok || !h.mapsEqual(o.reloadSnap, want) {
				t.Fatalf("C1: reloaded snapshot at version %d = %v, want %v", o.reloadV, o.reloadSnap, want)
			}
			continue
		}
		m.checkChanges(t, o.snapV, o.head, o.deltas)
	}
}

func testCompaction[K comparable, V any](t *testing.T, h Harness[K, V]) {
	ctx := ctxT(t)
	m := newModel(t, h)
	var head uint64
	for i := range 5 {
		v, err := h.Apply(ctx, refset.Op[K, V]{Key: h.Key(i), Value: h.Value(i)})
		if err != nil {
			t.Fatal(err)
		}
		head = v
	}
	if err := h.Compact(ctx); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if _, _, err := h.Source.Changes(ctx, m.first); !errors.Is(err, refset.ErrSnapshotRequired) {
		t.Fatalf("C4: Changes(%d) after compaction: %v, want ErrSnapshotRequired", m.first, err)
	}
	got, deltas, err := h.Source.Changes(ctx, head)
	if err != nil || got != head || len(deltas) != 0 {
		t.Fatalf("Changes(head) after compaction = %d, %d deltas, %v", got, len(deltas), err)
	}
}

func testHistoryReset[K comparable, V any](t *testing.T, h Harness[K, V]) {
	ctx := ctxT(t)
	v, _ := load(t, ctx, h.Source)
	if _, _, err := h.Source.Changes(ctx, v+1000); !errors.Is(err, refset.ErrSnapshotRequired) {
		t.Fatalf("history reset: Changes(%d) with head %d: %v, want ErrSnapshotRequired", v+1000, v, err)
	}
}

func testWatch[K comparable, V any](t *testing.T, h Harness[K, V]) {
	ctx := ctxT(t)
	wctx, cancel := context.WithCancel(ctx)
	ch, err := h.Source.Watch(wctx)
	if err != nil {
		cancel()
		t.Skipf("Watch unsupported (%v); the dataset will poll", err)
	}
	var head uint64
	for i := range 5 {
		if head, err = h.Apply(ctx, refset.Op[K, V]{Key: h.Key(i), Value: h.Value(i)}); err != nil {
			t.Fatal(err)
		}
	}
	timeout := time.After(200 * time.Millisecond)
collect:
	for {
		select {
		case v, ok := <-ch:
			if !ok {
				t.Fatal("C5: Watch channel closed before cancellation")
			}
			if v > head {
				t.Fatalf("C5: Watch sent version %d beyond the head %d", v, head)
			}
		case <-timeout:
			break collect
		}
	}
	cancel()
	closed := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-closed:
			t.Fatal("C5: Watch channel not closed after ctx was canceled")
		}
	}
}

func testKeyLoader[K comparable, V any](t *testing.T, h Harness[K, V]) {
	ctx := ctxT(t)
	kl := h.Source.(refset.KeyLoader[K, V])
	k := h.Key(1)
	if _, err := h.Apply(ctx, refset.Op[K, V]{Key: k, Value: h.Value(1)}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Apply(ctx, refset.Op[K, V]{Key: k, Value: h.Value(2)}); err != nil {
		t.Fatal(err)
	}
	v, ok, err := kl.LoadKey(ctx, k)
	if err != nil || !ok || !h.equal(v, h.Value(2)) {
		t.Fatalf("C6: LoadKey after two writes = %v, %v, %v; want the latest value", v, ok, err)
	}
	if _, err := h.Apply(ctx, refset.Op[K, V]{Key: k, Delete: true}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := kl.LoadKey(ctx, k); err != nil || ok {
		t.Fatalf("C6: LoadKey after delete = ok %v, err %v; want not found", ok, err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i%4 == 0 {
				_, err := h.Apply(ctx, refset.Op[K, V]{Key: h.Key(i % 10), Value: h.Value(i)})
				if err != nil {
					errs <- err
				}
				return
			}
			if _, _, err := kl.LoadKey(ctx, h.Key(i%10)); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent LoadKey: %v", err)
	}
}

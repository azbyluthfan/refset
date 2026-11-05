package refset_test

import (
	"context"
	"maps"
	"testing"
	"testing/quick"
	"time"

	"github.com/azbyluthfan/refset"
)

// step is one random operation for the property test.
type step struct {
	Key    uint8
	Value  int16
	Delete bool
	Batch  uint8 // extra ops committed in the same version
	Sync   bool  // wait for the replica and compare before continuing
}

// G1, G2: at every point the replica syncs, its snapshot equals the source
// state at that version, whatever the batch boundaries and delivery timing.
func TestPropertySnapshotMatchesModel(t *testing.T) {
	check := func(steps []step, dropNotify bool) bool {
		src := newSrc()
		d, err := refset.Open(context.Background(), src, refset.Options{
			Shards: 4, PollInterval: time.Hour, WaitBound: 5 * time.Second,
		})
		if err != nil {
			t.Log(err)
			return false
		}
		defer func() { _ = d.Close() }()
		src.SetDropNotifications(dropNotify)
		key := func(k uint8) string { return string(rune('a' + k%12)) }

		for i, s := range steps {
			ops := []refset.Op[string, int]{{Key: key(s.Key), Value: int(s.Value), Delete: s.Delete}}
			for j := range int(s.Batch % 4) {
				ops = append(ops, refset.Op[string, int]{Key: key(s.Key + uint8(j) + 1), Value: int(s.Value) + j})
			}
			v := src.Apply(ops...)
			if !s.Sync && i != len(steps)-1 {
				continue
			}
			if _, _, err := d.GetAtLeast(context.Background(), "a", v); err != nil {
				t.Logf("sync to v%d: %v", v, err)
				return false
			}
			snap := d.Snapshot()
			want := src.State()
			if snap.Version() != v || snap.Len() != len(want) || !maps.Equal(maps.Collect(snap.All()), want) {
				t.Logf("v%d: snapshot v%d %v, want %v", v, snap.Version(), maps.Collect(snap.All()), want)
				return false
			}
		}
		return true
	}
	if err := quick.Check(check, &quick.Config{MaxCount: 150}); err != nil {
		t.Fatal(err)
	}
}

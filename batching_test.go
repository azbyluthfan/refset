package refset_test

import (
	"context"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

// batchFixture opens a dataset on a fake clock, so batch timers fire only
// when the test advances time.
func batchFixture(t *testing.T) (*refsettest.MemSource[string, int], *refsettest.FakeClock, *refset.Dataset[string, int]) {
	t.Helper()
	src := newSrc()
	src.Upsert("a", 1)
	clock := refsettest.NewFakeClock(time.Date(2026, 11, 11, 9, 0, 0, 0, time.UTC))
	d := open(t, src, refset.Options{Clock: clock, PollInterval: time.Hour, BatchWindow: 5 * time.Millisecond})
	eventually(t, "poll ticker armed", func() bool { return clock.Timers() == 1 })
	return src, clock, d
}

// A change after a quiet period is applied as soon as its notification
// arrives: the batch window doesn't delay it.
func TestNotificationAfterQuietPeriodAppliesAtOnce(t *testing.T) {
	src, clock, d := batchFixture(t)
	clock.Advance(time.Second) // quiet since Open
	v := src.Upsert("a", 2)
	eventually(t, "applied without advancing the clock", func() bool { return d.Version() == v })
}

// Changes arriving within the window of the last poll are batched: the
// first is applied at once, the rest together when the window ends.
func TestBurstIsBatched(t *testing.T) {
	src, clock, d := batchFixture(t)
	clock.Advance(time.Second)
	v1 := src.Upsert("a", 2)
	eventually(t, "first change applied", func() bool { return d.Version() == v1 })
	v2 := src.Upsert("a", 3)
	v3 := src.Upsert("b", 4)
	eventually(t, "batch timer armed", func() bool { return clock.Timers() == 2 })
	if d.Version() != v1 {
		t.Fatalf("applied %d inside the batch window", d.Version())
	}
	polls := d.Stats().Polls
	clock.Advance(5 * time.Millisecond)
	eventually(t, "batch applied", func() bool { return d.Version() == v3 })
	if got := d.Stats().Polls - polls; got != 1 {
		t.Fatalf("%d polls for one batch", got)
	}
	_ = v2
}

// A request for a version a notification already announced polls at once
// instead of waiting for the batched poll.
func TestReadYourWritesDoesNotWaitForBatch(t *testing.T) {
	src, clock, d := batchFixture(t)
	clock.Advance(time.Second)
	v1 := src.Upsert("a", 2)
	eventually(t, "first change applied", func() bool { return d.Version() == v1 })
	v2 := src.Upsert("a", 3)
	eventually(t, "batch timer armed", func() bool { return clock.Timers() >= 2 })
	got, ok, err := d.GetAtLeast(context.Background(), "a", v2)
	if err != nil || !ok || got != 3 {
		t.Fatalf("GetAtLeast = %d, %v, %v", got, ok, err)
	}
}

func TestStatsCountCopies(t *testing.T) {
	src := newSrc()
	for i := range 10_000 {
		src.Upsert(string(rune('a'+i%26))+string(rune(i)), i)
	}
	d := open(t, src, quiet(refset.Options{}))
	st := d.Stats()
	if st.Shards != 256 || st.Applies != 0 {
		t.Fatalf("after Open: %+v", st)
	}
	v := src.Upsert("new", 1)
	if _, _, err := d.GetAtLeast(context.Background(), "new", v); err != nil {
		t.Fatal(err)
	}
	st = d.Stats()
	if st.Applies != 1 || st.ShardsCopied != 1 || st.EntriesCopied < 10 || st.EntriesCopied > 100 {
		t.Fatalf("one-key change: Applies %d, ShardsCopied %d, EntriesCopied %d", st.Applies, st.ShardsCopied, st.EntriesCopied)
	}
}

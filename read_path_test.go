package refset_test

import (
	"context"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
)

// Rule R1 (design §4.11): the read path never allocates. (It also must not
// write shared memory; that half is enforced by review and by the padded
// Dataset layout, see TestDatasetFieldGroupsOnSeparateCacheLines.)
func TestReadPathDoesNotAllocate(t *testing.T) {
	src := newSrc()
	for i := range 100 {
		src.Upsert("k"+string(rune('a'+i%26)), i)
	}
	d := open(t, src, refset.Options{
		PollInterval:   time.Hour,
		MaxStaleness:   2 * time.Hour,
		MaxLagVersions: 10,
	})
	ctx := context.Background()
	v := d.Version()
	withVersion := refset.ContextWithMinVersion(ctx, "plans", v)

	reads := map[string]func(){
		"Get":                   func() { d.Get("ka") },
		"Snapshot.Get":          func() { s := d.Snapshot(); s.Get("ka") },
		"GetAtLeast(satisfied)": func() { _, _, _ = d.GetAtLeast(ctx, "ka", v) },
		"GetCtx(no version)":    func() { _, _, _ = d.GetCtx(ctx, "ka") },
		"GetCtx(satisfied)":     func() { _, _, _ = d.GetCtx(withVersion, "ka") },
		"GetFresh(fresh)":       func() { _, _, _ = d.GetFresh("ka") },
		"Version":               func() { d.Version() },
	}
	for name, read := range reads {
		if n := testing.AllocsPerRun(1000, read); n != 0 {
			t.Errorf("%s allocates %.1f times per call, want 0", name, n)
		}
	}
}

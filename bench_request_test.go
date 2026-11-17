package refset_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
)

// Version-aware reads when the version is already applied: the request path
// of a read-your-writes service. They must cost no more than Get plus the
// version check, and allocate nothing.
func BenchmarkGetForRequestParallel(b *testing.B) {
	src := newSrc()
	const n = 1024
	keys := make([]string, n)
	for i := range n {
		keys[i] = "user-" + strconv.Itoa(i)
		src.Upsert(keys[i], i)
	}
	d, err := refset.Open(context.Background(), refset.Source[string, int](src), refset.Options{PollInterval: time.Hour})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = d.Close() })
	noMin := context.Background()
	withMin := refset.ContextWithMinVersion(context.Background(), d.Name(), 1)

	run := func(b *testing.B, read func(i int)) {
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				read(i & (n - 1))
				i++
			}
		})
	}
	b.Run("GetForRequest/no-min", func(b *testing.B) {
		run(b, func(i int) { _, _, _ = d.GetForRequest(noMin, keys[i]) })
	})
	b.Run("GetForRequest/min-applied", func(b *testing.B) {
		run(b, func(i int) { _, _, _ = d.GetForRequest(withMin, keys[i]) })
	})
	b.Run("GetAtLeast/applied", func(b *testing.B) {
		run(b, func(i int) { _, _, _ = d.GetAtLeast(noMin, keys[i], 1) })
	})
}

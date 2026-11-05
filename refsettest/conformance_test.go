package refsettest_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

func memHarness(src refset.Source[string, int], mem *refsettest.MemSource[string, int]) refsettest.Harness[string, int] {
	return refsettest.Harness[string, int]{
		Source: src,
		Apply: func(_ context.Context, ops ...refset.Op[string, int]) (uint64, error) {
			return mem.Apply(ops...), nil
		},
		Compact: func(context.Context) error { mem.Compact(); return nil },
		Key:     func(i int) string { return "k" + strconv.Itoa(i) },
		Value:   func(i int) int { return i },
	}
}

func TestMemSourceConformance(t *testing.T) {
	refsettest.RunConformance(t, func(*testing.T) refsettest.Harness[string, int] {
		mem := refsettest.NewMemSource[string, int]("plans")
		return memHarness(mem, mem)
	})
}

func TestMemSourceWithKeyLoaderConformance(t *testing.T) {
	refsettest.RunConformance(t, func(*testing.T) refsettest.Harness[string, int] {
		mem := refsettest.NewMemSource[string, int]("plans")
		return memHarness(refsettest.WithKeyLoader(mem), mem)
	})
}

// A Replica with no faults set behaves exactly like its source.
func TestReplicaConformance(t *testing.T) {
	refsettest.RunConformance(t, func(*testing.T) refsettest.Harness[string, int] {
		mem := refsettest.NewMemSource[string, int]("plans")
		return memHarness(refsettest.WithKeyLoader(mem.NewReplica()), mem)
	})
}

// A slow Replica (every call takes a few milliseconds) still meets the contract.
func TestSlowReplicaConformance(t *testing.T) {
	refsettest.RunConformance(t, func(*testing.T) refsettest.Harness[string, int] {
		mem := refsettest.NewMemSource[string, int]("plans")
		r := mem.NewReplica()
		r.SetDelay(2 * time.Millisecond)
		return memHarness(r, mem)
	})
}

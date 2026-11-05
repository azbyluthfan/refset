package refsettest_test

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

// Sources that each break one requirement, to check that RunConformance
// catches them rather than passing everything.

type skipsDelta struct {
	*refsettest.MemSource[string, int]
}

func (s skipsDelta) Changes(ctx context.Context, after uint64) (uint64, []refset.Delta[string, int], error) {
	head, deltas, err := s.MemSource.Changes(ctx, after)
	if len(deltas) >= 3 {
		deltas = append(deltas[:1], deltas[2:]...) // lose one change
	}
	return head, deltas, err
}

type dropsKeyFromSnapshot struct {
	*refsettest.MemSource[string, int]
}

func (s dropsKeyFromSnapshot) LoadSnapshot(ctx context.Context, add func(string, int)) (uint64, error) {
	skipped := false
	return s.MemSource.LoadSnapshot(ctx, func(k string, v int) {
		if !skipped {
			skipped = true
			return
		}
		add(k, v)
	})
}

type ignoresHistoryReset struct {
	*refsettest.MemSource[string, int]
}

func (s ignoresHistoryReset) Changes(ctx context.Context, after uint64) (uint64, []refset.Delta[string, int], error) {
	if head := s.Head(); after > head {
		return after, nil, nil
	}
	return s.MemSource.Changes(ctx, after)
}

var brokenSources = map[string]struct {
	wrap func(*refsettest.MemSource[string, int]) refset.Source[string, int]
	want string // substring expected in the failure output
}{
	"skips_delta":   {func(m *refsettest.MemSource[string, int]) refset.Source[string, int] { return skipsDelta{m} }, "C3:"},
	"drops_key":     {func(m *refsettest.MemSource[string, int]) refset.Source[string, int] { return dropsKeyFromSnapshot{m} }, "C1:"},
	"ignores_reset": {func(m *refsettest.MemSource[string, int]) refset.Source[string, int] { return ignoresHistoryReset{m} }, "history reset"},
}

// TestBrokenSourceChild runs RunConformance on one broken source. It only runs
// when TestConformanceCatchesBrokenSources starts it in a child process.
func TestBrokenSourceChild(t *testing.T) {
	name := os.Getenv("REFSET_BROKEN_SOURCE")
	if name == "" {
		t.Skip("run by TestConformanceCatchesBrokenSources")
	}
	b := brokenSources[name]
	refsettest.RunConformance(t, func(*testing.T) refsettest.Harness[string, int] {
		mem := refsettest.NewMemSource[string, int]("plans")
		return memHarness(b.wrap(mem), mem)
	})
}

func TestConformanceCatchesBrokenSources(t *testing.T) {
	for name, b := range brokenSources {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestBrokenSourceChild$", "-test.count=1")
			cmd.Env = append(os.Environ(), "REFSET_BROKEN_SOURCE="+name)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("RunConformance passed a source that breaks the contract:\n%s", out)
			}
			if !strings.Contains(string(out), b.want) {
				t.Fatalf("failure output does not mention %q:\n%s", b.want, out)
			}
		})
	}
}

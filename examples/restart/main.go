// Command restart shows refset.SnapshotFile: a replica saves its dataset to
// local disk, and after a restart serves reads from that file at once while
// it catches up with the source in the background. Until it has caught up,
// GetFresh refuses to answer, so fail-closed checks never act on old data.
//
//	go run ./restart
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

type Plan struct {
	Name     string
	MaxSeats int
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "refset-restart-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	path := filepath.Join(dir, "plans.snap")

	// The source of truth. An in-memory one here; any refset.Source works.
	source := refsettest.NewMemSource[string, Plan]("plans")
	source.Upsert("free", Plan{"Free", 1})
	source.Upsert("pro", Plan{"Pro", 10})

	opts := refset.Options{PollInterval: time.Second, MaxStaleness: 2 * time.Second}

	// First run: load from the source, then save before shutting down.
	file := &refset.SnapshotFile[string, Plan]{Source: source, Path: path}
	plans, err := refset.Open(ctx, file, opts)
	if err != nil {
		return err
	}
	if err := file.Save(plans.Snapshot()); err != nil {
		return err
	}
	_ = plans.Close()
	fmt.Printf("First run: saved version %d to %s\n", plans.Version(), filepath.Base(path))

	// While the replica is down, the source changes.
	source.Upsert("pro", Plan{"Pro", 25})

	// Restart, with the source slow to answer (a gate holds its replies).
	gate := make(chan struct{})
	source.SetChangesGate(gate)
	plans, err = refset.Open(ctx, file, opts)
	if err != nil {
		return err
	}
	defer func() { _ = plans.Close() }()

	p, _ := plans.Get("pro")
	_, _, freshErr := plans.GetFresh("pro")
	fmt.Printf("Restarted from the file:   Get pro = %d seats (version %d); GetFresh: %v\n",
		p.MaxSeats, plans.Version(), shortErr(freshErr))

	close(gate) // the source answers; the replica catches up
	for {
		if _, _, err := plans.GetFresh("pro"); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	p, _, _ = plans.GetFresh("pro")
	fmt.Printf("After catching up:         GetFresh pro = %d seats (version %d)\n", p.MaxSeats, plans.Version())
	return nil
}

func shortErr(err error) string {
	if errors.Is(err, refset.ErrTooStale) {
		return "ErrTooStale (not confirmed current yet)"
	}
	return fmt.Sprint(err)
}

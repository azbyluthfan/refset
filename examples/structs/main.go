// Command structs feeds refset from structs the application already holds in
// memory, with no database adapter: the app keeps its plans in a slice plus a
// version it bumps on every change, and refset.ReloadSource turns that into a
// dataset with lock-free reads and read-your-writes.
//
//	go run ./structs
package main

import (
	"context"
	"fmt"
	"log"
	"slices"
	"sync"

	"github.com/azbyluthfan/refset"
)

type Plan struct {
	ID         string
	Name       string
	MaxSeats   int
	PriceCents int
	Features   []string // shared with every reader: treat as read-only
}

// PlanStore is your application's own code. However it gets its data (a
// database query at startup, an admin API, a config file), it keeps plans as
// structs and bumps Version on every change, under the same lock.
type PlanStore struct {
	mu      sync.Mutex
	version uint64
	plans   []Plan
}

// Replace swaps in a new list of plans and returns the new version.
func (s *PlanStore) Replace(plans []Plan) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plans = slices.Clone(plans)
	s.version++
	return s.version
}

// Update changes one plan and returns the new version.
func (s *PlanStore) Update(id string, change func(*Plan)) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.plans {
		if s.plans[i].ID == id {
			p := s.plans[i]
			p.Features = slices.Clone(p.Features) // never modify what readers hold
			change(&p)
			s.plans[i] = p
		}
	}
	s.version++
	return s.version
}

// Load and Version are the two functions refset needs.
func (s *PlanStore) Load(context.Context) (uint64, []Plan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version, slices.Clone(s.plans), nil
}

func (s *PlanStore) Version(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version, nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	store := &PlanStore{}
	store.Replace([]Plan{
		{ID: "free", Name: "Free", MaxSeats: 1, PriceCents: 0, Features: []string{"basic-reports"}},
		{ID: "pro", Name: "Pro", MaxSeats: 10, PriceCents: 2900, Features: []string{"basic-reports", "advanced-reports"}},
	})

	plans, err := refset.Open(ctx, &refset.ReloadSource[string, Plan]{
		Dataset: "plans",
		Load:    store.Load,
		Version: store.Version,
		Key:     func(p Plan) string { return p.ID },
	}, refset.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = plans.Close() }()

	show := func(label string) {
		p, _ := plans.Get("pro")
		fmt.Printf("%-44s pro: %d seats, $%.2f  (version %d)\n", label, p.MaxSeats, float64(p.PriceCents)/100, plans.Version())
	}
	show("Served from memory:")

	// An admin raises the Pro seat limit. The store bumps its version.
	v := store.Update("pro", func(p *Plan) { p.MaxSeats = 25 })
	show("Right after the change, plain Get:")

	// A request that must see the change asks for that version: refset
	// reloads from the store at once instead of waiting for its next poll.
	p, _, err := plans.GetAtLeast(ctx, "pro", v)
	if err != nil {
		return err
	}
	fmt.Printf("%-44s pro: %d seats\n", fmt.Sprintf("GetAtLeast(version %d):", v), p.MaxSeats)
	show("And every later Get:")
	return nil
}

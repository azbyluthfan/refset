// Command startup shows refset.OpenBackground: a service that starts while
// the source of its reference data is down, instead of failing to start.
//
// The subscriptions here come from another service with two endpoints: a bulk
// one that lists every subscription (what refset loads) and a per-key one
// (what the application called on every request before it had refset). At
// startup the bulk endpoint is down. The application:
//
//  1. opens the dataset with OpenBackground and waits a bounded time on
//     Ready, so a normal start would be fully loaded before serving;
//
//  2. starts anyway when that times out: GetForRequest sends each lookup to
//     the per-key endpoint, through the source's KeyLoader;
//
//  3. serves locally once a background retry loads the dataset.
//
//     go run ./startup
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/azbyluthfan/refset"
)

type Subscription struct {
	UserID string
	Plan   string
}

// SubscriptionService stands in for the other service and its two
// endpoints. Its bulk endpoint can be switched off.
type SubscriptionService struct {
	mu       sync.Mutex
	version  uint64
	subs     map[string]Subscription
	bulkDown atomic.Bool
}

var errUnavailable = errors.New("subscription service: bulk endpoint unavailable")

// ListAll is the bulk endpoint: every subscription and the current version.
func (s *SubscriptionService) ListAll(context.Context) (uint64, []Subscription, error) {
	if s.bulkDown.Load() {
		return 0, nil, errUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		out = append(out, sub)
	}
	return s.version, out, nil
}

// Version is a cheap call that returns the version, for polling.
func (s *SubscriptionService) Version(context.Context) (uint64, error) {
	if s.bulkDown.Load() {
		return 0, errUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.version, nil
}

// Get is the per-key endpoint: one user's subscription.
func (s *SubscriptionService) Get(_ context.Context, userID string) (Subscription, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subs[userID]
	return sub, ok, nil
}

// source is a ReloadSource with a KeyLoader: until the dataset is loaded,
// GetForRequest and GetAtLeast ask the per-key endpoint instead. Embedding
// keeps everything ReloadSource does, including Equal.
type source struct {
	*refset.ReloadSource[string, Subscription]
	svc *SubscriptionService
}

// LoadKey implements refset.KeyLoader. refset caps how many run at once
// (Options.MaxConcurrentFallbacks, 32 by default), so an outage can't flood
// the per-key endpoint.
func (s source) LoadKey(ctx context.Context, userID string) (Subscription, bool, error) {
	return s.svc.Get(ctx, userID)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	svc := &SubscriptionService{version: 1, subs: map[string]Subscription{
		"u1": {UserID: "u1", Plan: "pro"},
		"u2": {UserID: "u2", Plan: "free"},
	}}
	svc.bulkDown.Store(true)
	fmt.Println("subscription service: bulk endpoint down")

	src := source{svc: svc, ReloadSource: &refset.ReloadSource[string, Subscription]{
		Dataset: "subscriptions",
		Load:    svc.ListAll,
		Version: svc.Version,
		Key:     func(s Subscription) string { return s.UserID },
	}}
	subs, err := refset.OpenBackground(src, refset.Options{PollInterval: time.Second})
	if err != nil {
		return err // invalid options; an unreachable source is never an error here
	}
	defer func() { _ = subs.Close() }()

	// Wait a bounded time, so a normal start serves local data from its first
	// request. Report ready to the platform after this, either way.
	start := time.Now()
	select {
	case <-subs.Ready():
		fmt.Println("loaded before serving")
	case <-time.After(500 * time.Millisecond):
		fmt.Printf("not loaded after %v; starting anyway\n", time.Since(start).Round(100*time.Millisecond))
	}

	lookup := func(userID string) {
		sub, ok, err := subs.GetForRequest(ctx, userID)
		st := subs.Stats()
		switch {
		case errors.Is(err, refset.ErrNotLoaded):
			fmt.Printf("  %s: not loaded and no fallback\n", userID)
		case err != nil:
			fmt.Printf("  %s: %v\n", userID, err)
		default:
			fmt.Printf("  %s: plan=%s found=%v (loaded=%v, fallbacks so far=%d)\n", userID, sub.Plan, ok, st.Loaded, st.Fallbacks)
		}
	}

	fmt.Println("requests while the dataset loads in the background:")
	lookup("u1")
	lookup("u2")
	if _, _, err := subs.GetFresh("u1"); err != nil {
		fmt.Printf("  GetFresh fails closed: %v\n", err)
	}

	time.Sleep(time.Second)
	svc.bulkDown.Store(false)
	fmt.Println("subscription service: bulk endpoint back")

	select {
	case <-subs.Ready():
	case <-time.After(10 * time.Second):
		return errors.New("dataset not loaded 10s after the source came back")
	}
	st := subs.Stats()
	fmt.Printf("loaded in the background: version %d, %d entries, after %d failed attempts\n",
		st.AppliedVersion, st.Entries, st.PollErrors)

	fmt.Println("requests now (local, no fallback calls):")
	lookup("u1")
	lookup("u2")
	if sub, ok, err := subs.GetFresh("u1"); err == nil {
		fmt.Printf("  GetFresh: plan=%s found=%v\n", sub.Plan, ok)
	}
	return nil
}

// Command baseline-lru plays the plan-upgrade story with a TTL cache
// (hashicorp/golang-lru) in front of the database, the usual way to get fast
// local reads, and then with refset. No database needed:
//
//	go run ./baseline-lru
//
// With the TTL cache, the replica keeps answering "free" until the entry
// expires. With refset, the request carries the version and the replica
// answers "pro".
package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/azbyluthfan/refset"
	"github.com/azbyluthfan/refset/refsettest"
)

// db stands in for the primary database.
type db struct {
	mu    sync.Mutex
	plans map[string]string
}

func (d *db) get(user string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.plans[user]
}

func (d *db) set(user, tier string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.plans[user] = tier
}

func main() {
	fmt.Println("== TTL cache (golang-lru expirable, TTL 300ms) ==")
	primary := &db{plans: map[string]string{"user-1": "free"}}
	cache := expirable.NewLRU[string, string](1000, nil, 300*time.Millisecond)
	read := func() string {
		if tier, ok := cache.Get("user-1"); ok {
			return tier
		}
		tier := primary.get("user-1")
		cache.Add("user-1", tier)
		return tier
	}
	fmt.Println("replica reads user-1:", read(), "(now cached)")
	primary.set("user-1", "pro")
	fmt.Println("billing upgrades user-1 to pro")
	start := time.Now()
	for read() != "pro" {
		time.Sleep(25 * time.Millisecond)
	}
	fmt.Printf("replica kept answering \"free\" for %v, until the TTL expired\n", time.Since(start).Round(10*time.Millisecond))
	fmt.Println("(a longer TTL means longer staleness; a shorter one means more database load)")

	fmt.Println("\n== refset ==")
	src := refsettest.NewMemSource[string, string]("plans")
	src.Upsert("user-1", "free")
	replica := src.NewReplica()
	replica.SetDropNotifications(true) // the replica hasn't heard about the change
	plans, err := refset.Open(context.Background(), replica, refset.Options{PollInterval: time.Hour})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = plans.Close() }()
	tier, _ := plans.Get("user-1")
	fmt.Println("replica reads user-1:", tier)
	version := src.Upsert("user-1", "pro")
	fmt.Printf("billing upgrades user-1 to pro and returns Refset-Version: plans=%d\n", version)
	tier, _ = plans.Get("user-1")
	fmt.Println("replica, request without the version:", tier, "(same staleness as the cache)")
	start = time.Now()
	tier, _, err = plans.GetAtLeast(context.Background(), "user-1", version)
	fmt.Printf("replica, request with the version:    %s in %v (err %v)\n", tier, time.Since(start).Round(time.Microsecond), err)
}

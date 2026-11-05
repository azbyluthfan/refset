// Command httpjson feeds refset from an internal HTTP service that returns
// structs as JSON, for example a feature-flag or pricing service owned by
// another team. The service exposes the full list and a cheap version
// endpoint; every replica keeps the list in memory and reloads it when the
// version moves.
//
//	go run ./httpjson
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"time"

	"github.com/azbyluthfan/refset"
)

type Flag struct {
	Name    string `json:"name"`
	Enabled bool   `json:"enabled"`
	Percent int    `json:"percent"` // rollout percentage
}

// flagService stands in for the other team's service.
type flagService struct {
	mu      sync.Mutex
	version uint64
	flags   []Flag
}

func (s *flagService) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /flags", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_ = json.NewEncoder(w).Encode(struct {
			Version uint64 `json:"version"`
			Flags   []Flag `json:"flags"`
		}{s.version, s.flags})
	})
	mux.HandleFunc("GET /flags/version", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, _ = w.Write([]byte(strconv.FormatUint(s.version, 10)))
	})
	return mux
}

func (s *flagService) set(f Flag) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.flags {
		if s.flags[i].Name == f.Name {
			s.flags[i] = f
		}
	}
	s.version++
	return s.version
}

// flagClient is the replica's code: fetch JSON into structs.
type flagClient struct {
	base string
	http *http.Client
}

func (c flagClient) Load(ctx context.Context) (uint64, []Flag, error) {
	var body struct {
		Version uint64 `json:"version"`
		Flags   []Flag `json:"flags"`
	}
	if err := c.getJSON(ctx, "/flags", &body); err != nil {
		return 0, nil, err
	}
	return body.Version, body.Flags, nil
}

func (c flagClient) Version(ctx context.Context) (uint64, error) {
	var v uint64
	err := c.getJSON(ctx, "/flags/version", &v)
	return v, err
}

func (c flagClient) getJSON(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	svc := &flagService{version: 1, flags: []Flag{
		{Name: "new-checkout", Enabled: true, Percent: 10},
		{Name: "dark-mode", Enabled: false},
	}}
	server := httptest.NewServer(svc.handler())
	defer server.Close()

	client := flagClient{base: server.URL, http: &http.Client{Timeout: 2 * time.Second}}
	flags, err := refset.Open(context.Background(), &refset.ReloadSource[string, Flag]{
		Dataset: "flags",
		Load:    client.Load,
		Version: client.Version,
		Key:     func(f Flag) string { return f.Name },
	}, refset.Options{PollInterval: 200 * time.Millisecond})
	if err != nil {
		return err // the flag service is down at startup: don't run without flags
	}
	defer func() { _ = flags.Close() }()

	f, _ := flags.Get("new-checkout")
	fmt.Printf("new-checkout rollout: %d%%   (version %d, read from memory)\n", f.Percent, flags.Version())

	svc.set(Flag{Name: "new-checkout", Enabled: true, Percent: 50})
	fmt.Println("The flag service raises the rollout to 50%...")
	for flags.Version() < 2 {
		time.Sleep(20 * time.Millisecond) // the replica's next poll picks it up
	}
	f, _ = flags.Get("new-checkout")
	fmt.Printf("new-checkout rollout: %d%%   (version %d, after the replica's next poll)\n", f.Percent, flags.Version())
	return nil
}

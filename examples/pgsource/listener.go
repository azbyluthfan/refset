package pgsource

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Listener holds one LISTEN connection per pool and fans notifications out to
// every subscribed source, so a process with ten datasets uses one connection,
// not ten. It reconnects with backoff, and after reconnecting sends each
// subscribed dataset's current head, since notifications may have been missed.
type Listener struct {
	pool *pgxpool.Pool
	log  *slog.Logger

	mu      sync.Mutex
	subs    map[string]map[chan uint64]struct{}
	started bool
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewListener returns a listener for pool. It connects on the first Subscribe.
// logger may be nil.
func NewListener(pool *pgxpool.Pool, logger *slog.Logger) *Listener {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Listener{
		pool:   pool,
		log:    logger,
		subs:   make(map[string]map[chan uint64]struct{}),
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
}

// Subscribe returns a channel of head versions for dataset. It is closed when
// ctx ends. Delivery is best effort.
func (l *Listener) Subscribe(ctx context.Context, dataset string) <-chan uint64 {
	ch := make(chan uint64, 16)
	l.mu.Lock()
	if l.subs[dataset] == nil {
		l.subs[dataset] = make(map[chan uint64]struct{})
	}
	l.subs[dataset][ch] = struct{}{}
	if !l.started {
		l.started = true
		go l.run()
	}
	l.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
		case <-l.ctx.Done():
		}
		l.mu.Lock()
		delete(l.subs[dataset], ch)
		close(ch)
		l.mu.Unlock()
	}()
	return ch
}

// Close stops the listener and closes every subscription.
func (l *Listener) Close() {
	l.cancel()
	l.mu.Lock()
	started := l.started
	l.mu.Unlock()
	if started {
		<-l.done
	}
}

func (l *Listener) run() {
	defer close(l.done)
	backoff := 100 * time.Millisecond
	for l.ctx.Err() == nil {
		if err := l.listen(); err != nil && l.ctx.Err() == nil {
			l.log.Warn("pgsource: listener disconnected; reconnecting", "err", err, "backoff", backoff)
			select {
			case <-time.After(backoff):
			case <-l.ctx.Done():
			}
			backoff = min(backoff*2, 5*time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
	}
}

// listen holds one dedicated connection until it fails or the listener closes.
func (l *Listener) listen() error {
	pc, err := l.pool.Acquire(l.ctx)
	if err != nil {
		return err
	}
	conn := pc.Hijack() // dedicated: never returned to the pool
	defer func() { _ = conn.Close(context.Background()) }()

	if _, err := conn.Exec(l.ctx, "LISTEN "+Channel); err != nil {
		return err
	}
	// Notifications sent while we were disconnected are lost: report heads.
	l.mu.Lock()
	datasets := make([]string, 0, len(l.subs))
	for ds := range l.subs {
		datasets = append(datasets, ds)
	}
	l.mu.Unlock()
	for _, ds := range datasets {
		if h, _, err := headOf(l.ctx, conn, ds); err == nil {
			l.dispatch(ds, h)
		}
	}

	for {
		n, err := conn.WaitForNotification(l.ctx)
		if err != nil {
			return err
		}
		i := strings.LastIndexByte(n.Payload, ':')
		if i < 0 {
			continue
		}
		v, err := strconv.ParseUint(n.Payload[i+1:], 10, 64)
		if err != nil {
			continue
		}
		l.dispatch(n.Payload[:i], v)
	}
}

func (l *Listener) dispatch(dataset string, v uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for ch := range l.subs[dataset] {
		select {
		case ch <- v:
		default: // best effort; the dataset also polls
		}
	}
}

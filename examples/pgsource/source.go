package pgsource

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/azbyluthfan/refset"
)

const defaultBacklogLimit = 10_000

// Table describes how to read one dataset.
type Table[K comparable, V any] struct {
	// Dataset is the name shared with writers (Write, version headers).
	Dataset string

	// Query reads the whole dataset, e.g. "SELECT code, decimals, rate_usd FROM currencies".
	Query string

	// ByKeysQuery reads the rows for a set of keys, given as $1 (a []K), e.g.
	// "... WHERE code = ANY($1)". It must return the same columns as Query. A
	// key with no row is treated as deleted.
	ByKeysQuery string

	// ByKeyQuery reads one row by key ($1), for the fallback when a replica
	// can't catch up in time. Optional: when empty, the source has no
	// refset.KeyLoader and slow reads get refset.ErrNotCaughtUp.
	ByKeyQuery string

	// Key extracts the key from a value.
	Key func(V) K

	// Scan converts a row to a value. Default pgx.RowToStructByName[V].
	Scan pgx.RowToFunc[V]

	// Codec converts keys to and from the changelog's text form. Default
	// handles string and integer keys; required for other key types.
	Codec *KeyCodec[K]

	// BacklogLimit is the number of changed keys above which the source asks
	// replicas to reload a full snapshot instead. Default 10,000.
	BacklogLimit int
}

// KeyCodec converts keys to and from text. Encode must produce what writers
// pass to LogUpsert and what triggers write (the column cast to text).
type KeyCodec[K comparable] struct {
	Encode func(K) string
	Decode func(string) (K, error)
}

// Source is a refset.Source over one PostgreSQL table or query.
type Source[K comparable, V any] struct {
	pool     *pgxpool.Pool
	listener *Listener
	t        Table[K, V]
	codec    KeyCodec[K]
}

// keyedSource adds refset.KeyLoader when Table.ByKeyQuery is set.
type keyedSource[K comparable, V any] struct{ *Source[K, V] }

// NewSource returns a source for table. listener may be shared by every source
// on the pool (one LISTEN connection per process); nil means no notifications,
// so datasets rely on polling. The result implements refset.KeyLoader if
// table.ByKeyQuery is set.
func NewSource[K comparable, V any](pool *pgxpool.Pool, listener *Listener, table Table[K, V]) (refset.Source[K, V], error) {
	switch {
	case pool == nil:
		return nil, errors.New("pgsource: nil pool")
	case table.Dataset == "" || table.Query == "" || table.ByKeysQuery == "" || table.Key == nil:
		return nil, errors.New("pgsource: Table needs Dataset, Query, ByKeysQuery and Key")
	}
	if table.Scan == nil {
		table.Scan = pgx.RowToStructByName[V]
	}
	if table.BacklogLimit <= 0 {
		table.BacklogLimit = defaultBacklogLimit
	}
	s := &Source[K, V]{pool: pool, listener: listener, t: table}
	if table.Codec != nil {
		s.codec = *table.Codec
	} else if c, ok := defaultCodec[K](); ok {
		s.codec = c
	} else {
		var zero K
		return nil, fmt.Errorf("pgsource: Table.Codec is required for key type %T", zero)
	}
	if table.ByKeyQuery != "" {
		return keyedSource[K, V]{s}, nil
	}
	return s, nil
}

// Name implements refset.Source.
func (s *Source[K, V]) Name() string { return s.t.Dataset }

func readOnlyRR() pgx.TxOptions {
	return pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}
}

// LoadSnapshot implements refset.Source: the version and the full query are
// read in one REPEATABLE READ transaction, so they agree (contract C1).
func (s *Source[K, V]) LoadSnapshot(ctx context.Context, add func(K, V)) (uint64, error) {
	tx, err := s.pool.BeginTx(ctx, readOnlyRR())
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	head, _, err := headOf(ctx, tx, s.t.Dataset)
	if err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, s.t.Query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		v, err := s.t.Scan(rows)
		if err != nil {
			return 0, err
		}
		add(s.t.Key(v), v)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return head, tx.Commit(ctx)
}

// Changes implements refset.Source. An idle poll is one statement (the head
// check). Otherwise the head, the changed keys and their current rows are read
// in one REPEATABLE READ transaction, so the result is exactly the state at
// the head (contract C3), returned as a single delta.
func (s *Source[K, V]) Changes(ctx context.Context, after uint64) (uint64, []refset.Delta[K, V], error) {
	head, compacted, err := headOf(ctx, s.pool, s.t.Dataset)
	switch {
	case err != nil:
		return 0, nil, err
	case after > head:
		return 0, nil, refset.ErrSnapshotRequired // history went backwards
	case after == head:
		return head, nil, nil
	case after < compacted:
		return 0, nil, refset.ErrSnapshotRequired
	}

	tx, err := s.pool.BeginTx(ctx, readOnlyRR())
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	head, compacted, err = headOf(ctx, tx, s.t.Dataset)
	if err != nil {
		return 0, nil, err
	}
	if after < compacted || after > head {
		return 0, nil, refset.ErrSnapshotRequired
	}

	rows, err := tx.Query(ctx, `
		SELECT DISTINCT key, reload FROM refset_changelog
		WHERE dataset = $1 AND version > $2 AND version <= $3`,
		s.t.Dataset, int64(after), int64(head))
	if err != nil {
		return 0, nil, err
	}
	var keys []K
	seen := make(map[K]struct{})
	for rows.Next() {
		var text string
		var reload bool
		if err := rows.Scan(&text, &reload); err != nil {
			rows.Close()
			return 0, nil, err
		}
		if reload {
			rows.Close()
			return 0, nil, refset.ErrSnapshotRequired
		}
		k, err := s.codec.Decode(text)
		if err != nil {
			rows.Close()
			return 0, nil, fmt.Errorf("pgsource: dataset %q: changelog key %q: %w", s.t.Dataset, text, err)
		}
		if _, dup := seen[k]; !dup {
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
		if len(keys) > s.t.BacklogLimit {
			rows.Close()
			return 0, nil, refset.ErrSnapshotRequired
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}

	found := make(map[K]V, len(keys))
	if len(keys) > 0 {
		rows, err := tx.Query(ctx, s.t.ByKeysQuery, keys)
		if err != nil {
			return 0, nil, err
		}
		vals, err := pgx.CollectRows(rows, s.t.Scan)
		if err != nil {
			return 0, nil, err
		}
		for _, v := range vals {
			found[s.t.Key(v)] = v
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, nil, err
	}

	d := refset.Delta[K, V]{Version: head, Ops: make([]refset.Op[K, V], 0, len(keys))}
	for _, k := range keys {
		if v, ok := found[k]; ok {
			d.Ops = append(d.Ops, refset.Op[K, V]{Key: k, Value: v})
		} else {
			d.Ops = append(d.Ops, refset.Op[K, V]{Key: k, Delete: true})
		}
	}
	return head, []refset.Delta[K, V]{d}, nil
}

// Watch implements refset.Source through the shared Listener.
func (s *Source[K, V]) Watch(ctx context.Context) (<-chan uint64, error) {
	if s.listener == nil {
		return nil, errors.New("pgsource: no Listener; relying on polling")
	}
	return s.listener.Subscribe(ctx, s.t.Dataset), nil
}

// LoadKey implements refset.KeyLoader with Table.ByKeyQuery, on the primary.
func (s keyedSource[K, V]) LoadKey(ctx context.Context, key K) (V, bool, error) {
	var zero V
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return zero, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var standby bool
	if err := tx.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&standby); err != nil {
		return zero, false, err
	}
	if standby {
		return zero, false, ErrStandby
	}
	rows, err := tx.Query(ctx, s.t.ByKeyQuery, key)
	if err != nil {
		return zero, false, err
	}
	v, err := pgx.CollectExactlyOneRow(rows, s.t.Scan)
	if errors.Is(err, pgx.ErrNoRows) {
		return zero, false, nil
	}
	if err != nil {
		return zero, false, err
	}
	return v, true, nil
}

func defaultCodec[K comparable]() (KeyCodec[K], bool) {
	var zero K
	cast := func(enc func(K) string, dec func(string) (any, error)) (KeyCodec[K], bool) {
		return KeyCodec[K]{Encode: enc, Decode: func(s string) (K, error) {
			v, err := dec(s)
			if err != nil {
				var z K
				return z, err
			}
			return v.(K), nil
		}}, true
	}
	switch any(zero).(type) {
	case string:
		return cast(func(k K) string { return any(k).(string) }, func(s string) (any, error) { return s, nil })
	case int:
		return cast(func(k K) string { return strconv.Itoa(any(k).(int)) },
			func(s string) (any, error) { return strconv.Atoi(s) })
	case int32:
		return cast(func(k K) string { return strconv.FormatInt(int64(any(k).(int32)), 10) },
			func(s string) (any, error) { v, err := strconv.ParseInt(s, 10, 32); return int32(v), err })
	case int64:
		return cast(func(k K) string { return strconv.FormatInt(any(k).(int64), 10) },
			func(s string) (any, error) { return strconv.ParseInt(s, 10, 64) })
	case uint64:
		return cast(func(k K) string { return strconv.FormatUint(any(k).(uint64), 10) },
			func(s string) (any, error) { return strconv.ParseUint(s, 10, 64) })
	}
	return KeyCodec[K]{}, false
}

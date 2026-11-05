package refset

import (
	"context"
	"maps"
)

type minVersionsKey struct{}

// ContextWithMinVersion returns a context that asks [Dataset.GetCtx] on the
// named dataset to return data at version v or newer. If the context already
// carries a minimum for that dataset, the larger one is kept.
//
// HTTP services usually call this through refsethttp.Middleware, which parses
// the Refset-Min-Version header.
func ContextWithMinVersion(ctx context.Context, dataset string, v uint64) context.Context {
	old, _ := ctx.Value(minVersionsKey{}).(map[string]uint64)
	if cur, ok := old[dataset]; ok && cur >= v {
		return ctx
	}
	m := maps.Clone(old)
	if m == nil {
		m = make(map[string]uint64, 1)
	}
	m[dataset] = v
	return context.WithValue(ctx, minVersionsKey{}, m)
}

// MinVersion returns the minimum version the context asks for on the named
// dataset, if any.
func MinVersion(ctx context.Context, dataset string) (uint64, bool) {
	m, _ := ctx.Value(minVersionsKey{}).(map[string]uint64)
	v, ok := m[dataset]
	return v, ok
}

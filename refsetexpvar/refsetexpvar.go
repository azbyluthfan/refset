// Package refsetexpvar publishes refset datasets' Stats through the standard
// library's expvar package, so they appear at /debug/vars and can be scraped
// by metrics agents that read it.
//
//	remove := refsetexpvar.Publish(plans)
//	defer remove()
//
// Every published dataset appears in one expvar map named "refset", keyed by
// dataset name, with the fields of refset.Stats (durations in nanoseconds,
// times as RFC 3339). Values are read when /debug/vars is served.
//
// It is a separate package because importing expvar registers /debug/vars on
// http.DefaultServeMux, which also exposes the process's command line and
// memory statistics; only applications that import this package get that.
// If your server uses http.DefaultServeMux, make sure /debug/vars isn't
// reachable from untrusted networks.
package refsetexpvar

import (
	"expvar"
	"sync"

	"github.com/azbyluthfan/refset"
)

// MapName is the expvar variable that holds every published dataset.
const MapName = "refset"

var (
	once     sync.Once
	datasets *expvar.Map
)

func refsetMap() *expvar.Map {
	once.Do(func() {
		if v, ok := expvar.Get(MapName).(*expvar.Map); ok {
			datasets = v
			return
		}
		datasets = expvar.NewMap(MapName)
	})
	return datasets
}

// Publish adds d's Stats to the "refset" expvar map under d.Name(), read
// fresh each time the map is served. Publishing a dataset with the same name
// again replaces the entry. The returned function removes it, for example
// when the dataset is closed.
func Publish[K comparable, V any](d *refset.Dataset[K, V]) (remove func()) {
	name := d.Name()
	m := refsetMap()
	m.Set(name, expvar.Func(func() any { return d.Stats() }))
	return func() { m.Delete(name) }
}

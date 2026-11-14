package refsettest

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"sort"
	"strings"

	"github.com/azbyluthfan/refset"
)

// TB is the part of testing.TB that DetectValueMutation uses.
type TB interface {
	Helper()
	Errorf(format string, args ...any)
	Cleanup(func())
}

// DetectValueMutation fails the test if code under test changes any value it
// read from d. Values a Dataset returns are shared with every other reader:
// changing a slice, map or pointed-to struct inside one changes it for all
// of them, and changing it while others read is a data race.
//
// It fingerprints every value in d's current version now (following
// pointers, slices, maps and unexported fields) and checks them again when
// the test ends, reporting each key whose value changed. Call it after Open
// and before the code under test runs. It is meant for tests: the
// fingerprint reads the whole dataset. Run tests with -race as well, which
// catches changes made while other goroutines read.
func DetectValueMutation[K comparable, V any](t TB, d *refset.Dataset[K, V]) {
	t.Helper()
	snap := d.Snapshot()
	before := make(map[K]uint64, snap.Len())
	for k, v := range snap.All() {
		before[k] = fingerprint(v)
	}
	t.Cleanup(func() {
		t.Helper()
		var changed []string
		for k, v := range snap.All() {
			if fingerprint(v) != before[k] {
				changed = append(changed, fmt.Sprint(k))
			}
		}
		if len(changed) == 0 {
			return
		}
		sort.Strings(changed)
		if len(changed) > 10 {
			changed = append(changed[:10], fmt.Sprintf("and %d more", len(changed)-10))
		}
		t.Errorf("refset: values read from dataset %q were modified (keys %s). "+
			"Values are shared by every reader: copy a value before changing what it contains",
			d.Name(), strings.Join(changed, ", "))
	})
}

// fingerprint hashes a value's contents, following pointers, slices and
// maps (in sorted key order), and reading unexported fields.
func fingerprint(v any) uint64 {
	h := fnv.New64a()
	var b strings.Builder
	writeValue(&b, reflect.ValueOf(v), map[uintptr]bool{})
	_, _ = h.Write([]byte(b.String()))
	return h.Sum64()
}

func writeValue(b *strings.Builder, v reflect.Value, seen map[uintptr]bool) {
	if !v.IsValid() {
		b.WriteString("<nil>")
		return
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			b.WriteString("nil")
			return
		}
		if seen[v.Pointer()] {
			b.WriteString("<cycle>")
			return
		}
		seen[v.Pointer()] = true
		b.WriteString("&")
		writeValue(b, v.Elem(), seen)
	case reflect.Interface:
		writeValue(b, v.Elem(), seen)
	case reflect.Slice, reflect.Array:
		if v.Kind() == reflect.Slice && v.IsNil() {
			b.WriteString("nil")
			return
		}
		b.WriteString("[")
		for i := range v.Len() {
			writeValue(b, v.Index(i), seen)
			b.WriteString(",")
		}
		b.WriteString("]")
	case reflect.Map:
		if v.IsNil() {
			b.WriteString("nil")
			return
		}
		entries := make([]string, 0, v.Len())
		for it := v.MapRange(); it.Next(); {
			var e strings.Builder
			writeValue(&e, it.Key(), seen)
			e.WriteString(":")
			writeValue(&e, it.Value(), seen)
			entries = append(entries, e.String())
		}
		sort.Strings(entries)
		b.WriteString("{" + strings.Join(entries, ",") + "}")
	case reflect.Struct:
		b.WriteString("{")
		for i := range v.NumField() {
			writeValue(b, v.Field(i), seen)
			b.WriteString(";")
		}
		b.WriteString("}")
	case reflect.String:
		fmt.Fprintf(b, "%q", v.String())
	case reflect.Bool:
		fmt.Fprint(b, v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		fmt.Fprint(b, v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		fmt.Fprint(b, v.Uint())
	case reflect.Float32, reflect.Float64:
		fmt.Fprint(b, v.Float())
	case reflect.Complex64, reflect.Complex128:
		fmt.Fprint(b, v.Complex())
	default: // chan, func, unsafe pointer: identity only
		fmt.Fprintf(b, "%s@%x", v.Kind(), v.Pointer())
	}
}

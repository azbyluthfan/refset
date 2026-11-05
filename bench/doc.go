// Package bench compares refset's read path with common alternatives and
// measures how it scales with dataset size and value size (design §8.7,
// rules R4 and R5). It is a separate module so the library stays free of
// dependencies. Results are recorded in docs/benchmarks.md.
//
//	cd bench && go test -run '^$' -bench . -benchmem
package bench

module github.com/azbyluthfan/refset/bench

go 1.26

// Benchmarks run against the library in this repository (the replace below),
// so a change can be measured before it is released.
replace github.com/azbyluthfan/refset => ../

require (
	github.com/azbyluthfan/refset v0.2.0
	github.com/hashicorp/golang-lru/v2 v2.0.7
)

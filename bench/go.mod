module github.com/azbyluthfan/refset/bench

go 1.26

// Benchmarks always run against the library in this repository.
replace github.com/azbyluthfan/refset => ../

require (
	github.com/azbyluthfan/refset v0.0.0-00010101000000-000000000000
	github.com/hashicorp/golang-lru/v2 v2.0.7
)

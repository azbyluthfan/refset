package refset

// cacheLineSize is the largest common cache line: 128 bytes on Apple Silicon,
// and also enough on x86 (64-byte lines) where the hardware often prefetches
// lines in adjacent pairs. Padding by it keeps two field groups off the same
// line on all of them (design §4.11, rule R2).
const cacheLineSize = 128

// cacheLinePad separates groups of struct fields written by different
// goroutines. It is defined here rather than imported to keep the module
// dependency-free.
type cacheLinePad struct{ _ [cacheLineSize]byte }

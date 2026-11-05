// Package refset keeps a whole reference dataset (plans, permissions, prices,
// feature flags) in the memory of every replica, coherent with its source of
// truth, with dataset-level versions that let a request demand "at least
// version N".
//
// Reads are local and lock-free. Changes arrive in the background through a
// [Source] that the application implements; refset itself has no knowledge of
// any database, cache, broker or network protocol, and depends only on the Go
// standard library.
//
// The package is under construction and its API is experimental. See
// docs/design.md in the repository for the full design and
// docs/source-contract.md for what a Source must guarantee.
package refset

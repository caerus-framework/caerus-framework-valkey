// Package patterns provides small, optional helpers for common Valkey usage
// patterns. It is not a Redis ORM: there are no struct tags, no HASH
// repositories, and no replacement for valkey-go's command API. The chassis
// (cf_valkey.CFValkey) owns lifecycle, health, reload, key prefix, and
// metrics; this package adds a few sharp, prefix-aware helpers on top.
//
// Allowlisted helpers:
//
//   - Mutex: a single-instance distributed lock (SET NX PX + Lua unlock).
//     Not Redlock. See the Mutex godoc for failure modes. Lock traffic is
//     counted on the component's LockMeter and exposed via /metrics as
//     lock_*_total counters.
//   - GetJSON / SetJSON: marshal/unmarshal helpers with prefix-aware keys.
//   - GetOrLoad: in-process singleflight cache-aside (one loader per key per
//     process). Process-local only; other pods still stampede.
//
// All helpers take a *cf_valkey.CFValkey (or a minimal interface) and use
// Client() + Key() so they are prefix-aware. Apps that only need a client
// never import this package.
package patterns

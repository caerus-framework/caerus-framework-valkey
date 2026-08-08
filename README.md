# caerus-framework-valkey

[![CI](https://github.com/caerus-framework/caerus-framework-valkey/actions/workflows/ci.yml/badge.svg)](https://github.com/caerus-framework/caerus-framework-valkey/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/caerus-framework/caerus-framework-valkey/graph/badge.svg)](https://codecov.io/gh/caerus-framework/caerus-framework-valkey)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)


Caerus Framework Valkey Component — the **client & ops chassis** for
[Valkey](https://valkey.io/) / Redis. Wraps a
[valkey-go](https://github.com/valkey-io/valkey-go) client and owns lifecycle,
health, reload, key prefix, named instances, TLS, timeouts, and metrics.

**Not a Redis ORM.** There are no struct tags, no HASH repositories, and no
replacement for valkey-go's command API. `Client()` remains first-class. For
common patterns (distributed lock, JSON helpers, singleflight cache-aside),
see the [`patterns`](#patterns) subpackage.

Requires the `data` stage to be registered.

## Wiring

```go
package main

import (
	"context"
	"log/slog"
	"os"

	cf "github.com/caerus-framework/caerus-framework"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
)

func main() {
	fw := cf.New()

	logs := cf_logs.New(cf_logs.WithWriter(os.Stdout))
	if err := fw.AddComponent(logs); err != nil { // "logs" is a required dependency
		slog.Error("register logs", "err", err)
		os.Exit(1)
	}

	valkey := cf_valkey.New(
		cf_valkey.WithAddress("127.0.0.1:6379"),
		cf_valkey.WithDB(0),
		cf_valkey.WithClientName("my-service"),
	)
	app := NewMyApp(valkey) // any component with GetDependencies() -> []string{cf_valkey.ComponentName}
	if err := fw.AddComponent(valkey); err != nil {
		slog.Error("register valkey", "err", err)
		os.Exit(1)
	}
	if err := fw.AddComponent(app); err != nil {
		slog.Error("register app", "err", err)
		os.Exit(1)
	}

	if err := fw.Run(context.Background()); err != nil {
		slog.Error("startup failed", "err", err)
		os.Exit(1)
	}
}
```

## Usage

After `fw.Run` (or in any component whose stage runs after `data`), get the
client and issue commands through the valkey-go builder API:

```go
client := cf.MustGet[*cf_valkey.CFValkey](fw).Client()

err := client.Do(ctx, client.B().Set().Key("k").Value("v").Build()).Error()
got, err := client.Do(ctx, client.B().Get().Key("k").Build()).ToString()
```

Commands are auto-pipelined by valkey-go for throughput. The client supports
RESP3, client-side caching (`DoCache`), pub/sub (`Receive`), blocking commands
and cluster/sentinel topologies.

### Key prefixing

Give a service a shared key namespace with `WithKeyPrefix` — every key it
reads or writes is scoped automatically, so several services can share one
Valkey without collisions. Use `Key(...)` to build keys through the same
prefixing:

```go
valkey := cf_valkey.New(
	cf_valkey.WithAddress("127.0.0.1:6379"),
	cf_valkey.WithKeyPrefix("auth"),
)

err := valkey.Client().Do(ctx, client.B().Set().Key(valkey.Key("session", "abc")).Value("1").Build()).Error()
// writes "auth:session:abc"
```

`Key("a", "b")` joins the parts with `:` and prepends the prefix
(`"auth:session:abc"`). An empty prefix collapses to a plain `:`-join, so code
written against `Key` runs unchanged with or without a prefix. `KeyPrefix()`
returns the configured prefix, and `Key()` is safe to call before `Init`
(it never touches the server).

## Options

| Option | Description |
| --- | --- |
| `WithConfig(ValkeyConfig)` | connection config loaded from the configuration component; non-zero fields override option-set defaults |
| `WithClientOption(valkey.ClientOption)` | full valkey-go client option; call before convenience setters you want overridden |
| `WithAddress(addr)` | single server address (default `127.0.0.1:6379`) |
| `WithAddresses(addrs...)` | multiple addresses (cluster/sentinel) |
| `WithUsername(u)` / `WithPassword(p)` | AUTH credentials |
| `WithDB(n)` | logical database selection |
| `WithClientName(name)` | `CLIENT SETNAME` on connections |
| `WithKeyPrefix(prefix)` | scope all keys (see above); trailing `:` trimmed |
| `WithPingTimeout(d)` | Init connectivity-ping timeout (default `5s`) |
| `WithName(name)` | custom component name for multiple instances (default `"valkey"`) |
| `WithLogger(*slog.Logger)` | explicit logger override; defaults to the framework `logs` component's logger (re-delivered on `logs` `Reconfigure`), falling back to `slog.Default()` |
| `WithTLS(caFile, certFile, keyFile)` | TLS from PEM file paths (Kubernetes-mounted secrets); CA for server verification, cert+key for mTLS |
| `WithDialTimeout(d)` | TCP dial timeout |
| `WithConnWriteTimeout(d)` | per-connection read/write timeout; bounds pipeline waits and triggers periodic PINGs |
| `WithConnLifetime(d)` | maximum connection lifetime; zero means no limit |

## Multiple instances (cookbook)

Use `WithName` to run multiple valkey clients in the same process (e.g., cache,
sessions, rate-limit). Each gets its own key prefix, health check, and metrics
labels:

```go
cache := cf_valkey.New(
    cf_valkey.WithName("cache"),
    cf_valkey.WithAddress("valkey:6379"),
    cf_valkey.WithDB(0),
    cf_valkey.WithKeyPrefix("app:cache"),
)
sessions := cf_valkey.New(
    cf_valkey.WithName("sessions"),
    cf_valkey.WithAddress("valkey:6379"),
    cf_valkey.WithDB(1),
    cf_valkey.WithKeyPrefix("app:sessions"),
)

fw.AddComponent(cache)
fw.AddComponent(sessions)

// Retrieve by name
cacheClient := cf.MustGetByName[*cf_valkey.CFValkey](fw, "cache").Client()
sessionsClient := cf.MustGetByName[*cf_valkey.CFValkey](fw, "sessions").Client()
```

When multiple instances exist, `cf.Get[*cf_valkey.CFValkey](fw)` returns `false`
to prevent ambiguous lookups. Always use `GetByName` for named instances. Each
instance's metrics carry a `component` label (e.g. `valkey_info{component="cache"}`).

## Configuration

Drive connection settings via `caerus-framework-configuration` (file → env →
URL). `ValkeyConfig` has json/yaml/`env` tags.

The module is **self-sufficient**: `WithConfigSource(name, path)` registers its
own `Source[ValkeyConfig]` with the configuration component (via
`cf.ConfigSourceRegistrar`, run by the framework during argv absorption). The
default `EnvPrefix` is the uppercase source name (`"valkey"` → `"VALKEY_"`).
`VALKEY_URL` is overlaid in `AfterLoad` inside the module. `main` only points
the instance at where config lives:

```go
valkey := cf_valkey.New(
	cf_valkey.WithConfigSource("valkey", "config.yaml"), // Init + OnConfigReload reconnect
)
```

For low-level control (custom `AfterLoad`, format, env prefix), register the
source manually instead:

```go
conf := cf_configuration.New()
_ = fw.AddComponent(conf)
_ = cf_configuration.AddSource(conf, cf_configuration.Source[cf_valkey.ValkeyConfig]{
	Name:      "valkey",
	Path:      "config.yaml", // optional if EnvPrefix set
	Format:    cf_configuration.FormatYAML,
	Owner:     cf_valkey.ComponentName,
	EnvPrefix: "VALKEY_",
	AfterLoad: func(c *cf_valkey.ValkeyConfig) error {
		if u := os.Getenv("VALKEY_URL"); u != "" {
			return cf_valkey.OverlayURL(c, u) // wins over file+env fields
		}
		return nil
	},
})

valkey := cf_valkey.New(
	cf_valkey.WithConfigSource("valkey", ""), // bind by name only
)
```

Helpers: `ParseURL` / `OverlayURL` for `redis://`, `valkey://`, or `host:port`.
`WithConfigSource` implements `ConfigReloader`: on file reload (or
`cfg.Reload`), builds a new client, pings, swaps, closes the old client; on
failure keeps the previous client. In Kubernetes prefer file-mounted secrets
for rotation; use env/URL for local and CI.

## Fail-fast behaviour

`Init` creates the client and pings the server. If the connection is refused or
the ping times out, `Init` returns an error and startup aborts before any
dependent component runs. `Client()` returns `nil` before `Init` or after
`Shutdown`.

## Observability

`CFValkey` implements `cf.HealthProvider`: `Health(ctx)` pings the server, so
the `observability` component's `/readyz` endpoint reflects real connectivity.
Before `Init` or after `Shutdown` (nil client) it reports unhealthy.

It also implements `cf_observability.MetricsProvider`: while connected it
contributes samples to `/metrics`:

| Sample | Type | Labels |
|---|---|---|
| `valkey_info` | gauge | `addresses`, `db`, `component` |
| `valkey_ping_failures_total` | counter | same |
| `valkey_reconnects_total` | counter | same |
| `valkey_lock_acquire_ok_total` | counter | same |
| `valkey_lock_acquire_busy_total` | counter | same |
| `valkey_lock_unlock_ok_total` | counter | same |
| `valkey_lock_unlock_mismatch_total` | counter | same |

The `valkey_lock_*` counters aggregate distributed-lock traffic from
`patterns.Mutex` across the component instance (per-lock breakdown is out of
scope to keep the lock helpers dependency-free and cardinality bounded).

Before `Init` or after `Shutdown` it reports nothing (lazy pickup). While
connected, the ping, reconnect, and lock counters are always emitted (zero
until first fire), so the series stay present on `/metrics`. Counter
samples use `cf_observability.MetricTypeCounter` and are scraped as Prometheus
counters (not gauges). The metrics contract lives in
[`caerus-framework-observability`](https://github.com/caerus-framework/caerus-framework-observability),
not core.

## Command hooks (tracing)

`CFValkey` exposes a generic command-hook seam so you can attach tracing,
slow-command logging, or command counters to every `Client().Do` / `DoMulti`
call — without importing an instrumentation library into this module. Register
hooks with `WithCommandHook`; they run in order around each command, each
calling `next` to continue the chain:

```go
type CommandHook interface {
    Do(ctx context.Context, cmd valkey.Completed,
        next func(context.Context, valkey.Completed) valkey.ValkeyResult) valkey.ValkeyResult
    DoMulti(ctx context.Context, cmds []valkey.Completed,
        next func(context.Context, []valkey.Completed) []valkey.ValkeyResult) []valkey.ValkeyResult
}
```

An OpenTelemetry example (the otel import lives in **your** app, not here):

```go
import (
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/attribute"
    "go.opentelemetry.io/otel/codes"
)

type spanHook struct{ tracer string }

func (h spanHook) Do(ctx context.Context, cmd valkey.Completed,
    next func(context.Context, valkey.Completed) valkey.ValkeyResult) valkey.ValkeyResult {
    ctx, span := otel.Tracer(h.tracer).Start(ctx, "valkey:"+cmd.Commands()[0])
    defer span.End()
    resp := next(ctx, cmd)
    if err := resp.Error(); err != nil {
        span.SetStatus(codes.Error, err.Error())
    }
    return resp
}
func (h spanHook) DoMulti(ctx context.Context, cmds []valkey.Completed,
    next func(context.Context, []valkey.Completed) []valkey.ValkeyResult) []valkey.ValkeyResult {
    return next(ctx, cmds)
}

v := cf_valkey.New(cf_valkey.WithAddress("valkey:6379"),
    cf_valkey.WithCommandHook(spanHook{tracer: "valkey"}))
```

`cmd.Commands()` returns the command words (e.g. `GET`, `key`). The hook sees
lock/JSON/GetOrLoad traffic from `patterns` too, since they all go through
`Client().Do`. `DoCache` / `DoStream` / `Dedicated` / `Nodes` bypass the hook
(advanced paths).

## Patterns

The `patterns` subpackage (`github.com/caerus-framework/caerus-framework-valkey/patterns`)
provides small, optional, prefix-aware helpers for common Valkey usage. It is
**not** a Redis ORM — `Client()` remains first-class. Apps that only need a
client never import it.

### Distributed lock (Mutex)

```go
import "github.com/caerus-framework/caerus-framework-valkey/patterns"

m := patterns.NewMutex(vk, "reconcile", 30*time.Second)
err := m.WithLock(ctx, func(ctx context.Context) error {
    // only one holder at a time (per Valkey instance)
    return doReconcile(ctx)
})
```

`TryLock` / `Unlock` are also available. TTL is mandatory; token-based unlock
(Lua) ensures you never delete another holder's lock. **Not Redlock** — see
godoc for failure modes. Lock traffic is counted and exposed on `/metrics`
(`valkey_lock_acquire_ok_total`, `valkey_lock_acquire_busy_total`,
`valkey_lock_unlock_ok_total`, `valkey_lock_unlock_mismatch_total`), so
contention and lost unlocks are observable without ad-hoc instrumentation.

### JSON helpers

```go
var user User
err := patterns.GetJSON(ctx, vk, &user, "user", id)
err = patterns.SetJSON(ctx, vk, user, 5*time.Minute, "user", id)
```

### Singleflight GetOrLoad

```go
import "golang.org/x/sync/singleflight"

g := &singleflight.Group{}
val, shared, err := patterns.GetOrLoad(ctx, vk, g, "price:"+sku, time.Minute,
    func(ctx context.Context) ([]byte, error) {
        return fetchPrice(ctx, sku)
    },
)
```

Coalesces concurrent loads **within this process**. Other pods still stampede;
compose with `Mutex` for cross-pod coalescing.

## TLS

Configure TLS from PEM file paths (suitable for Kubernetes-mounted secrets):

```go
vk := cf_valkey.New(
    cf_valkey.WithAddress("valkey:6380"),
    cf_valkey.WithTLS("/certs/ca.pem", "/certs/client.pem", "/certs/client-key.pem"),
)
```

Or via `ValkeyConfig` fields `tls_ca_file`, `tls_cert_file`, `tls_key_file`
(drivable by env: `TLS_CA_FILE`, `TLS_CERT_FILE`, `TLS_KEY_FILE`).

## Tests

Unit tests cover the component contract without a server. Integration tests are
gated on `VALKEY_ADDR`:

```
VALKEY_ADDR=127.0.0.1:6379 go test -race ./...
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).

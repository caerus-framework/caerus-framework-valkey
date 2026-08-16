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
see the [`patterns`](#patterns) subpackage. Rate-limit Lua (and the sticky-note
counter map) live on **`caerus-framework-valkey-state`**; HTTP 429 / middleware
live on **`caerus-framework-http-ratelimiter`**.

Registers in the `data` initialization stage.

## Wiring

Two wiring shapes. Prefer the **app-owned** shape.

### App-owned consumer (golden — demoapp pattern)

`main` declares valkey as chassis. The app holds `*CFValkey` and calls
`Client()` / `Key()` **per use** (never copy the client at Init).

```go
fw := cf.New(&cf.FrameworkOptions{
	Logs:          &cf.LogsSettings{Format: "json", Level: "info", ConfigSource: "logs"},
	Observability: &cf.ObservabilitySettings{Bind: ":9090", ConfigSource: "observability"},
	Components: []cf.CaerusComponent{
		cf_valkey.New(cf_valkey.WithConfigSource("valkey", "config/valkey.json")),
		app.New(),
	},
})
```

```go
type App struct {
	vk *cf_valkey.CFValkey
}

func (a *App) GetDependencies() []string {
	return []string{cf_valkey.ComponentName}
}

func (a *App) Init(ctx context.Context, fw *cf.CaerusFramework) error {
	vk, ok := cf.Get[*cf_valkey.CFValkey](fw)
	if !ok {
		return errors.New("app: valkey missing")
	}
	a.vk = vk
	return nil
}

func (a *App) set(ctx context.Context, k, v string) error {
	cl := a.vk.Client() // live client after reconnect / reload
	return cl.Do(ctx, cl.B().Set().Key(a.vk.Key(k)).Value(v).Build()).Error()
}
```

Wrong: `a.client = vk.Client()` once at Init (dead after reload).
Right: store `*CFValkey`; `Client()` per command.

### Simple `main`-level wiring

```go
fw := cf.New()
fw.AddComponent(cf_logs.New(cf_logs.WithWriter(os.Stdout)))
fw.AddComponent(cf_valkey.New(cf_valkey.WithAddress("127.0.0.1:6379")))
```

Then `cf.MustGet[*cf_valkey.CFValkey](fw)` in that binary. Still call
`Client()` per use, not a snapshot.

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
| `WithDegradedMode(bool)` | when true, Init may succeed without a live ping (default **off** / hard-fail) |
| `WithHealthWhenDegraded("not_ready"\|"ready")` | `/readyz` while degraded: default `not_ready`; `ready` is break-glass LB traffic |
| `WithName(name)` | custom component name for multiple instances (default `"valkey"`) |
| `WithLogger(*slog.Logger)` | explicit logger override; defaults to the framework `logs` component's logger (re-delivered on `logs` `Reconfigure`), falling back to `slog.Default()` |
| `WithTLS(caFile, certFile, keyFile)` | TLS from PEM file paths (Kubernetes-mounted secrets); CA for server verification, cert+key for mTLS |
| `WithTLSInsecureSkipVerify(true)` | skip cert verify (lab only; not implied by `rediss://`) |
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
`ParseURL` errors never include the raw URL (userinfo). `Password` is tagged
`secret:"redact"`; connect/reload logs use `password_set` only.
`WithConfigSource` implements `ConfigReloader`: on file reload (or
`cfg.Reload`), builds a new client, pings, swaps, closes the old client; on
failure keeps the previous client. In Kubernetes prefer file-mounted secrets
for rotation; use env/URL for local and CI.

## Fail-fast behaviour (default)

`Init` creates the client and pings the server. If the connection is refused or
the ping times out, `Init` returns an error and startup aborts before any
dependent component runs. `Client()` returns `nil` before `Init` or after
`Shutdown`.

## DegradedMode (optional break-glass)

**Not automatic.** Default remains hard Init. Set `degraded_mode: true` (or
`WithDegradedMode(true)`) when the process must finish Initialize even if
Valkey is unreachable (e.g. a rate limiter that can run on sticky notes with
`force_memory`).

```json
{
  "addr": "valkey:6379",
  "degraded_mode": true,
  "health_when_degraded": "not_ready"
}
```

| Setting | Meaning |
|---|---|
| `degraded_mode` | Init may succeed without a successful ping; logs/metrics scream (`degraded_unreachable`, `degraded_mode_uses_total`). **Default off.** |
| `health_when_degraded: "not_ready"` | Default — `Health` still fails → `/readyz` 503 while disconnected. |
| `health_when_degraded: "ready"` | Break-glass — `Health` returns nil while down so LB may send traffic. Prefer a **dedicated** Valkey instance when the same pod also has a hard session/DB dependency. |

DegradedMode answers “may Initialize finish?” — it does **not** mean the store
is healthy. Hot reload of the valkey source can reconnect later when the file
updates; env alone does not wake a running process.

## Observability

`CFValkey` implements `cf.HealthProvider`: `Health(ctx)` pings the server, so
the `observability` component's `/readyz` endpoint reflects real connectivity.
Before `Init` or after `Shutdown` (nil client) it reports unhealthy. After
DegradedMode without a live ping, behaviour follows `health_when_degraded`.

It also implements `cf_observability.MetricsProvider`: while connected it
contributes samples to `/metrics`:

| Sample | Type | Labels |
|---|---|---|
| `valkey_info` | gauge | `addresses`, `db`, `component` |
| `valkey_degraded_unreachable` | gauge | `degraded_mode`, `health_when_degraded`, … |
| `valkey_degraded_mode_uses_total` | counter | same |
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

Two different things:

| | What it means |
|---|---|
| **URL scheme** `rediss://` / `valkeys://` | This URL **wants TLS**. The client gets `TLSConfig` (MinVersion 1.2, system roots) even with **no** PEM files. `redis://` / `valkey://` do not enable TLS by themselves. |
| **PEM files** | Custom CA and/or mTLS. Files win for trust material. Re-read on reload / reconnect. |

Client cert and key are a **pair** (same rule as postgresql): both set, both
empty, or the overlay is rejected and last-good stays. Do not set only
`tls_cert_file` or only `tls_key_file` (env or JSON). CA (`tls_ca_file`)
rotates on its own.

Kubernetes layout (cert-manager / `kubernetes.io/tls`): mount `ca.crt`,
`tls.crt`, and `tls.key`; point the three settings at those paths. A Secret
update that replaces the files is picked up on the next reload.

`tls_insecure_skip_verify` is a **named switch** for broken lab certs. It is
**not** implied by `rediss://`.

```go
vk := cf_valkey.New(
    cf_valkey.WithAddress("valkey:6380"),
    cf_valkey.WithTLS("/certs/ca.pem", "/certs/client.pem", "/certs/client-key.pem"),
)
```

Or `tls: true` in JSON, or `VALKEY_URL=rediss://host:6379`.

With `degraded_mode`, a failed Init ping does not abort. A background loop
retries ping/rebuild (backoff + jitter) until the server is up or Shutdown.
`Health` stays honest unless `health_when_degraded` is `ready`.

## Tests

Unit tests cover the component contract without a server. Integration tests are
gated on `VALKEY_ADDR`:

```
VALKEY_ADDR=127.0.0.1:6379 go test -race ./...
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).

package cf_valkey

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	"github.com/valkey-io/valkey-go"
)

const (
	// ComponentName is the framework component name for the valkey component.
	// It is the identifier other components use in GetDependencies to require
	// valkey.
	ComponentName = "valkey"

	// ComponentStage is the stage data-layer components initialize in. It is
	// not a built-in bootstrap stage; AddComponent registers it automatically
	// the first time a component declares it.
	ComponentStage = cf.Stage("data")
)

// ValkeyConfig is the file/env-drivable connection configuration. Load it
// through the configuration component (caerus-framework-configuration) and
// pass it via WithConfig; both JSON and YAML tags are provided.
type ValkeyConfig struct {
	Addresses           []string `json:"addresses" yaml:"addresses" env:"ADDRESSES"`
	Username            string   `json:"username,omitempty" yaml:"username,omitempty" env:"USERNAME"`
	Password            string   `json:"password,omitempty" yaml:"password,omitempty" env:"PASSWORD"`
	DB                  int      `json:"db" yaml:"db" env:"DB"`
	ClientName          string   `json:"client_name,omitempty" yaml:"client_name,omitempty" env:"CLIENT_NAME"`
	KeyPrefix           string   `json:"key_prefix,omitempty" yaml:"key_prefix,omitempty" env:"KEY_PREFIX"`
	TLSCAFile           string   `json:"tls_ca_file,omitempty" yaml:"tls_ca_file,omitempty" env:"TLS_CA_FILE"`
	TLSCertFile         string   `json:"tls_cert_file,omitempty" yaml:"tls_cert_file,omitempty" env:"TLS_CERT_FILE"`
	TLSKeyFile          string   `json:"tls_key_file,omitempty" yaml:"tls_key_file,omitempty" env:"TLS_KEY_FILE"`
	DialTimeoutSec      float64  `json:"dial_timeout_sec,omitempty" yaml:"dial_timeout_sec,omitempty" env:"DIAL_TIMEOUT_SEC"`
	ConnWriteTimeoutSec float64  `json:"conn_write_timeout_sec,omitempty" yaml:"conn_write_timeout_sec,omitempty" env:"CONN_WRITE_TIMEOUT_SEC"`
	ConnLifetimeSec     float64  `json:"conn_lifetime_sec,omitempty" yaml:"conn_lifetime_sec,omitempty" env:"CONN_LIFETIME_SEC"`
}

// Option configures the valkey component at construction time.
type Option func(*options)

type options struct {
	clientOption valkey.ClientOption
	loaded       *ValkeyConfig // set by WithConfig; overrides option-set defaults
	configSource string        // named configuration source for live reload
	configPath   string        // source file path (module self-registration)
	srcEnvPrefix string        // source env overlay prefix (default: NAME_)
	srcFormat    cf_configuration.Format
	srcFormatSet bool
	keyPrefix    string
	logger       *slog.Logger
	loggerSet    bool // true when WithLogger was called explicitly
	pingTimeout  time.Duration
	name         string // custom component name; empty means use ComponentName
	tlsCAFile    string
	tlsCertFile  string
	tlsKeyFile   string
	hooks        []CommandHook
}

// SourceOption configures the self-registered configuration source created by
// WithConfigSource.
type SourceOption func(*sourceOptions)

type sourceOptions struct {
	envPrefix string
	format    cf_configuration.Format
	formatSet bool
}

// WithSourceEnvPrefix sets the environment overlay prefix for the source
// (default: the uppercase source name with "-" replaced by "_", plus "_").
// An empty prefix disables env overlay.
func WithSourceEnvPrefix(prefix string) SourceOption {
	return func(o *sourceOptions) { o.envPrefix = prefix }
}

// WithSourceFormat forces the file format instead of inferring it from the
// path extension (".yaml"/".yml" → YAML; anything else JSON).
func WithSourceFormat(f cf_configuration.Format) SourceOption {
	return func(o *sourceOptions) { o.format = f; o.formatSet = true }
}

// defaultSourceEnvPrefix derives an environment prefix from a source name.
func defaultSourceEnvPrefix(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_")) + "_"
}

// WithConfig sets a static connection configuration snapshot. Non-zero fields
// of cfg override the values set by the convenience options. Prefer
// WithConfigSource when using caerus-framework-configuration with hot-reload.
func WithConfig(cfg ValkeyConfig) Option {
	return func(o *options) { o.loaded = &cfg }
}

// WithConfigSource binds this component to a named configuration source and
// registers that source with the configuration component (via the framework's
// ConfigSourceRegistrar pass during argv absorption). The module owns the
// Source: the config type, default EnvPrefix, the VALKEY_URL AfterLoad overlay
// and its Owner (Name(), so named instances reload correctly). main only points
// the instance at where the config lives.
//
//	cf_valkey.New(cf_valkey.WithConfigSource("valkey", "config/valkey.json"))
//	cf_valkey.New(cf_valkey.WithConfigSource("valkey-cache", "/etc/app/valkey-cache.yaml",
//	    cf_valkey.WithSourceFormat(cf_configuration.FormatYAML)))
//
// A path of "" registers an env-only (fileless) source when the EnvPrefix is
// non-empty. The path CLI override stays --<source-name> (ParseFlags).
// Declares a dependency on "configuration".
func WithConfigSource(name, path string, opts ...SourceOption) Option {
	return func(o *options) {
		so := sourceOptions{envPrefix: defaultSourceEnvPrefix(name)}
		for _, opt := range opts {
			opt(&so)
		}
		o.configSource = name
		o.configPath = path
		o.srcEnvPrefix = so.envPrefix
		o.srcFormat = so.format
		o.srcFormatSet = so.formatSet
	}
}

// WithClientOption sets the full valkey-go client option. Convenience setters
// (WithAddress/WithAddresses, WithUsername, WithPassword, WithDB,
// WithClientName) override the matching fields, so call them after
// WithClientOption if you combine them.
func WithClientOption(opt valkey.ClientOption) Option {
	return func(o *options) { o.clientOption = opt }
}

// WithAddress sets the single server address (default "127.0.0.1:6379").
func WithAddress(addr string) Option {
	return func(o *options) { o.clientOption.InitAddress = []string{addr} }
}

// WithAddresses sets multiple server addresses (for cluster/sentinel setups).
func WithAddresses(addrs ...string) Option {
	return func(o *options) { o.clientOption.InitAddress = addrs }
}

// WithUsername sets the AUTH username.
func WithUsername(username string) Option {
	return func(o *options) { o.clientOption.Username = username }
}

// WithPassword sets the AUTH password.
func WithPassword(password string) Option {
	return func(o *options) { o.clientOption.Password = password }
}

// WithDB selects the logical database (0-15 on a standalone server).
func WithDB(db int) Option {
	return func(o *options) { o.clientOption.SelectDB = db }
}

// WithClientName sets CLIENT SETNAME on connections.
func WithClientName(name string) Option {
	return func(o *options) { o.clientOption.ClientName = name }
}

// WithKeyPrefix sets a namespace prefix applied by Key to every key this
// component's users build. Useful when several services or environments share
// one instance. The prefix is trimmed of a trailing ":"; an empty prefix keeps
// Key a plain ":"-join.
func WithKeyPrefix(prefix string) Option {
	return func(o *options) { o.keyPrefix = prefix }
}

// WithPingTimeout sets how long Init waits for the connectivity ping before
// failing (default 5s).
func WithPingTimeout(d time.Duration) Option {
	return func(o *options) { o.pingTimeout = d }
}

// WithLogger overrides the logger used for component diagnostics. By default
// the component logs through the framework logs component (declared in
// GetDependencies); WithLogger is an explicit override for tests and embedded
// use and wins over the framework logger. slog.Default() remains the fallback
// only when neither is available.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) { o.logger = logger; o.loggerSet = true }
}

// WithName sets a custom component name, allowing multiple valkey instances in
// the same process. The default name is "valkey" (ComponentName). Use this when
// you need multiple valkey clients (e.g., cache and sessions) in one binary.
// Retrieve named instances with GetByName[*CFValkey](fw, "cache").
func WithName(name string) Option {
	return func(o *options) { o.name = name }
}

// WithTLS configures TLS from PEM file paths. Suitable for Kubernetes-mounted
// secrets (External Secrets). All three files are optional; set at least
// TLSCAFile for server verification, or CertFile+KeyFile for mTLS.
func WithTLS(tlsCAFile, tlsCertFile, tlsKeyFile string) Option {
	return func(o *options) {
		o.tlsCAFile = tlsCAFile
		o.tlsCertFile = tlsCertFile
		o.tlsKeyFile = tlsKeyFile
	}
}

// WithDialTimeout sets the TCP dial timeout (default: valkey-go's default,
// typically 5s). Applied to the underlying net.Dialer.
func WithDialTimeout(d time.Duration) Option {
	return func(o *options) { o.clientOption.Dialer.Timeout = d }
}

// WithConnWriteTimeout sets the per-connection read/write timeout. It bounds
// pipeline response waits and triggers periodic PINGs for liveness.
func WithConnWriteTimeout(d time.Duration) Option {
	return func(o *options) { o.clientOption.ConnWriteTimeout = d }
}

// WithConnLifetime sets a maximum connection lifetime. Connections older than
// this are closed and replaced. Zero means no limit (valkey-go default).
func WithConnLifetime(d time.Duration) Option {
	return func(o *options) { o.clientOption.ConnLifetime = d }
}

// WithCommandHook registers command hooks on the component. Multiple calls
// append; hooks run in registration order, the first hook wrapping the
// outermost. Use it to attach tracing spans, slow-command logging, or command
// counters to every Client().Do / DoMulti call. The hook interface lives here
// so the valkey module needs no instrumentation dependency (e.g. OpenTelemetry);
// apps implement CommandHook against the instrumenter of their choice.
func WithCommandHook(hooks ...CommandHook) Option {
	return func(o *options) { o.hooks = append(o.hooks, hooks...) }
}

// CFValkey is the caerus-framework-valkey component. It wraps a valkey-go
// client, verifies connectivity at Init, and closes it at Shutdown.
type CFValkey struct {
	mu           sync.RWMutex
	baseOpts     valkey.ClientOption
	opts         valkey.ClientOption
	configSource string
	configPath   string
	srcEnvPrefix string
	srcFormat    cf_configuration.Format
	srcFormatSet bool
	pingTimeout  time.Duration
	basePrefix   string
	keyPrefix    string
	loggerSet    bool
	client       valkey.Client
	logger       *slog.Logger
	logsSub      *cf_logs.Subscription
	fw           *cf.CaerusFramework
	name         string // custom name; empty means use ComponentName
	tlsCAFile    string
	tlsCertFile  string
	tlsKeyFile   string
	pingFailures atomic.Uint64
	reconnects   atomic.Uint64
	lockMeter    *LockMeter
	hooks        []CommandHook
}

// New creates a valkey component. The client is created and pinged at Init,
// not here.
func New(opts ...Option) *CFValkey {
	o := options{
		logger:      slog.Default(),
		pingTimeout: 5 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if len(o.clientOption.InitAddress) == 0 {
		o.clientOption.InitAddress = []string{"127.0.0.1:6379"}
	}
	baseOpts := o.clientOption
	basePrefix := o.keyPrefix
	if o.loaded != nil {
		applyLoadedConfig(&o.clientOption, *o.loaded)
		if o.loaded.KeyPrefix != "" {
			o.keyPrefix = o.loaded.KeyPrefix
		}
	}
	return &CFValkey{
		baseOpts:     baseOpts,
		opts:         o.clientOption,
		configSource: o.configSource,
		configPath:   o.configPath,
		srcEnvPrefix: o.srcEnvPrefix,
		srcFormat:    o.srcFormat,
		srcFormatSet: o.srcFormatSet,
		logger:       o.logger,
		loggerSet:    o.loggerSet,
		pingTimeout:  o.pingTimeout,
		basePrefix:   basePrefix,
		keyPrefix:    o.keyPrefix,
		name:         o.name,
		tlsCAFile:    o.tlsCAFile,
		tlsCertFile:  o.tlsCertFile,
		tlsKeyFile:   o.tlsKeyFile,
		lockMeter:    &LockMeter{},
		hooks:        o.hooks,
	}
}

// applyLoadedConfig overlays non-zero fields of cfg onto the client option.
// It runs last, so a loaded config always wins over option-set defaults.
func applyLoadedConfig(opt *valkey.ClientOption, cfg ValkeyConfig) {
	if len(cfg.Addresses) > 0 {
		opt.InitAddress = cfg.Addresses
	}
	if cfg.Username != "" {
		opt.Username = cfg.Username
	}
	if cfg.Password != "" {
		opt.Password = cfg.Password
	}
	if cfg.DB != 0 {
		opt.SelectDB = cfg.DB
	}
	if cfg.ClientName != "" {
		opt.ClientName = cfg.ClientName
	}
	if cfg.DialTimeoutSec > 0 {
		opt.Dialer.Timeout = time.Duration(cfg.DialTimeoutSec * float64(time.Second))
	}
	if cfg.ConnWriteTimeoutSec > 0 {
		opt.ConnWriteTimeout = time.Duration(cfg.ConnWriteTimeoutSec * float64(time.Second))
	}
	if cfg.ConnLifetimeSec > 0 {
		opt.ConnLifetime = time.Duration(cfg.ConnLifetimeSec * float64(time.Second))
	}
}

// Name implements cf.CaerusComponent. Returns the custom name set via WithName,
// or the default ComponentName ("valkey") if no custom name was set.
func (c *CFValkey) Name() string {
	if c.name != "" {
		return c.name
	}
	return ComponentName
}

// GetInitOrderStage implements cf.CaerusComponent.
func (c *CFValkey) GetInitOrderStage() cf.Stage { return ComponentStage }

// GetDependencies implements cf.Dependencies. The component logs through the
// framework logs component, and depends on configuration when WithConfigSource
// is set.
func (c *CFValkey) GetDependencies() []string {
	deps := []string{cf_logs.ComponentName}
	if c.configSource != "" {
		deps = append(deps, cf_configuration.ComponentName)
	}
	return deps
}

// buildTLSConfig constructs a *tls.Config from the component's TLS file paths.
// Returns nil when no TLS files are configured. CA file enables server
// verification; cert+key files enable mTLS client authentication.
func buildTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	if caFile == "" && certFile == "" && keyFile == "" {
		return nil, nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("cf_valkey: read TLS CA file %s: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("cf_valkey: TLS CA file %s contains no valid certificates", caFile)
		}
		tlsCfg.RootCAs = pool
	}
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("cf_valkey: load TLS client keypair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

// Init implements cf.CaerusComponent. It creates the valkey-go client and
// verifies connectivity with a ping, so a broken connection fails startup
// (fail-fast) before any dependent component runs.
func (c *CFValkey) Init(ctx context.Context, fw *cf.CaerusFramework) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		return nil // already initialized
	}
	c.fw = fw
	if !c.loggerSet {
		if logs, ok := cf.Get[*cf_logs.Logs](fw); ok {
			c.logsSub = logs.OnReconfigureFor(c.Name(), func(l *slog.Logger) { c.logger = l })
		}
	}

	if c.configSource != "" {
		opts, prefix, err := c.clientOptsFromSource()
		if err != nil {
			return err
		}
		c.opts = opts
		c.keyPrefix = prefix
	}

	if err := c.applyTLS(&c.opts); err != nil {
		return err
	}

	client, err := valkey.NewClient(c.opts)
	if err != nil {
		return fmt.Errorf("cf_valkey: create client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, c.pingTimeout)
	defer cancel()
	if err := client.Do(pingCtx, client.B().Ping().Build()).Error(); err != nil {
		client.Close()
		c.pingFailures.Add(1)
		return fmt.Errorf("cf_valkey: ping %v failed: %w", c.opts.InitAddress, err)
	}

	c.client = client
	c.logger.Info("cf_valkey: connected",
		"addresses", c.opts.InitAddress,
		"db", c.opts.SelectDB,
		"client_name", c.opts.ClientName,
	)
	return nil
}

// applyTLS builds and attaches a TLS config to opts from the component's TLS
// file paths. No-op when no TLS files are configured.
func (c *CFValkey) applyTLS(opts *valkey.ClientOption) error {
	tlsCfg, err := buildTLSConfig(c.tlsCAFile, c.tlsCertFile, c.tlsKeyFile)
	if err != nil {
		return err
	}
	if tlsCfg != nil {
		opts.TLSConfig = tlsCfg
	}
	return nil
}

// OnConfigReload implements cf.ConfigReloader. It rebuilds the client from the
// bound configuration source. The fresh value is delivered as cfg but the
// client is rebuilt from the source so the translation stays in one place. On
// failure the previous client is kept.
func (c *CFValkey) OnConfigReload(source string, cfg any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if source != c.configSource || c.client == nil || c.fw == nil {
		return
	}
	opts, prefix, err := c.clientOptsFromSource()
	if err != nil {
		c.logger.Error("cf_valkey: config reload rejected", "err", err)
		return
	}
	if err := c.applyTLS(&opts); err != nil {
		c.logger.Error("cf_valkey: config reload TLS rejected", "err", err)
		return
	}
	newClient, err := valkey.NewClient(opts)
	if err != nil {
		c.logger.Error("cf_valkey: config reload create client failed; keeping previous", "err", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.pingTimeout)
	defer cancel()
	if err := newClient.Do(ctx, newClient.B().Ping().Build()).Error(); err != nil {
		newClient.Close()
		c.pingFailures.Add(1)
		c.logger.Error("cf_valkey: config reload ping failed; keeping previous", "err", err)
		return
	}
	old := c.client
	c.client = newClient
	c.opts = opts
	c.keyPrefix = prefix
	c.reconnects.Add(1)
	old.Close()
	c.logger.Info("cf_valkey: reconnected after config reload",
		"addresses", opts.InitAddress,
		"db", opts.SelectDB,
	)
}

func (c *CFValkey) clientOptsFromSource() (valkey.ClientOption, string, error) {
	conf, ok := cf.Get[*cf_configuration.Configuration](c.fw)
	if !ok {
		return valkey.ClientOption{}, "", errors.New("cf_valkey: configuration component not registered")
	}
	loaded, ok := cf_configuration.Get[ValkeyConfig](conf, c.configSource)
	if !ok {
		return valkey.ClientOption{}, "", fmt.Errorf("cf_valkey: configuration source %q not found", c.configSource)
	}
	opts := c.baseOpts
	applyLoadedConfig(&opts, *loaded)
	prefix := c.basePrefix
	if loaded.KeyPrefix != "" {
		prefix = loaded.KeyPrefix
	}
	if loaded.TLSCAFile != "" {
		c.tlsCAFile = loaded.TLSCAFile
	}
	if loaded.TLSCertFile != "" {
		c.tlsCertFile = loaded.TLSCertFile
	}
	if loaded.TLSKeyFile != "" {
		c.tlsKeyFile = loaded.TLSKeyFile
	}
	return opts, prefix, nil
}

// RegisterConfigSources implements cf.ConfigSourceRegistrar. The framework
// calls it during argv absorption; it registers this component's configuration
// source (name, path, env prefix, format, Owner and the VALKEY_URL AfterLoad
// overlay) with the configuration component. No-op when no source is bound.
func (c *CFValkey) RegisterConfigSources(conf any) error {
	cfg, ok := conf.(*cf_configuration.Configuration)
	if !ok {
		return fmt.Errorf("cf_valkey: RegisterConfigSources: expected configuration component, got %T", conf)
	}
	if c.configSource == "" {
		return nil
	}
	format := c.srcFormat
	if !c.srcFormatSet {
		if p := strings.ToLower(c.configPath); strings.HasSuffix(p, ".yaml") || strings.HasSuffix(p, ".yml") {
			format = cf_configuration.FormatYAML
		} else {
			format = cf_configuration.FormatJSON
		}
	}
	return cf_configuration.AddSource(cfg, cf_configuration.Source[ValkeyConfig]{
		Name:      c.configSource,
		Path:      c.configPath,
		Format:    format,
		Owner:     c.Name(),
		EnvPrefix: c.srcEnvPrefix,
		AfterLoad: func(vc *ValkeyConfig) error {
			if u := os.Getenv("VALKEY_URL"); u != "" {
				return OverlayURL(vc, u)
			}
			return nil
		},
	})
}

// Shutdown implements cf.CaerusComponent. It closes the valkey client; further
// use of Client() after shutdown returns the closed client.
func (c *CFValkey) Shutdown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.logsSub != nil {
		c.logsSub.Unsubscribe()
		c.logsSub = nil
	}
	if c.client == nil {
		return nil
	}
	client := c.client
	c.client = nil
	client.Close()
	return nil
}

// Client returns the valkey-go client. It is non-nil after a successful Init
// and nil before Init or after Shutdown. When command hooks are configured
// (WithCommandHook), the returned client routes Do and DoMulti through the
// hook chain; all other methods are delegated to the underlying client.
func (c *CFValkey) Client() valkey.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.hooks) == 0 || c.client == nil {
		return c.client
	}
	return &hookedClient{
		Client:  c.client,
		do:      buildDoChain(c.hooks, c.client),
		doMulti: buildDoMultiChain(c.hooks, c.client),
	}
}

// CommandHook intercepts commands sent through the component's client before
// they reach Valkey. Hooks are configured at construction with
// WithCommandHook and run in registration order around the real command: each
// hook calls next to continue the chain (and finally the network round-trip),
// or short-circuits by returning without calling next. Use it to attach spans,
// log slow commands, or count traffic without importing an instrumentation
// library into this module.
type CommandHook interface {
	// Do wraps a single command execution.
	Do(ctx context.Context, cmd valkey.Completed,
		next func(context.Context, valkey.Completed) valkey.ValkeyResult) valkey.ValkeyResult
	// DoMulti wraps a pipelined batch of commands.
	DoMulti(ctx context.Context, cmds []valkey.Completed,
		next func(context.Context, []valkey.Completed) []valkey.ValkeyResult) []valkey.ValkeyResult
}

// hookedClient wraps a valkey client so Do and DoMulti run through the
// component's command hooks. All other Client methods are delegated to the
// embedded client. It is returned by Client() only when hooks are configured.
type hookedClient struct {
	valkey.Client
	do      func(context.Context, valkey.Completed) valkey.ValkeyResult
	doMulti func(context.Context, []valkey.Completed) []valkey.ValkeyResult
}

// Do implements valkey.Client by running the command through the hook chain.
func (h *hookedClient) Do(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	return h.do(ctx, cmd)
}

// DoMulti implements valkey.Client by running the batch through the hook chain.
func (h *hookedClient) DoMulti(ctx context.Context, cmds ...valkey.Completed) []valkey.ValkeyResult {
	return h.doMulti(ctx, cmds)
}

var _ valkey.Client = (*hookedClient)(nil)

// buildDoChain composes the command hooks into a single closure over the raw
// client: hooks run in registration order, the last hook calls the network.
func buildDoChain(hooks []CommandHook, raw valkey.Client) func(context.Context, valkey.Completed) valkey.ValkeyResult {
	var chain func(context.Context, valkey.Completed) valkey.ValkeyResult
	chain = func(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
		return raw.Do(ctx, cmd)
	}
	for i := len(hooks) - 1; i >= 0; i-- {
		h := hooks[i]
		next := chain
		chain = func(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
			return h.Do(ctx, cmd, next)
		}
	}
	return chain
}

// buildDoMultiChain composes the command hooks into a single closure over the
// raw client for pipelined batches.
func buildDoMultiChain(hooks []CommandHook, raw valkey.Client) func(context.Context, []valkey.Completed) []valkey.ValkeyResult {
	var chain func(context.Context, []valkey.Completed) []valkey.ValkeyResult
	chain = func(ctx context.Context, cmds []valkey.Completed) []valkey.ValkeyResult {
		return raw.DoMulti(ctx, cmds...)
	}
	for i := len(hooks) - 1; i >= 0; i-- {
		h := hooks[i]
		next := chain
		chain = func(ctx context.Context, cmds []valkey.Completed) []valkey.ValkeyResult {
			return h.DoMulti(ctx, cmds, next)
		}
	}
	return chain
}

// KeyPrefix returns the configured namespace prefix (empty if none).
func (c *CFValkey) KeyPrefix() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.keyPrefix
}

// Health implements cf.HealthProvider. It pings the valkey server, so the
// observability component's readiness endpoint reflects real connectivity. A
// nil client (before Init or after Shutdown) is unhealthy.
func (c *CFValkey) Health(ctx context.Context) error {
	client := c.Client()
	if client == nil {
		return errors.New("cf_valkey: client is not initialized")
	}
	return client.Do(ctx, client.B().Ping().Build()).Error()
}

// Metrics implements cf_observability.MetricsProvider. It reports the connected
// valkey client's state; before Init or after Shutdown it returns nil, so the
// observability component skips it (lazy pickup).
func (c *CFValkey) Metrics() []cf_observability.Metric {
	if c.Client() == nil {
		return nil
	}
	labels := map[string]string{
		"addresses": strings.Join(c.opts.InitAddress, ","),
		"db":        strconv.Itoa(c.opts.SelectDB),
		"component": c.Name(),
	}
	metrics := []cf_observability.Metric{{
		Name:   "valkey_info",
		Help:   "Valkey client state.",
		Value:  1,
		Labels: copyLabels(labels),
	}}
	metrics = append(metrics,
		cf_observability.Metric{
			Name:   "valkey_ping_failures_total",
			Help:   "Total number of failed connectivity pings.",
			Value:  float64(c.pingFailures.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		cf_observability.Metric{
			Name:   "valkey_reconnects_total",
			Help:   "Total number of successful reconnects after config reload.",
			Value:  float64(c.reconnects.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
	)
	metrics = append(metrics, c.lockMeter.Metrics(labels)...)
	return metrics
}

// LockMeter returns the component's shared lock-traffic meter. Distributed
// lock helpers in the patterns subpackage feed it via this accessor; the
// totals ride the component's Metrics() output (aggregated per component
// instance, disambiguated by the component label).
func (c *CFValkey) LockMeter() *LockMeter { return c.lockMeter }

// LockMeter aggregates distributed-lock traffic counters for one valkey
// component. Mutexes created against the component increment it through
// LockMeter(); CFValkey.Metrics() then exposes the totals on /metrics as
// Prometheus counters. Counters are cumulative and only increase for the
// process lifetime and are emitted (zero until first increment) while the
// component is connected.
type LockMeter struct {
	acquireOK      atomic.Uint64
	acquireBusy    atomic.Uint64
	unlockOK       atomic.Uint64
	unlockMismatch atomic.Uint64
}

// IncAcquireOK records a successful lock acquisition.
func (m *LockMeter) IncAcquireOK() { m.acquireOK.Add(1) }

// IncAcquireBusy records an acquisition rejected because the lock was held.
func (m *LockMeter) IncAcquireBusy() { m.acquireBusy.Add(1) }

// IncUnlockOK records a release that actually deleted the lock key.
func (m *LockMeter) IncUnlockOK() { m.unlockOK.Add(1) }

// IncUnlockMismatch records a release where the caller no longer owned the
// lock (expired or stolen).
func (m *LockMeter) IncUnlockMismatch() { m.unlockMismatch.Add(1) }

// Metrics renders the meter's four counters, each carrying a copy of the
// caller's labels so the lock series share the component's identity. Counters
// are emitted while the component is connected (zero until first fired), so
// the series are always present on /metrics.
func (m *LockMeter) Metrics(labels map[string]string) []cf_observability.Metric {
	return []cf_observability.Metric{
		{
			Name:   "valkey_lock_acquire_ok_total",
			Help:   "Total number of successful distributed lock acquisitions.",
			Value:  float64(m.acquireOK.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_lock_acquire_busy_total",
			Help:   "Total number of distributed lock acquisitions rejected because the lock was held.",
			Value:  float64(m.acquireBusy.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_lock_unlock_ok_total",
			Help:   "Total number of successful distributed lock releases.",
			Value:  float64(m.unlockOK.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
		{
			Name:   "valkey_lock_unlock_mismatch_total",
			Help:   "Total number of distributed lock releases where the caller no longer owned the lock (expired or stolen).",
			Value:  float64(m.unlockMismatch.Load()),
			Labels: copyLabels(labels),
			Type:   cf_observability.MetricTypeCounter,
		},
	}
}

// copyLabels returns a shallow copy of a label map so callers cannot mutate
// the component's internal state.
func copyLabels(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Key builds a namespaced key by joining the configured prefix and parts with
// ":". The prefix's trailing ":" is normalized, so WithKeyPrefix("prod:") and
// WithKeyPrefix("prod") both produce the same keys:
//
//	v := cf_valkey.New(cf_valkey.WithKeyPrefix("prod:"))
//	v.Key("session", "abc")        // "prod:session:abc"
//	v.Key("ratelimit", c.RealIP()) // "prod:ratelimit:192.0.2.1"
//
// With an empty prefix, Key is a plain ":"-join of the parts.
func (c *CFValkey) Key(parts ...string) string {
	prefix := strings.TrimSuffix(c.KeyPrefix(), ":")
	if prefix == "" {
		return strings.Join(parts, ":")
	}
	return prefix + ":" + strings.Join(parts, ":")
}

var _ cf.CaerusComponent = (*CFValkey)(nil)
var _ cf.Dependencies = (*CFValkey)(nil)
var _ cf.HealthProvider = (*CFValkey)(nil)
var _ cf_observability.MetricsProvider = (*CFValkey)(nil)
var _ cf.ConfigReloader = (*CFValkey)(nil)
var _ cf.ConfigSourceRegistrar = (*CFValkey)(nil)

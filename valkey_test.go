package cf_valkey

import (
	"context"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	"github.com/valkey-io/valkey-go"
)

func newFramework(t *testing.T) *cf.CaerusFramework {
	t.Helper()
	return cf.New()
}

func TestComponentContract(t *testing.T) {
	v := New()
	if v.Name() != ComponentName {
		t.Fatalf("Name() = %q, want %q", v.Name(), ComponentName)
	}
	if v.GetInitOrderStage() != ComponentStage {
		t.Fatalf("GetInitOrderStage() = %q, want %q", v.GetInitOrderStage(), ComponentStage)
	}
	var _ cf.CaerusComponent = v

	if c := v.Client(); c != nil {
		t.Fatal("Client() should be nil before Init")
	}
	if err := v.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown before Init: %v", err)
	}
}

func TestHealthBeforeInit(t *testing.T) {
	v := New()
	if err := v.Health(context.Background()); err == nil {
		t.Fatal("Health before Init should fail")
	}
	if ms := v.Metrics(); ms != nil {
		t.Fatalf("Metrics before Init = %+v, want nil", ms)
	}
	var _ cf.HealthProvider = v
	var _ cf_observability.MetricsProvider = v
}

func TestNewDefaults(t *testing.T) {
	v := New()
	if len(v.opts.InitAddress) != 1 || v.opts.InitAddress[0] != "127.0.0.1:6379" {
		t.Fatalf("default InitAddress = %v, want [127.0.0.1:6379]", v.opts.InitAddress)
	}
	if v.opts.SelectDB != 0 {
		t.Fatalf("default SelectDB = %d, want 0", v.opts.SelectDB)
	}
}

func TestNewOptions(t *testing.T) {
	v := New(
		WithAddress("valkey:6379"),
		WithUsername("u"),
		WithPassword("p"),
		WithDB(3),
		WithClientName("svc"),
		WithClientOption(valkey.ClientOption{SelectDB: 9}),
		WithAddresses("a:6379", "b:6379"),
	)
	want := []string{"a:6379", "b:6379"}
	if len(v.opts.InitAddress) != len(want) {
		t.Fatalf("InitAddress = %v, want %v", v.opts.InitAddress, want)
	}
	for i := range want {
		if v.opts.InitAddress[i] != want[i] {
			t.Fatalf("InitAddress = %v, want %v", v.opts.InitAddress, want)
		}
	}
	if v.opts.SelectDB != 9 {
		t.Fatalf("SelectDB = %d, want 9 (WithClientOption wins over WithDB)", v.opts.SelectDB)
	}
}

func TestWithName(t *testing.T) {
	// Default name
	v1 := New()
	if v1.Name() != ComponentName {
		t.Fatalf("default Name() = %q, want %q", v1.Name(), ComponentName)
	}

	// Custom name
	v2 := New(WithName("cache"))
	if v2.Name() != "cache" {
		t.Fatalf("custom Name() = %q, want cache", v2.Name())
	}

	// Multiple instances with different names
	v3 := New(WithName("sessions"))
	if v3.Name() != "sessions" {
		t.Fatalf("custom Name() = %q, want sessions", v3.Name())
	}
}

func TestKeyPrefix(t *testing.T) {
	v := New(WithKeyPrefix("prod:"))
	if v.KeyPrefix() != "prod:" {
		t.Fatalf("KeyPrefix() = %q, want prod:", v.KeyPrefix())
	}
	if got := v.Key("session", "abc"); got != "prod:session:abc" {
		t.Fatalf("Key(session, abc) = %q, want prod:session:abc", got)
	}
	if got := v.Key("ratelimit", "1.2.3.4"); got != "prod:ratelimit:1.2.3.4" {
		t.Fatalf("Key(ratelimit, ip) = %q, want prod:ratelimit:1.2.3.4", got)
	}

	// trailing ":" is normalized: both spellings produce identical keys
	v2 := New(WithKeyPrefix("prod"))
	if got := v2.Key("session", "abc"); got != v.Key("session", "abc") {
		t.Fatalf("Key without trailing colon = %q, want %q", got, v.Key("session", "abc"))
	}

	// no prefix -> plain join
	v3 := New()
	if got := v3.Key("session", "abc"); got != "session:abc" {
		t.Fatalf("Key without prefix = %q, want session:abc", got)
	}
	if v3.KeyPrefix() != "" {
		t.Fatalf("KeyPrefix() = %q, want empty", v3.KeyPrefix())
	}
}

func TestWithConfigOverridesOptions(t *testing.T) {
	v := New(
		WithAddress("options:6379"),
		WithUsername("from-options"),
		WithDB(3),
		WithKeyPrefix("from-options:"),
		WithConfig(ValkeyConfig{
			Addresses: []string{"config:6379"},
			DB:        5,
			KeyPrefix: "from-config:",
		}),
	)
	if got := v.opts.InitAddress; len(got) != 1 || got[0] != "config:6379" {
		t.Fatalf("InitAddress = %v, want [config:6379] (config wins)", got)
	}
	if v.opts.SelectDB != 5 {
		t.Fatalf("SelectDB = %d, want 5 (config wins)", v.opts.SelectDB)
	}
	if v.KeyPrefix() != "from-config:" {
		t.Fatalf("KeyPrefix() = %q, want from-config: (config wins)", v.KeyPrefix())
	}
	// zero fields in config keep the option-set defaults
	if v.opts.Username != "from-options" {
		t.Fatalf("Username = %q, want from-options (empty config field keeps default)", v.opts.Username)
	}
}

func TestInitRejectsUnknownServer(t *testing.T) {
	v := New(
		WithAddress("127.0.0.1:1"),
		WithPingTimeout(500*time.Millisecond),
	)
	fw := newFramework(t)
	err := v.Init(context.Background(), fw)
	if err == nil {
		t.Fatal("Init against a closed port should fail")
	}
	if !strings.Contains(err.Error(), "ping") && !strings.Contains(err.Error(), "create client") {
		t.Fatalf("error should mention ping or create client, got: %v", err)
	}
	if c := v.Client(); c != nil {
		t.Fatal("Client() should be nil after failed Init")
	}
}

func TestInitUsesFrameworkLogger(t *testing.T) {
	logs := cf_logs.New(cf_logs.WithWriter(io.Discard))
	fw := cf.New()
	if err := fw.AddComponent(logs); err != nil {
		t.Fatalf("AddComponent(logs): %v", err)
	}

	v := New(WithAddress("127.0.0.1:1"), WithPingTimeout(200*time.Millisecond))
	_ = v.Init(context.Background(), fw)
	if v.logger == nil || v.logsSub == nil {
		t.Fatal("Init should subscribe to the framework logs component")
	}
	before := v.logger
	if before == logs.Logger() {
		t.Fatal("component logger must be OnReconfigureFor-scoped, not the process-global Logger()")
	}

	logs.Reconfigure(cf_logs.WithWriter(io.Discard))
	if v.logger == before {
		t.Fatal("component should receive the rebuilt logger on Reconfigure")
	}
	if v.logger == logs.Logger() {
		t.Fatal("rebuilt logger must remain OnReconfigureFor-scoped")
	}

	// an explicit WithLogger wins over the framework logger
	custom := slog.New(slog.NewTextHandler(io.Discard, nil))
	v2 := New(WithAddress("127.0.0.1:1"), WithPingTimeout(200*time.Millisecond), WithLogger(custom))
	_ = v2.Init(context.Background(), fw)
	if v2.logger != custom {
		t.Fatal("explicit WithLogger should win over the framework logger")
	}
}

func TestConcurrentClientAccess(t *testing.T) {
	v := New(WithAddress("127.0.0.1:1"))
	fw := newFramework(t)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = v.Init(context.Background(), fw) }()
	go func() { defer wg.Done(); _ = v.Init(context.Background(), fw) }()
	wg.Wait()

	// whichever Init won, the client must be non-nil only on success;
	// concurrent Shutdown/Client must not race
	var wg2 sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg2.Add(2)
		go func() { defer wg2.Done(); _ = v.Shutdown(context.Background()) }()
		go func() { defer wg2.Done(); _ = v.Client() }()
	}
	wg2.Wait()
}

func TestBuildTLSConfigNoFiles(t *testing.T) {
	cfg, err := buildTLSConfig("", "", "")
	if err != nil {
		t.Fatalf("buildTLSConfig with no files: %v", err)
	}
	if cfg != nil {
		t.Fatal("buildTLSConfig with no files should return nil")
	}
}

func TestBuildTLSConfigInvalidCA(t *testing.T) {
	_, err := buildTLSConfig("/nonexistent/ca.pem", "", "")
	if err == nil {
		t.Fatal("buildTLSConfig with nonexistent CA file should fail")
	}
}

func TestBuildTLSConfigCertWithoutKey(t *testing.T) {
	dir := t.TempDir()
	ca := writeTestPEM(t, dir, "ca.pem", testCACert)
	cert := writeTestPEM(t, dir, "cert.pem", testClientCert)
	_, err := buildTLSConfig(ca, cert, "")
	if err == nil {
		t.Fatal("buildTLSConfig with cert but no key should fail")
	}
}

func TestWithTLSOption(t *testing.T) {
	v := New(WithTLS("/path/ca.pem", "/path/cert.pem", "/path/key.pem"))
	if v.tlsCAFile != "/path/ca.pem" {
		t.Fatalf("tlsCAFile = %q, want /path/ca.pem", v.tlsCAFile)
	}
	if v.tlsCertFile != "/path/cert.pem" {
		t.Fatalf("tlsCertFile = %q, want /path/cert.pem", v.tlsCertFile)
	}
	if v.tlsKeyFile != "/path/key.pem" {
		t.Fatalf("tlsKeyFile = %q, want /path/key.pem", v.tlsKeyFile)
	}
}

func TestWithDialTimeoutOption(t *testing.T) {
	v := New(WithDialTimeout(3 * time.Second))
	if v.opts.Dialer.Timeout != 3*time.Second {
		t.Fatalf("Dialer.Timeout = %v, want 3s", v.opts.Dialer.Timeout)
	}
}

func TestWithConnWriteTimeoutOption(t *testing.T) {
	v := New(WithConnWriteTimeout(10 * time.Second))
	if v.opts.ConnWriteTimeout != 10*time.Second {
		t.Fatalf("ConnWriteTimeout = %v, want 10s", v.opts.ConnWriteTimeout)
	}
}

func TestWithConnLifetimeOption(t *testing.T) {
	v := New(WithConnLifetime(5 * time.Minute))
	if v.opts.ConnLifetime != 5*time.Minute {
		t.Fatalf("ConnLifetime = %v, want 5m", v.opts.ConnLifetime)
	}
}

func TestMetricsBeforeInitNil(t *testing.T) {
	v := New()
	if ms := v.Metrics(); ms != nil {
		t.Fatalf("Metrics before Init = %+v, want nil", ms)
	}
}

func TestMetricsCountersStartAtZero(t *testing.T) {
	v := New(WithName("cache"))
	if v.pingFailures.Load() != 0 {
		t.Fatalf("pingFailures = %d, want 0", v.pingFailures.Load())
	}
	if v.reconnects.Load() != 0 {
		t.Fatalf("reconnects = %d, want 0", v.reconnects.Load())
	}
}

func TestMetricsNameLabel(t *testing.T) {
	v := New(WithName("sessions"))
	if v.name != "sessions" {
		t.Fatalf("name = %q, want sessions", v.name)
	}
	v2 := New()
	if v2.name != "" {
		t.Fatalf("default name = %q, want empty", v2.name)
	}
}

func TestLockMeterEmpty(t *testing.T) {
	m := &LockMeter{}
	if ms := m.Metrics(nil); len(ms) != 0 {
		t.Fatalf("empty meter metrics = %+v, want none", ms)
	}
}

func TestLockMeterMetrics(t *testing.T) {
	m := &LockMeter{}
	m.IncAcquireOK()
	m.IncAcquireOK()
	m.IncAcquireBusy()
	m.IncUnlockOK()
	m.IncUnlockMismatch()

	labels := map[string]string{"name": "cache", "addresses": "127.0.0.1:6379"}
	ms := m.Metrics(labels)
	if len(ms) != 4 {
		t.Fatalf("metrics = %+v, want 4 samples", ms)
	}
	got := make(map[string]float64, len(ms))
	for _, s := range ms {
		if s.Type != cf_observability.MetricTypeCounter {
			t.Fatalf("%s Type = %v, want MetricTypeCounter", s.Name, s.Type)
		}
		got[s.Name] = s.Value
	}
	if got["lock_acquire_ok_total"] != 2 {
		t.Fatalf("lock_acquire_ok_total = %v, want 2", got["lock_acquire_ok_total"])
	}
	if got["lock_acquire_busy_total"] != 1 {
		t.Fatalf("lock_acquire_busy_total = %v, want 1", got["lock_acquire_busy_total"])
	}
	if got["lock_unlock_ok_total"] != 1 {
		t.Fatalf("lock_unlock_ok_total = %v, want 1", got["lock_unlock_ok_total"])
	}
	if got["lock_unlock_mismatch_total"] != 1 {
		t.Fatalf("lock_unlock_mismatch_total = %v, want 1", got["lock_unlock_mismatch_total"])
	}

	labels["mutated"] = "x"
	for _, s := range ms {
		if _, ok := s.Labels["mutated"]; ok {
			t.Fatalf("%s labels must be a copy, got %v", s.Name, s.Labels)
		}
	}
}

func TestLockMeterAccessor(t *testing.T) {
	v := New()
	if v.LockMeter() == nil {
		t.Fatal("LockMeter() should be non-nil after New")
	}
}

// fakeClient is a minimal valkey.Client that records whether Do / DoMulti were
// reached. It embeds the interface so tests only override what they use.
type fakeClient struct {
	valkey.Client
	doCalled      bool
	doMultiCalled bool
}

func (f *fakeClient) Do(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	f.doCalled = true
	return valkey.ValkeyResult{}
}

func (f *fakeClient) DoMulti(ctx context.Context, cmds ...valkey.Completed) []valkey.ValkeyResult {
	f.doMultiCalled = true
	return make([]valkey.ValkeyResult, len(cmds))
}

// recordingHook records when it is invoked (in registration order) and
// passes through to the next stage in the chain.
type recordingHook struct {
	name  string
	order *[]string
}

func (h *recordingHook) Do(ctx context.Context, cmd valkey.Completed,
	next func(context.Context, valkey.Completed) valkey.ValkeyResult) valkey.ValkeyResult {
	*h.order = append(*h.order, h.name)
	return next(ctx, cmd)
}

func (h *recordingHook) DoMulti(ctx context.Context, cmds []valkey.Completed,
	next func(context.Context, []valkey.Completed) []valkey.ValkeyResult) []valkey.ValkeyResult {
	*h.order = append(*h.order, h.name)
	return next(ctx, cmds)
}

// shortCircuitHook returns without calling next, so the rest of the chain is
// never reached.
type shortCircuitHook struct {
	called bool
}

func (h *shortCircuitHook) Do(ctx context.Context, cmd valkey.Completed,
	next func(context.Context, valkey.Completed) valkey.ValkeyResult) valkey.ValkeyResult {
	h.called = true
	return valkey.ValkeyResult{}
}

func (h *shortCircuitHook) DoMulti(ctx context.Context, cmds []valkey.Completed,
	next func(context.Context, []valkey.Completed) []valkey.ValkeyResult) []valkey.ValkeyResult {
	h.called = true
	return make([]valkey.ValkeyResult, len(cmds))
}

var _ CommandHook = (*recordingHook)(nil)
var _ CommandHook = (*shortCircuitHook)(nil)

func TestWithCommandHookAppends(t *testing.T) {
	h1 := &recordingHook{}
	h2 := &recordingHook{}
	v := New(WithCommandHook(h1), WithCommandHook(h2))
	if len(v.hooks) != 2 {
		t.Fatalf("hooks = %d, want 2", len(v.hooks))
	}
	if v.hooks[0] != CommandHook(h1) || v.hooks[1] != CommandHook(h2) {
		t.Fatalf("hooks order = %v, want [h1 h2]", v.hooks)
	}
}

func TestClientWithHooksBeforeInitNil(t *testing.T) {
	v := New(WithCommandHook(&recordingHook{}))
	if c := v.Client(); c != nil {
		t.Fatal("Client() should be nil before Init even with hooks")
	}
}

func TestBuildDoChainOrder(t *testing.T) {
	var order []string
	raw := &fakeClient{}
	chain := buildDoChain([]CommandHook{
		&recordingHook{name: "first", order: &order},
		&recordingHook{name: "second", order: &order},
	}, raw)
	_ = chain(context.Background(), valkey.Completed{})
	if want := []string{"first", "second"}; !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	if !raw.doCalled {
		t.Fatal("raw client Do should be reached at the end of the chain")
	}
}

func TestBuildDoChainShortCircuit(t *testing.T) {
	var order []string
	raw := &fakeClient{}
	h := &shortCircuitHook{}
	chain := buildDoChain([]CommandHook{
		h,
		&recordingHook{name: "never", order: &order},
	}, raw)
	_ = chain(context.Background(), valkey.Completed{})
	if !h.called {
		t.Fatal("short-circuit hook should run")
	}
	if len(order) != 0 {
		t.Fatalf("later hooks should not run after short-circuit, got %v", order)
	}
	if raw.doCalled {
		t.Fatal("raw client should not be reached after short-circuit")
	}
}

func TestBuildDoMultiChainOrder(t *testing.T) {
	var order []string
	raw := &fakeClient{}
	chain := buildDoMultiChain([]CommandHook{
		&recordingHook{name: "first", order: &order},
		&recordingHook{name: "second", order: &order},
	}, raw)
	_ = chain(context.Background(), make([]valkey.Completed, 2))
	if want := []string{"first", "second"}; !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	if !raw.doMultiCalled {
		t.Fatal("raw client DoMulti should be reached at the end of the chain")
	}
}

func TestBuildDoMultiChainShortCircuit(t *testing.T) {
	var order []string
	raw := &fakeClient{}
	h := &shortCircuitHook{}
	chain := buildDoMultiChain([]CommandHook{
		h,
		&recordingHook{name: "never", order: &order},
	}, raw)
	_ = chain(context.Background(), make([]valkey.Completed, 2))
	if !h.called {
		t.Fatal("short-circuit hook should run")
	}
	if len(order) != 0 {
		t.Fatalf("later hooks should not run after short-circuit, got %v", order)
	}
	if raw.doMultiCalled {
		t.Fatal("raw client should not be reached after short-circuit")
	}
}

func writeTestPEM(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := dir + "/" + name
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

const testCACert = `-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABLU3
jSeyBQaPmPptEiKqHnqJLqZOSnBnU2oaV5rXhXhIeKqJnqZOSnBnU2oaV5rXhXhI
eKqJnqZOSnBnU2oaV5rXhXijYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAK
BggrBgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9z
dDo1NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA1ygS9Alo
pDA1+Q3hMoL1MN+DoXQFQqJnW0GE7xGj2QcCIDz6g5OwJq8iDjJ3+u9oHa+Gj3qR
4yO2F4a9b0u1I0aF
-----END CERTIFICATE-----`

const testClientCert = `-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABLU3
jSeyBQaPmPptEiKqHnqJLqZOSnBnU2oaV5rXhXhIeKqJnqZOSnBnU2oaV5rXhXhI
eKqJnqZOSnBnU2oaV5rXhXijYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAK
BggrBgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9z
dDo1NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA1ygS9Alo
pDA1+Q3hMoL1MN+DoXQFQqJnW0GE7xGj2QcCIDz6g5OwJq8iDjJ3+u9oHa+Gj3qR
4yO2F4a9b0u1I0aF
-----END CERTIFICATE-----`

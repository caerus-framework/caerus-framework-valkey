package cf_valkey

import (
	"context"
	"io"
	"os"
	"slices"
	"testing"
	"time"

	cf "github.com/caerus-framework/caerus-framework"
	cf_logs "github.com/caerus-framework/caerus-framework-logs"
)

// TestIntegration is gated on the VALKEY_ADDR environment variable so the
// regular test run has no external dependency. Point it at a live server:
//
//	VALKEY_ADDR=127.0.0.1:6379 go test ./...
func TestIntegration(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	v := New(
		WithAddress(addr),
		WithClientName("caerus-valkey-test"),
		WithPingTimeout(3*time.Second),
	)
	fw := cf.New()

	if err := v.Init(context.Background(), fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = v.Shutdown(context.Background()) })

	if v.Client() == nil {
		t.Fatal("Client() is nil after Init")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client := v.Client()
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	key := "caerus-valkey-integration"
	_ = client.Do(ctx, client.B().Del().Key(key).Build()).Error()
	if err := client.Do(ctx, client.B().Set().Key(key).Value("hello").Build()).Error(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	got, err := client.Do(ctx, client.B().Get().Key(key).Build()).ToString()
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "hello" {
		t.Fatalf("Get = %q, want %q", got, "hello")
	}
}

// TestIntegrationCommandHook verifies a configured command hook actually wraps
// a real round-trip: it sees the command, the result still succeeds, and the
// chain reaches the network.
func TestIntegrationCommandHook(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	var order []string
	v := New(
		WithAddress(addr),
		WithPingTimeout(3*time.Second),
		WithCommandHook(&recordingHook{name: "hook", order: &order}),
	)
	if err := v.Init(context.Background(), nil); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = v.Shutdown(context.Background()) })

	client := v.Client()
	resp := client.Do(context.Background(), client.B().Ping().Build())
	if err := resp.Error(); err != nil {
		t.Fatalf("Ping through hook: %v", err)
	}
	s, err := resp.ToString()
	if err != nil || s != "PONG" {
		t.Fatalf("Ping = %q err=%v, want PONG", s, err)
	}
	if want := []string{"hook"}; !slices.Equal(order, want) {
		t.Fatalf("hook order = %v, want %v", order, want)
	}
}

// TestIntegrationHealthReflectsConnectivity verifies Health reports nil while
// connected and errors after Shutdown.
func TestIntegrationHealthReflectsConnectivity(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	v := New(WithAddress(addr), WithPingTimeout(3*time.Second))
	fw := cf.New()

	if err := v.Init(context.Background(), fw); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := v.Health(context.Background()); err != nil {
		t.Fatalf("Health while connected = %v, want nil", err)
	}
	if ms := v.Metrics(); len(ms) < 1 || ms[0].Name != "valkey_info" {
		t.Fatalf("Metrics while connected = %+v, want at least one valkey_info sample", ms)
	}
	if err := v.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := v.Health(context.Background()); err == nil {
		t.Fatal("Health after Shutdown should fail")
	}
	if ms := v.Metrics(); ms != nil {
		t.Fatalf("Metrics after Shutdown = %+v, want nil", ms)
	}
}

// TestIntegrationMultipleNamedInstances demonstrates multiple valkey instances
// in the same process using WithName and GetByName.
func TestIntegrationMultipleNamedInstances(t *testing.T) {
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}

	cache := New(WithName("cache"), WithAddress(addr), WithPingTimeout(3*time.Second))
	sessions := New(WithName("sessions"), WithAddress(addr), WithPingTimeout(3*time.Second))

	fw := cf.New()
	if err := fw.AddComponent(cf_logs.New(cf_logs.WithWriter(io.Discard))); err != nil {
		t.Fatalf("AddComponent(logs): %v", err)
	}
	if err := fw.AddComponent(cache); err != nil {
		t.Fatalf("AddComponent(cache): %v", err)
	}
	if err := fw.AddComponent(sessions); err != nil {
		t.Fatalf("AddComponent(sessions): %v", err)
	}

	if err := fw.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = fw.Shutdown(context.Background()) })

	// Get by name retrieves the correct instance
	cacheGot, ok := cf.GetByName[*CFValkey](fw, "cache")
	if !ok || cacheGot != cache {
		t.Fatalf("GetByName(cache) returned wrong component: %v, %v", cacheGot, ok)
	}
	sessionsGot, ok := cf.GetByName[*CFValkey](fw, "sessions")
	if !ok || sessionsGot != sessions {
		t.Fatalf("GetByName(sessions) returned wrong component: %v, %v", sessionsGot, ok)
	}

	// Get returns false when multiple instances exist
	if _, ok := cf.Get[*CFValkey](fw); ok {
		t.Fatal("Get should return false when multiple valkey instances exist")
	}

	// Both instances work independently
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cacheClient := cache.Client()
	sessionsClient := sessions.Client()

	// Write to cache
	cacheKey := "caerus-valkey-cache-test"
	_ = cacheClient.Do(ctx, cacheClient.B().Del().Key(cacheKey).Build()).Error()
	if err := cacheClient.Do(ctx, cacheClient.B().Set().Key(cacheKey).Value("cache-value").Build()).Error(); err != nil {
		t.Fatalf("cache Set: %v", err)
	}

	// Write to sessions
	sessionsKey := "caerus-valkey-sessions-test"
	_ = sessionsClient.Do(ctx, sessionsClient.B().Del().Key(sessionsKey).Build()).Error()
	if err := sessionsClient.Do(ctx, sessionsClient.B().Set().Key(sessionsKey).Value("sessions-value").Build()).Error(); err != nil {
		t.Fatalf("sessions Set: %v", err)
	}

	// Read from cache
	got, err := cacheClient.Do(ctx, cacheClient.B().Get().Key(cacheKey).Build()).ToString()
	if err != nil {
		t.Fatalf("cache Get: %v", err)
	}
	if got != "cache-value" {
		t.Fatalf("cache Get = %q, want cache-value", got)
	}

	// Read from sessions
	got, err = sessionsClient.Do(ctx, sessionsClient.B().Get().Key(sessionsKey).Build()).ToString()
	if err != nil {
		t.Fatalf("sessions Get: %v", err)
	}
	if got != "sessions-value" {
		t.Fatalf("sessions Get = %q, want sessions-value", got)
	}
}

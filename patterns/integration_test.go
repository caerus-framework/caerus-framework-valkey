package patterns

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cf_observability "github.com/caerus-framework/caerus-framework-observability"
	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
	"golang.org/x/sync/singleflight"
)

func setupVK(t *testing.T) *cf_valkey.CFValkey {
	t.Helper()
	addr := os.Getenv("VALKEY_ADDR")
	if addr == "" {
		t.Skip("VALKEY_ADDR not set; skipping integration test")
	}
	vk := cf_valkey.New(
		cf_valkey.WithAddress(addr),
		cf_valkey.WithKeyPrefix("patterns-test"),
	)
	if err := vk.Init(context.Background(), nil); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = vk.Shutdown(context.Background()) })
	return vk
}

func cleanKey(t *testing.T, vk *cf_valkey.CFValkey, parts ...string) {
	t.Helper()
	client := vk.Client()
	ctx := context.Background()
	key := vk.Key(parts...)
	_ = client.Do(ctx, client.B().Del().Key(key).Build()).Error()
}

func TestMutexTryLockUnlock(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "lock", "test1")

	m := NewMutex(vk, "test1", 5*time.Second)
	ok, err := m.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock: %v", err)
	}
	if !ok {
		t.Fatal("TryLock should succeed on a fresh key")
	}

	ok2, err := m.TryLock(ctx)
	if err != nil {
		t.Fatalf("second TryLock: %v", err)
	}
	if ok2 {
		t.Fatal("second TryLock should fail (lock held)")
	}

	if err := m.Unlock(ctx); err != nil {
		t.Fatalf("Unlock: %v", err)
	}

	ok3, err := m.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock after unlock: %v", err)
	}
	if !ok3 {
		t.Fatal("TryLock after unlock should succeed")
	}
	_ = m.Unlock(ctx)
}

func TestMutexWithLock(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "lock", "test2")

	m := NewMutex(vk, "test2", 5*time.Second)
	called := false
	err := m.WithLock(ctx, func(ctx context.Context) error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("WithLock: %v", err)
	}
	if !called {
		t.Fatal("fn should have been called")
	}
}

func TestMutexWithLockContention(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "lock", "test3")

	m := NewMutex(vk, "test3", 5*time.Second)
	ok, err := m.TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("setup TryLock: ok=%v err=%v", ok, err)
	}

	m2 := NewMutex(vk, "test3", 5*time.Second)
	err = m2.WithLock(ctx, func(ctx context.Context) error {
		t.Fatal("fn should not be called when lock is held")
		return nil
	})
	if err != ErrLocked {
		t.Fatalf("WithLock should return ErrLocked, got %v", err)
	}
	_ = m.Unlock(ctx)
}

func TestMutexUnlockDoesNotDeleteOtherToken(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "lock", "test4")

	m1 := NewMutex(vk, "test4", 200*time.Millisecond)
	ok, _ := m1.TryLock(ctx)
	if !ok {
		t.Fatal("m1 TryLock should succeed")
	}

	time.Sleep(300 * time.Millisecond) // m1's lock expires

	m2 := NewMutex(vk, "test4", 5*time.Second)
	ok, _ = m2.TryLock(ctx)
	if !ok {
		t.Fatal("m2 TryLock should succeed after m1's lock expired")
	}

	if err := m1.Unlock(ctx); err != nil {
		t.Fatalf("m1 Unlock with stale token: %v", err)
	}

	client := vk.Client()
	key := vk.Key("lock", "test4")
	resp := client.Do(ctx, client.B().Get().Key(key).Build())
	if resp.Error() != nil {
		t.Fatal("lock key should still exist (m2 still holds it)")
	}
	_ = m2.Unlock(ctx)
}

func TestMutexExpiry(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "lock", "test5")

	m := NewMutex(vk, "test5", 200*time.Millisecond)
	ok, err := m.TryLock(ctx)
	if err != nil || !ok {
		t.Fatalf("TryLock: ok=%v err=%v", ok, err)
	}

	time.Sleep(300 * time.Millisecond)

	m2 := NewMutex(vk, "test5", 5*time.Second)
	ok2, err := m2.TryLock(ctx)
	if err != nil {
		t.Fatalf("TryLock after expiry: %v", err)
	}
	if !ok2 {
		t.Fatal("TryLock after TTL expiry should succeed")
	}
	_ = m2.Unlock(ctx)
}

func TestMutexMetrics(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "lock", "metrics1")

	m1 := NewMutex(vk, "metrics1", 300*time.Millisecond)
	if ok, err := m1.TryLock(ctx); err != nil || !ok {
		t.Fatalf("m1 TryLock: ok=%v err=%v", ok, err)
	}

	time.Sleep(400 * time.Millisecond)

	m2 := NewMutex(vk, "metrics1", 5*time.Second)
	if ok, err := m2.TryLock(ctx); err != nil || !ok {
		t.Fatalf("m2 TryLock after expiry: ok=%v err=%v", ok, err)
	}

	if err := m1.Unlock(ctx); err != nil {
		t.Fatalf("m1 Unlock after expiry: %v", err)
	}

	if ok, err := m2.TryLock(ctx); err != nil || ok {
		t.Fatalf("m2 second TryLock: ok=%v err=%v, want busy", ok, err)
	}

	if err := m2.Unlock(ctx); err != nil {
		t.Fatalf("m2 Unlock: %v", err)
	}

	want := map[string]float64{
		"lock_acquire_ok_total":      2,
		"lock_acquire_busy_total":    1,
		"lock_unlock_ok_total":       1,
		"lock_unlock_mismatch_total": 1,
	}
	got := make(map[string]float64)
	for _, s := range vk.Metrics() {
		if _, ok := want[s.Name]; !ok {
			continue
		}
		if s.Type != cf_observability.MetricTypeCounter {
			t.Fatalf("%s Type = %v, want MetricTypeCounter", s.Name, s.Type)
		}
		got[s.Name] = s.Value
	}
	for name, w := range want {
		if got[name] != w {
			t.Fatalf("%s = %v, want %v (all: %+v)", name, got[name], w, got)
		}
	}
}

func TestGetJSONSetJSON(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "json", "item1")

	type item struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	want := item{Name: "widget", Value: 42}
	if err := SetJSON(ctx, vk, want, 5*time.Second, "json", "item1"); err != nil {
		t.Fatalf("SetJSON: %v", err)
	}

	var got item
	if err := GetJSON(ctx, vk, &got, "json", "item1"); err != nil {
		t.Fatalf("GetJSON: %v", err)
	}
	if got != want {
		t.Fatalf("GetJSON = %+v, want %+v", got, want)
	}
}

func TestGetJSONMissReturnsNil(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "json", "missing")

	var got map[string]any
	if err := GetJSON(ctx, vk, &got, "json", "missing"); err != nil {
		t.Fatalf("GetJSON on missing key: %v", err)
	}
	if got != nil {
		t.Fatalf("GetJSON on missing key = %+v, want nil", got)
	}
}

func TestGetOrLoad(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "gol", "key1")

	g := &singleflight.Group{}
	var loadCount atomic.Int32

	load := func(ctx context.Context) ([]byte, error) {
		loadCount.Add(1)
		return []byte(`"loaded"`), nil
	}

	val, shared, err := GetOrLoad(ctx, vk, g, "key1", 5*time.Second, load)
	if err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}
	if shared {
		t.Fatal("first call should not be shared")
	}
	if string(val) != `"loaded"` {
		t.Fatalf("GetOrLoad = %q, want %q", string(val), `"loaded"`)
	}
	if loadCount.Load() != 1 {
		t.Fatalf("load called %d times, want 1", loadCount.Load())
	}

	val2, shared2, err := GetOrLoad(ctx, vk, g, "key1", 5*time.Second, load)
	if err != nil {
		t.Fatalf("second GetOrLoad: %v", err)
	}
	if shared2 {
		t.Fatal("cache hit should not be shared")
	}
	if string(val2) != `"loaded"` {
		t.Fatalf("second GetOrLoad = %q, want %q", string(val2), `"loaded"`)
	}
	if loadCount.Load() != 1 {
		t.Fatalf("load called %d times on cache hit, want 1", loadCount.Load())
	}
}

func TestGetOrLoadSingleflight(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "gol", "key2")

	g := &singleflight.Group{}
	var loadCount atomic.Int32
	var wg sync.WaitGroup

	load := func(ctx context.Context) ([]byte, error) {
		loadCount.Add(1)
		time.Sleep(50 * time.Millisecond)
		return []byte(`"result"`), nil
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			val, _, err := GetOrLoad(ctx, vk, g, "key2", 5*time.Second, load)
			if err != nil {
				t.Errorf("GetOrLoad: %v", err)
				return
			}
			if string(val) != `"result"` {
				t.Errorf("GetOrLoad = %q, want %q", string(val), `"result"`)
			}
		}()
	}
	wg.Wait()

	if loadCount.Load() != 1 {
		t.Fatalf("load called %d times, want 1 (singleflight)", loadCount.Load())
	}
}

func TestGetOrLoadLoadError(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "gol", "key3")

	g := &singleflight.Group{}
	load := func(ctx context.Context) ([]byte, error) {
		return nil, context.DeadlineExceeded
	}

	_, _, err := GetOrLoad(ctx, vk, g, "key3", 5*time.Second, load)
	if err != context.DeadlineExceeded {
		t.Fatalf("GetOrLoad with failing load = %v, want DeadlineExceeded", err)
	}
}

func TestGetOrLoadComposesWithGetJSON(t *testing.T) {
	vk := setupVK(t)
	ctx := context.Background()
	cleanKey(t, vk, "gol", "json1")

	g := &singleflight.Group{}
	type payload struct {
		ID int `json:"id"`
	}

	load := func(ctx context.Context) ([]byte, error) {
		return json.Marshal(payload{ID: 99})
	}

	val, _, err := GetOrLoad(ctx, vk, g, "json1", 5*time.Second, load)
	if err != nil {
		t.Fatalf("GetOrLoad: %v", err)
	}

	var got payload
	if err := json.Unmarshal(val, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ID != 99 {
		t.Fatalf("ID = %d, want 99", got.ID)
	}

	var got2 payload
	if err := GetJSON(ctx, vk, &got2, "json1"); err != nil {
		t.Fatalf("GetJSON after GetOrLoad: %v", err)
	}
	if got2.ID != 99 {
		t.Fatalf("GetJSON ID = %d, want 99", got2.ID)
	}
}

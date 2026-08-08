package patterns

import (
	"context"
	"errors"
	"time"

	"github.com/valkey-io/valkey-go"
	"golang.org/x/sync/singleflight"
)

// LoadFunc fetches the canonical value on cache miss. Called at most once per
// key per in-flight wave inside this process.
type LoadFunc func(ctx context.Context) ([]byte, error)

// GetOrLoad returns cached bytes for key (via vk.Key), or runs load once for
// concurrent callers in this process, then stores the result with ttl.
//
// WARNING: process-local singleflight only. Other pods still stampede on a cold
// key. For cross-pod coalescing, compose with a Mutex.
//
// shared=true when this caller waited on another goroutine's load.
//
// Failure modes:
//   - Many goroutines, one process, cold key: one load, others share.
//   - Many pods, cold key: each pod may call load.
//   - load fails: error returned to waiters; nothing cached.
//   - Valkey down on GET: error returned (no silent bypass).
//   - Valkey down on SET after load: loaded value returned along with the set
//     error so callers can log; the successful load is not lost.
func GetOrLoad(
	ctx context.Context,
	vk ClientKeyer,
	g *singleflight.Group,
	key string,
	ttl time.Duration,
	load LoadFunc,
) (val []byte, shared bool, err error) {
	client := vk.Client()
	if client == nil {
		return nil, false, errors.New("patterns: valkey client is not initialized")
	}
	fullKey := vk.Key(key)

	resp := client.Do(ctx, client.B().Get().Key(fullKey).Build())
	if resp.Error() == nil {
		b, err := resp.AsBytes()
		if err == nil {
			return b, false, nil
		}
	}
	if resp.Error() != nil && !errors.Is(resp.Error(), valkey.Nil) {
		return nil, false, resp.Error()
	}

	fullKeyCopy := fullKey
	result, err, shared := g.Do(fullKeyCopy, func() (any, error) {
		b, err := load(ctx)
		if err != nil {
			return nil, err
		}
		ttlMs := ttl
		setErr := client.Do(ctx, client.B().Set().Key(fullKeyCopy).Value(string(b)).Px(ttlMs).Build()).Error()
		if setErr != nil {
			return b, setErr
		}
		return b, nil
	})
	if result != nil {
		val = result.([]byte)
	}
	if err != nil && val != nil {
		return val, shared, err
	}
	return val, shared, err
}

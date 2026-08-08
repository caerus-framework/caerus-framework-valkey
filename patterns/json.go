package patterns

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/valkey-io/valkey-go"
)

// GetJSON retrieves the value at vk.Key(parts...) and unmarshals it from JSON
// into dst. Returns nil and no error when the key does not exist (Valkey nil
// reply). A non-nil error is returned for transport failures or JSON decode
// errors.
func GetJSON(ctx context.Context, vk ClientKeyer, dst any, parts ...string) error {
	client := vk.Client()
	if client == nil {
		return errors.New("patterns: valkey client is not initialized")
	}
	key := vk.Key(parts...)
	resp := client.Do(ctx, client.B().Get().Key(key).Build())
	if resp.Error() != nil {
		if errors.Is(resp.Error(), valkey.Nil) {
			return nil
		}
		return resp.Error()
	}
	b, err := resp.AsBytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

// SetJSON marshals v as JSON and stores it at vk.Key(parts...) with the given
// TTL. The key is always prefix-aware through vk.Key.
func SetJSON(ctx context.Context, vk ClientKeyer, v any, ttl time.Duration, parts ...string) error {
	client := vk.Client()
	if client == nil {
		return errors.New("patterns: valkey client is not initialized")
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	key := vk.Key(parts...)
	return client.Do(ctx, client.B().Set().Key(key).Value(string(b)).Px(ttl).Build()).Error()
}

package patterns

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	cf_valkey "github.com/caerus-framework/caerus-framework-valkey"
	"github.com/valkey-io/valkey-go"
)

// ClientKeyer is the minimal interface patterns requires from a Valkey
// component. *cf_valkey.CFValkey satisfies it.
type ClientKeyer interface {
	Client() valkey.Client
	Key(parts ...string) string
}

// lockMeterProvider is the optional surface for feeding lock-traffic counters
// into the component's metrics. *cf_valkey.CFValkey satisfies it; a fake that
// only implements ClientKeyer simply records no counters.
type lockMeterProvider interface {
	LockMeter() *cf_valkey.LockMeter
}

var _ ClientKeyer = (*cf_valkey.CFValkey)(nil)
var _ lockMeterProvider = (*cf_valkey.CFValkey)(nil)

// Mutex is a small, single-instance distributed lock backed by Valkey.
// It uses SET NX PX for acquisition and a Lua script for ownership-checked
// deletion, so Unlock never removes another holder's lock.
//
// Failure modes (read before use):
//
//   - Single Valkey primary: at most one holder per key among clients talking
//     to that instance. This is the default deployment assumption.
//   - Holder crashes: lock expires after TTL; another pod may acquire. Size
//     TTL >> expected critical section; << how long "double work" is acceptable.
//   - Critical section outlives TTL: another pod can take the lock while the
//     first still runs. Keep work << TTL.
//   - Multi-master / split brain / Redlock: out of scope. Use a stronger
//     coordination system if you need that.
//
// This is not Redlock. It is for "only one pod should run this reconcile /
// refresh", not for substituting a consensus log.
type Mutex struct {
	vk    ClientKeyer
	key   string
	ttl   time.Duration
	token string
	meter *cf_valkey.LockMeter
}

// NewMutex creates a distributed mutex. The lock key is always built through
// vk.Key("lock", name) so it sits in the component's prefix-aware key space.
// TTL is mandatory; there is no "lock forever" mode. When vk exposes a
// LockMeter (the *cf_valkey.CFValkey chassis does), lock traffic is counted
// and exposed on /metrics as *total counters.
func NewMutex(vk ClientKeyer, name string, ttl time.Duration) *Mutex {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	var meter *cf_valkey.LockMeter
	if lm, ok := vk.(lockMeterProvider); ok {
		meter = lm.LockMeter()
	}
	return &Mutex{
		vk:    vk,
		key:   vk.Key("lock", name),
		ttl:   ttl,
		meter: meter,
	}
}

// TryLock attempts to acquire the lock. It returns ok=true if the lock was
// acquired, ok=false if another holder holds it. An error is returned only for
// transport-level failures (Valkey down, context cancelled).
func (m *Mutex) TryLock(ctx context.Context) (bool, error) {
	token, err := newToken()
	if err != nil {
		return false, fmt.Errorf("patterns: generate lock token: %w", err)
	}
	client := m.vk.Client()
	if client == nil {
		return false, errors.New("patterns: valkey client is not initialized")
	}
	resp := client.Do(ctx, client.B().Set().Key(m.key).Value(token).Nx().Px(m.ttl).Build())
	if resp.Error() != nil {
		if errors.Is(resp.Error(), valkey.Nil) {
			if m.meter != nil {
				m.meter.IncAcquireBusy()
			}
			return false, nil
		}
		return false, resp.Error()
	}
	if m.meter != nil {
		m.meter.IncAcquireOK()
	}
	m.token = token
	return true, nil
}

// Unlock releases the lock only if the caller still owns it (token match).
// If the lock has expired or been stolen, Unlock returns nil without error
// (the lock is no longer ours to release). The outcome is counted on the
// component's lock meter (unlock_ok vs unlock_mismatch).
func (m *Mutex) Unlock(ctx context.Context) error {
	if m.token == "" {
		return nil
	}
	client := m.vk.Client()
	if client == nil {
		return errors.New("patterns: valkey client is not initialized")
	}
	resp := client.Do(ctx, client.B().Eval().Script(luaUnlock).Numkeys(1).Key(m.key).Arg(m.token).Build())
	m.token = ""
	if resp.Error() != nil {
		return resp.Error()
	}
	n, err := resp.AsInt64()
	if err != nil {
		return err
	}
	if m.meter != nil {
		if n == 1 {
			m.meter.IncUnlockOK()
		} else {
			m.meter.IncUnlockMismatch()
		}
	}
	return nil
}

// WithLock acquires the lock, runs fn, and releases the lock. If TryLock
// returns ok=false, fn is not called and WithLock returns ErrLocked. Unlock
// is best-effort: it runs even if fn fails, and ctx cancellation still
// attempts Unlock.
func (m *Mutex) WithLock(ctx context.Context, fn func(context.Context) error) error {
	ok, err := m.TryLock(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return ErrLocked
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.Unlock(unlockCtx)
	}()
	return fn(ctx)
}

// ErrLocked is returned by WithLock when TryLock finds the lock already held.
var ErrLocked = errors.New("patterns: lock is held by another owner")

const luaUnlock = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`

func newToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b), nil
}

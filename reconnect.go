package cf_valkey

import (
	"context"
	"math/rand/v2"
	"time"

	cf_logs "github.com/caerus-framework/caerus-framework-logs"
	"github.com/valkey-io/valkey-go"
)

const (
	reconnectInitial = 500 * time.Millisecond
	reconnectMax     = 30 * time.Second
	reconnectHealthy = 5 * time.Second
)

func (c *CFValkey) startReconnectLocked() {
	if !c.degradedMode || c.reconnectCancel != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.reconnectCancel = cancel
	c.reconnectWG.Add(1)
	go func() {
		defer c.reconnectWG.Done()
		c.reconnectLoop(ctx)
	}()
}

func (c *CFValkey) reconnectLoop(ctx context.Context) {
	delay := reconnectInitial
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay + reconnectJitter(delay)):
		}
		if c.reconnectOnce() {
			delay = reconnectHealthy
			continue
		}
		delay *= 2
		if delay > reconnectMax {
			delay = reconnectMax
		}
	}
}

func reconnectJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d/4) + 1))
}

func (c *CFValkey) reconnectOnce() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.initDone.Load() {
		return false
	}
	if c.client != nil {
		pingCtx, cancel := context.WithTimeout(context.Background(), c.pingTimeout)
		err := c.client.Do(pingCtx, c.client.B().Ping().Build()).Error()
		cancel()
		if err == nil {
			c.liveConnected.Store(true)
			c.degradedUnreachable.Store(false)
			return true
		}
		c.pingFailures.Add(1)
		c.liveConnected.Store(false)
		c.degradedUnreachable.Store(true)
	}
	opts := c.opts
	if err := c.applyTLS(&opts); err != nil {
		c.logger.Error("cf_valkey: reconnect TLS failed", "err", err)
		return false
	}
	newClient, err := valkey.NewClient(opts)
	if err != nil {
		c.logger.Error("cf_valkey: reconnect create client failed", "err", err)
		return false
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), c.pingTimeout)
	err = newClient.Do(pingCtx, newClient.B().Ping().Build()).Error()
	cancel()
	if err != nil {
		newClient.Close()
		c.pingFailures.Add(1)
		return false
	}
	old := c.client
	c.client = newClient
	c.opts = opts
	c.liveConnected.Store(true)
	c.degradedUnreachable.Store(false)
	c.reconnects.Add(1)
	if old != nil {
		old.Close()
	}
	c.logger.Info("cf_valkey: reconnected",
		"addresses", opts.InitAddress,
		cf_logs.SecretSet("password", opts.Password),
	)
	return true
}

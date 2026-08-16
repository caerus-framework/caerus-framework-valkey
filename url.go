package cf_valkey

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ParseURL parses a redis:// or valkey:// URL (or host:port) into ValkeyConfig.
// Examples:
//
//	redis://user:pass@127.0.0.1:6379/0
//	valkey://127.0.0.1:6379
//	127.0.0.1:6379
func ParseURL(raw string) (ValkeyConfig, error) {
	var zero ValkeyConfig
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return zero, fmt.Errorf("cf_valkey: empty URL")
	}
	if !strings.Contains(raw, "://") {
		return ValkeyConfig{Addresses: []string{raw}}, nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		// url.Error.Error interpolates the raw URL (password in userinfo).
		return zero, errors.New("cf_valkey: parse URL: invalid URL")
	}
	host := u.Host
	if host == "" {
		return zero, fmt.Errorf("cf_valkey: URL missing host")
	}
	switch scheme := strings.ToLower(u.Scheme); scheme {
	case "redis", "rediss", "valkey", "valkeys":
		cfg := ValkeyConfig{Addresses: []string{host}}
		if scheme == "rediss" || scheme == "valkeys" {
			t := true
			cfg.TLS = &t
		}
		if u.User != nil {
			cfg.Username = u.User.Username()
			if p, ok := u.User.Password(); ok {
				cfg.Password = p
			}
		}
		if path := strings.TrimPrefix(u.Path, "/"); path != "" {
			db, err := strconv.Atoi(path)
			if err != nil {
				return zero, fmt.Errorf("cf_valkey: invalid DB path %q: %w", u.Path, err)
			}
			cfg.DB = db
		}
		return cfg, nil
	default:
		return zero, fmt.Errorf("cf_valkey: unsupported URL scheme %q", u.Scheme)
	}
}

// OverlayURL merges connection fields from a redis/valkey URL into cfg.
// URL-derived fields win over existing values (file/env).
func OverlayURL(cfg *ValkeyConfig, raw string) error {
	if cfg == nil {
		return fmt.Errorf("cf_valkey: OverlayURL nil config")
	}
	parsed, err := ParseURL(raw)
	if err != nil {
		return err
	}
	if len(parsed.Addresses) > 0 {
		cfg.Addresses = parsed.Addresses
	}
	if parsed.Username != "" {
		cfg.Username = parsed.Username
	}
	if parsed.Password != "" {
		cfg.Password = parsed.Password
	}
	// DB 0 is valid; apply whenever the URL specified a path or was host-only
	// with implicit 0. Host-only ParseURL leaves DB 0 — overlay only when URL
	// had an explicit path, or when Addresses were set from a bare host:port
	// (DB stays as previously configured unless path present).
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err == nil && strings.TrimPrefix(u.Path, "/") != "" {
			cfg.DB = parsed.DB
		}
		if err == nil {
			switch strings.ToLower(u.Scheme) {
			case "rediss", "valkeys":
				t := true
				cfg.TLS = &t
			case "redis", "valkey":
				if cfg.TLSCAFile == "" && cfg.TLSCertFile == "" && cfg.TLSKeyFile == "" {
					f := false
					cfg.TLS = &f
				}
			}
		}
	} else if parsed.TLS != nil {
		cfg.TLS = parsed.TLS
	}
	return nil
}

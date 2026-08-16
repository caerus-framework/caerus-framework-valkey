package cf_valkey

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
)

func TestParseURL(t *testing.T) {
	cfg, err := ParseURL("redis://alice:s3cret@10.0.0.1:6380/2")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Addresses) != 1 || cfg.Addresses[0] != "10.0.0.1:6380" {
		t.Fatalf("Addresses = %#v", cfg.Addresses)
	}
	if cfg.Username != "alice" || cfg.Password != "s3cret" || cfg.DB != 2 {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestParseURLBareHost(t *testing.T) {
	cfg, err := ParseURL("127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Addresses) != 1 || cfg.Addresses[0] != "127.0.0.1:6379" {
		t.Fatalf("Addresses = %#v", cfg.Addresses)
	}
}

func TestParseURLInvalidDoesNotEchoPassword(t *testing.T) {
	_, err := ParseURL("redis://alice:LEAKSECRET@[")
	if err == nil {
		t.Fatal("want parse error")
	}
	if strings.Contains(err.Error(), "LEAKSECRET") {
		t.Fatalf("password leaked in error: %v", err)
	}
}

func TestValkeyConfigLogArgsNeverCleartext(t *testing.T) {
	cfg := ValkeyConfig{Addresses: []string{"10.0.0.1:6379"}, Password: "s3cret-value"}
	var buf bytes.Buffer
	l := slog.New(slog.NewTextHandler(&buf, nil))
	l.Info("summary", cf_configuration.LogArgs(cfg)...)
	out := buf.String()
	if strings.Contains(out, "s3cret-value") {
		t.Fatalf("password leaked: %s", out)
	}
	if !strings.Contains(out, "[redacted]") {
		t.Fatalf("want [redacted] in %s", out)
	}
	if !strings.Contains(out, "10.0.0.1:6379") {
		t.Fatalf("address should stay visible: %s", out)
	}
}

func TestParseURLRedissSetsTLS(t *testing.T) {
	cfg, err := ParseURL("rediss://:secret@valkey.internal:6379")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLS == nil || !*cfg.TLS {
		t.Fatal("rediss:// must set TLS true")
	}
	cfg2, err := ParseURL("redis://127.0.0.1:6379")
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.TLS != nil && *cfg2.TLS {
		t.Fatal("redis:// must not enable TLS")
	}
}

func TestOverlayURLRedissEnablesTLS(t *testing.T) {
	cfg := ValkeyConfig{Addresses: []string{"file:6379"}}
	if err := OverlayURL(&cfg, "rediss://u:p@env:6380"); err != nil {
		t.Fatal(err)
	}
	if cfg.TLS == nil || !*cfg.TLS {
		t.Fatal("overlay rediss:// must set TLS")
	}
}

func TestOverlayURL(t *testing.T) {
	cfg := ValkeyConfig{Addresses: []string{"file:6379"}, DB: 5}
	if err := OverlayURL(&cfg, "valkey://u:p@env:6380/1"); err != nil {
		t.Fatal(err)
	}
	if cfg.Addresses[0] != "env:6380" || cfg.Username != "u" || cfg.Password != "p" || cfg.DB != 1 {
		t.Fatalf("cfg = %+v", cfg)
	}
}

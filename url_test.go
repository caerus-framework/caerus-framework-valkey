package cf_valkey

import "testing"

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

func TestOverlayURL(t *testing.T) {
	cfg := ValkeyConfig{Addresses: []string{"file:6379"}, DB: 5}
	if err := OverlayURL(&cfg, "valkey://u:p@env:6380/1"); err != nil {
		t.Fatal(err)
	}
	if cfg.Addresses[0] != "env:6380" || cfg.Username != "u" || cfg.Password != "p" || cfg.DB != 1 {
		t.Fatalf("cfg = %+v", cfg)
	}
}

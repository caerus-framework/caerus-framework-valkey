package cf_valkey

import (
	"testing"

	cf_configuration "github.com/caerus-framework/caerus-framework-configuration"
)

func TestWithConfigSourceDeclaresConfigurationDependency(t *testing.T) {
	v := New(WithConfigSource("valkey", ""))
	deps := v.GetDependencies()
	found := false
	for _, d := range deps {
		if d == cf_configuration.ComponentName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("deps = %v, want %q", deps, cf_configuration.ComponentName)
	}
}

func TestOnConfigReloadNoopWithoutSource(t *testing.T) {
	v := New()
	v.OnConfigReload("valkey", nil)
}

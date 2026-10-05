package main

import (
	"os"
	"path/filepath"
	"testing"
)

// The API serves embedded copies (router.go go:embed, which forbids `..`
// patterns), so the served files must stay byte-identical to their canonical
// sources: the root openapi.yaml and web/dashboard.html. This test fails the
// build on drift instead of letting docs and reality diverge silently.
func TestEmbeddedAssetsInSync(t *testing.T) {
	pairs := [][2]string{
		{"openapi.yaml", filepath.Join("..", "..", "openapi.yaml")},
		{"dashboard.html", filepath.Join("..", "..", "web", "dashboard.html")},
	}
	for _, pair := range pairs {
		served, err := os.ReadFile(pair[0])
		if err != nil {
			t.Fatalf("read %s: %v", pair[0], err)
		}
		canonical, err := os.ReadFile(pair[1])
		if err != nil {
			t.Fatalf("read %s: %v", pair[1], err)
		}
		if string(served) != string(canonical) {
			t.Errorf(
				"cmd/api/%s drifted from %s: edit the canonical file, then copy it over",
				pair[0],
				pair[1],
			)
		}
	}
}

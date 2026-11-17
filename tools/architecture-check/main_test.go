package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestImportBoundaries(t *testing.T) {
	cases := []struct {
		file, imp string
		bad       bool
	}{
		{"internal/domain/x.go", "github.com/jackc/pgx/v5", true},
		{"internal/domain/x.go", module + "services/dataset-versioning-service/internal/repo", true},
		{"internal/domain/x.go", module + "services/dataset-versioning-service/internal/models", false},
		{"internal/repo/x.go", "github.com/jackc/pgx/v5", false},
		{"internal/handlers/x.go", "github.com/nats-io/nats.go", true},
		{"internal/handlers/x.go", "github.com/nats-io/nats-server/v2/server", true},
		{"internal/handlers/x.go", "github.com/nats-iox/foo", false},
		{"internal/handlers/x.go", "github.com/segmentio/kafka-go", true},
		{"internal/handlers/x.go", "github.com/segmentio/kafka-go/sasl", true},
		{"internal/handlers/x.go", "github.com/segmentio/kafka-go-metrics", false},
		{"internal/handlers/x.go", "github.com/IBM/sarama", true},
		{"internal/handlers/x.go", "github.com/IBM/sarama-x", false},
		{"internal/handlers/x.go", module + "libs/event-bus-data", true},
		{"internal/handlers/x.go", module + "libs/event-bus-data/sub", true},
		{"internal/handlers/x.go", module + "libs/event-bus-data-v2", false},
		{"internal/handlers/x.go", module + "libs/event-bus-control", false},
		{"internal/handlers/x.go", module + "services/ontology-query-service/public", true},
		{"internal/domain/sub/x.go", "database/sql", true},
		{"internal/domain/x.go", "database/sqlx", false},
		{"internal/domain/x.go", "github.com/jackc/pgtype", false},
	}
	for _, c := range cases {
		t.Run(c.file+":"+c.imp, func(t *testing.T) {
			if got := forbidden("services/dataset-versioning-service/"+c.file, c.imp) != ""; got != c.bad {
				t.Fatalf("bad=%v want %v", got, c.bad)
			}
		})
	}
}
func TestScannerRejectsAliasedBuildTaggedImport(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "services/dataset-versioning-service/internal/domain")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "bad.go")
	if err := os.WriteFile(path, []byte("//go:build special\n\npackage domain\nimport db \"github.com/jackc/pgx/v5\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := check(root)
	if err != nil || len(got) != 1 {
		t.Fatalf("got %v err %v", got, err)
	}
	if err := os.WriteFile(path, []byte("package domain\nimport \"time\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	got, err = check(root)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v err %v", got, err)
	}
}
func TestMissingScopeFails(t *testing.T) {
	if _, err := check(t.TempDir()); err == nil {
		t.Fatal("missing scope must fail")
	}
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestScannerFlagsDeepPathsButNotTestFiles(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "services/dataset-versioning-service")
	writeFile(t, filepath.Join(base, "internal/runtime/retention/worker.go"),
		"package retention\n\nimport _ \"github.com/IBM/sarama\"\n")
	writeFile(t, filepath.Join(base, "internal/handlers/handlers_test.go"),
		"package handlers\n\nimport _ \"github.com/nats-io/nats.go\"\n")
	got, err := check(root)
	if err != nil || len(got) != 1 || !strings.Contains(got[0], "retention") {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestParseErrorFails(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "services/dataset-versioning-service/internal/domain/broken.go"),
		"package domain\n\nimport (\n")
	if _, err := check(root); err == nil {
		t.Fatal("parse error must fail, not pass")
	}
}

func TestEmptyScopeFails(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "services/dataset-versioning-service"), 0755); err != nil {
		t.Fatal(err)
	}
	if _, err := check(root); err == nil {
		t.Fatal("empty scope must fail, not pass")
	}
}

// Command architecture-check enforces the dataset pilot's import boundaries.
package main

import (
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const module = "github.com/openfoundry/openfoundry-go/"

// covers reports whether import path imp denotes package p itself or a
// package under p. Matching stops at the path-element boundary, so a rule
// for "kafka-go" does not catch an unrelated "kafka-go-metrics".
func covers(imp, p string) bool {
	return imp == p || strings.HasPrefix(imp, p+"/")
}

func forbidden(file, imp string) string {
	if !strings.HasPrefix(file, "services/dataset-versioning-service/") {
		return ""
	}
	for _, p := range []string{"github.com/nats-io", "github.com/segmentio/kafka-go", "github.com/confluentinc", "github.com/IBM/sarama", module + "libs/event-bus-data"} {
		if covers(imp, p) {
			return "dataset control-plane code must not import raw brokers or the data bus"
		}
	}
	if strings.HasPrefix(imp, module+"services/") && !covers(imp, module+"services/dataset-versioning-service") {
		return "use public contracts instead of another service's implementation"
	}
	if strings.Contains(file, "/internal/domain/") {
		for _, p := range []string{"database/sql", "github.com/jackc/pgx", module + "services/dataset-versioning-service/internal/repo", module + "services/dataset-versioning-service/internal/handlers"} {
			if covers(imp, p) {
				return "domain logic must not depend on persistence or HTTP handlers"
			}
		}
	}
	return ""
}

func check(root string) ([]string, error) {
	var failures []string
	scope := filepath.Join(root, "services/dataset-versioning-service")
	files := 0
	err := filepath.WalkDir(scope, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files++
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return fmt.Errorf("parse %s: %w", path, err)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		for _, i := range file.Imports {
			imp, err := strconv.Unquote(i.Path.Value)
			if err != nil {
				return err
			}
			if reason := forbidden(filepath.ToSlash(rel), imp); reason != "" {
				failures = append(failures, fmt.Sprintf("%s: %s: %s", rel, imp, reason))
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if files == 0 {
		return nil, fmt.Errorf("no production .go files found under %s", scope)
	}
	return failures, nil
}

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	failures, err := check(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	for _, f := range failures {
		fmt.Fprintln(os.Stderr, f)
	}
	if len(failures) > 0 {
		os.Exit(1)
	}
	fmt.Println("PASS: dataset import boundaries (production Go source, all build tags)")
}

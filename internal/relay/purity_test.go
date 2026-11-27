package relay_test

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRelayPurity_NoWasmOrOutboundHTTPWebhooks enforces ADR-0001: stravin-relay
// must be a pure DynamoDB Streams -> NATS JetStream outbox with zero Wasm
// execution and zero outbound HTTP webhook delivery.
func TestRelayPurity_NoWasmOrOutboundHTTPWebhooks(t *testing.T) {
	forbidden := []string{
		"github.com/tetratelabs/wazero",
		"stravin.dev/core/internal/hooks/wasm",
		"stravin.dev/core/internal/hooks/async",
		"net/http",
	}

	dirs := []string{
		".",
		filepath.Join("..", "..", "cmd", "stravin-relay"),
	}
	fset := token.NewFileSet()
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir(%s): %v", dir, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
			if err != nil {
				t.Fatalf("ParseFile(%s): %v", path, err)
			}
			for _, imp := range file.Imports {
				impPath := strings.Trim(imp.Path.Value, `"`)
				for _, bad := range forbidden {
					if impPath == bad {
						t.Errorf("%s imports forbidden package %q (violates ADR-0001 stravin-relay purity)", path, impPath)
					}
				}
			}
		}
	}
}

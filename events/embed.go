// Package events makes the JSON Schema files in this directory available to the Go
// services, compiled and ready to validate against.
//
// The Go package lives in the same directory as the schemas on purpose. `go:embed` cannot
// reach outside its own package directory, so the alternatives were to copy the schemas
// into a Go package (two copies that drift) or to read them from disk at runtime (works in
// development, fails in a scratch container). Putting the package here means the schemas
// are embedded in the binary and there is exactly one copy of them in the repository.
//
// This directory is therefore both an npm package and a Go package. That is unusual, and
// the reason is written above so the next person does not "tidy" it.
package events

import (
	"bytes"
	"embed"
	"fmt"
	"io/fs"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

//go:embed schemas/*.json
var schemaFS embed.FS

// Base URI that every schema's $id is built from. The $refs inside the schemas are
// relative ("common.schema.json#/$defs/uuid") and resolve against it.
const schemaBaseURI = "https://github.com/CGainsWorks/sw-event-driven-microservices/events/schemas/"

// Schemas that define shared types rather than a concrete event.
var sharedSchemas = map[string]bool{
	"common.schema.json":   true,
	"envelope.schema.json": true,
}

var (
	compileOnce sync.Once
	compiled    map[string]*jsonschema.Schema
	compileErr  error
)

// Validator returns the compiled schema for one event type, e.g. "orders.v1.OrderPlaced".
//
// Compilation happens once, lazily, and is safe for concurrent use afterwards. A schema
// that fails to compile is a build-time mistake that has escaped into runtime, so the
// error is returned rather than swallowed — a service that cannot validate its own events
// must refuse to start rather than publish unvalidated ones.
func Validator(eventType string) (*jsonschema.Schema, error) {
	if err := compileAll(); err != nil {
		return nil, err
	}
	sch, ok := compiled[eventType]
	if !ok {
		return nil, fmt.Errorf("events: no schema for event type %q", eventType)
	}
	return sch, nil
}

// EventTypes lists every event type with a compiled schema, sorted by nothing in
// particular. Used by tests and by the startup self-check.
func EventTypes() ([]string, error) {
	if err := compileAll(); err != nil {
		return nil, err
	}
	types := make([]string, 0, len(compiled))
	for t := range compiled {
		types = append(types, t)
	}
	return types, nil
}

func compileAll() error {
	compileOnce.Do(func() {
		c := jsonschema.NewCompiler()

		entries, err := fs.ReadDir(schemaFS, "schemas")
		if err != nil {
			compileErr = fmt.Errorf("events: reading embedded schemas: %w", err)
			return
		}

		// Every schema is registered as a resource under its $id before anything is
		// compiled, so that the relative $refs between them resolve without network
		// access. Nothing here ever touches the internet.
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			raw, err := schemaFS.ReadFile("schemas/" + e.Name())
			if err != nil {
				compileErr = fmt.Errorf("events: reading %s: %w", e.Name(), err)
				return
			}
			doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
			if err != nil {
				compileErr = fmt.Errorf("events: parsing %s: %w", e.Name(), err)
				return
			}
			if err := c.AddResource(schemaBaseURI+e.Name(), doc); err != nil {
				compileErr = fmt.Errorf("events: adding %s: %w", e.Name(), err)
				return
			}
			names = append(names, e.Name())
		}

		compiled = make(map[string]*jsonschema.Schema, len(names))
		for _, name := range names {
			if sharedSchemas[name] {
				continue
			}
			sch, err := c.Compile(schemaBaseURI + name)
			if err != nil {
				compileErr = fmt.Errorf("events: compiling %s: %w", name, err)
				return
			}
			// The filename is the event type. events/schemas.test.mjs asserts that the
			// filename and the schema's `title` agree, so this mapping is checked.
			compiled[strings.TrimSuffix(name, ".schema.json")] = sch
		}
	})
	return compileErr
}

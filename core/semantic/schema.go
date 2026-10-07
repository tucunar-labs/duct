package semantic

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"gopkg.in/yaml.v3"
)

// SpecVersion is the Ossie spec version of the vendored schema.
const SpecVersion = "0.2.0.dev0"

//go:embed schema/ossie-schema.json
var schemaJSON []byte

var messagePrinter = message.NewPrinter(language.English)

var (
	schemaOnce sync.Once
	schema     *jsonschema.Schema
	schemaErr  error
)

func compiledSchema() (*jsonschema.Schema, error) {
	schemaOnce.Do(func() {
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schemaJSON))
		if err != nil {
			schemaErr = err
			return
		}
		c := jsonschema.NewCompiler()
		if err := c.AddResource("ossie-schema.json", doc); err != nil {
			schemaErr = err
			return
		}
		schema, schemaErr = c.Compile("ossie-schema.json")
	})
	return schema, schemaErr
}

// validateDocument checks one YAML/JSON document against the Ossie schema and
// returns a readable error listing every violation.
func validateDocument(data []byte) error {
	var generic any
	if err := yaml.Unmarshal(data, &generic); err != nil {
		return fmt.Errorf("invalid YAML: %w", err)
	}
	if generic == nil {
		return fmt.Errorf("empty document")
	}
	// Round-trip through JSON so the validator sees JSON types only.
	js, err := json.Marshal(generic)
	if err != nil {
		return fmt.Errorf("document is not JSON-representable: %w", err)
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(js))
	if err != nil {
		return err
	}
	if m, ok := inst.(map[string]any); ok {
		if v, ok := m["version"].(string); ok && v != SpecVersion {
			return fmt.Errorf("unsupported Ossie version %q (this build handles %s)", v, SpecVersion)
		}
	}
	sc, err := compiledSchema()
	if err != nil {
		return fmt.Errorf("ossie schema: %w", err)
	}
	if err := sc.Validate(inst); err != nil {
		var ve *jsonschema.ValidationError
		if asValidation(err, &ve) {
			return fmt.Errorf("does not match the Ossie schema:\n%s", flatten(ve))
		}
		return err
	}
	return nil
}

func asValidation(err error, target **jsonschema.ValidationError) bool {
	ve, ok := err.(*jsonschema.ValidationError)
	if ok {
		*target = ve
	}
	return ok
}

// flatten lists the leaf causes of a validation error, one per line.
func flatten(ve *jsonschema.ValidationError) string {
	var lines []string
	var walk func(e *jsonschema.ValidationError)
	walk = func(e *jsonschema.ValidationError) {
		if len(e.Causes) == 0 {
			loc := "/" + strings.Join(e.InstanceLocation, "/")
			lines = append(lines, "  "+loc+": "+e.ErrorKind.LocalizedString(messagePrinter))
			return
		}
		for _, c := range e.Causes {
			walk(c)
		}
	}
	walk(ve)
	return strings.Join(lines, "\n")
}

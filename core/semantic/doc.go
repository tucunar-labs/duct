// Package semantic is Duct's semantic layer (ADR 0011): it loads Apache Ossie
// models (semantic/*.ossie.yaml), validates them against the vendored JSON
// Schema and compiles structured metric queries (dims, measures, filters) to
// DuckDB SQL over the ROLE VIEWS of the caller, never over base tables.
//
// The package is pure: no I/O besides reading model files, no engine. The SQL
// it returns still goes through the governed QueryHandler, whose guard is the
// second layer.
//
// Ossie spec handled: 0.2.0.dev0 (schema/ossie-schema.json). The Go CLI of
// Ossie (github.com/apache/ossie/cli) keeps everything under internal/ and its
// `validate` is not implemented yet, so there is nothing to import.
package semantic

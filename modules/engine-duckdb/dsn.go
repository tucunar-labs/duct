//go:build !ducttest

package engineduckdb

// startupDSN is the DuckDB DSN of a lake connection. Production builds always
// verify extension signatures; see dsn_test_build.go for the test-only variant.
func startupDSN() string { return "" }

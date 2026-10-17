package main

import (
	"os"
	"testing"
)

// TestMain fixes the process environment once so tests can run in parallel:
// t.Setenv is forbidden with t.Parallel, and the audit tests only need a
// stable acting user.
func TestMain(m *testing.M) {
	for k, v := range map[string]string{"USER": "tester", "DUCT_TEST_SKIP_EXTENSION_VERIFY": "1"} { // see engine-duckdb startupDSN
		if err := os.Setenv(k, v); err != nil {
			panic(err)
		}
	}
	os.Exit(m.Run())
}

//go:build ducttest

package engineduckdb

import "os"

// startupDSN, only in binaries built with -tags ducttest (the test suites):
// DUCT_TEST_SKIP_EXTENSION_VERIFY=1 skips the signature check of LOAD, which
// costs ~0.3 CPU-s per extension and dominates a suite that opens hundreds of
// engines. It can never be enabled in a release binary.
func startupDSN() string {
	if os.Getenv("DUCT_TEST_SKIP_EXTENSION_VERIFY") == "1" {
		return "?allow_unsigned_extensions=true"
	}
	return ""
}

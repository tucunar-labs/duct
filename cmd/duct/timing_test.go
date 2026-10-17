package main

import "testing"

// timingSem bounds how many timing-sensitive tests run at once.
var timingSem = make(chan struct{}, 3)

// TestTiming groups the tests that assert on latencies, cron ticks and other wall-clock
// behaviour. Being a serial test, it runs before the parallel batch (so these tests never share
// the CPU with the heavy e2e flows), and its subtests run three at a time, since they mostly wait.
// Run one with: go test ./cmd/duct -run 'TestTiming/TestAlertsRunner'.
func TestTiming(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   func(*testing.T)
	}{
		{"TestAlertsRunner", testAlertsRunner},
		{"TestAlertsDeliveryFailureIsRecorded", testAlertsDeliveryFailureIsRecorded},
		{"TestAlertsHysteresisAndReminders", testAlertsHysteresisAndReminders},
		{"TestAlertsSilences", testAlertsSilences},
		{"TestAlertsActiveActive", testAlertsActiveActive},
		{"TestE2EJobsServeSchedulerAndTriggers", testE2EJobsServeSchedulerAndTriggers},
		{"TestJobsHotReload", testJobsHotReload},
		{"TestJobsCatchup", testJobsCatchup},
		{"TestServeSemanticHotReload", testServeSemanticHotReload},
		{"TestServeFunctionsHotReload", testServeFunctionsHotReload},
		{"TestServeFunctionsHotReloadFirstFile", testServeFunctionsHotReloadFirstFile},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			timingSem <- struct{}{}
			defer func() { <-timingSem }()
			tc.fn(t)
		})
	}
}

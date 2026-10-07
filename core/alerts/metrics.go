package alerts

import "github.com/alonsoburon/duct/core/metrics"

var (
	silencedTotal = metrics.Default().Counter("duct_alert_silenced_total", "Notifications suppressed by a silence, by transition state.", "state")
	transitions   = metrics.Default().Counter("duct_alert_transitions_total", "Alert transitions notified, by state and whether every channel confirmed.", "state", "delivered")
)

func observeTransition(rule Rule, t Transition, delivered bool) {
	d := "true"
	if !delivered {
		d = "false"
	}
	transitions.Inc(string(t.State), d)
}

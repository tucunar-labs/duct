package orchestrator

import (
	"testing"
	"time"
)

func TestCronNextFireWithTimezone(t *testing.T) {
	sc, err := ParseSchedule("0 3 * * *", "America/Santiago")
	if err != nil {
		t.Fatal(err)
	}
	// 2026-01-10 is summer in Santiago (UTC-3): 03:00 local = 06:00 UTC.
	got := sc.Next(time.Date(2026, 1, 10, 5, 0, 0, 0, time.UTC))
	if want := time.Date(2026, 1, 10, 6, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("summer next = %s, want %s", got, want)
	}
	// 2026-07-10 is winter (UTC-4): 03:00 local = 07:00 UTC.
	got = sc.Next(time.Date(2026, 7, 10, 5, 0, 0, 0, time.UTC))
	if want := time.Date(2026, 7, 10, 7, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("winter next = %s, want %s", got, want)
	}
}

func TestEveryIsAlignedToTheEpoch(t *testing.T) {
	sc, err := ParseSchedule("@every 10s", "")
	if err != nil {
		t.Fatal(err)
	}
	// Two machines starting at different instants agree on the next tick.
	a := sc.Next(time.Date(2026, 1, 1, 0, 0, 3, 100, time.UTC))
	b := sc.Next(time.Date(2026, 1, 1, 0, 0, 7, 900, time.UTC))
	want := time.Date(2026, 1, 1, 0, 0, 10, 0, time.UTC)
	if !a.Equal(want) || !b.Equal(want) {
		t.Errorf("a=%s b=%s want %s", a, b, want)
	}
	if _, err := ParseSchedule("@every 100ms", ""); err == nil {
		t.Error("sub-second interval must be rejected")
	}
	if _, err := ParseSchedule("CRON_TZ=UTC * * * * *", ""); err == nil {
		t.Error("tz inside the expression must be rejected")
	}
}

func TestAdvanceCoalescesMissedTicks(t *testing.T) {
	sc, _ := ParseSchedule("@every 1m", "")
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	next := sc.Next(t0) // 12:01
	// Not due yet.
	if _, n, due := advance(sc, next, t0.Add(30*time.Second)); due || !n.Equal(next) {
		t.Fatalf("due=%v next=%s", due, n)
	}
	// 5 minutes later (a pause): one tick, the latest, and the cursor moves past now.
	now := t0.Add(5*time.Minute + 10*time.Second)
	tick, n, due := advance(sc, next, now)
	if !due || !tick.Equal(t0.Add(5*time.Minute)) || !n.Equal(t0.Add(6*time.Minute)) {
		t.Fatalf("tick=%s next=%s due=%v", tick, n, due)
	}
}

func TestNextFire(t *testing.T) {
	j := Job{Schedule: "@hourly"}
	n, ok := NextFire(j, time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC))
	if !ok || !n.Equal(time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("%s %v", n, ok)
	}
	if _, ok := NextFire(Job{}, time.Now()); ok {
		t.Error("no schedule, no fire")
	}
}

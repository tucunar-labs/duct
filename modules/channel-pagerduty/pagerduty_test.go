package pagerduty_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/contracttest"
	"github.com/alonsoburon/duct/core/registry"
	pagerduty "github.com/alonsoburon/duct/modules/channel-pagerduty"
)

func TestConformance(t *testing.T) {
	contracttest.TestAlertChannel(t, func(t *testing.T) (contract.AlertChannel, contract.Config) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusAccepted)
		}))
		t.Cleanup(srv.Close)
		return &pagerduty.Channel{}, contract.Config{"url": srv.URL, "routing_key": "rk"}
	})
}

func send(t *testing.T, a contract.Alert) map[string]any {
	t.Helper()
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("%s %s", r.Method, r.Header.Get("Content-Type"))
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	ch := &pagerduty.Channel{}
	if err := ch.Init(context.Background(), contract.Config{"url": srv.URL, "routing_key": "rk-secret", "source": "planta-1"}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Notify(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestTrigger(t *testing.T) {
	got := send(t, contract.Alert{Rule: "hot", Key: "s1", State: contract.AlertFiring, Message: "s1 a 91 C",
		Labels: map[string]string{"severity": "critical", "temp": "91"}, At: time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)})
	if got["routing_key"] != "rk-secret" || got["event_action"] != "trigger" || got["dedup_key"] != "hot:s1" {
		t.Fatalf("event = %v", got)
	}
	p, _ := got["payload"].(map[string]any)
	if p["summary"] != "s1 a 91 C" || p["source"] != "planta-1" || p["severity"] != "critical" || p["timestamp"] != "2026-10-07T12:00:00.000Z" {
		t.Fatalf("payload = %v", p)
	}
	if d, _ := p["custom_details"].(map[string]any); d["temp"] != "91" {
		t.Fatalf("custom_details = %v", p["custom_details"])
	}
}

func TestResolveUsesSameDedupKey(t *testing.T) {
	got := send(t, contract.Alert{Rule: "hot", Key: "s1", State: contract.AlertResolved})
	if got["event_action"] != "resolve" || got["dedup_key"] != "hot:s1" {
		t.Fatalf("event = %v", got)
	}
	if _, has := got["payload"]; has {
		t.Fatalf("resolve must not carry a payload: %v", got)
	}
}

func TestDedupKeyLimitAndGlobalAlert(t *testing.T) {
	if k := pagerduty.DedupKey(contract.Alert{Rule: "r"}); k != "r" {
		t.Fatalf("global alert key = %q", k)
	}
	if k := pagerduty.DedupKey(contract.Alert{Rule: "r", Key: strings.Repeat("x", 400)}); len(k) != 255 {
		t.Fatalf("len = %d", len(k))
	}
}

func TestErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	ch := &pagerduty.Channel{}
	if err := ch.Init(context.Background(), contract.Config{"url": srv.URL, "routing_key": "rk"}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Notify(context.Background(), contract.Alert{Rule: "r"}); err == nil {
		t.Fatal("expected error on 400")
	}
	if err := (&pagerduty.Channel{}).Init(context.Background(), nil); err == nil {
		t.Fatal("expected error without routing_key")
	}
}

func TestRegistered(t *testing.T) {
	if _, ok := registry.Lookup(contract.KindAlertChannel, "pagerduty"); !ok {
		t.Fatal("pagerduty not registered")
	}
}

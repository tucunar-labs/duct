package ntfy_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/contracttest"
	"github.com/alonsoburon/duct/core/registry"
	ntfy "github.com/alonsoburon/duct/modules/channel-ntfy"
)

func TestConformance(t *testing.T) {
	contracttest.TestAlertChannel(t, func(t *testing.T) (contract.AlertChannel, contract.Config) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)
		return &ntfy.Channel{}, contract.Config{"url": srv.URL, "topic": "t"}
	})
}

func publish(t *testing.T, a contract.Alert) (got map[string]any, hdr http.Header) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr = r.Header.Clone()
		if r.Method != http.MethodPost || r.URL.Path != "/" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
	}))
	defer srv.Close()
	ch := &ntfy.Channel{}
	if err := ch.Init(context.Background(), contract.Config{"url": srv.URL + "/", "topic": "incendios", "token": "tk_x"}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Notify(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	return got, hdr
}

func TestFiringCritical(t *testing.T) {
	got, hdr := publish(t, contract.Alert{Rule: "hot", Key: "s1", State: contract.AlertFiring,
		Message: "s1 a 91 °C", Labels: map[string]string{"severity": "critical"}, At: time.Now()})
	if got["topic"] != "incendios" || got["title"] != "FIRING: hot s1" || got["message"] != "s1 a 91 °C" || got["priority"] != float64(5) {
		t.Fatalf("payload = %v", got)
	}
	if tags, _ := got["tags"].([]any); len(tags) != 1 || tags[0] != "rotating_light" {
		t.Fatalf("tags = %v", got["tags"])
	}
	if hdr.Get("Authorization") != "Bearer tk_x" || hdr.Get("Content-Type") != "application/json" {
		t.Fatalf("headers = %v", hdr)
	}
}

func TestResolvedIsLowPriority(t *testing.T) {
	got, _ := publish(t, contract.Alert{Rule: "hot", Key: "s1", State: contract.AlertResolved,
		Labels: map[string]string{"severity": "critical"}})
	if got["title"] != "RESOLVED: hot s1" || got["priority"] != float64(2) || got["message"] != "RESOLVED: hot s1" {
		t.Fatalf("payload = %v", got)
	}
}

func TestNon2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	ch := &ntfy.Channel{}
	if err := ch.Init(context.Background(), contract.Config{"url": srv.URL, "topic": "t"}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Notify(context.Background(), contract.Alert{Rule: "r"}); err == nil {
		t.Fatal("expected error on 403")
	}
}

func TestInitRequiresTopic(t *testing.T) {
	if err := (&ntfy.Channel{}).Init(context.Background(), nil); err == nil {
		t.Fatal("expected error without topic")
	}
}

func TestRegistered(t *testing.T) {
	if _, ok := registry.Lookup(contract.KindAlertChannel, "ntfy"); !ok {
		t.Fatal("ntfy not registered")
	}
}

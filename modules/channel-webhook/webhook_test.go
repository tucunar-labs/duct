package webhook_test

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
	webhook "github.com/alonsoburon/duct/modules/channel-webhook"
)

func TestConformance(t *testing.T) {
	contracttest.TestAlertChannel(t, func(t *testing.T) (contract.AlertChannel, contract.Config) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
		t.Cleanup(srv.Close)
		return &webhook.Channel{}, contract.Config{"url": srv.URL}
	})
}

func TestPostsJSON(t *testing.T) {
	var got map[string]any
	var hdr http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hdr = r.Header.Clone()
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
	}))
	defer srv.Close()

	ch := &webhook.Channel{}
	cfg := contract.Config{"url": srv.URL, "headers": map[string]any{"X-Token": "t"}}
	if err := ch.Init(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	a := contract.Alert{Rule: "hot", Key: "s1", State: contract.AlertFiring, Message: "too hot", At: time.Unix(0, 0).UTC()}
	if err := ch.Notify(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if got["rule"] != "hot" || got["state"] != "firing" || got["key"] != "s1" {
		t.Fatalf("payload = %v", got)
	}
	if hdr.Get("Content-Type") != "application/json" || hdr.Get("X-Token") != "t" {
		t.Fatalf("headers = %v", hdr)
	}
}

func TestNon2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	ch := &webhook.Channel{}
	if err := ch.Init(context.Background(), contract.Config{"url": srv.URL}); err != nil {
		t.Fatal(err)
	}
	if err := ch.Notify(context.Background(), contract.Alert{Rule: "r"}); err == nil {
		t.Fatal("expected error on 500")
	}
}

func TestInitRequiresURL(t *testing.T) {
	if err := (&webhook.Channel{}).Init(context.Background(), nil); err == nil {
		t.Fatal("expected error without url")
	}
}

func TestRegistered(t *testing.T) {
	if _, ok := registry.Lookup(contract.KindAlertChannel, "webhook"); !ok {
		t.Fatal("webhook not registered")
	}
}

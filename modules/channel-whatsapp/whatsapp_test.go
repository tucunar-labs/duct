package whatsapp_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/contracttest"
	"github.com/alonsoburon/duct/core/registry"
	whatsapp "github.com/alonsoburon/duct/modules/channel-whatsapp"
)

func cfg(url string, extra contract.Config) contract.Config {
	c := contract.Config{"api_url": url, "phone_number_id": "1055", "token": "EAAG-secret", "template": "duct_alerta", "to": "56911112222"}
	for k, v := range extra {
		c[k] = v
	}
	return c
}

func TestConformance(t *testing.T) {
	contracttest.TestAlertChannel(t, func(t *testing.T) (contract.AlertChannel, contract.Config) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"messages":[{"id":"wamid.1"}]}`))
		}))
		t.Cleanup(srv.Close)
		return &whatsapp.Channel{}, cfg(srv.URL, nil)
	})
}

func TestTemplateRequest(t *testing.T) {
	var mu sync.Mutex
	var paths, auths []string
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		mu.Lock()
		paths, auths, bodies = append(paths, r.URL.Path), append(auths, r.Header.Get("Authorization")), append(bodies, b)
		mu.Unlock()
	}))
	defer srv.Close()
	ch := &whatsapp.Channel{}
	if err := ch.Init(context.Background(), cfg(srv.URL, contract.Config{"to": []any{"56911112222", "56933334444"}})); err != nil {
		t.Fatal(err)
	}
	a := contract.Alert{Rule: "hot", Key: "s1", State: contract.AlertFiring, Message: "linea 1\nlinea 2   con    espacios", At: time.Now()}
	if err := ch.Notify(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || paths[0] != "/v23.0/1055/messages" || auths[0] != "Bearer EAAG-secret" {
		t.Fatalf("requests = %v %v %v", paths, auths, bodies)
	}
	b := bodies[1]
	tpl, _ := b["template"].(map[string]any)
	if b["messaging_product"] != "whatsapp" || b["type"] != "template" || b["to"] != "56933334444" || tpl["name"] != "duct_alerta" {
		t.Fatalf("body = %v", b)
	}
	if lang, _ := tpl["language"].(map[string]any); lang["code"] != "es" {
		t.Fatalf("language = %v", tpl["language"])
	}
	comp, _ := tpl["components"].([]any)
	ps, _ := comp[0].(map[string]any)["parameters"].([]any)
	var got []string
	for _, p := range ps {
		m := p.(map[string]any)
		if m["type"] != "text" {
			t.Fatalf("param = %v", m)
		}
		got = append(got, m["text"].(string))
	}
	if strings.Join(got, "|") != "FIRING|hot|s1|linea 1 linea 2   con   espacios" {
		t.Fatalf("params = %q", got)
	}
}

func TestCustomParamsAndEmptyValue(t *testing.T) {
	var b map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&b)
	}))
	defer srv.Close()
	ch := &whatsapp.Channel{}
	if err := ch.Init(context.Background(), cfg(srv.URL, contract.Config{"params": []any{"sensor_id", "key"}, "language": "en"})); err != nil {
		t.Fatal(err)
	}
	if err := ch.Notify(context.Background(), contract.Alert{Rule: "r", Labels: map[string]string{"sensor_id": "s9"}}); err != nil {
		t.Fatal(err)
	}
	s, _ := json.Marshal(b)
	if !strings.Contains(string(s), `"text":"s9"`) || !strings.Contains(string(s), `"text":"-"`) || !strings.Contains(string(s), `"code":"en"`) {
		t.Fatalf("body = %s", s)
	}
}

func TestErrorIncludesMetaMessageButNoToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"Template name does not exist","code":132001}}`))
	}))
	defer srv.Close()
	ch := &whatsapp.Channel{}
	if err := ch.Init(context.Background(), cfg(srv.URL, nil)); err != nil {
		t.Fatal(err)
	}
	err := ch.Notify(context.Background(), contract.Alert{Rule: "r"})
	if err == nil || !strings.Contains(err.Error(), "Template name does not exist") || strings.Contains(err.Error(), "EAAG-secret") {
		t.Fatalf("err = %v", err)
	}
}

func TestInitRequiredKeys(t *testing.T) {
	for _, drop := range []string{"phone_number_id", "token", "template", "to"} {
		c := cfg("http://x", nil)
		delete(c, drop)
		if err := (&whatsapp.Channel{}).Init(context.Background(), c); err == nil {
			t.Errorf("Init without %s should fail", drop)
		}
	}
}

func TestRegistered(t *testing.T) {
	if _, ok := registry.Lookup(contract.KindAlertChannel, "whatsapp"); !ok {
		t.Fatal("whatsapp not registered")
	}
}

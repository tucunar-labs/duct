// Package pagerduty is an AlertChannel for the PagerDuty Events API v2:
// a firing alert triggers an event and its resolution resolves it, both with
// dedup_key = rule + key so PagerDuty keeps one incident per alert.
package pagerduty

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
)

const (
	name       = "pagerduty"
	defaultURL = "https://events.pagerduty.com/v2/enqueue"
)

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindAlertChannel,
		Name: name,
		New:  func() contract.Module { return &Channel{} },
	})
}

// Channel implements contract.AlertChannel.
//
// Config keys: "routing_key" (required: the integration key of an Events API
// v2 integration; in duct.toml use routing_key_env = "NAME_OF_ENV_VAR"),
// "source" (default "duct"), "url" (default the public Events API endpoint;
// override for tests), "timeout" (Go duration, default 10s).
type Channel struct {
	url    string
	key    string
	source string
	client *http.Client
}

var _ contract.AlertChannel = (*Channel)(nil)

// Name implements contract.Module.
func (*Channel) Name() string { return name }

// Init implements contract.Module.
func (c *Channel) Init(_ context.Context, cfg contract.Config) error {
	c.key, _ = cfg["routing_key"].(string)
	if c.key == "" {
		return errors.New("pagerduty: config \"routing_key\" is required")
	}
	c.url, _ = cfg["url"].(string)
	if c.url == "" {
		c.url = defaultURL
	}
	c.source, _ = cfg["source"].(string)
	if c.source == "" {
		c.source = "duct"
	}
	timeout := 10 * time.Second
	if s, ok := cfg["timeout"].(string); ok && s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("pagerduty: bad timeout: %w", err)
		}
		timeout = d
	}
	c.client = &http.Client{Timeout: timeout}
	return nil
}

// Close implements contract.Module. It is idempotent.
func (c *Channel) Close() error {
	if c.client != nil {
		c.client.CloseIdleConnections()
	}
	return nil
}

type payload struct {
	Summary       string            `json:"summary"`
	Source        string            `json:"source"`
	Severity      string            `json:"severity"`
	Timestamp     string            `json:"timestamp,omitempty"`
	Component     string            `json:"component,omitempty"`
	CustomDetails map[string]string `json:"custom_details,omitempty"`
}

type event struct {
	RoutingKey  string   `json:"routing_key"`
	EventAction string   `json:"event_action"`
	DedupKey    string   `json:"dedup_key"`
	Payload     *payload `json:"payload,omitempty"` // required by PagerDuty only for trigger
}

// DedupKey identifies the incident of an alert: rule and key, at most 255
// characters (PagerDuty's limit).
func DedupKey(a contract.Alert) string {
	k := a.Rule
	if a.Key != "" {
		k += ":" + a.Key
	}
	if len(k) > 255 {
		k = k[:255]
	}
	return k
}

func severity(s string) string {
	switch s {
	case "critical", "error", "warning", "info":
		return s
	}
	return "warning"
}

func build(routingKey, source string, a contract.Alert) event {
	e := event{RoutingKey: routingKey, DedupKey: DedupKey(a)}
	if a.State == contract.AlertResolved {
		e.EventAction = "resolve"
		return e
	}
	summary := a.Message
	if summary == "" {
		summary = a.Rule + " " + a.Key
	}
	if len(summary) > 1024 {
		summary = summary[:1024]
	}
	at := a.At
	if at.IsZero() {
		at = time.Now()
	}
	e.EventAction = "trigger"
	e.Payload = &payload{Summary: summary, Source: source, Severity: severity(a.Labels["severity"]),
		Timestamp: at.UTC().Format("2006-01-02T15:04:05.000Z"), Component: a.Rule, CustomDetails: a.Labels}
	return e
}

// Notify implements contract.AlertChannel. PagerDuty answers 202 Accepted;
// any non-2xx response is an error.
func (c *Channel) Notify(ctx context.Context, a contract.Alert) error {
	if c.client == nil {
		return errors.New("pagerduty: not initialised")
	}
	body, err := json.Marshal(build(c.key, c.source, a))
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("pagerduty: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("pagerduty: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("pagerduty: unexpected status %s", resp.Status)
	}
	return nil
}

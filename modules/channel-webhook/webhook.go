// Package webhook is an AlertChannel that POSTs each alert as JSON to a URL.
// It works with Slack, Teams, Discord or any HTTP endpoint that accepts JSON.
package webhook

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

const name = "webhook"

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindAlertChannel,
		Name: name,
		New:  func() contract.Module { return &Channel{} },
	})
}

// Channel implements contract.AlertChannel.
//
// Config keys: "url" (required, string), "timeout" (optional, Go duration
// string, default 10s), "headers" (optional map of extra request headers).
type Channel struct {
	url     string
	headers map[string]string
	client  *http.Client
}

var _ contract.AlertChannel = (*Channel)(nil)

// Name implements contract.Module.
func (*Channel) Name() string { return name }

// Init implements contract.Module.
func (c *Channel) Init(_ context.Context, cfg contract.Config) error {
	url, _ := cfg["url"].(string)
	if url == "" {
		return errors.New("webhook: config \"url\" is required")
	}
	timeout := 10 * time.Second
	if s, ok := cfg["timeout"].(string); ok && s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("webhook: bad timeout: %w", err)
		}
		timeout = d
	}
	c.headers = map[string]string{}
	switch h := cfg["headers"].(type) {
	case map[string]string:
		for k, v := range h {
			c.headers[k] = v
		}
	case map[string]any:
		for k, v := range h {
			if s, ok := v.(string); ok {
				c.headers[k] = s
			}
		}
	}
	c.url = url
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
	Rule    string              `json:"rule"`
	Key     string              `json:"key,omitempty"`
	State   contract.AlertState `json:"state"`
	Message string              `json:"message,omitempty"`
	Labels  map[string]string   `json:"labels,omitempty"`
	At      time.Time           `json:"at"`
}

// Notify implements contract.AlertChannel. Any non-2xx response is an error.
func (c *Channel) Notify(ctx context.Context, a contract.Alert) error {
	if c.client == nil {
		return errors.New("webhook: not initialised")
	}
	body, err := json.Marshal(payload(a))
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook: unexpected status %s", resp.Status)
	}
	return nil
}

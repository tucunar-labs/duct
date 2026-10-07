// Package ntfy is an AlertChannel that publishes each alert to an ntfy topic
// (push to Android/iOS, email through the server's SMTP).
package ntfy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
)

const name = "ntfy"

func init() {
	registry.Register(registry.Info{
		Kind: contract.KindAlertChannel,
		Name: name,
		New:  func() contract.Module { return &Channel{} },
	})
}

// Channel implements contract.AlertChannel.
//
// Config keys: "topic" (required), "url" (server, default https://ntfy.sh),
// "token" (optional access token, sent as a Bearer; put it in duct.toml as
// token_env = "NAME_OF_ENV_VAR"), "timeout" (Go duration, default 10s).
type Channel struct {
	url    string
	topic  string
	token  string
	client *http.Client
}

var _ contract.AlertChannel = (*Channel)(nil)

// Name implements contract.Module.
func (*Channel) Name() string { return name }

// Init implements contract.Module.
func (c *Channel) Init(_ context.Context, cfg contract.Config) error {
	c.topic, _ = cfg["topic"].(string)
	if c.topic == "" {
		return errors.New("ntfy: config \"topic\" is required")
	}
	c.url, _ = cfg["url"].(string)
	if c.url == "" {
		c.url = "https://ntfy.sh"
	}
	c.url = strings.TrimRight(c.url, "/")
	c.token, _ = cfg["token"].(string)
	timeout := 10 * time.Second
	if s, ok := cfg["timeout"].(string); ok && s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("ntfy: bad timeout: %w", err)
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

// message is ntfy's JSON publish format (POST to the server root).
type message struct {
	Topic    string   `json:"topic"`
	Title    string   `json:"title"`
	Message  string   `json:"message"`
	Priority int      `json:"priority"`
	Tags     []string `json:"tags"`
}

// build maps an alert to a ntfy message: a firing alert's priority follows
// its severity (critical 5 "max", error 4, warning 3, info 2) and a resolved
// one is always low (2); the tag is an emoji shortcode.
func build(topic string, a contract.Alert) message {
	sev := a.Labels["severity"]
	m := message{Topic: topic, Message: a.Message}
	label := "FIRING"
	if a.State == contract.AlertResolved {
		label = "RESOLVED"
		m.Priority, m.Tags = 2, []string{"white_check_mark"}
	} else {
		switch sev {
		case "critical":
			m.Priority, m.Tags = 5, []string{"rotating_light"}
		case "error":
			m.Priority, m.Tags = 4, []string{"x"}
		case "info":
			m.Priority, m.Tags = 2, []string{"information_source"}
		default:
			m.Priority, m.Tags = 3, []string{"warning"}
		}
	}
	m.Title = label + ": " + a.Rule
	if a.Key != "" {
		m.Title += " " + a.Key
	}
	if m.Message == "" {
		m.Message = m.Title
	}
	return m
}

// Notify implements contract.AlertChannel. Any non-2xx response is an error.
func (c *Channel) Notify(ctx context.Context, a contract.Alert) error {
	if c.client == nil {
		return errors.New("ntfy: not initialised")
	}
	body, err := json.Marshal(build(c.topic, a))
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("ntfy: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("ntfy: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("ntfy: unexpected status %s", resp.Status)
	}
	return nil
}

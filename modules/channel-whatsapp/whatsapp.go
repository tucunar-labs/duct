// Package whatsapp is an AlertChannel that sends each alert as a template
// message through the WhatsApp Business Cloud API (Meta). Templates must be of
// the "utility" category and approved beforehand; alerts are never sent as
// free text, which Meta only allows inside a 24-hour customer window.
package whatsapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
)

const (
	name           = "whatsapp"
	defaultAPIURL  = "https://graph.facebook.com"
	defaultVersion = "v23.0"
	maxParamLen    = 1024
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
// Config keys:
//   - "phone_number_id" (required): the WhatsApp Business phone number id.
//   - "token" (required): a permanent access token; in duct.toml use
//     token_env = "NAME_OF_ENV_VAR", never the value.
//   - "to" (required): recipient number(s) in international format without "+"
//     (string or list); one message per recipient.
//   - "template" (required): name of the approved utility template.
//   - "language" (default "es"): template language code.
//   - "params" (default state, rule, key, message): which alert fields fill the
//     template's body variables {{1}}, {{2}}...; any of state, rule, key,
//     message, severity, at, or a label name.
//   - "api_url" (default https://graph.facebook.com), "api_version" (default
//     v23.0), "timeout" (default 10s).
type Channel struct {
	endpoint string
	token    string
	to       []string
	template string
	language string
	params   []string
	client   *http.Client
}

var _ contract.AlertChannel = (*Channel)(nil)

// Name implements contract.Module.
func (*Channel) Name() string { return name }

func str(cfg contract.Config, key, def string) string {
	if s, ok := cfg[key].(string); ok && s != "" {
		return s
	}
	return def
}

func strList(v any) []string {
	switch x := v.(type) {
	case string:
		if x != "" {
			return []string{x}
		}
	case []string:
		return x
	case []any:
		var out []string
		for _, e := range x {
			if s, ok := e.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// Init implements contract.Module.
func (c *Channel) Init(_ context.Context, cfg contract.Config) error {
	phone := str(cfg, "phone_number_id", "")
	c.token = str(cfg, "token", "")
	c.template = str(cfg, "template", "")
	c.to = strList(cfg["to"])
	switch {
	case phone == "":
		return errors.New("whatsapp: config \"phone_number_id\" is required")
	case c.token == "":
		return errors.New("whatsapp: config \"token\" is required (token_env in duct.toml)")
	case c.template == "":
		return errors.New("whatsapp: config \"template\" is required (an approved utility template)")
	case len(c.to) == 0:
		return errors.New("whatsapp: config \"to\" is required")
	}
	c.language = str(cfg, "language", "es")
	c.params = strList(cfg["params"])
	if len(c.params) == 0 {
		c.params = []string{"state", "rule", "key", "message"}
	}
	base := strings.TrimRight(str(cfg, "api_url", defaultAPIURL), "/")
	c.endpoint = base + "/" + str(cfg, "api_version", defaultVersion) + "/" + url.PathEscape(phone) + "/messages"
	timeout := 10 * time.Second
	if s := str(cfg, "timeout", ""); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("whatsapp: bad timeout: %w", err)
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

// clean makes a value acceptable as a template variable: Meta rejects
// newlines, tabs and runs of four spaces, and long texts.
func clean(s string) string {
	s = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ").Replace(s)
	for strings.Contains(s, "    ") {
		s = strings.ReplaceAll(s, "    ", "   ")
	}
	s = strings.TrimSpace(s)
	if len(s) > maxParamLen {
		s = strings.ToValidUTF8(s[:maxParamLen], "")
	}
	if s == "" {
		s = "-" // an empty variable is rejected
	}
	return s
}

func (c *Channel) value(p string, a contract.Alert) string {
	switch p {
	case "state":
		return strings.ToUpper(string(a.State))
	case "rule":
		return a.Rule
	case "key":
		return a.Key
	case "message":
		return a.Message
	case "severity":
		return a.Labels["severity"]
	case "at":
		return a.At.UTC().Format(time.RFC3339)
	}
	return a.Labels[p]
}

type param struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func (c *Channel) body(to string, a contract.Alert) ([]byte, error) {
	ps := make([]param, len(c.params))
	for i, p := range c.params {
		ps[i] = param{Type: "text", Text: clean(c.value(p, a))}
	}
	return json.Marshal(map[string]any{
		"messaging_product": "whatsapp",
		"to":                to,
		"type":              "template",
		"template": map[string]any{
			"name":       c.template,
			"language":   map[string]string{"code": c.language},
			"components": []any{map[string]any{"type": "body", "parameters": ps}},
		},
	})
}

// Notify implements contract.AlertChannel: one template message per
// recipient; the error lists the recipients that failed. Any non-2xx
// response is a failure.
func (c *Channel) Notify(ctx context.Context, a contract.Alert) error {
	if c.client == nil {
		return errors.New("whatsapp: not initialised")
	}
	var errs []error
	for _, to := range c.to {
		if err := c.send(ctx, to, a); err != nil {
			errs = append(errs, fmt.Errorf("to %s: %w", to, err))
		}
	}
	return errors.Join(errs...)
}

func (c *Channel) send(ctx context.Context, to string, a contract.Alert) error {
	body, err := c.body(to, a)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("whatsapp: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("whatsapp: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("whatsapp: unexpected status %s: %s", resp.Status, errorMessage(detail))
	}
	return nil
}

// errorMessage extracts error.message from a Graph API error body.
func errorMessage(b []byte) string {
	var e struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(b, &e) == nil && e.Error.Message != "" {
		return e.Error.Message
	}
	return "no detail"
}

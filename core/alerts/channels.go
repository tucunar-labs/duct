package alerts

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/registry"
)

// Channels are the initialised AlertChannel modules of a project, keyed by
// the name of their [alerts.channels.<name>] table.
type Channels struct {
	byName map[string]contract.AlertChannel
	types  map[string]string
	// Retry waits between delivery attempts (default 200 ms, 1 s: 3 tries).
	Retry []time.Duration
}

// ResolveEnv returns cfg with every "<key>_env" entry (the name of an
// environment variable) replaced by "<key>" holding its value, so secrets
// stay out of duct.toml. A named variable that is unset or empty is an error.
func ResolveEnv(cfg map[string]any, getenv func(string) string) (map[string]any, error) {
	out := make(map[string]any, len(cfg))
	for k, v := range cfg {
		name, isEnv := strings.CutSuffix(k, "_env")
		if !isEnv || name == "" {
			out[k] = v
			continue
		}
		varName, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("%s must be the name of an environment variable", k)
		}
		val := getenv(varName)
		if val == "" {
			return nil, fmt.Errorf("%s: environment variable %s is not set", k, varName)
		}
		out[name] = val
	}
	return out, nil
}

// OpenChannels instantiates and Inits one module per table of cfg (each with
// a "type" naming an alert.channel module). Close releases them.
func OpenChannels(ctx context.Context, cfg map[string]map[string]any) (*Channels, error) {
	c := &Channels{byName: map[string]contract.AlertChannel{}, types: map[string]string{},
		Retry: []time.Duration{200 * time.Millisecond, time.Second}}
	names := make([]string, 0, len(cfg))
	for n := range cfg {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		raw := cfg[name]
		typ, _ := raw["type"].(string)
		if typ == "" {
			_ = c.Close()
			return nil, fmt.Errorf("[alerts.channels.%s]: type is required", name)
		}
		info, ok := registry.Lookup(contract.KindAlertChannel, typ)
		if !ok {
			_ = c.Close()
			return nil, fmt.Errorf("[alerts.channels.%s]: channel type %q is not linked into this binary (see `duct modules`)", name, typ)
		}
		mod, ok := info.New().(contract.AlertChannel)
		if !ok {
			_ = c.Close()
			return nil, fmt.Errorf("module alert.channel/%s does not implement contract.AlertChannel", typ)
		}
		m := make(map[string]any, len(raw))
		for k, v := range raw {
			if k != "type" {
				m[k] = v
			}
		}
		m, err := ResolveEnv(m, os.Getenv)
		if err == nil {
			err = mod.Init(ctx, contract.Config(m))
		}
		if err != nil {
			_ = mod.Close()
			_ = c.Close()
			return nil, fmt.Errorf("[alerts.channels.%s]: %w", name, err)
		}
		c.byName[name], c.types[name] = mod, typ
	}
	return c, nil
}

// Check reports whether every reference of the rules points to a configured
// channel of the right type.
func (c *Channels) Check(rules []Rule) error {
	var errs []error
	for _, r := range rules {
		for _, ref := range r.Channels {
			typ, ok := c.types[ref.Name]
			switch {
			case !ok:
				errs = append(errs, fmt.Errorf("rule %s: channel %q is not configured ([alerts.channels.%s] in duct.toml)", r.Name, ref, ref.Name))
			case ref.Type != "" && ref.Type != typ:
				errs = append(errs, fmt.Errorf("rule %s: channel %q is of type %q, not %q", r.Name, ref.Name, typ, ref.Type))
			}
		}
	}
	return errors.Join(errs...)
}

// Close closes every module.
func (c *Channels) Close() error {
	var errs []error
	for _, m := range c.byName {
		errs = append(errs, m.Close())
	}
	return errors.Join(errs...)
}

// Delivery is the outcome of notifying one channel.
type Delivery struct {
	Channel  string
	Attempts int
	Err      error
}

// Send notifies every channel concurrently, retrying each on failure.
func (c *Channels) Send(ctx context.Context, refs []ChannelRef, a contract.Alert) []Delivery {
	out := make([]Delivery, len(refs))
	var wg sync.WaitGroup
	for i, ref := range refs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := Delivery{Channel: ref.String()}
			ch, ok := c.byName[ref.Name]
			if !ok {
				d.Err = fmt.Errorf("channel %q is not configured", ref.Name)
				out[i] = d
				return
			}
			for {
				d.Attempts++
				d.Err = ch.Notify(ctx, a)
				if d.Err == nil || d.Attempts > len(c.Retry) || ctx.Err() != nil {
					break
				}
				t := time.NewTimer(c.Retry[d.Attempts-1])
				select {
				case <-ctx.Done():
				case <-t.C:
				}
				t.Stop()
			}
			out[i] = d
		}()
	}
	wg.Wait()
	return out
}

// urlRe hides URLs in recorded errors: webhook URLs (Slack, Teams) are secrets.
var urlRe = regexp.MustCompile(`https?://[^\s"]+`)

// Summary is the compact text stored in bronze.duct_alerts.channels.
func Summary(ds []Delivery) (text string, allOK bool) {
	allOK = true
	parts := make([]string, 0, len(ds))
	for _, d := range ds {
		if d.Err != nil {
			allOK = false
			parts = append(parts, fmt.Sprintf("%s: error after %d tries: %v", d.Channel, d.Attempts, urlRe.ReplaceAllString(d.Err.Error(), "<url>")))
			continue
		}
		parts = append(parts, d.Channel+": ok")
	}
	return strings.Join(parts, "; "), allOK
}

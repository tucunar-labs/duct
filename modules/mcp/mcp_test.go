package mcp

import (
	"context"
	"strings"
	"testing"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

func TestInitRefusals(t *testing.T) {
	cases := map[string]struct {
		cfg  contract.Config
		want string
	}{
		"remote without TLS": {contract.Config{"listen": "0.0.0.0:8765", "sql": true}, "loopback"},
		"half TLS":           {contract.Config{"tls_cert": "x"}, "together"},
		"nothing to serve":   {contract.Config{"listen": "127.0.0.1:0"}, "nothing to serve"},
		"bad model dir":      {contract.Config{"listen": "127.0.0.1:0", "semantic_dir": t.TempDir()}, "nothing to serve"},
	}
	for name, c := range cases {
		s := &Server{}
		err := s.Init(context.Background(), c.cfg)
		_ = s.Close()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %q", name, err, c.want)
		}
	}
}

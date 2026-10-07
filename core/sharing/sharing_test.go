package sharing

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

func TestSignerExpiryAndTampering(t *testing.T) {
	s, err := NewSigner([]byte("0123456789abcdef0123"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewSigner([]byte("short")); err == nil {
		t.Error("short key accepted")
	}
	now := time.Now()
	exp, sig := s.Sign("a/b/c.parquet", now.Add(time.Minute))
	if err := s.Verify("a/b/c.parquet", exp, sig, now); err != nil {
		t.Errorf("valid URL: %v", err)
	}
	if err := s.Verify("a/b/c.parquet", exp, sig, now.Add(2*time.Minute)); err == nil {
		t.Error("expired URL accepted")
	}
	if err := s.Verify("a/b/other.parquet", exp, sig, now); err == nil {
		t.Error("signature of another key accepted")
	}
	if err := s.Verify("a/b/c.parquet", "99999999999", sig, now); err == nil {
		t.Error("extended expiry accepted")
	}
	if err := s.Verify("a/b/c.parquet", "x", sig, now); err == nil {
		t.Error("non-numeric expiry accepted")
	}
}

func TestKeysCannotEscape(t *testing.T) {
	for _, k := range []string{"", "/etc/passwd", "a/../b", "../x", "a//b", "a/./b", `a\b`, "a/\x00"} {
		if validKey(k) == nil {
			t.Errorf("key %q accepted", k)
		}
		if _, ok := KeyFromPath(k); ok && k != "" && !strings.HasPrefix(k, "/") {
			t.Errorf("KeyFromPath accepted %q", k)
		}
	}
	if validKey("ventas/acme/sales/v3/part-00000.parquet") != nil {
		t.Error("normal key rejected")
	}
}

func TestLocalStorage(t *testing.T) {
	ctx := context.Background()
	root := filepath.Join(t.TempDir(), "shares")
	st, err := OpenStorage(StorageSpec{Location: root, BaseURL: "http://h:1/delta-sharing", SigningKey: bytes.Repeat([]byte{1}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"s/r/t/v1/part-00000.parquet", "s/r/t/v2/part-00000.parquet"} {
		if err := st.Put(ctx, k, strings.NewReader("data"), 4); err != nil {
			t.Fatal(err)
		}
	}
	objs, err := st.List(ctx, "s/r/t/v1/")
	if err != nil || len(objs) != 1 || objs[0].Size != 4 {
		t.Fatalf("list = %v %v", objs, err)
	}
	u, err := st.URL(ctx, "s/r/t/v1/part-00000.parquet", time.Minute)
	if err != nil || !strings.HasPrefix(u, "http://h:1/delta-sharing/_files/s/r/t/v1/part-00000.parquet?exp=") || !strings.Contains(u, "&sig=") {
		t.Fatalf("url = %q %v", u, err)
	}
	if err := DeletePrefix(ctx, st, "s/r/t/v1"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "s", "r", "t", "v1")); err == nil {
		t.Error("empty folder left behind")
	}
	if objs, _ = st.List(ctx, ""); len(objs) != 1 {
		t.Errorf("after delete: %v", objs)
	}
	if err := st.Put(ctx, "../escape.parquet", strings.NewReader("x"), 1); err == nil {
		t.Error("Put outside the root accepted")
	}
}

func TestOverlaps(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want bool
	}{
		{"/p/lake/shares", "/p/lake", true},
		{"/p/lake", "/p/lake/shares", true},
		{"/p/shares", "/p/lake", false},
		{"/p/lake2", "/p/lake", false},
		{"s3://b/proj.shares", "s3://b/proj", false},
		{"s3://b/proj/shares", "s3://b/proj", true},
		{"s3://b/x", "s3://b", true},
	} {
		if got := Overlaps(tc.a, tc.b); got != tc.want {
			t.Errorf("Overlaps(%q, %q) = %v", tc.a, tc.b, got)
		}
	}
}

func TestPlanColumnsAndSchema(t *testing.T) {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "n", Type: arrow.PrimitiveTypes.Int8},
		{Name: "u", Type: arrow.PrimitiveTypes.Uint64},
		{Name: "price", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}},
		{Name: "t", Type: arrow.FixedWidthTypes.Time64us},
		{Name: "ts", Type: &arrow.TimestampType{Unit: arrow.Microsecond}},
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)
	cols := planColumns(sc)
	want := []string{"long", "byte", "decimal(20,0)", "decimal(10,2)", "string", "timestamp", "string"}
	for i, c := range cols {
		if c.Type != want[i] {
			t.Errorf("column %s: %s, want %s", c.Name, c.Type, want[i])
		}
	}
	if cols[2].Cast != "DECIMAL(20,0)" || cols[4].Cast != "VARCHAR" || cols[0].Cast != "" {
		t.Errorf("casts = %+v", cols)
	}
	if got := selectList(cols[:1]); got != `"id"` {
		t.Errorf("selectList = %s", got)
	}
	if got := selectList(cols[4:5]); got != `CAST("t" AS VARCHAR) AS "t"` {
		t.Errorf("selectList = %s", got)
	}
	var schema struct {
		Type   string `json:"type"`
		Fields []struct {
			Name, Type string
			Nullable   bool
		} `json:"fields"`
	}
	if err := json.Unmarshal([]byte(schemaString(cols)), &schema); err != nil || schema.Type != "struct" || len(schema.Fields) != 7 || schema.Fields[3].Type != "decimal(10,2)" {
		t.Errorf("schemaString: %v %+v", err, schema)
	}
}

func TestRecipientOf(t *testing.T) {
	if n, ok := RecipientOf(contract.Identity{User: "share_acme", Role: "share_acme"}); !ok || n != "acme" {
		t.Errorf("recipient = %q %v", n, ok)
	}
	for _, id := range []contract.Identity{
		{User: "ana", Role: "share_acme"},       // a user that merely holds the role
		{User: "share_acme", Role: "analistas"}, // not a recipient role
		{User: "share_", Role: "share_"},
		{User: "share_ACME", Role: "share_ACME"},
	} {
		if n, ok := RecipientOf(id); ok {
			t.Errorf("%+v accepted as recipient %q", id, n)
		}
	}
}

func TestProfile(t *testing.T) {
	p := NewProfile("https://h/delta-sharing", Issued{Token: "dct_x", ExpiresAt: time.Date(2027, 1, 2, 3, 4, 5, 0, time.UTC)})
	var m map[string]any
	if err := json.Unmarshal(p.JSON(), &m); err != nil {
		t.Fatal(err)
	}
	if m["shareCredentialsVersion"] != float64(1) || m["bearerToken"] != "dct_x" || m["expirationTime"] != "2027-01-02T03:04:05Z" || m["endpoint"] != "https://h/delta-sharing" {
		t.Errorf("profile = %v", m)
	}
	path := filepath.Join(t.TempDir(), "x.share")
	if err := p.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode = %v", fi.Mode().Perm())
	}
	if err := p.WriteFile(path); err == nil {
		t.Error("existing profile overwritten")
	}
}

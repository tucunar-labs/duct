package governance

import "testing"

func TestPasswordHash(t *testing.T) {
	h, err := HashPassword("s3cret")
	if err != nil {
		t.Fatal(err)
	}
	if !VerifyPassword(h, "s3cret") || VerifyPassword(h, "other") || VerifyPassword("junk", "s3cret") {
		t.Error("verify mismatch")
	}
	h2, _ := HashPassword("s3cret")
	if h == h2 {
		t.Error("salt must differ")
	}
	if _, err := HashPassword(""); err == nil {
		t.Error("empty password accepted")
	}
}

func TestNormalizeRelation(t *testing.T) {
	if s, tb, err := NormalizeRelation("Gold.Revenue"); err != nil || s != "gold" || tb != "revenue" {
		t.Errorf("%s %s %v", s, tb, err)
	}
	if s, _, _ := NormalizeRelation("t"); s != "main" {
		t.Error("bare name must be main")
	}
	for _, bad := range []string{"a.b.c", "role_x.t", "a;b", "x.\"y\""} {
		if _, _, err := NormalizeRelation(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

package main

import (
	"os/exec"
	"strings"
	"testing"
)

// TestSDKTransports drives duct.connect() from the Python SDK against a real duct serve:
// HTTP ArrowStream and HTTP JSON (token), and the Postgres wire through psycopg (password),
// all as the governed user (row filter and mask applied).
func TestSDKTransports(t *testing.T) {
	py := pythonEnv(t)
	root := governedProject(t)
	srv := startServe(t, root, nil)
	tok := strings.TrimSpace(mustDuct(t, "token", "create", "--user", "ana", "--project", root))

	script := `
import os, sys
import duct

q = "SELECT name, rut, amt FROM customers ORDER BY amt"
want = [("Ana", "***", 100), ("Cami", "***", 300)]

with duct.connect() as con:                      # DUCT_URL / DUCT_TOKEN, ArrowStream
    res = con.sql(q)
    assert type(con).__name__ == "HTTPConnection" and con.format == "arrow"
    assert [(r["name"], r["rut"], r["amt"]) for r in res.rows] == want, res.rows
    assert res.to_arrow().num_rows == 2
    assert res.to_pandas()["amt"].tolist() == [100, 300]
    assert res.to_polars()["name"].to_list() == ["Ana", "Cami"]
    assert len(con.sql("SELECT name FROM customers WHERE amt > 1000")) == 0
    try:
        con.sql("SELECT * FROM main.secret")
        raise SystemExit("main.secret must be refused")
    except duct.DuctError as e:
        assert "403" in str(e), e

with duct.connect(os.environ["DUCT_URL"], os.environ["DUCT_TOKEN"], format="json") as con:
    assert [(r["name"], r["amt"]) for r in con.sql(q).rows] == [("Ana", 100), ("Cami", 300)]

with duct.connect(os.environ["PG_URL"]) as con:  # Postgres wire
    assert type(con).__name__ == "PostgresConnection"
    res = con.sql(q)
    assert [(r["name"], r["rut"], r["amt"]) for r in res.rows] == want, res.rows
    assert res.to_arrow().num_rows == 2
    try:
        con.sql("SELECT * FROM main.secret")
        raise SystemExit("pg main.secret must be refused")
    except duct.DuctError:
        pass
    assert con.sql("SELECT count(*) AS n FROM customers").rows[0]["n"] == 2   # still usable

with duct.connect("postgresql://ana@" + os.environ["PG_ADDR"] + "/duct?sslmode=disable", "pw-ana-123") as con:  # token= as password
    assert len(con.sql(q)) == 2
try:
    duct.connect("postgresql://ana:wrong@" + os.environ["PG_ADDR"] + "/duct?sslmode=disable")
    raise SystemExit("bad password must fail")
except duct.DuctError:
    pass
print("SDK OK")
`
	cmd := exec.Command(py, "-c", script)
	cmd.Env = append(cmd.Environ(),
		"DUCT_URL=http://"+srv.http, "DUCT_TOKEN="+tok,
		"PG_URL=postgresql://ana:pw-ana-123@"+srv.pg+"/duct?sslmode=disable", "PG_ADDR="+srv.pg)
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "SDK OK") {
		t.Fatalf("python: %v\n%s", err, out)
	}
}

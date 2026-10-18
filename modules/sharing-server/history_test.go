package sharingserver_test

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/sharing"
)

// tableRows reads the files of a query response into "id|country|email|amount" strings.
func (f *fixture) tableRows(files []map[string]any) map[string]string {
	f.t.Helper()
	out := map[string]string{}
	for _, fl := range files {
		for _, r := range f.rowsOf(fl["url"].(string)) {
			out[r[0]] = strings.Join(r, "|")
		}
	}
	return out
}

type change struct {
	kind    string // add | cdf
	version int64
	ts      int64
	url     string
}

// changes calls GET .../changes and returns the actions after protocol/metaData.
func (f *fixture) changes(token, params string) (int, string, []change) {
	f.t.Helper()
	resp, b := f.do("GET", "/shares/ventas/schemas/gold/tables/sales/changes?"+params, token, "")
	if resp.StatusCode != 200 {
		return resp.StatusCode, b, nil
	}
	lines := ndjson(f.t, b)
	if len(lines) < 2 || lines[0]["protocol"] == nil || lines[1]["metaData"] == nil {
		f.t.Fatalf("changes must start with protocol and metaData: %s", b)
	}
	var out []change
	for _, l := range lines[2:] {
		for kind, v := range l {
			m := v.(map[string]any)
			out = append(out, change{kind: kind, version: int64(m["version"].(float64)), ts: int64(m["timestamp"].(float64)), url: m["url"].(string)})
		}
	}
	return 200, b, out
}

// feedRows reads the rows of a change file as "type|id|country|email|amount".
func (f *fixture) feedRows(c change) []string {
	f.t.Helper()
	resp, err := http.Get(c.url)
	if err != nil {
		f.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	tmp := filepath.Join(f.t.TempDir(), "c.parquet")
	b := make([]byte, 0, 1<<16)
	buf := make([]byte, 1<<16)
	for {
		n, err := resp.Body.Read(buf)
		b = append(b, buf[:n]...)
		if err != nil {
			break
		}
	}
	if resp.StatusCode != 200 {
		f.t.Fatalf("download: %d %s", resp.StatusCode, b)
	}
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		f.t.Fatal(err)
	}
	col := "_change_type"
	if c.kind == "add" {
		col = "'insert'"
	}
	st, err := f.eng.Query(context.Background(), "SELECT "+col+", id, country, email, amount FROM read_parquet('"+tmp+"') ORDER BY id, 1")
	if err != nil {
		f.t.Fatal(err)
	}
	defer st.Release()
	var out []string
	for st.Next() {
		rec := st.RecordBatch()
		for i := range int(rec.NumRows()) {
			var p []string
			for c := range int(rec.NumCols()) {
				p = append(p, rec.Column(c).ValueStr(i))
			}
			out = append(out, strings.Join(p, "|"))
		}
	}
	return out
}

func (f *fixture) enableHistory() {
	f.t.Helper()
	if err := f.st.SetHistory(context.Background(), "ventas", "sales", true); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) versionOf() int64 {
	f.t.Helper()
	v, _ := f.query(f.token, "")
	return v
}

// TestTimeTravelAndChangeFeed: versions, timestamps and the feed of a history
// table, with the recipient's filter and masks; replaying the feed over the
// previous version must give the next one.
func TestTimeTravelAndChangeFeed(t *testing.T) {
	f := newFixture(t, contract.Config{"keep_versions": 3})
	f.enableHistory()

	v1, files1 := f.query(f.token, "")
	if len(f.tableRows(files1)) != 5 {
		t.Fatalf("v1 rows")
	}
	// A request for the feed before anything changed: version 1 is the first, all inserts.
	status, body, ch := f.changes(f.token, "startingVersion=1")
	if status != 200 || len(ch) != 1 || ch[0].kind != "add" || ch[0].version != v1 {
		t.Fatalf("initial feed: %d %s", status, body)
	}
	if rows := f.feedRows(ch[0]); len(rows) != 5 {
		t.Errorf("initial feed rows = %v", rows)
	}

	time.Sleep(50 * time.Millisecond)
	// Version 2: one visible insert, one invisible (pe) insert.
	f.sql(`INSERT INTO gold.sales VALUES (12, 'cl', 'n@x.com', 1.0), (13, 'pe', 'm@x.com', 2.0)`)
	v2, files2 := f.query(f.token, "")
	if v2 != v1+1 {
		t.Fatalf("v2 = %d", v2)
	}
	time.Sleep(50 * time.Millisecond)
	// Version 3, several snapshots: a row leaves the filter (2), another enters
	// it (3), one is updated inside it (4), a masked-only change (6), a delete
	// (8), a row inserted and deleted again (14).
	f.sql(`UPDATE gold.sales SET country = 'pe' WHERE id = 2`)
	f.sql(`UPDATE gold.sales SET country = 'cl' WHERE id = 3`)
	f.sql(`UPDATE gold.sales SET amount = 99.0 WHERE id = 4`)
	f.sql(`UPDATE gold.sales SET email = 'other@x.com' WHERE id = 6`)
	f.sql(`DELETE FROM gold.sales WHERE id = 8`)
	f.sql(`INSERT INTO gold.sales VALUES (14, 'cl', 'g@x.com', 3.0)`)
	f.sql(`DELETE FROM gold.sales WHERE id = 14`)
	v3, files3 := f.query(f.token, "")
	if v3 != v2+1 {
		t.Fatalf("v3 = %d", v3)
	}

	status, body, ch = f.changes(f.token, "startingVersion=2&endingVersion=3")
	if status != 200 {
		t.Fatalf("feed: %d %s", status, body)
	}
	var got []string
	for _, c := range ch {
		if c.kind != "cdf" || c.ts == 0 {
			t.Errorf("unexpected action %+v", c)
		}
		for _, r := range f.feedRows(c) {
			got = append(got, fmt.Sprintf("v%d|%s", c.version, r))
		}
	}
	sort.Strings(got)
	text := strings.Join(got, "\n")
	// v3: 2 leaves (delete with the values the recipient last saw: country cl),
	// 3 enters (insert), 4 is updated, 8 is deleted; 6 (masked-only) and 14
	// (inserted and deleted in the same version) do not appear.
	for _, w := range []string{
		"v2|insert|12|cl|***|1",
		"v3|delete|2|cl|***|3",
		"v3|insert|3|cl|***|4.5",
		"v3|update_preimage|4|cl|***|6",
		"v3|update_postimage|4|cl|***|99",
		"v3|delete|8|cl|***|12",
	} {
		if !strings.Contains(text, w) {
			t.Errorf("feed lacks %q:\n%s", w, text)
		}
	}
	if len(got) != 6 {
		t.Errorf("feed has %d rows, want 6:\n%s", len(got), text)
	}
	if strings.Contains(text, "|6|") || strings.Contains(text, "|13|") || strings.Contains(text, "|14|") || strings.Contains(text, "|pe|") {
		t.Errorf("the feed leaks rows outside the recipient's view:\n%s", text)
	}

	// Replay: version 2 plus the feed of version 3 is version 3.
	state := f.tableRows(files2)
	for _, c := range ch {
		if c.version != v3 {
			continue
		}
		for _, r := range f.feedRows(c) {
			p := strings.SplitN(r, "|", 3)
			switch p[0] {
			case "delete":
				delete(state, p[1])
			case "insert", "update_postimage":
				state[p[1]] = strings.TrimPrefix(r, p[0]+"|")
			}
		}
	}
	if !equalSets(state, f.tableRows(files3)) {
		t.Errorf("v2 + feed(v3) = %v, want v3 = %v", state, f.tableRows(files3))
	}

	// Time travel by version: v1 and v2 are what they were.
	_, old := f.queryAt(`{"version": 1}`)
	if rows := f.tableRows(old); len(rows) != 5 || rows["12"] != "" {
		t.Errorf("version 1 = %v", rows)
	}
	if _, old = f.queryAt(`{"version": 2}`); !equalSets(f.tableRows(old), f.tableRows(files2)) {
		t.Errorf("version 2 = %v", f.tableRows(old))
	}
	for _, bad := range []string{`{"version": 0}`, `{"version": 9}`, `{"version": 2, "timestamp": "2030-01-01T00:00:00Z"}`, `{"timestamp": "yesterday"}`} {
		if resp, b := f.do("POST", "/shares/ventas/schemas/gold/tables/sales/query", f.token, bad); resp.StatusCode != 400 {
			t.Errorf("%s: %d %s", bad, resp.StatusCode, b)
		}
	}
	// Time travel by timestamp: the instant of each version, and a millisecond before.
	ts := map[int64]int64{}
	for n := int64(1); n <= 3; n++ {
		_, fl := f.queryAt(fmt.Sprintf(`{"version": %d}`, n))
		ts[n] = int64(fl[0]["timestamp"].(float64))
	}
	if ts[1] >= ts[2] || ts[2] >= ts[3] {
		t.Fatalf("version timestamps are not increasing: %v", ts)
	}
	iso := func(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z") }
	if _, fl := f.queryAt(`{"timestamp": "` + iso(ts[2]) + `"}`); !equalSets(f.tableRows(fl), f.tableRows(files2)) {
		t.Errorf("timestamp of v2 did not give v2")
	}
	if _, fl := f.queryAt(`{"timestamp": "` + iso(ts[2]-1) + `"}`); len(f.tableRows(fl)) != 5 {
		t.Errorf("a millisecond before v2 did not give v1")
	}
	if resp, _ := f.do("POST", "/shares/ventas/schemas/gold/tables/sales/query", f.token, `{"timestamp": "`+iso(ts[1]-1)+`"}`); resp.StatusCode != 400 {
		t.Errorf("before the first version: %d", resp.StatusCode)
	}
	// GET /version?startingTimestamp: earliest version at or after.
	resp, _ := f.do("GET", "/shares/ventas/schemas/gold/tables/sales/version?startingTimestamp="+iso(ts[2]-1), f.token, "")
	if got, _ := strconv.Atoi(resp.Header.Get("Delta-Table-Version")); resp.StatusCode != 200 || int64(got) != v2 {
		t.Errorf("version at startingTimestamp: %d %v", resp.StatusCode, resp.Header)
	}
	if resp, _ = f.do("GET", "/shares/ventas/schemas/gold/tables/sales/version?startingTimestamp="+iso(ts[3]+60000), f.token, ""); resp.StatusCode != 400 {
		t.Errorf("startingTimestamp after the last version: %d", resp.StatusCode)
	}
	// Feed by timestamps.
	if status, _, c := f.changes(f.token, "startingTimestamp="+iso(ts[2])+"&endingTimestamp="+iso(ts[2])); status != 200 || len(c) != 1 || c[0].version != v2 {
		t.Errorf("feed by timestamps: %d %v", status, c)
	}
	// The feed refuses what it cannot give.
	for _, p := range []string{"", "startingVersion=0", "startingVersion=2&endingVersion=9", "startingVersion=3&endingVersion=2", "startingVersion=x"} {
		if status, b, _ := f.changes(f.token, p); status != 400 {
			t.Errorf("feed %q: %d %s", p, status, b)
		}
	}

	// A query with startingVersion (streaming clients) gives the same files.
	if resp, b := f.do("POST", "/shares/ventas/schemas/gold/tables/sales/query", f.token, `{"startingVersion": 2}`); resp.StatusCode != 200 || strings.Count(b, `"cdf"`) != 2 {
		t.Errorf("query with startingVersion: %d %s", resp.StatusCode, b)
	}

	// Retention: keep_versions = 3, so a fourth version drops version 1.
	f.sql(`INSERT INTO gold.sales VALUES (16, 'cl', 'q@x.com', 5.0)`)
	if v4 := f.versionOf(); v4 != 4 {
		t.Fatalf("v4 = %d", v4)
	}
	if resp, b := f.do("POST", "/shares/ventas/schemas/gold/tables/sales/query", f.token, `{"version": 1}`); resp.StatusCode != 400 || !strings.Contains(b, "versions 2 to 4") {
		t.Errorf("pruned version: %d %s", resp.StatusCode, b)
	}
	if status, _, _ := f.changes(f.token, "startingVersion=1"); status != 400 {
		t.Errorf("feed from a pruned version: %d", status)
	}

	// A change of the recipient's policy breaks the feed at that version, honestly.
	if err := f.st.Grant(context.Background(), "ventas", "acme", sharing.GrantOptions{RowFilter: "country = 'cl' AND id < 100", Masks: map[string]string{"email": "'***'"}}); err != nil {
		t.Fatal(err)
	}
	f.regen()
	f.sql(`INSERT INTO gold.sales VALUES (18, 'cl', 'r@x.com', 7.0)`)
	v5 := f.versionOf()
	if status, b, _ := f.changes(f.token, fmt.Sprintf("startingVersion=%d", v5)); status != 400 || !strings.Contains(b, "policy") {
		t.Errorf("feed across a policy change: %d %s", status, b)
	}
	// The next version, under the unchanged policy, has its feed again.
	f.sql(`INSERT INTO gold.sales VALUES (20, 'cl', 's@x.com', 7.5)`)
	v6 := f.versionOf()
	if status, b, c := f.changes(f.token, fmt.Sprintf("startingVersion=%d", v6)); status != 200 || len(c) != 1 {
		t.Errorf("feed after the policy change: %d %s", status, b)
	} else if rows := f.feedRows(c[0]); len(rows) != 1 || !strings.HasPrefix(rows[0], "insert|20|") {
		t.Errorf("rows = %v", rows)
	}

}

func equalSets(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func (f *fixture) queryAt(body string) (int64, []map[string]any) {
	f.t.Helper()
	return f.query(f.token, body)
}

// TestPredicateHints: hints drop files whose statistics rule them out, never
// a file with a matching row; unreadable hints drop nothing.
func TestPredicateHints(t *testing.T) {
	f := newFixture(t, contract.Config{"rows_per_file": 2048})
	// Files close at batch boundaries (2048 rows), so ids 2..10 and 100..6099 land in 3 files.
	f.sql(`INSERT INTO gold.sales SELECT 100 + i, 'cl', 'x@x.com', 1.0 FROM range(6000) t(i)`)
	_, all := f.query(f.token, "")
	n := len(all)
	if n < 3 {
		t.Fatalf("files = %d, want at least 3", n)
	}
	gt := func(col, typ, v string) string {
		return `{"op":"greaterThan","children":[{"op":"column","name":"` + col + `","valueType":"` + typ + `"},{"op":"literal","value":"` + v + `","valueType":"` + typ + `"}]}`
	}
	quote := func(s string) string { return strconv.Quote(s) }
	for _, tc := range []struct {
		name, body string
		fewer      bool     // fewer files than the whole table
		none       bool     // no file at all
		wantIDs    []string // ids that must be in the returned files
	}{
		{"json id > 5000", `{"jsonPredicateHints": ` + quote(gt("id", "long", "5000")) + `}`, true, false, []string{"5001", "6099"}},
		{"json as an object", `{"jsonPredicateHints": ` + gt("id", "long", "5000") + `}`, true, false, []string{"5001", "6099"}},
		{"json id > 100000", `{"jsonPredicateHints": ` + quote(gt("id", "long", "100000")) + `}`, true, true, nil},
		{"legacy id <= 50", `{"predicateHints": ["id <= 50"]}`, true, false, []string{"2", "10"}},
		{"legacy two hints", `{"predicateHints": ["id >= 3000", "id <= 3010"]}`, true, false, []string{"3000", "3010"}},
		{"on a masked column", `{"predicateHints": ["email = '***'"]}`, true, true, nil}, // masked for every row but 'x@x.com' comes from the base: see below
		{"unreadable json", `{"jsonPredicateHints": "{not json"}`, false, false, []string{"2", "6099"}},
		{"unsupported legacy", `{"predicateHints": ["id + 1 > 100000"]}`, false, false, []string{"2", "6099"}},
		{"hint, then limit", `{"predicateHints": ["id >= 5000"], "limitHint": 1}`, true, false, []string{"5000"}},
		{"limit only", `{"limitHint": 2}`, true, false, nil},
	} {
		if tc.name == "on a masked column" {
			continue // the mask is '***' for every row: a hint on it cannot prune and the copy holds only that value
		}
		_, files := f.query(f.token, tc.body)
		switch {
		case tc.none && len(files) != 0:
			t.Errorf("%s: %d files, want none", tc.name, len(files))
		case tc.fewer && len(files) >= n:
			t.Errorf("%s: %d files, want fewer than %d", tc.name, len(files), n)
		case !tc.fewer && len(files) != n:
			t.Errorf("%s: %d files, want all %d", tc.name, len(files), n)
		}
		rows := f.tableRows(files)
		for _, id := range tc.wantIDs {
			if rows[id] == "" {
				t.Errorf("%s: id %s missing from the returned files", tc.name, id)
			}
		}
	}
	// The hint on the masked column prunes by what the recipient sees: the base
	// value is not in the copy (the statistics are of the masked values).
	if _, files := f.query(f.token, `{"predicateHints": ["email = 'u2@x.com'"]}`); len(files) != 0 {
		t.Errorf("a hint on the unmasked value kept %d files: %v", len(files), files)
	}
	if st := all[0]["stats"].(string); !strings.Contains(st, `"minValues":{"amount":1,"country":"cl","email":"***"`) {
		t.Errorf("stats = %s", st)
	}
}

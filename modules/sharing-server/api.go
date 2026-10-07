package sharingserver

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/sharing"
)

const (
	hdrVersion      = "Delta-Table-Version"
	hdrCapabilities = "Delta-Sharing-Capabilities"
	defaultPage     = 500
	maxPage         = 5000
	maxBody         = 1 << 20
)

// api is the REST layer: auth, routing, protocol bodies and audit.
type api struct {
	svc   *sharing.Service
	auth  contract.AuthProvider
	audit contract.Auditor
	local *sharing.LocalStorage // nil with S3 storage
	now   func() time.Time
}

// apiError is an error with the Delta Sharing JSON body.
type apiError struct {
	status int
	code   string
	msg    string
}

func (e *apiError) Error() string { return e.msg }

var (
	errUnauthorized = &apiError{http.StatusUnauthorized, "UNAUTHORIZED", "missing, invalid, revoked or expired bearer token"}
	errForbidden    = &apiError{http.StatusForbidden, "FORBIDDEN", "this token is not a data-sharing recipient"}
)

func notFound(what string) *apiError {
	return &apiError{http.StatusNotFound, "RESOURCE_DOES_NOT_EXIST", what + " does not exist"}
}

func badParam(msg string) *apiError {
	return &apiError{http.StatusBadRequest, "INVALID_PARAMETER_VALUE", msg}
}

func writeError(w http.ResponseWriter, e *apiError) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if e.status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="duct-sharing"`)
	}
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(map[string]string{"errorCode": e.code, "message": e.msg})
}

// result is what a handler produced, for the audit record.
type result struct{ rows int64 }

func (a *api) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := a.now()
	rest := strings.TrimPrefix(r.URL.Path, RESTPrefix)
	if strings.HasPrefix(rest, sharing.FilesPath) {
		a.serveFile(w, r, strings.TrimPrefix(rest, sharing.FilesPath), start)
		return
	}
	id, rcp, aerr := a.authenticate(r)
	rec := contract.AuditRecord{At: start.UTC(), User: id.User, Role: id.Role, Protocol: "sharing", SQL: r.Method + " " + r.URL.Path}
	if rec.User == "" {
		rec.User, rec.Role = "-", "-"
	}
	finish := func(res result, e *apiError) {
		rec.Rows = res.rows
		rec.Duration = a.now().Sub(start)
		if e != nil {
			rec.Err = fmt.Sprintf("%d %s", e.status, e.code)
			if e.status == http.StatusUnauthorized || e.status == http.StatusForbidden {
				rec.Err = "permission denied: " + rec.Err
			}
		}
		a.audit.Audit(context.WithoutCancel(r.Context()), rec)
	}
	if aerr != nil {
		writeError(w, aerr)
		finish(result{}, aerr)
		return
	}
	res, e := a.route(w, r, rcp, strings.Trim(rest, "/"))
	if e != nil {
		writeError(w, e)
	}
	finish(res, e)
}

func (a *api) authenticate(r *http.Request) (contract.Identity, string, *apiError) {
	h := r.Header.Get("Authorization")
	tok, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || strings.TrimSpace(tok) == "" {
		return contract.Identity{}, "", errUnauthorized
	}
	id, err := a.auth.Authenticate(r.Context(), contract.Credentials{Scheme: "bearer", Secret: strings.TrimSpace(tok)})
	if err != nil {
		if errors.Is(err, contract.ErrUnauthenticated) {
			return contract.Identity{}, "", errUnauthorized
		}
		slog.Error("sharing: authentication provider failed", "error", err)
		return contract.Identity{}, "", &apiError{http.StatusInternalServerError, "INTERNAL_ERROR", "authentication unavailable"}
	}
	rcp, ok := sharing.RecipientOf(id)
	if ok {
		if ok, err = a.svc.Store.IsRecipient(r.Context(), rcp); err != nil {
			slog.Error("sharing: recipient lookup failed", "error", err)
			return id, "", &apiError{http.StatusInternalServerError, "INTERNAL_ERROR", "internal error"}
		}
	}
	if !ok {
		return id, "", errForbidden
	}
	return id, rcp, nil
}

func internal(err error) *apiError {
	if errors.Is(err, sharing.ErrNotFound) {
		return notFound("resource")
	}
	slog.Error("sharing: request failed", "error", err)
	return &apiError{http.StatusInternalServerError, "INTERNAL_ERROR", "internal error (see the server log)"}
}

func (a *api) route(w http.ResponseWriter, r *http.Request, rcp, path string) (result, *apiError) {
	seg := strings.Split(path, "/")
	for i := range seg {
		u, err := url.PathUnescape(seg[i])
		if err != nil {
			return result{}, badParam("bad path")
		}
		seg[i] = u
	}
	if seg[0] != "shares" {
		return result{}, notFound("path")
	}
	n := len(seg)
	switch {
	case n == 1 && isRead(r):
		return a.listShares(w, r, rcp)
	case n == 2 && isRead(r):
		return a.getShare(w, r, rcp, seg[1])
	case n == 3 && seg[2] == "schemas" && isRead(r):
		return a.listSchemas(w, r, rcp, seg[1])
	case n == 3 && seg[2] == "all-tables" && isRead(r):
		return a.listTables(w, r, rcp, seg[1], "")
	case n == 5 && seg[2] == "schemas" && seg[4] == "tables" && isRead(r):
		return a.listTables(w, r, rcp, seg[1], seg[3])
	case n == 7 && seg[2] == "schemas" && seg[4] == "tables":
		return a.tableOp(w, r, rcp, seg[1], seg[3], seg[5], seg[6])
	}
	return result{}, notFound("path")
}

func isRead(r *http.Request) bool { return r.Method == http.MethodGet || r.Method == http.MethodHead }

// ---- listings ----

type page struct{ from, to int }

// paginate reads maxResults / pageToken (an opaque offset) and returns the
// window of a list of n items and the next token.
func paginate(r *http.Request, n int) (page, string, *apiError) {
	size := defaultPage
	if v := r.URL.Query().Get("maxResults"); v != "" {
		k, err := strconv.Atoi(v)
		if err != nil || k < 0 {
			return page{}, "", badParam("maxResults must be a non-negative integer")
		}
		if k > 0 {
			size = min(k, maxPage)
		}
	}
	from := 0
	if tok := r.URL.Query().Get("pageToken"); tok != "" {
		b, err := base64.RawURLEncoding.DecodeString(tok)
		if err == nil {
			from, err = strconv.Atoi(string(b))
		}
		if err != nil || from < 0 {
			return page{}, "", badParam("invalid pageToken")
		}
	}
	from = min(from, n)
	to := min(from+size, n)
	next := ""
	if to < n {
		next = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(to)))
	}
	return page{from, to}, next, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func listBody(items []map[string]string, p page, next string) map[string]any {
	body := map[string]any{"items": items[p.from:p.to]}
	if next != "" {
		body["nextPageToken"] = next
	}
	return body
}

func (a *api) listShares(w http.ResponseWriter, r *http.Request, rcp string) (result, *apiError) {
	names, err := a.svc.Store.Granted(r.Context(), rcp)
	if err != nil {
		return result{}, internal(err)
	}
	items := make([]map[string]string, len(names))
	for i, n := range names {
		items[i] = map[string]string{"name": n}
	}
	p, next, e := paginate(r, len(items))
	if e != nil {
		return result{}, e
	}
	writeJSON(w, listBody(items, p, next))
	return result{rows: int64(p.to - p.from)}, nil
}

func (a *api) requireShare(r *http.Request, rcp, share string) *apiError {
	ok, err := a.svc.Store.IsGranted(r.Context(), share, rcp)
	if err != nil {
		return internal(err)
	}
	if !ok {
		return notFound("share")
	}
	return nil
}

func (a *api) getShare(w http.ResponseWriter, r *http.Request, rcp, share string) (result, *apiError) {
	if e := a.requireShare(r, rcp, share); e != nil {
		return result{}, e
	}
	writeJSON(w, map[string]any{"share": map[string]string{"name": share}})
	return result{rows: 1}, nil
}

func (a *api) listSchemas(w http.ResponseWriter, r *http.Request, rcp, share string) (result, *apiError) {
	if e := a.requireShare(r, rcp, share); e != nil {
		return result{}, e
	}
	ts, err := a.svc.Store.Tables(r.Context(), share)
	if err != nil {
		return result{}, internal(err)
	}
	seen := map[string]bool{}
	var items []map[string]string
	for _, t := range ts {
		if !seen[t.Schema()] {
			seen[t.Schema()] = true
			items = append(items, map[string]string{"name": t.Schema(), "share": share})
		}
	}
	p, next, e := paginate(r, len(items))
	if e != nil {
		return result{}, e
	}
	writeJSON(w, listBody(items, p, next))
	return result{rows: int64(p.to - p.from)}, nil
}

// listTables serves both .../schemas/{schema}/tables and .../all-tables (schema "").
func (a *api) listTables(w http.ResponseWriter, r *http.Request, rcp, share, schema string) (result, *apiError) {
	if e := a.requireShare(r, rcp, share); e != nil {
		return result{}, e
	}
	ts, err := a.svc.Store.Tables(r.Context(), share)
	if err != nil {
		return result{}, internal(err)
	}
	var items []map[string]string
	found := schema == ""
	for _, t := range ts {
		if schema != "" && t.Schema() != schema {
			continue
		}
		found = true
		items = append(items, map[string]string{"name": t.Alias, "schema": t.Schema(), "share": share})
	}
	if !found {
		return result{}, notFound("schema")
	}
	p, next, e := paginate(r, len(items))
	if e != nil {
		return result{}, e
	}
	writeJSON(w, listBody(items, p, next))
	return result{rows: int64(p.to - p.from)}, nil
}

// ---- table operations ----

func (a *api) tableOp(w http.ResponseWriter, r *http.Request, rcp, share, schema, table, op string) (result, *apiError) {
	switch {
	case op == "version" && isRead(r), op == "metadata" && isRead(r), op == "query" && r.Method == http.MethodPost:
	case op == "changes":
		return result{}, &apiError{http.StatusNotImplemented, "UNIMPLEMENTED", "change data feed is not supported"}
	default:
		return result{}, notFound("path")
	}
	var req queryRequest
	if op == "query" {
		b, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			return result{}, badParam("request body too large")
		}
		if len(strings.TrimSpace(string(b))) > 0 {
			if err := json.Unmarshal(b, &req); err != nil {
				return result{}, badParam("request body is not valid JSON")
			}
		}
	}
	c, err := a.svc.Resolve(r.Context(), rcp, share, schema, table)
	if err != nil {
		if errors.Is(err, sharing.ErrNotFound) {
			return result{}, notFound("table")
		}
		return result{}, internal(err)
	}
	w.Header().Set(hdrVersion, strconv.FormatInt(c.Version, 10))
	w.Header().Set(hdrCapabilities, "responseformat=parquet")
	if op == "version" {
		w.WriteHeader(http.StatusOK)
		return result{}, nil
	}
	if op == "query" {
		if req.Version != nil && *req.Version != c.Version {
			return result{}, badParam(fmt.Sprintf("version %d is not available: only the current version (%d) is served", *req.Version, c.Version))
		}
		if req.Timestamp != "" || req.StartingVersion != nil || req.EndingVersion != nil {
			return result{}, badParam("time travel and incremental reads are not supported: only the current version is served")
		}
	}
	w.Header().Set("Content-Type", "application/x-ndjson; charset=utf-8")
	enc := json.NewEncoder(w)
	_ = enc.Encode(map[string]any{"protocol": map[string]any{"minReaderVersion": 1}})
	_ = enc.Encode(map[string]any{"metaData": map[string]any{
		"id":               c.TableID,
		"name":             table,
		"format":           map[string]string{"provider": "parquet"},
		"schemaString":     c.SchemaString,
		"partitionColumns": []string{},
		"configuration":    map[string]string{},
		"version":          c.Version,
		"size":             c.TotalBytes,
		"numFiles":         len(c.Files),
	}})
	if op == "metadata" {
		return result{rows: 1}, nil
	}
	urls, err := a.svc.FileURLs(r.Context(), c)
	if err != nil {
		// The status line is already out: nothing honest is left but to cut it.
		slog.Error("sharing: cannot sign file URLs", "error", err)
		panic(http.ErrAbortHandler)
	}
	var sent int64
	for i, f := range c.Files {
		if req.LimitHint != nil && *req.LimitHint > 0 && sent >= *req.LimitHint {
			break
		}
		sent += f.Rows
		_ = enc.Encode(map[string]any{"file": map[string]any{
			"url":             urls[i],
			"id":              f.ID,
			"partitionValues": map[string]string{},
			"size":            f.Size,
			"stats":           fmt.Sprintf(`{"numRecords":%d}`, f.Rows),
		}})
	}
	return result{rows: sent}, nil
}

type queryRequest struct {
	PredicateHints  []string `json:"predicateHints"`
	LimitHint       *int64   `json:"limitHint"`
	Version         *int64   `json:"version"`
	Timestamp       string   `json:"timestamp"`
	StartingVersion *int64   `json:"startingVersion"`
	EndingVersion   *int64   `json:"endingVersion"`
}

// ---- files (local storage) ----

// serveFile serves a copy from the local folder to a holder of a signed,
// unexpired URL. Anything else is a plain 403, like an object store.
func (a *api) serveFile(w http.ResponseWriter, r *http.Request, rawKey string, start time.Time) {
	deny := func(why string) {
		writeError(w, &apiError{http.StatusForbidden, "FORBIDDEN", "invalid or expired URL"})
		a.audit.Audit(context.WithoutCancel(r.Context()), contract.AuditRecord{At: start.UTC(), User: "-", Role: "-", Protocol: "sharing",
			SQL: "GET file " + r.URL.Path, Duration: a.now().Sub(start), Err: "permission denied: " + why})
	}
	if a.local == nil || !isRead(r) {
		writeError(w, notFound("path"))
		return
	}
	key, ok := sharing.KeyFromPath(rawKey)
	if !ok || !strings.HasSuffix(key, ".parquet") {
		deny("bad key")
		return
	}
	q := r.URL.Query()
	if err := a.local.Signer.Verify(key, q.Get("exp"), q.Get("sig"), a.now()); err != nil {
		deny(err.Error())
		return
	}
	f, err := a.local.Open(key)
	if err != nil {
		writeError(w, notFound("file"))
		return
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		writeError(w, notFound("file"))
		return
	}
	w.Header().Set("Content-Type", "application/vnd.apache.parquet")
	w.Header().Set("Cache-Control", "private, no-store")
	http.ServeContent(w, r, "", st.ModTime(), f)
	user := "-"
	if parts := strings.Split(key, "/"); len(parts) > 1 {
		user = sharing.RoleOf(parts[1]) // the key is covered by the signature
	}
	a.audit.Audit(context.WithoutCancel(r.Context()), contract.AuditRecord{At: start.UTC(), User: user, Role: user, Protocol: "sharing",
		SQL: "GET file " + key, Duration: a.now().Sub(start)})
}

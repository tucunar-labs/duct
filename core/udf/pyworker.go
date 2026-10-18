package udf

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/contract/v1/sandbox"
)

// registrySnapshot is the JSON of `python -m duct.registry --json` (schema 3).
type registrySnapshot struct {
	SchemaVersion int `json:"schema_version"`
	Functions     []struct {
		Name           string     `json:"name"`
		Kind           string     `json:"kind"`
		Columns        [][]string `json:"columns"`
		Module         string     `json:"module"`
		Vectorized     bool       `json:"vectorized"`
		DuckDBArgs     []*string  `json:"duckdb_args"`
		DuckDBReturns  *string    `json:"duckdb_returns"`
		InputTypes     []string   `json:"input_types"`
		ReturnTypeName *string    `json:"return_type"`
	} `json:"functions"`
}

func discoverPython(ctx context.Context, opt Options) ([]Spec, error) {
	argv := append(append([]string{}, opt.Python...), "-m", "duct.registry", "--json", "--load", filepath.Join(opt.Root, DirName))
	cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	argv, env, err := opt.sandboxed(argv)
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(cctx, argv[0], argv[1:]...) //nolint:gosec // argv is the project's own interpreter setting
	cmd.Dir = opt.Root
	cmd.Env = env
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("udf: cannot load functions/*.py with %q (is duct-data installed in that Python? set [python] command): %w\n%s",
			strings.Join(opt.Python, " "), err, strings.TrimSpace(errb.String()))
	}
	var snap registrySnapshot
	if err := json.Unmarshal(out.Bytes(), &snap); err != nil {
		return nil, fmt.Errorf("udf: unreadable registry output: %w", err)
	}
	if snap.SchemaVersion != 3 {
		return nil, fmt.Errorf("udf: duct-data registry schema %d, this duct needs 3 (upgrade duct-data)", snap.SchemaVersion)
	}
	var specs []Spec
	for _, f := range snap.Functions {
		s := Spec{Name: f.Name, Kind: "python", Vectorized: f.Vectorized, Table: f.Kind == "table"}
		s.File = f.Module
		if stem, ok := strings.CutPrefix(f.Module, "_duct_user_"); ok { // registry's module alias
			s.File = filepath.Join(DirName, stem+".py")
		}
		for i, a := range f.DuckDBArgs {
			if a == nil {
				return nil, fmt.Errorf("udf: function %s: argument %d has no supported SQL type (annotation %q): "+
					"use @duct.function(types=[...], returns=...) with BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR, "+
					"DATE, TIMESTAMP, DECIMAL(p,s), T[] or STRUCT(name T, ...)",
					f.Name, i+1, f.InputTypes[i])
			}
			s.ArgTypes = append(s.ArgTypes, *a)
		}
		if s.Table {
			for _, c := range f.Columns {
				if len(c) != 2 {
					return nil, fmt.Errorf("udf: function %s: malformed columns", f.Name)
				}
				s.Columns = append(s.Columns, contract.TableColumn{Name: c[0], Type: c[1]})
			}
			specs = append(specs, s)
			continue
		}
		if f.DuckDBReturns == nil {
			return nil, fmt.Errorf("udf: function %s: no supported SQL return type: use @duct.function(returns=...)", f.Name)
		}
		s.ReturnType = *f.DuckDBReturns
		specs = append(specs, s)
	}
	return specs, nil
}

// pyPool is the set of worker processes of one project; they start lazily and
// each serves one batch at a time.
type pyPool struct {
	opt Options

	mu      sync.Mutex
	idle    []*pyWorker
	started int
	closed  bool
	wake    chan struct{} // signalled when a worker is returned or dropped
	done    chan struct{} // closed by Close: wakes every waiter
}

// errPoolClosed is what acquire returns once Close was called; the callers of a
// function whose pool was replaced by a reload retry with the new one.
var errPoolClosed = errors.New("udf: worker pool closed")

func newPyPool(opt Options) *pyPool {
	return &pyPool{opt: opt, wake: make(chan struct{}, 1), done: make(chan struct{})}
}

// acquire waits for an idle worker or starts one. Waiting is bounded by ctx;
// a cold start is bounded by Options.StartTimeout under parent, never by the
// per-batch timeout (a slow interpreter start must not fail the first query).
func (p *pyPool) acquire(ctx, parent context.Context) (*pyWorker, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, errPoolClosed
		}
		if n := len(p.idle); n > 0 {
			w := p.idle[n-1]
			p.idle = p.idle[:n-1]
			p.mu.Unlock()
			return w, nil
		}
		if p.started < p.opt.Workers {
			p.started++
			p.mu.Unlock()
			sctx, cancel := context.WithTimeout(parent, p.opt.StartTimeout)
			w, err := startWorker(sctx, p.opt)
			cancel()
			if err != nil {
				p.mu.Lock()
				p.started--
				p.mu.Unlock()
				return nil, err
			}
			return w, nil
		}
		p.mu.Unlock()
		select {
		case <-p.wake:
		case <-p.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *pyPool) release(w *pyWorker, healthy bool) {
	p.mu.Lock()
	if healthy && !p.closed {
		p.idle = append(p.idle, w)
	} else {
		p.started--
		go w.stop()
	}
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Close stops the idle workers now and every busy one as soon as its batch is
// answered (release); new acquisitions fail with errPoolClosed. Idempotent.
func (p *pyPool) Close() error {
	p.mu.Lock()
	if !p.closed {
		close(p.done)
	}
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()
	for _, w := range idle {
		w.stop()
	}
	return nil
}

// do sends one batch to a worker and returns the raw response frame (status
// byte first). Only I/O failures and timeouts are errors here (they cost the
// worker its life); an error raised by the function is a status-1 body.
func (p *pyPool) do(ctx context.Context, name string, args []arrow.Array) ([]byte, error) {
	parent := ctx
	wctx, wcancel := context.WithTimeout(parent, p.opt.Timeout)
	w, err := p.acquire(wctx, parent)
	wcancel()
	if err != nil {
		return nil, err
	}
	// The batch clock starts once the worker is ready.
	ctx, cancel := context.WithTimeout(parent, p.opt.Timeout)
	defer cancel()
	body, err := w.call(ctx, name, args)
	p.release(w, err == nil)
	return body, err
}

func (p *pyPool) caller(s Spec) callFunc {
	return func(ctx context.Context, args []arrow.Array) (arrow.Array, error) {
		body, err := p.do(ctx, s.Name, args)
		if err != nil {
			return nil, err
		}
		return decodeResponse(body)
	}
}

func (p *pyPool) tableCaller(s Spec) tableCallFunc {
	return func(ctx context.Context, args []arrow.Array) ([]arrow.RecordBatch, error) {
		body, err := p.do(ctx, s.Name, args)
		if err != nil {
			return nil, err
		}
		return decodeTable(body)
	}
}

type remoteError struct{ msg string }

func (e *remoteError) Error() string { return e.msg }

// pyWorker is one `python -m duct.udf.worker` process.
type pyWorker struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	out   io.Reader
	done  chan struct{}
}

func startWorker(ctx context.Context, opt Options) (*pyWorker, error) {
	argv := append(append([]string{}, opt.Python...), "-m", "duct.udf.worker", "--load", filepath.Join(opt.Root, DirName))
	argv, env, err := opt.sandboxed(argv)
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(argv[0], argv[1:]...) //nolint:gosec // argv is the project's own interpreter setting
	cmd.Dir = opt.Root
	cmd.Env = env
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("udf: start worker: %w", err)
	}
	w := &pyWorker{cmd: cmd, stdin: stdin, out: stdout, done: make(chan struct{})}
	go func() { _ = cmd.Wait(); close(w.done) }()
	// The ready frame arrives once the user modules are imported.
	type readyRes struct {
		body []byte
		err  error
	}
	ch := make(chan readyRes, 1)
	go func() { b, err := readFrame(stdout); ch <- readyRes{b, err} }()
	select {
	case r := <-ch:
		if r.err != nil || len(r.body) == 0 || r.body[0] != 0 {
			w.stop()
			return nil, fmt.Errorf("udf: worker did not start (is pyarrow installed? pip install 'duct-data[arrow]'): %v", r.err)
		}
	case <-ctx.Done():
		w.stop()
		return nil, ctx.Err()
	}
	return w, nil
}

func (w *pyWorker) stop() {
	_ = w.stdin.Close() // EOF is the polite shutdown
	select {
	case <-w.done:
	case <-time.After(2 * time.Second):
		_ = w.cmd.Process.Kill()
		<-w.done
	}
}

func readFrame(r io.Reader) ([]byte, error) {
	var head [4]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(head[:])
	if n > 1<<30 {
		return nil, fmt.Errorf("frame too large (%d)", n)
	}
	body := make([]byte, n)
	_, err := io.ReadFull(r, body)
	return body, err
}

// call sends one batch and waits for the response frame. On ctx expiry it kills the
// process (the pool replaces it).
func (w *pyWorker) call(ctx context.Context, name string, args []arrow.Array) ([]byte, error) {
	fields := make([]arrow.Field, len(args))
	for i, a := range args {
		fields[i] = arrow.Field{Name: fmt.Sprintf("c%d", i), Type: a.DataType(), Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)
	rec := array.NewRecordBatch(schema, args, int64(args[0].Len()))
	defer rec.Release()

	var buf bytes.Buffer
	var head [6]byte
	buf.Write(head[:]) // u32 length + u16 name length, patched below
	buf.WriteString(name)
	iw := ipc.NewWriter(&buf, ipc.WithSchema(schema), ipc.WithAllocator(memory.DefaultAllocator))
	if err := iw.Write(rec); err != nil {
		return nil, err
	}
	if err := iw.Close(); err != nil {
		return nil, err
	}
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b[0:4], uint32(len(b)-4))
	binary.BigEndian.PutUint16(b[4:6], uint16(len(name)))

	type result struct {
		body []byte
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		if _, err := w.stdin.Write(b); err != nil {
			ch <- result{nil, err}
			return
		}
		body, err := readFrame(w.out)
		ch <- result{body, err}
	}()
	select {
	case r := <-ch:
		if r.err != nil {
			return nil, fmt.Errorf("python worker: %w", r.err)
		}
		return r.body, nil
	case <-ctx.Done():
		_ = w.cmd.Process.Kill()
		return nil, fmt.Errorf("python worker: %w", ctx.Err())
	}
}

func decodeResponse(body []byte) (arrow.Array, error) {
	if len(body) == 0 {
		return nil, errors.New("python worker: empty response")
	}
	if body[0] != 0 {
		return nil, &remoteError{string(body[1:])}
	}
	rd, err := ipc.NewReader(bytes.NewReader(body[1:]))
	if err != nil {
		return nil, fmt.Errorf("python worker: %w", err)
	}
	defer rd.Release()
	if !rd.Next() {
		return nil, errors.New("python worker: no result batch")
	}
	col := rd.RecordBatch().Column(0)
	col.Retain()
	return col, nil
}

// decodeTable reads the batches of a table function's response.
func decodeTable(body []byte) ([]arrow.RecordBatch, error) {
	if len(body) == 0 {
		return nil, errors.New("python worker: empty response")
	}
	if body[0] != 0 {
		return nil, &remoteError{string(body[1:])}
	}
	rd, err := ipc.NewReader(bytes.NewReader(body[1:]))
	if err != nil {
		return nil, fmt.Errorf("python worker: %w", err)
	}
	defer rd.Release()
	var out []arrow.RecordBatch
	for rd.Next() {
		b := rd.RecordBatch()
		b.Retain()
		out = append(out, b)
	}
	if err := rd.Err(); err != nil {
		for _, b := range out {
			b.Release()
		}
		return nil, fmt.Errorf("python worker: %w", err)
	}
	return out, nil
}

var sandboxWarned sync.Once

// sandboxed returns the command line and environment of a Python process of the project: wrapped in bubblewrap
// when Options.Sandbox asks for it (the project read-only minus its secrets, see sandbox.ProjectHide).
func (o Options) sandboxed(argv []string) ([]string, []string, error) {
	env := SafeEnv(o.ExtraEnv)
	if o.Sandbox != sandbox.Bwrap {
		return argv, env, nil
	}
	wrapped, err := o.wrap(argv)
	if err != nil {
		if o.SandboxAuto {
			sandboxWarned.Do(func() {
				slog.Warn("udf: Python workers run without the sandbox (sandbox = auto): " + err.Error())
			})
			return argv, env, nil
		}
		return nil, nil, fmt.Errorf("udf: %w", err)
	}
	return wrapped, sandbox.Env(env), nil
}

func (o Options) wrap(argv []string) ([]string, error) {
	bw, err := sandbox.Available()
	if err != nil {
		return nil, err
	}
	if b := filepath.Base(argv[0]); b == "uv" || b == "uvx" {
		return nil, errors.New("`uv run` cannot run inside the sandbox: set [python] command to a direct interpreter (a venv's bin/python), or [sandbox] udf = \"none\"")
	}
	py := argv[0]
	if strings.Contains(py, "/") && !filepath.IsAbs(py) {
		py = filepath.Join(o.Root, py)
		argv = append([]string{py}, argv[1:]...)
	}
	return sandbox.Argv(sandbox.Spec{Bwrap: bw, Python: py, Root: o.Root, ProjectRO: true,
		Hide: sandbox.ProjectHide(o.Root, o.SandboxHide...), Chdir: o.Root}, argv)
}

package web

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/starfederation/datastar-go/datastar"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Jobs (ADR 0005, 0012): every role may look at the jobs and their runs; the
// actions (run now, cancel, pause/resume) are for duct_admin, enforced here
// (403) and again by the core, and audited by it. All data comes through the
// optional contract.JobsConsole of the QueryHandler.

const (
	runsShown = 50
	liveMax   = 6 * time.Hour // a live page gives up after this long
)

func (a *app) jobsRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /jobs", a.authed(a.jobsList))
	mux.HandleFunc("GET /jobs/{name}", a.authed(a.jobDetail))
	mux.HandleFunc("GET /jobs/{name}/runs/{id}", a.authed(a.runDetail))
	mux.HandleFunc("GET /jobs/{name}/runs/{id}/live", a.authed(a.runLive))
	mux.HandleFunc("POST /jobs/{name}/run", a.authed(a.jobRun))
	mux.HandleFunc("POST /jobs/{name}/enabled", a.authed(a.jobEnabled))
	mux.HandleFunc("POST /jobs/{name}/runs/{id}/cancel", a.authed(a.runCancel))
}

func (a *app) jobsConsole() (contract.JobsConsole, bool) {
	c, ok := a.q.(contract.JobsConsole)
	return c, ok
}

func isAdminSession(sess session) bool { return sess.who.Role == "duct_admin" }

func (a *app) html(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
}

func (a *app) forbidden(w http.ResponseWriter, r *http.Request, sess session, what string) {
	a.html(w, http.StatusForbidden)
	p := a.page(sess, "Sin acceso", "")
	_ = forbiddenPage(p, what).Render(r.Context(), w)
}

func (a *app) notFound(w http.ResponseWriter, r *http.Request, sess session, active, what string) {
	a.html(w, http.StatusNotFound)
	p := a.page(sess, "No encontrado", active)
	_ = notFoundPage(p, what).Render(r.Context(), w)
}

// jobsPage is the page data of the Jobs pages (breadcrumb: Trabajos).
func (a *app) jobsPage(sess session, title string) pageData {
	p := a.page(sess, title, "jobs")
	p.Crumbs = []crumb{{Label: "Trabajos"}}
	return p
}

type jobsView struct {
	Jobs  []contract.JobInfo
	Err   string
	Flash *flash
	Now   time.Time
	Admin bool
}

func (a *app) jobsList(w http.ResponseWriter, r *http.Request, sess session) {
	v := jobsView{Flash: a.takeFlash(r, sess), Now: time.Now(), Admin: isAdminSession(sess)}
	status := http.StatusOK
	if c, ok := a.jobsConsole(); !ok {
		v.Err = "Este servidor no expone los trabajos."
	} else {
		ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
		defer cancel()
		jobs, err := c.Jobs(ctx)
		if err != nil {
			slog.Warn("web: jobs list failed", "user", sess.who.User, "err", err)
			v.Err, status = "No se pudieron leer los trabajos: "+err.Error(), http.StatusBadGateway
		}
		v.Jobs = jobs
	}
	a.html(w, status)
	_ = jobsPage(a.jobsPage(sess, "Trabajos"), sess.who.Role, a.csrf(sess), v).Render(r.Context(), w)
}

func (a *app) csrf(sess session) string { return a.store.csrf(sess) }

// findJob loads the job named in the path (nil: not found, error already sent).
func (a *app) findJob(w http.ResponseWriter, r *http.Request, sess session) (contract.JobsConsole, *contract.JobInfo, bool) {
	c, ok := a.jobsConsole()
	if !ok {
		a.notFound(w, r, sess, "jobs", "Este servidor no expone los trabajos.")
		return nil, nil, false
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	jobs, err := c.Jobs(ctx)
	if err != nil {
		slog.Warn("web: jobs list failed", "user", sess.who.User, "err", err)
		a.html(w, http.StatusBadGateway)
		_ = notFoundPage(a.jobsPage(sess, "Trabajos"), "No se pudieron leer los trabajos: "+err.Error()).Render(r.Context(), w)
		return nil, nil, false
	}
	for i := range jobs {
		if jobs[i].Name == r.PathValue("name") {
			return c, &jobs[i], true
		}
	}
	a.notFound(w, r, sess, "jobs", "No existe el trabajo "+r.PathValue("name")+".")
	return nil, nil, false
}

type jobView struct {
	Job   *contract.JobInfo
	Runs  []contract.JobRunInfo
	Err   string
	Flash *flash
	Now   time.Time
	Admin bool
}

func (a *app) jobDetail(w http.ResponseWriter, r *http.Request, sess session) {
	c, job, ok := a.findJob(w, r, sess)
	if !ok {
		return
	}
	v := jobView{Job: job, Flash: a.takeFlash(r, sess), Now: time.Now(), Admin: isAdminSession(sess)}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	runs, err := c.JobRuns(ctx, job.Name, runsShown)
	if err != nil {
		slog.Warn("web: job runs failed", "user", sess.who.User, "job", job.Name, "err", err)
		v.Err = "No se pudo leer el historial: " + err.Error()
	}
	v.Runs = runs
	a.html(w, http.StatusOK)
	p := a.jobsPage(sess, job.Name)
	p.Crumbs = []crumb{{Label: "Trabajos", Href: "/jobs"}, {Label: job.Name}}
	_ = jobPage(p, a.csrf(sess), v).Render(r.Context(), w)
}

type runView struct {
	Job   *contract.JobInfo
	Run   contract.JobRunInfo
	Tasks []contract.JobTaskRun
	Flash *flash
	Now   time.Time
	Admin bool
	Live  bool // the page subscribes to /live
	CSRF  string
}

func parseRunID(r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	return id, err == nil && id > 0
}

// loadRun gets a run and checks it belongs to the job of the path.
func (a *app) loadRun(w http.ResponseWriter, r *http.Request, sess session) (contract.JobsConsole, *contract.JobInfo, contract.JobRunInfo, []contract.JobTaskRun, bool) {
	c, job, ok := a.findJob(w, r, sess)
	if !ok {
		return nil, nil, contract.JobRunInfo{}, nil, false
	}
	id, valid := parseRunID(r)
	if !valid {
		a.notFound(w, r, sess, "jobs", "Número de ejecución inválido.")
		return nil, nil, contract.JobRunInfo{}, nil, false
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	run, tasks, found, err := c.JobRun(ctx, id)
	if err != nil {
		slog.Warn("web: run read failed", "user", sess.who.User, "run", id, "err", err)
		a.html(w, http.StatusBadGateway)
		_ = notFoundPage(a.jobsPage(sess, "Trabajos"), "No se pudo leer la ejecución: "+err.Error()).Render(r.Context(), w)
		return nil, nil, contract.JobRunInfo{}, nil, false
	}
	if !found || run.Job != job.Name {
		a.notFound(w, r, sess, "jobs", "No existe la ejecución "+r.PathValue("id")+" de "+job.Name+".")
		return nil, nil, contract.JobRunInfo{}, nil, false
	}
	return c, job, run, tasks, true
}

func (a *app) runDetail(w http.ResponseWriter, r *http.Request, sess session) {
	_, job, run, tasks, ok := a.loadRun(w, r, sess)
	if !ok {
		return
	}
	v := runView{Job: job, Run: run, Tasks: orderTasks(tasks), Flash: a.takeFlash(r, sess), Now: time.Now(), Admin: isAdminSession(sess),
		Live: run.Status == contract.RunRunning, CSRF: a.csrf(sess)}
	a.html(w, http.StatusOK)
	p := a.jobsPage(sess, "Ejecución "+runLabel(run.ID))
	p.Crumbs = []crumb{{Label: "Trabajos", Href: "/jobs"}, {Label: job.Name, Href: "/jobs/" + job.Name}, {Label: "Ejecución " + runLabel(run.ID)}}
	_ = runPage(p, v).Render(r.Context(), w)
}

// runLive streams the run while it is in progress: every poll it patches the
// #run-live element (status, DAG and task table) and it stops, after a last
// patch, when the run is over or the browser goes away.
func (a *app) runLive(w http.ResponseWriter, r *http.Request, sess session) {
	c, ok := a.jobsConsole()
	if !ok {
		http.Error(w, "jobs not available", http.StatusNotFound)
		return
	}
	id, valid := parseRunID(r)
	if !valid {
		http.Error(w, "bad run id", http.StatusBadRequest)
		return
	}
	poll := a.cfg.livePoll
	if poll <= 0 {
		poll = time.Second
	}
	sse := datastar.NewSSE(w, r)
	ctx, cancel := context.WithTimeout(r.Context(), liveMax)
	defer cancel()
	tick := time.NewTicker(poll)
	defer tick.Stop()
	var last string
	for {
		qctx, qcancel := context.WithTimeout(ctx, a.cfg.queryTimeout)
		run, tasks, found, err := c.JobRun(qctx, id)
		qcancel()
		if err != nil || !found || run.Job != r.PathValue("name") {
			return
		}
		jobs, jerr := c.Jobs(ctx)
		var job *contract.JobInfo
		if jerr == nil {
			for i := range jobs {
				if jobs[i].Name == run.Job {
					job = &jobs[i]
				}
			}
		}
		v := runView{Job: job, Run: run, Tasks: orderTasks(tasks), Now: time.Now(), Admin: isAdminSession(sess), Live: run.Status == contract.RunRunning, CSRF: a.csrf(sess)}
		if sig := liveSignature(run, tasks); sig != last || v.Live {
			last = sig
			if err := sse.PatchElementTempl(runLiveBody(v), datastar.WithMode(datastar.ElementPatchModeReplace)); err != nil {
				return
			}
		}
		if !v.Live {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// liveSignature changes whenever something the page shows changes (the elapsed
// time of a running run is refreshed anyway, see runLive).
func liveSignature(run contract.JobRunInfo, tasks []contract.JobTaskRun) string {
	s := run.Status + "|" + run.FinishedAt.String()
	for _, t := range tasks {
		s += "|" + t.ID + ":" + t.Status + ":" + strconv.Itoa(t.Attempts) + ":" + strconv.Itoa(len(t.Output))
	}
	return s
}

// ---- actions (duct_admin) ----

// act is the shared shell of the three actions: admin only (403 otherwise),
// then fn; the outcome travels as a flash to where the browser lands next.
func (a *app) act(w http.ResponseWriter, r *http.Request, sess session, fn func(c contract.JobsConsole, ctx context.Context) (okMsg, dest string, err error), failDest string) {
	if !isAdminSession(sess) {
		a.forbidden(w, r, sess, "Solo el rol duct_admin puede ejecutar, cancelar o pausar trabajos.")
		return
	}
	c, ok := a.jobsConsole()
	if !ok {
		a.redirectFlash(w, r, sess, failDest, &flash{Kind: "error", Msg: "Este servidor no expone los trabajos."})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.queryTimeout)
	defer cancel()
	msg, dest, err := fn(c, ctx)
	if err != nil {
		slog.Warn("web: job action failed", "user", sess.who.User, "path", r.URL.Path, "err", err)
		a.redirectFlash(w, r, sess, failDest, &flash{Kind: "error", Msg: actionError(err)})
		return
	}
	a.redirectFlash(w, r, sess, dest, &flash{Kind: "ok", Msg: msg})
}

func actionError(err error) string {
	switch {
	case errors.Is(err, contract.ErrConsoleUnavailable):
		return "Este servidor no expone los trabajos."
	case err.Error() == "the run is not running":
		return "La ejecución ya terminó."
	}
	return err.Error()
}

func (a *app) jobRun(w http.ResponseWriter, r *http.Request, sess session) {
	name := r.PathValue("name")
	a.act(w, r, sess, func(c contract.JobsConsole, ctx context.Context) (string, string, error) {
		id, err := c.RunJob(ctx, sess.who, name)
		if err != nil {
			return "", "", err
		}
		return "Ejecución " + runLabel(id) + " iniciada.", "/jobs/" + name + "/runs/" + strconv.FormatInt(id, 10), nil
	}, backTo(r, name))
}

func (a *app) jobEnabled(w http.ResponseWriter, r *http.Request, sess session) {
	name := r.PathValue("name")
	enable := r.PostFormValue("enabled") == "1"
	back := backTo(r, name)
	a.act(w, r, sess, func(c contract.JobsConsole, ctx context.Context) (string, string, error) {
		if err := c.SetJobEnabled(ctx, sess.who, name, enable); err != nil {
			return "", "", err
		}
		if enable {
			return "Trabajo " + name + " reanudado.", back, nil
		}
		return "Trabajo " + name + " en pausa: no se ejecutará por horario ni por eventos.", back, nil
	}, back)
}

func (a *app) runCancel(w http.ResponseWriter, r *http.Request, sess session) {
	name := r.PathValue("name")
	id, valid := parseRunID(r)
	dest := "/jobs/" + name + "/runs/" + r.PathValue("id")
	if !valid {
		a.notFound(w, r, sess, "jobs", "Número de ejecución inválido.")
		return
	}
	a.act(w, r, sess, func(c contract.JobsConsole, ctx context.Context) (string, string, error) {
		// The run must belong to the job in the path.
		run, _, found, err := c.JobRun(ctx, id)
		if err != nil {
			return "", "", err
		}
		if !found || run.Job != name {
			return "", "", errors.New("no existe esa ejecución")
		}
		if err := c.CancelJobRun(ctx, sess.who, id); err != nil {
			return "", "", err
		}
		return "Cancelación solicitada: las tareas en curso se interrumpen en unos segundos.", dest, nil
	}, dest)
}

// backTo is where an action returns after it fails or when it has no better
// destination: the list when asked for ("back=list"), else the job's page.
func backTo(r *http.Request, name string) string {
	if r.PostFormValue("back") == "list" {
		return "/jobs"
	}
	return "/jobs/" + name
}

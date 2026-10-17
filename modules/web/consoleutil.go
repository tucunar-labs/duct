package web

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	contract "github.com/alonsoburon/duct/core/contract/v1"
)

// Pure helpers of the Jobs and Admin pages: Spanish wording for schedules,
// durations and times, status badges, and the layered layout of a job's DAG.

var weekdays = []string{"domingo", "lunes", "martes", "miércoles", "jueves", "viernes", "sábado"}

// humanSchedule describes a cron expression, a descriptor (@daily...) or an
// "@every" interval in Spanish. Anything it does not recognise is returned as
// "Cron: <expression>", so it never says something wrong.
func humanSchedule(spec string) string {
	spec = strings.TrimSpace(spec)
	if spec == "" {
		return "Sin horario"
	}
	switch spec {
	case "@hourly":
		return "Cada hora"
	case "@daily", "@midnight":
		return "Todos los días a las 00:00"
	case "@weekly":
		return "Los domingos a las 00:00"
	case "@monthly":
		return "El día 1 de cada mes a las 00:00"
	case "@yearly", "@annually":
		return "El 1 de enero a las 00:00"
	}
	if rest, ok := strings.CutPrefix(spec, "@every "); ok {
		d, err := time.ParseDuration(strings.TrimSpace(rest))
		if err != nil {
			return "Cron: " + spec
		}
		return "Cada " + everyWord(d)
	}
	f := strings.Fields(spec)
	if len(f) != 5 {
		return "Cron: " + spec
	}
	min, hour, dom, mon, dow := f[0], f[1], f[2], f[3], f[4]
	num := func(s string) (int, bool) {
		n, err := strconv.Atoi(s)
		return n, err == nil && n >= 0
	}
	step := func(s string) (int, bool) {
		if v, ok := strings.CutPrefix(s, "*/"); ok {
			return num(v)
		}
		return 0, false
	}
	at := func() (string, bool) {
		m, ok1 := num(min)
		h, ok2 := num(hour)
		if !ok1 || !ok2 || m > 59 || h > 23 {
			return "", false
		}
		return fmt.Sprintf("%02d:%02d", h, m), true
	}
	if mon != "*" {
		return "Cron: " + spec
	}
	switch {
	case min == "*" && hour == "*" && dom == "*" && dow == "*":
		return "Cada minuto"
	case hour == "*" && dom == "*" && dow == "*":
		if n, ok := step(min); ok && n > 0 {
			return fmt.Sprintf("Cada %d minutos", n)
		}
		if m, ok := num(min); ok && m <= 59 {
			return fmt.Sprintf("Cada hora, en el minuto %d", m)
		}
	case dom == "*" && dow == "*":
		if n, ok := step(hour); ok && n > 0 {
			if m, ok := num(min); ok && m <= 59 {
				return fmt.Sprintf("Cada %d horas, en el minuto %d", n, m)
			}
		}
		if t, ok := at(); ok {
			return "Todos los días a las " + t
		}
	case dom == "*":
		if t, ok := at(); ok {
			if d := weekdayList(dow); d != "" {
				return d + " a las " + t
			}
		}
	case dow == "*":
		if t, ok := at(); ok {
			if d, ok := num(dom); ok && d >= 1 && d <= 31 {
				return fmt.Sprintf("El día %d de cada mes a las %s", d, t)
			}
		}
	}
	return "Cron: " + spec
}

func everyWord(d time.Duration) string {
	plural := func(n int64, one, many string) string {
		if n == 1 {
			return one
		}
		return strconv.FormatInt(n, 10) + " " + many
	}
	switch {
	case d%(24*time.Hour) == 0:
		return plural(int64(d/(24*time.Hour)), "día", "días")
	case d%time.Hour == 0:
		return plural(int64(d/time.Hour), "hora", "horas")
	case d%time.Minute == 0:
		return plural(int64(d/time.Minute), "minuto", "minutos")
	}
	return plural(int64(d/time.Second), "segundo", "segundos")
}

// weekdayList renders a cron day-of-week field ("1", "1-5", "1,3") as "Los lunes".
func weekdayList(f string) string {
	day := func(s string) (int, bool) {
		n, err := strconv.Atoi(s)
		if err != nil || n < 0 || n > 7 {
			return 0, false
		}
		return n % 7, true
	}
	if a, b, ok := strings.Cut(f, "-"); ok {
		x, ok1 := day(a)
		y, ok2 := day(b)
		if !ok1 || !ok2 {
			return ""
		}
		return "De " + weekdays[x] + " a " + weekdays[y]
	}
	var names []string
	for _, p := range strings.Split(f, ",") {
		d, ok := day(p)
		if !ok {
			return ""
		}
		n := weekdays[d]
		if !strings.HasSuffix(n, "s") { // lunes, martes... do not change in the plural
			n += "s"
		}
		names = append(names, n)
	}
	if len(names) == 0 {
		return ""
	}
	if len(names) == 1 {
		return "Los " + names[0]
	}
	return "Los " + strings.Join(names[:len(names)-1], ", ") + " y " + names[len(names)-1]
}

// fmtDuration renders a duration compactly: 420 ms, 35 s, 4 min 05 s, 1 h 02 min.
func fmtDuration(d time.Duration) string {
	switch {
	case d <= 0:
		return "0 s"
	case d < time.Second:
		return fmt.Sprintf("%d ms", d.Milliseconds())
	case d < time.Minute:
		if d < 10*time.Second {
			return fmt.Sprintf("%.1f s", d.Seconds())
		}
		return fmt.Sprintf("%d s", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%d min %02d s", int(d.Minutes()), int(d.Seconds())%60)
	}
	return fmt.Sprintf("%d h %02d min", int(d.Hours()), int(d.Minutes())%60)
}

// fmtTime is an absolute UTC time; zero is an em dash.
func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
	return t.UTC().Format("2006-01-02 15:04:05")
}

// relTime says how far t is from now ("hace 3 min", "en 2 h").
func relTime(t, now time.Time) string {
	if t.IsZero() {
		return "—"
	}
	d := now.Sub(t)
	future := d < 0
	if future {
		d = -d
	}
	var s string
	switch {
	case d < 5*time.Second:
		return "ahora"
	case d < time.Minute:
		s = fmt.Sprintf("%d s", int(d.Seconds()))
	case d < time.Hour:
		s = fmt.Sprintf("%d min", int(d.Minutes()))
	case d < 48*time.Hour:
		s = fmt.Sprintf("%d h", int(d.Hours()))
	default:
		s = fmt.Sprintf("%d días", int(d.Hours()/24))
	}
	if future {
		return "en " + s
	}
	return "hace " + s
}

// runDuration is how long a run took (or has been taking).
func runDuration(r contract.JobRunInfo, now time.Time) time.Duration {
	if r.StartedAt.IsZero() {
		return 0
	}
	if r.FinishedAt.IsZero() {
		return now.Sub(r.StartedAt)
	}
	return r.FinishedAt.Sub(r.StartedAt)
}

// statusLabel is the Spanish name of a run or task status.
func statusLabel(s string) string {
	switch s {
	case contract.RunSuccess:
		return "Correcta"
	case contract.RunFailed:
		return "Fallida"
	case contract.RunRunning:
		return "En curso"
	case contract.RunPending:
		return "Pendiente"
	case contract.RunSkipped:
		return "Omitida"
	case contract.RunCancelled:
		return "Cancelada"
	}
	return s
}

func statusBadgeClass(s string) string {
	switch s {
	case contract.RunSuccess:
		return "badge-success"
	case contract.RunFailed:
		return "badge-danger"
	case contract.RunRunning:
		return "badge-info"
	case contract.RunCancelled:
		return "badge-warn"
	}
	return "badge-muted"
}

func triggerLabel(r contract.JobRunInfo) string {
	switch r.Trigger {
	case "cron":
		if r.Detail == "catch-up" {
			return "Horario (recuperada)"
		}
		return "Horario"
	case "manual":
		if r.Detail != "" {
			return "Manual · " + r.Detail
		}
		return "Manual"
	case "snapshot":
		return "Datos nuevos"
	case "file":
		return "Archivo nuevo"
	}
	return r.Trigger
}

// tail returns the last n lines of s.
func tail(s string, n int) string {
	s = strings.TrimRight(s, "\n")
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// ---- DAG layout ----

// dagNode is one task to draw; Status is "" in the definition view.
type dagNode struct {
	ID, Type, Status string
	DependsOn        []string
}

type dagBox struct {
	ID, Type, Status string
	X, Y, W, H       int
}

type dagEdge struct{ Path, Status string }

// dagLayout is a left-to-right layered drawing: a task sits in the column of
// its longest dependency chain; inside a column tasks are ordered by the mean
// row of their dependencies, so edges cross rarely.
type dagLayout struct {
	Boxes         []dagBox
	Edges         []dagEdge
	Width, Height int
}

const (
	dagW, dagH   = 176, 46
	dagGX, dagGY = 52, 14
	dagPad       = 8
)

func layoutDAG(nodes []dagNode) dagLayout {
	if len(nodes) == 0 {
		return dagLayout{}
	}
	byID := map[string]*dagNode{}
	for i := range nodes {
		byID[nodes[i].ID] = &nodes[i]
	}
	depth := map[string]int{}
	var depthOf func(id string, stack map[string]bool) int
	depthOf = func(id string, stack map[string]bool) int {
		if d, ok := depth[id]; ok {
			return d
		}
		if stack[id] { // a cycle cannot come from a validated job; do not loop
			return 0
		}
		stack[id] = true
		d := 0
		for _, dep := range byID[id].DependsOn {
			if _, ok := byID[dep]; ok {
				d = max(d, depthOf(dep, stack)+1)
			}
		}
		delete(stack, id)
		depth[id] = d
		return d
	}
	maxD := 0
	for _, n := range nodes {
		maxD = max(maxD, depthOf(n.ID, map[string]bool{}))
	}
	cols := make([][]string, maxD+1)
	for _, n := range nodes { // declaration order inside a column to start with
		cols[depth[n.ID]] = append(cols[depth[n.ID]], n.ID)
	}
	row := map[string]float64{}
	for ci, c := range cols {
		if ci > 0 {
			bary := func(id string) float64 {
				sum, k := 0.0, 0
				for _, dep := range byID[id].DependsOn {
					if r, ok := row[dep]; ok {
						sum += r
						k++
					}
				}
				if k == 0 {
					return 1e9
				}
				return sum / float64(k)
			}
			sort.SliceStable(c, func(i, j int) bool { return bary(c[i]) < bary(c[j]) })
		}
		for i, id := range c {
			row[id] = float64(i)
		}
	}
	tallest := 0
	for _, c := range cols {
		tallest = max(tallest, len(c))
	}
	colH := func(n int) int { return n*dagH + (n-1)*dagGY }
	totalH := colH(tallest)
	pos := map[string]dagBox{}
	var out dagLayout
	for ci, c := range cols {
		y0 := dagPad + (totalH-colH(len(c)))/2
		for i, id := range c {
			n := byID[id]
			b := dagBox{ID: id, Type: n.Type, Status: n.Status, X: dagPad + ci*(dagW+dagGX), Y: y0 + i*(dagH+dagGY), W: dagW, H: dagH}
			pos[id] = b
			out.Boxes = append(out.Boxes, b)
		}
	}
	for _, n := range nodes {
		to := pos[n.ID]
		deps := append([]string(nil), n.DependsOn...)
		sort.Strings(deps)
		for _, d := range deps {
			from, ok := pos[d]
			if !ok {
				continue
			}
			x1, y1 := from.X+from.W, from.Y+from.H/2
			x2, y2 := to.X, to.Y+to.H/2
			mx := (x1 + x2) / 2
			out.Edges = append(out.Edges, dagEdge{
				Path:   fmt.Sprintf("M%d %d C%d %d %d %d %d %d", x1, y1, mx, y1, mx, y2, x2, y2),
				Status: byID[d].Status,
			})
		}
	}
	out.Width = dagPad*2 + len(cols)*dagW + (len(cols)-1)*dagGX
	out.Height = dagPad*2 + totalH
	return out
}

func dagFromDefs(ts []contract.JobTaskDef) []dagNode {
	out := make([]dagNode, len(ts))
	for i, t := range ts {
		out[i] = dagNode{ID: t.ID, Type: t.Type, DependsOn: t.DependsOn}
	}
	return out
}

func dagFromRun(ts []contract.JobTaskRun) []dagNode {
	out := make([]dagNode, len(ts))
	for i, t := range ts {
		out[i] = dagNode{ID: t.ID, Type: t.Type, Status: t.Status, DependsOn: t.DependsOn}
	}
	return out
}

// runLabel is the short, readable form of a run id (ids are microsecond
// timestamps, 19 digits): "#" and the last seven digits. The full id is in the URL.
func runLabel(id int64) string {
	s := strconv.FormatInt(id, 10)
	if len(s) > 7 {
		s = s[len(s)-7:]
	}
	return "#" + s
}

// orderTasks sorts tasks by dependency layer, as the graph shows them.
func orderTasks(ts []contract.JobTaskRun) []contract.JobTaskRun {
	l := layoutDAG(dagFromRun(ts))
	idx := map[string]int{}
	for i, b := range l.Boxes {
		idx[b.ID] = i
	}
	out := append([]contract.JobTaskRun(nil), ts...)
	sort.SliceStable(out, func(i, j int) bool {
		bi, bj := l.Boxes[idx[out[i].ID]], l.Boxes[idx[out[j].ID]]
		if bi.X != bj.X {
			return bi.X < bj.X
		}
		return bi.Y < bj.Y
	})
	return out
}

package project

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/alonsoburon/duct/core/pgconn"
)

// DefaultMLflowRequirement is the pinned pip requirement `duct mlflow` installs.
const DefaultMLflowRequirement = "mlflow==3.17.0"

// MLflowSchema is the Postgres schema of the MLflow backend store when the
// catalog is Postgres (ADR 0010).
const MLflowSchema = "mlflow"

// MLflowFile is the SQLite backend store of MLflow inside .duct/ (local catalog).
const MLflowFile = ".duct/mlflow.sqlite"

// MLflowSettings is the [mlflow] table of duct.toml, written by `duct enable mlflow`:
//
//	[mlflow]
//	listen      = "127.0.0.1:5000"        # loopback only
//	requirement = "mlflow==3.17.0"        # pip requirement of the managed venv
//
// Metadata lives in the same database as the catalog (.duct/mlflow.sqlite, or schema
// "mlflow" of the Postgres catalog); artifacts under <lake>/_mlflow.
//
//	sync          = true                  # `duct serve` copies runs to bronze.mlflow_* periodically
//	sync_interval = "5m"
//
//	[mlflow.serving]                      # online models behind `duct serve` (ADR 0010)
//	roles = ["ml_users"]                  # who may call /models/<name>/invocations (duct_admin always)
//	[mlflow.serving.models.churn]         # `duct mlflow serve-model models:/churn/1 --port 5001`
//	port  = 5001
//	roles = ["scoring"]                   # optional: replaces the table-wide roles for this model
type MLflowSettings struct {
	Listen       string         `toml:"listen"`
	Requirement  string         `toml:"requirement"`
	Sync         bool           `toml:"sync"`
	SyncInterval string         `toml:"sync_interval"`
	Serving      *MLflowServing `toml:"serving"`
}

// MLflowServing is [mlflow.serving]: which model servers `duct serve` proxies and who may call them.
type MLflowServing struct {
	Roles     []string                     `toml:"roles"`
	MaxBodyMB int                          `toml:"max_body_mb"` // request body cap, default 16
	Timeout   string                       `toml:"timeout"`     // per request, default "60s"
	Models    map[string]MLflowServedModel `toml:"models"`
}

// MLflowServedModel is one [mlflow.serving.models.<name>]: the loopback port of its `mlflow models serve`.
type MLflowServedModel struct {
	Port  int      `toml:"port"`
	Roles []string `toml:"roles"`
}

var servedModelName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ValidModelName reports whether name is usable in /models/<name>/.
func ValidModelName(name string) bool { return servedModelName.MatchString(name) }

// MLflowSyncInterval is how often `duct serve` copies MLflow to Bronze (default 5m, minimum 10s).
func (p *Project) MLflowSyncInterval() (time.Duration, error) {
	if p.Config.MLflow == nil || p.Config.MLflow.SyncInterval == "" {
		return 5 * time.Minute, nil
	}
	d, err := time.ParseDuration(p.Config.MLflow.SyncInterval)
	if err != nil || d < 10*time.Second {
		return 0, fmt.Errorf("[mlflow] sync_interval %q: want a duration of at least 10s", p.Config.MLflow.SyncInterval)
	}
	return d, nil
}

// MLflowServingLimits returns the request body cap in bytes and the per-request timeout.
func (p *Project) MLflowServingLimits() (int64, time.Duration, error) {
	maxBody, timeout := int64(16<<20), 60*time.Second
	if p.Config.MLflow == nil || p.Config.MLflow.Serving == nil {
		return maxBody, timeout, nil
	}
	sv := p.Config.MLflow.Serving
	if sv.MaxBodyMB < 0 {
		return 0, 0, errors.New("[mlflow.serving] max_body_mb must be positive")
	}
	if sv.MaxBodyMB > 0 {
		maxBody = int64(sv.MaxBodyMB) << 20
	}
	if sv.Timeout != "" {
		d, err := time.ParseDuration(sv.Timeout)
		if err != nil || d <= 0 {
			return 0, 0, fmt.Errorf("[mlflow.serving] timeout %q: want a positive duration", sv.Timeout)
		}
		timeout = d
	}
	return maxBody, timeout, nil
}

// MLflowServedModels validates [mlflow.serving.models] (names, loopback ports, distinct ports).
func (p *Project) MLflowServedModels() (map[string]MLflowServedModel, error) {
	if p.Config.MLflow == nil || p.Config.MLflow.Serving == nil {
		return nil, nil
	}
	seen := map[int]string{}
	for name, m := range p.Config.MLflow.Serving.Models {
		if !ValidModelName(name) {
			return nil, fmt.Errorf("[mlflow.serving.models.%s]: the name may only have letters, digits, . _ - (max 64)", name)
		}
		if m.Port < 1 || m.Port > 65535 {
			return nil, fmt.Errorf("[mlflow.serving.models.%s] port %d is not valid", name, m.Port)
		}
		if other, dup := seen[m.Port]; dup {
			return nil, fmt.Errorf("[mlflow.serving.models.%s] port %d is also used by %s", name, m.Port, other)
		}
		seen[m.Port] = name
	}
	return p.Config.MLflow.Serving.Models, nil
}

// MLflowListen is the address `duct mlflow` binds (default 127.0.0.1:5000); an
// error unless it is a loopback address.
func (p *Project) MLflowListen() (string, error) {
	addr := "127.0.0.1:5000"
	if p.Config.MLflow != nil && p.Config.MLflow.Listen != "" {
		addr = p.Config.MLflow.Listen
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("[mlflow] listen %q: %w", addr, err)
	}
	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return "", fmt.Errorf("[mlflow] listen %q: MLflow has no authentication, it may only listen on loopback", addr)
	}
	return addr, nil
}

// MLflowRequirement is the pip requirement for MLflow, with the Postgres driver
// when the catalog is Postgres.
func (p *Project) MLflowRequirement() []string {
	req := DefaultMLflowRequirement
	if p.Config.MLflow != nil && p.Config.MLflow.Requirement != "" {
		req = p.Config.MLflow.Requirement
	}
	out := []string{req}
	if p.CatalogIsPostgres() {
		out = append(out, "psycopg2-binary")
	}
	return out
}

// MLflowSQLitePath is the absolute path of the SQLite backend store.
func (p *Project) MLflowSQLitePath() string { return p.abs(MLflowFile) }

// MLflowBackendURI is the --backend-store-uri: a SQLite file next to the catalog
// or, with a Postgres catalog, the same database with search_path=mlflow. The
// Postgres one carries the password: pass it by environment, never in argv.
func (p *Project) MLflowBackendURI() (string, error) {
	if !p.CatalogIsPostgres() {
		return "sqlite:///" + filepath.ToSlash(p.MLflowSQLitePath()), nil
	}
	dsn, err := p.CatalogDSN()
	if err != nil {
		return "", err
	}
	par, err := pgconn.Parse(dsn)
	if err != nil {
		return "", err
	}
	u := url.URL{Scheme: "postgresql+psycopg2", Path: "/" + par["database"]}
	host := par["host"]
	if host == "" {
		host = "localhost"
	}
	if par["port"] != "" {
		host += ":" + par["port"]
	}
	u.Host = host
	if par["user"] != "" {
		if pw, ok := par["password"]; ok {
			u.User = url.UserPassword(par["user"], pw)
		} else {
			u.User = url.User(par["user"])
		}
	}
	q := url.Values{}
	q.Set("options", "-csearch_path="+MLflowSchema)
	if m := par["sslmode"]; m != "" {
		q.Set("sslmode", m)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// MLflowArtifactsDestination is <lake>/_mlflow as a URI (file:// or s3://).
func (p *Project) MLflowArtifactsDestination() string {
	if p.StorageIsS3() {
		return strings.TrimRight(p.Config.Storage, "/") + "/_mlflow"
	}
	return "file://" + filepath.ToSlash(filepath.Join(p.DataPath(), "_mlflow"))
}

// MLflowS3Env is the environment MLflow's S3 client needs for an s3:// lake
// (boto3 names). The MLflow server proxies artifacts, so only it holds them.
func (p *Project) MLflowS3Env() (map[string]string, error) {
	cfg, ok, err := p.S3()
	if err != nil || !ok {
		return nil, err
	}
	env := map[string]string{
		"AWS_ACCESS_KEY_ID":     cfg.KeyID,
		"AWS_SECRET_ACCESS_KEY": cfg.Secret,
	}
	if cfg.SessionToken != "" {
		env["AWS_SESSION_TOKEN"] = cfg.SessionToken
	}
	if cfg.Region != "" {
		env["AWS_DEFAULT_REGION"] = cfg.Region
	}
	if cfg.Endpoint != "" {
		scheme := "https"
		if !cfg.UseSSL {
			scheme = "http"
		}
		env["MLFLOW_S3_ENDPOINT_URL"] = scheme + "://" + cfg.Endpoint
	}
	return env, nil
}

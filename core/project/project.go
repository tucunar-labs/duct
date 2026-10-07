// Package project knows the on-disk layout of a Duct project (duct.toml, the
// lake, the SQLite catalog), creates it, and runs the Duct-owned tables
// migrations and maintenance through a contract.Engine. It has no cgo and no
// dependency on a concrete engine.
package project

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"

	contract "github.com/alonsoburon/duct/core/contract/v1"
	"github.com/alonsoburon/duct/core/pgconn"
	"github.com/alonsoburon/duct/core/s3conn"
)

// Environment variables for a Postgres catalog (keeps secrets out of duct.toml).
const (
	// EnvCatalogURL replaces the catalog of duct.toml with a full connection
	// string (password included).
	EnvCatalogURL = "DUCT_CATALOG_URL"
	// EnvCatalogPassword supplies the password when the connection string in
	// use carries none.
	EnvCatalogPassword = "DUCT_CATALOG_PASSWORD"
)

// File and directory names inside a project.
const (
	ConfigFile  = "duct.toml"
	StateDir    = ".duct"
	LakeDir     = "lake"
	CatalogFile = ".duct/catalog.sqlite"
	StateFile   = ".duct/duct.sqlite" // Duct-owned tables, apart from the DuckLake catalog
)

// Config is duct.toml.
type Config struct {
	// Storage is the lake data path: a local directory (relative to the
	// project) or a URL such as s3://bucket/prefix.
	Storage string `toml:"storage"`
	// Catalog is the SQLite file holding the DuckLake metadata, or a Postgres
	// connection string (postgres://user@host:5432/db, or
	// "postgres:dbname=... host=...") for a catalog shared by several machines.
	// The password goes in DUCT_CATALOG_PASSWORD, or the whole string in
	// DUCT_CATALOG_URL.
	Catalog string `toml:"catalog"`
	// S3 is the [s3] table: how to reach an s3:// storage. Never credentials.
	S3 S3Config `toml:"s3"`
	// Serve configures `duct serve` (the [serve] table).
	Serve ServeConfig `toml:"serve"`
	// Kafka, Stream and Alerts configure streaming and alerts (ADR 0003, 0009);
	// see streamconfig.go. Stream and Alerts are nil when the table is absent.
	Kafka  KafkaConfig     `toml:"kafka"`
	Stream *StreamSettings `toml:"stream"`
	Alerts *AlertsSettings `toml:"alerts"`
}

// S3Config is the [s3] table of duct.toml, used when storage is s3://...:
//
//	storage = "s3://lake/prod"
//	[s3]
//	endpoint  = "rustfs.internal:9000"   # host[:port]; empty = AWS S3
//	region    = "us-east-1"
//	url_style = "path"                   # path (RustFS, Garage) or vhost
//	use_ssl   = true                     # default true
//	role_arn  = "arn:aws:iam::duct:role/lake"  # optional: STS AssumeRole scoped to the prefix
//
// Credentials never go here: DUCT_S3_ACCESS_KEY_ID / DUCT_S3_SECRET_ACCESS_KEY /
// DUCT_S3_SESSION_TOKEN, or AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY /
// AWS_SESSION_TOKEN. The DUCT_S3_ENDPOINT, _REGION, _URL_STYLE, _USE_SSL and
// _ROLE_ARN variables override the table.
type S3Config struct {
	Endpoint string `toml:"endpoint"`
	Region   string `toml:"region"`
	URLStyle string `toml:"url_style"`
	UseSSL   *bool  `toml:"use_ssl"`
	RoleARN  string `toml:"role_arn"`
}

// ServeConfig is the [serve] table of duct.toml:
//
//	[serve]
//	protocols = ["pg", "http"]       # default: every linked protocol
//	[serve.pg]
//	listen = "127.0.0.1:5433"        # module-defined keys, handed to Init
//	[serve.http]
//	listen = "127.0.0.1:8123"
type ServeConfig struct {
	Protocols []string `toml:"protocols"`
	// Modules holds the per-protocol sub-tables, keyed by protocol name.
	Modules map[string]map[string]any `toml:"-"`
}

// Project is a loaded project rooted at Root (absolute).
type Project struct {
	Root   string
	Config Config

	catalogOverride string // in-process Postgres connection string (init/migrate)
}

// Load reads dir/duct.toml.
func Load(dir string) (*Project, error) {
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if _, err := toml.DecodeFile(filepath.Join(root, ConfigFile), &cfg); err != nil {
		return nil, fmt.Errorf("read %s: %w", ConfigFile, err)
	}
	if cfg.Storage == "" || cfg.Catalog == "" {
		return nil, fmt.Errorf("%s: storage and catalog are required", ConfigFile)
	}
	// [serve.<protocol>] sub-tables are free-form per module.
	var raw struct {
		Serve map[string]any `toml:"serve"`
	}
	if _, err := toml.DecodeFile(filepath.Join(root, ConfigFile), &raw); err != nil {
		return nil, fmt.Errorf("read %s: %w", ConfigFile, err)
	}
	for k, v := range raw.Serve {
		if m, ok := v.(map[string]any); ok {
			if cfg.Serve.Modules == nil {
				cfg.Serve.Modules = map[string]map[string]any{}
			}
			cfg.Serve.Modules[k] = m
		}
	}
	if s3conn.Scheme(cfg.Storage) != "" {
		if _, _, err := s3conn.ParseStorage(cfg.Storage); err != nil {
			return nil, fmt.Errorf("%s: storage: only s3://bucket/prefix is supported as remote storage", ConfigFile)
		}
	}
	if pgconn.IsDSN(cfg.Catalog) {
		if _, err := pgconn.Parse(cfg.Catalog); err != nil {
			return nil, fmt.Errorf("%s: catalog: %w", ConfigFile, err)
		}
	}
	return &Project{Root: root, Config: cfg}, nil
}

// Find walks up from start until it finds a duct.toml.
func Find(start string) (*Project, error) {
	dir, err := filepath.Abs(start)
	if err != nil {
		return nil, err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, ConfigFile)); err == nil {
			return Load(dir)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil, errors.New("not inside a Duct project (no duct.toml found); run `duct init <dir>`")
		}
		dir = parent
	}
}

func (p *Project) abs(path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(p.Root, path)
}

// CatalogPath is the absolute path of the SQLite catalog ("" for a Postgres
// catalog).
func (p *Project) CatalogPath() string {
	if p.CatalogIsPostgres() {
		return ""
	}
	return p.abs(p.Config.Catalog)
}

// CatalogIsPostgres reports whether the DuckLake catalog lives in Postgres
// (duct.toml catalog, or DUCT_CATALOG_URL).
func (p *Project) CatalogIsPostgres() bool {
	return pgconn.IsDSN(p.catalogSetting())
}

// catalogSetting is the effective catalog before the password is applied.
func (p *Project) catalogSetting() string {
	if p.catalogOverride != "" {
		return p.catalogOverride
	}
	if u := os.Getenv(EnvCatalogURL); u != "" {
		return u
	}
	return p.Config.Catalog
}

// UseCatalogURL makes this process use dsn (password included) as the catalog,
// without touching duct.toml. Used by `duct init --catalog` and `duct catalog
// migrate`, which receive the password on the command line.
func (p *Project) UseCatalogURL(dsn string) error {
	if _, err := pgconn.Parse(dsn); err != nil {
		return err
	}
	p.catalogOverride = dsn
	return nil
}

// CatalogDSN is the connection string of a Postgres catalog with the password
// applied (from the string itself or DUCT_CATALOG_PASSWORD). It is a secret:
// hand it to the engine, never print it. Use CatalogRef for messages.
func (p *Project) CatalogDSN() (string, error) {
	dsn := p.catalogSetting()
	if !pgconn.IsDSN(dsn) {
		return "", errors.New("the catalog is not a Postgres connection string")
	}
	return pgconn.WithPassword(dsn, os.Getenv(EnvCatalogPassword))
}

// CatalogRef describes the catalog without secrets: the SQLite path or
// user@host:port/database.
func (p *Project) CatalogRef() string {
	if !p.CatalogIsPostgres() {
		return p.CatalogPath()
	}
	if params, err := pgconn.Parse(p.catalogSetting()); err == nil {
		return "postgres " + params.Describe()
	}
	return "postgres (invalid connection string)"
}

// LakeCatalogRef is the catalog value for a module that opens its own session
// on the lake (serve-pg): the SQLite path, or the Postgres connection string
// with the password applied (a secret: keep it out of logs).
func (p *Project) LakeCatalogRef() string {
	if p.CatalogIsPostgres() {
		dsn, err := p.CatalogDSN()
		if err != nil {
			return p.catalogSetting()
		}
		return dsn
	}
	return p.CatalogPath()
}

// StatePath is the absolute path of the SQLite file with Duct-owned tables.
func (p *Project) StatePath() string { return p.abs(StateFile) }

// StorageIsS3 reports whether the lake lives in an S3-compatible store.
func (p *Project) StorageIsS3() bool { return s3conn.Scheme(p.Config.Storage) == "s3" }

func firstEnv(names ...string) string {
	for _, n := range names {
		if v := os.Getenv(n); v != "" {
			return v
		}
	}
	return ""
}

// S3 is the object-store connection: the [s3] table overridden by DUCT_S3_*,
// credentials from DUCT_S3_* or AWS_*. ok is false for local storage.
func (p *Project) S3() (cfg s3conn.Config, ok bool, err error) {
	if !p.StorageIsS3() {
		return cfg, false, nil
	}
	t := p.Config.S3
	cfg = s3conn.Config{
		Endpoint:     firstEnv(s3conn.EnvEndpoint),
		Region:       firstEnv(s3conn.EnvRegion, "AWS_REGION", "AWS_DEFAULT_REGION"),
		URLStyle:     firstEnv(s3conn.EnvURLStyle),
		RoleARN:      firstEnv(s3conn.EnvRoleARN),
		UseSSL:       true,
		KeyID:        firstEnv(s3conn.EnvKeyID, "AWS_ACCESS_KEY_ID"),
		Secret:       firstEnv(s3conn.EnvSecret, "AWS_SECRET_ACCESS_KEY"),
		SessionToken: firstEnv(s3conn.EnvSessionToken, "AWS_SESSION_TOKEN"),
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = t.Endpoint
	}
	if cfg.Region == "" {
		cfg.Region = t.Region
	}
	if cfg.URLStyle == "" {
		cfg.URLStyle = t.URLStyle
	}
	if cfg.RoleARN == "" {
		cfg.RoleARN = t.RoleARN
	}
	if t.UseSSL != nil {
		cfg.UseSSL = *t.UseSSL
	}
	if v := os.Getenv(s3conn.EnvUseSSL); v != "" {
		switch strings.ToLower(v) {
		case "1", "true", "yes":
			cfg.UseSSL = true
		case "0", "false", "no":
			cfg.UseSSL = false
		default:
			return cfg, true, fmt.Errorf("%s must be true or false", s3conn.EnvUseSSL)
		}
	}
	if cfg.Endpoint != "" && cfg.URLStyle == "" {
		cfg.URLStyle = "path" // custom endpoints (RustFS, Garage) are path style
	}
	return cfg, true, cfg.Validate()
}

// DataPath is the absolute local lake directory, or the URL for remote storage.
func (p *Project) DataPath() string {
	if strings.Contains(p.Config.Storage, "://") {
		return p.Config.Storage
	}
	return p.abs(p.Config.Storage)
}

// EngineConfig is the Init config for contract.Engine (DuckDB engine keys).
//
// With a Postgres catalog the Duct-owned tables live in schema "duct" of the
// same database, so every machine sharing the catalog shares users, grants and
// runs; otherwise they stay in .duct/duct.sqlite.
func (p *Project) EngineConfig(locked bool) contract.Config {
	cfg := contract.Config{
		"catalog":   p.LakeCatalogRef(),
		"data_path": p.DataPath(),
		"state":     p.StatePath(),
		"locked":    locked,
	}
	if p.CatalogIsPostgres() {
		cfg["state"] = cfg["catalog"]
	}
	p.addS3(cfg)
	return cfg
}

// addS3 adds the "s3" key (an s3conn.Config) for an s3:// storage. A
// configuration error travels in "s3_error" and is reported by the module's
// Init (EngineConfig cannot fail).
func (p *Project) addS3(cfg contract.Config) {
	s3, ok, err := p.S3()
	if !ok {
		return
	}
	cfg["s3"] = s3
	if err != nil {
		cfg["s3_error"] = err.Error()
	}
}

// ModuleConfig adds the lake location to a protocol module's config: catalog,
// data_path and, for s3:// storage, s3.
func (p *Project) ModuleConfig(cfg contract.Config) {
	cfg["catalog"] = p.LakeCatalogRef()
	cfg["data_path"] = p.DataPath()
	p.addS3(cfg)
}

const gitignore = `# Duct local state and secrets
lake/
.duct/
.env*
!.env.example
`

// Scaffold creates the project layout in dir (duct.toml, lake/, .duct/,
// .gitignore). It refuses to overwrite an existing duct.toml. Existing
// .gitignore lines are kept and missing ones appended.
func Scaffold(dir string) (*Project, error) {
	p, _, err := ScaffoldWithCatalog(dir, "")
	return p, err
}

// ScaffoldOptions tunes ScaffoldWith.
type ScaffoldOptions struct {
	// Catalog is a Postgres connection string (empty: SQLite). A password in it
	// is stripped from duct.toml.
	Catalog string
	// Storage is "" or "./lake" for the local lake, or s3://bucket/prefix.
	Storage string
	// S3 fills the [s3] table (only with s3:// storage).
	S3 S3Config
}

// ScaffoldWithCatalog is Scaffold with a Postgres catalog connection string
// (empty: SQLite). A password in it is not written to duct.toml: the returned
// project uses the full string for this process only, and stripped is true so
// the caller can tell the user to export DUCT_CATALOG_PASSWORD.
func ScaffoldWithCatalog(dir, catalog string) (p *Project, stripped bool, err error) {
	return ScaffoldWith(dir, ScaffoldOptions{Catalog: catalog})
}

// ScaffoldWith is ScaffoldWithCatalog with a storage choice. With s3:// storage
// no lake/ folder is created and duct.toml gets an [s3] table (no credentials).
func ScaffoldWith(dir string, o ScaffoldOptions) (p *Project, stripped bool, err error) {
	catalog := o.Catalog
	storage := o.Storage
	if storage == "" {
		storage = "./lake"
	}
	remote := s3conn.Scheme(storage) != ""
	if remote {
		if _, _, err := s3conn.ParseStorage(storage); err != nil {
			return nil, false, errors.New("storage must be a local folder or s3://bucket/prefix")
		}
	}
	catLine := fmt.Sprintf("catalog = %q   # DuckLake catalog (SQLite, WAL)\n", CatalogFile)
	full := ""
	if catalog != "" {
		clean, had, err := pgconn.StripPassword(catalog)
		if err != nil {
			return nil, false, err
		}
		if had {
			full = catalog
		}
		stripped = had
		catLine = fmt.Sprintf("catalog = %q  # DuckLake catalog in Postgres; password in DUCT_CATALOG_PASSWORD\n", clean)
	}
	toml := "# Duct project. Paths are relative to this file.\n" +
		fmt.Sprintf("storage = %q   # data files (Parquet); or s3://bucket/prefix\n", storage) + catLine
	if remote {
		toml += s3Template(o.S3)
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, false, err
	}
	cfgPath := filepath.Join(root, ConfigFile)
	if _, err := os.Stat(cfgPath); err == nil {
		return nil, false, fmt.Errorf("%s already exists: this folder is already a Duct project", cfgPath)
	}
	dirs := []string{root, filepath.Join(root, StateDir)}
	if !remote {
		dirs = append(dirs, filepath.Join(root, storage))
	}
	for _, d := range dirs {
		if err := os.MkdirAll(d, 0o750); err != nil {
			return nil, false, err
		}
	}
	if err := os.WriteFile(cfgPath, []byte(toml), 0o600); err != nil {
		return nil, false, err
	}
	if err := writeGitignore(filepath.Join(root, ".gitignore")); err != nil {
		return nil, false, err
	}
	p, err = Load(root)
	if err != nil {
		return nil, false, err
	}
	if full != "" {
		if err := p.UseCatalogURL(full); err != nil {
			return nil, false, err
		}
	}
	return p, stripped, nil
}

// s3Template is the [s3] table written by `duct init --storage s3://...`.
func s3Template(c S3Config) string {
	var b strings.Builder
	b.WriteString("\n# Object store. Credentials: DUCT_S3_ACCESS_KEY_ID / DUCT_S3_SECRET_ACCESS_KEY\n# (or AWS_*), never in this file.\n[s3]\n")
	if c.Endpoint != "" {
		fmt.Fprintf(&b, "endpoint  = %q\n", c.Endpoint)
	} else {
		b.WriteString("# endpoint = \"host:9000\"   # empty = AWS S3\n")
	}
	if c.Region != "" {
		fmt.Fprintf(&b, "region    = %q\n", c.Region)
	}
	if c.URLStyle != "" {
		fmt.Fprintf(&b, "url_style = %q\n", c.URLStyle)
	}
	if c.UseSSL != nil {
		fmt.Fprintf(&b, "use_ssl   = %t\n", *c.UseSSL)
	}
	if c.RoleARN != "" {
		fmt.Fprintf(&b, "role_arn  = %q\n", c.RoleARN)
	}
	return b.String()
}

func writeGitignore(path string) error {
	existing, err := os.ReadFile(path) //nolint:gosec // path built from the project root
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	have := map[string]bool{}
	for _, l := range strings.Split(string(existing), "\n") {
		have[strings.TrimSpace(l)] = true
	}
	var add []string
	for _, l := range strings.Split(strings.TrimSpace(gitignore), "\n") {
		if !have[l] {
			add = append(add, l)
		}
	}
	if len(add) == 0 {
		return nil
	}
	out := string(existing)
	if out != "" && !strings.HasSuffix(out, "\n") {
		out += "\n"
	}
	out += strings.Join(add, "\n") + "\n"
	return os.WriteFile(path, []byte(out), 0o600)
}

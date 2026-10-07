package main

// Modules linked into the binary. Add or remove a blank import to change the build.
import (
	_ "github.com/alonsoburon/duct/modules/channel-webhook"
	_ "github.com/alonsoburon/duct/modules/engine-duckdb"
	_ "github.com/alonsoburon/duct/modules/serve-http"
	_ "github.com/alonsoburon/duct/modules/serve-pg"
	_ "github.com/alonsoburon/duct/modules/source-bento"
	_ "github.com/alonsoburon/duct/modules/source-files"
	_ "github.com/alonsoburon/duct/modules/source-odbc"
	_ "github.com/alonsoburon/duct/modules/web"
)

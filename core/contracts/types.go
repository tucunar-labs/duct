package contracts

import (
	"fmt"
	"strings"

	odcs "github.com/alonsoburon/odcs-go"
)

// Column is one contract property as Duct sees it.
type Column struct {
	Name     string // physical column name in the lake
	Logical  string
	Physical string
	Required bool
}

// Columns returns the top-level properties of the contract table.
func (c *Contract) Columns() []Column {
	out := make([]Column, 0, len(c.Object.Properties))
	for _, p := range c.Object.Properties {
		out = append(out, Column{Name: colName(p), Logical: p.LogicalType, Physical: p.PhysicalType, Required: p.Required || p.PrimaryKey})
	}
	return out
}

func colName(p odcs.SchemaProperty) string {
	if p.PhysicalName != "" {
		return p.PhysicalName
	}
	return p.Name
}

// physicalAlias maps the spellings of a physical type to DuckDB's canonical name.
var physicalAlias = map[string]string{
	"INT": "INTEGER", "INT4": "INTEGER", "SIGNED": "INTEGER",
	"INT8": "BIGINT", "LONG": "BIGINT", "INT64": "BIGINT",
	"INT2": "SMALLINT", "SHORT": "SMALLINT", "INT1": "TINYINT",
	"TEXT": "VARCHAR", "STRING": "VARCHAR", "CHAR": "VARCHAR", "BPCHAR": "VARCHAR", "NVARCHAR": "VARCHAR",
	"FLOAT8": "DOUBLE", "DOUBLE PRECISION": "DOUBLE", "FLOAT4": "FLOAT", "REAL": "FLOAT",
	"NUMERIC": "DECIMAL", "BOOL": "BOOLEAN", "LOGICAL": "BOOLEAN",
	"DATETIME": "TIMESTAMP", "TIMESTAMP_NTZ": "TIMESTAMP",
	"TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_LTZ": "TIMESTAMP WITH TIME ZONE",
	"TIMETZ": "TIME WITH TIME ZONE",
}

func baseType(t string) string {
	t = strings.ToUpper(strings.TrimSpace(t))
	if i := strings.IndexAny(t, "(["); i >= 0 {
		t = strings.TrimSpace(t[:i])
	}
	if a, ok := physicalAlias[t]; ok {
		return a
	}
	return t
}

var (
	intTypes = map[string]bool{"TINYINT": true, "SMALLINT": true, "INTEGER": true, "BIGINT": true, "HUGEINT": true,
		"UTINYINT": true, "USMALLINT": true, "UINTEGER": true, "UBIGINT": true, "UHUGEINT": true}
	realTypes = map[string]bool{"FLOAT": true, "DOUBLE": true, "DECIMAL": true}
	tsTypes   = map[string]bool{"TIMESTAMP": true, "TIMESTAMP WITH TIME ZONE": true, "TIMESTAMP_S": true,
		"TIMESTAMP_MS": true, "TIMESTAMP_NS": true}
)

// TypeCompatible reports whether a DuckDB column type (as printed by DESCRIBE,
// e.g. "DECIMAL(18,3)") satisfies a contract property. A physicalType wins when
// given (aliases and length parameters are ignored, except DECIMAL(p,s));
// otherwise the ODCS logicalType decides: string, integer, number (integers
// allowed), boolean, date, timestamp, time, object (STRUCT/MAP/JSON), array.
func TypeCompatible(logical, physical, duck string) bool {
	d := strings.ToUpper(strings.TrimSpace(duck))
	db := baseType(d)
	if strings.HasSuffix(d, "]") && !strings.HasPrefix(d, "STRUCT") && !strings.HasPrefix(d, "MAP") {
		db = "LIST"
	}
	if strings.HasPrefix(d, "STRUCT") {
		db = "STRUCT"
	}
	if strings.HasPrefix(d, "MAP") {
		db = "MAP"
	}
	if physical != "" {
		pb := baseType(physical)
		if strings.HasSuffix(strings.TrimSpace(physical), "]") || pb == "ARRAY" || pb == "LIST" {
			return db == "LIST"
		}
		if pb == "DECIMAL" && strings.Contains(physical, "(") && strings.Contains(d, "(") {
			return db == "DECIMAL" && strings.ReplaceAll(strings.ToUpper(physical), " ", "") == strings.ReplaceAll(d, " ", "")
		}
		return pb == db
	}
	switch strings.ToLower(logical) {
	case "":
		return true
	case "string":
		return db == "VARCHAR"
	case "integer":
		return intTypes[db]
	case "number":
		return intTypes[db] || realTypes[db]
	case "boolean":
		return db == "BOOLEAN"
	case "date":
		return db == "DATE"
	case "timestamp":
		return tsTypes[db]
	case "time":
		return db == "TIME" || db == "TIME WITH TIME ZONE"
	case "object":
		return db == "STRUCT" || db == "MAP" || db == "JSON"
	case "array":
		return db == "LIST"
	}
	return false
}

// SchemaIssues compares the contract columns with the actual ones
// (name -> DuckDB type, in table order) and returns one message per mismatch:
// missing columns, type mismatches and columns the contract does not declare.
func (c *Contract) SchemaIssues(actual []ActualColumn) []string {
	by := map[string]ActualColumn{}
	for _, a := range actual {
		by[strings.ToLower(a.Name)] = a
	}
	declared := map[string]bool{}
	var out []string
	for _, col := range c.Columns() {
		declared[strings.ToLower(col.Name)] = true
		a, ok := by[strings.ToLower(col.Name)]
		if !ok {
			out = append(out, fmt.Sprintf("column %q is in the contract but not in the table", col.Name))
			continue
		}
		if !TypeCompatible(col.Logical, col.Physical, a.Type) {
			want := col.Physical
			if want == "" {
				want = col.Logical
			}
			out = append(out, fmt.Sprintf("column %q has type %s, the contract says %s", col.Name, a.Type, want))
		}
	}
	for _, a := range actual {
		if !declared[strings.ToLower(a.Name)] {
			out = append(out, fmt.Sprintf("column %q (%s) is in the table but not in the contract", a.Name, a.Type))
		}
	}
	return out
}

// ActualColumn is a column of the real table.
type ActualColumn struct{ Name, Type string }

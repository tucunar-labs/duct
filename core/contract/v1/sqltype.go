package contract

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
)

// SQLType is a parsed UDF type name (ADR 0005): the scalars of ScalarFunction
// plus DATE, TIMESTAMP (microseconds, no zone), DECIMAL(p,s), lists (`T[]`) and
// STRUCT(name T, ...). Python (duct.udf.types), the executables and the engine
// all use this one grammar; String() is the canonical spelling.
type SQLType struct {
	Kind         string // BOOLEAN TINYINT SMALLINT INTEGER BIGINT FLOAT DOUBLE VARCHAR DATE TIMESTAMP DECIMAL LIST STRUCT
	Width, Scale uint8  // DECIMAL
	Elem         *SQLType
	Fields       []SQLField // STRUCT
}

// SQLField is one member of a STRUCT.
type SQLField struct {
	Name string
	Type *SQLType
}

var sqlAliases = map[string]string{
	"BOOL": "BOOLEAN", "INT1": "TINYINT", "INT2": "SMALLINT", "INT": "INTEGER", "INT4": "INTEGER",
	"INT8": "BIGINT", "REAL": "FLOAT", "FLOAT4": "FLOAT", "FLOAT8": "DOUBLE", "STRING": "VARCHAR",
	"TEXT": "VARCHAR", "NUMERIC": "DECIMAL",
}

var sqlScalars = map[string]bool{
	"BOOLEAN": true, "TINYINT": true, "SMALLINT": true, "INTEGER": true, "BIGINT": true,
	"FLOAT": true, "DOUBLE": true, "VARCHAR": true, "DATE": true, "TIMESTAMP": true,
}

// IsScalar reports whether t is neither a list nor a struct.
func (t *SQLType) IsScalar() bool { return t.Kind != "LIST" && t.Kind != "STRUCT" }

// String is the canonical spelling.
func (t *SQLType) String() string {
	switch t.Kind {
	case "DECIMAL":
		return fmt.Sprintf("DECIMAL(%d,%d)", t.Width, t.Scale)
	case "LIST":
		return t.Elem.String() + "[]"
	case "STRUCT":
		parts := make([]string, len(t.Fields))
		for i, f := range t.Fields {
			parts[i] = quoteIdent(f.Name) + " " + f.Type.String()
		}
		return "STRUCT(" + strings.Join(parts, ", ") + ")"
	}
	return t.Kind
}

func quoteIdent(s string) string {
	ok := s != ""
	for i, r := range s {
		if r != '_' && !unicode.IsLetter(r) && (i == 0 || !unicode.IsDigit(r)) {
			ok = false
		}
	}
	if ok {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// Arrow returns the Arrow type that carries values of t between the engine and
// the function body.
func (t *SQLType) Arrow() arrow.DataType {
	switch t.Kind {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16
	case "INTEGER":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64
	case "DATE":
		return arrow.FixedWidthTypes.Date32
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	case "DECIMAL":
		return &arrow.Decimal128Type{Precision: int32(t.Width), Scale: int32(t.Scale)}
	case "LIST":
		return arrow.ListOf(t.Elem.Arrow())
	case "STRUCT":
		fs := make([]arrow.Field, len(t.Fields))
		for i, f := range t.Fields {
			fs[i] = arrow.Field{Name: f.Name, Type: f.Type.Arrow(), Nullable: true}
		}
		return arrow.StructOf(fs...)
	}
	return arrow.BinaryTypes.String
}

// ParseSQLType parses a type name (case-insensitive, aliases accepted).
func ParseSQLType(s string) (*SQLType, error) {
	p := &typeParser{s: s}
	t, err := p.parse()
	if err != nil {
		return nil, fmt.Errorf("type %q: %w", s, err)
	}
	p.skip()
	if p.i != len(p.s) {
		return nil, fmt.Errorf("type %q: unexpected %q", s, p.s[p.i:])
	}
	return t, nil
}

type typeParser struct {
	s string
	i int
}

func (p *typeParser) skip() {
	for p.i < len(p.s) && (p.s[p.i] == ' ' || p.s[p.i] == '\t') {
		p.i++
	}
}

func (p *typeParser) peek() byte {
	p.skip()
	if p.i < len(p.s) {
		return p.s[p.i]
	}
	return 0
}

func (p *typeParser) eat(c byte) bool {
	if p.peek() == c {
		p.i++
		return true
	}
	return false
}

func (p *typeParser) word() string {
	p.skip()
	st := p.i
	for p.i < len(p.s) && (p.s[p.i] == '_' || unicode.IsLetter(rune(p.s[p.i])) || unicode.IsDigit(rune(p.s[p.i]))) {
		p.i++
	}
	return p.s[st:p.i]
}

func (p *typeParser) number() (uint8, error) {
	p.skip()
	st := p.i
	for p.i < len(p.s) && p.s[p.i] >= '0' && p.s[p.i] <= '9' {
		p.i++
	}
	n, err := strconv.ParseUint(p.s[st:p.i], 10, 8)
	if err != nil {
		return 0, fmt.Errorf("expected a number at %d", st)
	}
	return uint8(n), nil
}

func (p *typeParser) parse() (*SQLType, error) {
	w := strings.ToUpper(p.word())
	if w == "" {
		return nil, fmt.Errorf("expected a type name at %d", p.i)
	}
	if a, ok := sqlAliases[w]; ok {
		w = a
	}
	t := &SQLType{Kind: w}
	switch {
	case w == "DECIMAL":
		t.Width, t.Scale = 18, 3
		if p.eat('(') {
			var err error
			if t.Width, err = p.number(); err != nil {
				return nil, err
			}
			t.Scale = 0
			if p.eat(',') {
				if t.Scale, err = p.number(); err != nil {
					return nil, err
				}
			}
			if !p.eat(')') {
				return nil, fmt.Errorf("expected ) at %d", p.i)
			}
		}
		if t.Width < 1 || t.Width > 38 || t.Scale > t.Width {
			return nil, fmt.Errorf("DECIMAL(%d,%d) is out of range (width 1-38, scale <= width)", t.Width, t.Scale)
		}
	case w == "STRUCT":
		if !p.eat('(') {
			return nil, fmt.Errorf("STRUCT needs (name TYPE, ...)")
		}
		for {
			name, err := p.fieldName()
			if err != nil {
				return nil, err
			}
			ft, err := p.parse()
			if err != nil {
				return nil, err
			}
			t.Fields = append(t.Fields, SQLField{name, ft})
			if p.eat(',') {
				continue
			}
			if p.eat(')') {
				break
			}
			return nil, fmt.Errorf("expected , or ) at %d", p.i)
		}
	case sqlScalars[w]:
	default:
		return nil, fmt.Errorf("unsupported type %q (supported: BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, VARCHAR, DATE, TIMESTAMP, DECIMAL(p,s), T[], STRUCT(name T, ...))", w)
	}
	for p.peek() == '[' {
		p.i++
		if !p.eat(']') {
			return nil, fmt.Errorf("expected ] at %d", p.i)
		}
		t = &SQLType{Kind: "LIST", Elem: t}
	}
	return t, nil
}

func (p *typeParser) fieldName() (string, error) {
	if p.peek() == '"' {
		p.i++
		var b strings.Builder
		for p.i < len(p.s) {
			c := p.s[p.i]
			p.i++
			if c == '"' {
				if p.i < len(p.s) && p.s[p.i] == '"' {
					b.WriteByte('"')
					p.i++
					continue
				}
				return b.String(), nil
			}
			b.WriteByte(c)
		}
		return "", fmt.Errorf("unterminated quoted name")
	}
	n := p.word()
	if n == "" {
		return "", fmt.Errorf("expected a field name at %d", p.i)
	}
	return n, nil
}

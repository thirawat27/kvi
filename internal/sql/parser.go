package sql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Parser parses SQL-like queries
type Parser struct{}

// Statement represents a parsed SQL statement
type Statement struct {
	Type        StmtType
	Table       string
	Columns     []string
	Values      []interface{}
	Where       *Condition
	OrderBy     []OrderClause
	Limit       int
	VectorQuery *VectorClause // Kvi-specific
}

// StmtType represents the type of SQL statement
type StmtType int

const (
	Select StmtType = iota
	Insert
	Update
	Delete
	CreateTable
	VectorSearch // Kvi-specific
)

func (s StmtType) String() string {
	switch s {
	case Select:
		return "SELECT"
	case Insert:
		return "INSERT"
	case Update:
		return "UPDATE"
	case Delete:
		return "DELETE"
	case CreateTable:
		return "CREATE TABLE"
	case VectorSearch:
		return "VECTOR SEARCH"
	default:
		return "UNKNOWN"
	}
}

// Condition represents a WHERE condition
type Condition struct {
	Column   string
	Operator string
	Value    interface{}
	And      []*Condition
	Or       []*Condition
}

// OrderClause represents ORDER BY clause
type OrderClause struct {
	Column    string
	Direction string // ASC or DESC
}

// VectorClause represents vector search clause
type VectorClause struct {
	Vector []float32
	K      int
}

// NewParser creates a new parser
func NewParser() *Parser {
	return &Parser{}
}

// Parse parses a SQL string into a Statement
func (p *Parser) Parse(query string) (*Statement, error) {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, fmt.Errorf("empty query")
	}

	// Determine statement type
	upperQuery := strings.ToUpper(query)
	
	if strings.HasPrefix(upperQuery, "SELECT") {
		return p.parseSelect(query)
	} else if strings.HasPrefix(upperQuery, "INSERT") {
		return p.parseInsert(query)
	} else if strings.HasPrefix(upperQuery, "UPDATE") {
		return p.parseUpdate(query)
	} else if strings.HasPrefix(upperQuery, "DELETE") {
		return p.parseDelete(query)
	} else if strings.HasPrefix(upperQuery, "CREATE") {
		return p.parseCreate(query)
	} else if strings.HasPrefix(upperQuery, "VECTOR") {
		return p.parseVectorSearch(query)
	}

	return nil, fmt.Errorf("unsupported statement: %s", strings.Split(query, " ")[0])
}

// parseSelect parses SELECT statements
func (p *Parser) parseSelect(query string) (*Statement, error) {
	stmt := &Statement{Type: Select}

	// Pattern: SELECT columns FROM table [WHERE condition] [ORDER BY column] [LIMIT n]
	selectPattern := regexp.MustCompile(`(?i)SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?)?(?:\s+LIMIT\s+(\d+))?$`)

	matches := selectPattern.FindStringSubmatch(query)
	if matches == nil {
		return nil, fmt.Errorf("invalid SELECT syntax")
	}

	// Parse columns
	columnsStr := strings.TrimSpace(matches[1])
	if columnsStr == "*" {
		stmt.Columns = []string{"*"}
	} else {
		stmt.Columns = strings.Split(columnsStr, ",")
		for i, col := range stmt.Columns {
			stmt.Columns[i] = strings.TrimSpace(col)
		}
	}

	// Parse table
	stmt.Table = strings.TrimSpace(matches[2])

	// Parse WHERE
	if matches[3] != "" {
		cond, err := p.parseCondition(matches[3])
		if err != nil {
			return nil, err
		}
		stmt.Where = cond
	}

	// Parse ORDER BY
	if matches[4] != "" {
		order := OrderClause{Column: strings.TrimSpace(matches[4])}
		if matches[5] != "" {
			order.Direction = strings.ToUpper(matches[5])
		} else {
			order.Direction = "ASC"
		}
		stmt.OrderBy = []OrderClause{order}
	}

	// Parse LIMIT
	if matches[6] != "" {
		limit, err := strconv.Atoi(matches[6])
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT value")
		}
		stmt.Limit = limit
	}

	return stmt, nil
}

// parseInsert parses INSERT statements
func (p *Parser) parseInsert(query string) (*Statement, error) {
	stmt := &Statement{Type: Insert}

	// Pattern: INSERT INTO table (columns) VALUES (values)
	insertPattern := regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\w+)\s*\((.+?)\)\s*VALUES\s*\((.+?)\)$`)

	matches := insertPattern.FindStringSubmatch(query)
	if matches == nil {
		return nil, fmt.Errorf("invalid INSERT syntax")
	}

	stmt.Table = strings.TrimSpace(matches[1])

	// Parse columns
	columns := strings.Split(matches[2], ",")
	stmt.Columns = make([]string, len(columns))
	for i, col := range columns {
		stmt.Columns[i] = strings.TrimSpace(col)
	}

	// Parse values
	values, err := p.parseValues(matches[3])
	if err != nil {
		return nil, err
	}
	stmt.Values = values

	return stmt, nil
}

// parseUpdate parses UPDATE statements
func (p *Parser) parseUpdate(query string) (*Statement, error) {
	stmt := &Statement{Type: Update}

	// Pattern: UPDATE table SET column = value [WHERE condition]
	updatePattern := regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$`)

	matches := updatePattern.FindStringSubmatch(query)
	if matches == nil {
		return nil, fmt.Errorf("invalid UPDATE syntax")
	}

	stmt.Table = strings.TrimSpace(matches[1])

	// Parse SET clause (simplified: single column)
	setParts := strings.Split(matches[2], "=")
	if len(setParts) != 2 {
		return nil, fmt.Errorf("invalid SET clause")
	}
	stmt.Columns = []string{strings.TrimSpace(setParts[0])}
	val, err := p.parseValue(strings.TrimSpace(setParts[1]))
	if err != nil {
		return nil, err
	}
	stmt.Values = []interface{}{val}

	// Parse WHERE
	if matches[3] != "" {
		cond, err := p.parseCondition(matches[3])
		if err != nil {
			return nil, err
		}
		stmt.Where = cond
	}

	return stmt, nil
}

// parseDelete parses DELETE statements
func (p *Parser) parseDelete(query string) (*Statement, error) {
	stmt := &Statement{Type: Delete}

	// Pattern: DELETE FROM table [WHERE condition]
	deletePattern := regexp.MustCompile(`(?i)DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$`)

	matches := deletePattern.FindStringSubmatch(query)
	if matches == nil {
		return nil, fmt.Errorf("invalid DELETE syntax")
	}

	stmt.Table = strings.TrimSpace(matches[1])

	// Parse WHERE
	if matches[2] != "" {
		cond, err := p.parseCondition(matches[2])
		if err != nil {
			return nil, err
		}
		stmt.Where = cond
	}

	return stmt, nil
}

// parseCreate parses CREATE TABLE statements
func (p *Parser) parseCreate(query string) (*Statement, error) {
	stmt := &Statement{Type: CreateTable}

	// Pattern: CREATE TABLE table (columns)
	createPattern := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(\w+)\s*\((.+)\)$`)

	matches := createPattern.FindStringSubmatch(query)
	if matches == nil {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax")
	}

	stmt.Table = strings.TrimSpace(matches[1])
	
	// Parse column definitions
	columns := strings.Split(matches[2], ",")
	stmt.Columns = make([]string, len(columns))
	for i, col := range columns {
		stmt.Columns[i] = strings.TrimSpace(strings.Split(col, " ")[0])
	}

	return stmt, nil
}

// parseVectorSearch parses Kvi-specific vector search
func (p *Parser) parseVectorSearch(query string) (*Statement, error) {
	stmt := &Statement{Type: VectorSearch}

	// Pattern: VECTOR SEARCH table WITH [0.1, 0.2, ...] K 10
	vectorPattern := regexp.MustCompile(`(?i)VECTOR\s+SEARCH\s+(\w+)\s+WITH\s+\[(.+?)\]\s*K\s+(\d+)`)

	matches := vectorPattern.FindStringSubmatch(query)
	if matches == nil {
		return nil, fmt.Errorf("invalid VECTOR SEARCH syntax")
	}

	stmt.Table = strings.TrimSpace(matches[1])

	// Parse vector
	vectorStr := matches[2]
	vector, err := p.parseVector(vectorStr)
	if err != nil {
		return nil, err
	}

	k, err := strconv.Atoi(matches[3])
	if err != nil {
		return nil, fmt.Errorf("invalid K value")
	}

	stmt.VectorQuery = &VectorClause{
		Vector: vector,
		K:      k,
	}

	return stmt, nil
}

// parseCondition parses a WHERE condition
func (p *Parser) parseCondition(condStr string) (*Condition, error) {
	// Simple condition: column operator value
	operators := []string{"!=", ">=", "<=", "=", ">", "<"}

	for _, op := range operators {
		if strings.Contains(condStr, op) {
			parts := strings.SplitN(condStr, op, 2)
			if len(parts) == 2 {
				val, err := p.parseValue(strings.TrimSpace(parts[1]))
				if err != nil {
					return nil, err
				}
				return &Condition{
					Column:   strings.TrimSpace(parts[0]),
					Operator: op,
					Value:    val,
				}, nil
			}
		}
	}

	return nil, fmt.Errorf("invalid condition: %s", condStr)
}

// parseValues parses multiple values
func (p *Parser) parseValues(valuesStr string) ([]interface{}, error) {
	values := strings.Split(valuesStr, ",")
	result := make([]interface{}, len(values))

	for i, v := range values {
		val, err := p.parseValue(strings.TrimSpace(v))
		if err != nil {
			return nil, err
		}
		result[i] = val
	}

	return result, nil
}

// parseValue parses a single value
func (p *Parser) parseValue(v string) (interface{}, error) {
	// String (quoted)
	if strings.HasPrefix(v, "'") && strings.HasSuffix(v, "'") {
		return strings.Trim(v, "'"), nil
	}
	if strings.HasPrefix(v, "\"") && strings.HasSuffix(v, "\"") {
		return strings.Trim(v, "\""), nil
	}

	// Integer
	if intVal, err := strconv.ParseInt(v, 10, 64); err == nil {
		return intVal, nil
	}

	// Float
	if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
		return floatVal, nil
	}

	// Boolean
	if strings.ToUpper(v) == "TRUE" {
		return true, nil
	}
	if strings.ToUpper(v) == "FALSE" {
		return false, nil
	}

	// NULL
	if strings.ToUpper(v) == "NULL" {
		return nil, nil
	}

	// Default to string
	return v, nil
}

// parseVector parses a vector string
func (p *Parser) parseVector(v string) ([]float32, error) {
	parts := strings.Split(v, ",")
	vector := make([]float32, len(parts))

	for i, part := range parts {
		val, err := strconv.ParseFloat(strings.TrimSpace(part), 32)
		if err != nil {
			return nil, fmt.Errorf("invalid vector value: %s", part)
		}
		vector[i] = float32(val)
	}

	return vector, nil
}
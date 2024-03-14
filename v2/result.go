package oracli

import (
	"database/sql"

	goOra "github.com/sijms/go-ora/v2"
)

type (
	Cursor   *sql.Rows
	ClobType goOra.Clob
)

// Record result from unwrap goOra result
type Record map[string]any

// Container Data returned by Select
type Container struct {
	Data []Record
}

// Result unique returning type
type Result struct {
	*Container
	Error           error
	RecordsAffected int64
	HasData         bool
	Cursor          Cursor
}

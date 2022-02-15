package oracli

import "database/sql/driver"

// Record result from unwrap goOra result
type Record map[string]driver.Value

// Container Data returned by Select
type Container struct {
	Data []Record
}

// Result unique returning type
type Result struct {
	*Container
	Error           error
	RecordsAffected int64
}

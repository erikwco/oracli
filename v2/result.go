package oracli

// Record result from unwrap goOra result
type Record map[string]any

// Container Data returned by Select
type Container struct {
	Data []Record
}

// addToRows take the rows from the result and append the result
// to Container.Data
func (c *Container) addToRows(columns []string, rows []any) {
	// if full create a new one before add new one
	if len(c.Data) == cap(c.Data) {
		expanded := make([]Record, len(c.Data), cap(c.Data)+1)
		copy(expanded, c.Data)
		c.Data = expanded
	}
	// add new data
	c.Data = append(c.Data, unwrapToRecord(columns, rows))
}

// addToRowsString take rows and columns to create a rowString
// Parameters:
// @columns list of columns
// @rows list of rows
func (c *Container) addToRowsString(columns []string, rows []string) {
	// if full create a new one before add new one
	if len(c.Data) == cap(c.Data) {
		expanded := make([]Record, len(c.Data), cap(c.Data)+1)
		copy(expanded, c.Data)
		c.Data = expanded
	}
	// add new data
	c.Data = append(c.Data, unwrapToRecordString(columns, rows))
}

// newContainer creates a new Container
func newContainer() *Container {
	return &Container{
		Data: make([]Record, 0, 1),
	}
}

// Result unique returning type
type Result struct {
	*Container
	Error           error
	RecordsAffected int64
	HasData         bool
	ClobString      string
}

package oracli

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"

	goOra "github.com/sijms/go-ora/v2"
)

// Connector interface that define a connection
type Connector interface {
	Select(stmt string, params []Param) Result
	Exec(stmt string, params []Param) Result
	BeginTx() error
	Commit() error
	Rollback() error
	Close()
	Ping() error
	ReConnect() error
}

// ConnStatus exposes connection status
type ConnStatus int

const (
	ConnClosed ConnStatus = iota
	ConnOpened
)

// String allow string conversion to ConnStatus
func (cs ConnStatus) String() string {
	return [...]string{"ConnClosed", "ConnOpened"}[cs]
}

// Connection represents an object connection for Oracle
type Connection struct {
	Name   string
	ConStr string
	conn   *goOra.Connection
	tx     driver.Tx
	Status ConnStatus
}

// Param used to Select / Exec a statement
type Param struct {
	Name      string
	Value     driver.Value
	Size      int
	Direction goOra.ParameterDirection
	IsRef     bool
}

type params struct {
	values []driver.Value
	isRef  bool
	cursor *goOra.RefCursor
}

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

// NewConnection create and open a goOra Connection
func NewConnection(constr string, name string) (*Connection, error) {
	if constr == "" {
		return nil, EmptyConStrErr
	}

	// createConnection
	conn, err := createConnection(constr)
	if err != nil {
		return nil, err
	}
	// test database connection
	err = conn.Ping(nil)
	if err != nil {
		return nil, TestConnErr(err.Error())
	}

	// returning connection
	return &Connection{
		Name:   name,
		conn:   conn,
		ConStr: constr,
		Status: ConnOpened,
	}, nil
}

// NewParam creates and fill a new Param
func (c Connection) NewParam(name string, value driver.Value) *Param {
	return &Param{
		Name:      name,
		Value:     value,
		Size:      100,
		Direction: goOra.Input,
		IsRef:     false,
	}
}

func (c Connection) NewCursorParam(name string) *Param {
	return &Param{
		Name:      name,
		Value:     "",
		Size:      1000,
		Direction: goOra.Output,
		IsRef:     true,
	}
}

// Select takes a statement that could be a plain select or a procedure with
// ref-cursor return parameter and wrap in Result object
func (c Connection) Select(stmt string, params []Param) Result {

	// ***********************************************
	// Build Param List
	// ***********************************************
	p := buildParamsList(params)

	// ***********************************************
	// if isRef is found execution is used
	// ***********************************************
	if p.isRef {

		// ***********************************************
		// execute statement
		// ***********************************************
		_, err := c.conn.Exec(stmt, p.values...)
		if err != nil {
			return Result{
				Error:           err,
				RecordsAffected: 0,
			}
		}

		// ***********************************************
		// validate cursor information
		// ***********************************************
		if p.cursor != nil {

			// ***********************************************
			// defer closing cursor
			// ***********************************************
			defer func() {
				fmt.Println("**** Closing cursor ****")
				err := p.cursor.Close()
				if err != nil {
					fmt.Printf("Error closing statement [%s]\n", err.Error())
				}
			}()

			// ***********************************************
			// running through query
			// ***********************************************
			rows, err := p.cursor.Query()
			// defer closing rows
			defer func() {
				fmt.Println("**** Closing rows ****")
				err := rows.Close()
				if err != nil {
					fmt.Printf("Error closing statement [%s]\n", err.Error())
				}
			}()
			if err != nil {
				return Result{
					Error:           err,
					RecordsAffected: 0,
				}
			}

			// ***********************************************
			// unwrap rows and return
			// ***********************************************
			records, err := unwrapRows(rows)
			rowsAffected := 0
			if err == nil {
				rowsAffected = len(records.Data)
			}
			return Result{
				Container:       records,
				Error:           err,
				RecordsAffected: int64(rowsAffected),
			}

		} else {
			return Result{
				Error:           errors.New("refCursor not found"),
				RecordsAffected: 0,
			}
		}

	} else {
		// ***********************************************
		// Select execution - prepare statement
		// ***********************************************
		query := c.prepareStatement(stmt)
		// defer closing statement
		defer func() {
			fmt.Println("**** Closing query ****")
			err := query.Close()
			if err != nil {
				fmt.Printf("Error closing statement [%s]\n", err.Error())
			}
		}()

		// ***********************************************
		// parameters
		// ***********************************************
		err := parseParams(query, params)
		if err != nil {
			return Result{
				Error:           err,
				RecordsAffected: 0,
			}
		}

		// ***********************************************
		// running select
		// ***********************************************
		rows, err := query.Query(nil)
		// defer closing rows
		defer func() {
			err := rows.Close()
			if err != nil {
				fmt.Printf("Error closing statement [%s]\n", err.Error())
			}
		}()
		if err != nil {
			return Result{
				Error:           err,
				RecordsAffected: 0,
			}
		}

		// ***********************************************
		// unwrapping rows
		// ***********************************************
		records, err := unwrapRows(rows)
		rowsAffected := 0
		if err == nil {
			rowsAffected = len(records.Data)
		}
		return Result{
			Container:       records,
			Error:           err,
			RecordsAffected: int64(rowsAffected),
		}

	}

}

// Exec used to execute non-returnable DML as insert, update, delete
// or a procedure without return values
func (c Connection) Exec(stmt string, params []Param) Result {
	// prepare statement
	query := c.prepareStatement(stmt)
	// defer closing statement
	defer func() {
		err := query.Close()
		if err != nil {
			fmt.Printf("Error closing statement [%s]\n", err.Error())
		}
	}()

	// parse params
	err := parseParams(query, params)
	if err != nil {
		return Result{
			Error:           err,
			RecordsAffected: 0,
		}
	}

	// execute statement
	rows, err := query.Exec(nil)
	if err != nil {
		return Result{
			Error:           err,
			RecordsAffected: 0,
		}
	}

	rowsAffected, err := rows.RowsAffected()
	if err != nil {
		return Result{
			Error:           err,
			RecordsAffected: 0,
		}
	}
	return Result{
		RecordsAffected: rowsAffected,
		Error:           nil,
	}

}

// BeginTx start a new transaction to allow commit or rollback
func (c *Connection) BeginTx() error {
	// check if connection is open
	if c.conn.State != goOra.Opened {
		return errors.New("connection closed transaction can not be created")
	}
	// starting transaction
	tx, err := c.conn.Begin()
	if err != nil {
		return errors.New(fmt.Sprintf("transaction couldn't begin %s", err.Error()))
	}
	// store transaction
	c.tx = tx
	return nil
}

// Commit set commit to the current transaction
// if exists
func (c Connection) Commit() error {
	if c.tx != nil {
		return c.tx.Commit()
	} else {
		fmt.Println("Transaction not initialized")
		return nil
	}
}

// Rollback set rollback to the current transaction
// if exists
func (c Connection) Rollback() error {
	if c.tx != nil {
		return c.tx.Rollback()
	} else {
		fmt.Println("transaction not initialized to rollback")
		return nil
	}
}

// Close closes the current connection
func (c *Connection) Close() {
	c.Status = ConnClosed
	err := c.conn.Close()
	if err != nil {
		fmt.Printf("Error closing connection [%s]", err.Error())
	}
}

// Ping database connection
func (c Connection) Ping() error {
	if c.Status == ConnClosed {
		return errors.New("")
	}

	res := c.Select("select * from dual", nil)
	if res.Error != nil {
		return res.Error
	}
	if len(res.Data) <= 0 {
		return errors.New("")
	}
	return nil

}

// ReConnect test a select against the database to check connection
func (c *Connection) ReConnect() error {
	if c.Status == ConnOpened {
		err := c.Ping()
		if err != nil {
			c.Status = ConnClosed
			conn, err := createConnection(c.ConStr)
			if err != nil {
				return err
			}
			c.Status = ConnOpened
			c.conn = conn
		}
	} else {
		conn, err := createConnection(c.ConStr)
		if err != nil {
			return err
		}
		c.Status = ConnOpened
		c.conn = conn
	}
	return nil

}

// prepareStatement creates a new goOra Statement
func (c Connection) prepareStatement(statement string) *goOra.Stmt {
	// create statement
	return goOra.NewStmt(statement, c.conn)
}

// addToRows take the rows from the result and append the result
// to Container.Data
func (c *Container) addToRows(columns []string, rows []driver.Value) {
	// if full create a new one before add new one
	if len(c.Data) == cap(c.Data) {
		expanded := make([]Record, len(c.Data), cap(c.Data)+1)
		copy(expanded, c.Data)
		c.Data = expanded
	}
	// add new data
	c.Data = append(c.Data, unwrapToRecord(columns, rows))
}

// newContainer creates a new Container
func newContainer() *Container {
	return &Container{
		Data: make([]Record, 0, 1),
	}
}

// parseParams take []Param and assign one by one to the statement
// returning true if ref-cursor is found or false if not, and
// the position when ref-cursor was found
func parseParams(statement *goOra.Stmt, params []Param) error {
	for _, v := range params {
		if err := statement.AddParam(v.Name, v.Value, v.Size, v.Direction); err != nil {
			return err
		}
	}
	return nil
}

func buildParamsList(parameters []Param) *params {
	l := &params{}
	var v []driver.Value
	var cursor goOra.RefCursor

	for _, p := range parameters {
		if p.IsRef {
			l.isRef = true
			l.cursor = &cursor
			v = append(v, sql.Out{Dest: l.cursor})
			continue
		}
		v = append(v, p.Value)
	}
	l.values = v
	return l
}

func hasCursor(params []Param) bool {
	for _, v := range params {
		if v.IsRef {
			return true
		}
	}
	return false
}

// unwrapRows take driver.Rows and convert to Container
func unwrapRows(rows driver.Rows) (*Container, error) {
	// closing rows
	defer func() {
		err := rows.Close()
		if err != nil {
			fmt.Printf("Error closing statement [%s]\n", err.Error())
		}
	}()

	// Container
	container := newContainer()
	var err error

	// Get columns name
	columns := rows.Columns()
	values := make([]driver.Value, len(columns))

	// running records
	for {
		err = rows.Next(values)
		if err != nil {
			break
		}
		container.addToRows(columns, values)
	}
	if err != io.EOF {
		return nil, errors.New(fmt.Sprintf("error unwrapping rows [%s]", err.Error()))
	}

	// returning data
	return container, nil
}

// unwrapToRecord take every row and create a new Record
func unwrapToRecord(columns []string, values []driver.Value) Record {
	r := make(Record)
	for i, c := range values {
		r[columns[i]] = c
	}
	return r
}

func createConnection(constr string) (*goOra.Connection, error) {
	conn, err := goOra.NewConnection(constr)
	if err != nil {
		return nil, CantCreateConnErr(err.Error())
	}

	err = conn.Open()
	if err != nil {
		return nil, CantOpenDbErr(err.Error())
	}
	return conn, nil
}

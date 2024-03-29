package oracli

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/mitchellh/mapstructure"
	goOra "github.com/sijms/go-ora/v2"
	"log"
	"time"
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
	return [...]string{"ConnClosed", "ConnOpened", "ConnError"}[cs]
}

// ConnectionConfiguration represents the minimum configuration required for the connection pool
type ConnectionConfiguration struct {
	MaxOpenConnections    int
	MaxIdleConnections    int
	MaxConnectionLifeTime time.Duration
	MaxIdleConnectionTime time.Duration
}

// Connection represents an object connection for Oracle
type Connection struct {
	Name          string
	ConStr        string
	Configuration ConnectionConfiguration
	conn          *sql.DB
	tx            driver.Tx
	Status        ConnStatus
}

type ParameterDirection int

const (
	Input  ParameterDirection = 1
	Output ParameterDirection = 2
	InOut  ParameterDirection = 3
)

// Param used to Select / Exec a statement
type Param struct {
	Name      string
	Value     driver.Value
	Size      int
	Direction ParameterDirection
	IsRef     bool
}

// params parsed params list
type params struct {
	values []any
	isRef  bool
	cursor *goOra.RefCursor
}

// *****************************************************
// Public
// *****************************************************

// NewConnectionWithParams Conexión con parámetros nombrados
func NewConnectionWithParams(
	server string,
	port int,
	user string,
	password string,
	service string,
	options map[string]string,
	configuration ConnectionConfiguration,
	name string,
) (*Connection, error) {
	conStr := goOra.BuildUrl(server, port, service, user, password, options)
	return NewConnection(conStr, name, configuration)
}

// NewConnection create and open a goOra Connection
func NewConnection(constr string, name string, configuration ConnectionConfiguration) (*Connection, error) {
	if constr == "" {
		return nil, EmptyConStrErr
	}
	
	// createConnection
	conn, err := createConnection(constr, configuration)
	if err != nil {
		return nil, err
	}
	
	// returning connection
	return &Connection{
		Name:          name,
		conn:          conn,
		ConStr:        constr,
		Status:        ConnOpened,
		Configuration: configuration,
	}, nil
}


// NewParam creates and fill a new Param
func (c *Connection) NewParam(name string, value driver.Value) *Param {
	return &Param{
		Name:      name,
		Value:     value,
		Size:      100,
		Direction: Input,
		IsRef:     false,
	}
	
}

// NewCursorParam cursor para parámetros
func (c *Connection) NewCursorParam(name string) *Param {
	return &Param{
		Name:      name,
		Value:     "",
		Size:      1000,
		Direction: Output,
		IsRef:     true,
	}
}

// Parser converts Result object to structure
func Parser[T any](source Result) (T, error) {
	var empty T
	var data T
	err := mapstructure.Decode(source.Data, &data)
	if err != nil {
		return empty, err
	}
	return data, nil
}

// Select takes a statement that could be a plain select or a procedure with
// ref-cursor return parameter and wrap in Result object
func (c *Connection) Select(stmt string, params []*Param) Result {
	// ***********************************************
	// Evaluando conexión
	// ***********************************************
	err := c.Ping()
	if err != nil  {
		return Result {
			Error:           errors.New(fmt.Sprintf("Error en la conexión [%s]", err.Error())),
			RecordsAffected: 0,
			HasData: false,
		}
	}
	
	if c.Status == ConnClosed   {
		err := c.ReConnect()
		if err != nil {
			return Result{
				Error:           err,
				RecordsAffected: 0,
				HasData:         false,
			}
		}
	}

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
		fmt.Println(stmt)
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
			defer func(c *goOra.RefCursor) {
				if c != nil {
					err := c.Close()
					if err != nil {
						fmt.Printf("Error closing statement [%s]\n", err.Error())
					}
				}
			}(p.cursor)

			// ***********************************************
			// running through query
			// ***********************************************

			rows, err := p.cursor.Query()
			if err != nil {
				return Result{
					Error:           err,
					RecordsAffected: 0,
				}
			}

			// defer closing rows
			defer func() {
				err := rows.Close()
				if err != nil {
					fmt.Printf("Error closing statement [%s]\n", err.Error())
				}
			}()

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
				HasData:         rowsAffected > 0,
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
		query, err := c.prepareStatement(stmt)
		if err != nil {
			return Result{
				Error:           err,
				RecordsAffected: 0,
			}
		}

		// defer closing statement
		defer func(s *sql.Stmt) {
			//fmt.Println("**** Closing query ****")
			err := s.Close()
			if err != nil {
				fmt.Printf("Error closing statement [%s]\n", err.Error())
			}
		}(query)

		// ***********************************************
		// running select
		// ***********************************************
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		rows, err := query.QueryContext(ctx, p.values...)
		if err != nil {
			return Result{
				Error:           err,
				RecordsAffected: 0,
			}
		}

		// defer closing rows
		defer func(r *sql.Rows) {
			err := r.Close()
			if err != nil {
				fmt.Printf("Error closing statement [%s]\n", err.Error())
			}
		}(rows)

		///rows.Next()
		// ***********************************************
		// unwrapping rows
		// ***********************************************
		records, err := unwrapRowsSql(rows)
		rowsAffected := 0
		if err == nil {
			rowsAffected = len(records.Data)
		}
		return Result{
			Container:       records,
			Error:           err,
			RecordsAffected: int64(rowsAffected),
			HasData:         rowsAffected > 0,
		}

	}

}

func (c *Connection) ExecuteDDL(stmt string) Result {
	// ***********************************************
	// Evaluando conexión
	// ***********************************************
	if c.Status == ConnClosed || c.Ping() != nil {
		err := c.ReConnect()
		if err != nil {
			return Result{
				Error:           err,
				RecordsAffected: 0,
				HasData:         false,
			}
		}
	}

	//ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	//defer cancel()
	result, err := c.conn.Exec(stmt)
	if err != nil {
		return Result{
			Error:           err,
			RecordsAffected: 0,
		}
	}

	ra, err := result.RowsAffected()
	if err != nil {
		return Result{
			Error:           err,
			RecordsAffected: 0,
		}
	}

	return Result{
		Error:           nil,
		RecordsAffected: ra,
	}

}

// Exec used to execute non-returnable DML as insert, update, delete
// or a procedure without return values
func (c *Connection) Exec(stmt string, params []*Param) Result {
	// ***********************************************
	// Evaluando conexión
	// ***********************************************
	if c.Status == ConnClosed || c.Ping() != nil {
		err := c.ReConnect()
		if err != nil {
			return Result{
				Error:           err,
				RecordsAffected: 0,
				HasData:         false,
			}
		}
	}

	// prepare statement
	query, err := c.prepareStatement(stmt)
	// defer closing statement
	defer func() {
		err := query.Close()
		if err != nil {
			fmt.Printf("Error closing statement [%s]\n", err.Error())
		}
	}()

	// parse params
	p := buildParamsList(params)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// execute statement
	rows, err := query.ExecContext(ctx, p.values...)
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
		HasData:         rowsAffected > 0,
	}

}

// BeginTx start a new transaction to allow commit or rollback
func (c *Connection) BeginTx() error {
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
func (c *Connection) Commit() error {
	if c.tx != nil {
		return c.tx.Commit()
	} else {
		fmt.Println("Transaction not initialized")
		return nil
	}
}

// Rollback set rollback to the current transaction
// if exists
func (c *Connection) Rollback() error {
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
func (c *Connection) Ping() error {
	// test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// ping connection
	err := c.conn.PingContext(ctx)
	if err != nil {
		c.Status = ConnClosed
		return CantPingConnection(err.Error())
	}
	c.Status = ConnOpened
	return nil
}

// ReConnect test a select against the database to check connection
func (c *Connection) ReConnect() error {
	if c.Status == ConnOpened {
		err := c.Ping()
		if err != nil {
			c.Status = ConnClosed
			conn, err := createConnection(c.ConStr, c.Configuration)
			if err != nil {
				return err
			}
			c.Status = ConnOpened
			c.conn = conn
		}
	} else {
		c.Status = ConnClosed
		conn, err := createConnection(c.ConStr, c.Configuration)
		if err != nil {
			return err
		}
		c.Status = ConnOpened
		c.conn = conn
	}
	return nil

}

// GetConnection creates and individual connection
func (c *Connection) GetConnection(context context.Context) (*sql.Conn, error) {

	return c.conn.Conn(context)
}

// *****************************************************
// Private
// *****************************************************

// prepareStatement creates a new goOra Statement
func (c *Connection) prepareStatement(statement string) (*sql.Stmt, error) {
	// create statement
	return c.conn.Prepare(statement)
	//return goOra.NewStmt(statement, c.conn)
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

// buildParamsList
func buildParamsList(parameters []*Param) *params {
	l := &params{}
	var v []any
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

// unwrapRows take *goOra.DataSet and convert to Container
func unwrapRows(rows *goOra.DataSet) (*Container, error) {
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
	values := make([]string, len(columns))
	columnPointers := make([]interface{}, len(columns))
	for i := range values {
		columnPointers[i] = &values[i]
	}

	// running records
	for rows.Next_() {
		if err = rows.Scan(columnPointers...); err != nil {
			return nil, errors.New(fmt.Sprintf("error unwrapping rows [%s]", err.Error()))
		}
		container.addToRowsString(columns, values)
	}

	// returning data
	return container, nil
}

// unwrapRowsSql takes sql.Rows and convert to Container
func unwrapRowsSql(rows *sql.Rows) (*Container, error) {
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
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]any, len(columns))
	columnPointers := make([]any, len(columns))
	for i := range values {
		columnPointers[i] = &values[i]
	}

	// running records
	for rows.Next() {
		if err = rows.Scan(columnPointers...); err != nil {
			log.Printf("unwrapRowSQL - 7 - [%v]", err)
			return nil, errors.New(fmt.Sprintf("error unwrapping rows [%s]", err.Error()))
		}
		container.addToRows(columns, values)
	}

	// returning data
	return container, nil
}

// unwrapToRecord take every row and create a new Record
func unwrapToRecord(columns []string, values []any) Record {
	r := make(Record)
	for i, c := range values {
		r[columns[i]] = c
	}
	return r
}

// unwrapToRecord take every row and create a new Record
func unwrapToRecordString(columns []string, values []string) Record {
	r := make(Record)
	for i, c := range values {
		r[columns[i]] = c
	}
	return r
}

// createConnection
func createConnection(constr string, configuration ConnectionConfiguration) (*sql.DB, error) {
	//conn, err := goOra.NewConnection(constr)
	
	// Open connection via sql.Open interface
	conn, err := sql.Open("oracle", constr)
	if err != nil {
		return nil, CantCreateConnErr(err.Error())
	}
	
	// set limits
	// cantidad de conexiones activas que puede tener el pool
	if configuration.MaxOpenConnections > 0 {
		conn.SetMaxOpenConns(configuration.MaxOpenConnections)
	} else {
		conn.SetMaxOpenConns(100)
	}
	
	// cantidad de conexiones que pueden estar en espera de ser reutilizadas
	if configuration.MaxIdleConnections > 0 {
		conn.SetMaxIdleConns(configuration.MaxIdleConnections)
	} else {
		conn.SetMaxIdleConns(50)
	}
	
	// tiempo total que vivirá una conexión sea esta reutilizada o no
	if configuration.MaxConnectionLifeTime > 0 {
		conn.SetConnMaxLifetime(configuration.MaxConnectionLifeTime)
	} else {
		conn.SetConnMaxLifetime(15 * time.Minute)
	}
	
	if configuration.MaxIdleConnectionTime > 0 {
		conn.SetConnMaxIdleTime(configuration.MaxIdleConnectionTime)
	} else {
		conn.SetConnMaxIdleTime(5 * time.Minute)
	}
	
	//conn.SetConnMaxIdleTime(3 * time.Second)
	
	// test connection
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// ping connection
	err = conn.PingContext(ctx)
	if err != nil {
		return nil, CantPingConnection(fmt.Sprintf("PingContext %v", err.Error()))
	}

	return conn, nil
}

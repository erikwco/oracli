package oracli

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog"
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

// Connection status
const (
	ConnClosed ConnStatus = iota
	ConnOpened
)

// String allow string conversion to ConnStatus
func (cs ConnStatus) String() string {
	return [...]string{"ConnClosed", "ConnOpened", "ConnError"}[cs]
}

// ParameterDirection defines the direction of the parameter
type ParameterDirection int

const (
	Input ParameterDirection = iota
	Output
	InOut
)

func (p ParameterDirection) String() string {
	return [...]string{"Input", "Output", "InputOutput"}[p]
}

// ConnectionConfiguration represents the minimum configuration required for the connection pool
type ConnectionConfiguration struct {
	ConfigurationSet      bool
	MaxOpenConnections    int
	MaxIdleConnections    int
	ContextTimeout        int
	MaxConnectionLifeTime time.Duration
	MaxIdleConnectionTime time.Duration
}

// Connection represents an object connection for Oracle
type Connection struct {
	Name          string
	ConStr        string
	Configuration *ConnectionConfiguration
	log           *zerolog.Logger
	conn          *sql.DB
	tx            driver.Tx
	// Status        ConnStatus
	lock *sync.Mutex
}

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

// -----------------------------------------------------
// Public Methods
// -----------------------------------------------------

// NewConnectionWithParams create a new connection using every parameter independently
// Parameters:
// @server: Server Address - name or ip
// @port: Connection port
// @user: User name
// @password: password
// @service: Service Name for Oracle connection if SID is needed use @options
// @options: specified some options like TRACE, SID or conStr etc.
// @configuration: Specifies how connections parameters must be handled in ConnectionConfiguration
// @name: Connection name
// @log: In this version *zerolog.Logger is required
func NewConnectionWithParams(server string, port int, user, password, service string,
	options map[string]string,
	configuration *ConnectionConfiguration,
	name string,
	log *zerolog.Logger,
) (*Connection, error) {
	// TODO: evaluate to remove *zerolog.logger by generic interface
	conStr := goOra.BuildUrl(server, port, service, user, password, options)
	log.Info().Msgf(" +++ Connection String [%v]", conStr)
	return NewConnection(conStr, name, configuration, log)
}

// NewConnection create and open a goOra Connection based on buildUrl
// Parameters:
// @constr: Connection String built with buildUrl
// @name: Connection name
// @configuration:n Specifies how connection must be handled widh ConnectionConfiguration
// @log: In this version *zerolog.Logger is required
func NewConnection(constr string, name string, configuration *ConnectionConfiguration, log *zerolog.Logger) (*Connection, error) {
	// TODO: evaluate to remove *zerolog.logger by generic interface
	log.Info().Msgf("+++ Nuevo Pool de Conexiones [%v]", name)
	if constr == "" {
		log.Error().Msg("connection string sin valor")
		return nil, EmptyConStrErr
	}

	// createConnection
	conn, err := createConnection(constr, configuration, log)
	if err != nil {
		log.Err(err).Msg("apertura de conexión del pool no pudo realizarse")
		return nil, err
	}

	log.Info().Msgf("+++ Nuevo pool de Conexiones [%v] creado", name)
	// returning connection
	return &Connection{
		Name:   name,
		conn:   conn,
		ConStr: constr,
		// Status:        ConnOpened,
		Configuration: configuration,
		log:           log,
		lock:          &sync.Mutex{},
	}, nil
}

// NewInOutParam creates and fill a new InOut Parameter
// Parameters:
// @name: Parameter name - only for control
// @value: value to be passed
func (c *Connection) NewInOutParam(name string, value driver.Value) *Param {
	return &Param{
		Name:      name,
		Value:     value,
		Size:      100,
		Direction: InOut,
		IsRef:     false,
	}
}

// NewParam creates and fill a new Input Parameter
// Parameters:
// @name: Parameter name - only for control
// @value: value to be passed
func (c *Connection) NewParam(name string, value driver.Value) *Param {
	return &Param{
		Name:      name,
		Value:     value,
		Size:      100,
		Direction: Input,
		IsRef:     false,
	}
}

// NewCursorParam creates a new Output parameter of type sys_refcursor
// Parameters:
// @name: Parameter name - only for control
func (c *Connection) NewCursorParam(name string) *Param {
	return &Param{
		Name:      name,
		Value:     "",
		Size:      1000,
		Direction: Output,
		IsRef:     true,
	}
}

// Parser generic function to convert Result object to structure
// Parameters:
// @source: Result object that contains the data
func Parser[T any](source Result) (T, error) {
	var empty T
	var data T
	err := mapstructure.Decode(source.Data, &data)
	if err != nil {
		return empty, err
	}
	return data, nil
}

// GetConnectionStatus return the actual status of a connection
//func (c *Connection) GetConnectionStatus() (status ConnStatus) {
//	c.lock.Lock()
//	defer c.lock.Unlock()
//	status = c.Status
//	return
//}

// Select takes a statement that could be a plain select or a procedure with
// ref-cursor return parameter and wrap in Result object
// Parameters:
// @stmt: Statement to execute
// @params: []*Params - list of parameters to be replaced by position in the @stmt
func (c *Connection) Select(stmt string, params []*Param) Result {
	c.log.Info().Msgf("+++ Hit Select for  [%v]", stmt)
	c.log.Info().Msgf("+++ number of paramters [%v]", len(params))
	// ***********************************************
	// Evaluando conexión
	// ***********************************************
	err := c.Ping()
	if err != nil {
		c.log.Err(err).Msg("Error realizando Ping a la conexión")
		return Result{
			Error:           errors.New(fmt.Sprintf("Error en la conexión [%s]", err.Error())),
			RecordsAffected: 0,
			HasData:         false,
		}
	}

	//if c.GetConnectionStatus() == ConnClosed {
	//	err := c.ReConnect()
	//	if err != nil {
	//		c.log.Err(err).Msg("Error realizando ReConnect a la conexión")
	//		return Result{
	//			Error:           err,
	//			RecordsAffected: 0,
	//			HasData:         false,
	//		}
	//	}
	//}

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
			c.log.Err(err).Msg("Error ejecutando el statement")
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
			defer func(cursor *goOra.RefCursor) {
				if cursor != nil {
					err := cursor.Close()
					if err != nil {
						c.log.Err(err).Msgf("Error closing statement [%s]\n", err.Error())
					}
				}
			}(p.cursor)

			// ***********************************************
			// running through query
			// ***********************************************

			rows, err := p.cursor.Query()
			if err != nil {
				c.log.Err(err).Msg("Error ejecutando el cursor")
				return Result{
					Error:           err,
					RecordsAffected: 0,
				}
			}

			// defer closing rows
			defer func() {
				err := rows.Close()
				if err != nil {
					c.log.Err(err).Msgf("Error closing statement [%s]\n", err.Error())
				}
			}()

			// ***********************************************
			// unwrap rows and return
			// ***********************************************
			records, err := c.unwrapRows(rows)
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
			c.log.Err(err).Msg("Error ejecutando el cursor")
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
			c.log.Err(err).Msg("Error preparando el statement")
			return Result{
				Error:           err,
				RecordsAffected: 0,
			}
		}

		// defer closing statement
		defer func(s *sql.Stmt) {
			// fmt.Println("**** Closing query ****")
			err := s.Close()
			if err != nil {
				c.log.Err(err).Msgf("Error closing statement [%s]\n", err.Error())
			}
		}(query)

		// ***********************************************
		// running select
		// ***********************************************
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		rows, err := query.QueryContext(ctx, p.values...)
		if err != nil {
			c.log.Err(err).Msg("Error ejecutando el query")
			return Result{
				Error:           err,
				RecordsAffected: 0,
			}
		}

		// defer closing rows
		defer func(r *sql.Rows) {
			err := r.Close()
			if err != nil {
				c.log.Err(err).Msgf("Error closing statement [%s]\n", err.Error())
			}
		}(rows)

		///rows.Next()
		// ***********************************************
		// unwrapping rows
		// ***********************************************
		records, err := c.unwrapRowsSql(rows)
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
	c.log.Info().Msgf("+++ Hit ExecuteDDL for  [%v]", stmt)
	// ***********************************************
	// Evaluando conexión
	// ***********************************************

	if err := c.Ping(); err != nil {
		return Result{
			Error:           err,
			RecordsAffected: 0,
		}
	}

	//if c.GetConnectionStatus() == ConnClosed || c.Ping() != nil {
	//	err := c.ReConnect()
	//	if err != nil {
	//		return Result{
	//			Error:           err,
	//			RecordsAffected: 0,
	//			HasData:         false,
	//		}
	//	}
	//}

	// ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	// defer cancel()
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
	c.log.Info().Msgf("+++ Hit Exec for  [%v]", stmt)
	c.log.Info().Msgf("+++ number of paramters [%v]", len(params))
	for _, p := range params {
		c.log.Info().Msgf("+++ Param [%v] - Value [%v]", p.Name, p.Value)
	}

	// prepare statement
	query, err := c.prepareStatement(stmt)
	// defer closing statement
	defer func() {
		if query != nil {
			defer func() {
				if r := recover(); r != nil {
					c.log.Info().Msg("Query closing deferred error and recovery")
				}
			}()
			err := query.Close()
			if err != nil {
				fmt.Printf("Error closing statement [%s]\n", err.Error())
			}
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
	// TODO: remove c.Status field
	// c.Status = ConnClosed
	err := c.conn.Close()
	if err != nil {
		fmt.Printf("Error closing connection [%s]", err.Error())
	}
}

// Ping database connection
func (c *Connection) Ping() error {
	// TODO: Refactor Ping to not use StatusConn, works only with error from
	// ping result.

	// test connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// ping connection
	err := c.conn.PingContext(ctx)
	if err != nil {
		// c.lock.Lock()
		// defer c.lock.Unlock()
		// c.Status = ConnClosed
		return CantPingConnection(err.Error())
	}
	// c.lock.Lock()
	// defer c.lock.Unlock()
	// c.Status = ConnOpened
	return nil
}

// ReConnect test a select against the database to check connection
func (c *Connection) ReConnect() error {
	// WARNING: evaluate the fact ReConnect is generating
	// some kind of false positive, in connection Status
	// because could be many goroutines trying reConnect

	if err := c.Ping(); err != nil {
		conn, err := createConnection(c.ConStr, c.Configuration, c.log)
		if err != nil {
			return err
		}
		c.conn = conn
	}

	return nil

	//if c.GetConnectionStatus() == ConnOpened {
	//	err := c.Ping()
	//	if err != nil {
	//		c.lock.Lock()
	//		defer c.lock.Unlock()
	//		c.Status = ConnClosed
	//		conn, err := createConnection(c.ConStr, c.Configuration, c.log)
	//		if err != nil {
	//			return err
	//		}
	//		c.Status = ConnOpened
	//		c.conn = conn
	//	}
	//} else {
	//	c.lock.Lock()
	//	defer c.lock.Unlock()
	//	c.Status = ConnClosed
	//	conn, err := createConnection(c.ConStr, c.Configuration, c.log)
	//	if err != nil {
	//		return err
	//	}
	//	c.Status = ConnOpened
	//	c.conn = conn
	//}
	//return nil
}

// GetConnection creates and individual connection
func (c *Connection) GetConnection(context context.Context) (*sql.Conn, error) {
	return c.conn.Conn(context)
}

// -----------------------------------------------------
// Private
// -----------------------------------------------------

// prepareStatement creates a new goOra Statement, we use named returned values
// to override response in defer
func (c *Connection) prepareStatement(statement string) (stmt *sql.Stmt, err error) {
	// We will handle the possible panic error inside a defer function, if panic happens
	// stmt and error must be filled with nil, and recover as error
	defer func() {
		if r := recover(); r != nil {
			stmt = nil
			err = r.(error)
		}
	}()

	if err = c.Ping(); err != nil {
		return nil, err
	}

	// -----------------------------------------------
	// Evaluando conexión
	// -----------------------------------------------
	//if c.GetConnectionStatus() == ConnClosed || c.Ping() != nil {
	//	err = c.ReConnect()
	//	if err != nil {
	//		return nil, err
	//	}
	//}

	stmt, err = c.conn.Prepare(statement)
	if err != nil {
		return nil, err
	}

	// return
	return
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
func (c *Connection) unwrapRows(rows *goOra.DataSet) (*Container, error) {
	// closing rows
	defer func() {
		err := rows.Close()
		if err != nil {
			c.log.Err(err).Msg("Ocurrió un error al cerrar las filas")
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
			c.log.Err(err).Msg("Ocurrió un error al recorrer las filas")
			return nil, errors.New(fmt.Sprintf("error unwrapping rows [%s]", err.Error()))
		}
		container.addToRowsString(columns, values)
	}
	if rows.Err() != nil {
		c.log.Err(err).Msg("Ocurrió un error al final de recorrer las filas")
		return nil, errors.New(fmt.Sprintf("error unwrapping rows [%s]", err.Error()))
	}

	// returning data
	return container, nil
}

// unwrapRowsSql takes sql.Rows and convert to Container
func (c *Connection) unwrapRowsSql(rows *sql.Rows) (*Container, error) {
	// closing rows
	defer func() {
		err := rows.Close()
		if err != nil {
			c.log.Err(err).Msg("Ocurrió un error al cerrar las filas")
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
			c.log.Err(err).Msg("Ocurrió un error al recorrer las filas")
			return nil, errors.New(fmt.Sprintf("error unwrapping rows [%s]", err.Error()))
		}
		container.addToRows(columns, values)
	}
	if rows.Err() != nil {
		c.log.Err(err).Msg("Ocurrió un error al final de recorrer las filas")
		return nil, errors.New(fmt.Sprintf("error unwrapping rows [%s]", err.Error()))
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
func createConnection(constr string, configuration *ConnectionConfiguration, log *zerolog.Logger) (*sql.DB, error) {
	// Open connection via sql.Open interface
	conn, err := sql.Open("oracle", constr)
	if err != nil {
		return nil, CantCreateConnErr(err.Error())
	}

	// context timeout
	timeout := time.Duration(30) * time.Second

	if configuration != nil {
		if configuration.ConfigurationSet {

			log.Info().Msg(" ----------------------------------------  ")
			log.Info().Msgf(" ... ConfigurationSet : %v", configuration.ConfigurationSet)
			log.Info().Msgf(" ... MaxOpenConnections : %v", configuration.MaxOpenConnections)
			log.Info().Msgf(" ... MaxIdleConnections : %v", configuration.MaxIdleConnections)
			log.Info().Msgf(" ... MaxConnectionLifeTime : %v", configuration.MaxConnectionLifeTime)
			log.Info().Msgf(" ... MaxIdleConnectionTime : %v", configuration.MaxIdleConnectionTime)
			log.Info().Msg(" ----------------------------------------  ")

			conn.SetMaxOpenConns(configuration.MaxOpenConnections)
			conn.SetMaxIdleConns(configuration.MaxIdleConnections)
			conn.SetConnMaxLifetime(configuration.MaxConnectionLifeTime)
			conn.SetConnMaxIdleTime(configuration.MaxIdleConnectionTime)
			timeout = time.Duration(configuration.ContextTimeout) * time.Second
		}
	}

	// test connection
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// ping connection
	err = conn.PingContext(ctx)
	if err != nil {
		return nil, CantPingConnection(fmt.Sprintf("PingContext %v", err.Error()))
	}

	return conn, nil
}

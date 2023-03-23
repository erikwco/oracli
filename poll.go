package oracli

type Database struct {
	minConnection    int
	maxConnection    int64
	connectionString string
	inUse            int
	available        int
	total            int
	name             string
	Connections      map[string]*Connection
}

type Pool struct {
	Items map[string]Database
}

func NewPool() *Pool {
	// Database Container
	p := make(map[string]Database)
	//
	return &Pool{Items: p}
}

func (p *Pool) AddDatabase(minConnections int, maxConnections int64, connectionString string, name string) {
	cons := make(map[string]*Connection)

	// Connections
	//for i := 0; i < minConnections; i++ {
	//	con, err := NewConnection(connectionString, name)
	//	if err == nil {
	//		//cons[uuid] = con
	//	}
	//}

	// Database container
	db := Database{
		minConnection:    minConnections,
		maxConnection:    maxConnections,
		connectionString: connectionString,
		name:             name,
		Connections:      cons,
	}

	// Assign Database map
	p.Items[name] = db
}

func (p *Pool) GetConnection(name string) {
	_, ok := p.Items[name]
	if ok {

	}
}

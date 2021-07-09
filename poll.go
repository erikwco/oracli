package oracli

type Connections map[string][]Connection

type Pool struct {
	p Connections
}

func NewPool() *Pool {
	cons := make(Connections)
	return &Pool{p: cons}
}

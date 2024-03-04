package oracli

import (
	"errors"
	"fmt"
)

var EmptyConStrErr = errors.New("connection string can't be blank")
var CantCreateConnErr = func(error string) error {
	return errors.New(fmt.Sprintf("connection could not be created [%s]", error))
}
var CantPingConnection = func(error string) error { return errors.New(fmt.Sprintf("ping test failed [%s]", error)) }

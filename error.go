package oracli

import (
	"errors"
	"fmt"
)

var EmptyConStrErr = errors.New("connection string can't be blank")
var CantCreateConnErr = func(error string) error { return errors.New(fmt.Sprintf("connection could not be created [%s]", error)) }
var CantOpenDbErr = func(error string) error { return errors.New(fmt.Sprintf("connection can't be opened [%s]", error)) }
var TestConnErr = func(error string) error { return errors.New(fmt.Sprintf("testing connection [%s]", error)) }
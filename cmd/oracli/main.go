package main

import (
	"context"
	"fmt"
	"github.com/erikwco/oracli"
	"os"
	"strings"
)

func main() {

	ctx := context.Background()
	u := "SICAJA_SV"
	p := "Claro1920"
	srv := "172.24.2.150"
	prt := "1521"
	sid := "ODA_CR"
	conn, err := oracli.NewConnection(ctx, fmt.Sprintf("oracle://%s:%s@%s:%s/%s", u, p, srv, prt, sid), "SV")

	if err != nil {
		fmt.Printf("Error creating connection [%s]", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	// Executing select
	result := conn.Select("select cod_Agencia, nom_agencia from agencias", nil)
	if result.Error != nil {
		fmt.Printf("Error getting records [%s]", strings.TrimSpace(result.Error.Error()))
		os.Exit(1)
	}

	for _, v := range result.Data {
		fmt.Println(v)
	}

	// Executing procedure with ref-cursor return
	code := conn.NewParam("vCode", "110012865")
	ref := conn.NewParam("vResult", "")
	ref.IsRef = true
	params := make([]oracli.Param, 2)
	params[0] = *code
	params[1] = *ref
	//params = append(params, *code)
	//params = append(params, *ref)

	r := conn.Select("begin u_get_monedas(:vCode, :vResult); end;", params)

	if r.Error != nil {
		fmt.Printf("Error getting records [%s]", strings.TrimSpace(r.Error.Error()))
		os.Exit(1)
	}

	for _, v := range r.Data {
		fmt.Println(v)
	}

	//time.Sleep(5 * time.Second)


}

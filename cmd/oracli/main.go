package main

import (
	"github.com/erikwco/oracli"
	"log"
)

func main() {
	con, err := oracli.NewConnectionWithParams("172.24.2.150", 1521, "SICAJA_SV", "Claro1920", "ODA_CR", nil, "oda_503")
	if err != nil {
		log.Fatal(err)
	}
	defer con.Close()

	result := con.Select("select endpoint from si_configuration where process = 'OTC0050' and env = 'EPR'", nil)
	if result.Error != nil {
		log.Fatal(result.Error)
	}

	if !result.HasData {
		log.Fatal("sin datos")
	}

	log.Printf("Endpoint => %v", result.Data[0]["ENDPOINT"])

}

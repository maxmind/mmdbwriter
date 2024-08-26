// country-writer-scratch is an example of how to create an MaxMind DB file from
// any csv file having cidr address mapped to country, city data.

package main

import (
	"encoding/csv"
	"io"
	"log"
	"net"
	"os"

	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
)

func main() {
	writer, err := mmdbwriter.New(mmdbwriter.Options{
		DatabaseType: "My-Country-DB",
	},
	)

	if err != nil {
		log.Fatal(err)
	}

	// The csv file is in this format
	// 54.36.84.100/22,France,Paris
	// 142.44.196.0/25,India,Mumbai

	fh1, err := os.Open("C:\\Users\\Aabhas\\Desktop\\generate\\random-cidr-country.csv")
	if err != nil {
		log.Fatal(err)
	}

	r := csv.NewReader(fh1)

	// Reading the file line by line
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}

		_, network, err := net.ParseCIDR(row[0])
		if err != nil {
			log.Fatal(err)
		}

		// Expected output format is this
		// {"country": "France", "city": "Paris]"}
		record := mmdbtype.Map{
			"country": mmdbtype.String(row[1]),
			"city":    mmdbtype.String(row[2]),
		}

		err = writer.Insert(network, record)
		if err != nil {
			log.Println(err)
		}
	}

	fh2, err := os.Create("country-scratch-out.mmdb")
	if err != nil {
		log.Fatal(err)
	}

	//write to the mmdb file
	_, err = writer.WriteTo(fh2)
	if err != nil {
		log.Fatal(err)
	}

}

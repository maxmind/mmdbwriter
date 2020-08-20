package main

import (
	"log"
	"os"

	"github.com/maxmind/mmdbwriter"
)

func main() {
	writer, err := mmdbwriter.Load("GeoLite2-City.mmdb", mmdbwriter.Options{})
	if err != nil {
		log.Fatal(err)
	}

	// Insert your own data...

	fh, err := os.Create("out.mmdb")
	if err != nil {
		log.Fatal(err)
	}

	_, err = writer.WriteTo(fh)
	if err != nil {
		log.Fatal(err)
	}
}

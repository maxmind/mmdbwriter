package main

import (
	"log"
	"net"
	"os"
	"strings"

	"github.com/maxmind/mmdbwriter"
	"github.com/maxmind/mmdbwriter/mmdbtype"
	"github.com/oschwald/maxminddb-golang"
)

func main() {
	writer, err := mmdbwriter.New(
		mmdbwriter.Options{
			DatabaseType: "My-ASN-DB",
			RecordSize:   28,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	db, err := maxminddb.Open("GeoLite2-City.mmdb")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	records := map[uintptr]mmdbtype.DataType{}

	networks := db.Networks()
	for networks.Next() {
		offset, err := networks.Offset()
		if err != nil {
			log.Fatal(err)
		}
		var network *net.IPNet
		record, ok := records[offset]
		if ok {
			network, err = networks.Network(nil)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			network, err = networks.Network(&record)
			if err != nil {
				log.Fatal(err)
			}
			records[offset] = record
		}

		err = writer.Insert(network, record)
		if err != nil && !strings.Contains(err.Error(), "which is in an aliased network") {
			log.Fatal(err)
		}
	}
	writer.Finalize()

	fh, err := os.Create("out.mmdb")
	if err != nil {
		log.Fatal(err)
	}

	_, err = writer.WriteTo(fh)
	if err != nil {
		log.Fatal(err)
	}
}

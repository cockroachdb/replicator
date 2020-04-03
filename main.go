package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	_ "github.com/lib/pq"
)

var connectionString = flag.String("conn", "postgresql://root@localhost:26257/defaultdb?sslmode=disable", "cockroach connection string")
var port = flag.Int("port", 26258, "http server listening port")

var sendTable = flag.String("send_table", "", "Name of the table sending data")

var resultDB = flag.String("db", "defaultdb", "database for the receiving table")
var resultTable = flag.String("table", "", "receiving table, must exist")

var sinkDB = flag.String("sink_db", "_CDC_SINK", "db for storing temp sink tables")

var dropDB = flag.Bool("drop", false, "Drop the sink db before starting?")

func createHandler(db *sql.DB, sinks *Sinks) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "%+s\n", r.RequestURI)

		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(w, "%s\n", body)
		fmt.Printf("%s\n", r.Header)
		fmt.Printf("%s\n", r.RequestURI)
		fmt.Printf("%s\n", body)

		_, err = parseNdjsonURL(r.RequestURI)
		if err != nil {
			log.Printf(err.Error())
		}
	}
}

func main() {
	db, err := sql.Open("postgres", *connectionString)
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}
	defer db.Close()

	if *dropDB {
		if err := DropSinkDB(db); err != nil {
			log.Fatal(err)
		}
	}

	if err := CreateSinkDB(db); err != nil {
		log.Fatal(err)
	}

	sinks := CreateSinks()

	// Add all the sinks here
	if err := sinks.AddSink(db, *sendTable, *resultDB, *resultTable); err != nil {
		log.Fatal(err)
	}

	handler := createHandler(db, sinks)
	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}

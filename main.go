package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net/http"

	_ "github.com/lib/pq"
)

var connectionString = flag.String("conn", "postgresql://root@localhost:26257/defaultdb?sslmode=disable", "cockroach connection string")
var port = flag.Int("port", 26258, "http server listening port")

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "%+s", r.RequestURI)
}

func main() {
	db, err := sql.Open("postgres", *connectionString)
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}

	// Create the "accounts" table.
	if _, err := db.Exec(
		"CREATE DATABASE IF NOT EXISTS _CDC_SINK"); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", handler)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}

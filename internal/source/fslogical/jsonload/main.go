// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Command jsonload reads a file formatted as a single json array and
// inserts the records into a local firestore emulator. This is intended
// only for development of cdc-sink itself.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"

	"cloud.google.com/go/firestore"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func main() {
	collectionName := flag.String("c", "my-collection", "FS collection name")
	fPath := flag.String("f", "", "input file")
	projectName := flag.String("p", "my-project", "FS project name")
	workers := flag.Int("w", 16, "number of workers")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := os.Open(*fPath)
	if err != nil {
		log.Fatal(err)
	}

	// Create a connection to the emulator, to populate source docs.
	fs, err := firestore.NewClient(ctx, *projectName)
	if err != nil {
		log.Fatal(err)
	}
	coll := fs.Collection(*collectionName)

	eg, egCtx := errgroup.WithContext(ctx)

	ch := make(chan map[string]interface{}, *workers)
	ct := 0
	eg.Go(func() error {
		defer close(ch)

		in := json.NewDecoder(f)
		if tkn, err := in.Token(); err != nil {
			return err
		} else if tkn != json.Delim('[') {
			return errors.Errorf("expecting an array, got %v", tkn)
		}

		for in.More() {
			var data map[string]interface{}
			if err := in.Decode(&data); err != nil {
				return err
			}
			select {
			case ch <- data:
				ct++
			case <-egCtx.Done():
				return nil
			}
		}
		return nil
	})

	// Concurrent inserts.
	for i := 0; i < *workers; i++ {
		eg.Go(func() error {
			for data := range ch {
				id, ok := data["id"].(string)
				if !ok {
					return errors.New("no id or not a string")
				}
				delete(data, "id")
				data["_firestoreUpdatedAt"] = firestore.ServerTimestamp
				_, err := coll.Doc(id).Set(egCtx, data)
				if err != nil {
					return errors.Errorf("id=%s: %v", id, err)
				}
				log.Infof("inserted %s", id)
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}
	log.Infof("finished inserting %d docs", ct)
}

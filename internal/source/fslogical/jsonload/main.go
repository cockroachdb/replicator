// Copyright 2023 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

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

	ch := make(chan map[string]any, *workers)
	count := 0
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(ch)

		in := json.NewDecoder(f)
		if tkn, err := in.Token(); err != nil {
			return err
		} else if tkn != json.Delim('[') {
			return errors.Errorf("expecting an array, got %v", tkn)
		}

		for in.More() {
			var data map[string]any
			if err := in.Decode(&data); err != nil {
				return err
			}
			select {
			case ch <- data:
				count++
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
	log.Infof("finished inserting %d docs", count)
}

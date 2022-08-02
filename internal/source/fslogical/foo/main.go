// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"cloud.google.com/go/firestore"
	log "github.com/sirupsen/logrus"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a connection to the emulator, to populate source docs.
	fs, err := firestore.NewClient(ctx, "my-project")
	if err != nil {
		log.Fatal(err)
	}

	coll := fs.Collection("test2")
	docIds := make([]string, 1024)
	now := time.Now()
	for i := range docIds {
		doc, _, err := coll.Add(ctx, map[string]interface{}{
			"_firestoreUpdatedAt": time.Now(),
			"v":                   fmt.Sprintf("value %d", i),
			"updated_at":          now.Format(time.RFC3339),
			"random":              rand.Intn(1024),
		})
		if err != nil {
			log.Fatal(err)
		}

		log.Infof("inserted %s", doc.Path)
		docIds[i] = doc.ID
	}
}

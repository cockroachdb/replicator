// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sinktest

import (
	"context"
	"flag"
	"time"
)

var caseTimout = flag.Duration(
	"caseTimout",
	2*time.Minute,
	"raise this value when debugging to allow individual tests to run longer",
)

// key is a typesafe context key used by Context().
type key struct{}

// Context returns a per-test Context which has a common timeout
// behavior and global connection pool. This method will panic if
// the database could not be created
func Context() (context.Context, *DBInfo, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), *caseTimout)
	db, err := bootstrap(ctx)
	if err != nil {
		panic(err)
	}
	ctx = context.WithValue(ctx, key{}, db)
	return ctx, db, cancel
}

// DB returns the database associated with the Context.
func DB(ctx context.Context) *DBInfo {
	info, _ := ctx.Value(key{}).(*DBInfo)
	return info
}

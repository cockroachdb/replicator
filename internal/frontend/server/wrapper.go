// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"log"
	"net/http"
	"time"
)

type responseSpy struct {
	http.ResponseWriter
	statusCode int
}

func (s *responseSpy) WriteHeader(statusCode int) {
	s.statusCode = statusCode
	s.ResponseWriter.WriteHeader(statusCode)
}

func logWrapper(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spy := &responseSpy{w, 0}
		start := time.Now()
		h.ServeHTTP(spy, r)
		elapsed := time.Since(start)
		log.Printf("http status %d %s: %s in %s",
			spy.statusCode,
			http.StatusText(spy.statusCode),
			r.URL.Path,
			elapsed)
	})
}

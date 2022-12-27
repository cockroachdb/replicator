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
	"io"
	"net/http"
	"strconv"
	"time"

	joonix "github.com/joonix/log"
	log "github.com/sirupsen/logrus"
)

type readerSpy struct {
	r     io.ReadCloser
	count int64
}

var _ io.ReadCloser = (*readerSpy)(nil)

func (s *readerSpy) Close() error {
	return s.r.Close()
}

func (s *readerSpy) Read(dest []byte) (int, error) {
	n, err := s.r.Read(dest)
	s.count += int64(n)
	return n, err
}

type responseSpy struct {
	http.ResponseWriter
	statusCode int
	count      int64
}

var _ http.ResponseWriter = (*responseSpy)(nil)

func (s *responseSpy) Write(buf []byte) (int, error) {
	n, err := s.ResponseWriter.Write(buf)
	s.count += int64(n)
	return n, err
}

func (s *responseSpy) WriteHeader(statusCode int) {
	s.statusCode = statusCode
	s.ResponseWriter.WriteHeader(statusCode)
}

// logWrapper wraps the given handler with performance monitoring and
// logging.
func logWrapper(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Wrap interfaces, so we can observe payload sizes.
		bodySpy := &readerSpy{r: r.Body}
		r.Body = bodySpy
		rSpy := &responseSpy{w, 0, 0}

		start := time.Now()
		defer func() {
			latency := time.Since(start)
			// Per https://github.com/joonix/log#log
			msg := log.WithField("httpRequest", &joonix.HTTPRequest{
				Latency:      latency,
				Status:       rSpy.statusCode,
				Request:      r,
				ResponseSize: rSpy.count,
				RequestSize:  bodySpy.count,
			})

			r := recover()
			if r == nil {
				// Normal exit. Just log the request data.
				httpCodes.WithLabelValues(strconv.Itoa(rSpy.statusCode)).Inc()
				httpLatency.Observe(latency.Seconds())
				httpPayloadIn.Observe(float64(bodySpy.count))
				msg.Debug()
				return
			}

			// Trigger shutdown, but allow the goroutine to finish
			// normally. This allows the server's graceful shutdown
			// behavior to drain quickly.
			if err, ok := r.(error); ok {
				msg = msg.WithError(err)
			}
			go msg.Fatal("fatal error in request handler")
		}()

		h.ServeHTTP(rSpy, r)
	})
}

// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package pjson implements a parallelized wrapper for encoding and
// decoding json objects.
package pjson

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Decode a source of JSON bytes into a target slice in a parallel
// manner.
func Decode[T any](ctx context.Context, dest []T, src func(i int) []byte) error {
	numWorkers := runtime.GOMAXPROCS(0)
	eg, errCtx := errgroup.WithContext(ctx)
	for worker := 0; worker < numWorkers; worker++ {
		worker := worker
		eg.Go(func() error {
			for idx := worker; idx < len(dest); idx += numWorkers {
				if err := errCtx.Err(); err != nil {
					return err
				}
				dec := json.NewDecoder(bytes.NewReader(src(idx)))
				dec.UseNumber()
				if err := dec.Decode(&dest[idx]); err != nil {
					return errors.Wrapf(err, "idx %d", idx)
				}
			}
			return nil
		})
	}

	return eg.Wait()
}

// Encode objects into a target slice in a parallel manner.
func Encode[T any](ctx context.Context, dest [][]byte, src func(i int) T) error {
	numWorkers := runtime.GOMAXPROCS(0)
	eg, errCtx := errgroup.WithContext(ctx)
	for worker := 0; worker < numWorkers; worker++ {
		worker := worker
		eg.Go(func() error {
			for idx := worker; idx < len(dest); idx += numWorkers {
				if err := errCtx.Err(); err != nil {
					return err
				}
				var err error
				dest[idx], err = json.Marshal(src(idx))
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	return eg.Wait()
}

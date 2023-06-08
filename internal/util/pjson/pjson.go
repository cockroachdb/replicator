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

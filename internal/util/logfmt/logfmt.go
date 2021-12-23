// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package logfmt adds additional details to log messages with errors.
package logfmt

import (
	"fmt"

	log "github.com/sirupsen/logrus"
)

const detailKey = "detail"

// Wrap adds a workaround for there being no support for
// automatically printing the details of an error to expose the stack
// trace. This formatter adds an extra detail field to log entries
// that contain an ErrorKey.
//
// https://github.com/sirupsen/logrus/issues/895
func Wrap(f log.Formatter) log.Formatter {
	return &detailer{f}
}

type detailer struct {
	log.Formatter
}

// Format implements log.Formatter.
func (d *detailer) Format(e *log.Entry) ([]byte, error) {
	if e.Data != nil {
		if err, ok := e.Data[log.ErrorKey].(error); ok {
			// Don't overwrite anywhere there may already be a detail key.
			if _, existing := e.Data[detailKey]; !existing {
				e.Data[detailKey] = fmt.Sprintf("%+v", err)
			}
		}
	}
	return d.Formatter.Format(e)
}

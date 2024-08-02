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

// Package logfmt adds additional details to log messages with errors.
package logfmt

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
)

const detailKey = "detail"

// Wrap adds a workaround for there being no support for automatically
// printing the details of an error to expose the stack trace. This
// formatter adds an extra detail field to log entries that contain an
// ErrorKey. If the error to be formatted is a pgconn.PgError, its
// subfields will also be added to the error message.
//
// https://github.com/sirupsen/logrus/issues/895
func Wrap(f log.Formatter) log.Formatter {
	return &detailer{f}
}

type detailer struct {
	log.Formatter
}

// sqlDetail represents a pgconn.PgError in a way that plays nicely
// with the various formatters.
type sqlDetail struct {
	Severity            string `json:"severity,omitempty"`
	SeverityUnlocalized string `json:"-"`
	Code                string `json:"code,omitempty"`
	Message             string `json:"message,omitempty"`
	Detail              string `json:"detail,omitempty"`
	Hint                string `json:"hint,omitempty"`
	Position            int32  `json:"position,omitempty"`
	InternalPosition    int32  `json:"internalPosition,omitempty"`
	InternalQuery       string `json:"internalQuery,omitempty"`
	Where               string `json:"where,omitempty"`
	SchemaName          string `json:"schemaName,omitempty"`
	TableName           string `json:"tableName,omitempty"`
	ColumnName          string `json:"columnName,omitempty"`
	DataTypeName        string `json:"dataTypeName,omitempty"`
	ConstraintName      string `json:"constraintName,omitempty"`
	File                string `json:"file,omitempty"`
	Line                int32  `json:"line,omitempty"`
	Routine             string `json:"routine,omitempty"`
}

func (s *sqlDetail) String() string {
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	enc.SetIndent("", " ")
	_ = enc.Encode(s)
	return sb.String()
}

// Format implements log.Formatter.
func (d *detailer) Format(e *log.Entry) ([]byte, error) {
	if e.Data != nil {
		if err, ok := e.Data[log.ErrorKey].(error); ok {
			// Don't overwrite anywhere there may already be a detail key.
			if _, existing := e.Data[detailKey]; !existing {
				e.Data[detailKey] = fmt.Sprintf("%+v", err)
			}

			var pgErr *pgconn.PgError
			switch {
			case errors.As(err, &pgErr):
				// Improve formatting of SQL errors.
				s := sqlDetail(*pgErr)
				e.Data["sql"] = &s

			case errors.Is(err, context.Canceled),
				errors.Is(err, context.DeadlineExceeded):
				// These errors are often a side effect of some other
				// malfunction and can be somewhat noisy in the logs.
				// Adding this field allows them to be readily ignored.
				e.Data["noisy"] = true
			}
		}
	}

	// Make it easier for operators to know that the logs are worth
	// looking at. Note that the formatter is only invoked for messages
	// at an enabled level.
	messageCount.WithLabelValues(e.Level.String()).Inc()

	return d.Formatter.Format(e)
}

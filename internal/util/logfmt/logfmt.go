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
	Severity         string `json:"severity,omitempty"`
	Code             string `json:"code,omitempty"`
	Message          string `json:"message,omitempty"`
	Detail           string `json:"detail,omitempty"`
	Hint             string `json:"hint,omitempty"`
	Position         int32  `json:"position,omitempty"`
	InternalPosition int32  `json:"internalPosition,omitempty"`
	InternalQuery    string `json:"internalQuery,omitempty"`
	Where            string `json:"where,omitempty"`
	SchemaName       string `json:"schemaName,omitempty"`
	TableName        string `json:"tableName,omitempty"`
	ColumnName       string `json:"columnName,omitempty"`
	DataTypeName     string `json:"dataTypeName,omitempty"`
	ConstraintName   string `json:"constraintName,omitempty"`
	File             string `json:"file,omitempty"`
	Line             int32  `json:"line,omitempty"`
	Routine          string `json:"routine,omitempty"`
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

			if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
				s := sqlDetail(*pgErr)
				e.Data["sql"] = &s
			}
		}
	}
	return d.Formatter.Format(e)
}

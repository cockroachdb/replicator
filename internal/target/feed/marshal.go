// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package feed

// This file defines mirror types for external representations of the
// config types, which can optimize for human-readability and tooling
// without having to compromise the working data types, which rely
// on pointer-equivalence for ease of making cdc-sink work.

import (
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

// envelope is the top-level configuration file structure. This is
// added to support future evolution of the configuration language.
type envelope struct {
	Feeds []*Feed `yaml:"feeds"`
}

// columnPayload is the external form of Column.
type columnPayload struct {
	SourceColumn ident.Ident   `yaml:"column,omitempty"`
	Deadline     time.Duration `yaml:"deadline,omitempty"`
	Expression   string        `yaml:"expression,omitempty"`
	TargetColumn ident.Ident   `yaml:"targetColumn,omitempty"`
	Synthetic    bool          `yaml:"synthetic,omitempty"`
}

var _ yaml.IsZeroer = (*columnPayload)(nil)

// IsZero implements yaml.IsZeroer to allow the omitempty option to work.
func (c *columnPayload) IsZero() bool {
	return c == nil || *c == columnPayload{}
}

func (c *Column) marshalPayload(sourceName ident.Ident) *columnPayload {
	colP := &columnPayload{
		Deadline:     c.Deadline,
		Expression:   c.Expression,
		Synthetic:    c.Synthetic,
		SourceColumn: sourceName,
	}
	if c.Target != sourceName {
		colP.TargetColumn = c.Target
	}
	return colP
}

func (c *Column) unmarshalPayload(p *columnPayload) error {
	c.Deadline = p.Deadline
	c.Expression = p.Expression
	c.Synthetic = p.Synthetic
	if p.TargetColumn.IsEmpty() {
		c.Target = p.SourceColumn
	} else {
		c.Target = p.TargetColumn
	}

	if c.Deadline < 0 {
		return errors.New("deadline must be non-negative")
	}
	if c.Synthetic && c.Expression == "" {
		return errors.New("synthetic columns must have an expression")
	}
	if c.Target.IsEmpty() {
		return errors.New("column with no target")
	}
	return nil
}

// tablePayload is the external form of Table.
type tablePayload struct {
	// SourceSchema is optional; inferred from the feed if not present.
	SourceSchema ident.Ident `yaml:"schema,omitempty"`
	// SourceTable is the only required field.
	SourceTable ident.Ident `yaml:"table,omitempty"`

	// TargetSchema and TableTable allow the destination schema and/or
	// target to be overridden.
	TargetSchema ident.Ident `yaml:"targetSchema,omitempty"`
	TargetTable  ident.Ident `yaml:"targetTable,omitempty"`

	// A list of column names for compare-and-set operations.
	CAS []ident.Ident `yaml:"cas,omitempty"`

	// Optional extra data about columns.
	Columns []*columnPayload `yaml:"columns,omitempty"`
}

func (t *Table) marshalPayload(sourceName ident.Table) *tablePayload {
	t.mu.Lock()
	defer t.mu.Unlock()

	p := &tablePayload{
		CAS:         make([]ident.Ident, len(t.CAS)),
		Columns:     make([]*columnPayload, 0, len(t.mu.columns)),
		SourceTable: sourceName.Table(),
	}
	if sourceName.Schema() != ident.Public {
		p.SourceSchema = sourceName.Schema()
	}

	for idx := range p.CAS {
		p.CAS[idx] = t.CAS[idx].Target
	}

	// Only include columns with explicit configuration.
	for ident, col := range t.mu.columns {
		if colP := col.marshalPayload(ident); !colP.IsZero() {
			p.Columns = append(p.Columns, colP)
		}
	}
	// Ensure stable output since we iterate over a map.
	sort.Slice(p.Columns, func(i, j int) bool {
		return strings.Compare(
			p.Columns[i].SourceColumn.String(),
			p.Columns[j].SourceColumn.String(),
		) < 0
	})

	if t.Target.Schema() != sourceName.Schema() {
		p.TargetSchema = t.Target.Schema()
	}

	if t.Target.Table() != sourceName.Table() {
		p.TargetTable = t.Target.Table()
	}

	return p
}

func (t *Table) unmarshalPayload(feedName ident.Table, p *tablePayload) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.Target = feedName
	if !p.TargetSchema.IsEmpty() {
		t.Target = ident.NewTable(t.Target.Database(), p.TargetSchema, t.Target.Table())
	}
	if !p.TargetTable.IsEmpty() {
		t.Target = ident.NewTable(t.Target.Database(), t.Target.Schema(), p.TargetTable)
	}

	// Copy over configured properties.
	t.mu.columns = make(map[ident.Ident]*Column, len(p.Columns))
	for _, colP := range p.Columns {
		col := &Column{}
		if err := col.unmarshalPayload(colP); err != nil {
			return err
		}
		t.mu.columns[colP.SourceColumn] = col
	}

	// Set the CAS flag for named columns.
	for _, ident := range p.CAS {
		col := t.columnLocked(ident)
		col.CAS = true
		t.CAS = append(t.CAS, col)
	}

	return nil
}

// feedPayload is the external form of Feed. The order of the fields
// is preserved when serializing, so prefer a human-centric ordering.
type feedPayload struct {
	Name      string          `yaml:"name"`
	Database  ident.Ident     `yaml:"database"`
	Schema    ident.Ident     `yaml:"schema"`
	Immediate bool            `yaml:"immediate"`
	Version   int             `yaml:"version"`
	Tables    []*tablePayload `yaml:"tables"`
}

func (f *Feed) marshalPayload() *feedPayload {
	ret := &feedPayload{
		Immediate: f.Immediate,
		Name:      f.Name.Raw(),
		Database:  f.Schema.Database(),
		Schema:    f.Schema.Schema(),
		Tables:    make([]*tablePayload, 0, len(f.Tables)),
		Version:   f.Version,
	}
	for tableSourceName, tbl := range f.Tables {
		ret.Tables = append(ret.Tables, tbl.marshalPayload(tableSourceName))
	}
	// Sort tables for stable output.
	sort.Slice(ret.Tables, func(i, j int) bool {
		tblI := ret.Tables[i]
		tblJ := ret.Tables[j]
		if c := strings.Compare(tblI.SourceSchema.String(), tblJ.SourceSchema.String()); c != 0 {
			return c < 0
		}
		c := strings.Compare(tblI.SourceTable.String(), tblJ.SourceTable.String())
		return c < 0
	})

	return ret
}

// MarshalYAML returns a YAML representation of the Feed.
func (f *Feed) MarshalYAML() (interface{}, error) {
	return f.marshalPayload(), nil
}

// UnmarshalYAML parses a YAML representation of a Feed.
func (f *Feed) UnmarshalYAML(value *yaml.Node) error {
	var p feedPayload
	if err := value.Decode(&p); err != nil {
		return err
	}
	return f.unmarshalPayload(&p)
}

func (f *Feed) unmarshalPayload(p *feedPayload) error {
	*f = Feed{
		Immediate: p.Immediate,
		Name:      ident.New(p.Name),
		Schema:    ident.NewSchema(p.Database, p.Schema),
		Tables:    make(map[ident.Table]*Table, len(p.Tables)),
		Version:   p.Version,
	}
	for _, tblP := range p.Tables {
		if tblP.SourceTable.IsEmpty() {
			return errors.New("source table must not be empty")
		}
		var schema ident.Ident
		if tblP.SourceSchema.IsEmpty() {
			schema = ident.Public
		} else {
			schema = tblP.SourceSchema
		}
		tableSourceName := ident.NewTable(f.Schema.Database(), schema, tblP.SourceTable)

		var next Table
		if err := next.unmarshalPayload(tableSourceName, tblP); err != nil {
			return err
		}
		f.Tables[tableSourceName] = &next
	}
	return nil
}

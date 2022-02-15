--  Copyright 2022 The Cockroach Authors.
--
--  Use of this software is governed by the Business Source License
--  included in the file licenses/BSL.txt.
--
--  As of the Change Date specified in that file, in accordance with
--  the Business Source License, use of this software will be governed
--  by the Apache License, Version 2.0, included in the file
--  licenses/APL.txt.

-- noinspection SqlNoDataSourceInspectionForFile

-- NB: The qualified _cdc_sink names below will be replaced by tests
-- with a temporary database name.

-- General manifest and top-level properties of an incoming feed.
CREATE TABLE IF NOT EXISTS _cdc_sink.feeds
(
    feed_name      string PRIMARY KEY,            -- Appears in the incoming URL
    active         bool   NOT NULL DEFAULT TRUE,  -- Big red switch
    immediate      bool   NOT NULL DEFAULT FALSE, -- Bypass two-phase commit
    version        int    NOT NULL DEFAULT 1,     -- SELECT FOR UPDATE

-- These columns improve the UX by allowing a default value when
-- interpreting the YAML representation of the feed.
    default_db     STRING NOT NULL,
    default_schema STRING NOT NULL DEFAULT 'public'
);

-- Maps incoming table names to destination tables.
-- All tables in all incoming feeds must have an entry in this table.
CREATE TABLE IF NOT EXISTS _cdc_sink.feed_tables
(
    feed_name     string REFERENCES _cdc_sink.feeds
        ON UPDATE CASCADE ON DELETE CASCADE,
    source_schema string NOT NULL DEFAULT 'public',
    source_table  string NOT NULL,

-- It would be simple to allow in-flight renaming of tables for migration cases.
    target_db     string NOT NULL,
    target_schema string NOT NULL DEFAULT 'public',
    target_table  string NOT NULL,

    PRIMARY KEY (feed_name, source_schema, source_table),
-- Only one feed should ever target any single table in the destination cluster.
    UNIQUE INDEX feed_target_is_unique (target_db, target_schema, target_table)
);

-- Incoming columns need only be listed here if some property needs to be set.
CREATE TABLE IF NOT EXISTS _cdc_sink.feed_columns
(
    feed_name     string   NOT NULL,
    source_schema string   NOT NULL DEFAULT 'public',
    source_table  string   NOT NULL,
    source_column string   NOT NULL,

-- In general, this will be equal to feed_name.
    target_column string   NOT NULL,

-- A SQL expression to use in place of target_name, to support easy
-- migration adjustments.
    target_expr   string   NOT NULL DEFAULT '',

-- The 1-based index in which the column appears in the CAS tuple.
    cas_order     int      NOT NULL DEFAULT 0,

-- Non-zero values apply deadline behavior to the column.
    deadline      interval NOT NULL DEFAULT '0',

-- Indicates that we should not expect to see this column in the input
-- data. Implies the existence of target_expr.
    synthetic     bool     NOT NULL DEFAULT false
        CHECK (NOT synthetic OR target_expr != ''),

    PRIMARY KEY (feed_name, source_schema, source_table, source_column),
    FOREIGN KEY (feed_name, source_schema, source_table)
        REFERENCES _cdc_sink.feed_tables
        ON UPDATE CASCADE ON DELETE CASCADE
);

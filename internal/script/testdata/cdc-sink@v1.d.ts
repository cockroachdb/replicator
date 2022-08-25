/*
 * Copyright 2022 The Cockroach Authors.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.txt.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0, included in the file
 * licenses/APL.txt.
 */


/**
 * The user-script API provided by cdc-sink.
 *
 * The contents of this file can be retrieved by running
 * `cdc-sink userscript --api`.
 */
declare module "cdc-sink@v1" {
    /**
     * The name of a SQL column.
     */
    type Column = string;

    /**
     * A document is a loosely-typed bag of values. The specific
     * interpretation depends on the replication source. In general, any
     * data type which can be coerced to a JSON representation is safe
     * to use as a property value.
     */
    interface Document { // Interface necessary to break type cycle.
        [x: string]: DocumentValue;
    }

    /**
     * Property values to be found within a Document: any JSON-ish type.
     */
    type DocumentValue =
        null
        | boolean
        | number
        | string
        | Document
        | DocumentValueArray
    type DocumentValueArray = Array<DocumentValue>

    /**
     * A time duration.
     *
     * @see https://pkg.go.dev/time#ParseDuration
     */
    type Duration = string;

    /**
     * The name of a SQL table.
     */
    type Table = string;

    /**
     * Declare a datasource to operate on.
     *
     * @param sourceName - The name of a table, collection, or other
     * identifiable data product provided by the replication source.
     * @param props - Properties to configure the source.
     */
    function configureSource(
        sourceName: string,
        props: ConfigureSourceDestination & Partial<ConfigureSourceOptions>);


    /**
     * A mandatory destination for a configured source: either a mapper
     * function or the name of a destination table to pass through to.
     *
     * @see configureSource
     */
    type ConfigureSourceDestination = {
        /**
         * A function to dispatch documents to zero or more
         * destination tables. Mappers allow complex input
         * datastructures (e.g. nested documents or documents with
         * variable schemas) to be broken up and stored in some number
         * of tables that are subsequently joined with SQL queries.
         *
         * @param doc - The source document
         * @returns A mapping of target table names documents. A null
         * value will entirely discard the source document.
         */
        dispatch: (doc: Document) => Record<Table, Document[]>

        /**
         * The destination table to apply deletion operations to. In
         * cases when a dispatch function fans out an incoming document
         * across multiple tables, an <code>ON DELETE CASCADE</code>
         * foreign-key relationship should be used to ensure correct
         * propagation.
         */
        deletesTo: Table
    } | {
        /**
         * The name of a destination table.
         */
        target: Table
    };

    /**
     * Reserved for future expansion.
     *
     * @see configureSource
     */
    type ConfigureSourceOptions = {}

    /**
     * Configure a table within the destination database.
     *
     * @param tableName - The name of the table.
     * @param props - Properties to configure the table.
     * @see https://github.com/cockroachdb/cdc-sink#data-application-behaviors
     */
    function configureTable(
        tableName: Table,
        props: Partial<ConfigureTableOptions>);

    /**
     * @see configureTable
     */
    type ConfigureTableOptions = {
        /**
         * A list of columns to enable compare-and-set behavior.
         */
        cas: Column[];
        /**
         * Enable deadlining behavior, to discard mutations when the
         * named timestamp column is older than the given duration.
         */
        deadlines: { [k: Column]: Duration };
        /**
         * Replacement SQL expressions to use when upserting columns.
         * The placeholder <code>$0</code> will be replaced with the
         * specific value.
         */
        exprs: { [k: Column]: string };
        /**
         * The name of a JSONB column that unmapped properties will be
         * stored in.
         */
        extras: Column;
        /**
         * A mapping function which may modify or discard a single
         * mutation to be applied to the target table.
         * @param d - The source document
         * @returns The document to upsert, or null to do nothing.
         */
        map: (d: Document) => Document;
        /**
         * Columns that may be ignored in the input data. This allows,
         * for example, columns to be dropped from the destination
         * table.
         */
        ignore: { [k: Column]: boolean }
    };

    /**
     * Set runtime options. This function provides an alternate means of
     * setting some or all of the CLI flags. For example, this allows
     * configuration that is common to all (dev, test, staging,
     * production) environments to be checked into the user-script,
     * while the few per-environment options are set by CLI flags.
     *
     * @param opts - runtime options, refer to --help for details.
     */
    function setOptions(opts: { [k: string]: string });
}

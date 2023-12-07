/*
 * Copyright 2023 The Cockroach Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
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
    type Document = {
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
        | Array<DocumentValue>

    /**
     * A time duration.
     *
     * @see https://pkg.go.dev/time#ParseDuration
     */
    type Duration = string;

    /**
     * The name of a SQL table. This may be of the form
     * <code>table</code>, <code>database.table</code>, or
     * <code>database.schema.table</code>. Elements of the table name
     * may be quoted in cases where the element is not otherwise a valid
     * SQL identifier, e.g.:
     * <code>database.schema."table.with.dots.in.the.name"</code>.
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
        props: ConfigureSourceDestination & Partial<ConfigureSourceOptions>): void;


    /**
     * A mandatory destination for a configured source: either a
     * dispatch function or the name of a destination table to pass
     * through to.
     *
     * @see configureSource
     */
    type ConfigureSourceDestination = {
        /**
         * A function to dispatch documents to zero or more destination
         * tables. Dispatchers allow complex input datastructures (e.g.
         * nested documents or documents with variable schemas) to be
         * broken up and stored in some number of tables that are
         * subsequently joined with SQL queries.
         *
         * @param doc - The source document
         * @param meta - Source-specific metadata about the document.
         * @returns A mapping of target table names to documents. A null
         * value will entirely discard the source document.
         */
        dispatch: (doc: Document, meta: Document) => Record<Table, Document[]> | null

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
     * @see configureSource
     */
    type ConfigureSourceOptions = {
        /**
         * Sources which support dynamic sub-collections of data may
         * set the recurse property. This will cause any sub-documents
         * to be passed to the source's destination.
         */
        recurse: boolean;
    }

    /**
     * Configure a table within the destination database.
     *
     * @param tableName - The name of the table.
     * @param props - Properties to configure the table.
     * @see https://github.com/cockroachdb/cdc-sink#data-application-behaviors
     */
    function configureTable(
        tableName: Table,
        props: Partial<ConfigureTableOptions>): void;

    /**
     * Each mutation to apply is passed to the user-defined apply
     * function as an ApplyOp. It is a union type either representing
     * the deletion of some primary key or an upsert to be applied.
     */
    type ApplyOp = {
        action: "delete";

        pk: DocumentValue[];
    } | {
        action: "upsert";

        data: Document;
        meta: Document;
        pk: DocumentValue[];
    }

    /**
     * getTX may be called from any callback provided to {@link
     * configureTable} or subsequent promise invocations. It will throw
     * an exception if it is called from any other context.
     */
    function getTX(): TargetTX;

    /**
     * @see configureTable
     */
    type ConfigureTableOptions = {
        /**
         * Override cdc-sink's default apply behavior. The userscript
         * assumes all responsibility for interacting with the target
         * database. Access to the target database is provided via
         * {@link getTX}.
         *
         * @param ops - The operations to apply to the target database.
         */
        apply(ops: Iterable<ApplyOp>): Promise<any>;
    } | {
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
         * Columns that may be ignored in the input data. This allows,
         * for example, columns to be dropped from the destination
         * table.
         */
        ignore: { [k: Column]: boolean }
        /**
         * A mapping function which may modify or discard a single
         * mutation to be applied to the target table.
         * @param d - The source document
         * @param meta - Source-specific metadata about the document.
         * @returns The document to upsert, or null to do nothing.
         */
        map: (d: Document, meta: Document) => Document | null;
        /**
         * Enables a user-defined, two- or three-way merge function.
         */
        merge: MergeFunction | StandardMerge;
    };

    /**
     * A MergeFunction may be bound to a table to resolve two- or
     * three-way merge conflicts when CAS mode is enabled.
     */
    type MergeFunction = (op: MergeOperation) => MergeResult;

    /**
     * @see configureTable
     */
    type MergeOperation = {
        /**
         * This field will be present only in a three-way merge operation.
         */
        before?: Document;
        /**
         * Metadata similar to that found in the dispatch() or map() functions.
         */
        meta: Document;
        /**
         * The incoming data that could not be applied to the target row.
         */
        proposed: Document;
        /**
         * A view of the conflicting row in the target database.
         */
        target: Document;
        /**
         * Unmerged will be set if {@link standardMerge} calls the
         * fallback merge function. This array will be populated with
         * the names of the columns that standardMerge() could not
         * automatically merge.
         */
        unmerged?: Column[];
    };

    /**
     * @see configureTable
     */
    type MergeResult = {
        /**
         * Values to write, unconditionally, into the target database.
         */
        apply: Document
    } | {
        /**
         * The mutation should be sent to the named dead-letter queue
         * for offline processing.
         */
        dlq: string
    } | {
        /**
         * The mutation should be dropped. Note that choosing this
         * option will cause data loss and a DLQ may be a better option.
         */
        drop: true
    };

    /**
     * This is an opaque type returned from {@link standardMerge}.
     */
    type StandardMerge = {};

    /**
     * A TargetColumn provides a view of cdc-sink's introspection of a
     * table in the destination database.
     */
    type TargetColumn = {
        /**
         * The column's DEFAULT expression, if one exists.
         */
        defaultExpr?: string;
        /**
         * True if the column wouldn't normally be operated on by cdc-sink.
         */
        ignored: boolean;
        /**
         * A quoted, SQL-safe representation of the column name.
         */
        name: string;
        /**
         * True if the column is part of the primary key.
         */
        primary: boolean;
        /**
         * The type of the SQL column.
         */
        type: string;
    };

    /**
     * TargetTX allows the userscript to execute arbitrary SQL
     * statements against the target database.
     */
    type TargetTX = {
        /**
         * Returns schema information about the destination table.
         * Columns are returned such that primary key columns will be
         * sorted first and in their index order.
         */
        columns(): TargetColumn[];

        /**
         * Execute a SQL command that returns no rows.
         * @param query - A SQL command to execute in the target
         * database. Substitution parameter syntax is target-specific.
         * @param params - Values for substitution parameters.
         * @returns A promise that resolves when the database command
         * has completed.
         */
        exec(query: string, ...params: any): Promise<void>;

        /**
         * Execute a SQL query that returns some number of rows.
         * @param query - A SQL command to execute in the target
         * database. Substitution parameter syntax is target-specific.
         * @param params - Values for substitution parameters.
         * @returns A promise that will resolve to an iterable over the
         * column values.
         */
        query(query: string, ...params: any): Promise<Iterable<any[]>>;

        /**
         * Returns a quoted, SQL-safe representation of the target
         * schema.
         */
        schema(): string;

        /**
         * Returns a quoted, fully-qualified, SQL-safe representation of
         * the target table.
         */
        table(): string;
    };

    /**
     * @returns a string containing a random UUID.
     */
    function randomUUID(): string;

    /**
     * Set runtime options. This function provides an alternate means of
     * setting some or all of the CLI flags. For example, this allows
     * configuration that is common to all (dev, test, staging,
     * production) environments to be checked into the user-script,
     * while the remaining per-environment options are set by CLI flags.
     *
     * @param opts - runtime options, refer to --help for details.
     */
    function setOptions(opts: { [k: string]: string }): void;

    /**
     * standardMerge returns a basic three-way merge operator. It will
     * identify the properties that have changed in the input and apply
     * them if op.before[prop] equals op.target[prop].
     *
     * This operator may be supplied with a fallback merge function that
     * will be invoked if one or more properties cannot be merged. If
     * the fallback is invoked, the {@link MergeOperation.unmerged}
     * array will be populated with the names of the properties that
     * could not be merged.
     *
     * @param fallback an optional {@link MergeFunction} that will be
     * invoked if there are unresolved conflicts.
     */
    function standardMerge(fallback?: MergeFunction): StandardMerge;
}

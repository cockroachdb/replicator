
// Copyright 2024 The Cockroach Authors
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


import * as api from "cdc-sink@v1";
import { Document, Table } from "cdc-sink@v1";

 /**
 * Configure a replication flow. It assumes that each incoming
 * mutation has a `_source` and `_timestamp` column.
 * It will enforce the following behavior:
 * If the `_source` is the same of the destination, discard the mutation.
 * If the mutation is already in the destination table (same private key),
 * check the `_timestamp`, if the incoming timestamp is newer, update the row,
 * otherwise write the mutation to the DQL table.
 *
 * @param region - The name of the destination region.
 * @param schema - The name of the schema.
 * @param tables - An array of table names.
 */
export function replicateTo(region: string, schema: string, tables: string[]) {
    console.log("configureSource " + schema)
    api.configureSource(schema, {
        dispatch: (doc: Document, meta: Document): Record<Table, Document[]> => {
            console.trace(JSON.stringify(doc), JSON.stringify(meta));
            let ret: Record<Table, Document[]> = {};
            if (doc._source !== region) {
                console.log("Processing " + doc.k + "(" + typeof doc.k + "):" + doc._source)
                ret[meta.table as string] = [doc];
            } else {
                console.log("Skipping " + doc.k + ":" + doc._source)
            }
            return ret
        },
        deletesTo: ""
    })

    for (let table of tables) {
        console.log("configureTable " + table)
        api.configureTable(table, {
            cas: ['_timestamp'],
            merge: api.standardMerge(() => ({ dlq: table }))
        })}
}

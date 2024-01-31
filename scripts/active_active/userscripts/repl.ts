
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
import { ApplyOp } from "cdc-sink@v1";

 /**
 * Configure a replication flow. It assumes that each incoming
 * operation `data` has a `_source` and `_timestamp` column.
 * It will enforce the following behavior:
 * If the operation is a delete and `before` is null, discard the operation.
 * This assumes that the changefeed is created with the `diff` option:
 * `before` is populated with the value of the row before an update or delete
 * was applied. The changefeed may create an event for a point delete of a
 * row that doesn't exist. In this case `before` will be `null`.
 * If the operation is an upsert and  `data._source` is the same of the destination, 
 * discard the operation.
 * If the row is already in the destination table (same private key),
 * check the `_timestamp`, if the incoming timestamp is newer, update the row,
 * otherwise write the mutation to the DQL table.
 *
 * @param target - The name of the target cluster.
 * @param tables - An array of table names.
 */
export function replicateTo(target: string, tables: string[]) {
    for (let table of tables) {
        console.log("Configuring replication for " + table)
        replicateTable(target,table)
    }
}

function replicateTable(target: string, table: string) {
    api.configureTable(table, {
        apply: async(ops: ApplyOp[]): Promise<any> => {
            ops = ops.filter((op: ApplyOp) => {
                return (op.action === "delete" && op.before != null) ||
                       (op.action === "upsert" && op.data._source !== target)
            });
            return api.getTX().apply(ops);
        },
        cas: ['_timestamp'],
        merge: api.standardMerge(() => ({ dlq: table }))
    })
}

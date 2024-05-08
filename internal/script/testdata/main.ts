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

import * as api from "replicator@v1"; // Well-known module name
import {ApplyOp} from "replicator@v1"; // Verify additional code imports.
import externalData from "./data.txt"; // Can import additional files from disk.
import * as lib from "./lib";
// import * as something from "https://some.cdn/package@1" is viable

api.configureSource("expander", {
    dispatch: (doc, meta) => {
        console.log(JSON.stringify(doc), JSON.stringify(meta));
        console.log(api.randomUUID());

        return {
            "table1": [{dest: "table1", msg: doc.msg}],
            "table2": [
                {dest: "table2", msg: doc.msg, idx: 0},
                {dest: "table2", msg: doc.msg, idx: 1}
            ],
        };
    },
    deletesTo: "table1"
});

api.configureSource("passthrough", {
    target: "some_table"
});

api.configureSource("recursive", {
    target: "top_level",
    recurse: true,
})

api.configureTable("all_features", {
    // apply may access the database transaction. It will receive
    // mutations that have already been processed by the map function.
    apply: (ops: ApplyOp[]): Promise<any> => {
        // It can delegate to the standard apply pipeline.
        return api.getTX().apply(ops)
    },
    // Compare-and-set operations.
    cas: ["cas0", "cas1"],
    // Drop old data.
    deadlines: {
        "dl0": "1h",
        "dl1": "1m"
    },
    // Allow deletions to be modified to suit differing schemas.
    deleteKey: key => {
        return [key[0], key[2]];
    },
    // Provide alternate SQL expressions to (possibly filtered) data.
    exprs: {
        "expr0": "fnv32($0::BYTES)",
        "expr1": lib.libraryFn(),
    },
    // Place unmapped data into JSONB column.
    extras: "overflow_column",
    // Allow column in target database to be ignored.
    ignore: {
        "ign0": true,
        "ign1": true,
        "ign2": false
    },
    // Final document fixups, or return null to drop it.
    map: doc => {
        doc.msg = externalData.trim();
        doc.num = 42;
        console.log("Hello debug", JSON.stringify(doc));
        return doc;
    },
    // This shows how a counter-like column can be merged by applying a
    // delta computed from the incoming payload.
    merge: api.standardMerge(op => {
        console.log("merge", JSON.stringify(op));
        op.unmerged.forEach(colName => {
            switch (colName) {
                case "val":
                    // Leading unary + coerces to numeric.
                    op.target.val = +op.target.val + +op.proposed.val - +op.before.val;
                    break;
                default:
                    // This could also arrange to return a DLQ result.
                    throw new Error("unexpected column name: " + colName)
            }
        })
        return {apply: op.target};
    }),
    // This is a tuning parameter which allows the maximum number of
    // rows in a single UPSERT or DELETE statement to be limited. This
    // is mainly needed for ultra-wide tables and databases with a
    // relatively small number of available bind variables.
    rowLimit: 99,
});

// Elide all deletes for the table, e.g.: for archival use cases.
api.configureTable("delete_elide", {
    deleteKey: () => null,
});

// Swap the order of PK elements.
api.configureTable("delete_swap", {
    deleteKey: key => [key[1], key[0]]
});

api.configureTable("drop_all", {
    map: () => null
});

api.configureTable("merge_dlq_all", {
    merge: () => ({dlq: "dead"})
});

api.configureTable("merge_drop_all", {
    merge: () => ({drop: true})
});

// This uses a fallback to send unmerged updates to a DLQ.
api.configureTable("merge_or_dlq", {
    merge: api.standardMerge(() => ({dlq: "dead"}))
});

// This demonstrates how, when using a time-based CAS field, one can
// account for clock skew. In this case, we'll allow merges to be
// applied only if they are within one minute.
const updatedAt = 'updated_at';
api.configureTable("skewed_merge_times", {
    apply: async (ops: ApplyOp[]): Promise<any> => {
        ops = ops.map(op => {
            if (op.action === "upsert") {
                op.data['epoch'] = 42;
            }
            return op;
        })
        return api.getTX().apply(ops)
    },
    cas: [updatedAt],
    merge: api.standardMerge((op: api.MergeOperation) => {
        console.log(JSON.stringify(op));
        // The updatedAt symbol is a string constant defined above.
        if (op.unmerged.length === 1 && op.unmerged[0] === updatedAt) {
            let then = Date.parse(op.proposed[updatedAt] + "");
            let now = Date.parse(op.target[updatedAt] + "");
            // Accept if the delta is less than one minute.
            if (Math.abs(now - then) <= 60000 /* 1 minute */) {
                op.proposed[updatedAt] = op.target[updatedAt];
                return {apply: op.proposed};
            }
        }
        return {dlq: "dead"}
    }),
});

// This demonstrates how a delete operation with the 'diff' option
// can be transformed into a soft delete. The `before` field is
// copied into a replacement upsert operation.
//
// The ergonomics of this could be improved by one or both of:
// https://github.com/cockroachdb/replicator/issues/704
// https://github.com/cockroachdb/replicator/issues/705
api.configureTable("soft_deletes", {
    apply: async (ops: ApplyOp[]): Promise<any> => {
        ops = ops.map((op: ApplyOp) => {
            if (op.action == "delete") {
                op = {
                    ...op,
                    action: "upsert",
                    data: {
                        ...op.before,
                        is_deleted: 1,
                    }
                };
            }
            return op;
        })
        return api.getTX().apply(ops);
    }
})

// Demonstrate how upsert and delete SQL operations can be entirely
// overridden by the userscript. In this test, we perform some basic
// arithmetic on the keys and values to validate that this script is
// actually running.
api.configureTable("sql_test", {
    apply: async (ops: ApplyOp[]): Promise<void> => {
        let tx = api.getTX();

        // We can perform arbitrary queries against the database. This
        // API returns a Promise. We're in an async function and can
        // therefore use await to improve readability.
        let rows = tx.query(
            `SELECT *
             FROM (VALUES (1, 2), (3, 4))`);
        for (let row of await rows) {
            console.log("rows query", JSON.stringify(row));

            if (typeof row[0] !== "string") {
                throw new Error("numbers are presented as strings to prevent loss of precision");
            }
        }

        // Demonstrate date parsing.
        for (let row of await tx.query("SELECT now()")) {
            let d = new Date(row[0]);
            console.log("the database time is", row[0], "=>", d);
        }

        // Demonstrate INT8 can be queried.
        for (let row of await tx.query("SELECT (1<<60)::INT8")) {
            if (typeof row[0] !== "string") {
                throw new Error("numbers are presented as strings to prevent loss of precision");
            }
            if (row[0] !== "1152921504606846976") {
                throw new Error(`found ${row[0]}`);
            }
        }

        // This is a not-entirely-contrived example where one might
        // interact with a database that can't update, but can delete
        // and then insert.
        for (let op of ops) {
            await tx.exec(
                `DELETE
                 FROM ${tx.table()}
                 WHERE pk = $1`, 2 * +op.pk[0])
            if (op.action == "upsert") {
                await tx.exec(
                    `INSERT INTO ${tx.table()} (pk, val)
                     VALUES ($1, $2)`,
                    2 * +op.data.pk, -2 * +op.data.val);
            }
        }

        await countCurrentTable()
    },
});

// Another example of a query that uses its result.
async function countCurrentTable() {
    let tx = api.getTX();
    let rows = await tx.query(`SELECT count(*)
                               FROM ${tx.table()}`);
    for (let row of rows) {
        console.log(`there are now ${row[0]} rows in ${api.getTX().table()}`)
    }
}

api.setOptions({"hello": "world"});

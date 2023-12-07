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

import * as api from "cdc-sink@v1"; // Well-known module name
import externalData from "./data.txt"; // Can import additional files from disk.
import * as lib from "./lib"; // Verify additional code imports.
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
    // Compare-and-set operations.
    cas: ["cas0", "cas1"],
    // Drop old data.
    deadlines: {
        "dl0": "1h",
        "dl1": "1m"
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

// Demonstrate how upsert and delete SQL operations can be entirely
// overridden by the userscript. In this test, we perform some basic
// arithmetic on the keys and values to validate that this script is
// actually running.
api.configureTable("sql_test", {
    delete: (tx:api.TargetTX, keys: api.DocumentValue[][]):Promise<any> =>
        Promise.all(keys.map(key => tx.exec(
            `DELETE FROM ${tx.table()} WHERE pk = $1`, 2*+key[0]))),
    upsert: async (tx:api.TargetTX, docs:api.Document[]):Promise<any> => {
        // We can perform arbitrary queries against the database. This
        // API returns a Promise. We're in an async function and can
        // therefore use await to improve readability.
        let rows = tx.query(`SELECT * FROM (VALUES (1, 2), (3, 4))`);
        for (let row of await rows) {
            console.log("rows query", JSON.stringify(row));
        }
        return Promise.all(docs.map(doc =>
            tx.exec(`UPSERT INTO ${tx.table()} (pk, val) VALUES ($1, $2)`,
                2*+doc.pk, -2*+doc.val)));
    },
});

api.setOptions({"hello": "world"});

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
    // This shows how a counter-like value can be merged.
    merge: op => {
        console.log("merge", JSON.stringify(op));
        return {
            apply: {
                // Leading unary + coerces to numeric.
                val: +op.existing.val + +op.proposed.val - +op.before.val,
            }
        };
    },
});

api.configureTable("drop_all", {
    map: () => null
});

api.configureTable("merge_dlq_all", {
    merge: op => ({dlq: "dead"})
});

api.configureTable("merge_drop_all", {
    merge: op => ({drop: true})
});


api.setOptions({"hello": "world"});

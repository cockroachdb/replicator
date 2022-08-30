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

import * as api from "cdc-sink@v1"; // Well-known module name
import externalData from "./data.txt"; // Can import additional files from disk.
// import * as something from "https://some.cdn/package@1" is viable

api.configureSource("expander", {
    dispatch: (doc, meta) => {
        console.log(JSON.stringify(doc), JSON.stringify(meta));

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
        "expr1": "true"
    },
    // Place unmapped data into JSONB column.
    extras: "overflow_column",
    // Final document fixups, or return null to drop it.
    map: doc => {
        doc.msg = externalData.trim();
        doc.num = 42;
        console.log("Hello debug", JSON.stringify(doc));
        return doc;
    },
    // Allow column in target database to be ignored.
    ignore: {
        "ign0": true,
        "ign1": true,
        "ign2": false
    }
});

api.configureTable("drop_all", {
    map: () => null
});

api.setOptions({"hello": "world"});

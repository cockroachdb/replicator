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

import * as api from "cdc-sink@v1";

api.configureTable("votr_{{DEST}}.public.candidates", {
    cas: ["version"],
});

api.configureTable("votr_{{DEST}}.public.totals", {
    cas: ["candidate"], // Generate a conflict if a target row exists
    map: (doc: api.Document): api.Document => {

    },
    merge: api.standardMerge((op: api.MergeOperation): api.MergeResult => {
        console.log(JSON.stringify(op));
        op.unmerged.forEach((col: api.Column) => {
            switch (col) {
                case "total":
                    let delta = (+op.proposed["total"]) - (+op.before["total"]);
                    op.target["total"] = (+op.target["total"]) + delta;
                    break;
                default:
                    throw new Error("unexpected column name: " + col);
            }
        })
        return {apply: op.target};
    }),
})
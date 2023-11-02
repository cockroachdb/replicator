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

// These constants are set by the votr script loader.

const destination: number = DESTINATION_INDEX;
const votrDB: string = DESTINATION_DB;

// If the document has already passed through the destination, we don't
// want it to continue looping around. Since this decision can be made
// without loading the row in the destination, we can use the per-table
// map function, rather than filtering in the merge function.
const filterWhence = (doc: api.Document) => doc.src !== destination ? doc : null;

// vectorMerge is a helper function to admit an incoming mutation based
// on its vector clock versus the destination's knowledge of the clock.
const vectorMerge =
    (fallback?: api.MergeFunction): api.MergeFunction =>
        (op: api.MergeOperation): api.MergeResult => {
            // Replay prevention: Don't process messages from older
            // clock values in the src column.
            let newInfo = false;
            let learned = {};
            let whence = {};
            for (let key in op.proposed.whence) {
                let incomingClock: string = op.proposed.whence[key] ?? 0;
                let existingClock: string = op.target.whence[key] ?? 0;
                if (incomingClock > existingClock) {
                    newInfo = true;
                    whence[key] = incomingClock;
                    learned[key] = true;
                } else {
                    whence[key] = existingClock;
                }
            }
            for (let key in op.target.whence) {
                if (whence[key] === undefined) {
                    whence[key] = op.target.whence[key];
                }
            }
            console.log(JSON.stringify(learned), JSON.stringify(whence));
            if (!newInfo) {
                return {drop: true};
            }

            // Use the target's view of the vector clock, but update it
            // with the mutation source's value.
            op.proposed.whence = whence;

            // Absent a fallback, act like last-one-wins.
            if (!fallback) {
                return {apply: op.proposed};
            }

            // Delegate to specialty logic.
            return fallback(op);
        };


api.configureTable(`${votrDB}.public.ballots`, {
    cas: ["xyzzy"], // TODO: The CAS option needs an "always" setting
    map: filterWhence,
    merge: vectorMerge(),
});

api.configureTable(`${votrDB}.public.candidates`, {
    cas: ["xyzzy"],
    map: filterWhence,
    merge: vectorMerge(),
});

api.configureTable(`${votrDB}.public.totals`, {
    cas: ["xyzzy"],
    map: filterWhence,
    merge: vectorMerge((op: api.MergeOperation): api.MergeResult => {
        // Apply a delta based on before and proposed values.
        let a: number = op.proposed.total ?? 0;
        let b: number = op.before?.total ?? 0;
        let delta = a - b;
        if (delta === 0) {
            return {drop: true};
        }

        op.proposed.total = op.target.total + delta;
        return {apply: op.proposed};
    }),
})
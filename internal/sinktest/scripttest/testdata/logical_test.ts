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

import * as api from "replicator@v1";
import {Document, Table} from "replicator@v1";

function trimPartition(table: string): string {
    let idx = table.indexOf("_P_");
    if (idx != -1) {
        table = table.substring(0, idx)
    }
    return table;
}

// The sentinel name will be replaced by the test rig. It would normally be
// "my_db.public" or "my_db" depending on the target product.
api.configureSource("{{ SCHEMA }}", {
    dispatch: (doc: Document, meta: Document): Record<Table, Document[]> => {
        console.log(JSON.stringify(doc), JSON.stringify(meta));
        return {
            [trimPartition(meta.table)]: [{
                pk: doc.pk,
                ignored: 'by_configuration_below',
                v_dispatched: doc.v, // Rename the property
            }]
        };
    },
    deletesTo: (doc: Document, meta: Document): Record<Table, Document[]> => ({
        [trimPartition(meta.table)]: [doc],
    }),
})

// We introduce an unknown column in the dispatch function above.
// We'll add an extra configuration here to ignore it.
// The sentinel name would be replaced by "my_table".
let commonConfig = {
    map: (doc: Document): Document => {
        console.log("map", JSON.stringify(doc));
        if (doc.v_dispatched === undefined) {
            throw "did not find expected property";
        }
        doc.v_mapped = doc.v_dispatched;
        delete doc.v_dispatched; // Avoid schema-drift error due to unknown column.
        return doc;
    },
    ignore: {
        "ignored": true,
    }
};
api.configureTable("{{ TABLE }}", commonConfig);
api.configureTable("t1", commonConfig);
api.configureTable("t2", commonConfig);

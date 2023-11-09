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
import {Document, Table} from "cdc-sink@v1";

api.configureTable("{{ TABLE }}", {
  cas: ["deleted"],
  merge: (op: MergeOperation): MergeResult => {
    console.log("proposed " + op.proposed.j.substring(0,40))
    console.log("on db " + op.target.j.substring(0,40))
    console.log(op.proposed.j === "__cdc__toast__")
    if (op.proposed.j === "__cdc__toast__") {
      op.proposed.j = op.target.j
    }
    console.log("resolution " + op.proposed.j.substring(0,40))
    return { apply: op.proposed };
  },
});
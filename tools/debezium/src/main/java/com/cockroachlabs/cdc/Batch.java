// Copyright 2023 The Cockroach Authors
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
package com.cockroachlabs.cdc;

import java.io.IOException;
import java.io.Writer;

/**
 * Batch writes events to a io.Writer.
 * The output is a serialized JSON array that can be sent to
 * cdc-sink for processing.
 **/
public class Batch {
    private Writer out;
    private Boolean first = true;

    /**
     * Constructs a Batch object
     **/
    public Batch(Writer out) throws IOException {
        this.out = out;
        this.maybeWrite("[");
    }

    /**
     * Ready to write the the next JSON object in the batch.
     * 
     * @throws IOException
     */
    public void next() throws IOException {
        if (first) {
            first = false;
        } else {
            maybeWrite(",");
        }
    }

    /**
     * Writes the key.
     * 
     * @throws IOException
     */
    public void key(String key) throws IOException {
        maybeWrite("{\"key\": ");
        maybeWrite(key);
    }

    /**
     * Writes the value.
     * 
     * @throws IOException
     */
    public void value(String value) throws IOException {
        if (value != null) {
            maybeWrite(",\"value\": ");
            maybeWrite(value);
        }
        maybeWrite("}");
    }

    /**
     * Batch is done.
     * 
     * @throws IOException
     */
    public void end() throws IOException {
        maybeWrite("]\n");
    }

    /**
     * Writes a string to the writer.
     * 
     * @throws IOException
     */
    private void maybeWrite(String s) throws IOException {
        if (out != null) {
            out.write(s);
        }
    }

}

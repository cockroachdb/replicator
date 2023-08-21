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

// Package httpauth contains a common function for extracting
// credentials from an HTTP request.
package httpauth

import (
	"net/http"
	"strings"
)

// Token returns the bearer authorization header or the access_token
// HTTP parameter associated with the request. If an authorization query
// parameter is used, the request will be updated to delete that
// parameter. This function will return an empty string if no
// authorization token is associated with the request.
func Token(req *http.Request) string {
	var token string
	if raw := req.Header.Get("Authorization"); raw != "" {
		if strings.HasPrefix(raw, "Bearer ") {
			token = raw[7:]
		}
	} else if token = req.URL.Query().Get("access_token"); token != "" {
		// Delete the token query parameter to prevent logging later
		// on. This doesn't take care of issues with L7
		// loadbalancers, but this param is used by other
		// OAuth-style implementations and will hopefully be
		// discarded without logging.
		values := req.URL.Query()
		values.Del("access_token")
		req.URL.RawQuery = values.Encode()
	}
	return token
}

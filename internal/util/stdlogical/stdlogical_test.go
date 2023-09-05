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

package stdlogical

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestSmoke(t *testing.T) {
	r := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ready := make(chan struct{})

	cmd := New(&Template{
		Bind:    nil, // No extra CLI flags
		Metrics: "127.0.0.1:13013",
		Start: func(cmd *cobra.Command) (started any, cancel func(), err error) {
			return nil, nil, nil
		},
		Use: "test",
		testCallback: func() {
			close(ready)
		},
	})
	// Override os.Args.
	cmd.SetArgs([]string{})

	// Start the server in the background.
	go func() {
		r.NoError(cmd.ExecuteContext(ctx))
	}()

	select {
	case <-ctx.Done():
		r.Fail("timed out waiting for server")
	case <-ready:
	}

	resp, err := http.Get("http://127.0.0.1:13013/_/diag")
	r.NoError(err)
	r.Equal(http.StatusOK, resp.StatusCode)

	w := log.StandardLogger().Writer()
	count, err := io.Copy(w, resp.Body)
	r.NoError(err)
	r.NotZero(count)
	r.NoError(w.Close())

	cancel()
}

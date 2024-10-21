// Copyright 2024 The Cockroach Authors
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

package stdpool

import (
	"fmt"
	"net/url"

	"github.com/cockroachdb/field-eng-powertools/stopper"
	"github.com/cockroachdb/replicator/internal/types"
	"github.com/pkg/errors"
)

// ProvideBackup provides the version backup service
func ProvideBackup(memo types.Memo, stagingPool *types.StagingPool) *Backup {
	return &Backup{
		memo:        memo,
		stagingPool: stagingPool,
	}
}

// Backup backs up the version string for the target database
type Backup struct {
	memo        types.Memo
	stagingPool *types.StagingPool
}

// Store the value
func (b *Backup) Store(ctx *stopper.Context, connectString, ver string) error {
	key, err := b.targetVersionMemoKey(connectString)
	if err != nil {
		return err
	}
	return b.memo.Put(ctx, b.stagingPool, key, []byte(ver))
}

// Load the value
func (b *Backup) Load(ctx *stopper.Context, connectString string) (string, error) {
	key, err := b.targetVersionMemoKey(connectString)
	if err != nil {
		return "", err
	}
	bs, err := b.memo.Get(ctx, b.stagingPool, key)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

func (b *Backup) targetVersionMemoKey(connectString string) (string, error) {
	u, err := url.Parse(connectString)
	if err != nil {
		return "", errors.Wrap(err, "could not parse connection string")
	}
	key := fmt.Sprintf("%s://%s:%s/dbVersion", u.Scheme, u.Hostname(), u.Port())
	return key, nil
}

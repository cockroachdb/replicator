#!/bin/bash
# Copyright 2024 The Cockroach Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# SPDX-License-Identifier: Apache-2.0



echo "SET CLUSTER SETTING cluster.organization = '${COCKROACH_DEV_ORGANIZATION}'"
cockroach sql --url "postgresql://root@${TARGET}:26257/?sslmode=disable" --insecure --execute "SET CLUSTER SETTING enterprise.license = '${COCKROACH_DEV_LICENSE}';"
cockroach sql --url "postgresql://root@${TARGET}:26257/?sslmode=disable" --insecure --execute "SET CLUSTER SETTING cluster.organization = '${COCKROACH_DEV_ORGANIZATION}';"
cockroach sql --url "postgresql://root@${TARGET}:26257/?sslmode=disable" --insecure --execute 'SET CLUSTER SETTING kv.rangefeed.enabled = true;'

cockroach sql --insecure --url "postgresql://root@${TARGET}:26257/?sslmode=disable" --insecure --execute 'create database if not exists _replicator'
cockroach sql --insecure --url "postgresql://root@${TARGET}:26257/?sslmode=disable" --insecure --execute 'create database if not exists kv' 

# We are adding 2 columns to each table that we need to replicate.
# _source indicates where the change was originally made
# _timestamp is a timestamp in the future to take into account for a window of uncertainty,
# used to resolve conflicts. This assumes that conflicts are rare and it provides
# a sensible default behavior to resolve them.
cockroach sql --insecure --url "postgresql://root@${TARGET}:26257/?sslmode=disable" --insecure  <<!
CREATE TABLE IF NOT EXISTS kv.kv (
    k int8 primary key,
    v bytes not null,
    _source string  NOT VISIBLE default '${TARGET}' on update '${TARGET}',
    _timestamp timestamptz NOT VISIBLE DEFAULT now() + '1s' on update now() + '1s');
!

# Table to collect unresolved conflicts.
cockroach sql --insecure --url "postgresql://root@${TARGET}:26257/?sslmode=disable" --insecure  <<!
CREATE TABLE IF NOT EXISTS kv.replicator_dlq (
event UUID DEFAULT gen_random_uuid() PRIMARY KEY,
dlq_name TEXT NOT NULL,
source_nanos INT8 NOT NULL,
source_logical INT8 NOT NULL,
data_after JSONB NOT NULL,
data_before JSONB NOT NULL
);
!

echo "Creating change feed ${DIR}"
cockroach sql --insecure --url "postgresql://root@${TARGET}:26257/?sslmode=disable" --insecure --execute "CREATE CHANGEFEED FOR TABLE kv.kv INTO 'webhook-https://${DIR}:30004/kv/public?insecure_tls_skip_verify=true' WITH diff, updated, resolved='5s';"


echo "DONE ${TARGET}" > /tmp/done.txt

tail -f /dev/null

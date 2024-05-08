#!/bin/bash
# Copyright 2023 The Cockroach Authors
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


. ./crdb_env

cockroach sql --insecure --url 'postgresql://root@roach_target:26257/?sslmode=disable' --insecure --execute 'create database if not exists _replicator'

cockroach sql --url 'postgresql://root@roach_source:26257/?sslmode=disable' --insecure --execute "SET CLUSTER SETTING enterprise.license = '${COCKROACH_DEV_LICENSE}';"
cockroach sql --url 'postgresql://root@roach_source:26257/?sslmode=disable' --insecure --execute "SET CLUSTER SETTING cluster.organization = '${COCKROACH_DEV_ORGANIZATION}';"
cockroach sql --url 'postgresql://root@roach_source:26257/?sslmode=disable' --insecure --execute 'SET CLUSTER SETTING kv.rangefeed.enabled = true;'

cockroach workload init movr 'postgresql://root@roach_source:26257/?sslmode=disable'
cockroach sql --url 'postgresql://root@roach_source:26257/?sslmode=disable' --insecure --execute "backup database movr into 'nodelocal://1/backup/movr';"
cockroach sql --url 'postgresql://root@roach_target:26257/?sslmode=disable' --insecure --execute " restore database movr from latest in 'nodelocal://1/backup/movr';"


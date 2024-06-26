#!/usr/bin/env bash
#
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
#

#set -e

export SRC_DB=movr
export SRC_TABLES=promo_codes
export SRC_HTTP=8090
export SRC_PORT=26267
export SRC_HOST=127.0.0.1

export SINK_HTTP=8080
export SINK_SQL_PORT=26257
export SINK_PORT=26258
export SINK_HOST=127.0.0.1
export CDC_PORT=26259

echo ">>> Build Replicator..."
go build ../

echo ">>> Start Source with Movr..."
cockroach start-single-node --insecure --background --http-addr=:${SRC_HTTP} --listen-addr=:${SRC_PORT} --store=cockroach_source
cockroach sql --insecure --port=${SRC_PORT} -e="SET CLUSTER SETTING kv.rangefeed.enabled = true; SET CLUSTER SETTING enterprise.license = \"${COCKROACH_DEV_LICENSE}\"; SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing';"
cockroach workload init movr "postgresql://root@${SRC_HOST}:${SRC_PORT}/movr?sslmode=disable"

echo ">>> Start Sink..."
cockroach start-single-node --insecure --background --http-addr=:${SINK_HTTP} --listen-addr=:${SINK_PORT} --sql-addr=:${SINK_SQL_PORT} --store=cockroach_sink
cockroach workload init movr --num-histories=9 --num-promo-codes=9 --num-rides=9 --num-users=9 --num-vehicles=9 "postgresql://root@$SINK_HOST:$SINK_SQL_PORT/movr?sslmode=disable"
cockroach sql --insecure --port=$SINK_SQL_PORT -e="TRUNCATE TABLE MOVR.RIDES CASCADE; TRUNCATE TABLE MOVR.USERS CASCADE; TRUNCATE MOVR.VEHICLES CASCADE; TRUNCATE MOVR.VEHICLE_LOCATION_HISTORIES CASCADE; TRUNCATE TABLE MOVR.PROMO_CODES; TRUNCATE TABLE MOVR.USER_PROMO_CODES;"

###TODO(Chris): Creat logic to create config for multiple tables
echo ">>> Create Replicator"
config="$(echo [{\"endpoint\":\"${SRC_TABLES}.sql\", \"source_table\":\"${SRC_TABLES}\", \"destination_database\":\"${SRC_DB}\", \"destination_table\":\"${SRC_TABLES}\"}])"
replicator --conn=postgresql://root@${SINK_HOST}:${SINK_SQL_PORT}/${SRC_DB}?sslmode=disable --port=${CDC_PORT} --config="$config" > replicator.log 2>&1 &

echo ">>> Create Changefeeds on Source..."
cockroach sql --insecure --port=${SRC_PORT} -e="CREATE CHANGEFEED FOR TABLE ${SRC_DB}.${SRC_TABLES} INTO \"experimental-http://${SINK_HOST}:${CDC_PORT}/promo_codes.sql\" WITH updated,resolved;"

open "http://${SRC_HOST}:${SRC_HTTP}/#/metrics/overview/cluster"
open "http://${SINK_HOST}:${SINK_HTTP}/#/metrics/overview/cluster"

cockroach sql --insecure --port=${SRC_PORT} -e="select count(*) as SourceRecords from ${SRC_DB}.${SRC_TABLES};"
cockroach sql --insecure --port=${SINK_SQL_PORT} -e="select count(*) as TargetRecords from ${SRC_DB}.${SRC_TABLES};"

echo ">>> Start Movr Workload..."
cockroach workload run movr --duration=1m --display-every=10s --concurrency=1 "postgresql://root@${SRC_HOST}:${SRC_PORT}/movr?sslmode=disable" > workload.log 2>&1 &

cockroach sql --insecure --port=$SINK_SQL_PORT --watch=10s -e="select count(*) as TargetRecords from ${SRC_DB}.${SRC_TABLES};"

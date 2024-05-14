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


#export COCKROACH_DEV_LICENSE=""
#export COCKROACH_DEV_ORGANIZATION=""

if [ -z "COCKROACH_DEV_LICENSE" ] || [ -z "COCKROACH_DEV_ORGANIZATION" ]
then
     echo "The COCKROACH_DEV_LICENSE and COCKROACH_DEV_ORGANIZATION env variables must be set to run CRDB Changefeeds needed for replicator"
     exit
fi

echo "export COCKROACH_DEV_LICENSE='"$COCKROACH_DEV_LICENSE"'" > scripts/crdb_env
echo "export COCKROACH_DEV_ORGANIZATION='"$COCKROACH_DEV_ORGANIZATION"'" >> scripts/crdb_env

docker-compose up --detach

echo "Resetting Grafana Admin Password...."
echo ""
export cid=`docker ps |grep grafana- |awk '{printf("%s",$1)}'`

echo "docker exec -it ${cid} /usr/share/grafana/bin/grafana cli --homepath /usr/share/grafana admin reset-admin-password replicator"
docker exec -it ${cid} /usr/share/grafana/bin/grafana cli --homepath "/usr/share/grafana" admin reset-admin-password replicator
RESULT=$?
if [ $RESULT -eq 0 ]; then
  echo "Grafana login is:  admin/replicator"
else
  echo "Grafana password was not reset. Must do it manually... (admin/admin) is the initial password"
fi

echo "Grafana URL: http://localhost:3000/login"

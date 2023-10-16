#!/bin/bash

#export COCKROACH_DEV_LICENSE=""
#export COCKROACH_DEV_ORGANIZATION=""

if [ -z "COCKROACH_DEV_LICENSE" ] || [ -z "COCKROACH_DEV_ORGANIZATION" ]
then
     echo "The COCKROACH_DEV_LICENSE and COCKROACH_DEV_ORGANIZATION env variables must be set to run CRDB Changefeeds needed for cdc-sink"
     exit
fi

docker-compose up --detach 

echo "Resetting Grafana Admin Password...."
echo ""
export cid=`docker ps |grep grafana- |awk '{printf("%s",$1)}'`

echo "docker exec -it ${cid} /usr/share/grafana/bin/grafana cli --homepath /usr/share/grafana admin reset-admin-password cdc-sink"
docker exec -it ${cid} /usr/share/grafana/bin/grafana cli --homepath "/usr/share/grafana" admin reset-admin-password cdc-sink
RESULT=$?
if [ $RESULT -eq 0 ]; then
  echo "Grafana login is:  admin/cdc-sink"
else
  echo "Grafana password was not reset. Must do it manually... (admin/admin) is the initial password"
fi

echo "Grafana URL: http://localhost:3000/login"

#!/bin/bash

#export LICENSE=""
#export ORGANIZATION=""

if [ -z "$LICENSE" ] || [ -z "$ORGANIZATION" ]
then
     echo "The LICENSE and ORGANIZTION env variables must be set to run CRDB Changefeeds needed for cdc-sink"
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

#!/bin/bash

cockroach sql --insecure --url 'postgresql://root@roach_target:26257/?sslmode=disable' --insecure --execute 'create database if not exists _cdc_sink' 
cockroach sql --url 'postgresql://root@roach_source:26257/?sslmode=disable' --insecure --execute "SET CLUSTER SETTING enterprise.license = '${COCKROACH_DEV_LICENSE}';"
cockroach sql --url 'postgresql://root@roach_source:26257/?sslmode=disable' --insecure --execute "SET CLUSTER SETTING cluster.organization = '${COCKROACH_DEV_ORGANIZATION}';"
cockroach sql --url 'postgresql://root@roach_source:26257/?sslmode=disable' --insecure --execute 'SET CLUSTER SETTING kv.rangefeed.enabled = true;'
cockroach workload init movr 'postgresql://root@roach_source:26257/?sslmode=disable'

cockroach sql --url 'postgresql://root@roach_source:26257/?sslmode=disable' --insecure --execute "backup database movr into 'nodelocal://1/backup/movr';"
cockroach sql --url 'postgresql://root@roach_target:26257/?sslmode=disable' --insecure --execute " restore database movr from latest in 'nodelocal://1/backup/movr';"

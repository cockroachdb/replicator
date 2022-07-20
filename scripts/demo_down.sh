#!/usr/bin/env bash

## CLEAN UP - Remove and put into demo_cleanup.sh
pkill -9 cockroach cdc-sink
rm -Rf ./cockroach_sink
rm -Rf ./cockroach_source

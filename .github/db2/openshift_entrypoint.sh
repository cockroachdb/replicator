#!/bin/bash

# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
# https://github.com/debezium/debezium-examples/tree/main/tutorial/debezium-db2-init/db2server

################################################################################
#  Runs custom init scripts followed by the original entry point
###############################################################################

source ${SETUPDIR?}/include/db2_constants
source ${SETUPDIR?}/include/db2_common_functions

if [[ -d /var/custom-init ]]; then
    echo "(*) Running user-provided init scripts ... "
    chmod  -R 777 /var/custom-init
    for script in `ls /var/custom-init`; do
       echo "(*) Running $script ..."
       /var/custom-init/$script
    done
fi

echo "Running original entry point"
/var/db2_setup/lib/setup_db2_instance.sh

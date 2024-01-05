#!/bin/bash


# Copyright Debezium Authors. Licensed under the Apache License, Version 2.0.
# https://github.com/debezium/debezium-examples/tree/main/tutorial/debezium-db2-init/db2server


################################################################################
# Wipes the storage directory.
# Note that this essentially makes the database non-persistent when pod is deleted
################################################################################

echo "Inspecting database directory"
ls  $STORAGE_DIR

echo "Wiping database directory"
rm -rf $STORAGE_DIR/*

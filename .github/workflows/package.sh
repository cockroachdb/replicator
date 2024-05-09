#!/usr/bin/env bash
#
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
#

# This shell script is intended to be executed from the root of the
# repository. It will create an output folder to be uploaded to a
# storage bucket. This exists outside of the workflow file to aid
# in local testing.

set -ex

# Variables that may be set by the workflow.
PACKAGE_NAME=${PACKAGE_NAME:-replicator-local-dev} # replicator-linux-amd64[-target]
PACKAGE_BIN_NAME=${PACKAGE_BIN_NAME:-replicator}   # replicator(.exe)
UPLOAD_DIR=${UPLOAD_DIR:-upload}   # upload/
EDGE_DIR="$UPLOAD_DIR/edge"

# Temp dir.
WORK_DIR=$(mktemp -d)

# Extract names from git checkout.
SHORT_VERSION=$(git rev-parse --short HEAD)                                 # Short SHA
TAG_VERSION=$(git describe --tags --exact-match HEAD 2> /dev/null || true ) # v1.0.1
BRANCH_NAME=$( (git symbolic-ref -q --short HEAD | tr / -) || true )        # Replace slashes in branch name

# Bundle license files
go generate ./internal/cmd/licenses

# Build main binary
# We want the build flags to fully expand
# shellcheck disable=SC2086
CGO_ENABLED=$PACKAGE_CGO_ENABLED \
GOOS=$PACKAGE_GOOS \
GOARCH=$PACKAGE_GOARCH \
go build -v -ldflags="-s -w" $PACKAGE_BUILD_FLAGS -o "$WORK_DIR/$PACKAGE_BIN_NAME" .

echo "$SHORT_VERSION" > "$WORK_DIR/VERSION.txt"

# Package into a temporary file. We use the transform option to add an
# extra path element to the files within the archive.
tar zcvf "$WORK_DIR/package.tgz" --transform "s|^.*/|./$PACKAGE_NAME/|" "$WORK_DIR/$PACKAGE_BIN_NAME" ./README.md "$WORK_DIR/VERSION.txt"

mkdir -p "$EDGE_DIR/sha" "$EDGE_DIR/branch"
cp "$WORK_DIR/package.tgz" "$EDGE_DIR/sha/$PACKAGE_NAME-$SHORT_VERSION.tgz"
if [ -n "$BRANCH_NAME" ]; then
  cp "$WORK_DIR/package.tgz" "$EDGE_DIR/branch/$PACKAGE_NAME-$BRANCH_NAME.tgz"
fi
TAG_REGEX="^v[0-9]+\.[0-9]+\.[0-9]+$"
if [[ "$TAG_VERSION" =~ $TAG_REGEX ]]; then
  cp "$WORK_DIR/package.tgz" "$UPLOAD_DIR/$PACKAGE_NAME-$TAG_VERSION.tgz"
fi

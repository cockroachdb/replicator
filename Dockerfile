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

# This Dockerfile is based on the example in
# https://www.docker.com/blog/faster-multi-platform-builds-dockerfile-cross-compilation-guide/
#
# The go compiler will run using the host architecture, but
# cross-compile to the desired target platform given to buildx.
FROM --platform=$BUILDPLATFORM golang:1.23 AS builder
WORKDIR /tmp/compile
ARG TARGETOS TARGETARCH
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=0 go build -v -ldflags="-s -w" -o /usr/bin/replicator .

# This is a cgo build, which make cross-compilation non-trivial. The
# only use case that we need to support at the moment is linux/amd64.
FROM --platform=$TARGETPLATFORM golang:1.23 AS builder-cgo
WORKDIR /tmp/compile
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/var/cache/apt \
    apt-get update && apt-get install -yqq --no-install-recommends unzip
RUN curl -o ora-libs.zip https://replicator.cockroachdb.com/third_party/instantclient-basiclite-$TARGETOS-$TARGETARCH.zip \
    && unzip ora-libs.zip -d ora-libs \
    && rm -rf ora-libs/instantclient_*/*.jar ora-libs/instantclient_*/META-INF # Don't need java support
RUN --mount=target=. \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=1 go build -v -ldflags="-s -w" -tags target_oracle -o /usr/bin/replicator .

# Create an image that supports the Oracle Database targets.
FROM alpine:latest AS oracle
ENTRYPOINT ["/usr/bin/replicator"]
RUN apk --no-cache add gcompat libaio
COPY --from=builder-cgo /tmp/compile/ora-libs/instantclient_*/ /usr/lib
COPY --from=builder-cgo /usr/bin/replicator /usr/bin/

# Create a single-binary docker image, including a set of core CA
# certificates so that we can call out to any external APIs.
FROM scratch
ENTRYPOINT ["/usr/bin/replicator"]
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/bin/replicator /usr/bin/

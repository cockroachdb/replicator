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

# Build from a Node base, add the required JDK, then install.
# https://firebase.google.com/docs/emulator-suite/install_and_configure
FROM node:alpine
RUN apk add openjdk11
RUN npm install -g firebase-tools
# Pre-cache the firestore emulator.
RUN firebase setup:emulators:firestore
COPY emulators.json .
CMD ["firebase", "emulators:start", "--only", "firestore", "-c", "emulators.json"]

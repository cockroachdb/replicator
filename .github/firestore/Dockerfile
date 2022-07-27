# Build from a Node base, add the required JDK, then install.
# https://firebase.google.com/docs/emulator-suite/install_and_configure
FROM node:alpine
LABEL org.opencontainers.image.description="Firestore emulator for testing logical replication"
RUN apk add openjdk11
RUN npm install -g firebase-tools
# Pre-cache the firestore emulator.
RUN firebase setup:emulators:firestore
COPY emulators.json .
CMD ["firebase", "emulators:start", "--only", "firestore", "-c", "emulators.json"]
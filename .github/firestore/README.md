# Firestore emulator container

This directory builds a container which runs the Google Cloud Firestore emulator. We save time in
each of the tests by pre-caching the jar files which the firebase cli will download. We also
configure the emulator to bind to all addresses instead of loopback.

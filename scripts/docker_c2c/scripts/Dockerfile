FROM cockroachdb/cockroach:latest-v23.1
WORKDIR .
COPY setup.sh setup.sh
COPY crdb_env crdb_env
ENTRYPOINT setup.sh

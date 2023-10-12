FROM cockroachdb/cockroach:latest-v23.1
WORKDIR .
ENV COCKROACH_DEV_LICENSE="crl-0-EJL04ukFGAEiI0NvY2tyb2FjaCBMYWJzIC0gUHJvZHVjdGlvbiBUZXN0aW5n"
ENV COCKROACH_DEV_ORGANIZATION="Cockroach Labs - Production Testing"
COPY setup.sh setup.sh
ENTRYPOINT setup.sh

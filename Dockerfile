FROM golang:1.16 AS builder
WORKDIR /tmp/compile
COPY . .
RUN CGO_ENABLED=0 go build -v -ldflags="-s -w -X main.buildVersion=$(git describe --tags --always --dirty)" -o /usr/bin/cdc-sink .

# Create a single-binary docker image, including a set of core CA
# certificates so that we can call out to any external APIs.
FROM scratch
WORKDIR /data/
ENTRYPOINT ["/usr/bin/cdc-sink"]
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/bin/cdc-sink /usr/bin/

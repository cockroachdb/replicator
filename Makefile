
PWD = $(shell pwd)
COVER_OUT ?= cover.out
IBM_DRIVER = ${PWD}/drivers/clidriver
export CGO_CFLAGS=-I${IBM_DRIVER}/include
export CGO_LDFLAGS=-L${IBM_DRIVER}/lib
export DYLD_LIBRARY_PATH=${IBM_DRIVER}/lib
export LD_LIBRARY_PATH=${IBM_DRIVER}/lib

.PHONY: all clidriver db2 clean realclean testdb2

all: cdc-sink

cdc-sink:
	go build

clidriver: 
	go run . db2install --dest drivers

db2: clidriver
	go build -tags db2

clean:
	rm cdc-sink

realclean: clean
	rm -rf drivers

testdb2: clidriver
	go test -tags db2 -count 1 -coverpkg=./internal/source/db2  \
          -covermode=atomic \
          -coverprofile=${COVER_OUT} \
          -race \
          -v ./internal/source/db2

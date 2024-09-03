#!/bin/bash

# Ensure copyright in new files
go run github.com/google/addlicense -c "The Cockroach Authors" -l apache -s -v -check -ignore '**/testdata/**/*.sql'  -ignore '**/thirdparty/**' -ignore '*.ddt' -ignore '*.md' -ignore '.idea/*' .

# Standardize formatting
go run github.com/cockroachdb/crlfmt -w -ignore '_gen.go|plsql_parser.go' .

# Lints
go run golang.org/x/lint/golint -set_exit_status $(go list ./... | grep -v "/thirdparty" | grep -v "/oracleparser")
go run honnef.co/go/tools/cmd/staticcheck -checks all $(go list ./... | grep -v "/thirdparty" | grep -v "/oracleparser")

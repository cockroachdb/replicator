// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package server contains a generic HTTP server that installs
// the CDC listener.
package server

// This file contains code repackaged from main.go

import (
	_ "net/http/pprof" // Register pprof debugging endpoints.

	"github.com/spf13/pflag"
)

// Config contains the user-visible configuration for running a CDC
// changefeed server.
type Config struct {
	BindAddr           string
	ConnectionString   string
	DisableAuth        bool
	GenerateSelfSigned bool
	Immediate          bool
	StagingDB          string
	TLSCertFile        string
	TLSPrivateKey      string
}

// Bind registers flags.
func (c *Config) Bind(flags *pflag.FlagSet) {
	flags.StringVar(
		&c.BindAddr,
		"bindAddr",
		":26258",
		"the network address to bind to")
	flags.StringVar(
		&c.ConnectionString,
		"conn",
		"postgresql://root@localhost:26257/?sslmode=disable",
		"cockroach connection string")
	flags.BoolVar(
		&c.DisableAuth,
		"disableAuthentication",
		false,
		"disable authentication of incoming cdc-sink requests; not recommended for production.")
	flags.BoolVar(
		&c.Immediate,
		"immediate",
		false,
		"apply mutations immediately without preserving transaction boundaries")
	flags.StringVar(
		&c.StagingDB,
		"stagingDB",
		"_cdc_sink",
		"a sql database name to store metadata information in")
	flags.BoolVar(
		&c.GenerateSelfSigned,
		"tlsSelfSigned",
		false,
		"if true, generate a self-signed TLS certificate valid for 'localhost'")
	flags.StringVar(
		&c.TLSCertFile,
		"tlsCertificate",
		"",
		"a path to a PEM-encoded TLS certificate chain")
	flags.StringVar(
		&c.TLSPrivateKey,
		"tlsPrivateKey",
		"",
		"a path to a PEM-encoded TLS private key")
}

// A Server receives incoming CockroachDB changefeed messages and
// applies them to a target cluster.
type Server struct {
	stopped chan struct{}
}

// Stopped returns a channel that will be closed when the server has
// been shut down.
func (s *Server) Stopped() <-chan struct{} {
	return s.stopped
}

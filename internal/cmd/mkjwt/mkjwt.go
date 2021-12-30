// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package mkjwt contains a command to generate a signed JWT token from
// a user-provided private key.
package mkjwt

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	jwtAuth "github.com/cockroachdb/cdc-sink/internal/target/auth/jwt"
	"github.com/cockroachdb/cdc-sink/internal/util/ident"
	"github.com/golang-jwt/jwt/v4"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// Command reads a PEM-encoded private key to produce a JWT token
// that grants access to one or more target schemas.
func Command() *cobra.Command {
	var allow []string
	var claimOnly bool
	var out, pkPath string
	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Short: "generate JWT tokens from user-provided keys",
		Use:   "make-jwt -key pk.pem -a database.schema -a other_database.schema ...",
		Example: strings.TrimSpace(fmt.Sprintf(`
# Generate a EC private key using OpenSSL.
openssl ecparam -out ec.key -genkey -name prime256v1

# Write the public key components to a separate file.
openssl ec -in ec.key -pubout -out ec.pub

# Generate a token which can write to the ycsb.public schema.
# The key can be decoded using the debugger at https://jwt.io
cdc-sink make-jwt -k ec.key -a ycsb.public -o out.jwt

# Upload the public key for cdc-sink to find it.
cockroach sql -e "INSERT INTO %s.%s (public_key) VALUES ('$(cat ec.pub)')"

# Reload configuration, or wait a minute.
killall -HUP cdc-sink
`, ident.StagingDB.Raw(), jwtAuth.PublicKeysTable.Raw())),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(allow) == 0 {
				return errors.New("no schemas specified")
			}

			schemas := make([]ident.Schema, len(allow))
			for idx, input := range allow {
				parts := strings.Split(input, ".")
				if len(parts) != 2 {
					return errors.Errorf("could not interpret %q as schema name", input)
				}
				schemas[idx] = ident.NewSchema(ident.New(parts[0]), ident.New(parts[1]))
			}

			if claimOnly {
				cl, err := jwtAuth.NewClaim(schemas)
				if err != nil {
					return err
				}
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				return enc.Encode(cl)
			}

			if pkPath == "" {
				return errors.New("no private key specified")
			}

			key, err := readKey(pkPath)
			if err != nil {
				return err
			}

			method, err := signingMethod(key)
			if err != nil {
				return err
			}

			_, token, err := jwtAuth.Sign(method, key, schemas)
			if err != nil {
				return err
			}

			if out == "" {
				fmt.Println(token)
			} else if err := ioutil.WriteFile(out, []byte(token), 0600); err != nil {
				return err
			}
			return nil
		},
	}

	f := cmd.Flags()
	f.StringSliceVarP(&allow, "allow", "a", nil,
		"one or more 'database.schema' identifiers")
	f.BoolVar(&claimOnly, "claim", false, "if true, print a minimal JWT claim, instead of signing")
	f.StringVarP(&out, "out", "o", "", "a file to write the token to")
	f.StringVarP(&pkPath, "key", "k", "",
		"the path to a PEM-encoded private key to sign the token with")
	return cmd
}

// readKey returns the first private key seen in a PEM-formatted file.
func readKey(path string) (crypto.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	for {
		var block *pem.Block
		// Decode doesn't return an error, just the rest of the bytes.
		block, data = pem.Decode(data)
		if block == nil {
			return nil, errors.New("did find find expected PEM data")
		}

		switch block.Type {
		case "PRIVATE KEY":
			return x509.ParsePKCS8PrivateKey(block.Bytes)
		case "EC PRIVATE KEY":
			return x509.ParseECPrivateKey(block.Bytes)
		case "RSA PRIVATE KEY":
			return x509.ParsePKCS1PrivateKey(block.Bytes)
		}
	}
}

// https://datatracker.ietf.org/doc/html/rfc7518#section-3.1
func signingMethod(key crypto.PrivateKey) (jwt.SigningMethod, error) {
	switch t := key.(type) {
	case *ecdsa.PrivateKey:
		switch t.Curve {
		case elliptic.P256():
			return jwt.SigningMethodES256, nil
		case elliptic.P384():
			return jwt.SigningMethodES384, nil
		case elliptic.P521():
			return jwt.SigningMethodES512, nil
		default:
			return nil, errors.Errorf("unexpected elliptic curve %s", t.Curve.Params().Name)
		}
	case *rsa.PrivateKey:
		// The hash size is generally independent of the key size.
		return jwt.SigningMethodRS256, nil
	default:
		return nil, errors.Errorf("unsupported key type %t", key)
	}
}

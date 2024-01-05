// Copyright 2024 The Cockroach Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package db2

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/codeclysm/extract/v3"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

const (
	defaultURL  = "https://public.dhe.ibm.com/ibmdl/export/pub/software/data/db2/drivers/odbc_cli/"
	defaultDest = "drivers"
)

// Install downloads and installs the IBM DB2 ODBC driver.
// Because of license restrictions, the driver cannot be under
// source control. It needs to be downloaded when used.
func Install() *cobra.Command {
	var downloadURL, dest string
	cmd := &cobra.Command{
		Args:  cobra.NoArgs,
		Short: "installs the db2 driver",
		Use:   "db2install",
		RunE: func(cmd *cobra.Command, args []string) error {
			driverPath := path.Join(dest, "clidriver")
			_, err := os.Stat(driverPath)
			_, includeErr := os.Stat(path.Join(driverPath, "include"))
			_, libErr := os.Stat(path.Join(driverPath, "lib"))
			if os.IsNotExist(err) || os.IsNotExist(includeErr) || os.IsNotExist(libErr) {
				pkgName, err := install(cmd.Context(), dest, downloadURL)
				if err != nil {
					return err
				}
				fmt.Printf("Installed %s into %s.\n", pkgName, dest)
				return nil
			}
			if err != nil {
				return err
			}
			if includeErr != nil {
				return includeErr
			}
			if libErr != nil {
				return libErr
			}
			fmt.Println("Driver already installed.")
			return nil
		},
	}
	cmd.Flags().StringVar(&dest, "dest", defaultDest,
		"destination dir")
	cmd.Flags().StringVar(&downloadURL, "url", defaultURL,
		"url to download the driver")
	return cmd
}

// download a package to specified temp destination.
func download(tmpFile, baseURL, pkg string) (downloadError error) {
	out, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := out.Close(); downloadError != nil && err != nil {
			downloadError = err
		}
	}()
	pkgURL, err := url.JoinPath(baseURL, pkg)
	if err != nil {
		return err
	}
	resp, err := http.Get(pkgURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, downloadError = io.Copy(out, resp.Body)
	return
}

// install the IBM DB2 driver for the runtime platform into
// the specified directory
func install(ctx context.Context, target, baseURL string) (string, error) {
	var pkgName string
	const wordsize = 32 << (^uint(0) >> 32 & 1)
	switch runtime.GOOS {
	case "darwin":
		pkgName = "macos64_odbc_cli.tar.gz"
	case "windows":
		pkgName = fmt.Sprintf("ntx%d_odbc_cli.zip", wordsize)
	case "linux":
		switch runtime.GOARCH {
		case "amd64":
			pkgName = "linuxx64_odbc_cli.tar.gz"
		default:
			return "", errors.Errorf("unknown arch %q", runtime.GOARCH)
		}
	default:
		return "", errors.Errorf("unknown OS %q", runtime.GOOS)
	}
	pkg := filepath.Join(target, pkgName)
	_, err := os.Stat(pkg)
	if os.IsNotExist(err) {
		fmt.Printf("Installing %s%s in %s.\n", baseURL, pkgName, target)
		tmpFile := path.Join(os.TempDir(), pkgName)
		defer os.Remove(tmpFile)
		err := download(tmpFile, baseURL, pkgName)
		if err != nil {
			return "", err
		}
		fmt.Println("Download successful.")
		pkg = tmpFile
	} else {
		fmt.Printf("Found %s in %s\n", pkgName, target)
	}
	f, err := os.Open(pkg)
	if err != nil {
		return "", err
	}
	if strings.HasSuffix(pkg, ".zip") {
		return pkgName, extract.Zip(ctx, f, target, nil)
	}
	if strings.HasSuffix(pkg, ".gz") {
		return pkgName, extract.Gz(ctx, f, target, nil)
	}
	return "", errors.Errorf("unknown file type %s", pkgName)
}

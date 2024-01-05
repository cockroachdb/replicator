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
	"archive/tar"
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"

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
				return install(dest, downloadURL)
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
			fmt.Println("Driver already installed")
			return nil
		},
	}
	cmd.Flags().StringVar(&dest, "dest", defaultDest,
		"destination dir")
	cmd.Flags().StringVar(&downloadURL, "url", defaultURL,
		"url to download the driver")
	return cmd
}

// copy creates a file and copies the content from the source reader.
func copy(path string, src io.Reader, mode fs.FileMode) error {
	writer, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, mode)
	if err != nil {
		return errors.Wrapf(err, "failed to extract file")
	}
	defer writer.Close()
	if _, err = io.Copy(writer, src); err != nil {
		return errors.Wrapf(err, "failed to extract file")
	}
	return nil
}

// download a package to specified temp destination.
func download(tmpFile, baseURL, pkg string) error {
	out, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer out.Close()
	pkgURL, err := url.JoinPath(baseURL, pkg)
	if err != nil {
		return err
	}
	resp, err := http.Get(pkgURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return err
	}
	return nil
}

// extractTar extracts the content of the tar file into the
// target directory
func extractTar(sourcefile string, targetDirectory string) error {
	stream, err := os.Open(sourcefile)
	if err != nil {
		return err
	}
	defer stream.Close()
	uncompressedStream, err := gzip.NewReader(stream)
	if err != nil {
		return err
	}
	defer uncompressedStream.Close()
	tarReader := tar.NewReader(uncompressedStream)
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.Wrapf(err, "failed to extract file")
		}
		target := path.Join(targetDirectory, strings.TrimPrefix(header.Name, "/"))
		fmt.Println(target)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0755); err != nil {
				return errors.Wrapf(err, "failed to extract file")
			}
		case tar.TypeReg:
			err := copy(target, tarReader, header.FileInfo().Mode())
			if err != nil {
				return errors.Wrapf(err, "failed to extract file")
			}
		case tar.TypeSymlink:
			err := os.Symlink(header.Linkname, target)
			if err != nil {
				return errors.Wrapf(err, "failed to extract file")
			}
		default:
			return errors.Errorf(
				"uknown file type: %s in %s",
				string(header.Typeflag),
				header.Name)
		}
	}
}

// extractZip extracts the content of the zip file into the
// target directory
func extractZip(sourcefile string, targetDirectory string) error {
	reader, err := zip.OpenReader(sourcefile)
	if err != nil {
		return errors.Wrapf(err, "failed to extract file")
	}
	defer reader.Close()
	for _, f := range reader.Reader.File {
		zipped, err := f.Open()
		if err != nil {
			return errors.Wrapf(err, "failed to extract file")
		}
		defer zipped.Close()
		path := filepath.Join(targetDirectory, f.Name)
		if f.FileInfo().IsDir() {
			err := os.MkdirAll(path, f.Mode())
			if err != nil {
				return errors.Wrapf(err, "failed to extract file")
			}
		} else {
			err := copy(path, zipped, f.Mode())
			if err != nil {
				return errors.Wrapf(err, "failed to extract file")
			}
		}
	}
	return nil
}

// install the IBM DB2 driver for the runtime platform into
// the specified directory
func install(target, baseURL string) error {
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
			return errors.Errorf("unknown arch %q", runtime.GOARCH)
		}
	default:
		return errors.Errorf("unknown OS %q", runtime.GOOS)
	}
	fmt.Printf("Installing %s in %s\n", pkgName, target)
	tmpFile := path.Join(os.TempDir(), pkgName)
	defer os.Remove(tmpFile)
	err := download(tmpFile, baseURL, pkgName)
	if err != nil {
		return err
	}
	fmt.Println("download successful")
	if strings.HasSuffix(pkgName, ".zip") {
		return extractZip(tmpFile, target)
	}
	return extractTar(tmpFile, target)
}

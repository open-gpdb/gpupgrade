// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/blang/semver/v4"
	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/testutils/exectest"
)

var versionCommand = exec.Command

// XXX: for internal testing only
func SetVersionCommand(command exectest.Command) {
	versionCommand = command
}

// XXX: for internal testing only
func ResetVersionCommand() {
	versionCommand = exec.Command
}

var versionPattern = regexp.MustCompile(`\d+\.\d+\.\d+`)

// VersionWithProduct returns both the product (Greenplum or Cloudberry) and its
// version, parsed from the `postgres --gp-version` banner. Detecting the product
// here is the authoritative way to tell Greenplum from Cloudberry; callers must
// not infer it from the version number (see Product).
func VersionWithProduct(gphome string) (Product, semver.Version, error) {
	cmd := versionCommand(filepath.Join(gphome, "bin", "postgres"), "--gp-version")
	cmd.Env = []string{}

	log.Printf("Executing: %q", cmd.String())
	output, err := cmd.CombinedOutput()
	if err != nil {
		return ProductUnknown, semver.Version{}, fmt.Errorf("%q failed with %q: %w", cmd.String(), string(output), err)
	}

	return parseGPVersion(string(output))
}

func parseGPVersion(rawVersion string) (Product, semver.Version, error) {
	trimmed := strings.TrimSpace(rawVersion)

	var product Product
	switch {
	case strings.Contains(trimmed, "(Greenplum Database)"):
		product = ProductGreenplum
	case strings.Contains(trimmed, "(Apache Cloudberry)"):
		product = ProductCloudberry
	default:
		return ProductUnknown, semver.Version{}, xerrors.Errorf(`version %q is not of the form "postgres (Greenplum Database|Apache Cloudberry) #.#.#"`, rawVersion)
	}

	matches := versionPattern.FindStringSubmatch(trimmed)
	if len(matches) < 1 {
		return ProductUnknown, semver.Version{}, xerrors.Errorf("no version number found in %q", rawVersion)
	}

	version, err := semver.Parse(matches[0])
	if err != nil {
		return ProductUnknown, semver.Version{}, xerrors.Errorf("parsing version %q: %w", rawVersion, err)
	}

	return product, version, nil
}

// Version returns only the version number. Prefer VersionWithProduct when the
// product identity matters.
func Version(gphome string) (semver.Version, error) {
	_, version, err := VersionWithProduct(gphome)
	return version, err
}

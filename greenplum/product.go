// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

// Product identifies the database product behind a GPHome, independent of its
// version number.
//
// The product's marketing major version is NOT a reliable proxy for the
// product or for the underlying PostgreSQL release: Apache Cloudberry versions
// its releases on a track separate from Greenplum (Cloudberry 2 is based on
// PostgreSQL 14, Cloudberry 3 on PostgreSQL 16+), and its major number may
// collide with or eventually exceed Greenplum's. Detect the product from the
// `postgres --gp-version` banner, never from a version-number range.
type Product int

const (
	ProductUnknown Product = iota
	ProductGreenplum
	ProductCloudberry
)

func (p Product) String() string {
	switch p {
	case ProductGreenplum:
		return "Greenplum"
	case ProductCloudberry:
		return "Cloudberry"
	default:
		return "Unknown"
	}
}

// MarshalText/UnmarshalText persist the product as a stable string in
// config.json rather than as a brittle integer ordinal.
func (p Product) MarshalText() ([]byte, error) {
	return []byte(p.String()), nil
}

func (p *Product) UnmarshalText(text []byte) error {
	switch string(text) {
	case "Greenplum":
		*p = ProductGreenplum
	case "Cloudberry":
		*p = ProductCloudberry
	default:
		*p = ProductUnknown
	}
	return nil
}

// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"database/sql"
	"fmt"

	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
)

func addMirrorsToCatalog(intermediate *greenplum.Cluster) error {
	options := []greenplum.Option{
		greenplum.UtilityMode(),
		greenplum.AllowSystemTableMods(),
	}

	db, err := sql.Open("pgx", intermediate.Connection(options...))
	if err != nil {
		return err
	}
	defer func() {
		if cErr := db.Close(); cErr != nil {
			err = errorlist.Append(err, cErr)
		}
	}()

	return AddMirrorsToGpSegmentConfiguration(db, intermediate)
}

func AddMirrorsToGpSegmentConfiguration(db *sql.DB, intermediate *greenplum.Cluster) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return xerrors.Errorf("begin transaction: %w", err)
	}
	defer func() {
		err = commitOrRollback(tx, err)
	}()

	for _, seg := range intermediate.Mirrors.ExcludingStandby() {
		if err := addSegment(tx, seg, intermediate.IsCloudberry()); err != nil {
			return err
		}
	}

	return nil
}

func addSegment(tx *sql.Tx, seg greenplum.SegConfig, isCloudberry bool) error {
	// Cloudberry's gp_segment_configuration has a warehouseid column that
	// Greenplum lacks. It has no SQL-level default, so omitting it leaves the
	// row's warehouseid NULL, which prevents FTS from finding the mirror
	// ("FTS cannot find dbid=N in gp_segment_configuration"). Insert the
	// default warehouse id (0) explicitly for Cloudberry targets.
	columns := "(dbid, content, role, preferred_role, mode, status, port, hostname, address, datadir"
	values := "VALUES($1, $2, $3, $4, 'n', 'u', $5, $6, $7, $8"
	if isCloudberry {
		columns += ", warehouseid"
		values += ", 0"
	}
	columns += ") "
	values += ");"

	result, err := tx.Exec("INSERT INTO gp_segment_configuration "+columns+values,
		seg.DbID, seg.ContentID, seg.Role, seg.Role, seg.Port, seg.Hostname, seg.Hostname, seg.DataDir)
	if err != nil {
		return xerrors.Errorf("insert into gp_segment_configuration: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		panic(fmt.Sprintf("retrieve rows affected: %v", err))
	}

	if rows != 1 {
		return xerrors.Errorf("Expected 1 row to be added for segment dbid %d, but added %d rows instead.", seg.DbID, rows)
	}

	return nil
}

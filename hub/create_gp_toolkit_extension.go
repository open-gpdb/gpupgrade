// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"database/sql"

	"golang.org/x/xerrors"

	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
)

// CreateGpToolkitExtension creates the gp_toolkit extension in every database of
// the target cluster. Cloudberry ships gp_toolkit as an extension that is not
// installed by default, unlike Greenplum where it is part of the catalog. After
// an upgrade the user databases therefore lack gp_toolkit, so we create it
// explicitly. The operation is idempotent via CREATE EXTENSION IF NOT EXISTS.
func CreateGpToolkitExtension(cluster *greenplum.Cluster) (err error) {
	db, err := sql.Open("pgx", cluster.Connection())
	if err != nil {
		return err
	}
	defer func() {
		if cErr := db.Close(); cErr != nil {
			err = errorlist.Append(err, cErr)
		}
	}()

	databases, err := GetConnectableDatabases(db)
	if err != nil {
		return err
	}

	for _, database := range databases {
		if dErr := createGpToolkitInDatabase(cluster, database); dErr != nil {
			err = errorlist.Append(err, dErr)
		}
	}

	return err
}

// GetConnectableDatabases returns the names of all databases that accept
// connections. template0 is excluded because it does not allow connections.
func GetConnectableDatabases(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT datname FROM pg_database WHERE datallowconn AND datname != 'template0';`)
	if err != nil {
		return nil, xerrors.Errorf("querying pg_database: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var datname string
		if sErr := rows.Scan(&datname); sErr != nil {
			return nil, xerrors.Errorf("scanning pg_database: %w", sErr)
		}

		databases = append(databases, datname)
	}

	if rErr := rows.Err(); rErr != nil {
		return nil, xerrors.Errorf("iterating pg_database: %w", rErr)
	}

	return databases, nil
}

func createGpToolkitInDatabase(cluster *greenplum.Cluster, database string) (err error) {
	db, err := sql.Open("pgx", cluster.Connection(greenplum.Database(database)))
	if err != nil {
		return err
	}
	defer func() {
		if cErr := db.Close(); cErr != nil {
			err = errorlist.Append(err, cErr)
		}
	}()

	if _, err := db.Exec(`CREATE EXTENSION IF NOT EXISTS gp_toolkit;`); err != nil {
		return xerrors.Errorf("creating gp_toolkit extension in database %q: %w", database, err)
	}

	return nil
}

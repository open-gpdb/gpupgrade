// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package greenplum

import (
	"database/sql"
	"log"
	"time"

	"golang.org/x/xerrors"
)

func WaitForSegments(db *sql.DB, timeout time.Duration, cluster *Cluster) error {
	startTime := time.Now()
	for {
		// gp_request_fts_probe_scan exists from GPDB6 (PostgreSQL 9) onward,
		// including all Cloudberry releases; GPDB5 (PostgreSQL 8) lacks it.
		if cluster.PostgresMajorVersion() >= 9 {
			rows, err := db.Query("SELECT gp_request_fts_probe_scan();")
			if err != nil {
				return xerrors.Errorf("requesting gp_request_fts_probe_scan: %w", err)
			}

			if err := rows.Close(); err != nil {
				return xerrors.Errorf("closing rows for gp_request_fts_probe_scan: %w", err)
			}
		}

		ready, err := areSegmentsReady(db, cluster)
		if err != nil {
			return err
		}

		if ready {
			return nil
		}

		if time.Since(startTime) > timeout {
			return xerrors.Errorf("%s timeout exceeded waiting for all segments to be up, in their preferred roles, and synchronized.", timeout)
		}

		time.Sleep(time.Second)
	}
}

func areSegmentsReady(db *sql.DB, cluster *Cluster) (bool, error) {
	var segments int

	// check gp_segment_configuration for the segments
	whereClause := "AND mode = 's'"
	if !cluster.HasMirrors() {
		whereClause = ""
	}

	row := db.QueryRow(`SELECT COUNT(*) FROM gp_segment_configuration 
WHERE content > -1 AND status = 'u' AND (role = preferred_role) ` + whereClause)

	if err := row.Scan(&segments); err != nil {
		if err == sql.ErrNoRows {
			log.Printf("no rows found when querying gp_segment_configuration")
			return false, nil
		}

		return false, xerrors.Errorf("querying gp_segment_configuration: %w", err)
	}

	if segments != len(cluster.ExcludingCoordinatorOrStandby()) {
		return false, nil
	}

	// check pg_stat_replication for the standby
	if !cluster.HasStandby() {
		return true, nil
	}

	// PostgreSQL 10 renamed the pg_stat_replication *_location columns to *_lsn.
	// GPDB7 (PG12) and all Cloudberry releases use the LSN names; GPDB6 (PG9)
	// still uses the location names.
	whereClause = "sent_location = flush_location;"
	if cluster.PostgresMajorVersion() >= 10 {
		whereClause = "sent_lsn = flush_lsn;"
	}

	row = db.QueryRow("SELECT COUNT(*) FROM pg_stat_replication WHERE state = 'streaming' AND " + whereClause)
	if err := row.Scan(&segments); err != nil {
		if err == sql.ErrNoRows {
			log.Printf("no rows found when querying pg_stat_replication")
			return false, nil
		}

		return false, xerrors.Errorf("querying pg_stat_replication: %w", err)
	}

	if segments != 1 {
		return false, nil
	}

	return true, nil
}

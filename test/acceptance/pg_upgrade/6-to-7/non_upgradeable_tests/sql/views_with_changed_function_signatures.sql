-- Copyright (c) 2017-2024 VMware, Inc. or its affiliates
-- SPDX-License-Identifier: Apache-2.0

--------------------------------------------------------------------------------
-- Create and setup non-upgradeable objects
--------------------------------------------------------------------------------

DROP SCHEMA IF EXISTS functions_with_changed_signature CASCADE;
CREATE SCHEMA functions_with_changed_signature;
SET search_path to functions_with_changed_signature;

CREATE TABLE ao_table(i int) with (appendonly=true);
CREATE TABLE aoco_table(i int) with (appendonly=true, orientation=column);
INSERT INTO ao_table SELECT generate_series(1,10);
INSERT INTO aoco_table SELECT generate_series(1,10);

CREATE VIEW v01 AS SELECT * FROM gp_toolkit.__gp_aocsseg('aoco_table');
CREATE VIEW v02 AS SELECT * FROM gp_toolkit.__gp_aocsseg_history('aoco_table');
CREATE VIEW v03 AS SELECT * FROM gp_toolkit.__gp_aoseg('ao_table');
CREATE VIEW v04 AS SELECT * FROM gp_toolkit.__gp_aoseg_history('ao_table');
CREATE VIEW v05 AS SELECT * FROM pg_catalog.pg_create_logical_replication_slot('orig_slot1', 'slot_test');
CREATE VIEW v06 AS SELECT * FROM pg_catalog.pg_create_physical_replication_slot('orig_slot1', true);
CREATE VIEW v07 AS SELECT * FROM pg_catalog.gp_dist_wait_status();
CREATE VIEW v08 AS SELECT * FROM pg_catalog.gp_execution_segment();
CREATE VIEW v09 AS SELECT * FROM pg_catalog.gp_request_fts_probe_scan();
CREATE VIEW v10 AS SELECT * FROM pg_catalog.pg_show_all_settings();
CREATE VIEW v11 AS SELECT * FROM pg_catalog.pg_start_backup('testbackup');
CREATE VIEW v12 AS SELECT * FROM pg_catalog.pg_stat_get_wal_senders();
CREATE VIEW v13 AS SELECT * FROM pg_catalog.pg_stat_get_activity(NULL);

---------------------------------------------------------------------------------
--- Assert that pg_upgrade --check correctly detects the non-upgradeable objects
---------------------------------------------------------------------------------
!\retcode gpupgrade initialize --source-gphome="${GPHOME_SOURCE}" --target-gphome=${GPHOME_TARGET} --source-master-port=${PGPORT} --disk-free-ratio 0 --non-interactive;
! cat ~/gpAdminLogs/gpupgrade/pg_upgrade/p-1/views_with_changed_function_signatures.txt;

---------------------------------------------------------------------------------
--- Cleanup
---------------------------------------------------------------------------------
DROP VIEW v13;
DROP VIEW v12;
DROP VIEW v11;
DROP VIEW v10;
DROP VIEW v09;
DROP VIEW v08;
DROP VIEW v07;
DROP VIEW v06;
DROP VIEW v05;
DROP VIEW v04;
DROP VIEW v03;
DROP VIEW v02;
DROP VIEW v01;
DROP TABLE aoco_table;
DROP TABLE ao_table;

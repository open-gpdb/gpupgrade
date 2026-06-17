// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package agent_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/greenplum-db/gpupgrade/agent"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/testutils"
	"github.com/greenplum-db/gpupgrade/testutils/testlog"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
)

func TestCreateRecoveryConf(t *testing.T) {
	testlog.SetupTestLogger()
	agentServer := agent.New()

	t.Run("creates recovery.conf for GPDB 6", func(t *testing.T) {
		mirrorDataDir := testutils.GetTempDir(t, "")

		connReqs := []*idl.CreateRecoveryConfRequest_Connection{{
			MirrorDataDir: mirrorDataDir,
			User:          "gpadmin",
			PrimaryHost:   "sdw1",
			PrimaryPort:   int32(123),
		}}

		_, err := agentServer.CreateRecoveryConf(context.Background(), &idl.CreateRecoveryConfRequest{
			Connections:         connReqs,
			TargetPostgresMajor: 9, // PostgreSQL 9 (GPDB 6) -> legacy recovery.conf
		})
		if err != nil {
			t.Errorf("unexpected error %#v", err)
		}

		contents := testutils.MustReadFile(t, filepath.Join(mirrorDataDir, "recovery.conf"))
		expected := `standby_mode = 'on'
primary_conninfo = 'user=gpadmin host=sdw1 port=123 sslmode=disable sslcompression=1 krbsrvname=postgres application_name=gp_walreceiver'
primary_slot_name = 'internal_wal_replication_slot'`

		if contents != expected {
			t.Errorf("got %q, want %q", contents, expected)
		}
	})

	t.Run("creates standby.signal and postgresql.auto.conf for PG12+ (Cloudberry 3.0, GPDB 7+)", func(t *testing.T) {
		mirrorDataDir := testutils.GetTempDir(t, "")

		connReqs := []*idl.CreateRecoveryConfRequest_Connection{{
			MirrorDataDir: mirrorDataDir,
			User:          "gpadmin",
			PrimaryHost:   "sdw1",
			PrimaryPort:   int32(123),
		}}

		_, err := agentServer.CreateRecoveryConf(context.Background(), &idl.CreateRecoveryConfRequest{
			Connections:         connReqs,
			TargetPostgresMajor: 16, // PostgreSQL 16 (Cloudberry) -> standby.signal
		})
		if err != nil {
			t.Errorf("unexpected error %#v", err)
		}

		standbySignalPath := filepath.Join(mirrorDataDir, "standby.signal")
		if _, err := os.Stat(standbySignalPath); err != nil {
			t.Errorf("standby.signal not created: %v", err)
		}

		autoconfPath := filepath.Join(mirrorDataDir, "postgresql.auto.conf")
		contents := testutils.MustReadFile(t, autoconfPath)
		expectedConninfo := "primary_conninfo = 'user=gpadmin host=sdw1 port=123 sslmode=disable sslcompression=1 krbsrvname=postgres application_name=gp_walreceiver'"
		if !strings.Contains(contents, expectedConninfo) {
			t.Errorf("postgresql.auto.conf missing primary_conninfo: got %q", contents)
		}
		if !strings.Contains(contents, "primary_slot_name = 'internal_wal_replication_slot'") {
			t.Errorf("postgresql.auto.conf missing primary_slot_name: got %q", contents)
		}
		// recovery.conf must not exist for PG12+
		recoveryConfPath := filepath.Join(mirrorDataDir, "recovery.conf")
		if _, err := os.Stat(recoveryConfPath); err == nil {
			t.Error("recovery.conf should not be created for PG12+ targets")
		}
	})

	t.Run("returns multiple errors when failing to write recovery.conf", func(t *testing.T) {
		connReqs := []*idl.CreateRecoveryConfRequest_Connection{
			{
				MirrorDataDir: "/does/not/exist",
				User:          "gpadmin",
				PrimaryHost:   "sdw1",
				PrimaryPort:   int32(123),
			},
			{
				MirrorDataDir: "/also/does/not/exist",
				User:          "gpadmin",
				PrimaryHost:   "sdw2",
				PrimaryPort:   int32(456),
			}}

		_, err := agentServer.CreateRecoveryConf(context.Background(), &idl.CreateRecoveryConfRequest{Connections: connReqs})
		if err == nil {
			t.Error("expected error, returned nil")
		}

		var errs errorlist.Errors
		if !errors.As(err, &errs) {
			t.Fatalf("got error %#v, want type %T", err, errs)
		}

		if len(errs) != 2 {
			t.Errorf("got %d errors want 2", len(errs))
		}

		for _, err := range errs {
			var pathError *os.PathError
			if !errors.As(err, &pathError) {
				t.Errorf("got type %T want %T", err, pathError)
			}
		}
	})
}

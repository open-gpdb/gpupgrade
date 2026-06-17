// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package agent

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/utils/errorlist"
)

func (s *Server) CreateRecoveryConf(ctx context.Context, req *idl.CreateRecoveryConfRequest) (*idl.CreateRecoveryConfReply, error) {
	log.Print("starting create recovery configuration for mirrors")

	err := createRecoveryConf(req.GetConnections(), req.GetTargetPostgresMajor())
	if err != nil {
		return &idl.CreateRecoveryConfReply{}, err
	}

	return &idl.CreateRecoveryConfReply{}, nil
}

// usePg12Recovery reports whether the target uses PostgreSQL 12+ recovery
// (standby.signal + postgresql.auto.conf) rather than the legacy recovery.conf.
// targetPostgresMajor is the target's PostgreSQL major version, decided hub-side
// from the product and version (see hub.CreateRecoveryConfOnSegments). A zero
// value (field absent) falls back to the legacy recovery.conf.
func usePg12Recovery(targetPostgresMajor int32) bool {
	return targetPostgresMajor >= 12
}

func createRecoveryConf(connReqs []*idl.CreateRecoveryConfRequest_Connection, targetPostgresMajor int32) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(connReqs))
	usePg12 := usePg12Recovery(targetPostgresMajor)

	for _, connReq := range connReqs {
		wg.Add(1)

		go func(connReq *idl.CreateRecoveryConfRequest_Connection) {
			defer wg.Done()

			mirrorDataDir := connReq.GetMirrorDataDir()
			primaryConninfo := fmt.Sprintf("user=%s host=%s port=%d sslmode=disable sslcompression=1 krbsrvname=postgres application_name=gp_walreceiver",
				connReq.GetUser(), connReq.GetPrimaryHost(), connReq.GetPrimaryPort())

			if usePg12 {
				// PostgreSQL 12+ (GPDB 7+, Cloudberry): standby.signal + postgresql.auto.conf
				if err := os.WriteFile(filepath.Join(mirrorDataDir, "standby.signal"), []byte{}, 0644); err != nil {
					errs <- err
					return
				}
				recoveryConfig := fmt.Sprintf("primary_conninfo = '%s'\nprimary_slot_name = 'internal_wal_replication_slot'\n", primaryConninfo)
				autoconfPath := filepath.Join(mirrorDataDir, "postgresql.auto.conf")
				f, err := os.OpenFile(autoconfPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					errs <- err
					return
				}
				if _, err := f.WriteString(recoveryConfig); err != nil {
					f.Close()
					errs <- err
					return
				}
				if err := f.Close(); err != nil {
					errs <- err
				}
			} else {
				// GPDB 6: legacy recovery.conf
				config := fmt.Sprintf(`standby_mode = 'on'
primary_conninfo = '%s'
primary_slot_name = 'internal_wal_replication_slot'`, primaryConninfo)
				if err := os.WriteFile(filepath.Join(mirrorDataDir, "recovery.conf"), []byte(config), 0644); err != nil {
					errs <- err
				}
			}
		}(connReq)
	}

	wg.Wait()
	close(errs)

	var err error
	for e := range errs {
		err = errorlist.Append(err, e)
	}

	return err
}

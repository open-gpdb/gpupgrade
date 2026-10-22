// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/greenplum-db/gpupgrade/config"
	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/idl"
)

type restartedTestAgent struct {
	idl.UnimplementedAgentServer
	stops atomic.Int32
}

func (s *restartedTestAgent) StopAgent(context.Context, *idl.StopAgentRequest) (*idl.StopAgentReply, error) {
	s.stops.Add(1)
	return &idl.StopAgentReply{}, nil
}

func TestRestartAgentsRefreshesPartialShutdownCache(t *testing.T) {
	for _, alreadyClosed := range []bool{false, true} {
		t.Run(fmt.Sprintf("cached_connection_closed=%t", alreadyClosed), func(t *testing.T) {
			t.Setenv("GPUPGRADE_HOME", t.TempDir())
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			rpcServer := grpc.NewServer()
			agent := &restartedTestAgent{}
			idl.RegisterAgentServer(rpcServer, agent)
			go func() { _ = rpcServer.Serve(listener) }()
			defer rpcServer.Stop()
			source := MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Port: 15432, Hostname: "localhost", DataDir: "/data/master", Role: greenplum.PrimaryRole},
				{ContentID: 0, DbID: 2, Port: 25432, Hostname: "localhost", DataDir: "/data/segment0", Role: greenplum.PrimaryRole},
				{ContentID: 1, DbID: 3, Port: 25433, Hostname: "127.0.0.1", DataDir: "/data/segment1", Role: greenplum.PrimaryRole},
			})
			server := New(&config.Config{Source: source, AgentPort: listener.Addr().(*net.TCPAddr).Port})
			defer server.Stop(true)
			conns, err := server.AgentConns()
			if err != nil {
				t.Fatal(err)
			}
			if len(conns) != 2 {
				t.Fatalf("initial connections: got %d, want 2", len(conns))
			}

			// A partial shutdown retains only the failed agent. Both source hosts are
			// available again when RestartAgents is invoked, including the removed one.
			if err := conns[1].Conn.Close(); err != nil {
				t.Fatal(err)
			}
			conns[1].CancelContext()
			server.agentConns = conns[:1]
			retained := conns[0]
			if alreadyClosed {
				if err := retained.Conn.Close(); err != nil {
					t.Fatal(err)
				}
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := server.RestartAgents(ctx, &idl.RestartAgentsRequest{}); err != nil {
				t.Fatal(err)
			}
			refreshed, err := server.AgentConns()
			if err != nil {
				t.Fatal(err)
			}
			if len(refreshed) != 2 {
				t.Fatalf("connections after restart: got %d, want all 2 source hosts", len(refreshed))
			}
			if retained.Conn.GetState() != connectivity.Shutdown {
				t.Error("stale agent connection was not closed")
			}
			if err := server.StopAgents(); err != nil {
				t.Fatal(err)
			}
			if got := agent.stops.Load(); got != 2 {
				t.Errorf("shutdown contacted %d agents, want 2", got)
			}
		})
	}
}

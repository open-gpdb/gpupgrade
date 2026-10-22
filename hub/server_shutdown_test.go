// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/greenplum-db/gpupgrade/agent"
	"github.com/greenplum-db/gpupgrade/config"
	"github.com/greenplum-db/gpupgrade/greenplum"
	"github.com/greenplum-db/gpupgrade/hub"
	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/testutils"
)

func shutdownTestConn(t *testing.T, port int) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, fmt.Sprintf("localhost:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func waitForShutdown(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop after acknowledging shutdown")
	}
}

func TestStopServicesAcknowledgesShutdown(t *testing.T) {
	t.Setenv("GPUPGRADE_HOME", t.TempDir())
	agentPort := testutils.MustGetPort(t)
	agentServer := agent.New()
	agentResult := make(chan error, 1)
	stateDir := t.TempDir()
	go func() { agentResult <- agentServer.Start(agentPort, stateDir, false) }()
	shutdownTestConn(t, agentPort)

	conf := &config.Config{
		Source: hub.MustCreateCluster(t, greenplum.SegConfigs{
			{ContentID: -1, DbID: 1, Port: 15432, Hostname: "localhost", DataDir: "/data/master", Role: greenplum.PrimaryRole},
			{ContentID: 0, DbID: 2, Port: 25432, Hostname: "localhost", DataDir: "/data/segment", Role: greenplum.PrimaryRole},
		}),
		AgentPort: agentPort,
	}
	hubServer := hub.New(conf)
	hubPort := testutils.MustGetPort(t)
	hubResult := make(chan error, 1)
	go func() { hubResult <- hubServer.Start(hubPort, false) }()
	client := idl.NewCliToHubClient(shutdownTestConn(t, hubPort))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reply, err := client.StopServices(ctx, &idl.StopServicesRequest{})
	waitForShutdown(t, agentResult)
	waitForShutdown(t, hubResult)
	if err != nil {
		t.Fatalf("shutdown must acknowledge success before closing transport: %v", err)
	}
	if reply == nil {
		t.Fatal("missing shutdown acknowledgement")
	}
	// Cleanup remains safe after the asynchronous graceful shutdown completes.
	hubServer.Stop(true)
	agentServer.Stop()
}

type shutdownTestAgent struct {
	idl.UnimplementedAgentServer
	calls      atomic.Int32
	firstError error
}

func (s *shutdownTestAgent) StopAgent(context.Context, *idl.StopAgentRequest) (*idl.StopAgentReply, error) {
	if s.calls.Add(1) == 1 && s.firstError != nil {
		return nil, s.firstError
	}
	return &idl.StopAgentReply{}, nil
}

func TestStopServicesPreservesFailedAgentsForRetry(t *testing.T) {
	for _, failure := range []error{
		status.Error(codes.Unavailable, "error reading from server: EOF"),
		status.Error(codes.PermissionDenied, "shutdown denied"),
	} {
		t.Run(status.Code(failure).String(), func(t *testing.T) {
			servers := []*shutdownTestAgent{{}, {firstError: failure}}
			addresses := make(map[string]string)
			source := hub.MustCreateCluster(t, greenplum.SegConfigs{
				{ContentID: -1, DbID: 1, Port: 15432, Hostname: "localhost", DataDir: "/data/master", Role: greenplum.PrimaryRole},
				{ContentID: 0, DbID: 2, Port: 25432, Hostname: "agent0", DataDir: "/data/segment0", Role: greenplum.PrimaryRole},
				{ContentID: 1, DbID: 3, Port: 25433, Hostname: "agent1", DataDir: "/data/segment1", Role: greenplum.PrimaryRole},
			})
			hubServer := hub.New(&config.Config{UpgradeID: "shutdown-test", Source: source})
			for i, server := range servers {
				listener, err := net.Listen("tcp", "localhost:0")
				if err != nil {
					t.Fatal(err)
				}
				rpcServer := grpc.NewServer()
				idl.RegisterAgentServer(rpcServer, server)
				go func() { _ = rpcServer.Serve(listener) }()
				t.Cleanup(rpcServer.Stop)
				addresses[fmt.Sprintf("agent%d", i)] = listener.Addr().String()
			}
			hub.SetgRPCDialer(func(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
				hostname, _, err := net.SplitHostPort(target)
				if err != nil {
					return nil, err
				}
				return grpc.DialContext(ctx, addresses[hostname], opts...)
			})
			defer hub.ResetgRPCDialer()
			port := testutils.MustGetPort(t)
			result := make(chan error, 1)
			go func() { result <- hubServer.Start(port, false) }()
			client := idl.NewCliToHubClient(shutdownTestConn(t, port))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := client.StopServices(ctx, &idl.StopServicesRequest{})
			if err == nil || !strings.Contains(err.Error(), status.Convert(failure).Message()) {
				t.Fatalf("got shutdown error %v, want agent failure %v", err, failure)
			}
			if _, err := client.GetConfig(ctx, &idl.GetConfigRequest{Name: "upgrade-id"}); err != nil {
				t.Fatalf("hub must remain available after agent shutdown failure: %v", err)
			}
			if _, err := client.StopServices(ctx, &idl.StopServicesRequest{}); err != nil {
				t.Fatal(err)
			}
			waitForShutdown(t, result)
			if got := servers[0].calls.Load(); got != 1 {
				t.Errorf("successful agent stopped %d times, want 1", got)
			}
			if got := servers[1].calls.Load(); got != 2 {
				t.Errorf("failed agent stopped %d times, want 2", got)
			}
			hubServer.Stop(true)
		})
	}
}

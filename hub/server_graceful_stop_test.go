// Copyright (c) 2017-2023 VMware, Inc. or its affiliates
// SPDX-License-Identifier: Apache-2.0

package hub

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/greenplum-db/gpupgrade/config"
	"github.com/greenplum-db/gpupgrade/idl"
)

type drainingListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *drainingListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return l.Listener.Close()
}

type drainingHubRPC struct {
	idl.UnimplementedCliToHubServer
	hub     *Server
	entered chan struct{}
	resume  chan struct{}
}

func (s *drainingHubRPC) GetConfig(context.Context, *idl.GetConfigRequest) (*idl.GetConfigReply, error) {
	close(s.entered)
	<-s.resume
	// Represent an in-flight RPC that needs the hub's connection/config lock.
	s.hub.mutex.Lock()
	defer s.hub.mutex.Unlock()
	return &idl.GetConfigReply{}, nil
}

func TestGracefulStopAllowsInflightRPCToAcquireMutex(t *testing.T) {
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	trackedListener := &drainingListener{Listener: listener, closed: make(chan struct{})}
	server := New(&config.Config{})
	rpcServer := grpc.NewServer()
	server.gRPCserver = rpcServer
	rpc := &drainingHubRPC{hub: server, entered: make(chan struct{}), resume: make(chan struct{})}
	idl.RegisterCliToHubServer(rpcServer, rpc)
	defer rpcServer.Stop()
	go func() {
		_ = rpcServer.Serve(trackedListener)
		server.stopped <- struct{}{}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rpcResult := make(chan error, 1)
	go func() {
		_, err := idl.NewCliToHubClient(conn).GetConfig(ctx, &idl.GetConfigRequest{})
		rpcResult <- err
	}()
	select {
	case <-rpc.entered:
	case <-ctx.Done():
		t.Fatal("RPC did not start")
	}
	stopped := make(chan struct{})
	go func() { server.stopGracefully(); close(stopped) }()
	select {
	case <-trackedListener.closed:
	case <-ctx.Done():
		t.Fatal("graceful shutdown did not start")
	}
	close(rpc.resume)
	select {
	case err := <-rpcResult:
		if err != nil {
			t.Fatalf("in-flight RPC did not finish successfully: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("in-flight RPC could not acquire hub mutex")
	}
	select {
	case <-stopped:
	case <-ctx.Done():
		t.Fatal("graceful shutdown did not finish")
	}
}

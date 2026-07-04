// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"sync"

	"google.golang.org/grpc"
)

// RpcServer mirrors GRPC::RpcServer: it binds one or more HTTP/2 ports, has
// services registered on it, and runs until stopped. The actual listener comes
// from the injected [Transport], so the server logic itself never touches a
// socket and is fully exercisable in-process over [MemTransport].
type RpcServer struct {
	transport Transport
	server    *grpc.Server

	mu       sync.Mutex
	addrs    []string
	running  bool
	stopped  bool
	serveErr chan error
}

// ServerOption configures an RpcServer at construction.
type ServerOption func(*RpcServer)

// WithTransport injects the network seam. Without it an RpcServer uses
// [NetTransport] (real sockets).
func WithTransport(t Transport) ServerOption {
	return func(s *RpcServer) { s.transport = t }
}

// NewRpcServer builds an RpcServer, mirroring GRPC::RpcServer.new. Register
// services with Handle, bind ports with AddHTTP2Port, then Run.
func NewRpcServer(opts ...ServerOption) *RpcServer {
	s := &RpcServer{serveErr: make(chan error, 1)}
	for _, o := range opts {
		o(s)
	}
	if s.transport == nil {
		s.transport = NetTransport{}
	}
	// ForceServerCodec makes the server speak raw bytes regardless of the
	// content-subtype the peer advertises, so it interoperates with any
	// protobuf peer while staying message-agnostic like the gem.
	s.server = grpc.NewServer(grpc.ForceServerCodec(rawCodec{}))
	return s
}

// AddHTTP2Port mirrors GRPC::RpcServer#add_http2_port(addr, creds). The
// credentials argument is accepted for surface fidelity — pass ":this_port_is_
// insecure" for a plaintext port, as the gem does. TLS credentials are a
// follow-up; the address is remembered and bound when the server runs. It
// returns the bound address.
func (s *RpcServer) AddHTTP2Port(addr, creds string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addrs = append(s.addrs, addr)
	return addr
}

// Handle mirrors GRPC::RpcServer#handle(service): it registers a service's RPCs.
// It must be called before Run.
func (s *RpcServer) Handle(svc Service) {
	desc := svc.toGRPCServiceDesc()
	s.server.RegisterService(&desc, nil)
}

// Run mirrors GRPC::RpcServer#run: it binds every added port and serves. It
// blocks until the server is stopped, returning the first serve error (nil on a
// clean Stop). At least one port must have been added.
func (s *RpcServer) Run() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return NewCallError("server already running")
	}
	if len(s.addrs) == 0 {
		s.mu.Unlock()
		return NewCallError("no http2 port added")
	}
	s.running = true
	addrs := append([]string(nil), s.addrs...)
	s.mu.Unlock()

	listeners := make([]interface{ Close() error }, 0, len(addrs))
	for _, addr := range addrs {
		lis, err := s.transport.Listen(addr)
		if err != nil {
			for _, l := range listeners {
				_ = l.Close()
			}
			return err
		}
		listeners = append(listeners, lis)
		go func() {
			if err := s.server.Serve(lis); err != nil {
				select {
				case s.serveErr <- err:
				default:
				}
			}
		}()
	}
	// Block until Stop closes the server, then report any serve error.
	err := <-s.serveErr
	return err
}

// RunTillTerminated mirrors GRPC::RpcServer#run_till_terminated: it runs the
// server and returns when it is stopped. The gem also installs SIGINT/SIGTERM
// handlers; wiring signals is a host concern, so here it is an alias for Run
// that a host can pair with its own signal trap calling Stop.
func (s *RpcServer) RunTillTerminated() error {
	return s.Run()
}

// Stop mirrors GRPC::RpcServer#stop: it gracefully drains and shuts the server
// down, unblocking Run. It is safe to call once; subsequent calls are no-ops.
func (s *RpcServer) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()

	s.server.GracefulStop()
	select {
	case s.serveErr <- nil:
	default:
	}
}

// Running reports whether Run has been entered and Stop not yet called.
func (s *RpcServer) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running && !s.stopped
}

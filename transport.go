// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"

	"google.golang.org/grpc/test/bufconn"
)

// Transport is the network seam. The core RpcServer and ClientStub never touch
// sockets directly: a server obtains its listener from Listen and a client
// obtains its connections from Dial. Production wiring uses [NetTransport] (real
// TCP); tests use [MemTransport] (an in-process bufconn pipe carrying a real
// HTTP/2 gRPC session). Injecting the transport keeps the whole stack testable
// in-process without binding a port, mirroring the host seam used by the OIDC /
// OAuth2 bindings.
type Transport interface {
	// Listen returns a net.Listener for the given "host:port" address.
	Listen(addr string) (net.Listener, error)
	// Dial returns a connection to the given address, honouring ctx for
	// cancellation and deadlines.
	Dial(ctx context.Context, addr string) (net.Conn, error)
}

// NetTransport is the production transport: it listens and dials real TCP
// sockets via the standard library. It is the default when a server or stub is
// created without an explicit transport.
type NetTransport struct{}

// Listen binds a TCP socket at addr.
func (NetTransport) Listen(addr string) (net.Listener, error) {
	return net.Listen("tcp", addr)
}

// Dial opens a TCP connection to addr.
func (NetTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}

// bufSize is the bufconn pipe buffer; large enough that test messages never
// block on a full pipe.
const bufSize = 1024 * 1024

// MemTransport is an in-process transport backed by bufconn. A server's Listen
// registers an in-memory listener under its address; a client's Dial to that
// same address is wired straight to it. No OS socket or port is used, yet a real
// gRPC HTTP/2 session runs over the pipe, so servers and stubs are exercised
// end-to-end. Share one MemTransport between the server and the stubs that call
// it.
type MemTransport struct {
	mu        sync.Mutex
	listeners map[string]*bufconn.Listener
}

// NewMemTransport creates an empty in-memory transport.
func NewMemTransport() *MemTransport {
	return &MemTransport{listeners: map[string]*bufconn.Listener{}}
}

// Listen registers and returns an in-memory listener for addr. Re-listening on
// an address that already has a live listener is an error, matching a bind
// conflict on a real socket.
func (t *MemTransport) Listen(addr string) (net.Listener, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.listeners == nil {
		t.listeners = map[string]*bufconn.Listener{}
	}
	if _, ok := t.listeners[addr]; ok {
		return nil, fmt.Errorf("grpc: address %q already in use", addr)
	}
	lis := bufconn.Listen(bufSize)
	t.listeners[addr] = lis
	return &memListener{Listener: lis, transport: t, addr: addr}, nil
}

// Dial connects to the in-memory listener registered for addr.
func (t *MemTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	t.mu.Lock()
	lis, ok := t.listeners[addr]
	t.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("grpc: no in-memory listener for %q", addr)
	}
	return lis.DialContext(ctx)
}

// remove drops the listener registration for addr when its server stops.
func (t *MemTransport) remove(addr string) {
	t.mu.Lock()
	delete(t.listeners, addr)
	t.mu.Unlock()
}

// memListener wraps a bufconn.Listener so that closing it also deregisters the
// address, freeing it for reuse after the server stops.
type memListener struct {
	*bufconn.Listener
	transport *MemTransport
	addr      string
}

// Close closes the underlying listener and unregisters the address.
func (l *memListener) Close() error {
	l.transport.remove(l.addr)
	return l.Listener.Close()
}

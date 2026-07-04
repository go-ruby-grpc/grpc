// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

// strMarshal / strUnmarshal are the trivial string codec the functional tests
// use as stand-ins for a generated message's marshal/unmarshal procs.
func strMarshal(v any) ([]byte, error)   { return []byte(v.(string)), nil }
func strUnmarshal(b []byte) (any, error) { return string(b), nil }

// echoService builds a Service exercising all four RPC cardinalities plus
// metadata, deadline and error behaviours.
func echoService() Service {
	return Service{
		Name: "test.Echo",
		Methods: []Method{
			{
				Name: "Unary", Type: Unary,
				RequestUnmarshal: strUnmarshal, ResponseMarshal: strMarshal,
				UnaryHandler: func(req any, call *ActiveCall) (any, error) {
					if user := call.Metadata()["x-user"]; user != "" {
						return "hello " + req.(string) + " from " + user, nil
					}
					if req.(string) == "boom" {
						return nil, NewBadStatus(InvalidArgument, "bad argument", nil)
					}
					if req.(string) == "panic" {
						return nil, errors.New("unexpected")
					}
					if req.(string) == "slow" {
						time.Sleep(500 * time.Millisecond)
						return "late", nil
					}
					return "hello " + req.(string), nil
				},
			},
			{
				Name: "ClientStream", Type: ClientStream,
				RequestUnmarshal: strUnmarshal, ResponseMarshal: strMarshal,
				ClientStreamHandler: func(call *ActiveCall) (any, error) {
					var parts []string
					err := call.EachRemoteRead(func(msg any) error {
						parts = append(parts, msg.(string))
						return nil
					})
					if err != nil {
						return nil, err
					}
					return strings.Join(parts, ","), nil
				},
			},
			{
				Name: "ServerStream", Type: ServerStream,
				RequestUnmarshal: strUnmarshal, ResponseMarshal: strMarshal,
				ServerStreamHandler: func(req any, call *ActiveCall) error {
					for _, w := range strings.Fields(req.(string)) {
						if err := call.Send("<" + w + ">"); err != nil {
							return err
						}
					}
					return nil
				},
			},
			{
				Name: "BidiStream", Type: BidiStream,
				RequestUnmarshal: strUnmarshal, ResponseMarshal: strMarshal,
				BidiStreamHandler: func(call *ActiveCall) error {
					return call.EachRemoteRead(func(msg any) error {
						return call.Send("re:" + msg.(string))
					})
				},
			},
		},
	}
}

// startEcho spins up an RpcServer with the echo service over a shared
// MemTransport and returns a connected stub. It registers cleanup.
func startEcho(t *testing.T) (*ClientStub, *RpcServer, *MemTransport) {
	t.Helper()
	tr := NewMemTransport()
	srv := NewRpcServer(WithTransport(tr))
	srv.AddHTTP2Port("bufnet:1", ":this_port_is_insecure")
	srv.Handle(echoService())
	go func() { _ = srv.Run() }()
	waitRunning(t, srv)

	stub, err := NewClientStub("bufnet:1", ":this_channel_is_insecure", WithStubTransport(tr))
	if err != nil {
		t.Fatalf("NewClientStub: %v", err)
	}
	t.Cleanup(func() {
		_ = stub.Close()
		srv.Stop()
	})
	return stub, srv, tr
}

func waitRunning(t *testing.T, srv *RpcServer) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if srv.Running() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("server never became running")
}

func opts() CallOptions {
	return CallOptions{Marshal: strMarshal, Unmarshal: strUnmarshal}
}

func TestUnary(t *testing.T) {
	stub, _, _ := startEcho(t)
	resp, err := stub.RequestResponse("/test.Echo/Unary", "world", opts())
	if err != nil {
		t.Fatalf("RequestResponse: %v", err)
	}
	if resp != "hello world" {
		t.Fatalf("got %q", resp)
	}
}

func TestUnaryMetadata(t *testing.T) {
	stub, _, _ := startEcho(t)
	o := opts()
	o.Metadata = Metadata{"x-user": "Alice"}
	resp, err := stub.RequestResponse("/test.Echo/Unary", "world", o)
	if err != nil {
		t.Fatalf("RequestResponse: %v", err)
	}
	if resp != "hello world from Alice" {
		t.Fatalf("metadata not propagated: %q", resp)
	}
}

func TestUnaryBadStatus(t *testing.T) {
	stub, _, _ := startEcho(t)
	_, err := stub.RequestResponse("/test.Echo/Unary", "boom", opts())
	var bs *BadStatus
	if !errors.As(err, &bs) {
		t.Fatalf("want *BadStatus, got %T: %v", err, err)
	}
	if bs.Code != InvalidArgument {
		t.Fatalf("code = %s", bs.Code)
	}
	if bs.Details != "bad argument" {
		t.Fatalf("details = %q", bs.Details)
	}
}

func TestUnaryUnknownError(t *testing.T) {
	stub, _, _ := startEcho(t)
	_, err := stub.RequestResponse("/test.Echo/Unary", "panic", opts())
	var bs *BadStatus
	if !errors.As(err, &bs) || bs.Code != Unknown {
		t.Fatalf("want UNKNOWN BadStatus, got %v", err)
	}
}

func TestUnaryDeadline(t *testing.T) {
	stub, _, _ := startEcho(t)
	o := opts()
	o.Deadline = time.Now().Add(80 * time.Millisecond)
	_, err := stub.RequestResponse("/test.Echo/Unary", "slow", o)
	var bs *BadStatus
	if !errors.As(err, &bs) || bs.Code != DeadlineExceeded {
		t.Fatalf("want DEADLINE_EXCEEDED, got %v", err)
	}
}

func TestClientStream(t *testing.T) {
	stub, _, _ := startEcho(t)
	resp, err := stub.ClientStreamer("/test.Echo/ClientStream",
		[]any{"a", "b", "c"}, opts())
	if err != nil {
		t.Fatalf("ClientStreamer: %v", err)
	}
	if resp != "a,b,c" {
		t.Fatalf("got %q", resp)
	}
}

func TestServerStream(t *testing.T) {
	stub, _, _ := startEcho(t)
	resp, err := stub.ServerStreamer("/test.Echo/ServerStream", "x y z", opts())
	if err != nil {
		t.Fatalf("ServerStreamer: %v", err)
	}
	if !reflect.DeepEqual(resp, []any{"<x>", "<y>", "<z>"}) {
		t.Fatalf("got %v", resp)
	}
}

func TestBidiStream(t *testing.T) {
	stub, _, _ := startEcho(t)
	resp, err := stub.BidiStreamer("/test.Echo/BidiStream",
		[]any{"1", "2"}, opts())
	if err != nil {
		t.Fatalf("BidiStreamer: %v", err)
	}
	if !reflect.DeepEqual(resp, []any{"re:1", "re:2"}) {
		t.Fatalf("got %v", resp)
	}
}

// TestStubTimeoutOption exercises the WithTimeout stub default.
func TestStubTimeoutOption(t *testing.T) {
	tr := NewMemTransport()
	srv := NewRpcServer(WithTransport(tr))
	srv.AddHTTP2Port("to:1", ":insecure")
	srv.Handle(echoService())
	go func() { _ = srv.Run() }()
	waitRunning(t, srv)
	defer srv.Stop()

	stub, err := NewClientStub("to:1", ":insecure",
		WithStubTransport(tr), WithTimeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()
	resp, err := stub.RequestResponse("/test.Echo/Unary", "t", opts())
	if err != nil || resp != "hello t" {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
}

// TestReadEOFAtDrain confirms drain returns collected messages on EOF and that
// EachRemoteRead terminates cleanly.
func TestServerStreamEmpty(t *testing.T) {
	stub, _, _ := startEcho(t)
	resp, err := stub.ServerStreamer("/test.Echo/ServerStream", "", opts())
	if err != nil {
		t.Fatalf("ServerStreamer: %v", err)
	}
	if len(resp) != 0 {
		t.Fatalf("want empty, got %v", resp)
	}
	_ = io.EOF
}

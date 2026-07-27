// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"reflect"
	"strings"
	"testing"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// genericEchoService builds a GenericService exercising every cardinality, the
// way a generated *_services_pb.rb Service base declares its rpcs. The string
// codec stands in for a message class' encode/decode.
func genericEchoService() *GenericService {
	strDesc := func(name string, t MethodType) RpcDesc {
		return RpcDesc{
			Name: name, Type: t,
			RequestMarshal: strMarshal, RequestUnmarshal: strUnmarshal,
			ResponseMarshal: strMarshal, ResponseUnmarshal: strUnmarshal,
		}
	}
	return NewGenericService("test.Svc").
		RPC(strDesc("Echo", Unary)).
		RPC(strDesc("Sum", ClientStream)).
		RPC(strDesc("Split", ServerStream)).
		RPC(strDesc("Chat", BidiStream))
}

func genericEchoHandlers() Handlers {
	return Handlers{
		"Echo": func(req any, call *ActiveCall) (any, error) {
			return "echo:" + req.(string), nil
		},
		"Sum": func(call *ActiveCall) (any, error) {
			var parts []string
			if err := call.EachRemoteRead(func(m any) error {
				parts = append(parts, m.(string))
				return nil
			}); err != nil {
				return nil, err
			}
			return strings.Join(parts, ""), nil
		},
		"Split": func(req any, call *ActiveCall) error {
			if err := call.Send(req.(string) + "!"); err != nil {
				return err
			}
			return call.Send(req.(string) + "?")
		},
		"Chat": func(call *ActiveCall) error {
			return call.EachRemoteRead(func(m any) error {
				return call.Send("re:" + m.(string))
			})
		},
	}
}

// startGenericEcho registers a GenericService-built Service on an RpcServer over
// a MemTransport and returns a GenericStub over a connected ClientStub.
func startGenericEcho(t *testing.T) *GenericStub {
	t.Helper()
	gs := genericEchoService()
	svc, err := gs.BuildService(genericEchoHandlers())
	if err != nil {
		t.Fatalf("BuildService: %v", err)
	}
	tr := NewMemTransport()
	srv := NewRpcServer(WithTransport(tr))
	srv.AddHTTP2Port("generic:1", ":this_port_is_insecure")
	srv.Handle(svc)
	go func() { _ = srv.Run() }()
	waitRunning(t, srv)

	stub, err := NewClientStub("generic:1", ":this_channel_is_insecure", WithStubTransport(tr))
	if err != nil {
		t.Fatalf("NewClientStub: %v", err)
	}
	t.Cleanup(func() {
		_ = stub.Close()
		srv.Stop()
	})
	return gs.StubClass(stub)
}

// TestGenericStubAllCardinalities drives every RPC shape end-to-end through the
// generated-service layer. Crucially the CallOptions carry no Marshal/Unmarshal:
// the generated stub supplies the descriptor's codec, exactly as the gem's
// rpc_stub_class does.
func TestGenericStubAllCardinalities(t *testing.T) {
	stub := startGenericEcho(t)

	resp, err := stub.RequestResponse("Echo", "hi", CallOptions{Metadata: Metadata{"x-trace": "1"}})
	if err != nil {
		t.Fatalf("RequestResponse: %v", err)
	}
	if resp != "echo:hi" {
		t.Errorf("unary = %q, want %q", resp, "echo:hi")
	}

	sum, err := stub.ClientStreamer("Sum", []any{"a", "b", "c"}, CallOptions{})
	if err != nil {
		t.Fatalf("ClientStreamer: %v", err)
	}
	if sum != "abc" {
		t.Errorf("client-stream = %q, want %q", sum, "abc")
	}

	split, err := stub.ServerStreamer("Split", "x", CallOptions{})
	if err != nil {
		t.Fatalf("ServerStreamer: %v", err)
	}
	if !reflect.DeepEqual(split, []any{"x!", "x?"}) {
		t.Errorf("server-stream = %v, want [x! x?]", split)
	}

	chat, err := stub.BidiStreamer("Chat", []any{"1", "2"}, CallOptions{})
	if err != nil {
		t.Fatalf("BidiStreamer: %v", err)
	}
	if !reflect.DeepEqual(chat, []any{"re:1", "re:2"}) {
		t.Errorf("bidi-stream = %v, want [re:1 re:2]", chat)
	}
}

// TestGenericServiceDeclarations covers ServiceName, RpcDescs (a copy in
// declaration order), LookupRPC and the duplicate-replace path of RPC.
func TestGenericServiceDeclarations(t *testing.T) {
	gs := NewGenericService("a.B").
		RPC(RpcDesc{Name: "One", Type: Unary}).
		RPC(RpcDesc{Name: "Two", Type: ServerStream}).
		RPC(RpcDesc{Name: "One", Type: BidiStream}) // redeclare One

	if gs.ServiceName() != "a.B" {
		t.Errorf("ServiceName = %q", gs.ServiceName())
	}
	descs := gs.RpcDescs()
	if len(descs) != 2 {
		t.Fatalf("RpcDescs len = %d, want 2 (dup replaced in place)", len(descs))
	}
	if descs[0].Name != "One" || descs[0].Type != BidiStream {
		t.Errorf("redeclared One = %+v, want name One type BidiStream", descs[0])
	}
	if descs[1].Name != "Two" {
		t.Errorf("descs[1] = %q, want Two", descs[1].Name)
	}
	// Mutating the returned slice must not affect the service.
	descs[0].Name = "mutated"
	if again := gs.RpcDescs(); again[0].Name != "One" {
		t.Errorf("RpcDescs did not return a copy: %q", again[0].Name)
	}
	if _, ok := gs.LookupRPC("nope"); ok {
		t.Error("LookupRPC found a nonexistent rpc")
	}
}

// TestBuildServiceErrors covers every BuildService failure branch.
func TestBuildServiceErrors(t *testing.T) {
	unary := func(req any, call *ActiveCall) (any, error) { return nil, nil }
	clientStream := func(call *ActiveCall) (any, error) { return nil, nil }
	serverStream := func(req any, call *ActiveCall) error { return nil }
	bidi := func(call *ActiveCall) error { return nil }

	t.Run("no_rpcs", func(t *testing.T) {
		if _, err := NewGenericService("x").BuildService(nil); err == nil {
			t.Fatal("want error for a service with no rpcs")
		}
	})
	t.Run("missing_handler", func(t *testing.T) {
		gs := NewGenericService("x").RPC(RpcDesc{Name: "M", Type: Unary})
		if _, err := gs.BuildService(Handlers{}); err == nil {
			t.Fatal("want error for a missing handler")
		}
	})

	wrongShape := []struct {
		name string
		typ  MethodType
		good any // a correctly shaped handler, to prove the good path builds
		bad  any // a mis-shaped handler
	}{
		{"unary", Unary, unary, clientStream},
		{"client_stream", ClientStream, clientStream, unary},
		{"server_stream", ServerStream, serverStream, bidi},
		{"bidi_stream", BidiStream, bidi, serverStream},
	}
	for _, ws := range wrongShape {
		t.Run("wrong_shape_"+ws.name, func(t *testing.T) {
			gs := NewGenericService("x").RPC(RpcDesc{Name: "M", Type: ws.typ})
			if _, err := gs.BuildService(Handlers{"M": ws.bad}); err == nil {
				t.Fatal("want error for a mis-shaped handler")
			}
			if _, err := gs.BuildService(Handlers{"M": ws.good}); err != nil {
				t.Fatalf("well-shaped handler rejected: %v", err)
			}
		})
	}

	t.Run("unknown_cardinality", func(t *testing.T) {
		gs := NewGenericService("x").RPC(RpcDesc{Name: "M", Type: MethodType(99)})
		if _, err := gs.BuildService(Handlers{"M": unary}); err == nil {
			t.Fatal("want error for an unknown cardinality")
		}
	})
}

// TestGenericStubMismatch covers the unknown-rpc and wrong-cardinality guards on
// each stub method.
func TestGenericStubMismatch(t *testing.T) {
	stub := startGenericEcho(t)

	if _, err := stub.RequestResponse("Nope", "x", CallOptions{}); err == nil {
		t.Error("want error for an unknown rpc")
	}
	if _, err := stub.RequestResponse("Sum", "x", CallOptions{}); err == nil {
		t.Error("want error calling a client-stream rpc as unary")
	}
	if _, err := stub.ClientStreamer("Echo", nil, CallOptions{}); err == nil {
		t.Error("want error calling a unary rpc as client-stream")
	}
	if _, err := stub.ServerStreamer("Echo", "x", CallOptions{}); err == nil {
		t.Error("want error calling a unary rpc as server-stream")
	}
	if _, err := stub.BidiStreamer("Echo", nil, CallOptions{}); err == nil {
		t.Error("want error calling a unary rpc as bidi-stream")
	}
}

// TestMethodTypeName covers the cardinality-name mapping including its fallback.
func TestMethodTypeName(t *testing.T) {
	cases := map[MethodType]string{
		Unary:          "request_response",
		ClientStream:   "client_streamer",
		ServerStream:   "server_streamer",
		BidiStream:     "bidi_streamer",
		MethodType(99): "unknown",
	}
	for typ, want := range cases {
		if got := methodTypeName(typ); got != want {
			t.Errorf("methodTypeName(%d) = %q, want %q", typ, got, want)
		}
	}
}

// TestGenericStubToRealServer is the wire-interop oracle for the generated-stub
// path: a GenericStub calls a stock google.golang.org/grpc server over the
// in-memory transport, with real protobuf messages. It proves the generated stub
// speaks the gRPC wire faithfully, not just to our own server.
func TestGenericStubToRealServer(t *testing.T) {
	tr := NewMemTransport()
	lis, err := tr.Listen("genericoracle:1")
	if err != nil {
		t.Fatal(err)
	}
	real := ggrpc.NewServer()
	real.RegisterService(&realEchoDesc, nil)
	go func() { _ = real.Serve(lis) }()
	defer real.Stop()

	stub, err := NewClientStub("genericoracle:1", ":insecure", WithStubTransport(tr))
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()

	gs := NewGenericService("oracle.Echo").RPC(RpcDesc{
		Name: "Unary", Type: Unary,
		RequestMarshal: pbMarshal, RequestUnmarshal: pbUnmarshal,
		ResponseMarshal: pbMarshal, ResponseUnmarshal: pbUnmarshal,
	})
	gstub := gs.StubClass(stub)

	resp, err := gstub.RequestResponse("Unary", wrapperspb.String("world"), CallOptions{})
	if err != nil {
		t.Fatalf("RequestResponse: %v", err)
	}
	if got := resp.(*wrapperspb.StringValue).Value; got != "echo:world" {
		t.Errorf("got %q, want %q", got, "echo:world")
	}
}

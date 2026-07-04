// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"context"
	"io"
	"testing"

	rbpb "github.com/go-ruby-protobuf/protobuf"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// The oracle: a real google.golang.org/grpc peer on the other end of the
// in-memory transport. These tests prove wire interop in both directions —
// our stub talking to a stock grpc-go server, and a stock grpc-go client
// talking to our RpcServer — with real protobuf messages.

// realEchoDesc is a hand-written grpc-go ServiceDesc (exactly the shape
// protoc-gen-go-grpc emits) for a stock server. It uses the default proto
// codec and echoes a wrapperspb.StringValue.
var realEchoDesc = ggrpc.ServiceDesc{
	ServiceName: "oracle.Echo",
	HandlerType: (*any)(nil),
	Methods: []ggrpc.MethodDesc{{
		MethodName: "Unary",
		Handler: func(srv any, ctx context.Context, dec func(any) error, _ ggrpc.UnaryServerInterceptor) (any, error) {
			in := new(wrapperspb.StringValue)
			if err := dec(in); err != nil {
				return nil, err
			}
			return wrapperspb.String("echo:" + in.Value), nil
		},
	}},
}

// pbMarshal / pbUnmarshal are the real-protobuf codec funcs a generated
// *_services_pb.rb would attach, using google.golang.org/protobuf directly.
func pbMarshal(v any) ([]byte, error) { return proto.Marshal(v.(*wrapperspb.StringValue)) }
func pbUnmarshal(b []byte) (any, error) {
	m := new(wrapperspb.StringValue)
	if err := proto.Unmarshal(b, m); err != nil {
		return nil, err
	}
	return m, nil
}

// TestOracleOurStubToRealServer: our ClientStub → stock grpc-go server.
func TestOracleOurStubToRealServer(t *testing.T) {
	tr := NewMemTransport()
	lis, err := tr.Listen("oracleA:1")
	if err != nil {
		t.Fatal(err)
	}
	real := ggrpc.NewServer()
	real.RegisterService(&realEchoDesc, nil)
	go func() { _ = real.Serve(lis) }()
	defer real.Stop()

	stub, err := NewClientStub("oracleA:1", ":insecure", WithStubTransport(tr))
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()

	resp, err := stub.RequestResponse("/oracle.Echo/Unary",
		wrapperspb.String("hi"), CallOptions{Marshal: pbMarshal, Unmarshal: pbUnmarshal})
	if err != nil {
		t.Fatalf("RequestResponse: %v", err)
	}
	if got := resp.(*wrapperspb.StringValue).Value; got != "echo:hi" {
		t.Fatalf("got %q", got)
	}
}

// TestOracleRealClientToOurServer: stock grpc-go client → our RpcServer (unary).
func TestOracleRealClientToOurServer(t *testing.T) {
	tr := NewMemTransport()
	srv := NewRpcServer(WithTransport(tr))
	srv.AddHTTP2Port("oracleB:1", ":insecure")
	srv.Handle(Service{
		Name: "oracle.Echo2",
		Methods: []Method{{
			Name: "Unary", Type: Unary,
			RequestUnmarshal: pbUnmarshal, ResponseMarshal: pbMarshal,
			UnaryHandler: func(req any, call *ActiveCall) (any, error) {
				return wrapperspb.String("srv:" + req.(*wrapperspb.StringValue).Value), nil
			},
		}},
	})
	go func() { _ = srv.Run() }()
	waitRunning(t, srv)
	defer srv.Stop()

	cc, err := ggrpc.NewClient("passthrough:///oracleB:1",
		ggrpc.WithTransportCredentials(insecure.NewCredentials()),
		ggrpc.WithContextDialer(tr.Dial))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	out := new(wrapperspb.StringValue)
	if err := cc.Invoke(context.Background(), "/oracle.Echo2/Unary",
		wrapperspb.String("yo"), out); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if out.Value != "srv:yo" {
		t.Fatalf("got %q", out.Value)
	}
}

// TestOracleRealClientStreamToOurServer: stock grpc-go client server-streaming
// → our RpcServer ServerStream handler.
func TestOracleRealClientStreamToOurServer(t *testing.T) {
	tr := NewMemTransport()
	srv := NewRpcServer(WithTransport(tr))
	srv.AddHTTP2Port("oracleC:1", ":insecure")
	srv.Handle(Service{
		Name: "oracle.Echo3",
		Methods: []Method{{
			Name: "Stream", Type: ServerStream,
			RequestUnmarshal: pbUnmarshal, ResponseMarshal: pbMarshal,
			ServerStreamHandler: func(req any, call *ActiveCall) error {
				for i := 0; i < 3; i++ {
					if err := call.Send(wrapperspb.String(req.(*wrapperspb.StringValue).Value)); err != nil {
						return err
					}
				}
				return nil
			},
		}},
	})
	go func() { _ = srv.Run() }()
	waitRunning(t, srv)
	defer srv.Stop()

	cc, err := ggrpc.NewClient("passthrough:///oracleC:1",
		ggrpc.WithTransportCredentials(insecure.NewCredentials()),
		ggrpc.WithContextDialer(tr.Dial))
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()

	desc := &ggrpc.StreamDesc{StreamName: "Stream", ServerStreams: true}
	st, err := cc.NewStream(context.Background(), desc, "/oracle.Echo3/Stream")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SendMsg(wrapperspb.String("z")); err != nil {
		t.Fatal(err)
	}
	if err := st.CloseSend(); err != nil {
		t.Fatal(err)
	}
	var n int
	for {
		out := new(wrapperspb.StringValue)
		err := st.RecvMsg(out)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("RecvMsg: %v", err)
		}
		if out.Value != "z" {
			t.Fatalf("got %q", out.Value)
		}
		n++
	}
	if n != 3 {
		t.Fatalf("want 3 responses, got %d", n)
	}
}

// TestGoRubyProtobufMessage round-trips a message built with go-ruby-protobuf
// through the whole stack, confirming the message-layer integration: the
// dynamic message's Encode/Decode are the call's marshal/unmarshal.
func TestGoRubyProtobufMessage(t *testing.T) {
	pool := rbpb.NewDescriptorPool()
	if err := pool.Build(func(b *rbpb.Builder) {
		b.AddMessage("Greeting", func(mb *rbpb.MessageBuilder) {
			mb.Optional("text", "string", 1)
		})
	}); err != nil {
		t.Fatal(err)
	}
	class := pool.LookupMsgclass("Greeting")

	marshal := func(v any) ([]byte, error) { return rbpb.Encode(v.(*rbpb.Message)) }
	unmarshal := func(b []byte) (any, error) { return rbpb.Decode(class, b) }

	tr := NewMemTransport()
	srv := NewRpcServer(WithTransport(tr))
	srv.AddHTTP2Port("pb:1", ":insecure")
	srv.Handle(Service{
		Name: "pb.Greeter",
		Methods: []Method{{
			Name: "Hello", Type: Unary,
			RequestUnmarshal: unmarshal, ResponseMarshal: marshal,
			UnaryHandler: func(req any, call *ActiveCall) (any, error) {
				in := req.(*rbpb.Message)
				text, _ := in.Get("text")
				out, _ := class.New()
				_ = out.Set("text", "hi "+text.(string))
				return out, nil
			},
		}},
	})
	go func() { _ = srv.Run() }()
	waitRunning(t, srv)
	defer srv.Stop()

	stub, err := NewClientStub("pb:1", ":insecure", WithStubTransport(tr))
	if err != nil {
		t.Fatal(err)
	}
	defer stub.Close()

	req, _ := class.New()
	_ = req.Set("text", "bob")
	resp, err := stub.RequestResponse("/pb.Greeter/Hello", req,
		CallOptions{Marshal: marshal, Unmarshal: unmarshal})
	if err != nil {
		t.Fatalf("RequestResponse: %v", err)
	}
	text, _ := resp.(*rbpb.Message).Get("text")
	if text != "hi bob" {
		t.Fatalf("got %q", text)
	}
}

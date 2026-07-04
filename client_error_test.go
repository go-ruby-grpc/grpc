// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"context"
	"errors"
	"testing"
)

// errMarshal always fails, standing in for a message that cannot be serialized.
func errMarshal(any) ([]byte, error) { return nil, errors.New("marshal fail") }

func failOpts() CallOptions {
	return CallOptions{Marshal: errMarshal, Unmarshal: strUnmarshal}
}

func TestNewClientStubBadTarget(t *testing.T) {
	// A host that yields an invalid gRPC target makes grpc.NewClient fail, and
	// the stub surfaces it as a *BadStatus.
	_, err := NewClientStub("%zz", ":insecure", WithStubTransport(NewMemTransport()))
	var bs *BadStatus
	if !errors.As(err, &bs) {
		t.Fatalf("want *BadStatus, got %T: %v", err, err)
	}
}

func TestRequestResponseMarshalError(t *testing.T) {
	stub, _, _ := startEcho(t)
	if _, err := stub.RequestResponse("/test.Echo/Unary", "x", failOpts()); err == nil {
		t.Fatal("want marshal error")
	}
}

func TestStreamMarshalErrors(t *testing.T) {
	stub, _, _ := startEcho(t)
	if _, err := stub.ClientStreamer("/test.Echo/ClientStream", []any{"a"}, failOpts()); err == nil {
		t.Fatal("ClientStreamer: want marshal error")
	}
	if _, err := stub.ServerStreamer("/test.Echo/ServerStream", "a", failOpts()); err == nil {
		t.Fatal("ServerStreamer: want marshal error")
	}
	if _, err := stub.BidiStreamer("/test.Echo/BidiStream", []any{"a"}, failOpts()); err == nil {
		t.Fatal("BidiStreamer: want marshal error")
	}
}

func TestStreamNewStreamErrorOnClosedStub(t *testing.T) {
	stub, _, _ := startEcho(t)
	_ = stub.Close() // closing the conn makes NewStream fail
	if _, err := stub.ClientStreamer("/test.Echo/ClientStream", []any{"a"}, opts()); err == nil {
		t.Fatal("ClientStreamer: want newStream error")
	}
	if _, err := stub.ServerStreamer("/test.Echo/ServerStream", "a", opts()); err == nil {
		t.Fatal("ServerStreamer: want newStream error")
	}
	if _, err := stub.BidiStreamer("/test.Echo/BidiStream", []any{"a"}, opts()); err == nil {
		t.Fatal("BidiStreamer: want newStream error")
	}
}

func TestStreamHandlerErrorsPropagate(t *testing.T) {
	stub, _, _ := startEcho(t)
	// ClientStream handler returns a non-OK status -> the final Read errors.
	if _, err := stub.ClientStreamer("/test.Echo/ClientStream", []any{"a", "err"}, opts()); err == nil {
		t.Fatal("ClientStreamer: want handler error")
	}
	// ServerStream handler sends one then errors -> drain returns the error
	// after collecting the partial message.
	if _, err := stub.ServerStreamer("/test.Echo/ServerStream", "x err", opts()); err == nil {
		t.Fatal("ServerStreamer: want handler error")
	}
	// BidiStream handler errors mid-stream -> drain returns the error.
	if _, err := stub.BidiStreamer("/test.Echo/BidiStream", []any{"1", "err"}, opts()); err == nil {
		t.Fatal("BidiStreamer: want handler error")
	}
}

// TestUnaryActiveCallSeams drives a unary handler that touches its ActiveCall:
// Send and Read raise a CallError (a unary call is not a stream), while
// Metadata and Deadline read fine.
func TestUnaryActiveCallSeams(t *testing.T) {
	m := Method{
		Name: "M", Type: Unary,
		RequestUnmarshal: strUnmarshal, ResponseMarshal: strMarshal,
		UnaryHandler: func(req any, call *ActiveCall) (any, error) {
			if err := call.Send("nope"); err == nil {
				return nil, errors.New("Send should fail on a unary call")
			}
			if _, err := call.Read(); err == nil {
				return nil, errors.New("Read should fail on a unary call")
			}
			_, _ = call.Deadline() // exercises ctxOnlyStream.Context
			_ = call.Metadata()
			return "ok", nil
		},
	}
	h := m.unaryGRPCHandler()
	out, err := h(nil, context.Background(), decOf("in"), nil)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if string(out.(rawMessage)) != "ok" {
		t.Fatalf("got %q", out)
	}
}

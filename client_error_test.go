// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"context"
	"errors"
	"io"
	"testing"
)

// errMarshal always fails, standing in for a message that cannot be serialized.
func errMarshal(any) ([]byte, error) { return nil, errors.New("marshal fail") }

// sendErrStream is a grpcStream whose SendMsg always fails with a preset error,
// standing in for a stream the server has half-closed (e.g. after rejecting the
// call as UNIMPLEMENTED). It lets sendAll's stream-break branch be exercised
// deterministically, without racing a real transport.
type sendErrStream struct{ sendErr error }

func (s sendErrStream) SendMsg(any) error        { return s.sendErr }
func (s sendErrStream) RecvMsg(any) error        { return io.EOF }
func (s sendErrStream) Context() context.Context { return context.Background() }

// TestSendAllStreamBreak covers sendAll's two error branches deterministically.
// A transport SendMsg failure (surfaced by ActiveCall.Send as a *BadStatus) is a
// broken stream: sendAll stops without error so the caller's Read/drain can
// report the true RPC status. A client-side marshal failure is a genuine error
// and is returned as-is.
func TestSendAllStreamBreak(t *testing.T) {
	// Stream break: strMarshal succeeds, SendMsg returns io.EOF, which Send maps
	// to a *BadStatus -> sendAll swallows it and returns nil.
	broke := newActiveCall(sendErrStream{sendErr: io.EOF}, strMarshal, strUnmarshal, nil)
	if err := sendAll(broke, []any{"a", "b"}); err != nil {
		t.Fatalf("stream break: want nil (defer to drain), got %v", err)
	}

	// Marshal failure: Send returns the raw (non-*BadStatus) error unchanged, so
	// sendAll returns it rather than silently dropping the request.
	badMarshal := newActiveCall(sendErrStream{}, errMarshal, strUnmarshal, nil)
	err := sendAll(badMarshal, []any{"a"})
	if err == nil {
		t.Fatal("marshal failure: want error, got nil")
	}
	var bs *BadStatus
	if errors.As(err, &bs) {
		t.Fatalf("marshal failure should not be a *BadStatus, got %v", err)
	}
}

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

// TestStreamUnimplemented calls a method the server does not register over each
// streaming cardinality. The server rejects the stream as UNIMPLEMENTED and
// half-closes it, so mid-send a Send may lose the race and fail with a transport
// error; the fix makes the streamers break out of the send loop and surface the
// stream's final status from the drain instead of mapping that Send error to
// UNKNOWN. The reported code is therefore deterministically UNIMPLEMENTED (12),
// independent of send/reject timing.
func TestStreamUnimplemented(t *testing.T) {
	stub, _, _ := startEcho(t)
	reqs := make([]any, 64) // many sends widen the window for one to lose the race
	for i := range reqs {
		reqs[i] = "x"
	}

	check := func(name string, err error) {
		t.Helper()
		var bs *BadStatus
		if !errors.As(err, &bs) {
			t.Fatalf("%s: want *BadStatus, got %T: %v", name, err, err)
		}
		if bs.Code != Unimplemented {
			t.Fatalf("%s: want UNIMPLEMENTED(12), got %s(%d)", name, bs.Code, uint32(bs.Code))
		}
	}

	_, err := stub.ClientStreamer("/test.Echo/NoSuchMethod", reqs, opts())
	check("ClientStreamer", err)
	_, err = stub.ServerStreamer("/test.Echo/NoSuchMethod", "x", opts())
	check("ServerStreamer", err)
	_, err = stub.BidiStreamer("/test.Echo/NoSuchMethod", reqs, opts())
	check("BidiStreamer", err)
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

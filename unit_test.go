// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
)

// --- status.go ---

func TestStatusCodeName(t *testing.T) {
	if InvalidArgument.Name() != "INVALID_ARGUMENT" {
		t.Fatalf("Name = %q", InvalidArgument.Name())
	}
	if InvalidArgument.String() != "INVALID_ARGUMENT" {
		t.Fatalf("String = %q", InvalidArgument.String())
	}
	if got := StatusCode(999).Name(); got != "CODE(999)" {
		t.Fatalf("unknown Name = %q", got)
	}
}

func TestBadStatus(t *testing.T) {
	b := NewBadStatus(InvalidArgument, "bad", nil)
	if b.Error() != "3:bad" {
		t.Fatalf("Error = %q", b.Error())
	}
	if b.Metadata == nil {
		t.Fatal("nil metadata not defaulted")
	}
	code, details, md := b.ToStatus()
	if code != InvalidArgument || details != "bad" || md == nil {
		t.Fatalf("ToStatus = %v %q %v", code, details, md)
	}
	// Non-nil metadata is retained.
	b2 := NewBadStatus(NotFound, "x", Metadata{"k": "v"})
	if b2.Metadata["k"] != "v" {
		t.Fatal("metadata dropped")
	}
}

func TestBadStatusFromError(t *testing.T) {
	if badStatusFromError(nil) != nil {
		t.Fatal("nil error should map to nil")
	}
	// A plain (non-status) error becomes UNKNOWN.
	bs := badStatusFromError(errors.New("plain"))
	if bs.Code != Unknown || bs.Details != "plain" {
		t.Fatalf("got %v/%q", bs.Code, bs.Details)
	}
	// A *BadStatus round-trips through gRPC status with code + details intact.
	orig := NewBadStatus(PermissionDenied, "nope", nil)
	bs2 := badStatusFromError(orig.toGRPC())
	if bs2.Code != PermissionDenied || bs2.Details != "nope" {
		t.Fatalf("got %v/%q", bs2.Code, bs2.Details)
	}
}

func TestToGRPCError(t *testing.T) {
	if toGRPCError(nil) != nil {
		t.Fatal("nil should map to nil")
	}
	if err := toGRPCError(NewBadStatus(Aborted, "a", nil)); err == nil {
		t.Fatal("BadStatus should map to error")
	}
	if err := toGRPCError(errors.New("x")); err == nil {
		t.Fatal("plain error should map to error")
	}
}

func TestCallError(t *testing.T) {
	e := NewCallError("boom")
	if e.Error() != "boom" {
		t.Fatalf("Error = %q", e.Error())
	}
}

// --- metadata.go ---

func TestMetadataKeys(t *testing.T) {
	md := Metadata{"b": "2", "a": "1"}
	if got := md.Keys(); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("Keys = %v", got)
	}
}

func TestFromMD(t *testing.T) {
	in := metadata.MD{
		"x-user":       []string{"alice"},
		"content-type": []string{"application/grpc"}, // reserved, dropped
		"empty":        nil,                          // no values, dropped
		":authority":   []string{"h"},                // reserved, dropped
		"grpc-timeout": []string{"1s"},               // reserved, dropped
	}
	out := fromMD(in)
	if len(out) != 1 || out["x-user"] != "alice" {
		t.Fatalf("fromMD = %v", out)
	}
}

func TestIsReservedKey(t *testing.T) {
	for _, k := range []string{"", ":authority", "content-type", "grpc-status", "te"} {
		if !isReservedKey(k) {
			t.Fatalf("%q should be reserved", k)
		}
	}
	if isReservedKey("x-app") {
		t.Fatal("x-app should not be reserved")
	}
}

func TestOutgoingContextEmpty(t *testing.T) {
	ctx := context.Background()
	if (Metadata{}).outgoingContext(ctx) != ctx {
		t.Fatal("empty metadata should return ctx unchanged")
	}
	got := Metadata{"k": "v"}.outgoingContext(ctx)
	md, ok := metadata.FromOutgoingContext(got)
	if !ok || md.Get("k")[0] != "v" {
		t.Fatalf("metadata not attached: %v", md)
	}
}

func TestIncomingMetadataNone(t *testing.T) {
	if got := incomingMetadata(context.Background()); len(got) != 0 {
		t.Fatalf("want empty, got %v", got)
	}
}

func TestLower(t *testing.T) {
	if lower("already") != "already" {
		t.Fatal("no-op case")
	}
	if lower("MixedCASE") != "mixedcase" {
		t.Fatalf("got %q", lower("MixedCASE"))
	}
}

func TestMetadataToMDLowercases(t *testing.T) {
	md := Metadata{"X-Foo": "bar"}.toMD()
	if md.Get("x-foo")[0] != "bar" {
		t.Fatalf("toMD = %v", md)
	}
}

// --- codec.go ---

func TestRawCodec(t *testing.T) {
	c := rawCodec{}
	if c.Name() != "proto" {
		t.Fatalf("Name = %q", c.Name())
	}
	// Marshal accepts rawMessage and []byte, rejects others.
	if b, _ := c.Marshal(rawMessage("hi")); string(b) != "hi" {
		t.Fatal("rawMessage marshal")
	}
	if b, _ := c.Marshal([]byte("yo")); string(b) != "yo" {
		t.Fatal("[]byte marshal")
	}
	if _, err := c.Marshal(42); err == nil {
		t.Fatal("want marshal error for int")
	}
	// Unmarshal into *rawMessage and *[]byte, rejects others.
	var rm rawMessage
	if err := c.Unmarshal([]byte("a"), &rm); err != nil || string(rm) != "a" {
		t.Fatalf("unmarshal rawMessage: %v %q", err, rm)
	}
	var bs []byte
	if err := c.Unmarshal([]byte("b"), &bs); err != nil || string(bs) != "b" {
		t.Fatalf("unmarshal []byte: %v %q", err, bs)
	}
	if err := c.Unmarshal([]byte("c"), 42); err == nil {
		t.Fatal("want unmarshal error for int")
	}
}

// --- transport.go ---

func TestNetTransport(t *testing.T) {
	tr := NetTransport{}
	lis, err := tr.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer lis.Close()

	done := make(chan struct{})
	go func() {
		conn, err := lis.Accept()
		if err == nil {
			_ = conn.Close()
		}
		close(done)
	}()

	conn, err := tr.Dial(context.Background(), lis.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	_ = conn.Close()
	<-done
}

func TestMemTransportErrors(t *testing.T) {
	// Zero-value MemTransport (nil map) must lazily initialise in Listen.
	var zero MemTransport
	if _, err := zero.Listen("z:1"); err != nil {
		t.Fatalf("zero-value Listen: %v", err)
	}

	tr := NewMemTransport()
	if _, err := tr.Listen("a:1"); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Listen("a:1"); err == nil {
		t.Fatal("re-listen should conflict")
	}
	if _, err := tr.Dial(context.Background(), "missing:1"); err == nil {
		t.Fatal("dial to unknown address should fail")
	}
}

func TestMemListenerCloseDeregisters(t *testing.T) {
	tr := NewMemTransport()
	lis, err := tr.Listen("c:1")
	if err != nil {
		t.Fatal(err)
	}
	if err := lis.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// After close the address is free to reuse.
	if _, err := tr.Listen("c:1"); err != nil {
		t.Fatalf("re-listen after close: %v", err)
	}
}

// --- active_call.go ---

// fakeStream is a scriptable grpcStream for unit-testing ActiveCall.
type fakeStream struct {
	sends   []any
	sendErr error
	recvs   [][]byte
	recvErr error
	ctx     context.Context
}

func (f *fakeStream) SendMsg(m any) error {
	if f.sendErr != nil {
		return f.sendErr
	}
	f.sends = append(f.sends, m)
	return nil
}

func (f *fakeStream) RecvMsg(m any) error {
	if f.recvErr != nil {
		return f.recvErr
	}
	if len(f.recvs) == 0 {
		return io.EOF
	}
	next := f.recvs[0]
	f.recvs = f.recvs[1:]
	*m.(*rawMessage) = rawMessage(next)
	return nil
}

func (f *fakeStream) Context() context.Context {
	if f.ctx == nil {
		return context.Background()
	}
	return f.ctx
}

func TestActiveCallSendMarshalError(t *testing.T) {
	call := newActiveCall(&fakeStream{}, func(any) ([]byte, error) {
		return nil, errors.New("marshal fail")
	}, strUnmarshal, nil)
	if err := call.Send("x"); err == nil {
		t.Fatal("want marshal error")
	}
}

func TestActiveCallSendStreamError(t *testing.T) {
	call := newActiveCall(&fakeStream{sendErr: errors.New("send fail")},
		strMarshal, strUnmarshal, nil)
	if err := call.Send("x"); err == nil {
		t.Fatal("want send error")
	}
}

func TestActiveCallReadError(t *testing.T) {
	call := newActiveCall(&fakeStream{recvErr: errors.New("recv fail")},
		strMarshal, strUnmarshal, nil)
	if _, err := call.Read(); err == nil {
		t.Fatal("want recv error")
	}
}

func TestActiveCallEachRemoteReadError(t *testing.T) {
	// fn returns an error -> propagated.
	call := newActiveCall(&fakeStream{recvs: [][]byte{[]byte("a")}},
		strMarshal, strUnmarshal, nil)
	want := errors.New("fn fail")
	if err := call.EachRemoteRead(func(any) error { return want }); err != want {
		t.Fatalf("got %v", err)
	}
	// Read error (not EOF) -> propagated.
	call2 := newActiveCall(&fakeStream{recvErr: errors.New("boom")},
		strMarshal, strUnmarshal, nil)
	if err := call2.EachRemoteRead(func(any) error { return nil }); err == nil {
		t.Fatal("want read error")
	}
}

func TestActiveCallDeadlineAndContext(t *testing.T) {
	deadline := time.Now().Add(time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	call := newActiveCall(&fakeStream{ctx: ctx}, strMarshal, strUnmarshal, nil)
	got, ok := call.Deadline()
	if !ok || !got.Equal(deadline) {
		t.Fatalf("Deadline = %v %v", got, ok)
	}
	if call.Context() != ctx {
		t.Fatal("Context mismatch")
	}
}

// --- service.go: direct handler-closure invocation for the error branches ---

func unaryMethod() Method {
	return Method{
		Name: "M", Type: Unary,
		RequestUnmarshal: strUnmarshal, ResponseMarshal: strMarshal,
		UnaryHandler: func(req any, call *ActiveCall) (any, error) {
			return "ok:" + req.(string), nil
		},
	}
}

func TestUnaryHandlerDecError(t *testing.T) {
	h := unaryMethod().unaryGRPCHandler()
	_, err := h(nil, context.Background(),
		func(any) error { return errors.New("dec fail") }, nil)
	if err == nil {
		t.Fatal("want dec error")
	}
}

func TestUnaryHandlerUnmarshalError(t *testing.T) {
	m := unaryMethod()
	m.RequestUnmarshal = func([]byte) (any, error) { return nil, errors.New("unmarshal") }
	h := m.unaryGRPCHandler()
	_, err := h(nil, context.Background(), decOf("x"), nil)
	if err == nil {
		t.Fatal("want unmarshal error")
	}
}

func TestUnaryHandlerHandlerError(t *testing.T) {
	m := unaryMethod()
	m.UnaryHandler = func(any, *ActiveCall) (any, error) { return nil, NewBadStatus(NotFound, "nf", nil) }
	h := m.unaryGRPCHandler()
	_, err := h(nil, context.Background(), decOf("x"), nil)
	if err == nil {
		t.Fatal("want handler error")
	}
}

func TestUnaryHandlerMarshalError(t *testing.T) {
	m := unaryMethod()
	m.ResponseMarshal = func(any) ([]byte, error) { return nil, errors.New("marshal") }
	h := m.unaryGRPCHandler()
	_, err := h(nil, context.Background(), decOf("x"), nil)
	if err == nil {
		t.Fatal("want marshal error")
	}
}

func TestUnaryHandlerSuccess(t *testing.T) {
	h := unaryMethod().unaryGRPCHandler()
	out, err := h(nil, context.Background(), decOf("bob"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.(rawMessage)) != "ok:bob" {
		t.Fatalf("got %q", out)
	}
}

// decOf returns a dec func that decodes s into the target *rawMessage.
func decOf(s string) func(any) error {
	return func(v any) error {
		*v.(*rawMessage) = rawMessage(s)
		return nil
	}
}

func TestStreamHandlerClientStreamError(t *testing.T) {
	m := Method{
		Name: "CS", Type: ClientStream,
		RequestUnmarshal: strUnmarshal, ResponseMarshal: strMarshal,
		ClientStreamHandler: func(*ActiveCall) (any, error) {
			return nil, NewBadStatus(Cancelled, "cancel", nil)
		},
	}
	h := m.streamGRPCHandler()
	if err := h(nil, &fakeServerStream{}); err == nil {
		t.Fatal("want client-stream handler error")
	}
}

func TestStreamHandlerServerStreamNoRequest(t *testing.T) {
	m := Method{
		Name: "SS", Type: ServerStream,
		RequestUnmarshal: strUnmarshal, ResponseMarshal: strMarshal,
		ServerStreamHandler: func(any, *ActiveCall) error { return nil },
	}
	h := m.streamGRPCHandler()
	// Immediate EOF -> "no request received" CallError.
	if err := h(nil, &fakeServerStream{eof: true}); err == nil {
		t.Fatal("want no-request error")
	}
}

func TestStreamHandlerServerStreamRecvError(t *testing.T) {
	m := Method{
		Name: "SS", Type: ServerStream,
		RequestUnmarshal: strUnmarshal, ResponseMarshal: strMarshal,
		ServerStreamHandler: func(any, *ActiveCall) error { return nil },
	}
	h := m.streamGRPCHandler()
	if err := h(nil, &fakeServerStream{recvErr: errors.New("boom")}); err == nil {
		t.Fatal("want recv error")
	}
}

// fakeServerStream adapts fakeStream to grpc.ServerStream for streamGRPCHandler.
type fakeServerStream struct {
	fakeStream
	eof     bool
	recvErr error
}

func (f *fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeServerStream) SetTrailer(metadata.MD)       {}
func (f *fakeServerStream) RecvMsg(m any) error {
	if f.recvErr != nil {
		return f.recvErr
	}
	if f.eof {
		return io.EOF
	}
	*m.(*rawMessage) = rawMessage("req")
	return nil
}

// --- server.go: Run guard branches ---

func TestRunNoPort(t *testing.T) {
	srv := NewRpcServer(WithTransport(NewMemTransport()))
	if err := srv.Run(); err == nil {
		t.Fatal("want no-port error")
	}
}

func TestRunAlreadyRunning(t *testing.T) {
	tr := NewMemTransport()
	srv := NewRpcServer(WithTransport(tr))
	srv.AddHTTP2Port("run:1", ":insecure")
	go func() { _ = srv.Run() }()
	waitRunning(t, srv)
	defer srv.Stop()
	if err := srv.Run(); err == nil {
		t.Fatal("second Run should error")
	}
}

func TestRunListenErrorCleanup(t *testing.T) {
	tr := NewMemTransport()
	// Occupy the second address so the server's second Listen fails and the
	// first listener must be cleaned up.
	if _, err := tr.Listen("busy:2"); err != nil {
		t.Fatal(err)
	}
	srv := NewRpcServer(WithTransport(tr))
	srv.AddHTTP2Port("free:1", ":insecure")
	srv.AddHTTP2Port("busy:2", ":insecure")
	if err := srv.Run(); err == nil {
		t.Fatal("Run should fail on the busy port")
	}
	if srv.Running() {
		t.Fatal("server should not be running after a failed Run")
	}
}

func TestRunTillTerminated(t *testing.T) {
	tr := NewMemTransport()
	srv := NewRpcServer(WithTransport(tr))
	srv.AddHTTP2Port("rtt:1", ":insecure")
	done := make(chan error, 1)
	go func() { done <- srv.RunTillTerminated() }()
	waitRunning(t, srv)
	srv.Stop()
	srv.Stop() // idempotent
	if err := <-done; err != nil {
		t.Fatalf("RunTillTerminated = %v", err)
	}
	if srv.Running() {
		t.Fatal("Running should be false after Stop")
	}
}

func TestDefaultTransports(t *testing.T) {
	// NewRpcServer / NewClientStub without a transport default to NetTransport.
	srv := NewRpcServer()
	if _, ok := srv.transport.(NetTransport); !ok {
		t.Fatalf("server transport = %T", srv.transport)
	}
	stub, err := NewClientStub("127.0.0.1:1", ":insecure")
	if err != nil {
		t.Fatalf("NewClientStub: %v", err)
	}
	_ = stub.Close()
}

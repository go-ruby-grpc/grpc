// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"context"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ClientStub mirrors GRPC::ClientStub: a handle to a remote service over which
// typed RPC helpers (request_response, client_streamer, server_streamer,
// bidi_streamer) are issued. The connection is dialed through the injected
// [Transport], so the stub is exercisable in-process over [MemTransport].
type ClientStub struct {
	host    string
	cc      *grpc.ClientConn
	timeout time.Duration
}

// StubOption configures a ClientStub at construction.
type StubOption func(*stubConfig)

type stubConfig struct {
	transport Transport
	timeout   time.Duration
}

// WithStubTransport injects the network seam for the stub. Without it the stub
// dials via [NetTransport].
func WithStubTransport(t Transport) StubOption {
	return func(c *stubConfig) { c.transport = t }
}

// WithTimeout sets a default per-call deadline, mirroring the gem's
// ClientStub.new(..., timeout:). CallOptions.Deadline overrides it per call.
func WithTimeout(d time.Duration) StubOption {
	return func(c *stubConfig) { c.timeout = d }
}

// NewClientStub mirrors GRPC::ClientStub.new(host, creds). The creds argument is
// accepted for surface fidelity (pass ":this_channel_is_insecure" for plaintext,
// as the gem does); TLS is a follow-up. It dials host through the transport.
func NewClientStub(host, creds string, opts ...StubOption) (*ClientStub, error) {
	cfg := &stubConfig{}
	for _, o := range opts {
		o(cfg)
	}
	if cfg.transport == nil {
		cfg.transport = NetTransport{}
	}
	// The "passthrough" scheme hands host straight to the injected dialer
	// instead of running it through the DNS resolver, which is what makes a
	// non-resolvable in-memory address (or any transport-defined name) work.
	cc, err := grpc.NewClient("passthrough:///"+host,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(cfg.transport.Dial),
	)
	if err != nil {
		return nil, badStatusFromError(err)
	}
	return &ClientStub{host: host, cc: cc, timeout: cfg.timeout}, nil
}

// Close releases the stub's connection.
func (s *ClientStub) Close() error { return s.cc.Close() }

// CallOptions carries the per-call knobs the gem's stub methods accept: the
// message (un)marshalling, request metadata (a Hash) and a deadline.
type CallOptions struct {
	Marshal   Marshaler
	Unmarshal Unmarshaler
	Metadata  Metadata
	Deadline  time.Time
}

// callContext builds the context for a call, applying the deadline (explicit,
// else the stub default) and outgoing metadata. It returns a cancel func the
// caller must invoke.
func (s *ClientStub) callContext(opts CallOptions) (context.Context, context.CancelFunc) {
	ctx := context.Background()
	cancel := context.CancelFunc(func() {})
	if !opts.Deadline.IsZero() {
		ctx, cancel = context.WithDeadline(ctx, opts.Deadline)
	} else if s.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
	}
	ctx = opts.Metadata.outgoingContext(ctx)
	return ctx, cancel
}

// RequestResponse mirrors GRPC::ClientStub#request_response: a unary call. It
// marshals req, invokes method (e.g. "/helloworld.Greeter/SayHello"), and
// returns the unmarshalled response. A non-OK status is returned as *BadStatus.
func (s *ClientStub) RequestResponse(method string, req any, opts CallOptions) (any, error) {
	ctx, cancel := s.callContext(opts)
	defer cancel()

	b, err := opts.Marshal(req)
	if err != nil {
		return nil, err
	}
	var reply rawMessage
	if err := s.cc.Invoke(ctx, method, rawMessage(b), &reply, grpc.ForceCodec(rawCodec{})); err != nil {
		return nil, badStatusFromError(err)
	}
	return opts.Unmarshal(reply)
}

// newStream opens a client stream for method with the given cardinality.
func (s *ClientStub) newStream(ctx context.Context, method string, clientStreams, serverStreams bool) (grpc.ClientStream, error) {
	desc := &grpc.StreamDesc{
		StreamName:    method,
		ClientStreams: clientStreams,
		ServerStreams: serverStreams,
	}
	st, err := s.cc.NewStream(ctx, desc, method, grpc.ForceCodec(rawCodec{}))
	if err != nil {
		return nil, badStatusFromError(err)
	}
	return st, nil
}

// ClientStreamer mirrors GRPC::ClientStub#client_streamer: it streams every
// request to method and returns the single response.
func (s *ClientStub) ClientStreamer(method string, requests []any, opts CallOptions) (any, error) {
	ctx, cancel := s.callContext(opts)
	defer cancel()

	st, err := s.newStream(ctx, method, true, false)
	if err != nil {
		return nil, err
	}
	call := newActiveCall(st, opts.Marshal, opts.Unmarshal, nil)
	for _, req := range requests {
		if err := call.Send(req); err != nil {
			return nil, err
		}
	}
	// Best-effort half-close; a genuine transport failure surfaces on the
	// subsequent Read/drain as a *BadStatus.
	_ = st.CloseSend()
	return call.Read()
}

// ServerStreamer mirrors GRPC::ClientStub#server_streamer: it sends one request
// and returns every response the server streams back.
func (s *ClientStub) ServerStreamer(method string, req any, opts CallOptions) ([]any, error) {
	ctx, cancel := s.callContext(opts)
	defer cancel()

	st, err := s.newStream(ctx, method, false, true)
	if err != nil {
		return nil, err
	}
	call := newActiveCall(st, opts.Marshal, opts.Unmarshal, nil)
	if err := call.Send(req); err != nil {
		return nil, err
	}
	// Best-effort half-close; a genuine transport failure surfaces on the
	// subsequent Read/drain as a *BadStatus.
	_ = st.CloseSend()
	return drain(call)
}

// BidiStreamer mirrors GRPC::ClientStub#bidi_streamer: it streams every request
// and returns every response. Requests are sent first, then responses drained;
// over a buffered transport this exercises full-duplex handlers.
func (s *ClientStub) BidiStreamer(method string, requests []any, opts CallOptions) ([]any, error) {
	ctx, cancel := s.callContext(opts)
	defer cancel()

	st, err := s.newStream(ctx, method, true, true)
	if err != nil {
		return nil, err
	}
	call := newActiveCall(st, opts.Marshal, opts.Unmarshal, nil)
	for _, req := range requests {
		if err := call.Send(req); err != nil {
			return nil, err
		}
	}
	// Best-effort half-close; a genuine transport failure surfaces on the
	// subsequent Read/drain as a *BadStatus.
	_ = st.CloseSend()
	return drain(call)
}

// drain reads an ActiveCall to EOF, collecting every response message.
func drain(call *ActiveCall) ([]any, error) {
	var out []any
	for {
		msg, err := call.Read()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
}

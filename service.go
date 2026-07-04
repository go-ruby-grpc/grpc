// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"context"
	"io"

	"google.golang.org/grpc"
)

// MethodType is the cardinality of an RPC, mirroring the four shapes a
// GRPC::RpcDesc can take.
type MethodType int

const (
	// Unary is a single request, single response (request_response).
	Unary MethodType = iota
	// ClientStream is a stream of requests, single response (client_streamer).
	ClientStream
	// ServerStream is a single request, stream of responses (server_streamer).
	ServerStream
	// BidiStream is a stream of requests and a stream of responses
	// (bidi_streamer).
	BidiStream
)

// Method describes one RPC of a service, mirroring a single GRPC::RpcDesc: its
// name, cardinality, the request/response (un)marshalling, and the handler.
// Exactly one handler field is used, selected by Type.
type Method struct {
	// Name is the RPC method name as it appears on the wire, e.g. "SayHello".
	Name string
	// Type selects the cardinality and which handler field is invoked.
	Type MethodType
	// RequestUnmarshal decodes an incoming request message.
	RequestUnmarshal Unmarshaler
	// ResponseMarshal encodes an outgoing response message.
	ResponseMarshal Marshaler

	// UnaryHandler serves a Unary method: one request in, one response out.
	UnaryHandler func(req any, call *ActiveCall) (any, error)
	// ClientStreamHandler serves a ClientStream method: it reads requests from
	// call (via Read / EachRemoteRead) and returns the single response.
	ClientStreamHandler func(call *ActiveCall) (any, error)
	// ServerStreamHandler serves a ServerStream method: it receives the single
	// request and emits responses with call.Send.
	ServerStreamHandler func(req any, call *ActiveCall) error
	// BidiStreamHandler serves a BidiStream method: it reads requests and emits
	// responses over the same call.
	BidiStreamHandler func(call *ActiveCall) error
}

// Service describes a gRPC service to register on an RpcServer, mirroring the
// service object passed to GRPC::RpcServer#handle. Name is the fully-qualified
// service name (e.g. "helloworld.Greeter").
type Service struct {
	Name    string
	Methods []Method
}

// fullMethod returns the "/service/method" path for an RPC.
func fullMethod(service, method string) string {
	return "/" + service + "/" + method
}

// toGRPCServiceDesc converts a Service into a google.golang.org/grpc ServiceDesc
// so the real gRPC runtime can dispatch to it. Unary methods become MethodDescs;
// streaming methods become StreamDescs.
func (s Service) toGRPCServiceDesc() grpc.ServiceDesc {
	desc := grpc.ServiceDesc{
		ServiceName: s.Name,
		HandlerType: (*any)(nil),
	}
	for _, m := range s.Methods {
		m := m
		switch m.Type {
		case Unary:
			desc.Methods = append(desc.Methods, grpc.MethodDesc{
				MethodName: m.Name,
				Handler:    m.unaryGRPCHandler(),
			})
		default:
			desc.Streams = append(desc.Streams, grpc.StreamDesc{
				StreamName:    m.Name,
				Handler:       m.streamGRPCHandler(),
				ServerStreams: m.Type == ServerStream || m.Type == BidiStream,
				ClientStreams: m.Type == ClientStream || m.Type == BidiStream,
			})
		}
	}
	return desc
}

// unaryGRPCHandler builds the grpc unary handler for a Unary method.
func (m Method) unaryGRPCHandler() func(any, context.Context, func(any) error, grpc.UnaryServerInterceptor) (any, error) {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		var rm rawMessage
		if err := dec(&rm); err != nil {
			return nil, err
		}
		req, err := m.RequestUnmarshal(rm)
		if err != nil {
			return nil, toGRPCError(err)
		}
		call := newActiveCall(ctxOnlyStream{ctx: ctx}, m.ResponseMarshal, m.RequestUnmarshal, incomingMetadata(ctx))
		resp, err := m.UnaryHandler(req, call)
		if err != nil {
			return nil, toGRPCError(err)
		}
		b, err := m.ResponseMarshal(resp)
		if err != nil {
			return nil, toGRPCError(err)
		}
		return rawMessage(b), nil
	}
}

// streamGRPCHandler builds the grpc stream handler for a streaming method.
func (m Method) streamGRPCHandler() func(any, grpc.ServerStream) error {
	return func(srv any, stream grpc.ServerStream) error {
		ctx := stream.Context()
		call := newActiveCall(stream, m.ResponseMarshal, m.RequestUnmarshal, incomingMetadata(ctx))
		switch m.Type {
		case ClientStream:
			resp, err := m.ClientStreamHandler(call)
			if err != nil {
				return toGRPCError(err)
			}
			return toGRPCError(call.Send(resp))
		case ServerStream:
			req, err := call.Read()
			if err != nil {
				if err == io.EOF {
					return toGRPCError(NewCallError("server_streamer: no request received"))
				}
				return toGRPCError(err)
			}
			return toGRPCError(m.ServerStreamHandler(req, call))
		default: // BidiStream
			return toGRPCError(m.BidiStreamHandler(call))
		}
	}
}

// ctxOnlyStream is the minimal grpcStream a unary handler's ActiveCall wraps:
// it exposes the context (for metadata and deadline) but rejects streaming
// Send/Read, exactly as calling send on a unary call raises in the gem.
type ctxOnlyStream struct{ ctx context.Context }

func (s ctxOnlyStream) SendMsg(any) error        { return NewCallError("send on a unary call") }
func (s ctxOnlyStream) RecvMsg(any) error        { return NewCallError("read on a unary call") }
func (s ctxOnlyStream) Context() context.Context { return s.ctx }

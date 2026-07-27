// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

// This file ports the generated-service layer of the grpc gem: the
// GRPC::GenericService mixin that a protoc-generated *_services_pb.rb Service
// base class includes, and the Stub class that GenericService.rpc_stub_class
// derives from it. Together with [GenerateRubyServices] (the
// grpc_tools_ruby_protoc equivalent, in codegen.go) this closes the service
// codegen surface: a .proto's service block yields a GenericService that binds
// straight onto [RpcServer] (server) and [ClientStub] (client), exactly as the
// gem's generated code does.

// RpcDesc mirrors GRPC::RpcDesc: one declared RPC of a service — its wire name,
// cardinality, and the marshal/unmarshal functions for the request and response
// messages. In the gem a RpcDesc carries the marshal/unmarshal *procs* a message
// class' marshal_class_method / unmarshal_class_method yield; here, staying
// message-agnostic like the gem, it carries the equivalent [Marshaler] /
// [Unmarshaler] functions. Messages from github.com/go-ruby-protobuf/protobuf
// drop straight in via its Encode / Decode.
type RpcDesc struct {
	// Name is the RPC method name as declared and as it appears on the wire,
	// e.g. "SayHello".
	Name string
	// Type is the cardinality of the RPC.
	Type MethodType
	// RequestMarshal encodes a request message (used by the client stub).
	RequestMarshal Marshaler
	// RequestUnmarshal decodes a request message (used by the server).
	RequestUnmarshal Unmarshaler
	// ResponseMarshal encodes a response message (used by the server).
	ResponseMarshal Marshaler
	// ResponseUnmarshal decodes a response message (used by the client stub).
	ResponseUnmarshal Unmarshaler
}

// GenericService mirrors the GRPC::GenericService mixin a generated
// *_services_pb.rb Service base class includes: it names the service (the gem's
// self.service_name) and collects the RPC declarations (the gem's `rpc`
// class-macro calls). From it, [GenericService.BuildService] derives the
// server-side [Service] to register on an [RpcServer], and
// [GenericService.StubClass] derives the client-side stub — the gem's
// rpc_stub_class.
type GenericService struct {
	serviceName string
	descs       []RpcDesc
	index       map[string]RpcDesc
}

// NewGenericService builds a GenericService for the fully-qualified service name
// (e.g. "helloworld.Greeter"), mirroring a generated Service base whose
// self.service_name is set to that name.
func NewGenericService(serviceName string) *GenericService {
	return &GenericService{serviceName: serviceName, index: map[string]RpcDesc{}}
}

// ServiceName returns the fully-qualified service name, mirroring the gem's
// GenericService.service_name.
func (g *GenericService) ServiceName() string { return g.serviceName }

// RPC declares one RPC on the service, mirroring the gem's
// `rpc :Name, Input, Output` class macro. It is chainable so a generated
// service reads as a sequence of RPC declarations. A duplicate name replaces the
// earlier declaration, as re-declaring an rpc does in the gem.
func (g *GenericService) RPC(d RpcDesc) *GenericService {
	if _, dup := g.index[d.Name]; !dup {
		g.descs = append(g.descs, d)
	} else {
		for i := range g.descs {
			if g.descs[i].Name == d.Name {
				g.descs[i] = d
				break
			}
		}
	}
	g.index[d.Name] = d
	return g
}

// RpcDescs returns the declared RPCs in declaration order, mirroring the gem's
// GenericService.rpc_descs.
func (g *GenericService) RpcDescs() []RpcDesc {
	out := make([]RpcDesc, len(g.descs))
	copy(out, g.descs)
	return out
}

// LookupRPC returns the RpcDesc for name and whether it was declared.
func (g *GenericService) LookupRPC(name string) (RpcDesc, bool) {
	d, ok := g.index[name]
	return d, ok
}

// methodPath returns the wire path "/<service_name>/<rpc>" for an RPC.
func (g *GenericService) methodPath(rpc string) string {
	return "/" + g.serviceName + "/" + rpc
}

// Handlers binds each declared RPC name to its handler implementation. Each
// value must be the handler func matching the RPC's cardinality — the same four
// shapes [Method] accepts:
//
//	Unary        func(req any, call *ActiveCall) (any, error)
//	ClientStream func(call *ActiveCall) (any, error)
//	ServerStream func(req any, call *ActiveCall) error
//	BidiStream   func(call *ActiveCall) error
//
// This mirrors defining the instance methods of a generated Service subclass.
type Handlers map[string]any

// BuildService pairs each declared RpcDesc with its handler from h and returns
// the runtime [Service] to register on an [RpcServer] with Handle. It mirrors
// implementing a generated Service subclass and passing an instance to
// GRPC::RpcServer#handle. It errors if the service declares no RPCs, if a
// declared RPC has no handler, or if a handler's Go type does not match the
// RPC's cardinality.
func (g *GenericService) BuildService(h Handlers) (Service, error) {
	if len(g.descs) == 0 {
		return Service{}, NewCallError("grpc: service " + g.serviceName + " declares no rpcs")
	}
	svc := Service{Name: g.serviceName}
	for _, d := range g.descs {
		handler, ok := h[d.Name]
		if !ok {
			return Service{}, NewCallError("grpc: no handler for rpc " + d.Name)
		}
		m := Method{
			Name:             d.Name,
			Type:             d.Type,
			RequestUnmarshal: d.RequestUnmarshal,
			ResponseMarshal:  d.ResponseMarshal,
		}
		switch d.Type {
		case Unary:
			fn, ok := handler.(func(req any, call *ActiveCall) (any, error))
			if !ok {
				return Service{}, wrongHandler(d)
			}
			m.UnaryHandler = fn
		case ClientStream:
			fn, ok := handler.(func(call *ActiveCall) (any, error))
			if !ok {
				return Service{}, wrongHandler(d)
			}
			m.ClientStreamHandler = fn
		case ServerStream:
			fn, ok := handler.(func(req any, call *ActiveCall) error)
			if !ok {
				return Service{}, wrongHandler(d)
			}
			m.ServerStreamHandler = fn
		case BidiStream:
			fn, ok := handler.(func(call *ActiveCall) error)
			if !ok {
				return Service{}, wrongHandler(d)
			}
			m.BidiStreamHandler = fn
		default:
			return Service{}, NewCallError("grpc: rpc " + d.Name + " has an unknown cardinality")
		}
		svc.Methods = append(svc.Methods, m)
	}
	return svc, nil
}

// wrongHandler builds the error returned when a handler's type does not match
// the RPC's declared cardinality.
func wrongHandler(d RpcDesc) error {
	return NewCallError("grpc: handler for rpc " + d.Name + " has the wrong shape for its cardinality")
}

// GenericStub mirrors the Stub class GenericService.rpc_stub_class generates: it
// wraps a [ClientStub] and, for each declared RPC, issues the call with the
// right cardinality and the descriptor's own marshal/unmarshal already applied,
// so the caller supplies only the request(s) and optional metadata/deadline —
// exactly the ergonomics of a generated Stub#say_hello(req).
type GenericStub struct {
	stub *ClientStub
	svc  *GenericService
}

// StubClass derives the client stub over an existing [ClientStub], mirroring
// `Stub = Service.rpc_stub_class` followed by `Stub.new(host, creds)`.
func (g *GenericService) StubClass(stub *ClientStub) *GenericStub {
	return &GenericStub{stub: stub, svc: g}
}

// descFor looks up rpc and verifies it has the wanted cardinality, returning the
// descriptor with its codec ready to apply to a [CallOptions].
func (s *GenericStub) descFor(rpc string, want MethodType) (RpcDesc, error) {
	d, ok := s.svc.LookupRPC(rpc)
	if !ok {
		return RpcDesc{}, NewCallError("grpc: unknown rpc " + rpc)
	}
	if d.Type != want {
		return RpcDesc{}, NewCallError("grpc: rpc " + rpc + " is not " + methodTypeName(want))
	}
	return d, nil
}

// withCodec fills opts with the descriptor's request-marshal / response-unmarshal
// (the client-side codec direction), preserving the caller's metadata and
// deadline. The generated stub carries the codec so the caller never repeats it.
func withCodec(d RpcDesc, opts CallOptions) CallOptions {
	opts.Marshal = d.RequestMarshal
	opts.Unmarshal = d.ResponseUnmarshal
	return opts
}

// RequestResponse issues the unary RPC named rpc, mirroring a generated unary
// stub method. It errors if rpc is unknown or is not a Unary RPC.
func (s *GenericStub) RequestResponse(rpc string, req any, opts CallOptions) (any, error) {
	d, err := s.descFor(rpc, Unary)
	if err != nil {
		return nil, err
	}
	return s.stub.RequestResponse(s.svc.methodPath(rpc), req, withCodec(d, opts))
}

// ClientStreamer issues the client-streaming RPC named rpc, mirroring a
// generated client-streaming stub method. It errors if rpc is unknown or is not
// a ClientStream RPC.
func (s *GenericStub) ClientStreamer(rpc string, requests []any, opts CallOptions) (any, error) {
	d, err := s.descFor(rpc, ClientStream)
	if err != nil {
		return nil, err
	}
	return s.stub.ClientStreamer(s.svc.methodPath(rpc), requests, withCodec(d, opts))
}

// ServerStreamer issues the server-streaming RPC named rpc, mirroring a
// generated server-streaming stub method. It errors if rpc is unknown or is not
// a ServerStream RPC.
func (s *GenericStub) ServerStreamer(rpc string, req any, opts CallOptions) ([]any, error) {
	d, err := s.descFor(rpc, ServerStream)
	if err != nil {
		return nil, err
	}
	return s.stub.ServerStreamer(s.svc.methodPath(rpc), req, withCodec(d, opts))
}

// BidiStreamer issues the bidirectional-streaming RPC named rpc, mirroring a
// generated bidi stub method. It errors if rpc is unknown or is not a BidiStream
// RPC.
func (s *GenericStub) BidiStreamer(rpc string, requests []any, opts CallOptions) ([]any, error) {
	d, err := s.descFor(rpc, BidiStream)
	if err != nil {
		return nil, err
	}
	return s.stub.BidiStreamer(s.svc.methodPath(rpc), requests, withCodec(d, opts))
}

// methodTypeName renders a cardinality as the gem's stub-method name, used in
// mismatch errors.
func methodTypeName(t MethodType) string {
	switch t {
	case Unary:
		return "request_response"
	case ClientStream:
		return "client_streamer"
	case ServerStream:
		return "server_streamer"
	case BidiStream:
		return "bidi_streamer"
	default:
		return "unknown"
	}
}

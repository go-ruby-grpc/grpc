// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

// Package grpc is a pure-Go (CGO-free), MRI-faithful reimplementation of the
// surface of Ruby's grpc gem — the object model a Ruby program sees as the GRPC
// namespace — without any Ruby runtime and without the gem's C extension
// (upstream grpc ships as a heavy C extension around the gRPC-core C library).
//
// It does not reimplement HTTP/2 or the gRPC wire protocol: it is a
// Ruby-faithful API layer built on top of google.golang.org/grpc and
// google.golang.org/protobuf, the official pure-Go gRPC and protobuf runtimes.
// Every byte on the wire is therefore produced by the canonical Go gRPC stack,
// so a server built here interoperates with any conformant gRPC peer and a
// stub built here can call any conformant gRPC server, by construction.
//
// # The network seam
//
// The library never hardwires a socket into its core logic. Both the server's
// listener and the client's dialer are obtained from an injected [Transport].
// [NetTransport] is the production transport (real TCP sockets); [MemTransport]
// is an in-process, bufconn-backed transport that carries a real HTTP/2 gRPC
// session over an in-memory pipe, so the whole stack is exercised end-to-end in
// tests without binding a port. This mirrors how the OIDC/OAuth2 bindings treat
// the HTTP round-trip as a host seam.
//
// # Mapping to the gem
//
//	Ruby (grpc)                           Go (this package)
//	-----------                           -----------------
//	GRPC::RpcServer.new                   NewRpcServer(...)
//	  #add_http2_port(addr, creds)          (*RpcServer).AddHTTP2Port
//	  #handle(service)                      (*RpcServer).Handle
//	  #run / #run_till_terminated           (*RpcServer).Run / RunTillTerminated
//	  #stop                                 (*RpcServer).Stop
//	GRPC::ClientStub.new(host, creds)     NewClientStub(...)
//	  #request_response                     (*ClientStub).RequestResponse
//	  #client_streamer                      (*ClientStub).ClientStreamer
//	  #server_streamer                      (*ClientStub).ServerStreamer
//	  #bidi_streamer                        (*ClientStub).BidiStreamer
//	GRPC::ActiveCall                      *ActiveCall
//	GRPC::Core::StatusCodes               the StatusCode constants (OK, …)
//	GRPC::BadStatus                       *BadStatus
//	GRPC::Core::CallError                 *CallError
//	metadata (a Hash)                     Metadata (map[string]string)
//
// # Messages
//
// Like the gem, this package is message-agnostic: a call carries a marshal and
// an unmarshal function, exactly as GRPC::RpcDesc carries the marshal/unmarshal
// procs a generated *_pb.rb / *_services_pb.rb attaches. Messages produced by
// github.com/go-ruby-protobuf/protobuf (the pure-Go google-protobuf gem) drop
// straight in via its Encode/Decode.
package grpc

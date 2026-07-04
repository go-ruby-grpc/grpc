// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"context"
	"io"
	"time"
)

// Marshaler serializes a message to bytes. It plays the role of the marshal
// proc a generated *_services_pb.rb attaches to each GRPC::RpcDesc.
type Marshaler func(any) ([]byte, error)

// Unmarshaler deserializes bytes into a message, mirroring the unmarshal proc a
// generated *_services_pb.rb attaches to each GRPC::RpcDesc.
type Unmarshaler func([]byte) (any, error)

// grpcStream is the subset of google.golang.org/grpc's ServerStream and
// ClientStream that ActiveCall drives. Both implement it, so one ActiveCall type
// serves both directions.
type grpcStream interface {
	SendMsg(m any) error
	RecvMsg(m any) error
	Context() context.Context
}

// ActiveCall mirrors GRPC::ActiveCall: the object a handler or a streaming stub
// uses to move messages and metadata over a single call. It applies the call's
// marshal/unmarshal functions so Send/Read speak in messages, not bytes.
//
// The marshal/unmarshal directions are set by whichever side constructs it: on
// the server Send marshals responses and Read unmarshals requests; on the client
// it is the reverse.
type ActiveCall struct {
	stream    grpcStream
	marshal   Marshaler
	unmarshal Unmarshaler
	md        Metadata
}

// newActiveCall wires an ActiveCall to a stream with the given directions.
func newActiveCall(s grpcStream, marshal Marshaler, unmarshal Unmarshaler, md Metadata) *ActiveCall {
	if md == nil {
		md = Metadata{}
	}
	return &ActiveCall{stream: s, marshal: marshal, unmarshal: unmarshal, md: md}
}

// Send marshals msg and writes it to the call. It mirrors ActiveCall#send /
// the block-yield a streaming handler uses to emit a response.
func (c *ActiveCall) Send(msg any) error {
	b, err := c.marshal(msg)
	if err != nil {
		return err
	}
	if err := c.stream.SendMsg(rawMessage(b)); err != nil {
		return badStatusFromError(err)
	}
	return nil
}

// Read receives the next message and unmarshals it, mirroring
// ActiveCall#remote_read. It returns io.EOF when the peer half-closes, so a
// handler can range over the request stream until EOF.
func (c *ActiveCall) Read() (any, error) {
	var rm rawMessage
	if err := c.stream.RecvMsg(&rm); err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, badStatusFromError(err)
	}
	return c.unmarshal(rm)
}

// EachRemoteRead calls fn for every message the peer sends until the stream is
// half-closed, mirroring ActiveCall#each_remote_read. A non-nil error from fn or
// from the transport stops the iteration and is returned.
func (c *ActiveCall) EachRemoteRead(fn func(msg any) error) error {
	for {
		msg, err := c.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(msg); err != nil {
			return err
		}
	}
}

// Metadata returns the request metadata the peer sent with the call, mirroring
// ActiveCall#metadata (a Hash).
func (c *ActiveCall) Metadata() Metadata { return c.md }

// Deadline returns the call deadline and whether one was set, mirroring
// ActiveCall#deadline.
func (c *ActiveCall) Deadline() (time.Time, bool) {
	return c.stream.Context().Deadline()
}

// Context exposes the underlying call context for advanced callers.
func (c *ActiveCall) Context() context.Context { return c.stream.Context() }

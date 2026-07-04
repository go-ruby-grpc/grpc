// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import "fmt"

// rawCodec is a passthrough gRPC codec that carries already-serialized message
// bytes. The gem is message-agnostic — a call marshals with a proc and the C
// core just moves the resulting String across the wire — so this package does
// the same: the Marshal/Unmarshal functions attached to a call produce and
// consume []byte, and this codec moves those bytes verbatim.
//
// Its Name is "proto" so that on the wire the content-subtype is the default
// "application/grpc+proto". A conformant peer using generated protobuf stubs
// therefore interoperates transparently: to it these are ordinary protobuf
// messages; to us they are opaque bytes produced by the caller's marshal
// function (which, for real messages, is protobuf serialization and so emits
// the very same bytes).
type rawCodec struct{}

// rawMessage is the concrete type this codec speaks: a byte slice.
type rawMessage []byte

// Name reports the codec name; "proto" keeps the default content-subtype.
func (rawCodec) Name() string { return "proto" }

// Marshal returns the bytes as-is. It accepts a rawMessage or a []byte.
func (rawCodec) Marshal(v any) ([]byte, error) {
	switch m := v.(type) {
	case rawMessage:
		return []byte(m), nil
	case []byte:
		return m, nil
	default:
		return nil, fmt.Errorf("grpc: rawCodec cannot marshal %T", v)
	}
}

// Unmarshal copies the wire bytes into the destination, which must be a
// *rawMessage or *[]byte.
func (rawCodec) Unmarshal(data []byte, v any) error {
	switch p := v.(type) {
	case *rawMessage:
		b := make(rawMessage, len(data))
		copy(b, data)
		*p = b
		return nil
	case *[]byte:
		b := make([]byte, len(data))
		copy(b, data)
		*p = b
		return nil
	default:
		return fmt.Errorf("grpc: rawCodec cannot unmarshal into %T", v)
	}
}

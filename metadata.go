// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"context"
	"sort"

	"google.golang.org/grpc/metadata"
)

// Metadata mirrors the gem's representation of call metadata: a plain Hash of
// string keys to string values. In the gem a handler reads request metadata
// from ActiveCall#metadata and sends response metadata with a Hash; this type
// is that Hash.
//
// gRPC keys are case-insensitive on the wire and the runtime lower-cases them;
// this type follows suit so a key set as "Foo" reads back as "foo", matching the
// gem's observed behaviour.
type Metadata map[string]string

// Keys returns the metadata keys in sorted order — handy for deterministic
// iteration and tests.
func (m Metadata) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// toMD converts to a google.golang.org/grpc metadata.MD for transmission. Keys
// are lower-cased by the runtime; we lower-case here for a stable round-trip.
func (m Metadata) toMD() metadata.MD {
	md := metadata.MD{}
	for k, v := range m {
		md.Set(lower(k), v)
	}
	return md
}

// fromMD collapses a metadata.MD (whose values are []string) into the gem's
// single-value Hash shape, taking the first value for each key. The gRPC
// runtime injects some reserved headers (":authority", "content-type",
// "user-agent", "grpc-*"); those are internal transport plumbing, not
// application metadata, so they are dropped to match what a gem handler sees.
func fromMD(md metadata.MD) Metadata {
	out := Metadata{}
	for k, vs := range md {
		if len(vs) == 0 || isReservedKey(k) {
			continue
		}
		out[k] = vs[0]
	}
	return out
}

// isReservedKey reports whether a header is transport plumbing rather than
// application metadata.
func isReservedKey(k string) bool {
	if k == "" {
		return true
	}
	if k[0] == ':' {
		return true
	}
	switch k {
	case "content-type", "user-agent", "te", "grpc-encoding",
		"grpc-accept-encoding", "accept-encoding":
		return true
	}
	// grpc-timeout, grpc-status, grpc-message, etc.
	if len(k) >= 5 && k[:5] == "grpc-" {
		return true
	}
	return false
}

// outgoingContext attaches metadata to a client context so it rides the request
// as gRPC headers.
func (m Metadata) outgoingContext(ctx context.Context) context.Context {
	if len(m) == 0 {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, m.toMD())
}

// incomingMetadata reads the request metadata a server handler received.
func incomingMetadata(ctx context.Context) Metadata {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Metadata{}
	}
	return fromMD(md)
}

// lower lower-cases an ASCII header key without allocating for the common
// already-lower case.
func lower(s string) string {
	needs := false
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= 'A' && c <= 'Z' {
			needs = true
			break
		}
	}
	if !needs {
		return s
	}
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

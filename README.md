<p align="center"><img src="https://raw.githubusercontent.com/go-ruby-grpc/brand/main/social/go-ruby-grpc-grpc.png" alt="go-ruby-grpc/grpc" width="720"></p>

# grpc — go-ruby-grpc

[![Docs](https://img.shields.io/badge/docs-mkdocs--material-DC2626)](https://go-ruby-grpc.github.io/docs/)
[![License](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![Coverage](https://img.shields.io/badge/coverage-100%25-1a7f37)](#tests--coverage)

**A pure-Go (no cgo), MRI-faithful reimplementation of the surface of Ruby's
[`grpc`](https://github.com/grpc/grpc/tree/master/src/ruby) gem** — the `GRPC`
object model a Ruby program uses to build servers and stubs — **without any Ruby
runtime and without the gem's C extension** (upstream `grpc` ships as a heavy C
extension around the gRPC-core C library).

It does **not** reimplement HTTP/2 or the gRPC wire protocol. It is a
Ruby-faithful API layer on top of
[`google.golang.org/grpc`](https://pkg.go.dev/google.golang.org/grpc) and
[`google.golang.org/protobuf`](https://pkg.go.dev/google.golang.org/protobuf),
the official pure-Go gRPC and protobuf runtimes. Every byte on the wire is
produced by the canonical Go gRPC stack, so a server built here interoperates
with any conformant gRPC peer and a stub built here can call any conformant gRPC
server — **by construction**.

It is the gRPC binding for
[go-embedded-ruby](https://github.com/go-embedded-ruby/ruby), and it reuses
[go-ruby-protobuf](https://github.com/go-ruby-protobuf/protobuf) (the pure-Go
`google-protobuf` gem) for the message layer. It is a sibling of
[go-ruby-oauth2](https://github.com/go-ruby-oauth2/oauth2),
[go-ruby-regexp](https://github.com/go-ruby-regexp/regexp) and
[go-ruby-net-http](https://github.com/go-ruby-net-http/net-http).

> **The network is a host seam.** The core server logic and client stub never
> touch a socket: both the listener and the dialer come from an injected
> `Transport`. `NetTransport` is the production transport (real TCP);
> `MemTransport` is an in-process, bufconn-backed transport that carries a
> **real HTTP/2 gRPC session** over an in-memory pipe. So the whole stack is
> exercised end-to-end in tests without binding a port — mirroring the host seam
> the OIDC/OAuth2 bindings use for their HTTP round-trip.

## Features

Faithful port of the `grpc` gem's server and client surface:

- **`GRPC::RpcServer`** → `RpcServer` — `#add_http2_port` → `AddHTTP2Port`,
  `#handle` → `Handle`, `#run` / `#run_till_terminated` → `Run` /
  `RunTillTerminated`, `#stop` → `Stop`.
- **`GRPC::ClientStub`** → `ClientStub` — all four cardinalities:
  `#request_response` → `RequestResponse`, `#client_streamer` →
  `ClientStreamer`, `#server_streamer` → `ServerStreamer`, `#bidi_streamer` →
  `BidiStreamer`; per-call **deadlines** and **metadata** (a Hash).
- **`GRPC::ActiveCall`** → `ActiveCall` — `Send`, `Read`, `EachRemoteRead`,
  `Metadata`, `Deadline` over one call.
- **`GRPC::Core::StatusCodes`** → the `StatusCode` constants (`OK`,
  `InvalidArgument`, `DeadlineExceeded`, …, all 17), with the gem's
  SCREAMING_SNAKE names.
- **`GRPC::BadStatus`** → `*BadStatus` (`code`, `details`, trailing metadata,
  `to_status`), and **`GRPC::Core::CallError`** → `*CallError`; full error
  mapping to and from the gRPC runtime.
- **Message-agnostic**, exactly like the gem: each call carries a `Marshal` /
  `Unmarshal` function (the marshal/unmarshal procs a generated
  `*_services_pb.rb` attaches to a `RpcDesc`). Messages from
  [go-ruby-protobuf](https://github.com/go-ruby-protobuf/protobuf) drop straight
  in via its `Encode` / `Decode`.

CGO-free, **100% test coverage**, `-race` clean, `gofmt` + `go vet` clean, and
green across the six 64-bit Go targets (amd64, arm64, riscv64, loong64, ppc64le,
s390x — including the big-endian s390x).

## Install

```sh
go get github.com/go-ruby-grpc/grpc
```

## Usage

```go
package main

import (
	"fmt"

	grpc "github.com/go-ruby-grpc/grpc"
)

func main() {
	tr := grpc.NewMemTransport() // or grpc.NetTransport{} in production

	// --- server: mirrors GRPC::RpcServer ---
	srv := grpc.NewRpcServer(grpc.WithTransport(tr))
	srv.AddHTTP2Port("localhost:50051", ":this_port_is_insecure")
	srv.Handle(grpc.Service{
		Name: "helloworld.Greeter",
		Methods: []grpc.Method{{
			Name:             "SayHello",
			Type:             grpc.Unary,
			RequestUnmarshal: func(b []byte) (any, error) { return string(b), nil },
			ResponseMarshal:  func(m any) ([]byte, error) { return []byte(m.(string)), nil },
			UnaryHandler: func(req any, call *grpc.ActiveCall) (any, error) {
				return "Hello " + req.(string), nil
			},
		}},
	})
	go srv.Run()
	defer srv.Stop()

	// --- client: mirrors GRPC::ClientStub ---
	stub, _ := grpc.NewClientStub("localhost:50051", ":this_channel_is_insecure",
		grpc.WithStubTransport(tr))
	defer stub.Close()

	resp, _ := stub.RequestResponse("/helloworld.Greeter/SayHello", "world", grpc.CallOptions{
		Marshal:   func(m any) ([]byte, error) { return []byte(m.(string)), nil },
		Unmarshal: func(b []byte) (any, error) { return string(b), nil },
		Metadata:  grpc.Metadata{"x-trace": "abc"},
	})
	fmt.Println(resp) // Hello world
}
```

## Streaming

`ClientStreamer`, `ServerStreamer` and `BidiStreamer` mirror the gem's streaming
helpers; a streaming handler uses `ActiveCall.Read` / `EachRemoteRead` to consume
requests and `ActiveCall.Send` to emit responses.

## Status codes & errors

```go
_ = grpc.NewBadStatus(grpc.InvalidArgument, "bad argument", grpc.Metadata{})
// err.Error() == "3:bad argument"; err.Code.Name() == "INVALID_ARGUMENT"
```

A handler returns a `*BadStatus` to fail a call with a specific code; the stub
raises the matching `*BadStatus` on the client side. Any other handler error maps
to `UNKNOWN`, exactly as the gem surfaces a bare exception.

## Mapping to the gem

| gem                                   | this package                              |
| ------------------------------------- | ----------------------------------------- |
| `GRPC::RpcServer.new`                 | `NewRpcServer(...)`                       |
| `#add_http2_port(addr, creds)`        | `(*RpcServer).AddHTTP2Port`               |
| `#handle(service)`                    | `(*RpcServer).Handle`                     |
| `#run` / `#run_till_terminated`       | `(*RpcServer).Run` / `RunTillTerminated`  |
| `#stop`                               | `(*RpcServer).Stop`                       |
| `GRPC::ClientStub.new(host, creds)`   | `NewClientStub(host, creds, ...)`         |
| `#request_response`                   | `(*ClientStub).RequestResponse`           |
| `#client_streamer`                    | `(*ClientStub).ClientStreamer`            |
| `#server_streamer`                    | `(*ClientStub).ServerStreamer`            |
| `#bidi_streamer`                      | `(*ClientStub).BidiStreamer`              |
| `GRPC::ActiveCall`                    | `*ActiveCall`                             |
| `GRPC::Core::StatusCodes::*`          | the `StatusCode` constants               |
| `GRPC::BadStatus`                     | `*BadStatus`                              |
| `GRPC::Core::CallError`               | `*CallError`                              |
| metadata (a Hash)                     | `Metadata` (`map[string]string`)         |
| generated marshal/unmarshal procs     | `Marshaler` / `Unmarshaler` per call      |

## Tests & coverage

The suite drives every RPC cardinality (unary + the three streaming shapes),
status-code and deadline propagation, metadata round-trips and error mapping
**over the in-memory transport**, verified against a **real
`google.golang.org/grpc` server and client as the oracle**: our stub calls a
plain grpc-go server and a plain grpc-go client calls our `RpcServer`, both over
bufconn, so protocol conformance is checked end-to-end — not asserted. A
go-ruby-protobuf message is round-tripped through the stack to confirm the
message-layer integration.

```sh
COVERPKG=$(go list ./... | paste -sd, -)
go test -race -coverpkg="$COVERPKG" -coverprofile=cover.out ./...
go tool cover -func=cover.out | tail -1   # 100.0%
```

## License

BSD-3-Clause — see [LICENSE](LICENSE). Copyright the go-ruby-grpc/grpc authors.

## WebAssembly

Being pure Go (CGO=0), this library also compiles to **WebAssembly** — both
`GOOS=js GOARCH=wasm` (browser / Node.js) and `GOOS=wasip1 GOARCH=wasm` (WASI).
CI builds both targets on every push, alongside the six 64-bit native/qemu arches.

```sh
GOOS=js     GOARCH=wasm go build ./...   # browser / Node
GOOS=wasip1 GOARCH=wasm go build ./...   # WASI (wasmtime, wasmer, wasmedge, …)
```

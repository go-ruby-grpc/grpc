// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// The differential oracle for the code generator: run the real
// grpc_tools_ruby_protoc on a .proto and assert that GenerateRubyServices
// produces byte-identical output for the equivalent ServiceFile. When the binary
// is not installed the test skips (the inline goldens in codegen_test.go still
// pin the format), so the suite stays green on a host without the Ruby gRPC
// toolchain.

// findRubyProtoc locates grpc_tools_ruby_protoc on PATH or in the per-user gem
// bin directories, returning "" if it is not installed.
func findRubyProtoc() string {
	if p, err := exec.LookPath("grpc_tools_ruby_protoc"); err == nil {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	matches, _ := filepath.Glob(filepath.Join(home, ".gem", "ruby", "*", "bin", "grpc_tools_ruby_protoc"))
	for _, m := range matches {
		if info, err := os.Stat(m); err == nil && !info.IsDir() {
			return m
		}
	}
	return ""
}

// oracleProtos pairs .proto source with the ServiceFile our generator is fed for
// the same file; the two outputs must match to the byte.
var oracleProtos = []struct {
	name  string
	proto string
	file  ServiceFile
}{
	{
		name: "helloworld",
		proto: `syntax = "proto3";
package helloworld;
message HelloRequest { string name = 1; }
message HelloReply { string message = 1; }
service Greeter {
  rpc SayHello (HelloRequest) returns (HelloReply) {}
}
`,
		file: ServiceFile{
			ProtoFile: "helloworld.proto", Package: "helloworld",
			Services: []ServiceGen{{Name: "Greeter", Methods: []MethodGen{
				{Name: "SayHello", InputType: "helloworld.HelloRequest", OutputType: "helloworld.HelloReply"},
			}}},
		},
	},
	{
		name: "route_guide",
		proto: `syntax = "proto3";
package routeguide;
message Point { int32 latitude = 1; }
message Rectangle { Point lo = 1; }
message Feature { string name = 1; }
message RouteSummary { int32 point_count = 1; }
message RouteNote { string message = 1; }
service RouteGuide {
  rpc GetFeature(Point) returns (Feature) {}
  rpc ListFeatures(Rectangle) returns (stream Feature) {}
  rpc RecordRoute(stream Point) returns (RouteSummary) {}
  rpc RouteChat(stream RouteNote) returns (stream RouteNote) {}
}
`,
		file: ServiceFile{
			ProtoFile: "route_guide.proto", Package: "routeguide",
			Services: []ServiceGen{{Name: "RouteGuide", Methods: []MethodGen{
				{Name: "GetFeature", InputType: "routeguide.Point", OutputType: "routeguide.Feature"},
				{Name: "ListFeatures", InputType: "routeguide.Rectangle", OutputType: "routeguide.Feature", ServerStreaming: true},
				{Name: "RecordRoute", InputType: "routeguide.Point", OutputType: "routeguide.RouteSummary", ClientStreaming: true},
				{Name: "RouteChat", InputType: "routeguide.RouteNote", OutputType: "routeguide.RouteNote", ClientStreaming: true, ServerStreaming: true},
			}}},
		},
	},
}

func TestGenerateRubyServicesAgainstRealProtoc(t *testing.T) {
	bin := findRubyProtoc()
	if bin == "" {
		t.Skip("grpc_tools_ruby_protoc not installed; skipping the live differential oracle")
	}
	for _, tc := range oracleProtos {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			protoPath := filepath.Join(dir, tc.file.ProtoFile)
			if err := os.WriteFile(protoPath, []byte(tc.proto), 0o644); err != nil {
				t.Fatal(err)
			}
			out := filepath.Join(dir, "out")
			if err := os.Mkdir(out, 0o755); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(bin, "-I", dir, "--grpc_out="+out, "--ruby_out="+out, tc.file.ProtoFile)
			if combined, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("grpc_tools_ruby_protoc failed: %v\n%s", err, combined)
			}
			base := tc.file.ProtoFile[:len(tc.file.ProtoFile)-len(".proto")]
			refBytes, err := os.ReadFile(filepath.Join(out, base+"_services_pb.rb"))
			if err != nil {
				t.Fatal(err)
			}
			got, err := GenerateRubyServices(tc.file)
			if err != nil {
				t.Fatal(err)
			}
			if got != string(refBytes) {
				t.Errorf("generator diverges from grpc_tools_ruby_protoc\n--- ours ---\n%s\n--- protoc ---\n%s", got, refBytes)
			}
		})
	}
}

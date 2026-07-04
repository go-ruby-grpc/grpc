// Copyright (c) the go-ruby-grpc/grpc authors
//
// SPDX-License-Identifier: BSD-3-Clause

package grpc

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StatusCode mirrors GRPC::Core::StatusCodes — the canonical gRPC status codes.
// The integer values are wire-fixed and identical to the gem's constants and to
// google.golang.org/grpc/codes.
type StatusCode uint32

// The gRPC status codes, matching GRPC::Core::StatusCodes::* one-to-one.
const (
	OK                 StatusCode = 0
	Cancelled          StatusCode = 1
	Unknown            StatusCode = 2
	InvalidArgument    StatusCode = 3
	DeadlineExceeded   StatusCode = 4
	NotFound           StatusCode = 5
	AlreadyExists      StatusCode = 6
	PermissionDenied   StatusCode = 7
	ResourceExhausted  StatusCode = 8
	FailedPrecondition StatusCode = 9
	Aborted            StatusCode = 10
	OutOfRange         StatusCode = 11
	Unimplemented      StatusCode = 12
	Internal           StatusCode = 13
	Unavailable        StatusCode = 14
	DataLoss           StatusCode = 15
	Unauthenticated    StatusCode = 16
)

// codeName maps each status code to the SCREAMING_SNAKE_CASE spelling the gem
// exposes as GRPC::Core::StatusCodes constants.
var codeName = map[StatusCode]string{
	OK:                 "OK",
	Cancelled:          "CANCELLED",
	Unknown:            "UNKNOWN",
	InvalidArgument:    "INVALID_ARGUMENT",
	DeadlineExceeded:   "DEADLINE_EXCEEDED",
	NotFound:           "NOT_FOUND",
	AlreadyExists:      "ALREADY_EXISTS",
	PermissionDenied:   "PERMISSION_DENIED",
	ResourceExhausted:  "RESOURCE_EXHAUSTED",
	FailedPrecondition: "FAILED_PRECONDITION",
	Aborted:            "ABORTED",
	OutOfRange:         "OUT_OF_RANGE",
	Unimplemented:      "UNIMPLEMENTED",
	Internal:           "INTERNAL",
	Unavailable:        "UNAVAILABLE",
	DataLoss:           "DATA_LOSS",
	Unauthenticated:    "UNAUTHENTICATED",
}

// Name returns the gem's constant name for the code, e.g. "INVALID_ARGUMENT".
// Unknown numeric codes render as "CODE(<n>)".
func (c StatusCode) Name() string {
	if n, ok := codeName[c]; ok {
		return n
	}
	return fmt.Sprintf("CODE(%d)", uint32(c))
}

// String satisfies fmt.Stringer with the gem's constant name.
func (c StatusCode) String() string { return c.Name() }

// codeOf returns the google.golang.org/grpc codes.Code for a StatusCode.
func codeOf(c StatusCode) codes.Code { return codes.Code(c) }

// BadStatus mirrors GRPC::BadStatus: an operation that finished with a non-OK
// status. It carries the status code, the detail message and any trailing
// metadata, and it is the error a client raises when a call fails.
type BadStatus struct {
	Code     StatusCode
	Details  string
	Metadata Metadata
}

// NewBadStatus builds a BadStatus, matching GRPC::BadStatus.new(code, details,
// metadata). A nil metadata becomes an empty Metadata.
func NewBadStatus(code StatusCode, details string, md Metadata) *BadStatus {
	if md == nil {
		md = Metadata{}
	}
	return &BadStatus{Code: code, Details: details, Metadata: md}
}

// Error renders the gem's "<n>:<details>" message, e.g. "3:bad argument".
func (b *BadStatus) Error() string {
	return fmt.Sprintf("%d:%s", uint32(b.Code), b.Details)
}

// ToStatus converts the BadStatus to the gem's Struct::Status shape: a code,
// its details and trailing metadata. It is what GRPC::BadStatus#to_status
// returns.
func (b *BadStatus) ToStatus() (code StatusCode, details string, md Metadata) {
	return b.Code, b.Details, b.Metadata
}

// toGRPC converts a BadStatus into a google.golang.org/grpc *status.Status so
// the wire carries the exact code and details.
func (b *BadStatus) toGRPC() error {
	return status.New(codeOf(b.Code), b.Details).Err()
}

// badStatusFromError maps any error returned by the gRPC runtime back to a
// *BadStatus, exactly as the gem raises GRPC::BadStatus for a non-OK call. A nil
// error yields nil; a non-status error becomes an UNKNOWN BadStatus.
func badStatusFromError(err error) *BadStatus {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return NewBadStatus(Unknown, err.Error(), Metadata{})
	}
	md := Metadata{}
	if d := st.Proto(); d != nil {
		_ = d
	}
	return NewBadStatus(StatusCode(st.Code()), st.Message(), md)
}

// toGRPCError maps a handler's error to a google.golang.org/grpc status error so
// the wire carries a faithful code and message. A *BadStatus keeps its exact
// code and details; any other non-nil error becomes an UNKNOWN status, matching
// how the gem surfaces a bare handler exception.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if b, ok := err.(*BadStatus); ok {
		return b.toGRPC()
	}
	return status.New(codeOf(Unknown), err.Error()).Err()
}

// CallError mirrors GRPC::Core::CallError: a low-level error from the call
// machinery itself (as opposed to a non-OK application status). It is raised for
// misuse such as writing to a finished call.
type CallError struct {
	Message string
}

// NewCallError builds a CallError with the given message.
func NewCallError(msg string) *CallError { return &CallError{Message: msg} }

// Error implements error.
func (e *CallError) Error() string { return e.Message }

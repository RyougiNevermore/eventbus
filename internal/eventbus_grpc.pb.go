// Code generated by protoc-gen-go-grpc. DO NOT EDIT.

package internal

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// EventbusClient is the client API for Eventbus service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type EventbusClient interface {
	Send(ctx context.Context, in *Message, opts ...grpc.CallOption) (*Void, error)
	Request(ctx context.Context, in *Message, opts ...grpc.CallOption) (*Message, error)
}

type eventbusClient struct {
	cc grpc.ClientConnInterface
}

func NewEventbusClient(cc grpc.ClientConnInterface) EventbusClient {
	return &eventbusClient{cc}
}

func (c *eventbusClient) Send(ctx context.Context, in *Message, opts ...grpc.CallOption) (*Void, error) {
	out := new(Void)
	err := c.cc.Invoke(ctx, "/internal.Eventbus/Send", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *eventbusClient) Request(ctx context.Context, in *Message, opts ...grpc.CallOption) (*Message, error) {
	out := new(Message)
	err := c.cc.Invoke(ctx, "/internal.Eventbus/Request", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// EventbusServer is the server API for Eventbus service.
// All implementations must embed UnimplementedEventbusServer
// for forward compatibility
type EventbusServer interface {
	Send(context.Context, *Message) (*Void, error)
	Request(context.Context, *Message) (*Message, error)
	mustEmbedUnimplementedEventbusServer()
}

// UnimplementedEventbusServer must be embedded to have forward compatible implementations.
type UnimplementedEventbusServer struct {
}

func (UnimplementedEventbusServer) Send(context.Context, *Message) (*Void, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Send not implemented")
}
func (UnimplementedEventbusServer) Request(context.Context, *Message) (*Message, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Request not implemented")
}
func (UnimplementedEventbusServer) mustEmbedUnimplementedEventbusServer() {}

// UnsafeEventbusServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to EventbusServer will
// result in compilation errors.
type UnsafeEventbusServer interface {
	mustEmbedUnimplementedEventbusServer()
}

func RegisterEventbusServer(s grpc.ServiceRegistrar, srv EventbusServer) {
	s.RegisterService(&Eventbus_ServiceDesc, srv)
}

func _Eventbus_Send_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Message)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(EventbusServer).Send(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/internal.Eventbus/Send",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(EventbusServer).Send(ctx, req.(*Message))
	}
	return interceptor(ctx, in, info, handler)
}

func _Eventbus_Request_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Message)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(EventbusServer).Request(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/internal.Eventbus/Request",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(EventbusServer).Request(ctx, req.(*Message))
	}
	return interceptor(ctx, in, info, handler)
}

// Eventbus_ServiceDesc is the grpc.ServiceDesc for Eventbus service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Eventbus_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "internal.Eventbus",
	HandlerType: (*EventbusServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Send",
			Handler:    _Eventbus_Send_Handler,
		},
		{
			MethodName: "Request",
			Handler:    _Eventbus_Request_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "internal/eventbus.proto",
}

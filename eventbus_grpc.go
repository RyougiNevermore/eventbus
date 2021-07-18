package eventbus

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/eventbus/internal"
	"github.com/aacfactory/workers"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
	"sync/atomic"
	"time"
)

func newGrpcEventbusServer(ln net.Listener, requestWorkers *workers.Workers, tlsConfig *tls.Config) (srv *grpcEventBusServer) {
	var server *grpc.Server
	if tlsConfig != nil {
		server = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		server = grpc.NewServer()
	}
	srv = &grpcEventBusServer{
		running:        int64(1),
		requestWorkers: requestWorkers,
		server:         server,
		ln:             ln,
		tlsConfig:      tlsConfig,
	}
	internal.RegisterEventbusServer(server, srv)
	return
}

type grpcEventBusServer struct {
	internal.UnimplementedEventbusServer
	running        int64
	requestWorkers *workers.Workers
	server         *grpc.Server
	ln             net.Listener
	tlsConfig      *tls.Config
}

func (srv *grpcEventBusServer) Send(_ context.Context, remoteMessage *internal.Message) (result *internal.Void, err error) {
	msg, msgErr := newMessageFromBytes(remoteMessage.Header, remoteMessage.Body)
	if msgErr != nil {
		err = msgErr
		return
	}
	address, hasAddress := msg.getAddress()
	if !hasAddress || address == "" {
		err = fmt.Errorf("eventbus handle remote request failed, no address in message head")
		return
	}
	if srv.closed() {
		err = fmt.Errorf("eventbus handle remote request failed, server has been closed")
		return
	}

	rm := &requestMessage{
		message: msg,
		replyCh: nil,
	}

	if !srv.requestWorkers.Command(rm, "type", "local") {
		err = fmt.Errorf("eventbus send failed, send to workers failed")
		return
	}

	result = &internal.Void{}

	return
}

func (srv *grpcEventBusServer) Request(_ context.Context, remoteMessage *internal.Message) (result *internal.Message, err error) {
	msg, msgErr := newMessageFromBytes(remoteMessage.Header, remoteMessage.Body)
	if msgErr != nil {
		err = msgErr
		return
	}
	address, hasAddress := msg.getAddress()
	if !hasAddress || address == "" {
		err = fmt.Errorf("eventbus handle remote request failed, no address in message head")
		return
	}
	if srv.closed() {
		err = fmt.Errorf("eventbus handle remote request failed, server has been closed")
		return
	}
	replyCh := make(chan *message, 1)

	rm := &requestMessage{
		message: msg,
		replyCh: replyCh,
	}

	if !srv.requestWorkers.Command(rm, "type", "local") {
		err = fmt.Errorf("eventbus send failed, send to workers failed")
		return
	}

	reply := <-replyCh
	if reply == nil {
		result = &internal.Message{
			Header: []byte{'{', '}'},
			Body:   []byte{},
		}
		return
	}

	result = &internal.Message{
		Header: jsonEncode(reply.Head),
		Body:   reply.Body,
	}

	return
}

func (srv *grpcEventBusServer) closed() bool {
	return atomic.LoadInt64(&srv.running) == int64(0)
}

func (srv *grpcEventBusServer) start() {
	go func(ln net.Listener, server *grpc.Server) {

		serveErr := server.Serve(ln)
		if serveErr != nil {
			panic(fmt.Errorf("eventbus start server failed, %v", serveErr))
		}
	}(srv.ln, srv.server)
}

func (srv *grpcEventBusServer) Close() {
	atomic.StoreInt64(&srv.running, int64(0))
	time.Sleep(500 * time.Millisecond)
	srv.server.GracefulStop()
}

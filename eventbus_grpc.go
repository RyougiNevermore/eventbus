package eventbus

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/eventbus/internal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

func newGrpcEventbusServer(ln net.Listener, dispatchMessageCh chan<- *requestMessage, handleCount *sync.WaitGroup, tlsConfig *tls.Config) (srv *grpcEventBusServer) {
	var server *grpc.Server
	if tlsConfig != nil {
		server = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		server = grpc.NewServer()
	}
	srv = &grpcEventBusServer{
		running:           int64(1),
		dispatchMessageCh: dispatchMessageCh,
		server:            server,
		ln:                ln,
		handleCount:       handleCount,
		tlsConfig:         tlsConfig,
	}
	internal.RegisterEventbusServer(server, srv)
	return
}

type grpcEventBusServer struct {
	internal.UnimplementedEventbusServer
	running           int64
	dispatchMessageCh chan<- *requestMessage
	server            *grpc.Server
	ln                net.Listener
	handleCount       *sync.WaitGroup
	tlsConfig         *tls.Config
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

	srv.handleCount.Add(1)
	srv.dispatchMessageCh <- &requestMessage{
		message: msg,
		replyCh: nil,
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

	srv.handleCount.Add(1)
	srv.dispatchMessageCh <- &requestMessage{
		message: msg,
		replyCh: replyCh,
	}

	reply := <-replyCh
	if reply == nil {
		err = fmt.Errorf("eventbus handle remote request failed, no reply message fetched")
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

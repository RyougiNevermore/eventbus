package eventbus

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/cluster"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/workers"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ClusterEventbusOption struct {
	Host                     string              `json:"host,omitempty"`
	Port                     int                 `json:"port,omitempty"`
	PublicHost               string              `json:"publicHost,omitempty"`
	PublicPort               int                 `json:"publicPort,omitempty"`
	Meta                     cluster.ServiceMeta `json:"meta,omitempty"`
	Tags                     []string            `json:"tags,omitempty"`
	TLS                      cluster.ServiceTLS  `json:"tls,omitempty"`
	EventChanCap             int                 `json:"eventChanCap,omitempty"`
	WorkersMaxIdleTime       time.Duration       `json:"workersMaxIdleTime,omitempty"`
	WorkersCommandTimeout    time.Duration       `json:"workersCommandTimeout,omitempty"`
	WorkersCommandBufferSize int                 `json:"workersCommandBufferSize,omitempty"`
	Workers                  int                 `json:"workers,omitempty"`
}

func NewClusterEventbus(discovery cluster.ServiceDiscovery, option ClusterEventbusOption) (bus Eventbus, err error) {
	if discovery == nil {
		err = fmt.Errorf("create cluster eventbus failed, dicovery is nil")
		return
	}

	eventChanCap := option.EventChanCap
	if eventChanCap < 1 {
		eventChanCap = getDefaultEventChanCap()
	}

	clusterEventbus := &clusterEventBus{
		running:             0,
		ln:                  nil,
		grpcServer:          nil,
		discovery:           discovery,
		clients:             nil,
		handlers:            newLocalEventHandleStore(),
		registrationAddress: "",
		meta:                option.Meta,
		endpointTLS:         option.TLS,
		registrationsLock:   sync.Mutex{},
		registrations:       make([]cluster.Registration, 0, 1),
	}

	host := option.Host
	if host == "" {
		err = fmt.Errorf("create cluster eventbus failed, host is empty")
		return
	}
	port := option.Port
	if port < 1 || port > 65535 {
		err = fmt.Errorf("create cluster eventbus failed, port is invalid")
		return
	}
	publicHost := option.PublicHost
	if publicHost == "" {
		publicHost = host
	}
	publicPort := option.PublicPort
	if publicPort < 1 {
		publicPort = port
	}
	if publicPort < 1 || publicPort > 65535 {
		err = fmt.Errorf("create cluster eventbus failed, public port is invalid")
		return
	}

	clusterEventbus.registrationAddress = fmt.Sprintf("%s:%d", publicHost, publicPort)

	address := fmt.Sprintf("%s:%d", host, port)

	// ln
	ln, lnErr := net.Listen("tcp", address)
	if lnErr != nil {
		err = fmt.Errorf("create cluster eventbus failed, listen %s failed, %v", address, lnErr)
		return
	}
	clusterEventbus.ln = ln

	// workers

	rw := workers.NewWorkers(workers.Option{
		MaxWorkerNum:      option.Workers,
		MaxIdleTime:       option.WorkersMaxIdleTime,
		CommandTimeout:    option.WorkersCommandTimeout,
		CommandBufferSize: option.WorkersCommandBufferSize,
		Fn:                clusterEventbus.handleRequestMessageWorkFn,
	})

	clusterEventbus.requestWorkers = rw

	// grpcServer
	var tlsConfig *tls.Config = nil
	endpointTLS := clusterEventbus.endpointTLS
	if endpointTLS.Enable() {
		tlsConfig, err = endpointTLS.ToServerTLSConfig()
		if err != nil {
			err = fmt.Errorf("create cluster eventbus failed, create server tls config failed, %v", err)
			return
		}
	}

	grpcServer := newGrpcEventbusServer(ln, rw, tlsConfig)
	clusterEventbus.grpcServer = grpcServer
	// clients
	clients, clientsErr := newGrpcEventbusClient(discovery)
	if clientsErr != nil {
		err = fmt.Errorf("create cluster eventbus failed, create remote clients failed, %v", clientsErr)
		return
	}
	clusterEventbus.clients = clients

	bus = clusterEventbus

	return
}

type clusterEventBus struct {
	running             int64
	requestWorkers      *workers.Workers
	ln                  net.Listener
	grpcServer          *grpcEventBusServer
	discovery           cluster.ServiceDiscovery
	clients             *grpcEventbusClient
	handlers            *localEventHandleStore
	registrationAddress string
	meta                cluster.ServiceMeta
	endpointTLS         cluster.ServiceTLS
	registrationsLock   sync.Mutex
	registrations       []cluster.Registration
}

func (bus *clusterEventBus) Send(address string, v interface{}, options ...DeliveryOptions) (err error) {
	if address == "" {
		err = errors.ServiceError("eventbus send failed, address is empty")
		return
	}

	if bus.closed() {
		err = errors.ServiceError("eventbus send failed, eventbus has been closed")
		return
	}

	msg := newMessage(address, v, options)

	tags, _ := msg.getTags()

	rm := &requestMessage{
		message: msg,
		replyCh: nil,
	}

	//local
	if bus.handlers.contains(address, tags) {
		if !bus.requestWorkers.Command(rm, "type", "local") {
			err = errors.ServiceError("eventbus send failed, send to workers failed")
			return
		}
		return
	}

	// remote
	if !bus.requestWorkers.Command(rm, "type", "remote") {
		err = errors.ServiceError("eventbus send failed, send to workers failed")
		return
	}

	return
}

func (bus *clusterEventBus) Request(address string, v interface{}, options ...DeliveryOptions) (reply ReplyFuture) {
	if address == "" {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, address is empty"))
		return
	}

	if bus.closed() {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, eventbus has been closed"))
		return
	}

	msg := newMessage(address, v, options)

	tags, _ := msg.getTags()

	replyCh := make(chan *message, 1)
	rm := &requestMessage{
		message: msg,
		replyCh: replyCh,
	}

	reply = newFuture(replyCh)

	// local
	if bus.handlers.contains(address, tags) {
		if !bus.requestWorkers.Command(rm, "type", "local") {
			rm.failed(fmt.Errorf("eventbus send failed, send to workers failed"))
			return
		}
		return
	}

	// remote
	if !bus.requestWorkers.Command(rm, "type", "remote") {
		rm.failed(fmt.Errorf("eventbus send failed, send to workers failed"))
		return
	}

	return
}

func (bus *clusterEventBus) RegisterHandler(address string, handler EventHandler, tags ...string) (err error) {
	bus.registrationsLock.Lock()
	defer bus.registrationsLock.Unlock()

	err = bus.RegisterLocalHandler(address, handler, tags...)

	if err != nil {
		return
	}

	tags = tagsClean(tags)

	registration, publishErr := bus.discovery.Publish(defaultGroupName, address, "tcp", bus.registrationAddress, tags, bus.meta, bus.endpointTLS)

	if publishErr != nil {
		bus.handlers.remove(address, tags)
		err = fmt.Errorf("eventbus register event handler failed for publush into discovery, address is %s, %v", address, publishErr)
		return
	}

	bus.registrations = append(bus.registrations, registration)

	return
}

func (bus *clusterEventBus) RegisterLocalHandler(address string, handler EventHandler, tags ...string) (err error) {
	if !bus.closed() {
		err = fmt.Errorf("eventbus register handler failed, it is running")
		return
	}

	if address == "" {
		err = errors.ServiceError("eventbus register event handler failed, address is empty")
		return
	}
	if strings.Contains(address, ":") {
		err = errors.ServiceError("eventbus register event handler failed, address can not has : ")
		return
	}
	if handler == nil {
		err = errors.ServiceError("eventbus register event handler failed, handler is nil")
		return
	}

	tags = tagsClean(tags)

	has := bus.handlers.contains(address, tags)
	if has {
		err = errors.ServiceError("eventbus register event handler failed, address event handler has been bound")
		return
	}

	bus.handlers.put(address, tags, handler)

	return
}

func (bus *clusterEventBus) Start(_ context.Context) {
	if !bus.closed() {
		panic(fmt.Errorf("eventbus start failed, it is running"))
	}
	atomic.StoreInt64(&bus.running, int64(1))

	// workers
	bus.requestWorkers.Start()
	// grpc serve
	bus.grpcServer.start()
	// grpc clients
	bus.clients.Start()

}

func (bus *clusterEventBus) Close(context context.Context) {
	if bus.closed() {
		panic(fmt.Errorf("eventbus has been closed, close falied"))
	}
	if !atomic.CompareAndSwapInt64(&bus.running, int64(1), int64(0)) {
		panic(fmt.Errorf("eventbus is not running, close failed"))
	}

	for _, registration := range bus.registrations {
		_ = bus.discovery.UnPublish(registration)
	}

	bus.requestWorkers.Stop()

	bus.grpcServer.Close()
	bus.clients.Close()

	closeCh := make(chan struct{}, 1)

	go func(closeCh chan struct{}, eb *clusterEventBus) {

		eb.requestWorkers.Sync()

		closeCh <- struct{}{}
		close(closeCh)
	}(closeCh, bus)

	select {
	case <-context.Done():
		return
	case <-closeCh:
		return
	default:
	}
}

func (bus *clusterEventBus) closed() bool {
	return atomic.LoadInt64(&bus.running) == int64(0)
}

func (bus *clusterEventBus) handleRequestMessageWorkFn(v interface{}, meta map[string]string) {
	if v == nil {
		return
	}
	requestMsg, ok := v.(*requestMessage)
	if !ok {
		return
	}

	if meta["type"] == "local" {
		bus.handleLocalRequestMessage(requestMsg)
	} else if meta["type"] == "remote" {
		bus.handleRemoteRequestMessage(requestMsg)
	} else {
		requestMsg.failed(fmt.Errorf("eventbus handle event failed, no type in meta of worker"))
	}
	return
}

func (bus *clusterEventBus) handleLocalRequestMessage(requestMsg *requestMessage) {
	msg := requestMsg.message
	address, has := msg.getAddress()
	if !has || address == "" {
		requestMsg.failed(fmt.Errorf("eventbus handle event failed, address is empty, %v", msg))
		return
	}
	tags, _ := msg.getTags()
	handler, existed := bus.handlers.get(address, tags)
	if !existed {
		requestMsg.failed(fmt.Errorf("eventbus handle event failed, no handler handle address %s", address))
		return
	}
	reply, handleErr := handler(&defaultEvent{head: defaultEventHead{msg.Head}, body: msg.Body})
	if handleErr != nil {
		requestMsg.failed(handleErr)
		return
	}
	requestMsg.succeed(reply)
}

func (bus *clusterEventBus) handleRemoteRequestMessage(requestMsg *requestMessage) {
	if !requestMsg.needReply() {
		_ = bus.clients.Send(requestMsg.message)
		return
	}
	bus.clients.Request(requestMsg)
}

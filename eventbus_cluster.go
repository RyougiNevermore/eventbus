package eventbus

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	defaultGroupName = "_ebs_"
)

type ClusterEventbusOption struct {
	Host         string        `json:"host,omitempty"`
	Port         int           `json:"port,omitempty"`
	PublicHost   string        `json:"publicHost,omitempty"`
	PublicPort   int           `json:"publicPort,omitempty"`
	Meta         *EndpointMeta `json:"meta,omitempty"`
	Tags         []string      `json:"tags,omitempty"`
	TLS          *EndpointTLS  `json:"tls,omitempty"`
	EventChanCap int           `json:"eventChanCap,omitempty"`
	EventWorkers int           `json:"eventWorkers,omitempty"`
}

func NewClusterEventbus(discovery ServiceDiscovery, option ClusterEventbusOption) (bus Eventbus, err error) {
	if discovery == nil {
		err = fmt.Errorf("create cluster eventbus failed, dicovery is nil")
		return
	}

	eventChanCap := option.EventChanCap
	if eventChanCap < 1 {
		eventChanCap = getDefaultEventChanCap()
	}
	eventWorkers := option.EventWorkers
	if eventWorkers < 1 {
		eventWorkers = getDefaultWorkers()
	}

	clusterEventbus := &clusterEventBus{
		running:             0,
		ln:                  nil,
		grpcServer:          nil,
		discovery:           discovery,
		clients:             nil,
		handlersLock:        new(sync.Mutex),
		handlers:            make(map[string]EventHandler),
		handleCount:         new(sync.WaitGroup),
		registrationAddress: "",
		meta:                option.Meta,
		endpointTLS:         option.TLS,
		registrations:       make([]Registration, 0, 1),
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
	wp := &workerPool{
		WorkerFunc:      clusterEventbus.handleRequestMessage,
		MaxWorkersCount: eventWorkers,
	}

	clusterEventbus.requestWorkers = wp

	// grpcServer
	var tlsConfig *tls.Config = nil
	endpointTLS := clusterEventbus.endpointTLS
	if endpointTLS != nil && endpointTLS.Enable() {
		tlsConfig, err = endpointTLS.ToServerTLSConfig()
		if err != nil {
			err = fmt.Errorf("create cluster eventbus failed, create server tls config failed, %v", err)
			return
		}
	} else {
		clusterEventbus.endpointTLS = &EndpointTLS{
			Enable_: false,
		}
	}

	grpcServer := newGrpcEventbusServer(ln, wp, clusterEventbus.handleCount, tlsConfig)
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
	requestWorkers      *workerPool
	ln                  net.Listener
	grpcServer          *grpcEventBusServer
	discovery           ServiceDiscovery
	clients             *grpcEventbusClient
	handlersLock        *sync.Mutex
	handlers            map[string]EventHandler
	handleCount         *sync.WaitGroup
	registrationAddress string
	meta                *EndpointMeta
	endpointTLS         *EndpointTLS
	registrations       []Registration
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

	if existed := bus.addressExisted(address, options...); existed {
		rm := &requestMessage{
			message: msg,
			replyCh: nil,
		}

		if !bus.requestWorkers.SendRequestMessage(rm) {
			err = errors.ServiceError("eventbus send failed, send to workers failed")
			return
		}
		return
	}
	// remote
	err = bus.clients.Send(newMessage(address, v, options))

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

	replyCh := make(chan *message, 1)
	reply = newFuture(replyCh)

	if existed := bus.addressExisted(address, options...); existed {

		rm := &requestMessage{
			message: msg,
			replyCh: replyCh,
		}

		if !bus.requestWorkers.SendRequestMessage(rm) {
			rm.replyCh <- failedReplyMessage(errors.ServiceError("eventbus send failed, send to workers failed"))
			close(rm.replyCh)
		}

		return
	}

	// remote
	replyMsg := bus.clients.Request(msg)
	replyCh <- replyMsg

	return
}

func (bus *clusterEventBus) RegisterHandler(address string, handler EventHandler, tags ...string) (err error) {

	err = bus.RegisterLocalHandler(address, handler, tags...)
	if err != nil {
		return
	}

	registration, publishErr := bus.discovery.Publish(defaultGroupName, address, "tcp", bus.registrationAddress, tags, bus.meta, bus.endpointTLS)

	if publishErr != nil {
		bus.handlersLock.Lock()
		delete(bus.handlers, address)
		bus.handlersLock.Unlock()
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

	bus.handlersLock.Lock()
	defer bus.handlersLock.Unlock()

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
	_, has := bus.handlers[address]
	if has {
		err = errors.ServiceError("eventbus register event handler failed, address event handler has been bound")
		return
	}

	tags = tagsClean(tags)

	bus.handlers[tagsAddress(address, tags)] = handler
	return
}

func (bus *clusterEventBus) Start(context context.Context) {
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
		eb.handleCount.Wait()
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

func (bus *clusterEventBus) addressExisted(address string, options ...DeliveryOptions) (has bool) {
	tags := tagsFromDeliveryOptions(options...)
	key := tagsAddress(address, tags)
	_, has = bus.handlers[key]
	return
}

func (bus *clusterEventBus) handleRequestMessage(requestMsg *requestMessage) (err error) {
	bus.handleCount.Add(1)
	defer bus.handleCount.Done()

	msg := requestMsg.message
	address, has := msg.getAddress()
	if !has || address == "" {
		err = fmt.Errorf("eventbus handle event failed, address is empty, %v", msg)
		return
	}
	tags, _ := msg.getTags()
	key := tagsAddress(address, tags)
	handler, existed := bus.handlers[key]
	replyCh := requestMsg.replyCh
	if !existed {
		err = errors.NotFoundError(fmt.Sprintf("eventbus handle event failed, event handler for address[%s] is not bound", address))
		return
	}
	reply, handleErr := handler(&defaultEvent{head: defaultEventHead{msg.Head}, body: msg.Body})
	if handleErr != nil {
		err = handleErr
		return
	}
	replyCh <- succeedReplyMessage(reply)
	close(replyCh)
	bus.handleCount.Done()

	return
}

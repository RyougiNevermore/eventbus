package eventbus

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	defaultGroupName = "_ebs"
)

type ClusterEventbusOption struct {
	Host                       string        `json:"host,omitempty"`
	Port                       int           `json:"port,omitempty"`
	PublicHost                 string        `json:"publicHost,omitempty"`
	PublicPort                 int           `json:"publicPort,omitempty"`
	Meta                       *EndpointMeta `json:"meta,omitempty"`
	Tags                       []string      `json:"tags,omitempty"`
	TLS                        *EndpointTLS  `json:"tls,omitempty"`
	EventChanCap               int           `json:"eventChanCap,omitempty"`
	EventHandlerInstanceNumber int           `json:"eventHandlerInstanceNumber,omitempty"`
}

type clusterEventBus struct {
	running                    int64
	ln                         net.Listener
	grpcServer                 *grpcEventBusServer
	discovery                  ServiceDiscovery
	clients                    *grpcEventbusClient
	requestMessageDispatcher   chan *requestMessage
	handlersLock               *sync.Mutex
	eventHandlerInstanceNumber int
	handlers                   map[string]EventHandler // key is address
	registrationAddress        string
	meta                       *EndpointMeta
	endpointTLS                *EndpointTLS
	registrations              []Registration
	handleCount                *sync.WaitGroup
	enableLocal                bool
	localed                    *localedEventbus
}

func (bus *clusterEventBus) sendToDiscovered(registration Registration, msg *message) (reply *ReplyFuture, err error) {

	return
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

	// if localed
	if bus.enableLocal {
		err = bus.localed.Send(address, v, options...)
		return
	}

	// remote
	err = bus.clients.Send(newMessage(address, v, options))
	return
}

func (bus *clusterEventBus) Request(address string, v interface{}, options ...DeliveryOptions) (reply *ReplyFuture) {
	if address == "" {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, address is empty"))
		return
	}

	if bus.closed() {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, eventbus has been closed"))
		return
	}
	// if localed
	if bus.enableLocal {
		reply = bus.localed.Request(address, v, options...)
		return
	}

	// remote
	reply = bus.clients.Request(newMessage(address, v, options))
	return
}

func (bus *clusterEventBus) RegisterHandler(address string, handler EventHandler, tags ...string) (err error) {
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

	if bus.enableLocal {
		registerLocalErr := bus.localed.RegisterHandler(address, handler, tags...)
		if registerLocalErr != nil {
			err = registerLocalErr
			return
		}
	}

	registration, publishErr := bus.discovery.Publish(defaultGroupName, address, "tcp", bus.registrationAddress, tags, bus.meta, bus.endpointTLS)

	if publishErr != nil {
		delete(bus.handlers, address)
		if bus.enableLocal {
			delete(bus.localed.handlers, address)
		}
		err = fmt.Errorf("eventbus register event handler failed for publush into discovery, address is %s, %v", address, publishErr)
		return
	}

	bus.registrations = append(bus.registrations, registration)

	return
}

func (bus *clusterEventBus) Start(context context.Context) {
	if !bus.closed() {
		panic(fmt.Errorf("eventbus start failed, it is running"))
	}
	atomic.StoreInt64(&bus.running, int64(1))

	// local listen
	if bus.enableLocal {
		bus.localed.Start(context)
	}
	// listen
	bus.listen()
	// grpc serve
	bus.grpcServer.start()

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

	close(bus.requestMessageDispatcher)

	bus.grpcServer.close()

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

func (bus *clusterEventBus) listen() {
	for i := 0; i < bus.eventHandlerInstanceNumber; i++ {
		go func(bus *clusterEventBus) {
			for {
				requestMsg, ok := <-bus.requestMessageDispatcher
				if !ok {
					break
				}
				msg := requestMsg.message
				address, has := msg.getAddress()
				if !has || address == "" {
					panic(fmt.Errorf("eventbus handle event failed, address is empty, %v", msg))
				}
				tags, _ := msg.getTags()
				key := tagsAddress(address, tags)
				handler, existed := bus.handlers[key]
				replyCh := requestMsg.replyCh
				if !existed {
					if replyCh != nil {
						replyCh <- failedReplyMessage(fmt.Errorf("eventbus handle event failed, no handler handle address %s, %v", address, msg))
						close(replyCh)
					}
					continue
				}
				reply, handleErr := handler(msg.Head, msg.Body)
				if replyCh != nil {
					var replyMsg *message
					if handleErr != nil {
						replyMsg = failedReplyMessage(handleErr)
					} else {
						replyMsg = succeedReplyMessage(reply)
					}
					replyCh <- replyMsg
					close(replyCh)
				}
				bus.handleCount.Done()
			}
		}(bus)
	}
}

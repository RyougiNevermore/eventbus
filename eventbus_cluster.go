package eventbus

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"net"
	"sync"
	"sync/atomic"
)

const (
	defaultGroupName      = "_ebs"
)

type ClusterEventbusOption struct {
	Host                       string       `json:"host,omitempty"`
	Port                       int          `json:"port,omitempty"`
	PublicHost                 string       `json:"publicHost,omitempty"`
	PublicPort                 int          `json:"publicPort,omitempty"`
	TLS                        *EndpointTLS `json:"tls,omitempty"`
	EventChanCap               int          `json:"eventChanCap,omitempty"`
	EventHandlerInstanceNumber int          `json:"eventHandlerInstanceNumber,omitempty"`
}

// todo using grpc
type clusterEventBus struct {
	id                         string
	running                    int64
	ln                         *net.TCPListener
	discovery                  ServiceDiscovery
	enableLocal                bool
	inbound                    chan *message
	outboundsLock              *sync.Mutex
	outbounds                  map[string]chan *message // key is reply address
	handlers                   map[string]EventHandler  // key is address
	eventChanCap               int
	eventHandlerInstanceNumber int
	handleCount                *sync.WaitGroup
	localed                    Eventbus
}

func (bus *clusterEventBus) removeOutbound(mid string) {
	bus.outboundsLock.Lock()
	close(bus.outbounds[mid])
	delete(bus.outbounds, mid)
	bus.outboundsLock.Unlock()
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
	if _, has := bus.handlers[address]; has {
		err = bus.localed.Send(address, v, options...)
		return
	}

	// remote
	registration, has, getErr := bus.discovery.Get(defaultGroupName, address)
	if getErr != nil {
		err = fmt.Errorf("eventbus send failed, discovery %s failed, %v", address, getErr)
		return
	}
	if !has {
		err = fmt.Errorf("eventbus send failed, %s was not found", address)
		return
	}
	msg := newMessage(address, v, options)

	_, sendToRemoteErr := bus.sendToDiscovered(registration, msg)
	if sendToRemoteErr != nil {
		err = fmt.Errorf("eventbus send failed, send message to discovered failed, address is %s, %v", address, sendToRemoteErr)
		return
	}

	return
}

func (bus *clusterEventBus) replyAddress() string {
	return bus.id
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
	if _, has := bus.handlers[address]; has {
		reply = bus.localed.Request(address, v, options...)
		return
	}

	// remote
	registration, has, getErr := bus.discovery.Get(defaultGroupName, address)
	if getErr != nil {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, discovery %s failed, %v", address, getErr))
		return
	}
	if !has {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, %s was not found", address))
		return
	}
	msg := newMessage(address, v, options)

	reply0, sendToRemoteErr := bus.sendToDiscovered(registration, msg)
	if sendToRemoteErr != nil {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, send message to discovered failed, address is %s, %v", address, sendToRemoteErr))
		return
	}
	reply = reply0

	return
}

func (bus *clusterEventBus) RegisterHandler(address string, handler EventHandler) (err error) {
	panic("implement me")
}

func (bus *clusterEventBus) Start(context context.Context) {
	// todo register reply bound , name = bus.replyAddress
}

func (bus *clusterEventBus) Close(context context.Context) {

}

func (bus *clusterEventBus) closed() bool {
	return atomic.LoadInt64(&bus.running) == int64(0)
}

func encodeClusterMessage(msg *message) (v []byte) {

	return
}

func decodeClusterMessage(v []byte) (msg *message, err error) {

	return
}

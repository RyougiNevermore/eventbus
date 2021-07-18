package eventbus

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/dgraph-io/ristretto"
	"github.com/nats-io/nats.go"
	"github.com/rs/xid"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type NatsEventbusOption struct {
	Name                 string        `json:"name,omitempty"`
	Servers              []string      `json:"servers,omitempty"`
	Username             string        `json:"username,omitempty"`
	Password             string        `json:"password,omitempty"`
	MaxReconnects        int           `json:"maxReconnects,omitempty"`
	ReconnectWaitSecond  int           `json:"reconnectWaitSecond,omitempty"`
	RetryOnFailedConnect bool          `json:"retryOnFailedConnect,omitempty"`
	Meta                 *EndpointMeta `json:"meta,omitempty"`
	Tags                 []string      `json:"tags,omitempty"`
	TLS                  *EndpointTLS  `json:"tls,omitempty"`
	EventChanCap         int           `json:"eventChanCap,omitempty"`
	EventWorkers         int           `json:"eventWorkers,omitempty"`
}

func NewNatsEventbus(discovery ServiceDiscovery, option NatsEventbusOption) (bus Eventbus, err error) {
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

	servers := option.Servers
	if servers == nil || len(servers) == 0 {
		err = fmt.Errorf("create cluster eventbus failed, servers is nil")
		return
	}

	replySubject := ""

	addr := strings.Join(servers, ",")
	opts := make([]nats.Option, 0, 1)
	name := strings.TrimSpace(option.Name)
	if name != "" {
		opts = append(opts, nats.Name(name))
		replySubject = fmt.Sprintf("%s:reply:%s", name, xid.New().String())
	} else {
		replySubject = fmt.Sprintf("%s:reply:%s", hostname(), xid.New().String())
	}

	username := strings.TrimSpace(option.Username)
	password := strings.TrimSpace(option.Password)
	if username != "" && password != "" {
		opts = append(opts, nats.UserInfo(name, password))
	}
	maxReconnects := option.MaxReconnects
	if maxReconnects > 0 {
		opts = append(opts, nats.MaxReconnects(maxReconnects))
	}
	reconnectWaitSecond := option.ReconnectWaitSecond
	if reconnectWaitSecond > 0 {
		opts = append(opts, nats.ReconnectWait(time.Duration(reconnectWaitSecond)*time.Second))
	}
	if option.RetryOnFailedConnect {
		opts = append(opts, nats.RetryOnFailedConnect(true))
	}

	if option.TLS != nil {
		tlsConfig, tlsConfigErr := option.TLS.ToClientTLSConfig()
		if tlsConfigErr != nil {
			err = fmt.Errorf("create cluster eventbus failed, make nats tlc config failed, %v", tlsConfigErr)
			return
		}
		opts = append(opts, nats.Secure(tlsConfig))
	}
	nc, ncErr := nats.Connect(addr, opts...)
	if ncErr != nil {
		err = fmt.Errorf("create cluster eventbus failed, connect to nats failed, %v", ncErr)
		return
	}

	eb := &natsEventbus{
		running:        0,
		requestWorkers: nil,
		discovery:      discovery,
		conn:           nc,
		handlersLock:   new(sync.Mutex),
		handlers:       make(map[string]EventHandler),
		handleCount:    new(sync.WaitGroup),
		registrations:  make([]Registration, 0, 1),
		replySubject:   replySubject,
	}

	replySub, replySubErr := nc.Subscribe(replySubject, eb.handleReplyRequestMessage)
	if replySubErr != nil {
		err = fmt.Errorf("create cluster eventbus failed, create reply subscription failed, %v", replySubErr)
		return
	}
	eb.replySubscription = replySub

	// workers
	wp := &workerPool{
		WorkerFunc:      eb.handleRequestMessage,
		MaxWorkersCount: eventWorkers,
	}

	eb.requestWorkers = wp

	bus = eb

	return
}

type natsEventbus struct {
	running           int64
	requestWorkers    *workerPool
	discovery         ServiceDiscovery
	conn              *nats.Conn
	handlersLock      *sync.Mutex
	handlers          map[string]EventHandler
	handleCount       *sync.WaitGroup
	registrations     []Registration
	replySubject      string
	replies           *ristretto.Cache
	replySubscription *nats.Subscription
}

func (bus *natsEventbus) getRemoteSubject(address string, options []DeliveryOptions) (sub string, has bool, err error) {
	tags := tagsFromDeliveryOptions(options...)
	reg, has0, getErr := bus.discovery.Get(defaultGroupName, address, tags...)
	if getErr != nil {
		err = fmt.Errorf("event bus get remote address failed, %v", getErr)
		return
	}
	if !has0 {
		return
	}
	sub = reg.Address()
	if sub != "" {
		has = true
	}
	return
}

func (bus *natsEventbus) Send(address string, v interface{}, options ...DeliveryOptions) (err error) {
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
	sub, has, getErr := bus.getRemoteSubject(address, options)
	if getErr != nil {
		err = errors.ServiceError(getErr.Error())
		return
	}
	if !has {
		err = errors.NotFoundError(fmt.Sprintf("eventbus send failed, %s is not found", address))
		return
	}

	pubErr := bus.conn.Publish(sub, msg.toJson())
	if pubErr != nil {
		err = errors.ServiceError(fmt.Sprintf("eventbus send failed, nats publish failed, %v", pubErr))
		return
	}
	return
}

func (bus *natsEventbus) Request(address string, v interface{}, options ...DeliveryOptions) (reply ReplyFuture) {
	if address == "" {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, address is empty"))
		return
	}

	if bus.closed() {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, eventbus has been closed"))
		return
	}

	msg := newMessage(address, v, options)

	if existed := bus.addressExisted(address, options...); existed {
		rm := &requestMessage{
			message: msg,
			replyCh: nil,
		}

		if !bus.requestWorkers.SendRequestMessage(rm) {
			reply = newFailedFuture(fmt.Errorf("eventbus request failed, send to workers failed"))
			return
		}
		return
	}
	// remote
	sub, has, getErr := bus.getRemoteSubject(address, options)
	if getErr != nil {
		reply = newFailedFuture(getErr)
		return
	}
	if !has {
		reply = newFailedFuture(errors.NotFoundError(fmt.Sprintf("eventbus request failed, %s is not found", address)))
		return
	}

	replyId := xid.New().String()
	replyCh := make(chan *message, 1)
	reply = newFuture(replyCh)
	bus.replies.Set(replyId, reply, 1)
	msg.putReplyAddress(replyId)

	pubErr := bus.conn.PublishRequest(sub, bus.replySubject, msg.toJson())

	if pubErr != nil {
		bus.replies.Del(replyId)
		replyCh <- failedReplyMessage(fmt.Errorf("eventbus send failed, nats publish failed, %v", pubErr))
		close(replyCh)
		return
	}


	return
}

func (bus *natsEventbus) RegisterHandler(address string, handler EventHandler, tags ...string) (err error) {
	return
}

func (bus *natsEventbus) RegisterLocalHandler(address string, handler EventHandler, tags ...string) (err error) {
	return
}

func (bus *natsEventbus) Start(context context.Context) {
	return
}

func (bus *natsEventbus) Close(context context.Context) {
	return
}

func (bus *natsEventbus) closed() bool {
	return atomic.LoadInt64(&bus.running) == int64(0)
}

func (bus *natsEventbus) addressExisted(address string, options ...DeliveryOptions) (has bool) {
	tags := tagsFromDeliveryOptions(options...)
	key := tagsAddress(address, tags)
	_, has = bus.handlers[key]
	return
}

func (bus *natsEventbus) handleRequestMessage(requestMsg *requestMessage) (err error) {
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

func (bus *natsEventbus) handleReplyRequestMessage(msg *nats.Msg) {

}

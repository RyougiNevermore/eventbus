package eventbus

import (
	"context"
	"fmt"
	"github.com/aacfactory/cluster"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/workers"
	"github.com/dgraph-io/ristretto"
	"github.com/nats-io/nats.go"
	"github.com/rs/xid"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type NatsEventbusOption struct {
	Name                     string        `json:"name,omitempty"`
	Servers                  []string      `json:"servers,omitempty"`
	Username                 string        `json:"username,omitempty"`
	Password                 string        `json:"password,omitempty"`
	MaxReconnects            int           `json:"maxReconnects,omitempty"`
	ReconnectWaitSecond      int           `json:"reconnectWaitSecond,omitempty"`
	RetryOnFailedConnect     bool          `json:"retryOnFailedConnect,omitempty"`
	Meta                     cluster.ServiceMeta `json:"meta,omitempty"`
	Tags                     []string      `json:"tags,omitempty"`
	TLS                      cluster.ServiceTLS  `json:"tls,omitempty"`
	EventChanCap             int           `json:"eventChanCap,omitempty"`
	WorkersMaxIdleTime       time.Duration `json:"workersMaxIdleTime,omitempty"`
	WorkersCommandTimeout    time.Duration `json:"workersCommandTimeout,omitempty"`
	WorkersCommandBufferSize int           `json:"workersCommandBufferSize,omitempty"`
	Workers                  int           `json:"workers,omitempty"`
}

func NewNatsEventbus(discovery cluster.ServiceDiscovery, option NatsEventbusOption) (bus Eventbus, err error) {
	if discovery == nil {
		err = fmt.Errorf("create cluster eventbus failed, dicovery is nil")
		return
	}

	eventChanCap := option.EventChanCap
	if eventChanCap < 1 {
		eventChanCap = getDefaultEventChanCap()
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

	if option.TLS.Enable() {
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
		running:               0,
		requestWorkers:        nil,
		discovery:             discovery,
		conn:                  nc,
		handlers:              newLocalEventHandleStore(),
		registrationsLock:     sync.Mutex{},
		registrations:         make([]cluster.Registration, 0, 1),
		replySubject:          replySubject,
		replies:               nil,
		replySubscription:     nil,
		addrSubscriptionsLock: sync.Mutex{},
		addrSubscriptions:     make([]*nats.Subscription, 0, 1),
	}

	cache, newCacheErr := ristretto.NewCache(&ristretto.Config{
		NumCounters: defaultGrpcClientCacheNumCounters,
		MaxCost:     defaultGrpcClientCacheMaxCost,
		BufferItems: 64,
		OnEvict:     eb.onEvictConn,
	})

	if newCacheErr != nil {
		err = fmt.Errorf("eventbus new cluster client cache failed, %v", newCacheErr)
		return
	}

	eb.replies = cache

	replySub, replySubErr := nc.Subscribe(replySubject, eb.subNatsReply)
	if replySubErr != nil {
		err = fmt.Errorf("create cluster eventbus failed, create reply subscription failed, %v", replySubErr)
		return
	}
	eb.replySubscription = replySub

	// workers
	rw := workers.NewWorkers(workers.Option{
		MaxWorkerNum:      option.Workers,
		MaxIdleTime:       option.WorkersMaxIdleTime,
		CommandTimeout:    option.WorkersCommandTimeout,
		CommandBufferSize: option.WorkersCommandBufferSize,
		Fn:                eb.handleRequestMessageWorkFn,
	})

	eb.requestWorkers = rw

	bus = eb

	return
}

type natsEventbus struct {
	running               int64
	requestWorkers        *workers.Workers
	discovery             cluster.ServiceDiscovery
	conn                  *nats.Conn
	handlers              *localEventHandleStore
	registrationsLock     sync.Mutex
	registrations         []cluster.Registration
	replySubject          string
	replies               *ristretto.Cache
	replySubscription     *nats.Subscription
	addrSubscriptionsLock sync.Mutex
	addrSubscriptions     []*nats.Subscription
}

func (bus *natsEventbus) onEvictConn(item *ristretto.Item) {
	rm, ok := item.Value.(*requestMessage)
	if !ok || rm == nil {
		return
	}
	rm.failed(errors.ServiceError("timeout"))
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
	sub = reg.Id()
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

	tags, _ := msg.getTags()

	rm := &requestMessage{
		message: msg,
		replyCh: nil,
	}

	//local
	if bus.handlers.contains(address, tags) {
		if !bus.requestWorkers.Command(rm) {
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

	tags, _ := msg.getTags()

	replyCh := make(chan *message, 1)
	rm := &requestMessage{
		message: msg,
		replyCh: replyCh,
	}

	reply = newFuture(replyCh)

	// local
	if bus.handlers.contains(address, tags) {
		if !bus.requestWorkers.Command(rm) {
			rm.failed(fmt.Errorf("eventbus send failed, send to workers failed"))
			return
		}
		return
	}

	// remote
	sub, has, getErr := bus.getRemoteSubject(address, options)
	if getErr != nil {
		rm.failed(getErr)
		return
	}
	if !has {
		rm.failed(errors.NotFoundError(fmt.Sprintf("eventbus request failed, %s is not found", address)))
		return
	}

	replyId := xid.New().String()
	bus.replies.Set(replyId, rm, 1)
	bus.replies.Wait()
	msg.putReplyAddress(replyId)

	pubErr := bus.conn.PublishRequest(sub, bus.replySubject, msg.toJson())

	if pubErr != nil {
		bus.replies.Del(replyId)
		bus.replies.Wait()
		rm.failed(fmt.Errorf("eventbus send failed, nats publish failed, %v", pubErr))
		return
	}

	return
}

func (bus *natsEventbus) RegisterHandler(address string, handler EventHandler, tags ...string) (err error) {
	bus.registrationsLock.Lock()
	defer bus.registrationsLock.Unlock()

	err = bus.RegisterLocalHandler(address, handler, tags...)

	if err != nil {
		return
	}

	tags = tagsClean(tags)

	registration, publishErr := bus.discovery.Publish(defaultGroupName, address, "nats", address, tags, nil, nil)

	if publishErr != nil {
		bus.handlers.remove(address, tags)
		err = fmt.Errorf("eventbus register event handler failed for publush into discovery, address is %s, %v", address, publishErr)
		return
	}

	err = bus.subNatsRequest(registration)
	if err != nil {
		bus.handlers.remove(address, tags)
		_ = bus.discovery.UnPublish(registration)
		err = fmt.Errorf("eventbus register event handler failed for publush into discovery, address is %s, %v", address, err)
		return
	}

	bus.registrations = append(bus.registrations, registration)

	return
}

func (bus *natsEventbus) RegisterLocalHandler(address string, handler EventHandler, tags ...string) (err error) {
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

func (bus *natsEventbus) Start(context context.Context) {
	if !bus.closed() {
		panic(fmt.Errorf("eventbus start failed, it is running"))
	}
	atomic.StoreInt64(&bus.running, int64(1))

	// workers
	bus.requestWorkers.Start()
	return
}

func (bus *natsEventbus) Close(context context.Context) {
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

	_ = bus.replySubscription.Unsubscribe()
	for _, subscription := range bus.addrSubscriptions {
		_ = subscription.Unsubscribe()
	}

	closeCh := make(chan struct{}, 1)

	go func(closeCh chan struct{}, eb *natsEventbus) {

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
	return
}

func (bus *natsEventbus) closed() bool {
	return atomic.LoadInt64(&bus.running) == int64(0)
}

func (bus *natsEventbus) subNatsReply(natsMsg *nats.Msg) {
	if string(natsMsg.Data) == "+ACK" {
		return
	}
	msg := &message{}
	decodeErr := jsonAPI().Unmarshal(natsMsg.Data, msg)
	if decodeErr != nil {
		return
	}
	replyId, has := msg.getReplyAddress()
	if !has {
		return
	}
	reply0, hasReply := bus.replies.Get(replyId)
	if !hasReply {
		return
	}
	reply, ok := reply0.(*requestMessage)

	if !ok {
		return
	}
	if reply.needReply() {
		reply.sendMessage(msg)
	}
	bus.replies.Del(replyId)
	bus.replies.Wait()
	return
}

func (bus *natsEventbus) handleRequestMessageWorkFn(v interface{}, meta map[string]string) {
	if v == nil {
		return
	}
	requestMsg, ok := v.(*requestMessage)
	if !ok {
		return
	}

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
	return
}

func (bus *natsEventbus) subNatsRequest(registration cluster.Registration) (err error) {
	bus.addrSubscriptionsLock.Lock()
	defer bus.addrSubscriptionsLock.Unlock()

	sub, subErr := bus.conn.Subscribe(registration.Id(), func(natsMsg *nats.Msg) {

		_ = natsMsg.Ack()

		reply := natsMsg.Reply
		msg := &message{}
		decodeErr := jsonAPI().Unmarshal(natsMsg.Data, msg)
		if decodeErr != nil {
			if reply != "" {
				_ = natsMsg.Respond(failedReplyMessage(fmt.Errorf("decode message failed")).toJson())
				return
			}
			return
		}

		replyCh := make(chan *message, 1)
		rm := &requestMessage{
			message: msg,
			replyCh: replyCh,
		}

		if !bus.requestWorkers.Command(rm) {
			rm.failed(fmt.Errorf("eventbus send failed, send to workers failed"))
			return
		}

		result := <-replyCh
		if result != nil {
			replyId, _ := msg.getReplyAddress()
			result.putReplyAddress(replyId)
			_ = natsMsg.Respond(result.toJson())
		}

	})

	if subErr != nil {
		err = fmt.Errorf("eventbus sub failed, %v", subErr)
		return
	}

	bus.addrSubscriptions = append(bus.addrSubscriptions, sub)

	return
}

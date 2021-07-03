package eventbus

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/rs/xid"
	"runtime"
	"sync"
	"sync/atomic"
)

func NewEventbus() Eventbus {
	return NewEventbusWithOption(LocaledEventbusOption{EventChanCap: runtime.NumCPU() * 64, EventHandlerInstanceNumber: runtime.NumCPU() * 2})
}

func NewEventbusWithOption(option LocaledEventbusOption) (eb *localedEventbus) {

	eventChanCap := option.EventChanCap
	if eventChanCap < 1 {
		eventChanCap = runtime.NumCPU() * 64
	}
	eventHandlerInstanceNumber := option.EventHandlerInstanceNumber
	if eventHandlerInstanceNumber < 1 {
		eventHandlerInstanceNumber = runtime.NumCPU() * 2
	}

	eb = &localedEventbus{
		running:                    int64(1),
		lock:                       new(sync.Mutex),
		inbound:                    make(chan *message, eventChanCap),
		outbounds:                  make(map[string]chan *message),
		handlers:                   make(map[string]EventHandler),
		handleCount:                new(sync.WaitGroup),
		eventChanCap:               eventChanCap,
		eventHandlerInstanceNumber: eventHandlerInstanceNumber,
	}

	eb.listen()
	return
}

type LocaledEventbusOption struct {
	EventChanCap               int
	EventHandlerInstanceNumber int
}

type localedEventbus struct {
	running                    int64
	lock                       *sync.Mutex
	inbound                    chan *message
	outbounds                  map[string]chan *message // key is reply address
	handlers                   map[string]EventHandler  // key is address
	eventChanCap               int
	eventHandlerInstanceNumber int
	handleCount                *sync.WaitGroup
}

func (eb *localedEventbus) Send(address string, v interface{}, options ...DeliveryOptions) (err error) {
	if address == "" {
		err = errors.ServiceError("eventbus send failed, address is empty")
		return
	}

	if addressErr := eb.addressExisted(address); addressErr != nil {
		err = addressErr
		return
	}

	_msg := newMessage(v)
	if options != nil && len(options) > 0 {
		for _, option := range options {
			_msg.Head.Merge(MultiMap(option))
		}
	}
	_msg.putAddress(address)

	if eb.closed() {
		err = errors.ServiceError("eventbus send failed, eventbus has been closed")
		return
	}

	eb.inbound <- _msg
	eb.handleCount.Add(1)

	return
}

func (eb *localedEventbus) Request(address string, v interface{}, options ...DeliveryOptions) (reply *ReplyFuture) {
	if address == "" {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, address is empty"))
		return
	}

	if addressErr := eb.addressExisted(address); addressErr != nil {
		reply = newFailedFuture(addressErr)
		return
	}

	_msg := newMessage(v)
	if options != nil && len(options) > 0 {
		for _, option := range options {
			_msg.Head.Merge(MultiMap(option))
		}
	}
	_msg.putAddress(address)
	replyAddress := fmt.Sprintf("%s:%s", address, xid.New().String())
	_msg.putReplyAddress(replyAddress)

	if eb.closed() {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, eventbus has been closed"))
		return
	}

	eb.inbound <- _msg
	eb.handleCount.Add(1)

	replyCh := make(chan *message, 1)

	eb.lock.Lock()
	eb.outbounds[replyAddress] = replyCh
	eb.lock.Unlock()

	reply = newFuture(replyCh)

	return
}

func (eb *localedEventbus) RegisterHandler(address string, handler EventHandler) (err error) {
	eb.lock.Lock()
	defer eb.lock.Unlock()

	if address == "" {
		err = errors.ServiceError("eventbus bind event handler failed, address is empty")
		return
	}
	if handler == nil {
		err = errors.ServiceError("eventbus bind event handler failed, handler is nil")
		return
	}
	_, has := eb.handlers[address]
	if has {
		err = errors.ServiceError("eventbus bind event handler failed, address event handler has been bound")
		return
	}

	eb.handlers[address] = handler

	return
}

func (eb *localedEventbus) Close(context context.Context) {
	if eb.closed() {
		panic(fmt.Errorf("eventbus has been closed, close falied"))
	}
	if !atomic.CompareAndSwapInt64(&eb.running, int64(1), int64(0)) {
		panic(fmt.Errorf("eventbus is not running, close failed"))
	}

	close(eb.inbound)

	closeCh := make(chan struct{}, 1)

	go func(closeCh chan struct{}, eb *localedEventbus) {
		eb.handleCount.Wait()
		closeCh <- struct{}{}
	}(closeCh, eb)

	select {
	case <-context.Done():
		return
	case <-closeCh:
		return
	default:
	}

}

func (eb *localedEventbus) closed() bool {
	return atomic.LoadInt64(&eb.running) == int64(0)
}

func (eb *localedEventbus) addressExisted(address string) (err error) {
	_, has := eb.handlers[address]
	if !has {
		err = errors.ServiceError(fmt.Sprintf("event handler for address[%s] is not bound", address))
	}
	return
}

func (eb *localedEventbus) listen() {
	for i := 0; i < eb.eventHandlerInstanceNumber; i++ {
		go func(eb *localedEventbus) {
			for {
				msg, ok := <-eb.inbound
				if !ok {
					break
				}
				address, has := msg.getAddress()
				if !has || address == "" {
					panic(fmt.Errorf("eventbus handle event failed, address is empty, %v", msg))
				}
				handler, existed := eb.handlers[address]
				if !existed {
					panic(fmt.Errorf("eventbus handle event failed, no handler handle address %s, %v", address, msg))

				}
				reply, handleErr := handler(context.TODO(), msg)

				replyAddress, needReply := msg.getReplyAddress()
				if needReply {
					eb.lock.Lock()
					replyCh := eb.outbounds[replyAddress]
					delete(eb.outbounds, replyAddress)
					eb.lock.Unlock()
					var replyMsg *message
					if handleErr != nil {
						replyMsg = newFailedMessage(handleErr)
					} else {
						replyMsg = newMessage(reply)
					}
					replyCh <- replyMsg
					close(replyCh)
				}

				eb.handleCount.Done()
			}
		}(eb)
	}
}

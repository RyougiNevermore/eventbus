package eventbus

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"runtime"
	"strings"
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
		running:                    int64(0),
		handlersLock:               new(sync.Mutex),
		requestCh:                  make(chan *requestMessage, eventChanCap),
		handlers:                   make(map[string]EventHandler),
		handleCount:                new(sync.WaitGroup),
		eventHandlerInstanceNumber: eventHandlerInstanceNumber,
	}

	return
}

type LocaledEventbusOption struct {
	EventChanCap               int
	EventHandlerInstanceNumber int
}

type localedEventbus struct {
	running                    int64
	handlersLock               *sync.Mutex
	requestCh                  chan *requestMessage
	handlers                   map[string]EventHandler // key is address
	eventHandlerInstanceNumber int
	handleCount                *sync.WaitGroup
}

func (eb *localedEventbus) Send(address string, v interface{}, options ...DeliveryOptions) (err error) {
	if address == "" {
		err = errors.ServiceError("eventbus send failed, address is empty")
		return
	}

	if existed := eb.addressExisted(address, options...); !existed {
		err = errors.ServiceError(fmt.Sprintf("eventbus send failed, event handler for address[%s] is not bound", address))
		return
	}

	msg := newMessage(address, v, options)

	if eb.closed() {
		err = errors.ServiceError("eventbus send failed, eventbus has been closed")
		return
	}

	eb.requestCh <- &requestMessage{
		message: msg,
		replyCh: nil,
	}
	eb.handleCount.Add(1)

	return
}

func (eb *localedEventbus) Request(address string, v interface{}, options ...DeliveryOptions) (reply ReplyFuture) {
	if address == "" {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, address is empty"))
		return
	}

	if existed := eb.addressExisted(address, options...); !existed {
		reply = newFailedFuture(errors.ServiceError(fmt.Sprintf("eventbus request failed, event handler for address[%s] is not bound", address)))
		return
	}

	msg := newMessage(address, v, options)

	replyCh := make(chan *message, 1)
	reply = newFuture(replyCh)

	if eb.closed() {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, eventbus has been closed"))
		return
	}

	eb.requestCh <- &requestMessage{
		message: msg,
		replyCh: replyCh,
	}
	eb.handleCount.Add(1)

	return
}

func (eb *localedEventbus) RegisterHandler(address string, handler EventHandler, tags ...string) (err error) {
	if !eb.closed() {
		err = fmt.Errorf("eventbus register handler failed, it is running")
		return
	}

	eb.handlersLock.Lock()
	defer eb.handlersLock.Unlock()

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
	key := tagsAddress(address, tags)
	_, has := eb.handlers[key]
	if has {
		err = errors.ServiceError("eventbus register event handler failed, address event handler has been bound")
		return
	}

	eb.handlers[key] = handler

	return
}

func (eb *localedEventbus) Start(_ context.Context) {
	if !eb.closed() {
		panic(fmt.Errorf("eventbus start failed, it is running"))
	}
	atomic.StoreInt64(&eb.running, int64(1))
	eb.listen()
}

func (eb *localedEventbus) Close(context context.Context) {
	if eb.closed() {
		panic(fmt.Errorf("eventbus has been closed, close falied"))
	}
	if !atomic.CompareAndSwapInt64(&eb.running, int64(1), int64(0)) {
		panic(fmt.Errorf("eventbus is not running, close failed"))
	}

	close(eb.requestCh)

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

func (eb *localedEventbus) addressExisted(address string, options ...DeliveryOptions) (has bool) {
	tags := tagsFromDeliveryOptions(options...)
	key := tagsAddress(address, tags)
	_, has = eb.handlers[key]
	return
}

func (eb *localedEventbus) listen() {
	for i := 0; i < eb.eventHandlerInstanceNumber; i++ {
		go func(eb *localedEventbus) {
			for {
				requestMsg, ok := <-eb.requestCh
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
				handler, existed := eb.handlers[key]
				replyCh := requestMsg.replyCh
				if !existed {
					if replyCh != nil {
						replyCh <- failedReplyMessage(fmt.Errorf("eventbus handle event failed, no handler handle address %s, %v", address, msg))
						close(replyCh)
					}
					continue
				}
				reply, handleErr := handler(&defaultEvent{head: defaultEventHead{msg.Head}, body: msg.Body})
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
				eb.handleCount.Done()
			}
		}(eb)
	}
}

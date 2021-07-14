package eventbus

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
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
		running:                    int64(0),
		lock:                       new(sync.Mutex),
		requestCh:                  make(chan *RequestMessage, eventChanCap),
		handlers:                   make(map[string]EventHandler),
		handleCount:                new(sync.WaitGroup),
		eventChanCap:               eventChanCap,
		eventHandlerInstanceNumber: eventHandlerInstanceNumber,
	}

	return
}

type RequestMessage struct {
	message *message
	replyCh chan<- *message
}

type LocaledEventbusOption struct {
	EventChanCap               int
	EventHandlerInstanceNumber int
}

type localedEventbus struct {
	running                    int64
	lock                       *sync.Mutex
	requestCh                  chan *RequestMessage
	handlers                   map[string]EventHandler // key is address
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

	msg := newMessage(address, v, options)

	if eb.closed() {
		err = errors.ServiceError("eventbus send failed, eventbus has been closed")
		return
	}

	eb.requestCh <- &RequestMessage{
		message: msg,
		replyCh: nil,
	}
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

	msg := newMessage(address, v, options)

	replyCh := make(chan *message, 1)
	reply = newFuture(replyCh)

	if eb.closed() {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, eventbus has been closed"))
		return
	}

	eb.requestCh <- &RequestMessage{
		message: msg,
		replyCh: replyCh,
	}
	eb.handleCount.Add(1)

	return
}

func (eb *localedEventbus) RegisterHandler(address string, handler EventHandler) (err error) {
	if !eb.closed() {
		err = fmt.Errorf("eventbus register handler failed, it is running")
		return
	}

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

func (eb *localedEventbus) Start(context context.Context) {
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
				requestMessage, ok := <-eb.requestCh
				if !ok {
					break
				}
				msg := requestMessage.message
				address, has := msg.getAddress()
				if !has || address == "" {
					panic(fmt.Errorf("eventbus handle event failed, address is empty, %v", msg))
				}
				handler, existed := eb.handlers[address]
				if !existed {
					panic(fmt.Errorf("eventbus handle event failed, no handler handle address %s, %v", address, msg))

				}
				reply, handleErr := handler(msg.Head, msg.Body)
				replyCh := requestMessage.replyCh
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

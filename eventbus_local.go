package eventbus

import (
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/workers"
	"strings"
	"sync/atomic"
	"time"
)

func NewEventbus() Eventbus {
	return NewEventbusWithOption(LocaledEventbusOption{EventChanCap: 0, Workers: 0})
}

func NewEventbusWithOption(option LocaledEventbusOption) (eb *localedEventbus) {

	eventChanCap := option.EventChanCap
	if eventChanCap < 1 {
		eventChanCap = getDefaultEventChanCap()
	}
	eventWorkers := option.Workers

	eb = &localedEventbus{
		running:  int64(0),
		handlers: newLocalEventHandleStore(),
	}

	rw := workers.NewWorkers(workers.Option{
		MaxWorkerNum:      eventWorkers,
		MaxIdleTime:       option.WorkersMaxIdleTime,
		CommandTimeout:    option.WorkersCommandTimeout,
		CommandBufferSize: option.WorkersCommandBufferSize,
		Fn:                eb.handleRequestMessageWorkFn,
	})

	eb.requestWorkers = rw

	return
}

type LocaledEventbusOption struct {
	EventChanCap             int           `json:"eventChanCap,omitempty"`
	WorkersMaxIdleTime       time.Duration `json:"workersMaxIdleTime,omitempty"`
	WorkersCommandTimeout    time.Duration `json:"workersCommandTimeout,omitempty"`
	WorkersCommandBufferSize int           `json:"workersCommandBufferSize,omitempty"`
	Workers                  int           `json:"workers,omitempty"`
}

type localedEventbus struct {
	running        int64
	requestWorkers *workers.Workers
	handlers       *localEventHandleStore
}

func (eb *localedEventbus) Send(address string, v interface{}, options ...DeliveryOptions) (err error) {
	if address == "" {
		err = errors.ServiceError("eventbus send failed, address is empty")
		return
	}

	msg := newMessage(address, v, options)

	tags, _ := msg.getTags()
	if !eb.handlers.contains(address, tags) {
		err = errors.ServiceError(fmt.Sprintf("eventbus send failed, %s has not be registered", address))
		return
	}

	if eb.closed() {
		err = errors.ServiceError("eventbus send failed, eventbus has been closed")
		return
	}

	rm := &requestMessage{
		message: msg,
		replyCh: nil,
	}

	if !eb.requestWorkers.Command(rm) {
		err = errors.ServiceError("eventbus send failed, send to workers failed")
		return
	}

	return
}

func (eb *localedEventbus) Request(address string, v interface{}, options ...DeliveryOptions) (reply ReplyFuture) {
	if address == "" {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, address is empty"))
		return
	}

	msg := newMessage(address, v, options)

	replyCh := make(chan *message, 1)
	reply = newFuture(replyCh)

	if eb.closed() {
		reply = newFailedFuture(fmt.Errorf("eventbus request failed, eventbus has been closed"))
		return
	}

	rm := &requestMessage{
		message: msg,
		replyCh: replyCh,
	}

	if !eb.requestWorkers.Command(rm) {
		rm.replyCh <- failedReplyMessage(errors.ServiceError("eventbus send failed, send to workers failed"))
		close(rm.replyCh)
	}
	return
}

func (eb *localedEventbus) RegisterHandler(address string, handler EventHandler, tags ...string) (err error) {
	if !eb.closed() {
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

	_, has := eb.handlers.get(address, tags)
	if has {
		err = errors.ServiceError("eventbus register event handler failed, address event handler has been bound")
		return
	}

	eb.handlers.put(address, tags, handler)

	return
}

func (eb *localedEventbus) RegisterLocalHandler(address string, handler EventHandler, tags ...string) (err error) {
	err = eb.RegisterHandler(address, handler, tags...)
	return
}

func (eb *localedEventbus) Start(_ context.Context) {
	if !eb.closed() {
		panic(fmt.Errorf("eventbus start failed, it is running"))
	}
	atomic.StoreInt64(&eb.running, int64(1))
	eb.requestWorkers.Start()
}

func (eb *localedEventbus) Close(context context.Context) {
	if eb.closed() {
		panic(fmt.Errorf("eventbus has been closed, close falied"))
	}
	if !atomic.CompareAndSwapInt64(&eb.running, int64(1), int64(0)) {
		panic(fmt.Errorf("eventbus is not running, close failed"))
	}

	eb.requestWorkers.Stop()

	closeCh := make(chan struct{}, 1)

	go func(closeCh chan struct{}, eb *localedEventbus) {
		eb.requestWorkers.Sync()
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

func (eb *localedEventbus) handleRequestMessageWorkFn(v interface{}, meta map[string]string) {
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
	handler, existed := eb.handlers.get(address, tags)
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

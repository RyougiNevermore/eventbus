package eventbus

import (
	"github.com/aacfactory/errors"
	"runtime"
)

func NewDeliveryOptions() DeliveryOptions {
	return &defaultDeliveryOptions{
		MultiMap{},
	}
}

type defaultDeliveryOptions struct {
	MultiMap
}

func (options *defaultDeliveryOptions) AddTag(tags ...string) {
	if tags == nil {
		return
	}
	tags = tagsClean(tags)
	if len(tags) > 0 {
		options.Put("tag", tags)
	}
}

func newFuture(ch <-chan *message) ReplyFuture {
	return &defaultReplyFuture{
		ch: ch,
	}
}

func newSucceedFuture(msg *message) ReplyFuture {
	ch := make(chan *message, 1)
	ch <- msg
	close(ch)
	return &defaultReplyFuture{
		ch: ch,
	}
}

func newFailedFuture(err error) ReplyFuture {
	ch := make(chan *message, 1)
	ch <- failedReplyMessage(err)
	close(ch)
	return &defaultReplyFuture{
		ch: ch,
	}
}

type defaultReplyFuture struct {
	ch <-chan *message
}

func (r *defaultReplyFuture) Get(v interface{}) (err error) {
	msg, ok := <-r.ch
	if !ok {
		err = errors.ServiceError("eventbus get reply failed, no reply")
		return
	}
	if msg.failed() {
		err = msg.cause()
		return
	}
	if (msg.Body == nil || len(msg.Body) == 0) && v != nil {
		err = errors.ServiceError("eventbus get reply failed, result is nil")
		return
	}
	err = jsonAPI().Unmarshal(msg.Body, v)
	return
}

type defaultEventHead struct {
	MultiMap
}

type defaultEvent struct {
	head defaultEventHead
	body []byte
}

func (e *defaultEvent) Head() EventHead {
	return e.head
}

func (e *defaultEvent) Body() []byte {
	return e.body
}

func getDefaultEventChanCap() int {
	return runtime.NumCPU() * 256
}

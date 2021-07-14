package eventbus

import (
	"context"
	"github.com/aacfactory/errors"
)

func NewDeliveryOptions() DeliveryOptions {
	return DeliveryOptions{
		MultiMap{},
	}
}

type DeliveryOptions struct {
	MultiMap
}

func (options *DeliveryOptions) AddTag(tags ...string) {
	if tags == nil {
		return
	}
	tags = tagsClean(tags)
	if len(tags) > 0 {
		options.Put("tag", tags)
	}
}

type Eventbus interface {
	Send(address string, v interface{}, options ...DeliveryOptions) (err error)
	Request(address string, v interface{}, options ...DeliveryOptions) (reply *ReplyFuture)
	RegisterHandler(address string, handler EventHandler, tags ...string) (err error)
	Start(context context.Context)
	Close(context context.Context)
}

type EventHandler func(head MultiMap, body []byte) (result interface{}, err error)

func newFuture(ch <-chan *message) *ReplyFuture {
	return &ReplyFuture{
		ch: ch,
	}
}

func newSucceedFuture(msg *message) *ReplyFuture {
	ch := make(chan *message, 1)
	ch <- msg
	close(ch)
	return &ReplyFuture{
		ch: ch,
	}
}

func newFailedFuture(err error) *ReplyFuture {
	ch := make(chan *message, 1)
	ch <- failedReplyMessage(err)
	close(ch)
	return &ReplyFuture{
		ch: ch,
	}
}

type ReplyFuture struct {
	ch <-chan *message
}

func (r *ReplyFuture) Result(v interface{}) (err error) {
	msg, _ := <-r.ch
	if msg.failed() {
		err = msg.cause()
		return
	}
	if (msg.Body == nil || len(msg.Body) == 0) && v != nil {
		err = errors.ServiceError("eventbus get reply failed, result is nil")
	}
	err = jsonAPI().Unmarshal(msg.Body, v)
	return
}

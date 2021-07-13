package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aacfactory/errors"
)

const (
	noReplyAddress = ""
)

func newMessage(address string, replyAddress string, v interface{}, options []DeliveryOptions) (msg *message) {
	msg = &message{
		Head: MultiMap{},
		Body: nil,
	}
	if v == nil {
		return msg
	}
	msg.Body = jsonEncode(v)
	if options != nil && len(options) > 0 {
		for _, option := range options {
			msg.Head.Merge(option.MultiMap)
		}
	}
	msg.putAddress(address)
	if replyAddress != noReplyAddress {
		msg.putReplyAddress(replyAddress)
	}
	return
}

func failedReplyMessage(err error) (msg *message) {
	msg = &message{
		Head: MultiMap{},
		Body: nil,
	}
	codeErr, transferred := errors.Transfer(err)
	if !transferred {
		codeErr = newReplyErr(err)
	}
	msg.Head.Add(messageHeadReplyErrorCause, "true")
	msg.Body = codeErr.ToJson()
	return
}

func succeedReplyMessage(v interface{}) (msg *message) {
	msg = &message{
		Head: MultiMap{},
		Body: nil,
	}
	if v == nil {
		return msg
	}
	msg.Body = jsonEncode(v)
	return
}

type message struct {
	Head MultiMap        `json:"header,omitempty"`
	Body json.RawMessage `json:"body,omitempty"`
}

func (msg *message) putAddress(address string) {
	msg.Head.Add(messageHeadAddress, address)
}

func (msg *message) getAddress() (string, bool) {
	return msg.Head.Get(messageHeadAddress)
}

func (msg *message) putReplyAddress(address string) {
	msg.Head.Add(messageHeadReplyAddress, address)
}

func (msg *message) getReplyAddress() (string, bool) {
	return msg.Head.Get(messageHeadReplyAddress)
}

func (msg *message) failed() (failed bool) {
	_, failed = msg.Head.Get(messageHeadReplyErrorCause)
	return
}

func (msg *message) cause() (err error) {
	if !msg.failed() {
		return
	}
	_err, ok := errors.FromJson(msg.Body)
	if ok {
		err = _err
	} else {
		panic(fmt.Errorf("get message cause failed, failed but body is not errors.CodeError"))
	}
	return
}

func NewDeliveryOptions() DeliveryOptions {
	return DeliveryOptions{
		MultiMap{},
	}
}

type DeliveryOptions struct {
	MultiMap
}

type Eventbus interface {
	Send(address string, v interface{}, options ...DeliveryOptions) (err error)
	Request(address string, v interface{}, options ...DeliveryOptions) (reply *ReplyFuture)
	RegisterHandler(address string, handler EventHandler) (err error)
	Start(context context.Context)
	Close(context context.Context)
}

type EventHandler func(head MultiMap, body []byte) (result interface{}, err error)

func newFuture(ch <-chan *message) *ReplyFuture {
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

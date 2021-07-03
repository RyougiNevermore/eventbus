package eventbus

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aacfactory/errors"
)

func NewMessage(v interface{}) (msg *Message) {
	msg = &Message{
		Head: MultiMap{},
		Body: nil,
	}
	if v == nil {
		return msg
	}
	msg.Body = jsonEncode(v)
	return
}

func newFailedMessage(err error) (msg *Message) {
	msg = &Message{
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

type Message struct {
	Head MultiMap        `json:"header,omitempty"`
	Body json.RawMessage `json:"body,omitempty"`
}

func (msg *Message) putAddress(address string) {
	msg.Head.Add(messageHeadAddress, address)
}

func (msg *Message) getAddress() (string, bool) {
	return msg.Head.Get(messageHeadAddress)
}

func (msg *Message) putReplyAddress(address string) {
	msg.Head.Add(messageHeadReplyAddress, address)
}

func (msg *Message) getReplyAddress() (string, bool) {
	return msg.Head.Get(messageHeadReplyAddress)
}

func (msg *Message) failed() (failed bool) {
	_, failed = msg.Head.Get(messageHeadReplyErrorCause)
	return
}

func (msg *Message) cause() (err error) {
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

type Eventbus interface {
	Send(context context.Context, address string, msg Message) (err error)
	Request(context context.Context, address string, msg Message) (reply *ReplyFuture)
	Handle(context context.Context, address string, handler EventHandler) (err error)
	Close(context context.Context)
}

type EventHandler func(context context.Context, msg *Message) (result interface{}, err error)

func newFuture(ch <-chan *Message) *ReplyFuture {
	return &ReplyFuture{
		ch: ch,
	}
}

func newFailedFuture(err error) *ReplyFuture {
	ch := make(chan *Message, 1)
	ch <- newFailedMessage(err)
	close(ch)
	return &ReplyFuture{
		ch: ch,
	}
}

type ReplyFuture struct {
	ch <-chan *Message
}

func (r *ReplyFuture) Result(v interface{}) (err error) {
	msg, _ := <-r.ch
	if msg.failed() {
		err = msg.cause()
		return
	}
	jsonDecode(msg.Body, v)
	return
}

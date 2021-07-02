package eventbus

import (
	"context"
	"encoding/json"
)

func NewMessage(v interface{}) (msg *Message) {
	msg = &Message{
		Head: MultiMap{},
		Body: nil,
	}
	if v == nil {
		return msg
	}
	msg.Body = JsonEncode(v)
	return
}

type Message struct {
	Head MultiMap        `json:"header,omitempty"`
	Body json.RawMessage `json:"body,omitempty"`
}

type Eventbus interface {
	Send(context context.Context, address string, msg *Message) (err error)
	Request(context context.Context, address string, msg *Message) (reply []byte, err error)
	Handle(context context.Context, address string, handler EventHandler) (err error)
	Close(context context.Context)
}

type EventHandler func(context context.Context, msg *Message) (reply []byte, err error)

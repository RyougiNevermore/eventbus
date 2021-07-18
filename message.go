package eventbus

import (
	"encoding/json"
	"fmt"
	"github.com/aacfactory/errors"
)

func newMessageFromJson(b []byte) (msg *message) {
	msg = &message{}
	jsonDecode(b, msg)
	return
}

func newMessage(address string, v interface{}, options []DeliveryOptions) (msg *message) {
	if address == "" {
		panic(fmt.Errorf("eventbus create message failed, address is empty"))
	}
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
			for _, k := range option.Keys() {
				ov, hasV := msg.Head.Values(k)
				if !hasV {
					ov = make([]string, 0, 1)
				}
				v, _ := option.Values(k)
				if v == nil {
					v = make([]string, 0, 1)
				}
				for i := 0; i < len(v); i++ {
					exists := false
					for _, ovv := range ov {
						if ovv == v[i] {
							exists = true
						}
					}
					if !exists {
						ov = append(ov, v[i])
					}
				}
				msg.Head.Put(k, ov)
			}
		}
	}
	msg.putAddress(address)
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
	msg.Head.Add(messageHeadReplyError, "true")
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

func newMessageFromBytes(head []byte, body []byte) (msg *message, err error) {
	msg = &message{
		Head: MultiMap{},
		Body: nil,
	}
	if body != nil && len(body) > 0 {
		msg.Body = body
	}
	if head != nil && len(head) > 0 {
		decodeErr := jsonAPI().Unmarshal(head, &msg.Head)
		if decodeErr != nil {
			msg = nil
			err = fmt.Errorf("eventbus create message from bytes failed, decode head failed, head is %s, %v", string(head), decodeErr)
			return
		}
	}
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
	msg.Head.Put(messageHeadReplyAddress, []string{address})
}

func (msg *message) getReplyAddress() (string, bool) {
	return msg.Head.Get(messageHeadReplyAddress)
}

func (msg *message) getTags() ([]string, bool) {
	return msg.Head.Values("tag")
}

func (msg *message) failed() (failed bool) {
	_, failed = msg.Head.Get(messageHeadReplyError)
	return
}

func (msg *message) toJson() []byte {
	return jsonEncode(msg)
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

type requestMessage struct {
	message *message
	replyCh chan<- *message
}

func (msg *requestMessage) needReply() (ok bool) {
	ok = msg.replyCh != nil
	return
}

func (msg *requestMessage) failed(err error) {
	if msg.replyCh == nil {
		return
	}
	msg.replyCh <- failedReplyMessage(err)
	close(msg.replyCh)
}

func (msg *requestMessage) succeed(v interface{}) {
	if msg.replyCh == nil {
		return
	}
	msg.replyCh <- succeedReplyMessage(v)
	close(msg.replyCh)
}

func (msg *requestMessage) sendMessage(v *message) {
	if msg.replyCh == nil {
		return
	}
	msg.replyCh <- v
	close(msg.replyCh)
}

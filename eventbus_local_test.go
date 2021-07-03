package eventbus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/eventbus"
	"testing"
	"time"
)

type Arg struct {
	Id       string    `json:"id,omitempty"`
	Num      int       `json:"num,omitempty"`
	Datetime time.Time `json:"datetime,omitempty"`
}

type Result struct {
	Value string `json:"value,omitempty"`
}

func HandlerReply(head eventbus.MultiMap, body []byte) (result interface{}, err error) {
	arg := &Arg{}
	_ = json.Unmarshal(body, arg)
	fmt.Println("handle reply", head, arg)
	if arg.Num < 0 {
		err = errors.InvalidArgumentErrorWithDetails("bad number", "num", "less than 0")
		return
	}
	result = &Result{
		Value: "result",
	}
	return
}

func HandlerVoid(head eventbus.MultiMap, body []byte) (result interface{}, err error) {
	arg := &Arg{}
	_ = json.Unmarshal(body, arg)
	fmt.Println("handle void", head, arg)
	return
}

func TestNewEventbus(t *testing.T) {

	eb := eventbus.NewEventbus()
	_ = eb.RegisterHandler("void", HandlerVoid)
	_ = eb.RegisterHandler("reply", HandlerReply)

	options := eventbus.NewDeliveryOptions()
	options.Add("h1", "1")
	options.Add("h2", "2")

	sendErr := eb.Send("reply", &Arg{
		Id:       "id",
		Num:      10,
		Datetime: time.Now(),
	}, options)

	if sendErr != nil {
		fmt.Println("send failed", sendErr)
	}

	for i := 0; i < 2; i++ {
		rf := eb.Request("reply", &Arg{
			Id:       "id",
			Num:      i - 1,
			Datetime: time.Now(),
		}, options)
		result := &Result{}
		requestErr := rf.Result(result)
		if requestErr != nil {
			fmt.Println("request failed", requestErr)
		} else {
			fmt.Println("request succeed", result)
		}
	}

	eb.Close(context.TODO())
}

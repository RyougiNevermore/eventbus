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

func TestNewClusterEventbus(t *testing.T) {

	discovery := NewTestDiscovery()

	c1, c1Err := createEventbusA(discovery)
	if c1Err != nil {
		t.Error(c1Err)
		return
	}
	defer c1.Close(context.TODO())

	c2, c2Err := createEventbusB(discovery)
	if c2Err != nil {
		t.Error(c2Err)
		return
	}
	defer c2.Close(context.TODO())

	var err error
	var reply eventbus.ReplyFuture
	result := &Result{}

	// c1 local good
	err = c1.Send("local", &Arg{
		Id:       "id",
		Num:      1,
		Datetime: time.Now(),
	})
	if err != nil {
		t.Error("c1 local", err)
	}
	// c1 send good
	err = c1.Send("send", &Arg{
		Id:       "id",
		Num:      1,
		Datetime: time.Now(),
	})
	if err != nil {
		t.Error("c1 send", err)
	}
	// c1 request good
	reply = c1.Request("b.request", &Arg{
		Id:       "id",
		Num:      1,
		Datetime: time.Now(),
	})
	err = reply.Get(result)
	if err != nil {
		t.Error("c1 request", err)
	}
	fmt.Println("c1 request result", result)

	// c2 local bad
	err = c2.Send("local", &Arg{
		Id:       "id",
		Num:      1,
		Datetime: time.Now(),
	})

	if err != nil {
		t.Log("c2 local bad", err)
	}
	// c1 send good
	err = c2.Send("send", &Arg{
		Id:       "id",
		Num:      1,
		Datetime: time.Now(),
	})
	if err != nil {
		t.Error("c1 send", err)
	}
	// c1 request good
	reply = c2.Request("b.request", &Arg{
		Id:       "id",
		Num:      1,
		Datetime: time.Now(),
	})
	err = reply.Get(result)
	if err != nil {
		t.Error("c2 request", err)
	}
	fmt.Println("c2 request result", result)

}

func createEventbusA(discovery eventbus.ServiceDiscovery) (bus eventbus.Eventbus, err error) {

	options := eventbus.ClusterEventbusOption{
		Host:         "0.0.0.0",
		Port:         9090,
		PublicHost:   "127.0.0.1",
		PublicPort:   0,
		Meta:         &eventbus.EndpointMeta{},
		Tags:         nil,
		TLS:          &eventbus.EndpointTLS{},
		EventChanCap: 64,
	}

	bus, err = eventbus.NewClusterEventbus(discovery, options)
	if err != nil {
		return
	}
	err = bus.RegisterLocalHandler("local", HandleRequestLocalOnly)
	if err != nil {
		return
	}

	err = bus.RegisterHandler("send", HandleSend)
	if err != nil {
		return
	}

	bus.Start(context.TODO())

	return
}

func createEventbusB(discovery eventbus.ServiceDiscovery) (bus eventbus.Eventbus, err error) {

	options := eventbus.ClusterEventbusOption{
		Host:         "0.0.0.0",
		Port:         9191,
		PublicHost:   "127.0.0.1",
		PublicPort:   0,
		Meta:         &eventbus.EndpointMeta{},
		Tags:         nil,
		TLS:          &eventbus.EndpointTLS{},
		EventChanCap: 64,
	}

	bus, err = eventbus.NewClusterEventbus(discovery, options)
	if err != nil {
		return
	}

	err = bus.RegisterHandler("b.send", HandleSend)
	if err != nil {
		return
	}
	err = bus.RegisterHandler("b.request", HandleRequest)
	if err != nil {
		return
	}

	bus.Start(context.TODO())

	return
}

func HandleRequestLocalOnly(event eventbus.Event) (result interface{}, err error) {

	fmt.Println("handle local only:", event.Head(), string(event.Body()))

	result = &Result{Value: time.Now().String()}

	return
}

func HandleSend(event eventbus.Event) (result interface{}, err error) {
	fmt.Println("handle send:", event.Head(), string(event.Body()))
	return
}

func HandleRequest(event eventbus.Event) (result interface{}, err error) {

	arg := &Arg{}
	decodeErr := json.Unmarshal(event.Body(), arg)
	if decodeErr != nil {
		err = errors.InvalidArgumentErrorWithDetails("bad body")
		return
	}

	fmt.Println("handle reply", event.Head(), string(event.Body()))
	if arg.Num < 0 {
		err = errors.InvalidArgumentErrorWithDetails("bad number", "num", "less than 0")
		return
	}
	result = &Result{
		Value: "succeed",
	}

	return
}

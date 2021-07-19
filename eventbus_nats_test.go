package eventbus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aacfactory/cluster"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/eventbus"
	"testing"
	"time"
)

func TestNewNatsEventbus(t *testing.T) {
	discovery := NewTestDiscovery()

	busA, errA := createEventbusNatsA(discovery)
	if errA != nil {
		t.Error(errA)
		return
	}
	defer busA.Close(context.TODO())
	busB, errB := createEventbusNatsB(discovery)
	if errB != nil {
		t.Error(errB)
		return
	}
	defer busB.Close(context.TODO())

	var err error
	var reply eventbus.ReplyFuture
	result := &Result{}

	// a
	err = busA.Send("a.send", &Arg{
		Id:       "id",
		Num:      1,
		Datetime: time.Now(),
	})
	fmt.Println("c1 send result", "a.send", err)
	err = busA.Send("b.send", &Arg{
		Id:       "id",
		Num:      1,
		Datetime: time.Now(),
	})
	fmt.Println("c1 send result", "b.send", err)
	reply = busA.Request("a.request", &Arg{
		Id:       "id",
		Num:      1,
		Datetime: time.Now(),
	})
	err = reply.Get(result)
	fmt.Println("c1 request", "a.request", result, err)
	reply = busA.Request("b.request", &Arg{
		Id:       "id",
		Num:      1,
		Datetime: time.Now(),
	})
	err = reply.Get(result)
	fmt.Println("c1 request", "b.request", result, err)

	// b
	reply = busA.Request("a.request", &Arg{
		Id:       "id",
		Num:      1,
		Datetime: time.Now(),
	})
	err = reply.Get(result)
	fmt.Println("c2 request", "a.request", result, err)
}

func createEventbusNatsA(discovery cluster.ServiceDiscovery) (bus eventbus.Eventbus, err error) {

	bus, err = eventbus.NewNatsEventbus(discovery, eventbus.NatsEventbusOption{
		Name:                 "A",
		Servers:              []string{"nats://120.55.167.188:14222"},
		Username:             "ruser",
		Password:             "T0pS3cr3t",
		MaxReconnects:        10,
		ReconnectWaitSecond:  3,
		RetryOnFailedConnect: true,
		EventChanCap:         64,
	})

	if err != nil {
		return
	}

	err = bus.RegisterHandler("a.send", HandleSend)
	if err != nil {
		return
	}
	err = bus.RegisterHandler("a.request", HandleRequest)
	if err != nil {
		return
	}

	bus.Start(context.TODO())

	return
}

func createEventbusNatsB(discovery cluster.ServiceDiscovery) (bus eventbus.Eventbus, err error) {

	bus, err = eventbus.NewNatsEventbus(discovery, eventbus.NatsEventbusOption{
		Name:                 "B",
		Servers:              []string{"nats://120.55.167.188:14222"},
		Username:             "ruser",
		Password:             "T0pS3cr3t",
		MaxReconnects:        10,
		ReconnectWaitSecond:  3,
		RetryOnFailedConnect: true,
		EventChanCap:         64,
	})

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

func natsHandleSend(event eventbus.Event) (result interface{}, err error) {
	fmt.Println("nats handle send:", event.Head(), string(event.Body()))
	return
}

func natsHandleRequest(event eventbus.Event) (result interface{}, err error) {

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

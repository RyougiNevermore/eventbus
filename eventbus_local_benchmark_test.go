package eventbus_test

import (
	"context"
	"encoding/json"
	"github.com/aacfactory/eventbus"
	"runtime"
	"testing"
	"time"
)

func HandlerReplyBenchmark(event eventbus.Event) (result interface{}, err error) {
	arg := &Arg{}
	_ = json.Unmarshal(event.Body(), arg)
	if arg.Num < 0 {
		return
	}
	result = &Result{
		Value: "result",
	}
	return
}

func HandlerVoidBenchmark(event eventbus.Event) (result interface{}, err error) {
	arg := &Arg{}
	_ = json.Unmarshal(event.Body(), arg)
	return
}

func BenchmarkNewEventbus(b *testing.B) {
	runtime.GOMAXPROCS(0)
	iterations := int64(b.N)

	eb := eventbus.NewEventbus()
	_ = eb.RegisterHandler("void", HandlerVoidBenchmark, "tag1")
	_ = eb.RegisterHandler("reply", HandlerReplyBenchmark)

	eb.Start(context.TODO())

	b.ReportAllocs()
	b.ResetTimer()

	for i := int64(0); i < iterations; i++ {
		options := eventbus.NewDeliveryOptions()
		options.AddTag("tag1")

		sendErr := eb.Send("void", &Arg{
			Id:       "id",
			Num:      10,
			Datetime: time.Now(),
		}, options)

		if sendErr != nil {
		}

		options = eventbus.NewDeliveryOptions()
		options.Add("h1", "1")
		options.Add("h2", "2")
		for i := 0; i < 2; i++ {
			rf := eb.Request("reply", &Arg{
				Id:       "id",
				Num:      i - 1,
				Datetime: time.Now(),
			}, options)
			result := &Result{}
			requestErr := rf.Get(result)
			if requestErr != nil {
			} else {
			}
		}
	}

	eb.Close(context.TODO())
}

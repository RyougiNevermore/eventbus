package eventbus

import (
	"github.com/aacfactory/errors"
	"runtime"
	"sync"
)

const (
	defaultGroupName = "_ebs_"
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

type eventHandleUnit struct {
	handle EventHandler
	tags   []string
}

func newLocalEventHandleStore() *localEventHandleStore {
	return &localEventHandleStore{
		lock:       sync.Mutex{},
		handlerMap: make(map[string][]eventHandleUnit),
	}
}

type localEventHandleStore struct {
	lock       sync.Mutex
	handlerMap map[string][]eventHandleUnit
}

func (s *localEventHandleStore) put(addr string, tags []string, handle EventHandler) {
	s.lock.Lock()
	defer s.lock.Unlock()
	handles, has := s.handlerMap[addr]
	if !has {
		handles = make([]eventHandleUnit, 0, 1)
	}
	if tags == nil {
		tags = make([]string, 0, 1)
	}
	handles = append(handles, eventHandleUnit{
		handle: handle,
		tags:   tags,
	})
	s.handlerMap[addr] = handles
}

func (s *localEventHandleStore) contains(addr string, tags []string) (has bool) {
	_, has = s.get(addr, tags)
	return
}

func (s *localEventHandleStore) get(addr string, tags []string) (handle EventHandler, has bool) {
	handles, has0 := s.handlerMap[addr]
	if !has0 {
		return
	}

	for _, stored := range handles {
		if tags == nil || len(tags) == 0 {
			if len(stored.tags) == 0 {
				handle = stored.handle
				has = true
				return
			}
			continue
		}
		mapped := 0
		for _, tag := range tags {
			for _, st := range stored.tags {
				if tag == st {
					mapped++
					break
				}
			}
		}
		if mapped == len(tags) {
			handle = stored.handle
			has = true
			return
		}
	}
	return
}

func (s *localEventHandleStore) remove(addr string, tags []string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	handles, has := s.handlerMap[addr]
	if !has {
		return
	}
	if tags == nil {
		tags = make([]string, 0, 1)
	}
	newHandles := make([]eventHandleUnit, 0, 1)
	for _, stored := range handles {
		mapped := 0
		for _, st := range stored.tags {
			for _, tag := range tags {
				if tag == st {
					mapped++
					break
				}
			}
		}
		if mapped != len(tags) {
			newHandles = append(newHandles, stored)
		}
	}
	if len(newHandles) == 0 {
		delete(s.handlerMap, addr)
	} else {
		s.handlerMap[addr] = newHandles
	}
}

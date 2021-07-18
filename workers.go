package eventbus

import (
	"runtime"
	"sync"
	"time"
)

func getDefaultWorkers() int {
	return runtime.NumCPU() * 2
}

type requestMessageHandle func(rm *requestMessage) (err error)

type workerPool struct {
	WorkerFunc            requestMessageHandle
	MaxWorkersCount       int
	MaxIdleWorkerDuration time.Duration
	lock                  sync.Mutex
	workersCount          int
	mustStop              bool
	ready                 []*workerChan
	stopCh                chan struct{}
	workerChanPool        sync.Pool
}

type workerChan struct {
	lastUseTime time.Time
	ch          chan *requestMessage
}

var workerChanCap = func() int {

	if runtime.GOMAXPROCS(0) == 1 {
		return 0
	}

	return 1
}()

func (wp *workerPool) Start() {
	if wp.stopCh != nil {
		panic("eventbus worker pool already started")
	}
	wp.stopCh = make(chan struct{})
	stopCh := wp.stopCh
	wp.workerChanPool.New = func() interface{} {
		return &workerChan{
			ch: make(chan *requestMessage, workerChanCap),
		}
	}
	go func() {
		var scratch []*workerChan
		for {
			wp.clean(&scratch)
			select {
			case <-stopCh:
				return
			default:
				time.Sleep(wp.getMaxIdleWorkerDuration())
			}
		}
	}()
}

func (wp *workerPool) Stop() {
	if wp.stopCh == nil {
		panic("eventbus worker pool wasn't started")
	}
	close(wp.stopCh)
	wp.stopCh = nil

	wp.lock.Lock()
	ready := wp.ready
	for i := range ready {
		ready[i].ch <- nil
		ready[i] = nil
	}
	wp.ready = ready[:0]
	wp.mustStop = true
	wp.lock.Unlock()
}

func (wp *workerPool) getMaxIdleWorkerDuration() time.Duration {
	if wp.MaxIdleWorkerDuration <= 0 {
		return 10 * time.Second
	}
	return wp.MaxIdleWorkerDuration
}

func (wp *workerPool) clean(scratch *[]*workerChan) {
	maxIdleWorkerDuration := wp.getMaxIdleWorkerDuration()

	criticalTime := time.Now().Add(-maxIdleWorkerDuration)

	wp.lock.Lock()
	ready := wp.ready
	n := len(ready)

	l, r, mid := 0, n-1, 0
	for l <= r {
		mid = (l + r) / 2
		if criticalTime.After(wp.ready[mid].lastUseTime) {
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	i := r
	if i == -1 {
		wp.lock.Unlock()
		return
	}

	*scratch = append((*scratch)[:0], ready[:i+1]...)
	m := copy(ready, ready[i+1:])
	for i = m; i < n; i++ {
		ready[i] = nil
	}
	wp.ready = ready[:m]
	wp.lock.Unlock()

	tmp := *scratch
	for i := range tmp {
		tmp[i].ch <- nil
		tmp[i] = nil
	}
}

func (wp *workerPool) SendRequestMessage(rm *requestMessage) bool {
	ch := wp.getCh()
	if ch == nil {
		return false
	}
	ch.ch <- rm
	return true
}

func (wp *workerPool) getCh() *workerChan {
	var ch *workerChan
	createWorker := false

	wp.lock.Lock()
	ready := wp.ready
	n := len(ready) - 1
	if n < 0 {
		if wp.workersCount < wp.MaxWorkersCount {
			createWorker = true
			wp.workersCount++
		}
	} else {
		ch = ready[n]
		ready[n] = nil
		wp.ready = ready[:n]
	}
	wp.lock.Unlock()

	if ch == nil {
		if !createWorker {
			return nil
		}
		vch := wp.workerChanPool.Get()
		ch = vch.(*workerChan)
		go func() {
			wp.workerFunc(ch)
			wp.workerChanPool.Put(vch)
		}()
	}
	return ch
}

func (wp *workerPool) release(ch *workerChan) bool {
	ch.lastUseTime = time.Now()
	wp.lock.Lock()
	if wp.mustStop {
		wp.lock.Unlock()
		return false
	}
	wp.ready = append(wp.ready, ch)
	wp.lock.Unlock()
	return true
}

func (wp *workerPool) workerFunc(ch *workerChan) {
	var rm *requestMessage

	var err error
	for rm = range ch.ch {
		if rm == nil {
			break
		}

		if err = wp.WorkerFunc(rm); err != nil && rm.replyCh != nil {
			rm.replyCh <- failedReplyMessage(err)
			close(rm.replyCh)
		}

		rm = nil

		if !wp.release(ch) {
			break
		}
	}

	wp.lock.Lock()
	wp.workersCount--
	wp.lock.Unlock()
}

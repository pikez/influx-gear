package engine

import (
	"container/list"
	"errors"
	. "gear/influx"
	"sync"
	"time"
)


const (
	retryInitial    = 500 * time.Millisecond
	retryMultiplier = 2
)

type RetryHTTPNode struct {
	ReplicaHTTPNode

	buffering int32

	initialInterval time.Duration
	multiplier      time.Duration
	maxInterval     time.Duration

	list *bufferList
}

func NewRetryHTTPNode(node ReplicaHTTPNode, maxSize int, maxInterval time.Duration) Node {
	r := &RetryHTTPNode{
		ReplicaHTTPNode: node,
		buffering:       0,
		initialInterval: retryInitial,
		multiplier:      retryMultiplier,
		maxInterval:     maxInterval,
		list:            newBufferList(maxSize),
	}
	go r.run()
	return r
}

func (r *RetryHTTPNode) run() {
	for {
		wr := r.list.pop()
		interval := r.initialInterval
		for {
			err := r.ReplicaHTTPNode.WritePoints(wr)
			if err == nil {
				break
			}
			if interval != r.maxInterval {
				interval *= r.multiplier
				if interval > r.maxInterval {
					interval = r.maxInterval
				}
			}
			time.Sleep(interval)
		}
	}
}

func (r *RetryHTTPNode) WritePoints(wr WriteRequest) (err error) {
	err = r.ReplicaHTTPNode.WritePoints(wr)
	if err != nil {
		err = r.list.add(wr)
	}
	return nil
}

type bufferList struct {
	cond    *sync.Cond
	maxSize int
	size    int
	num     int
	list    *list.List
}

func newBufferList(maxSize int) *bufferList {
	return &bufferList{
		cond:    sync.NewCond(new(sync.Mutex)),
		maxSize: maxSize,
		list:    list.New(),
	}
}

// pop will remove and return the first element of the list, blocking if necessary
func (l *bufferList) pop() (wr WriteRequest) {
	l.cond.L.Lock()

	for l.list.Len() == 0 {
		l.cond.Wait()
	}

	e := l.list.Front()
	wr = e.Value.(WriteRequest)
	l.list.Remove(e)
	l.size -= wr.Size()
	RetryRequestCount.Dec()
	RetryBufferSize.Sub(float64(wr.Size()))

	l.cond.L.Unlock()

	return
}

func (l *bufferList) add(wr WriteRequest) error {
	l.cond.L.Lock()

	if l.size+wr.Size() > l.maxSize {
		l.cond.L.Unlock()
		return errors.New("ErrBufferFull")
	}

	l.list.PushBack(wr)
	l.size += wr.Size()
	l.cond.Signal()
	RetryRequestCount.Inc()
	RetryBufferSize.Add(float64(wr.Size()))

	defer l.cond.L.Unlock()
	return nil
}

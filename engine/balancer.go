package engine

import "sync"

type Picker interface {
	Pick() Node
}

type roundRobin struct {
	instanceList []Node
	mu           sync.Mutex
	next         int
}

func NewRRPicker(ii []Node) Picker {
	return &roundRobin{
		instanceList: ii,
		mu:           sync.Mutex{},
		next:         0,
	}
}



func (rr *roundRobin) Put(i Node) (err error) {
	rr.instanceList = append(rr.instanceList, i)
	return
}

func (rr *roundRobin) Close()  {
	for _, instance := range rr.instanceList {
		instance.Shutdown()
	}
}

func (rr *roundRobin) Pick() (instance Node) {
	rr.mu.Lock()
	instance = rr.instanceList[rr.next]
	rr.next = (rr.next + 1) % len(rr.instanceList)
	rr.mu.Unlock()
	return instance
}

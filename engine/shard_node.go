package engine

import (
	"gear/config"
	. "gear/influx"
	"github.com/influxdata/influxdb/query"
	"hash/crc32"
	"sync"
)


type ShardHTTPNode struct {
	id       uint64
	name     string
	weight   int
	nodeMap  map[string]Node
	nodeNum  int
	nodeList []Node
	picker   Picker
}

func NewShardHTTPNode(node config.HTTPShardNode) *ShardHTTPNode {
	newShardHTTPNode := &ShardHTTPNode{
		name:   node.Name,
		weight: node.Weight,
	}

	var flagString string
	for _, instance := range node.HTTPReplicaNode {
		newInstance, _ := NewReplicaHTTPNode(instance)
		newShardHTTPNode.nodeList = append(newShardHTTPNode.nodeList, newInstance)
		flagString += instance.Address
	}
	if len(newShardHTTPNode.nodeList) < 1 {
		panic("node dont't have any replica node.")
	}
	newShardHTTPNode.picker = NewRRPicker(newShardHTTPNode.nodeList)

	newShardHTTPNode.id = uint64(crc32.ChecksumIEEE([]byte(flagString)))
	return newShardHTTPNode
}

func (n ShardHTTPNode) Weight() int {
	return n.weight
}

func (n ShardHTTPNode) ID() uint64 {
	return n.id
}

func (n ShardHTTPNode) Name() string {
	return n.name
}

func (n *ShardHTTPNode) GetInstances() []Node {
	return n.nodeList
}

func (n *ShardHTTPNode) Query(q QueryRequest) (result *query.Result, err error) {
	instance := n.picker.Pick()
	result, err = instance.Query(q)

	return result, err
}

// QueryEachInstance is usually used by statements such as Create, Drop,etc
// So It only needs to run sequentially
func (n *ShardHTTPNode) QueryEachInstance(q QueryRequest) (result *query.Result, err error) {
	for _, instance := range n.nodeList {
		instance := instance
		result, err = instance.Query(q)
		if err != nil {
			return
		}
		if result.Err != nil {
			return
		}
	}

	return
}

func (n *ShardHTTPNode) WritePoints(wr WriteRequest) error {
	var wg sync.WaitGroup
	wg.Add(len(n.nodeList))

	var responses = make(chan error, len(n.nodeList))
	for _, instance := range n.nodeList {
		instance := instance
		go func() {
			defer wg.Done()
			err := instance.WritePoints(wr)
			responses <- err
		}()
	}

	go func() {
		wg.Wait()
		close(responses)
	}()

	var writeError error
	for resp := range responses {
		// write consistency handler
		if resp != nil {
			writeError = resp
		}
	}

	return writeError
}

func (n *ShardHTTPNode) Ping() (err error) {
	for _, instance := range n.nodeList {
		err = instance.Ping()
		if err == nil {
			return
		}
	}
	return err
}

func (n *ShardHTTPNode) Shutdown()  {
	for _, instance := range n.nodeList {
		instance.Shutdown()
	}
}

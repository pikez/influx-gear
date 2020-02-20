package engine

import (
	. "gear/influx"
	"sync"
)

func (e HTTPEngine) Write(wr WriteRequest) error {
	if e.sharding {
		shardMappings, err := e.MapShards(&wr)
		if err != nil {
			return err
		}
		nodeResp := make(chan error, len(shardMappings.Points))
		var wg sync.WaitGroup
		wg.Add(len(shardMappings.Points))

		for shardID, points := range shardMappings.Points {
			wr.Points = points
			go func(node Node, w WriteRequest) {
				defer wg.Done()
				nodeResp <- e.writeNode(node, w)
			}(shardMappings.Nodes[shardID], wr)
		}

		go func() {
			wg.Wait()
			close(nodeResp)
		}()
		return mergeResponse(nodeResp)
	} else {
		node := e.NodeList()[0]
		return e.writeNode(node, wr)
	}
}

func mergeResponse(responses chan error) error {
	var writeError error
	for resp := range responses {
		// write consistency handler
		if resp != nil {
			writeError = resp
		}
	}

	return writeError
}

func (e *HTTPEngine) writeNode(node Node, w WriteRequest) error {
	return node.WritePoints(w)
}

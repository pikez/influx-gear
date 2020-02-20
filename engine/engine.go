package engine

import (
	"gear/config"
	. "gear/influx"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxql"
	log "github.com/sirupsen/logrus"
)

type Engine interface {
	Write(wr WriteRequest) error
	Query(qr QueryRequest) *Response
}

type Shard interface {

}

type HTTPEngine struct {
	nodeList []Node
	grid     Grid
	sharding bool
	picker   Picker
	config   config.GearConfig
}


func NewEngine(gearConfig config.GearConfig) Engine {
	engine := &HTTPEngine{
		config: gearConfig,
	}
	engine.InitNode()

	return engine
}

type ShardMapping struct {
	Points map[uint64][]models.Point // The points associated with a shard ID
	Nodes  map[uint64]Node           // The shards that have been mapped, keyed by shard ID
}

func NewShardMapping() ShardMapping {
	return ShardMapping{
		Points: map[uint64][]models.Point{},
		Nodes:  map[uint64]Node{},
	}
}

func (s *ShardMapping) MapPoint(node Node, p models.Point) {
	if points, ok := s.Points[node.ID()]; !ok {
		s.Points[node.ID()] = []models.Point{p}
	} else {
		s.Points[node.ID()] = append(points, p)
	}
	s.Nodes[node.ID()] = node
}

func (e *HTTPEngine) ShardFor(hash uint64) Node {
	return e.grid[hash%uint64(len(e.grid))]
}

func (e *HTTPEngine) MapShards(wp *WriteRequest) (ShardMapping, error) {
	mapping := NewShardMapping()
	for _, p := range wp.Points {
		h := models.NewInlineFNV64a()
		h.Write(p.Name())
		sum := h.Sum64()
		node := e.ShardFor(sum)
		mapping.MapPoint(node, p)
	}
	return mapping, nil
}

func (e *HTTPEngine) MapMeasurements(stmt *influxql.SelectStatement) (Node, error) {
	h := models.NewInlineFNV64a()
	h.Write([]byte(stmt.Sources.String()))
	sum := h.Sum64()
	node := e.ShardFor(sum)
	return node, nil
}

func (e *HTTPEngine) InitNode() {
	nodeNum := len(e.config.HTTPShardNode)
	if nodeNum > 1 {
		e.sharding = true
		log.Info("starting as sharding mode")
	} else {
		e.sharding = false
	}
	for _, node := range e.config.HTTPShardNode {
		newHTTPNode := NewShardHTTPNode(node)
		e.nodeList = append(e.nodeList, newHTTPNode)
	}
	e.picker = NewRRPicker(e.nodeList)
	e.grid = NewGrid(e.nodeList, e.config.Shard.GridSize)
}

func (e *HTTPEngine) Grid() Grid {
	return e.grid
}

func (e *HTTPEngine) NodeList() []Node {
	return e.nodeList
}

type Grid []Node

func NewGrid(nodes []Node, size int) []Node {
	grid := make([]Node, size)
	var index = 0
	for {
		for nodeIndex := range nodes {
			for i := 0; i < nodes[nodeIndex].Weight(); i++ {
				grid[index] = nodes[nodeIndex]
				index++
				if index >= size {
					return grid
				}
			}
		}
	}
}

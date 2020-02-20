package engine

import (
	. "gear/influx"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/stretchr/testify/assert"
	"testing"
)

type MockNode struct{}

func (m *MockNode) ID() uint64                                              { return uint64(1) }
func (m *MockNode) Ping() error                                             { return nil }
func (m *MockNode) Query(q QueryRequest) (*query.Result, error)             { return &query.Result{}, nil }
func (m *MockNode) QueryEachInstance(q QueryRequest) (*query.Result, error) { return &query.Result{}, nil }
func (m *MockNode) WritePoints(wr WriteRequest) error                       { return nil }
func (m *MockNode) Shutdown()                                               {}
func (m *MockNode) Weight() int                                             { return 1 }

var (
	mockNodeA = &MockNode{}
	mockNodeB = &MockNode{}
	mockNodeList = []Node{mockNodeA, mockNodeB}

	mockEngine = HTTPEngine{
		nodeList: mockNodeList,
		grid: NewGrid(mockNodeList, 100),
	}

	pointsStr = []byte("weather,location=us-midwest temperature=82 1465839830100400200\n" +
		"weather,location=us-midwest temperature=21 1465839830100400200")

	points, _ = models.ParsePoints(pointsStr)
)

func TestShardMapping_MapPoint(t *testing.T) {
	mapping := NewShardMapping()
	assert.Equal(t, len(mapping.Nodes), 0)
	for index, point := range points {
		mapping.MapPoint(mockNodeA, point)
		assert.Equal(t, len(mapping.Nodes), 1)
		assert.Equal(t, len(mapping.Points), 1)
		assert.Equal(t, len(mapping.Points[mockNodeA.ID()]), index+1)
		assert.Equal(t, mapping.Nodes[mockNodeA.ID()], mockNodeA)
	}

}

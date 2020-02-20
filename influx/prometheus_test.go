package influx

import (
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/prometheus/remote"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	SelectQueryStatement = "SELECT value FROM foo.bar.value WHERE __name__ = 'value' AND time >= '1970-01-01T00:00:00.001Z' AND time <= '1970-01-01T00:00:00.002Z' GROUP BY *"
)


func TestNewPromReadRequest(t *testing.T) {
	var b = []byte("124")
	_, err := NewPromReadRequest(b, "foo", "bar", "true")
	assert.NotNil(t, err)

	req := &remote.ReadRequest{
		Queries: []*remote.Query{{
			Matchers: []*remote.LabelMatcher{
				{
					Type:  remote.MatchType_EQUAL,
					Name:  "__name__",
					Value: "value",
				},
			},
			StartTimestampMs: 1,
			EndTimestampMs:   2,
		}},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}
	compressed := snappy.Encode(nil, data)

	q, err := NewPromReadRequest(compressed, "foo", "bar", "true")
	assert.Nil(t, err)
	assert.Equal(t, q.Query.String(), SelectQueryStatement)
}

func TestNewPromWriteRequest(t *testing.T) {
	var b = []byte("124")

	_, err := NewPromWriteRequest(b, "foo", "bar", "foo")
	assert.NotNil(t, err)


	req := &remote.WriteRequest{
		Timeseries: []*remote.TimeSeries{
			{
				Labels: []*remote.LabelPair{
					{Name: "host", Value: "server1"},
					{Name: "region", Value: "west"},
					{Name: "__name__", Value: "cpu_usage"},
				},
				Samples: []*remote.Sample{
					{TimestampMs: 1, Value: 1.2},
					{TimestampMs: 3, Value: 14.5},
					{TimestampMs: 6, Value: 222.99},
				},
			},
		},
	}

	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}
	compressed := snappy.Encode(nil, data)

	wr, err := NewPromWriteRequest(compressed, "foo", "bar", "foo")
	assert.Nil(t, err)
	point := wr.Points[0]
	assert.Equal(t, string(point.Name()), "cpu_usage")
	assert.Equal(t, point.String(), "cpu_usage,__name__=cpu_usage,host=server1,region=west value=1.2 1000000")
}

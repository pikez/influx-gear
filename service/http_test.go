package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"gear/config"
	. "gear/influx"
	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/prometheus/remote"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

var (
	SelectQueryResponseString = "{\"results\":[{\"statement_id\":0,\"series\":[{\"name\":\"bar\",\"columns\":[\"time\",\"value\"],\"values\":" +
		"[[\"1970-01-01T00:00:00.001Z\",0],[\"1970-01-01T00:00:00.002Z\",0]]}]}]}"
)

var (
	mockEngine = MockEngine{}

	gs = GearService{
		config:     config.GearConfig{},
		bufferPool: NewBufferPool(),
		Engine:     &mockEngine,
	}
)

type MockEngine struct {
	QueryFn func(qr QueryRequest) *Response
	WriteFn func(wr WriteRequest) error
}

func (m *MockEngine) Write(wr WriteRequest) error {
	return m.WriteFn(wr)
}

func (m *MockEngine) Query(qr QueryRequest) *Response {
	return m.QueryFn(qr)
}

func MustNewRequest(method, urlStr string, body io.Reader) *http.Request {
	r, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		panic(err.Error())
	}
	return r
}

func TestGearService_Query(t *testing.T) {
	r := MustNewRequest("GET", "/query?q=select * from bar", nil)
	w := httptest.NewRecorder()
	mockEngine.QueryFn = func(qr QueryRequest) *Response {
		var data Response
		dataByte := []byte(SelectQueryResponseString)
		err := json.Unmarshal(dataByte, &data)
		if err != nil {
			panic(err.Error())
		}
		return &data
	}
	gs.Query(w, r)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, SelectQueryResponseString, w.Body.String())
}

func TestGearService_Query_BadRequest(t *testing.T) {
	r := MustNewRequest("GET", "/query?q=select hello", nil)
	w := httptest.NewRecorder()

	mockEngine.QueryFn = func(qr QueryRequest) *Response {
		return &Response{}
	}

	gs.Query(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGearService_PromRead(t *testing.T) {
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
	b := bytes.NewReader(compressed)
	r := MustNewRequest("POST", "/api/v1/prom/read?db=foo&rp=bar", b)
	w := httptest.NewRecorder()

	mockEngine.QueryFn = func(qr QueryRequest) *Response {
		var data Response
		dataByte := []byte(SelectQueryResponseString)
		err := json.Unmarshal(dataByte, &data)
		if err != nil {
			panic(err.Error())
		}
		return &data
	}

	gs.PromRead(w, r)
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, SelectQueryResponseString, w.Body.String())
}

func TestGearService_PromRead_MethodError(t *testing.T) {
	r := MustNewRequest("GET", "influxdb", nil)
	w := httptest.NewRecorder()
	mockEngine.QueryFn = func(qr QueryRequest) *Response {
		return &Response{}
	}

	gs.PromRead(w, r)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestGearService_PromRead_BadRequest(t *testing.T) {
	b := bytes.NewReader([]byte{})
	r := MustNewRequest("POST", "/api/v1/prom/read?db=foo&rp=bar", b)
	w := httptest.NewRecorder()

	mockEngine.QueryFn = func(qr QueryRequest) *Response {
		return &Response{}
	}

	gs.PromRead(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGearService_Write_MethodError(t *testing.T) {
	r := MustNewRequest("GET", "influxdb", nil)
	w := httptest.NewRecorder()
	mockEngine.QueryFn = func(qr QueryRequest) *Response {
		return &Response{}
	}

	gs.Write(w, r)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestGearService_Write_BadRequest(t *testing.T) {
	b := bytes.NewReader([]byte{1, 2, 3})
	r := MustNewRequest("POST", "influxdb", b)
	w := httptest.NewRecorder()
	mockEngine.QueryFn = func(qr QueryRequest) *Response {
		return &Response{}
	}

	gs.Write(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
	gs.PromWrite(w, r)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestGearService_Write_ServerError(t *testing.T) {
	b := bytes.NewBuffer([]byte("cpu_load_short,host=server01,region=us-west value=0.64 1434055562000000000"))
	r := MustNewRequest("POST", "influxdb", b)
	w := httptest.NewRecorder()
	mockEngine.WriteFn = func(wr WriteRequest) error {
		return errors.New("server internal error")
	}

	gs.Write(w, r)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestGearService_PromWrite_MethodError(t *testing.T) {
	r := MustNewRequest("GET", "influxdb", nil)
	w := httptest.NewRecorder()
	mockEngine.QueryFn = func(qr QueryRequest) *Response {
		return &Response{}
	}

	gs.PromWrite(w, r)
	assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestGearService_PromWrite_ServerError(t *testing.T) {
	req := &remote.WriteRequest{
		Timeseries: []*remote.TimeSeries{
			{
				Labels: []*remote.LabelPair{
					{Name: "host", Value: "server01"},
					{Name: "region", Value: "us-west"},
				},
				Samples: []*remote.Sample{
					{TimestampMs: 1, Value: 0},
				},
			},
		},
	}

	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatal("couldn't marshal prometheus request")
	}
	compressed := snappy.Encode(nil, data)
	b := bytes.NewReader(compressed)

	r := MustNewRequest("POST", "influxdb", b)
	w := httptest.NewRecorder()
	mockEngine.WriteFn = func(wr WriteRequest) error {
		return errors.New("server internal error")
	}

	gs.PromWrite(w, r)
	assert.Equal(t, w.Code, http.StatusInternalServerError)
}

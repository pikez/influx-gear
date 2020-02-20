package engine

import (
	"encoding/json"
	"gear/config"
	"gear/influx"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPInstance_Ping(t *testing.T) {
	var testVersion = "influx-gear"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data influx.Response
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Influxdb-Version", testVersion)
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	httpConfig := config.HTTPReplicaNode{Address: ts.URL}
	i, _ := NewReplicaHTTPNode(httpConfig)
	defer i.Shutdown()

	err := i.Ping()
	assert.Nil(t, err)
}

func TestHTTPInstance_QueryError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data influx.Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	httpConfig := config.HTTPReplicaNode{Address: ts.URL}
	i, _ := NewReplicaHTTPNode(httpConfig)
	defer i.Shutdown()

	selectQuery, _ := influx.NewQueryRequest(
		"select * from bar",
		"foo",
		"ms",
		"false")

	_, err := i.Query(selectQuery)
	assert.NotNil(t, err)
}

func TestHTTPInstance_Query(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data influx.Response
		dataByte := []byte("[{\"statement_id\":0,\"series\":[{\"name\":\"bar\",\"columns\":[\"time\",\"value\"],\"values\":" +
			"[[\"2019-09-29T07:33:35.295Z\",0],[\"2019-09-29T07:33:35.971Z\", 0]]}]}]")
		err := json.Unmarshal(dataByte, &data.Results)
		if err != nil {
			log.Fatal(err)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	httpConfig := config.HTTPReplicaNode{Address: ts.URL}
	i, _ := NewReplicaHTTPNode(httpConfig)
	defer i.Shutdown()

	selectQuery, _ := influx.NewQueryRequest(
		"select * from bar",
		"foo",
		"ms",
		"false")

	result, err := i.Query(selectQuery)
	assert.Nil(t, err)
	assert.Equal(t, result.Series[0].Name, "bar")
	assert.Equal(t, len(result.Series[0].Columns), 2)
}

func TestHTTPInstance_WritePoint(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data influx.Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	httpConfig := config.HTTPReplicaNode{Address: ts.URL}
	i, _ := NewReplicaHTTPNode(httpConfig)
	defer i.Shutdown()

	writeRequest, _ := influx.NewWriteRequest(
		[]byte("weather,location=us-midwest temperature=82 1465839830100400200"),
		"foo",
		"ms",
		"")

	err := i.WritePoints(writeRequest)
	assert.Nil(t, err)
}

func TestHTTPInstance_WritePointError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data influx.Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("{\"error\": \"retention policy not found: half\"}"))
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer ts.Close()

	httpConfig := config.HTTPReplicaNode{Address: ts.URL}
	i, _ := NewReplicaHTTPNode(httpConfig)
	defer i.Shutdown()

	writeRequest, _ := influx.NewWriteRequest(
		[]byte("weather,location=us-midwest temperature=82 1465839830100400200"),
		"foo",
		"ms",
		"")

	err := i.WritePoints(writeRequest)
	assert.NotNil(t, err)
}

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

var (
	queryOKServer    *httptest.Server
	queryErrorServer *httptest.Server
	writeOKServer    *httptest.Server
	writeErrorServer *httptest.Server
)

func TestMain(m *testing.M) {
	queryOKServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	queryErrorServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data influx.Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(data)
	}))
	writeOKServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data influx.Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNoContent)
		_ = json.NewEncoder(w).Encode(data)
	}))
	writeErrorServer = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var data influx.Response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("{\"error\": \"retention policy not found: half\"}"))
		_ = json.NewEncoder(w).Encode(data)
	}))
	defer queryOKServer.Close()
	defer queryErrorServer.Close()
	defer writeOKServer.Close()
	defer writeErrorServer.Close()

	m.Run()
}

func TestHTTPNode_Query(t *testing.T) {
	httpInstanceA := config.HTTPReplicaNode{Address: queryOKServer.URL}
	httpInstanceB := config.HTTPReplicaNode{Address: queryOKServer.URL}
	httpNodeConfig := config.HTTPShardNode{HTTPReplicaNode: []config.HTTPReplicaNode{httpInstanceA, httpInstanceB}}
	node := NewShardHTTPNode(httpNodeConfig)

	selectQuery, _ := influx.NewQueryRequest(
		"select * from bar",
		"foo",
		"ms",
		"false")

	result, err := node.Query(selectQuery)
	assert.Nil(t, err)
	assert.Equal(t, result.Series[0].Name, "bar")
	assert.Equal(t, len(result.Series[0].Columns), 2)
}

func TestHTTPNode_QueryEachNodeError(t *testing.T) {
	httpInstanceA := config.HTTPReplicaNode{Address: queryOKServer.URL}
	httpInstanceB := config.HTTPReplicaNode{Address: queryErrorServer.URL}
	httpNodeConfig := config.HTTPShardNode{HTTPReplicaNode: []config.HTTPReplicaNode{httpInstanceA, httpInstanceB}}
	node := NewShardHTTPNode(httpNodeConfig)

	selectQuery, _ := influx.NewQueryRequest(
		"select * from bar",
		"foo",
		"ms",
		"false")

	_, err := node.QueryEachInstance(selectQuery)
	assert.NotNil(t, err)
}

func TestHTTPNode_QueryEachNode(t *testing.T) {
	httpInstanceA := config.HTTPReplicaNode{Address: queryOKServer.URL}
	httpInstanceB := config.HTTPReplicaNode{Address: queryOKServer.URL}
	httpNodeConfig := config.HTTPShardNode{HTTPReplicaNode: []config.HTTPReplicaNode{httpInstanceA, httpInstanceB}}
	node := NewShardHTTPNode(httpNodeConfig)

	selectQuery, _ := influx.NewQueryRequest(
		"select * from bar",
		"foo",
		"ms",
		"false")

	_, err := node.QueryEachInstance(selectQuery)
	assert.Nil(t, err)
}

func TestHTTPNode_QueryEachInstanceError(t *testing.T) {
	httpInstanceA := config.HTTPReplicaNode{Address: queryErrorServer.URL}
	httpInstanceB := config.HTTPReplicaNode{Address: queryErrorServer.URL}
	httpNodeConfig := config.HTTPShardNode{HTTPReplicaNode: []config.HTTPReplicaNode{httpInstanceA, httpInstanceB}}
	node := NewShardHTTPNode(httpNodeConfig)

	selectQuery, _ := influx.NewQueryRequest(
		"select * from bar",
		"foo",
		"ms",
		"false")

	_, err := node.Query(selectQuery)
	assert.NotNil(t, err)
}

func TestHTTPNode_WritePoints(t *testing.T) {
	httpInstanceA := config.HTTPReplicaNode{Address: writeOKServer.URL}
	httpInstanceB := config.HTTPReplicaNode{Address: writeOKServer.URL}
	httpNodeConfig := config.HTTPShardNode{HTTPReplicaNode: []config.HTTPReplicaNode{httpInstanceA, httpInstanceB}}
	node := NewShardHTTPNode(httpNodeConfig)

	writeRequest, _ := influx.NewWriteRequest(
		[]byte("weather,location=us-midwest temperature=82 1465839830100400200"),
		"foo",
		"ms",
		"")

	err := node.WritePoints(writeRequest)
	assert.Nil(t, err)
}

func TestHTTPNode_WritePointsOneNodeError(t *testing.T) {
	httpInstanceA := config.HTTPReplicaNode{Address: writeOKServer.URL}
	httpInstanceB := config.HTTPReplicaNode{Address: writeErrorServer.URL}
	httpNodeConfig := config.HTTPShardNode{HTTPReplicaNode: []config.HTTPReplicaNode{httpInstanceA, httpInstanceB}}
	node := NewShardHTTPNode(httpNodeConfig)

	writeRequest, _ := influx.NewWriteRequest(
		[]byte("weather,location=us-midwest temperature=82 1465839830100400200"),
		"foo",
		"ms",
		"")

	err := node.WritePoints(writeRequest)
	assert.NotNil(t, err)
}

func TestHTTPNode_PickInstance(t *testing.T) {
	httpInstanceA := config.HTTPReplicaNode{Address: writeOKServer.URL}
	httpInstanceB := config.HTTPReplicaNode{Address: writeOKServer.URL}
	httpNodeConfig := config.HTTPShardNode{HTTPReplicaNode: []config.HTTPReplicaNode{httpInstanceA, httpInstanceB}}
	node := NewShardHTTPNode(httpNodeConfig)

	instanceA := node.picker.Pick()
	instanceB := node.picker.Pick()
	assert.NotEqual(t, instanceA, instanceB)
	instanceB = node.picker.Pick()
	assert.Equal(t, instanceA, instanceB)
}

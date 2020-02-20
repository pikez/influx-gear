package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"gear/config"
	. "gear/influx"

	"github.com/influxdata/influxdb/query"
	log "github.com/sirupsen/logrus"
	"hash/crc32"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync/atomic"
	"time"
)

const (
	DefaultMaxDelayInterval = 10 * time.Second

	KB = 1024
	MB = 1024 * KB
)

type ReplicaHTTPNode struct {
	id       uint64
	client   *http.Client
	url      url.URL
	username string
	password string
	status   uint32
	bufferPool BufferPool
}

func NewReplicaHTTPNode(instance config.HTTPReplicaNode) (newNode Node, err error) {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	u, err := url.Parse(strings.Trim(instance.Address, "/"))
	if err != nil {
		return
	}
	newReplicaHTTPNode := ReplicaHTTPNode{
		client: &http.Client{
			Transport: transport,
		},
		id:       uint64(crc32.ChecksumIEEE([]byte(u.Host))),
		url:      *u,
		username: instance.Username,
		password: instance.Password,
		bufferPool: NewBufferPool(),
	}
	if instance.BufferSizeMb > 0 {
		log.Info("set replica node is retry.")
		max := DefaultMaxDelayInterval
		if instance.MaxDelayInterval != "" {
			m, err := time.ParseDuration(instance.MaxDelayInterval)
			if err != nil {
				return nil, fmt.Errorf("error parsing max retry time %v", err)
			}
			max = m
		}
		return NewRetryHTTPNode(newReplicaHTTPNode, instance.BufferSizeMb*MB, max), nil
	}
	err = newReplicaHTTPNode.Ping()

	return &newReplicaHTTPNode, nil
}

func (i *ReplicaHTTPNode) Ping() (err error) {
	u := i.url
	u.Path = path.Join(u.Path, "ping")

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return
	}

	if i.username != "" {
		req.SetBasicAuth(i.username, i.password)
	}

	resp, err := i.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		atomic.StoreUint32(&i.status, 0)
		return
	}
	if resp.StatusCode != http.StatusNoContent {
		err = errors.New(string(body))
		atomic.StoreUint32(&i.status, 0)
		return
	}

	atomic.StoreUint32(&i.status, 1)
	return
}

func (i *ReplicaHTTPNode) createDefaultRequest(q QueryRequest) (*http.Request, error) {
	u := i.url
	u.Path = path.Join(u.Path, "query")

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", "")

	if i.username != "" {
		req.SetBasicAuth(i.username, i.password)
	}

	params := req.URL.Query()
	params.Set("q", q.Query.String())
	params.Set("db", q.Database)

	if q.Precision != "" {
		params.Set("epoch", q.Precision)
	}
	req.URL.RawQuery = params.Encode()

	return req, nil

}

func (i *ReplicaHTTPNode) Query(q QueryRequest) (*query.Result, error) {
	req, err := i.createDefaultRequest(q)
	if err != nil {
		return nil, err
	}
	resp, err := i.client.Do(req)
	if err != nil {
		log.Error("service error: ", err)
		return nil, err
	}
	defer resp.Body.Close()

	log.Infof("query status code: %d, the backend is %s\n", resp.StatusCode, req.URL)

	var response Response
	var result query.Result
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	decErr := dec.Decode(&response)

	// ignore this error if we got an invalid status code
	if decErr != nil && decErr.Error() == "EOF" && resp.StatusCode != http.StatusOK {
		decErr = nil
	}
	// If we got a valid decode error, send that back
	if decErr != nil {
		return nil, fmt.Errorf("unable to decode json: received status code %d err: %s", resp.StatusCode, decErr)
	}

	if resp.StatusCode != http.StatusOK && response.Error == nil {
		return &result, fmt.Errorf("received status code %d from server", resp.StatusCode)
	}
	if response.Error != nil {
		return &result, response.Error
	}
	return response.Results[0], nil
}

func (i *ReplicaHTTPNode) QueryEachInstance(q QueryRequest) (result *query.Result, err error) {
	return i.Query(q)
}

func (i *ReplicaHTTPNode) WritePoints(wr WriteRequest) error {
	b := i.bufferPool.Get()
	defer i.bufferPool.Put(b)

	for _, p := range wr.Points {
		if p == nil {
			continue
		}
		if _, err := b.WriteString(p.PrecisionString(wr.Precision)); err != nil {
			return err
		}

		if err := b.WriteByte('\n'); err != nil {
			return err
		}
	}

	u := i.url
	u.Path = path.Join(u.Path, "write")

	req, err := http.NewRequest("POST", u.String(), b)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "")
	req.Header.Set("User-Agent", "")
	if i.username != "" {
		req.SetBasicAuth(i.username, i.password)
	}

	params := req.URL.Query()
	params.Set("db", wr.Database)
	params.Set("rp", wr.RetentionPolicy)
	params.Set("precision", wr.Precision)
	req.URL.RawQuery = params.Encode()

	resp, err := i.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		var err = errors.New(string(body))
		return err
	}

	return nil
}

func (i *ReplicaHTTPNode) Shutdown() {
	i.client.CloseIdleConnections()
}

func (i *ReplicaHTTPNode) IsAlive() bool {
	var status uint32
	atomic.LoadUint32(&status)
	if status > 0 {
		return true
	} else {
		return false
	}
}

func (i *ReplicaHTTPNode) Weight() int {
	return 1
}

func (i *ReplicaHTTPNode) ID() uint64 {
	return 1
}

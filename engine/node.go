package engine

import (
	. "gear/influx"
	"github.com/influxdata/influxdb/query"
)

type Node interface {
	ID() uint64
	Ping() error
	Query(q QueryRequest) (*query.Result, error)
	QueryEachInstance(q QueryRequest) (*query.Result, error)
	WritePoints(wr WriteRequest) error
	Shutdown()
	Weight() int
}

package influx

import (
	"encoding/json"
	"errors"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	"strings"
	"time"
)

type Response struct {
	Results []*query.Result `json:"results"`
	Error   error
}

func (r Response) MarshalJSON() ([]byte, error) {
	// Define a struct that outputs "error" as a string.
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Error   string          `json:"error,omitempty"`
	}

	// Copy fields to output struct.
	o.Results = r.Results
	if r.Error != nil {
		o.Error = r.Error.Error()
	}

	return json.Marshal(&o)
}

func (r *Response) UnmarshalJSON(b []byte) error {
	var o struct {
		Results []*query.Result `json:"results,omitempty"`
		Err     string          `json:"error,omitempty"`
	}

	err := json.Unmarshal(b, &o)
	if err != nil {
		return err
	}
	r.Results = o.Results
	if o.Err != "" {
		r.Error = errors.New(o.Err)
	}
	return nil
}

type QueryRequest struct {
	Query     *influxql.Query
	Database  string
	Precision string
	Chunked   string
}

func NewQueryRequest(q, database, precision, chunked string) (QueryRequest, error) {
	parser := influxql.NewParser(strings.NewReader(q))
	query, err := parser.ParseQuery()
	if err != nil {
		return QueryRequest{}, err
	}
	return QueryRequest{
		Query:     query,
		Database:  database,
		Precision: precision,
		Chunked:   chunked,
	}, nil
}

type WriteRequest struct {
	Points          []models.Point
	Database        string
	RetentionPolicy string
	Precision       string
	pointSize       int
}

func (wr WriteRequest) Size() int {
	return wr.pointSize
}

func (wr WriteRequest) PointNum() int {
	return len(wr.Points)
}

func NewWriteRequest(lineData []byte, db, precision, rp string) (WriteRequest, error) {
	var err error
	var points models.Points
	if precision != "" {
		points, err = models.ParsePointsWithPrecision(lineData, time.Now().UTC(), precision)
	} else {
		points, err = models.ParsePoints(lineData)
	}
	if err != nil {
		return WriteRequest{}, err
	}

	return WriteRequest{
		Points:          points,
		Database:        db,
		Precision:       precision,
		RetentionPolicy: rp,
		pointSize:       len(lineData),
	}, nil
}

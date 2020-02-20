package influx

import (
	"errors"
	"fmt"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/influxdata/influxdb/prometheus"
	"github.com/influxdata/influxdb/prometheus/remote"
	"github.com/influxdata/influxql"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
	"regexp"
	"time"
)

const (
	measurementName = "prom_metric_not_specified"

	fieldName = "value"
)

func NewPromWriteRequest(lineData []byte, db, precision, rp string) (WriteRequest, error) {
	var req remote.WriteRequest

	lineData, err := snappy.Decode(nil, lineData)
	if err != nil {
		log.Error(err)
		return WriteRequest{}, err
	}

	if err := proto.Unmarshal(lineData, &req); err != nil {
		log.Error(err)
		return WriteRequest{}, err
	}

	points, err := prometheus.WriteRequestToPoints(&req)
	if err != nil {
		if _, ok := err.(prometheus.DroppedValuesError); !ok {
			return WriteRequest{}, err
		}
	}

	return WriteRequest{
		Points:          points,
		Database:        db,
		Precision:       precision,
		RetentionPolicy: rp,
		pointSize:       len(lineData),
	}, nil
}

func NewPromReadRequest(lineData []byte, db, rp, chunked string) (QueryRequest, error) {
	var req remote.ReadRequest

	reqBuf, err := snappy.Decode(nil, lineData)
	if err != nil {
		log.Error(err)
		return QueryRequest{}, err
	}

	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.Error(err)
		return QueryRequest{}, err
	}

	// Query the DB and create a ReadResponse for Prometheus
	query, err := ReadRequestToInfluxQLQuery(&req, db, rp)
	if err != nil {
		log.Error(err)
		return QueryRequest{}, err
	}

	return QueryRequest{
		Query: query,
		Database:  db,
		Precision: "ms",
		Chunked:   chunked,
	}, nil
}

func ReadRequestToInfluxQLQuery(req *remote.ReadRequest, db, rp string) (*influxql.Query, error) {
	if len(req.Queries) != 1 {
		return nil, errors.New("prometheus read endpoint currently only supports one query at a time")
	}
	promQuery := req.Queries[0]

	var measurement = measurementName
	for _, m := range promQuery.Matchers {
		if m.Name == model.MetricNameLabel {
			measurement = m.Value
		}
	}

	stmt := &influxql.SelectStatement{
		IsRawQuery: true,
		Fields: []*influxql.Field{
			{Expr: &influxql.VarRef{Val: fieldName}},
		},
		Sources: []influxql.Source{&influxql.Measurement{
			Name:            measurement,
			Database:        db,
			RetentionPolicy: rp,
		}},
		Dimensions: []*influxql.Dimension{{Expr: &influxql.Wildcard{}}},
	}

	cond, err := condFromMatchers(promQuery, promQuery.Matchers)
	if err != nil {
		return nil, err
	}

	stmt.Condition = cond

	return &influxql.Query{Statements: []influxql.Statement{stmt}}, nil
}

func condFromMatchers(q *remote.Query, matchers []*remote.LabelMatcher) (*influxql.BinaryExpr, error) {
	if len(matchers) > 0 {
		lhs, err := condFromMatcher(matchers[0])
		if err != nil {
			return nil, err
		}
		rhs, err := condFromMatchers(q, matchers[1:])
		if err != nil {
			return nil, err
		}

		return &influxql.BinaryExpr{
			Op:  influxql.AND,
			LHS: lhs,
			RHS: rhs,
		}, nil
	}

	return &influxql.BinaryExpr{
		Op: influxql.AND,
		LHS: &influxql.BinaryExpr{
			Op:  influxql.GTE,
			LHS: &influxql.VarRef{Val: "time"},
			RHS: &influxql.TimeLiteral{Val: time.Unix(0, q.StartTimestampMs*int64(time.Millisecond))},
		},
		RHS: &influxql.BinaryExpr{
			Op:  influxql.LTE,
			LHS: &influxql.VarRef{Val: "time"},
			RHS: &influxql.TimeLiteral{Val: time.Unix(0, q.EndTimestampMs*int64(time.Millisecond))},
		},
	}, nil
}

func condFromMatcher(m *remote.LabelMatcher) (*influxql.BinaryExpr, error) {
	var op influxql.Token
	var rhs influxql.Expr

	switch m.Type {
	case remote.MatchType_EQUAL:
		op = influxql.EQ
	case remote.MatchType_NOT_EQUAL:
		op = influxql.NEQ
	case remote.MatchType_REGEX_MATCH:
		op = influxql.EQREGEX
	case remote.MatchType_REGEX_NO_MATCH:
		op = influxql.NEQREGEX
	default:
		return nil, fmt.Errorf("unknown match type %v", m.Type)
	}

	if op == influxql.EQREGEX || op == influxql.NEQREGEX {
		re, err := regexp.Compile(m.Value)
		if err != nil {
			return nil, err
		}

		// Convert regex values to InfluxDB format.
		rhs = &influxql.RegexLiteral{Val: re}
	} else {
		rhs = &influxql.StringLiteral{Val: m.Value}
	}

	return &influxql.BinaryExpr{
		Op:  op,
		LHS: &influxql.VarRef{Val: m.Name},
		RHS: rhs,
	}, nil
}

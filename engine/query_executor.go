package engine

import (
	. "gear/influx"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxql"
	log "github.com/sirupsen/logrus"
)

func (e HTTPEngine) Query(qr QueryRequest) *Response {
	var i int
	var resp Response
	for ; i < len(qr.Query.Statements); i++ {
		stmt := qr.Query.Statements[i]
		log.Debug(stmt.String())
		statementQuery := QueryRequest{
			Query:     &influxql.Query{Statements: influxql.Statements{stmt}},
			Database:  qr.Database,
			Precision: qr.Precision,
			Chunked:   qr.Chunked,
		}
		result, err := e.executeStatementQuery(statementQuery)
		if err != nil {
			return &Response{
				Results: nil,
				Error:   err,
			}
		}
		resp.Results = append(resp.Results, result)
	}

	return &resp
}

func (e HTTPEngine) executeStatementQuery(qr QueryRequest) (result *query.Result, err error) {
	stmt := qr.Query.Statements[0]
	switch stmt := stmt.(type) {
	case *influxql.AlterRetentionPolicyStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.CreateContinuousQueryStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.CreateDatabaseStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.CreateRetentionPolicyStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.CreateSubscriptionStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.CreateUserStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.DeleteStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.DeleteSeriesStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.DropContinuousQueryStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.DropDatabaseStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.DropMeasurementStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.DropRetentionPolicyStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.DropShardStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.DropSubscriptionStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.DropUserStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.GrantStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.GrantAdminStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.RevokeAdminStatement:
		result, err = e.executeStatementEachNode(qr)
	case *influxql.SelectStatement:
		result, err = e.executeSelectStatement(qr)
	case *influxql.ShowDatabasesStatement:
		result, err = e.executeStatementOneNode(qr)
	case *influxql.ShowContinuousQueriesStatement:
		result, err = e.executeStatementOneNode(qr)
	case *influxql.ShowGrantsForUserStatement:
		result, err = e.executeStatementOneNode(qr)
	case *influxql.ShowMeasurementCardinalityStatement:
		result, err = e.executeStatementOneNode(qr)
	case *influxql.ShowSeriesCardinalityStatement:
		result, err = e.executeStatementOneNode(qr)
	case *influxql.ShowShardsStatement:
		result, err = e.executeStatementOneNode(qr)
	case *influxql.ShowShardGroupsStatement:
		result, err = e.executeStatementOneNode(qr)
	case *influxql.ShowStatsStatement:
		result, err = e.executeStatementOneNode(qr)
	case *influxql.ShowMeasurementsStatement:
		result, err = e.executeStatementEachNodeMergeValues(qr)
	case *influxql.ShowDiagnosticsStatement:
		result, err = e.executeStatementEachNodeMergeSeries(qr)
	case *influxql.ShowTagKeysStatement:
		result, err = e.executeStatementEachNodeMergeSeries(qr)
	case *influxql.ShowTagValuesStatement:
		result, err = e.executeStatementEachNodeMergeSeries(qr)
	case *influxql.ShowUsersStatement:
		result, err = e.executeStatementOneNode(qr)
	case *influxql.ShowRetentionPoliciesStatement:
		result, err = e.executeStatementOneNode(qr)

	default:
		err := query.ErrInvalidQuery
		log.Error(stmt)
		return nil, err
	}

	return result, err
}

func (e HTTPEngine) executeSelectStatement(qr QueryRequest) (result *query.Result, err error) {
	stmt := qr.Query.Statements[0].(*influxql.SelectStatement)
	node, err := e.MapMeasurements(stmt)
	if err != nil {
		log.Error("can't locate the cluster node")
		return result, err
	}

	result, err = node.Query(qr)

	if err != nil {
		return result, err
	}

	return
}

func (e HTTPEngine) executeStatementOneNode(qr QueryRequest) (result *query.Result, err error) {
	node := e.picker.Pick()

	result, err = node.Query(qr)

	if err != nil {
		return result, err
	}

	return
}

func (e HTTPEngine) executeStatementEachNode(qr QueryRequest) (result *query.Result, err error) {
	for _, node := range e.NodeList() {
		result, err = node.QueryEachInstance(qr)

		if err != nil {
			return result, err
		}
	}
	return
}

func (e HTTPEngine) executeStatementEachNodeMergeSeries(qr QueryRequest) (result *query.Result, err error) {
	var series Series

	for _, node := range e.NodeList() {
		result, err := node.Query(qr)

		if err != nil {
			return result, err
		}
		series.MergeSeries(Series(result.Series))
	}
	result = &query.Result{
		Series: models.Rows(series),
	}

	return
}

func (e HTTPEngine) executeStatementEachNodeMergeValues(qr QueryRequest) (result *query.Result, err error) {
	var values Values
	var queryResult *query.Result

	for _, node := range e.NodeList() {
		queryResult, err = node.Query(qr)

		if err != nil {
			return result, err
		}
		values.MergeSeriesValues(queryResult.Series[0].Values)
	}

	name := queryResult.Series[0].Name
	columns := queryResult.Series[0].Columns
	result = &query.Result{
		Series: []*models.Row{{
			Name:    name,
			Columns: columns,
			Values:  values,
		}},
	}
	return
}

type Values [][]interface{}

func (v *Values) MergeSeriesValues(newValue Values) {
	*v = append(*v, newValue...)
}

type Series models.Rows

func (s *Series) MergeSeries(newSeries Series) {
	*s = append(*s, newSeries...)
}

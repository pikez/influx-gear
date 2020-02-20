package service

import (
	"gear/engine"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	HTTPClientErrorsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "http_client_error_total",
			Help: "Number of http client error in total",
		},
	)
	HTTPServerErrorsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "http_server_error_total",
			Help: "Number of http client error in total",
		},
	)
	HTTPRequestTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_request_total",
			Help: "Number of hello requests in total",
		},
		[]string{"method", "endpoint"},
	)
	HTTPRequestDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "http_request_duration_seconds",
			Help: "http request duration distribution",
		},
		[]string{"method", "endpoint"},
	)
)

func init() {
	prometheus.MustRegister(HTTPRequestTotal)
	prometheus.MustRegister(HTTPRequestDuration)
	prometheus.MustRegister(engine.RetryRequestCount)
	prometheus.MustRegister(engine.RetryBufferSize)
}

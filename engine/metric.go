package engine

import "github.com/prometheus/client_golang/prometheus"

var (
	RetryRequestCount = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "retry_request_count",
			Help: "Number of retry requests in total",
		},
	)
	RetryBufferSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "retry_buffer_size",
			Help: "Size of retry requests in total",
		},
	)
)

package service

import (
	"encoding/json"
	"errors"
	"gear/config"
	"gear/engine"
	. "gear/influx"
	"github.com/influxdata/influxdb/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"math"
	"net/http"
	"net/http/pprof"
	_ "net/http/pprof"
)

type GearService struct {
	config     config.GearConfig
	Engine     engine.Engine
	bufferPool BufferPool
}

func NewGearService(gearConfig config.GearConfig) *GearService {
	gearEngine := engine.NewEngine(gearConfig)
	return &GearService{
		config:     gearConfig,
		Engine:     gearEngine,
		bufferPool: NewBufferPool(),
	}
}

func (g *GearService) httpError(w http.ResponseWriter, errMsg string, code int) {
	switch code / 100 {
	case 4:
		HTTPClientErrorsTotal.Inc()
	case 5:
		HTTPServerErrorsTotal.Inc()
	}
	if code/100 != 2 {
		sz := math.Min(float64(len(errMsg)), 1024.0)
		w.Header().Set("X-InfluxDB-Error", errMsg[:int(sz)])
	}

	response := Response{
		Error: errors.New(errMsg),
	}
	if rw, ok := w.(ResponseWriter); ok {
		rw.WriteHeader(code)
		rw.WriteResponse(response)
		return
	}

	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(code)
	b, _ := json.Marshal(response)
	w.Write(b)
}

func (g *GearService) Query(w http.ResponseWriter, r *http.Request) {
	rw := NewResponseWriter(w, r)
	queryRequest, err := NewQueryRequest(
		r.FormValue("q"),
		r.FormValue("db"),
		r.FormValue("epoch"),
		r.FormValue("chunked"))

	if err != nil {
		log.Error(err)
		g.httpError(rw, "error parsing query: "+err.Error(), http.StatusBadRequest)
		return
	}

	response := g.Engine.Query(queryRequest)
	rw.WriteResponse(*response)
}

func (g *GearService) Write(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
		} else {
			g.httpError(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
	}
	bodyBuf := g.bufferPool.Get()
	defer g.bufferPool.Put(bodyBuf)

	_, _ = bodyBuf.ReadFrom(r.Body)
	writeRequest, err := NewWriteRequest(
		bodyBuf.Bytes(),
		r.FormValue("db"),
		r.FormValue("precision"),
		r.FormValue("rp"),
	)
	if err != nil {
		log.Error(err)
		g.httpError(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = g.Engine.Write(writeRequest)
	if err != nil {
		g.httpError(w, err.Error(), http.StatusInternalServerError)
	}
	w.WriteHeader(http.StatusNoContent)
}

func (g *GearService) PromWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
		} else {
			g.httpError(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
	}

	bodyBuf := g.bufferPool.Get()
	defer g.bufferPool.Put(bodyBuf)

	_, _ = bodyBuf.ReadFrom(r.Body)

	writeRequest, err := NewPromWriteRequest(
		bodyBuf.Bytes(),
		r.FormValue("db"),
		r.FormValue("precision"),
		r.FormValue("rp"),
	)

	if err != nil {
		if _, ok := err.(prometheus.DroppedValuesError); !ok {
			g.httpError(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	err = g.Engine.Write(writeRequest)
	if err != nil {
		g.httpError(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (g *GearService) PromRead(w http.ResponseWriter, r *http.Request) {
	rw := NewResponseWriter(w, r)
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
		} else {
			g.httpError(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
	}

	bodyBuf := g.bufferPool.Get()
	defer g.bufferPool.Put(bodyBuf)
	_, _ = bodyBuf.ReadFrom(r.Body)

	queryRequest, err := NewPromReadRequest(
		bodyBuf.Bytes(),
		r.FormValue("db"),
		r.FormValue("rp"),
		r.FormValue("chunked"),
	)
	log.Info(queryRequest.Query)

	if err != nil {
		g.httpError(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	response := g.Engine.Query(queryRequest)

	_, err = rw.WriteResponse(*response)
	if err != nil {
		log.Error(err)
	}
}

func (g *GearService) Run() {
	mux := http.NewServeMux()
	mux.HandleFunc("/query", RecordMetricMiddleware(g.Query))
	mux.HandleFunc("/write", RecordMetricMiddleware(g.Write))
	mux.HandleFunc("/api/v1/prom/write", RecordMetricMiddleware(g.PromWrite))
	mux.HandleFunc("/api/v1/prom/read", RecordMetricMiddleware(g.PromRead))

	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	mux.Handle("/metrics", promhttp.Handler())
	log.Info("Listen on ", g.config.HTTP.BindAddress)
	log.Fatal(http.ListenAndServe(g.config.HTTP.BindAddress, mux))
}

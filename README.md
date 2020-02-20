# influx-gear
InfluxDB middleware for data replication and sharding. It can effectively improved the availability and scalability when using InfluxDB on a small or medium scale.

## Features
It has many advantages compared to similar projects:

* [influxdb-relay] (https://github.com/influxdata/influxdb-relay)
* [influx-proxy] (https://github.com/shell909090/influx-proxy)

Supports the following features

* Support full read and write endpoint, transparent to the client
* Support data sharding through measurement
* Support Prometheus remote read and write endpoint
* Support caching of failed write requests and retry laterly
* Support metric data export
* Simple configuration, stateless, and conducive to multi-instance deployment


## Usage

Compile and run from source

```bash
$ # Install influx-gear to your $GOPATH/bin
$ go get -u github.com/pikez/influx-gear
$ # Edit your configuration file
$ cp $GOPATH/src/github.com/pikez/influx-gear/sample.toml ./config.toml
$ vim config.toml
$ # Start 
$ $GOPATH/bin/influx-gear -config config.toml
```

## Configuration
[example](https://github.com/pikez/influx-gear/tree/master/examples)

## Requirements
* Golang >= 1.11
* InfluxDB >= 1.7

## Detailed
### EndPoint
* Use `/query` & ` /write` to query and write data and manage the databases,retention policies, and users. influx-gear supports all query management statements except `select into`, which means that it can be used transparently. See [query](https://docs.influxdata.com/influxdb/v1.7/tools/api/#query-http-endpoint) for details 
* Use `/api/v1/prom/write` &`/api/v1/prom/read` to remote reading and writing metric data for Prometheus
* Use `/metrics` to get metric data
* Use `/debug/pprof/*` to get profiling data for influx-gear


## License
[MIT] (https://choosealicense.com/licenses/mit/)
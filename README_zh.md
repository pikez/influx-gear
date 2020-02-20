# influx-gear
influx-gear是InfluxDB的中间件。通过对数据多写和分片，能够在中小规模使用InfluxDB时有效提高可用性和扩展性。

## 主要特性
相对于目前已有的类似项目：
[influxdb-relay](https://github.com/influxdata/influxdb-relay)
[influx-proxy](https://github.com/shell909090/influx-proxy)

支持以下特性
* 支持完全的读写接口，对客户端透明，无需关注后端的InfluxDB实例
* 支持通过measurement进行数据分片
* 支持Prometheus远程读写接口
* 支持失败写请求的缓存并重试
* 支持运行状态监控
* 配置简单，无状态化，利于多实例部署

## 架构

###主要概念
* ReplicaNode: 复制集节点，每个InfluxDB实例为一个ReplicaNode节点，多个ReplicaNode构成一组互相备份的数据节点，也就是同一组ReplicaNode拥有完全一致的数据.
* ShardNode: 分片集节点，一个或一组ReplicaNode构成一个ShardNode节点，ShardNode下的ReplicaNode数量代表对同一份数据写几份。所有ShardNode组成一个InfluxDB逻辑集群，构成数据的全集.
* Cluster: InfluxDB逻辑集群，由一个或多个ShardNode组成。Cluster下的ShardNode数量代表一个数据库中对measurement的分片数量


## 使用

从源码进行编译运行
```bash
$ # Install influx-gear to your $GOPATH/bin
$ go get -u github.com/pikez/influx-gear
$ # Edit your configuration file
$ cp $GOPATH/src/github.com/pikez/influx-gear/sample.toml ./config.toml
$ vim relay.toml
$ # Start relay!
$ $GOPATH/bin/influx-gear -config config.toml
```

## 配置

## 详解
### EndPoint
* `/query` & `/write` 读写数据和管理数据库接口，influx-gear支持除`select into`以外的所有查询管理语句，意味着可透明地使用influx-gear接口. 详见 [query](https://docs.influxdata.com/influxdb/v1.7/tools/api/#query-http-endpoint)
* `/api/v1/prom/write` & `/api/v1/prom/read` 用于Prometheus远程读写的接口，可直接对接Prometheus进行监控数据持久存储
* `/metrics` influx-gear的运行状态信息，用于接入Prometheus进行状态监控
* `/debug/pprof/*` influx-gear的pprof信息


## License
[MIT](https://choosealicense.com/licenses/mit/)

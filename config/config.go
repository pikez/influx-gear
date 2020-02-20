package config

import (
	"github.com/BurntSushi/toml"
	log "github.com/sirupsen/logrus"
	"path/filepath"
	"sync"
)

type GearConfig struct {
	HTTP  HTTP  `toml:"http"`
	Shard Shard `toml:"shard"`
	HTTPShardNode []HTTPShardNode `toml:"http-shard-node"`
}

type HTTP struct {
	BindAddress string `toml:"bind-address"`
}

type Shard struct {
	GridSize int `toml:"grid-size"`
}

type HTTPShardNode struct {
	Name            string            `toml:"name"`
	HTTPReplicaNode []HTTPReplicaNode `toml:"replica-node"`
	Weight          int
}

type HTTPReplicaNode struct {
	Address          string
	Username         string
	Password         string
	BufferSizeMb     int    `toml:"buffer-size-mb"`
	MaxDelayInterval string `toml:"max-delay-interval"`
}

var (
	cfg  *GearConfig
	once sync.Once
)

func Config(path string) *GearConfig {
	once.Do(func() {
		filePath, err := filepath.Abs(path)
		if err != nil {
			panic(err)
		}
		log.Infof("parse toml file once. filePath: %s\n", filePath)
		if _, err := toml.DecodeFile(filePath, &cfg); err != nil {
			panic(err)
		}
	})
	return cfg
}

func (cfg *GearConfig) WithDefaults() *GearConfig {
	d := cfg
	for index := range d.HTTPShardNode {
		if d.HTTPShardNode[index].Weight == 0 {
			d.HTTPShardNode[index].Weight = 1
		}
	}

	return d
}

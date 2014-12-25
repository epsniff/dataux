package config

import (
	"github.com/lytics/confl"
	"io/ioutil"
)

func LoadConfigFromFile(filename string) (*Config, error) {
	var c Config
	confBytes, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	//func Decode(data string, v interface{}) (MetaData, error) {
	if _, err = confl.Decode(string(confBytes), &c); err != nil {
		return nil, err
	}

	return &c, nil
}

func LoadConfig(conf string) (*Config, error) {
	var c Config
	//func Decode(data string, v interface{}) (MetaData, error) {
	if _, err := confl.Decode(conf, &c); err != nil {
		return nil, err
	}

	return &c, nil
}

type Config struct {
	Addr     string         `confl:"addr"`      // net.Conn compatible ip/dns address
	User     string         `confl:"user"`      // user to talk to backend with
	Password string         `confl:"password"`  // optional pwd for backend
	LogLevel string         `confl:"log_level"` // [debug,info,error,]
	Nodes    []NodeConfig   `confl:"nodes"`
	Schemas  []SchemaConfig `confl:"schemas"`
}

type NodeConfig struct {
	Name             string `confl:"name"`
	DownAfterNoAlive int    `confl:"down_after_noalive"`
	IdleConns        int    `confl:"idle_conns"`
	RWSplit          bool   `confl:"rw_split"`
	User             string `confl:"user"`
	Password         string `confl:"password"`
	Master           string `confl:"master"`
	Slave            string `confl:"slave"`
}

type SchemaConfig struct {
	BackendType string      `confl:"backend_type"` // [mysql,elasticsearch]
	DB          string      `confl:"db"`
	Nodes       []string    `confl:"nodes"`
	RulesConifg RulesConfig `confl:"rules"`
}

type RulesConfig struct {
	Default   string        `confl:"default"`
	ShardRule []ShardConfig `confl:"shard"`
}

type ShardConfig struct {
	Table string   `confl:"table"`
	Key   string   `confl:"key"`
	Nodes []string `confl:"nodes"`
	Type  string   `confl:"type"`
	Range string   `confl:"range"`
}

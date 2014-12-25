package models

import (
	"fmt"
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

// Master config
type Config struct {
	Addr      string            `json:"addr"`      // net.Conn compatible ip/dns address
	User      string            `json:"user"`      // user to talk to backend with
	Password  string            `json:"password"`  // optional pwd for backend
	LogLevel  string            `json:"log_level"` // [debug,info,error,]
	Frontends []*ListenerConfig `json:"listeners"` // tcp listener configs
	Backends  []*BackendConfig  `json:"backends"`  // backend servers (es, mysql etc)
	Schemas   []*SchemaConfig   `json:"schemas"`   // virtual schema
}

// Backends are storage/database/servers/csvfiles
// eventually this should come from a coordinator (etcd/zk/etc)
type BackendConfig struct {
	Name             string `json:"name"`
	BackendType      string `json:"backend_type"`
	DownAfterNoAlive int    `json:"down_after_noalive"`
	IdleConns        int    `json:"idle_conns"`
	RWSplit          bool   `json:"rw_split"`
	User             string `json:"user"`
	Password         string `json:"password"`
	Master           string `json:"master"`
	Slave            string `json:"slave"`
}

func (m *BackendConfig) String() string {
	return fmt.Sprintf("<backendconf %s type=%s />", m.Name, m.BackendType)
}

type ListenerConfig struct {
	Type        string      `json:"type"` // [mysql,mongo,mc,etc]
	DB          string      `json:"db"`
	Backends    []string    `json:"backends"`
	RulesConifg RulesConfig `json:"rules"`
}

type SchemaConfig struct {
	BackendType string      `json:"backend_type"` // [mysql,elasticsearch]
	DB          string      `json:"db"`
	Backends    []string    `json:"backends"`
	RulesConifg RulesConfig `json:"rules"`
}

type RulesConfig struct {
	Default   string        `json:"default"`
	ShardRule []ShardConfig `json:"shard"`
}

type ShardConfig struct {
	Table    string   `json:"table"`
	Key      string   `json:"key"`
	Backends []string `json:"backends"`
	Type     string   `json:"type"`
	Range    string   `json:"range"`
}

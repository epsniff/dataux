package models

import (
	u "github.com/araddon/gou"
	"strings"
	"sync"
)

var (
	_ = u.EMPTY

	listenerMu    sync.Mutex
	listenerFuncs = make(map[string]ListenerInit)
)

type Listener interface {
	Run(stop chan bool) error
	Close() error
}

//type func(*models.Config) (models.Listener, error)
type ListenerInit func(*ListenerConfig, *Config) (Listener, error)

func ListenerRegister(name string, fn ListenerInit) {
	listenerMu.Lock()
	defer listenerMu.Unlock()
	name = strings.ToLower(name)
	u.Infof("registering listner [%s] ", name)
	listenerFuncs[name] = fn
}

func Listeners() map[string]ListenerInit {
	return listenerFuncs
}

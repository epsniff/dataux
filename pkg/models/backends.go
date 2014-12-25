package models

import (
	u "github.com/araddon/gou"
	"strings"
	"sync"
)

var (
	_ = u.EMPTY

	backendMu    sync.Mutex
	backendFuncs = make(map[string]BackendInit)
)

type BackendRunner interface {
	// Initial setup/configuration and validaton
	Init() error
	Run(stop chan bool) error
	Close() error
}

type BackendInit func(*BackendConfig) BackendRunner

func BackendRegister(name string, fn BackendInit) {
	backendMu.Lock()
	defer backendMu.Unlock()
	backendFuncs[strings.ToLower(name)] = fn
}

func BackendInitGet(backendType string) BackendInit {
	return backendFuncs[strings.ToLower(backendType)]
}

type Backend struct {
	mu     sync.Mutex
	Runner BackendRunner
	Conf   *BackendConfig
}

func NewBackend(conf *BackendConfig, runner BackendRunner) *Backend {
	return &Backend{Runner: runner, Conf: conf}
}

package proxy

import (
	"github.com/araddon/dataux/pkg/models"
	u "github.com/araddon/gou"

	"fmt"
	"strings"
)

var asciiIntro = `
     _       _
    | |     | |
  __| | __ _| |_ __ _ _   ___  __
 / _* |/ _* | __/ _* | | | \ \/ /
| (_| | (_| | || (_| | |_| |>  <
 \__,_|\__,_|\__\__,_|\__,_/_/\_\

`

func banner() string {
	return strings.Replace(asciiIntro, "*", "`", -1)
}

// Server is the main DataUx server, it is responsible for
//  1) starting *listeners* - network transports/protocols (mysql,mongo,redis)
//  2) sending requests through *Handlers*(plugins) which
//      filtering, transform, log, etc
//  3) routing to backend-transports which return results
type Server struct {
	conf *models.Config

	// Frontend listener is a Listener Protocol handler
	// to listen on specific port such as mysql
	frontends []models.Listener

	// backends/servers
	backends map[string]*models.Backend

	// schemas
	schemas map[string]*models.Schema

	stop chan bool
}

type Reason struct {
	Reason  string
	err     error
	Message string
}

// Running a server consists of steps
// - setup backend nodes
// - setup schemas
// - setup plugins/transforms
// - start listeners
func NewServer(conf *models.Config) (*Server, error) {

	svr := &Server{conf: conf, stop: make(chan bool)}

	svr.backends = make(map[string]*models.Backend, 0)

	if err := svr.setupBackends(); err != nil {
		return nil, err
	}

	if err := setupSchemas(svr); err != nil {
		return nil, err
	}

	return svr, nil
}

// Run is a blocking runner, that starts listeners
// and returns if connection to listeners cannot be established
func (m *Server) Run() {

	for _, frontend := range m.frontends {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					u.Errorf("frontend shutdown: %v", r)
				}
			}()
			// Blocking runner
			if err := frontend.Run(m.stop); err != nil {
				u.Errorf("error on frontend? %#v %v", frontend, err)
				m.Shutdown(Reason{"error", err, ""})
			}
		}()
	}
}

// Shutdown listeners and close down
func (m *Server) Shutdown(reason Reason) {
	m.stop <- true
}

// Find and setup/validate backend nodes
func (m *Server) setupBackends() error {

	for _, beConf := range m.conf.Backends {
		err := m.AddBackend(beConf)
		if err != nil {
			return err
		}
	}

	return nil
}

// Given a backend server config, add it to server/proxy
//  find the backend runner (starts network for backend)
//
func (m *Server) AddBackend(beConf *models.BackendConfig) error {

	beConf.Name = strings.ToLower(beConf.Name)

	if _, ok := m.backends[beConf.Name]; ok {
		return fmt.Errorf("duplicate backend [%s].", beConf.Name)
	}

	if beConf.BackendType == "" {
		for _, schemaConf := range m.conf.Schemas {
			if strings.ToLower(schemaConf.DB) == beConf.Name {
				beConf.BackendType = strings.ToLower(schemaConf.BackendType)
			}
		}
	}

	runnerFunc := models.BackendInitGet(beConf.BackendType)
	if runnerFunc == nil {
		return fmt.Errorf("Could not find backend runner for [%s]", beConf.BackendType)
	}

	runner := runnerFunc(beConf)

	if err := runner.Init(); err != nil {
		return fmt.Errorf("Error initializing runner: [%s]", beConf.BackendType)
	}

	go runner.Run(m.stop)

	m.backends[beConf.Name] = models.NewBackend(beConf, runner)

	return nil
}

func (m *Server) BackendFind(serverName string) *models.Backend {
	// for _, be := range m.conf.Backends {
	// 	if be.Name == serverName {
	// 		return be
	// 	}
	// }
	return m.backends[serverName]
	//return nil
}

package elasticsearch

import (
	"fmt"
	"github.com/araddon/dataux/pkg/models"
	u "github.com/araddon/gou"
	"github.com/araddon/qlbridge/vm"
)

/*
type Request struct {
	Raw []byte
	// Do we need source?
	// do we really need statement here
	Stmt sqlparser.Statement
}
type Handler interface {
	// Get and Set this db/schema for this persistent handler
	SchemaUse(db string) *Schema
	Handle(writer ResultWriter, req *Request) error
	Close() error
}

// Some handlers implement the Session Specific interface
type HandlerSession interface {
	Clone(conn interface{}) Handler
}

type ResultWriter interface {
	WriteResult(Result) error
}

type Result interface{}
*/

var _ = vm.ErrValue
var _ = u.EMPTY

const ListenerType = "elasticsearch"

// Handle request splitting, a single connection session
// not threadsafe, not shared
type HandlerElasticsearch struct {
	conf    *models.Config
	nodes   map[string]*models.BackendConfig
	schemas map[string]*models.Schema
	schema  *models.Schema
}

func NewHandlerElasticsearch(conf *models.Config) (models.Handler, error) {
	handler := &HandlerElasticsearch{conf: conf}
	err := handler.Init()
	return handler, err
}

func (m *HandlerElasticsearch) Init() error {

	if err := m.findEsNodes(); err != nil {
		return err
	}
	if err := m.loadSchemasFromConfig(); err != nil {
		return err
	}
	return nil
}

func (m *HandlerElasticsearch) Close() error {
	return nil
}

func (m *HandlerElasticsearch) Handle(writer models.ResultWriter, req *models.Request) error {
	u.Infof("Handle: %v", string(req.Raw))
	//return m.chooseCommand(writer, req)
	return nil
}

func (m *HandlerElasticsearch) SchemaUse(db string) *models.Schema {
	schema, ok := m.schemas[db]
	if ok {
		m.schema = schema
	} else {
		u.Warnf("Could not find schema for db=%s", db)
	}
	return schema
}

func (m *HandlerElasticsearch) loadSchemasFromConfig() error {

	m.schemas = make(map[string]*models.Schema)

	for _, schemaConf := range m.conf.Schemas {
		u.Infof("parse schemas: %v", schemaConf)
		if _, ok := m.schemas[schemaConf.DB]; ok {
			return fmt.Errorf("duplicate schema '%s'", schemaConf.DB)
		}
		if len(schemaConf.Backends) == 0 {
			return fmt.Errorf("schema '%s' must have at least one node", schemaConf.DB)
		}

		schema := &models.Schema{
			Db: schemaConf.DB,
		}

		m.schemas[schemaConf.DB] = schema
	}

	return nil
}

func (m *HandlerElasticsearch) getSchema(db string) *models.Schema {
	u.Debugf("get schema for %s", db)
	return m.schemas[db]
}

func (m *HandlerElasticsearch) findEsNodes() error {

	//m.nodes = make(map[string]*Node)

	for _, be := range m.conf.Backends {
		if be.BackendType == "" {
			for _, schemaConf := range m.conf.Schemas {
				for _, bename := range schemaConf.Backends {
					if bename == be.Name {
						be.BackendType = schemaConf.BackendType
					}
				}
			}
		}
		if be.BackendType == ListenerType {
			// if _, ok := m.nodes[be.Name]; ok {
			// 	return fmt.Errorf("duplicate node '%s'", be.Name)
			// }

			// n, err := m.startMysqlNode(be)
			// if err != nil {
			// 	return err
			// }

			u.Infof("adding node: %s", be.String())
			//m.nodes[be.Name] = n
		}
	}

	return nil
}

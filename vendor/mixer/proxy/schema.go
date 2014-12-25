package proxy

import (
	"fmt"
	"github.com/araddon/dataux/vendor/mixer/router"
)

// Schema is the schema for a named database, shared
// across multiple nodes
type Schema struct {
	db string

	nodes map[string]*Node

	rule *router.Router
}

func (m *MysqlListener) parseSchemas() error {

	m.schemas = make(map[string]*Schema)

	for _, schemaCfg := range m.cfg.Schemas {
		if _, ok := m.schemas[schemaCfg.DB]; ok {
			return fmt.Errorf("duplicate schema [%s].", schemaCfg.DB)
		}
		if len(schemaCfg.Backends) == 0 {
			return fmt.Errorf("schema [%s] must have a node.", schemaCfg.DB)
		}

		nodes := make(map[string]*Node)
		for _, n := range schemaCfg.Backends {
			if m.getNode(n) == nil {
				return fmt.Errorf("schema [%s] node [%s] config is not exists.", schemaCfg.DB, n)
			}

			if _, ok := nodes[n]; ok {
				return fmt.Errorf("schema [%s] node [%s] duplicate.", schemaCfg.DB, n)
			}

			nodes[n] = m.getNode(n)
		}

		rule, err := router.NewRouter(schemaCfg)
		if err != nil {
			return err
		}

		m.schemas[schemaCfg.DB] = &Schema{
			db:    schemaCfg.DB,
			nodes: nodes,
			rule:  rule,
		}
	}

	return nil
}

func (m *MysqlListener) getSchema(db string) *Schema {
	return m.schemas[db]
}

package proxy

import (
	"fmt"
	"github.com/araddon/dataux/pkg/models"
)

func setupSchemas(s *Server) error {

	s.schemas = make(map[string]*models.Schema)

	for _, schemaConf := range s.conf.Schemas {
		if _, ok := s.schemas[schemaConf.DB]; ok {
			return fmt.Errorf("duplicate schema [%s].", schemaConf.DB)
		}
		if len(schemaConf.Backends) == 0 {
			return fmt.Errorf("schema [%s] must have a node.", schemaConf.DB)
		}

		nodes := make(map[string]*models.Backend)
		for _, n := range schemaConf.Backends {

			if s.BackendFind(n) == nil {
				return fmt.Errorf("schema [%s] node [%s] config is not exists.", schemaConf.DB, n)
			}

			if _, ok := nodes[n]; ok {
				return fmt.Errorf("schema [%s] node [%s] duplicate.", schemaConf.DB, n)
			}

			nodes[n] = s.BackendFind(n)
		}

		// rule, err := router.NewRouter(&schemaConf)
		// if err != nil {
		// 	return err
		// }

		s.schemas[schemaConf.DB] = &models.Schema{
			Db:    schemaConf.DB,
			Nodes: nodes,
		}
		// rule:  rule,
	}

	return nil
}

func (s *Server) getSchema(db string) *models.Schema {
	return s.schemas[db]
}

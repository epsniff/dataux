package models

import ()

// Schema is the schema for a named database, shared
// across multiple nodes
type Schema struct {
	Db    string
	Nodes map[string]*BackendConfig
	Conf  *SchemaConfig
}

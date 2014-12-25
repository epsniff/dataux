package models

import (
//u "github.com/araddon/gou"
)

// Schema is the schema for a named database, shared
// across multiple nodes
type Schema struct {
	Db string

	Nodes map[string]*Backend

	//rule *Router

	Conf *SchemaConfig
}

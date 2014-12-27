package models

import (
	"github.com/araddon/dataux/vendor/mixer/sqlparser"
)

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

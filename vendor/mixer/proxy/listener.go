package proxy

import (
	"fmt"
	"net"
	"runtime"
	"strings"

	"github.com/araddon/dataux/pkg/models"
	u "github.com/araddon/gou"
)

var (
	// Ensure that we implement the interfaces we expect
	_ models.Listener = (*MysqlListener)(nil)

	// The "backend_type" for backends
	// or the listener type for frontends
	ListenerType = "mysql"

	_ = u.EMPTY
)

func ListenerInit(feConf *models.ListenerConfig, conf *models.Config) (models.Listener, error) {
	return NewMysqlListener(feConf, conf)
}

func NewMysqlListener(feConf *models.ListenerConfig, conf *models.Config) (*MysqlListener, error) {

	myl := new(MysqlListener)

	myl.cfg = conf
	myl.feconf = feConf
	myl.addr = feConf.Addr
	myl.user = feConf.User
	myl.password = feConf.Password

	var err error
	netProto := "tcp"
	if strings.Contains(netProto, "/") {
		netProto = "unix"
	}
	myl.netlistener, err = net.Listen(netProto, myl.addr)

	if err != nil {
		return nil, err
	}

	u.Infof("Server run MySql Protocol Listen(%s) at '%s'", netProto, myl.addr)
	return myl, nil
}

// MysqlListener implements proxy.Listener interface for
//  running listener connections for mysql
type MysqlListener struct {
	cfg         *models.Config
	feconf      *models.ListenerConfig
	addr        string
	user        string
	password    string
	running     bool
	netlistener net.Listener
	handler     models.Handler
}

func (m *MysqlListener) Run(handler models.Handler, stop chan bool) error {

	m.handler = handler
	u.Debugf("using handler:  %T", handler)
	m.running = true

	for m.running {
		conn, err := m.netlistener.Accept()
		if err != nil {
			u.Errorf("accept error %s", err.Error())
			continue
		}

		go m.OnConn(conn)
	}

	return nil
}

func (m *MysqlListener) Close() error {
	m.running = false
	if m.netlistener != nil {
		return m.netlistener.Close()
	}
	return nil
}

// For each new client tcp connection to this proxy
func (m *MysqlListener) OnConn(c net.Conn) {

	conn := newConn(m, c)

	if !m.cfg.SupressRecover {
		defer func() {
			if err := recover(); err != nil {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]
				u.Errorf("onConn panic %v: %v\n%s", c.RemoteAddr().String(), err, buf)
			}

			conn.Close()
		}()
	}

	u.Infof("client connected")
	if err := conn.Handshake(); err != nil {
		u.Errorf("handshake error %s", err.Error())
		c.Close()
		return
	}

	conn.Run()
}

func (m *MysqlListener) UpMaster(node string, addr string) error {

	if shardHandler, ok := m.handler.(*HandlerSharded); ok {
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node %s", node)
		}
		return n.upMaster(addr)
	} else {
		u.Warnf("UpMaster not implemented for T:%T", m.handler)
	}
	return nil
}

func (m *MysqlListener) UpSlave(node string, addr string) error {

	if shardHandler, ok := m.handler.(*HandlerSharded); ok {
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node %s", node)
		}

		return n.upSlave(addr)
	} else {
		u.Warnf("UpSlave not implemented for T:%T", m.handler)
	}
	return nil
}
func (m *MysqlListener) DownMaster(node string) error {

	if shardHandler, ok := m.handler.(*HandlerSharded); ok {
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node %s", node)
		}
		n.db = nil
		return n.downMaster()
	} else {
		u.Warnf("DownMaster not implemented for T:%T", m.handler)
	}
	return nil
}

func (m *MysqlListener) DownSlave(node string) error {

	if shardHandler, ok := m.handler.(*HandlerSharded); ok {
		return nil
		n := shardHandler.getNode(node)
		if n == nil {
			return fmt.Errorf("invalid node [%s].", node)
		}
		return n.downSlave()
	} else {
		u.Warnf("DownSlave not implemented for T:%T", m.handler)
	}
	return nil
}

// func (m *MysqlListener) getNode(name string) *Node {
// 	return m.nodes[name]
// }

// func (m *MysqlListener) parseNodes() error {

// 	cfg := m.cfg
// 	m.nodes = make(map[string]*Node)

// 	for _, be := range cfg.Backends {
// 		if be.BackendType == "" {
// 			for _, schemaConf := range m.cfg.Schemas {
// 				for _, bename := range schemaConf.Backends {
// 					if bename == be.Name {
// 						be.BackendType = schemaConf.BackendType
// 					}
// 				}
// 			}
// 		}
// 		if be.BackendType == ListenerType {
// 			if _, ok := m.nodes[be.Name]; ok {
// 				return fmt.Errorf("duplicate node '%s'", be.Name)
// 			}

// 			n, err := m.parseNode(be)
// 			if err != nil {
// 				return err
// 			}

// 			u.Infof("adding node: %s", be.String())
// 			m.nodes[be.Name] = n
// 		}
// 	}

// 	return nil
// }

// func (m *MysqlListener) parseNode(cfg *models.BackendConfig) (*Node, error) {

// 	n := new(Node)
// 	n.listener = m
// 	n.cfg = cfg

// 	n.downAfterNoAlive = time.Duration(cfg.DownAfterNoAlive) * time.Second

// 	if len(cfg.Master) == 0 {
// 		return nil, fmt.Errorf("must setting master MySQL node.")
// 	}

// 	var err error
// 	if n.master, err = n.openDB(cfg.Master); err != nil {
// 		return nil, err
// 	}

// 	n.db = n.master

// 	if len(cfg.Slave) > 0 {
// 		if n.slave, err = n.openDB(cfg.Slave); err != nil {
// 			u.Errorf("open db error", err)
// 			n.slave = nil
// 		}
// 	}

// 	go n.run()

// 	return n, nil
// }

// func (m *MysqlListener) parseSchemas() error {

// 	m.schemas = make(map[string]*Schema)

// 	for _, schemaCfg := range m.cfg.Schemas {
// 		u.Infof("parse schemas: %v", schemaCfg)
// 		if _, ok := m.schemas[schemaCfg.DB]; ok {
// 			return fmt.Errorf("duplicate schema '%s'", schemaCfg.DB)
// 		}
// 		if len(schemaCfg.Backends) == 0 {
// 			return fmt.Errorf("schema '%s' must have at least one node", schemaCfg.DB)
// 		}

// 		nodes := make(map[string]*Node)
// 		for _, n := range schemaCfg.Backends {
// 			if m.getNode(n) == nil {
// 				return fmt.Errorf("schema '%s' node '%s' config does not exist", schemaCfg.DB, n)
// 			}

// 			if _, ok := nodes[n]; ok {
// 				return fmt.Errorf("schema '%s' node '%s' is duplicate", schemaCfg.DB, n)
// 			}

// 			nodes[n] = m.getNode(n)
// 		}

// 		rule, err := router.NewRouter(schemaCfg)
// 		if err != nil {
// 			return err
// 		}

// 		m.schemas[schemaCfg.DB] = &Schema{
// 			db:    schemaCfg.DB,
// 			nodes: nodes,
// 			rule:  rule,
// 		}
// 	}

// 	return nil
// }

// func (m *MysqlListener) getSchema(db string) *Schema {
// 	u.Debugf("get schema for %s", db)
// 	return m.schemas[db]
// }

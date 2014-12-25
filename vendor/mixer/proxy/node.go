package proxy

import (
	"fmt"
	"github.com/araddon/dataux/pkg/models"
	"github.com/araddon/dataux/vendor/mixer/client"
	u "github.com/araddon/gou"
	"sync"
	"time"
)

const (
	Master = "master"
	Slave  = "slave"
)

type Node struct {
	sync.Mutex

	listener *MysqlListener

	cfg *models.BackendConfig

	//running master db
	db *client.DB

	master *client.DB
	slave  *client.DB

	downAfterNoAlive time.Duration

	lastMasterPing int64
	lastSlavePing  int64
}

func (n *Node) run() {
	//to do
	//1 check connection alive
	//2 check remove mysql server alive

	t := time.NewTicker(3000 * time.Second)
	defer t.Stop()

	n.lastMasterPing = time.Now().Unix()
	n.lastSlavePing = n.lastMasterPing
	for {
		select {
		case <-t.C:
			n.checkMaster()
			n.checkSlave()
		}
	}
}

func (n *Node) String() string {
	return n.cfg.Name
}

func (n *Node) getMasterConn() (*client.SqlConn, error) {
	n.Lock()
	db := n.db
	n.Unlock()

	if db == nil {
		return nil, fmt.Errorf("master is down")
	}
	u.Debugf("about to GetConn:   client.SqlConn")
	return db.GetConn()
}

func (n *Node) getSelectConn() (*client.SqlConn, error) {
	var db *client.DB

	n.Lock()
	if n.cfg.RWSplit && n.slave != nil {
		db = n.slave
	} else {
		db = n.db
	}
	n.Unlock()

	if db == nil {
		return nil, fmt.Errorf("no alive mysql server")
	}

	return db.GetConn()
}

func (n *Node) checkMaster() {
	n.Lock()
	db := n.db
	n.Unlock()

	if db == nil {
		u.Infof("no master avaliable")
		return
	}

	if err := db.Ping(); err != nil {
		u.Errorf("%s ping master %s error %s", n, db.Addr(), err.Error())
	} else {
		n.lastMasterPing = time.Now().Unix()
		return
	}

	if int64(n.downAfterNoAlive) > 0 && time.Now().Unix()-n.lastMasterPing > int64(n.downAfterNoAlive) {
		u.Errorf("%s down master db %s", n, n.master.Addr())

		n.downMaster()
	}
}

func (n *Node) checkSlave() {
	if n.slave == nil {
		return
	}

	db := n.slave
	if err := db.Ping(); err != nil {
		u.Errorf("%s ping slave %s error %s", n, db.Addr(), err.Error())
	} else {
		n.lastSlavePing = time.Now().Unix()
	}

	if int64(n.downAfterNoAlive) > 0 && time.Now().Unix()-n.lastSlavePing > int64(n.downAfterNoAlive) {
		u.Errorf("%s slave db %s not alive over %ds, down it",
			n, db.Addr(), int64(n.downAfterNoAlive/time.Second))

		n.downSlave()
	}
}

func (n *Node) openDB(addr string) (*client.DB, error) {
	db, err := client.Open(addr, n.cfg.User, n.cfg.Password, "")
	if err != nil {
		return nil, err
	}

	db.SetMaxIdleConnNum(n.cfg.IdleConns)
	return db, nil
}

func (n *Node) checkUpDB(addr string) (*client.DB, error) {
	db, err := n.openDB(addr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (n *Node) upMaster(addr string) error {
	n.Lock()
	if n.master != nil {
		n.Unlock()
		return fmt.Errorf("%s master must be down first", n)
	}
	n.Unlock()

	db, err := n.checkUpDB(addr)
	if err != nil {
		return err
	}

	n.Lock()
	n.master = db
	n.db = db
	n.Unlock()

	return nil
}

func (n *Node) upSlave(addr string) error {
	n.Lock()
	if n.slave != nil {
		n.Unlock()
		return fmt.Errorf("%s, slave must be down first", n)
	}
	n.Unlock()

	db, err := n.checkUpDB(addr)
	if err != nil {
		return err
	}

	n.Lock()
	n.slave = db
	n.Unlock()

	return nil
}

func (n *Node) downMaster() error {
	n.Lock()
	if n.master != nil {
		n.master = nil
	}
	return nil
}

func (n *Node) downSlave() error {
	n.Lock()
	db := n.slave
	n.slave = nil
	n.Unlock()

	if db != nil {
		db.Close()
	}

	return nil
}

func (m *MysqlListener) UpMaster(node string, addr string) error {
	n := m.getNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	return n.upMaster(addr)
}

func (m *MysqlListener) UpSlave(node string, addr string) error {
	n := m.getNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}

	return n.upSlave(addr)
}
func (m *MysqlListener) DownMaster(node string) error {
	n := m.getNode(node)
	if n == nil {
		return fmt.Errorf("invalid node %s", node)
	}
	n.db = nil
	return n.downMaster()
}

func (m *MysqlListener) DownSlave(node string) error {
	n := m.getNode(node)
	if n == nil {
		return fmt.Errorf("invalid node [%s].", node)
	}
	return n.downSlave()
}

func (m *MysqlListener) getNode(name string) *Node {
	return m.nodes[name]
}

func (m *MysqlListener) parseNodes() error {
	cfg := m.cfg
	m.nodes = make(map[string]*Node)

	for _, be := range cfg.Backends {
		if be.BackendType == "" {
			for _, schemaConf := range m.cfg.Schemas {
				for _, bename := range schemaConf.Backends {
					if bename == be.Name {
						be.BackendType = schemaConf.BackendType
					}
				}
			}
		}
		if be.BackendType == ListenerType {
			if _, ok := m.nodes[be.Name]; ok {
				return fmt.Errorf("duplicate node [%s].", be.Name)
			}

			n, err := m.parseNode(be)
			if err != nil {
				return err
			}

			u.Infof("adding node: [%s]", be.String())
			m.nodes[be.Name] = n
		}
	}

	return nil
}

func (m *MysqlListener) parseNode(cfg *models.BackendConfig) (*Node, error) {
	n := new(Node)
	n.listener = m
	n.cfg = cfg

	n.downAfterNoAlive = time.Duration(cfg.DownAfterNoAlive) * time.Second

	if len(cfg.Master) == 0 {
		return nil, fmt.Errorf("must setting master MySQL node.")
	}

	var err error
	if n.master, err = n.openDB(cfg.Master); err != nil {
		return nil, err
	}

	n.db = n.master

	if len(cfg.Slave) > 0 {
		if n.slave, err = n.openDB(cfg.Slave); err != nil {
			u.Errorf("open db error", err)
			n.slave = nil
		}
	}

	go n.run()

	return n, nil
}

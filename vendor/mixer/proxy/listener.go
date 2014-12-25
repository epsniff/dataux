package proxy

import (
	"github.com/araddon/dataux/pkg/models"
	"github.com/araddon/dataux/vendor/mixer/client"
	"github.com/araddon/dataux/vendor/mixer/mysql"
	u "github.com/araddon/gou"

	"net"
	"runtime"
	"strings"
	"sync/atomic"
)

var ListenerType = "mysql"

func ListenerInit(lc *models.ListenerConfig, cfg *models.Config) (models.Listener, error) {
	return NewMysqlListener(cfg)
}

func NewMysqlListener(cfg *models.Config) (*MysqlListener, error) {
	myl := new(MysqlListener)

	myl.cfg = cfg

	myl.addr = cfg.Addr
	myl.user = cfg.User
	myl.password = cfg.Password

	if err := myl.parseNodes(); err != nil {
		return nil, err
	}

	if err := myl.parseSchemas(); err != nil {
		return nil, err
	}

	var err error
	netProto := "tcp"
	if strings.Contains(netProto, "/") {
		netProto = "unix"
	}
	myl.netlistener, err = net.Listen(netProto, myl.addr)

	if err != nil {
		return nil, err
	}

	u.Infof("Server run MySql Protocol Listen(%s) at [%s]", netProto, myl.addr)
	return myl, nil
}

// MysqlListener implements proxy.Listener interface for
//  running listener connections for mysql
type MysqlListener struct {
	cfg *models.Config

	addr     string
	user     string
	password string

	running bool

	netlistener net.Listener

	nodes map[string]*Node

	schemas map[string]*Schema
}

func (m *MysqlListener) Run(stop chan bool) error {
	m.running = true

	for m.running {
		conn, err := m.netlistener.Accept()
		if err != nil {
			u.Errorf("accept error %s", err.Error())
			continue
		}

		go m.onConn(conn)
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

func (m *MysqlListener) onConn(c net.Conn) {

	conn := m.newConn(c)

	defer func() {
		if err := recover(); err != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]
			u.Errorf("onConn panic %v: %v\n%s", c.RemoteAddr().String(), err, buf)
		}

		conn.Close()
	}()

	u.Infof("client connected")
	if err := conn.Handshake(); err != nil {
		u.Errorf("handshake error %s", err.Error())
		c.Close()
		return
	}

	conn.Run()
}

func (m *MysqlListener) newConn(co net.Conn) *Conn {
	c := new(Conn)

	c.c = co

	c.pkg = mysql.NewPacketIO(co)

	c.listener = m

	c.c = co
	c.pkg.Sequence = 0

	c.connectionId = atomic.AddUint32(&baseConnId, 1)

	c.status = mysql.SERVER_STATUS_AUTOCOMMIT

	c.salt, _ = mysql.RandomBuf(20)

	c.txConns = make(map[*Node]*client.SqlConn)

	c.closed = false

	c.collation = mysql.DEFAULT_COLLATION_ID
	c.charset = mysql.DEFAULT_CHARSET

	c.stmtId = 0
	c.stmts = make(map[uint32]*Stmt)

	return c
}

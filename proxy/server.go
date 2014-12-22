package proxy

import (
	"github.com/araddon/dataux/config"
	u "github.com/araddon/gou"

	"net"
	"runtime"
	"strings"
)

// Server is the Proxy-Server, that fronts the backends(nodes)
//
type Server struct {
	cfg *config.Config

	addr     string
	user     string
	password string

	running bool

	listener net.Listener

	nodes map[string]*Node

	schemas map[string]*Schema
}

func NewServer(cfg *config.Config) (*Server, error) {

	s := new(Server)

	s.cfg = cfg

	s.addr = cfg.Addr
	s.user = cfg.User
	s.password = cfg.Password

	if err := s.parseNodes(); err != nil {
		return nil, err
	}

	if err := s.parseSchemas(); err != nil {
		return nil, err
	}

	var err error
	netProto := "tcp"
	if strings.Contains(netProto, "/") {
		netProto = "unix"
	}
	s.listener, err = net.Listen(netProto, s.addr)

	if err != nil {
		return nil, err
	}

	u.Infof("Server run MySql Protocol Listen(%s) at [%s]", netProto, s.addr)
	return s, nil
}

func (s *Server) Run() error {
	s.running = true

	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			u.Errorf("accept error %s", err.Error())
			continue
		}

		go s.onConn(conn)
	}

	return nil
}

func (s *Server) Close() {
	s.running = false
	if s.listener != nil {
		s.listener.Close()
	}
}

func (s *Server) onConn(c net.Conn) {

	conn := s.newConn(c)

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

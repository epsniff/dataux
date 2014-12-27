package proxy

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/araddon/dataux/pkg/models"
	"github.com/araddon/dataux/vendor/mixer/client"
	"github.com/araddon/dataux/vendor/mixer/mysql"
	u "github.com/araddon/gou"
)

// Each new connection gets a connection id
var baseConnId uint32 = 10000

var DEFAULT_CAPABILITY uint32 = mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_LONG_FLAG |
	mysql.CLIENT_CONNECT_WITH_DB | mysql.CLIENT_PROTOCOL_41 |
	mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_SECURE_CONNECTION

// Conn serves as a Frontend (inbound listener) on mysql
// protocol
//
//	--> frontend --> handlers --> backend
type Conn struct {
	sync.Mutex

	pkg          *mysql.PacketIO
	c            net.Conn
	listener     *MysqlListener
	noRecover    bool
	handler      models.Handler // Handle inbound Requests to be routed to backends
	capability   uint32
	connectionId uint32
	status       uint16
	collation    mysql.CollationId
	charset      string
	user         string
	db           string
	salt         []byte
	schema       *models.Schema
	txConns      map[*Node]*client.SqlConn
	closed       bool
	lastInsertId int64
	affectedRows int64
	stmtId       uint32
	stmts        map[uint32]*Stmt
}

func newConn(m *MysqlListener, co net.Conn) *Conn {
	c := new(Conn)

	c.c = co

	c.pkg = mysql.NewPacketIO(co)

	c.listener = m
	if handlerMaker, ok := c.listener.handler.(models.HandlerSession); ok {
		c.handler = handlerMaker.Clone(c)
	} else {
		u.Warnf("We are not cloning?  %T", c.listener.handler)
		// not session specific so re-use handler
		c.handler = c.listener.handler
	}

	c.noRecover = c.listener.cfg.SupressRecover
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

// Run is a blocking command PER client connection it is called
// AFTER Handshake()
func (c *Conn) Run() {

	if !c.noRecover {
		u.Debugf("running recovery? %v", !c.noRecover)
		defer func() {
			r := recover()
			if err, ok := r.(error); ok {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]

				u.Errorf("%v, %s", err, buf)
			}

			c.Close()
		}()
	} else {
		u.Debugf("Suppressing recovery? %v", !c.noRecover)
	}

	for {
		data, err := c.readPacket()

		if err != nil {
			return
		}

		u.Debugf("Run() -> handler.Handle(): %v", string(data))
		if err := c.handler.Handle(c, &models.Request{Raw: data}); err != nil {
			u.Errorf("dispatch error %v", err)
			if err != mysql.ErrBadConn {
				c.writeError(err)
			}
		}

		if c.closed {
			return
		}

		c.pkg.Sequence = 0
	}
}

func (c *Conn) Handshake() error {

	if err := c.writeInitialHandshake(); err != nil {
		u.Errorf("send initial handshake error %s", err.Error())
		return err
	}

	if err := c.readHandshakeResponse(); err != nil {
		u.Errorf("recv handshake response error %s", err.Error())

		c.writeError(err)

		return err
	}

	if err := c.writeOK(nil); err != nil {
		u.Errorf("write ok fail %s", err.Error())
		return err
	}

	c.pkg.Sequence = 0

	return nil
}

func (c *Conn) Close() error {
	if c.closed {
		return nil
	}

	c.c.Close()

	c.rollback()

	c.closed = true

	return nil
}

func (c *Conn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)

	//connection id
	data = append(data, byte(c.connectionId), byte(c.connectionId>>8), byte(c.connectionId>>16), byte(c.connectionId>>24))

	//auth-plugin-data-part-1
	data = append(data, c.salt[0:8]...)

	//filter [00]
	data = append(data, 0)

	//capability flag lower 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY), byte(DEFAULT_CAPABILITY>>8))

	//charset, utf-8 default
	data = append(data, uint8(mysql.DEFAULT_COLLATION_ID))

	//status
	data = append(data, byte(c.status), byte(c.status>>8))

	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY>>16), byte(DEFAULT_CAPABILITY>>24))

	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, c.salt[8:]...)

	//filter [00]
	data = append(data, 0)

	return c.writePacket(data)
}

func (c *Conn) readPacket() ([]byte, error) {
	return c.pkg.ReadPacket()
}

func (c *Conn) writePacket(data []byte) error {
	return c.pkg.WritePacket(data)
}

func (c *Conn) readHandshakeResponse() error {

	data, err := c.readPacket()

	if err != nil {
		return err
	}

	pos := 0

	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	//user name
	c.user = string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
	pos += len(c.user) + 1

	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos+authLen]

	checkAuth := mysql.CalcPassword(c.salt, []byte(c.listener.feconf.Password))

	if !bytes.Equal(auth, checkAuth) {
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.c.RemoteAddr().String(), c.user, "Yes")
	}

	pos += authLen

	if c.capability|mysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db := string(data[pos : pos+bytes.IndexByte(data[pos:], 0)])
		pos += len(c.db) + 1

		if err := c.useDB(db); err != nil {
			return err
		}
	}

	return nil
}

// func (c *Conn) dispatch(data []byte) error {

// 	cmd := data[0]
// 	data = data[1:]

// 	u.Debugf("dispatch: %v ", cmd)
// 	switch cmd {
// 	case mysql.COM_QUIT:
// 		c.Close()
// 		return nil
// 	case mysql.COM_QUERY:
// 		return c.handleQuery(hack.String(data))
// 	case mysql.COM_PING:
// 		return c.writeOK(nil)
// 	case mysql.COM_INIT_DB:
// 		if err := c.useDB(hack.String(data)); err != nil {
// 			return err
// 		} else {
// 			return c.writeOK(nil)
// 		}
// 	case mysql.COM_FIELD_LIST:
// 		return c.handleFieldList(data)
// 	case mysql.COM_STMT_PREPARE:
// 		return c.handleStmtPrepare(hack.String(data))
// 	case mysql.COM_STMT_EXECUTE:
// 		return c.handleStmtExecute(data)
// 	case mysql.COM_STMT_CLOSE:
// 		return c.handleStmtClose(data)
// 	case mysql.COM_STMT_SEND_LONG_DATA:
// 		return c.handleStmtSendLongData(data)
// 	case mysql.COM_STMT_RESET:
// 		return c.handleStmtReset(data)
// 	default:
// 		msg := fmt.Sprintf("command %d not supported now", cmd)
// 		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
// 	}

// 	return nil
// }

func (c *Conn) useDB(db string) error {
	u.Infof("listener connection UseDB: %v", db)
	if s := c.handler.SchemaUse(db); s == nil {
		u.Errorf("could not load schema: %v", db)
		return mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, db)
	} else {
		c.schema = s
		c.db = db
	}
	return nil
}

func (c *Conn) writeOK(r *mysql.Result) error {
	if r == nil {
		r = &mysql.Result{Status: c.status}
	}
	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	u.Infof("writeOk: %v", r.AffectedRows)
	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status>>8))
		data = append(data, 0, 0)
	}
	err := c.writePacket(data)
	if err != nil && err == io.EOF {
		c.c.Close()
		u.Errorf("closing conn:  %v", err)
		return err
	} else if err != nil {
		c.c.Close()
		u.Errorf("closing conn:  %v", err)
		return err
	}
	return c.writePacket(data)
}

func (c *Conn) writeError(e error) error {
	var m *mysql.SqlError
	var ok bool
	if m, ok = e.(*mysql.SqlError); !ok {
		m = mysql.NewError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16+len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code>>8))

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.writePacket(data)
}

func (c *Conn) writeEOF(status uint16) error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status>>8))
	}

	return c.writePacket(data)
}

func buildSimpleSelectResult(value interface{}, name []byte, asName []byte) (*mysql.Resultset, error) {

	field := &mysql.Field{}

	field.Name = name

	if asName != nil {
		field.Name = asName
	}

	field.OrgName = name

	formatField(field, value)

	r := &mysql.Resultset{Fields: []*mysql.Field{field}}
	row, err := formatValue(value)
	if err != nil {
		return nil, err
	}
	r.RowDatas = append(r.RowDatas, mysql.PutLengthEncodedString(row))

	return r, nil
}

func (c *Conn) writeFieldList(status uint16, fs []*mysql.Field) error {
	c.affectedRows = int64(-1)

	data := make([]byte, 4, 1024)

	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump()...)
		if err := c.writePacket(data); err != nil {
			return err
		}
	}

	if err := c.writeEOF(status); err != nil {
		return err
	}
	return nil
}

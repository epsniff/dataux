package proxy

import (
	"flag"
	"sync"
	"testing"
	"time"

	"github.com/araddon/dataux/pkg/models"
	"github.com/araddon/dataux/vendor/mixer/client"
	u "github.com/araddon/gou"
	"github.com/bmizerany/assert"
)

var (
	_              = u.EMPTY
	testServerOnce sync.Once
	testListener   *testListenerWraper
	testHandler    *HandlerSharded
	testDBOnce     sync.Once
	testDB         *client.DB
	verbose        bool
)

func init() {
	flag.BoolVar(&verbose, "vv", false, "verbose tests")
	flag.Parse()
	if verbose {
		u.SetupLogging("debug")
		u.SetColorOutput()
	}
}

var testConfigData = `

supress_recover: true

frontends [
  {
    name : mysql 
    type : "mysql"
    addr : "127.0.0.1:4000"
    user : root
    #password : 
  }
]



backends [
  {
    name : node1 
    down_after_noalive : 300
    idle_conns : 16
    rw_split : false
    user : root
    #password: ""
    master : "localhost:3307"
    #slave : ""
  },
  {
    name : node2
    user: root
    down_after_noalive : 300
    idle_conns : 16
    rw_split: false
    master : "localhost:3308"
  },
  {
    name : node3 
    down_after_noalive : 300
    idle_conns : 16
    rw_split: false
    user : root
    master : "localhost:3309"
  }
]

# schemas
schemas : [
  {
    db : mixer
    backends : ["node1", "node2", "node3"]
    backend_type : mysql
    # list of rules
    rules : {
      default : node1
      # shards
      shard : [
        {
          table : mixer_test_shard_hash
          key : id
          backends: [ "node2", "node3"]
          type : hash
        },
        {
          table: mixer_test_shard_range
          key: id
          type: range
          backends: [ node2, node3 ]
          range: "-10000-"
        }
      ]
    }
  }
]
`

type testListenerWraper struct {
	*MysqlListener
	nodes map[string]*Node
}

func newTestServer(t *testing.T) *testListenerWraper {
	f := func() {
		cfg, err := models.LoadConfig(testConfigData)
		assert.Tf(t, err == nil, "must load config without err: %v", err)

		myl, err := NewMysqlListener(cfg.Frontends[0], cfg)
		assert.Tf(t, err == nil, "must create listener without err: %v", err)
		handler, err := NewHandlerSharded(cfg)
		assert.Tf(t, err == nil, "must create handler without err: %v", err)
		testHandler = handler.(*HandlerSharded)

		testListener = &testListenerWraper{myl, testHandler.nodes}

		go testListener.Run(testHandler, make(chan bool))

		// delay to ensure we have time to connect
		time.Sleep(100 * time.Millisecond)
	}

	testServerOnce.Do(f)

	return testListener
}

func newTestDB(t *testing.T) *client.DB {
	newTestServer(t)

	f := func() {
		var err error
		testDB, err = client.Open("127.0.0.1:4000", "root", "", "mixer")

		assert.Tf(t, err == nil, "must not err: %v", err)

		testDB.SetMaxIdleConnNum(4)
	}

	testDBOnce.Do(f)
	return testDB
}

func newTestDBConn(t *testing.T) *client.SqlConn {
	db := newTestDB(t)

	c, err := db.GetConn()

	if err != nil {
		t.Fatal(err)
	}

	return c
}

func TestServer(t *testing.T) {
	newTestServer(t)
}

package proxy

import (
	"github.com/araddon/dataux/pkg/models"
	"github.com/araddon/dataux/vendor/mixer/client"
	u "github.com/araddon/gou"

	"sync"
	"testing"
	"time"
)

var _ = u.EMPTY
var testServerOnce sync.Once
var testListener *MysqlListener
var testDBOnce sync.Once
var testDB *client.DB

func init() {
	u.SetupLogging("debug")
	u.SetColorOutput()
}

var testConfigData = `

addr : "127.0.0.1:4000"
user : root

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

func newTestServer(t *testing.T) *MysqlListener {
	f := func() {
		cfg, err := models.LoadConfig(testConfigData)
		if err != nil {
			t.Fatal(err.Error())
		}

		testListener, err = NewMysqlListener(cfg)
		if err != nil {
			t.Fatal(err)
		}

		go testListener.Run(make(chan bool))

		time.Sleep(1 * time.Second)
	}

	testServerOnce.Do(f)

	return testListener
}

func newTestDB(t *testing.T) *client.DB {
	newTestServer(t)

	f := func() {
		var err error
		testDB, err = client.Open("127.0.0.1:4000", "root", "", "mixer")

		if err != nil {
			t.Fatal(err)
		}

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

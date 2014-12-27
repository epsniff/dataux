package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/araddon/dataux/pkg/models"
	"github.com/araddon/dataux/pkg/proxy"
	mysqlproxy "github.com/araddon/dataux/vendor/mixer/proxy"
	u "github.com/araddon/gou"
)

var (
	configFile *string = flag.String("config", "dataux.conf", "dataux proxy config file")
	logLevel   *string = flag.String("loglevel", "debug", "log level [debug|info|warn|error], default error")
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if len(*configFile) == 0 {
		u.Errorf("must use a config file")
		return
	}
	u.SetupLogging(*logLevel)
	u.SetColorIfTerminal()

	// get config
	conf, err := models.LoadConfigFromFile(*configFile)
	if err != nil {
		u.Errorf("Could not load config: %v", err)
		os.Exit(1)
	}
	mysqlShardedHandler, err := mysqlproxy.NewHandlerSharded(conf)
	if err != nil {
		u.Errorf("Could not create handlers: %v", err)
		os.Exit(1)
	}
	// Load our Frontend Listener's
	models.ListenerRegister(mysqlproxy.ListenerType,
		mysqlproxy.ListenerInit,
		mysqlShardedHandler,
	)

	var svr *proxy.Server
	svr, err = proxy.NewServer(conf)
	if err != nil {
		u.Errorf("%v", err)
		return
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		u.Infof("Got signal [%d] to exit.", sig)
		svr.Shutdown(proxy.Reason{Reason: "signal", Message: fmt.Sprintf("%v", sig)})
	}()

	svr.Run()
}

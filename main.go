package main

import (
	"flag"
	"github.com/araddon/dataux/config"
	"github.com/araddon/dataux/proxy"
	u "github.com/araddon/gou"
	"github.com/siddontang/go-log/log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
)

var (
	configFile *string = flag.String("config", "mixer.conf", "mixer proxy config file")
	logLevel   *string = flag.String("loglevel", "debug", "log level [debug|info|warn|error], default error")
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if len(*configFile) == 0 {
		u.Errorf("must use a config file")
		return
	}

	cfg, err := config.ParseConfigFile(*configFile)
	if err != nil {
		u.Errorf(err.Error())
		return
	}

	u.SetupLogging(*logLevel)
	u.SetColorIfTerminal()
	if *logLevel != "" {
		setLogLevel(*logLevel)
	} else {
		setLogLevel(cfg.LogLevel)
	}

	var svr *proxy.Server
	svr, err = proxy.NewServer(cfg)
	if err != nil {
		u.Errorf(err.Error())
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
		svr.Close()
	}()

	svr.Run()
}

func setLogLevel(level string) {
	switch strings.ToLower(level) {
	case "debug":
		log.SetLevel(log.LevelDebug)
	case "info":
		log.SetLevel(log.LevelInfo)
	case "warn":
		log.SetLevel(log.LevelWarn)
	case "error":
		log.SetLevel(log.LevelError)
	default:
		log.SetLevel(log.LevelError)
	}
}

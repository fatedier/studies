// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/c4pt0r/cfg"
	"github.com/docopt/docopt-go"

	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

// global objects
var (
	globalEnv            Env // 全局的环境变量
	livingNode           string
	createdDashboardNode bool // 是否在zk上成功创建dashboard节点
)

type Command struct {
	Run   func(cmd *Command, args []string)
	Usage string
	Short string
	Long  string
	Flag  flag.FlagSet
	Ctx   interface{}
}

var usage = `usage: codis-config  [-c <config_file>] [-L <log_file>] [--log-level=<loglevel>]
		<command> [<args>...]
options:
   -c	set config file
   -L	set output log file, default is stdout
   --log-level=<loglevel>	set log level: info, warn, error, debug [default: info]

commands:
	server      redis 服务器组管理
	slot        slot 管理
	dashboard   启动 dashboard 服务
	action      事件管理 (目前只有删除历史事件的日志)
	proxy       proxy 管理
`

func init() {
	log.SetLevel(log.LEVEL_INFO)
}

func setLogLevel(level string) {
	var lv = log.LEVEL_INFO
	switch strings.ToLower(level) {
	case "error":
		lv = log.LEVEL_ERROR
	case "warn", "warning":
		lv = log.LEVEL_WARN
	case "debug":
		lv = log.LEVEL_DEBUG
	case "info":
		fallthrough
	default:
		lv = log.LEVEL_INFO
	}
	log.SetLevel(lv)
	log.Infof("set log level to %s", lv)
}

// 根据cmd执行不同的操作
func runCommand(cmd string, args []string) (err error) {
	argv := make([]string, 1)
	argv[0] = cmd
	argv = append(argv, args...)
	switch cmd {
	case "action":
		return errors.Trace(cmdAction(argv))
	case "dashboard":
		return errors.Trace(cmdDashboard(argv))
	case "server":
		return errors.Trace(cmdServer(argv))
	case "proxy":
		return errors.Trace(cmdProxy(argv))
	case "slot":
		return errors.Trace(cmdSlot(argv))
	}
	return errors.Errorf("%s is not a valid command. See 'codis-config -h'", cmd)
}

func main() {
	// 设置程序中断时的回调函数
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)    // ctrl-c
	signal.Notify(c, syscall.SIGTERM) // kill 15
	go func() {
		<-c
		// 如果是作为dashboard启动，会去zk中删除dashboard节点
		if createdDashboardNode {
			releaseDashboardNode()
		}
		log.Panicf("ctrl-c or SIGTERM found, exit")
	}()

	// 解析命令行参数
	args, err := docopt.Parse(usage, nil, true, utils.Version, true)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// set output log file
	if s, ok := args["-L"].(string); ok && s != "" {
		f, err := os.OpenFile(s, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
		if err != nil {
			log.PanicErrorf(err, "open log file failed: %s", s)
		} else {
			defer f.Close()
			log.StdLog = log.New(f, "")
		}
	}
	log.SetLevel(log.LEVEL_INFO)
	log.SetFlags(log.Flags() | log.Lshortfile)

	// set log level
	if s, ok := args["--log-level"].(string); ok && s != "" {
		setLogLevel(s)
	}

	// set config file
	var configFile string
	if args["-c"] != nil {
		configFile = args["-c"].(string)
	} else {
		configFile = "config.ini"
	}
	config := cfg.NewCfg(configFile)

	if err := config.Load(); err != nil {
		log.PanicErrorf(err, "load config file error")
	}

	// load global vars
	globalEnv = LoadCodisEnv(config)

	cmd := args["<command>"].(string)    // 启动命令
	cmdArgs := args["<args>"].([]string) // 针对启动命令的参数

	// 监听10086端口
	go http.ListenAndServe(":10086", nil)

	// 根据command执行不同的操作
	err = runCommand(cmd, cmdArgs)
	if err != nil {
		log.PanicErrorf(err, "run sub-command failed")
	}
}

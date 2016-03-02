// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	stdlog "log"

	"github.com/codegangsta/martini-contrib/binding"
	"github.com/codegangsta/martini-contrib/render"
	"github.com/docopt/docopt-go"
	"github.com/go-martini/martini"
	"github.com/martini-contrib/cors"
	"github.com/wandoulabs/zkhelper"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

// 解析启动dashboard的参数
func cmdDashboard(argv []string) (err error) {
	usage := `usage: codis-config dashboard [--addr=<address>] [--http-log=<log_file>]

options:
	--addr	listen ip:port, e.g. localhost:18087, :18087, [default: :18087]
	--http-log	http request log [default: request.log ]
`

	args, err := docopt.Parse(usage, argv, true, "", false)
	if err != nil {
		log.ErrorErrorf(err, "parse args failed")
		return err
	}
	log.Debugf("parse args = {%+v}", args)

    // 日志路径
	logFileName := "request.log"
	if args["--http-log"] != nil {
		logFileName = args["--http-log"].(string)
	}

	addr := ":18087"
    // dashboard绑定地址
	if args["--addr"] != nil {
		addr = args["--addr"].(string)
	}

	runDashboard(addr, logFileName)
	return nil
}

var (
	proxiesSpeed int64              // ops/s，每隔一秒中更新一次，从proxy的debug-http接口获取
	safeZkConn   zkhelper.Conn      // 和zk之间的安全连接（如果连接出错，重连不上，程序会挂掉）
	unsafeZkConn zkhelper.Conn      // 和zk之间的非安全连接
)

func jsonRet(output map[string]interface{}) (int, string) {
	b, err := json.Marshal(output)
	if err != nil {
		log.WarnErrorf(err, "to json failed")
	}
	return 200, string(b)
}

func jsonRetFail(errCode int, msg string) (int, string) {
	return jsonRet(map[string]interface{}{
		"ret": errCode,
		"msg": msg,
	})
}

func jsonRetSucc() (int, string) {
	return jsonRet(map[string]interface{}{
		"ret": 0,
		"msg": "OK",
	})
}

// 先从zk上获取所有proxy的信息，之后通过每个proxy的debug-http接口获取该proxy的ops，汇总后返回
func getAllProxyOps() int64 {
    // 从zk上获取所有proxy的状态信息
    // 这里用的是 unsafeZkConn，如果出错的话不会退出程序
	proxies, err := models.ProxyList(unsafeZkConn, globalEnv.ProductName(), nil)
	if err != nil {
		log.ErrorErrorf(err, "get proxy list failed")
		return -1
	}

	var total int64
	for _, p := range proxies {
		i, err := p.Ops()
		if err != nil {
			log.WarnErrorf(err, "get proxy ops failed")
		}
		total += i
	}
	return total
}

// for debug
// 获取所有proxy的状态信息
func getAllProxyDebugVars() map[string]map[string]interface{} {
	proxies, err := models.ProxyList(unsafeZkConn, globalEnv.ProductName(), nil)
	if err != nil {
		log.ErrorErrorf(err, "get proxy list failed")
		return nil
	}

	ret := make(map[string]map[string]interface{})
	for _, p := range proxies {
		m, err := p.DebugVars()
		if err != nil {
			log.WarnErrorf(err, "get proxy debug varsfailed")
		}
		ret[p.Id] = m
	}
	return ret
}

func pageSlots(r render.Render) {
	r.HTML(200, "slots", nil)
}

// 在zk上创建dashboard节点
func createDashboardNode() error {

	// make sure root dir is exists
	rootDir := fmt.Sprintf("/zk/codis/db_%s", globalEnv.ProductName())
    // 创建集群在zk上的根节点
	zkhelper.CreateRecursive(safeZkConn, rootDir, "", 0, zkhelper.DefaultDirACLs())

	zkPath := fmt.Sprintf("%s/dashboard", rootDir)
	// make sure we're the only one dashboard
    // 如果zk上已经存在dashboard节点信息，报错退出程序
	if exists, _, _ := safeZkConn.Exists(zkPath); exists {
		data, _, _ := safeZkConn.Get(zkPath)
		return errors.New("dashboard already exists: " + string(data))
	}

    // dashboard节点信息
	content := fmt.Sprintf(`{"addr": "%v", "pid": %v}`, globalEnv.DashboardAddr(), os.Getpid())
	pathCreated, err := safeZkConn.Create(zkPath, []byte(content), 0, zkhelper.DefaultFileACLs())
	createdDashboardNode = true
	log.Infof("dashboard node created: %v, %s", pathCreated, string(content))
	log.Warn("********** Attention **********")
	log.Warn("You should use `kill {pid}` rather than `kill -9 {pid}` to stop me,")
	log.Warn("or the node resisted on zk will not be cleaned when I'm quiting and you must remove it manually")
	log.Warn("*******************************")
	return errors.Trace(err)
}

// 将 dashboard 信息从zk中删除
func releaseDashboardNode() {
	zkPath := fmt.Sprintf("/zk/codis/db_%s/dashboard", globalEnv.ProductName())
	if exists, _, _ := safeZkConn.Exists(zkPath); exists {
		log.Infof("removing dashboard node")
		safeZkConn.Delete(zkPath, 0)
	}
}

// 启动dashboard
func runDashboard(addr string, httpLogFile string) {
	log.Infof("dashboard listening on addr: %s", addr)
    // 利用martini框架创建web服务，这里获取一个martini实例
	m := martini.Classic()

    // http日志
	f, err := os.OpenFile(httpLogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.PanicErrorf(err, "open http log file failed")
	}
	defer f.Close()
	m.Map(stdlog.New(f, "[martini]", stdlog.LstdFlags))

    // 获取当前目录的绝对路径
	binRoot, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.PanicErrorf(err, "get binroot path failed")
	}

    // 设置静态文件目录以及模版目录
	m.Use(martini.Static(filepath.Join(binRoot, "assets/statics")))
	m.Use(render.Renderer(render.Options{
		Directory:  filepath.Join(binRoot, "assets/template"),
		Extensions: []string{".tmpl", ".html"},
		Charset:    "UTF-8",
		IndentJSON: true,
	}))

    // 设置允许的类型
	m.Use(cors.Allow(&cors.Options{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"POST", "GET", "DELETE", "PUT"},
		AllowHeaders:     []string{"Origin", "x-requested-with", "Content-Type", "Content-Range", "Content-Disposition", "Content-Description"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: false,
	}))

    // 设置url路由
    // 具体实现代码在 dashborad_api.go 中

    // 获取集群中所有 group 的信息，包括所有redis-server的信息
	m.Get("/api/server_groups", apiGetServerGroupList)
    // 获取总览信息，包括每个group中处于master身份的redis-server信息
	m.Get("/api/overview", apiOverview)
    // 获取具体某一台redis-server的info信息
	m.Get("/api/redis/:addr/stat", apiRedisStat)
    // 获取指定redis-serer上的指定slotId的key的数量
	m.Get("/api/redis/:addr/:id/slotinfo", apiGetRedisSlotInfo)
    // 查询指定slotId在某一group中的key的数量
	m.Get("/api/redis/group/:group_id/:slot_id/slotinfo", apiGetRedisSlotInfoFromGroupId)

    // 创建新的group节点信息
	m.Put("/api/server_groups", binding.Json(models.ServerGroup{}), apiAddServerGroup)
    // 添加指定redis-server到已经存在的group中，如果不存在，就创建这个group
	m.Put("/api/server_group/(?P<id>[0-9]+)/addServer", binding.Json(models.Server{}), apiAddServerToGroup)
    // 删除group信息（要确定没有slot存储在这个group中）
	m.Delete("/api/server_group/(?P<id>[0-9]+)", apiRemoveServerGroup)

    // 从group中删除指定地址的redis-server
	m.Put("/api/server_group/(?P<id>[0-9]+)/removeServer", binding.Json(models.Server{}), apiRemoveServerFromGroup)
    // 获取指定id的group的信息，包括内部所有的redis-server
	m.Get("/api/server_group/(?P<id>[0-9]+)", apiGetServerGroup)
    // 将指定group_id中的指定地址的redis-server提升为master
	m.Post("/api/server_group/(?P<id>[0-9]+)/promote", binding.Json(models.Server{}), apiPromoteServer)

    // 获取集群当前的迁移信息，每次只能有一个slot处于迁移状态
	m.Get("/api/migrate/status", apiMigrateStatus)
    // 获取zk migrate_tasks节点下的所有迁移任务信息，每次只有一个slot处于迁移状态，其他任务都需要等待
	m.Get("/api/migrate/tasks", apiGetMigrateTasks)
    // 执行迁移slot的任务，依次迁移，每次迁移一个slot
	m.Post("/api/migrate", binding.Json(migrateTaskForm{}), apiDoMigrate)

    // 对slot进行负载均衡
	m.Post("/api/rebalance", apiRebalance)

    // 获取所有slot的信息
	m.Get("/api/slot/list", apiGetSlots)
    // 获取指定id的slot的信息
	m.Get("/api/slot/:id", apiGetSingleSlot)
    // 初始化所有slot信息
	m.Post("/api/slots/init", apiInitSlots)
    // 获取所有slot的信息，同 /api/slot/list
	m.Get("/api/slots", apiGetSlots)
    // 分配 from-to slotId之间的slot到指定group
	m.Post("/api/slot", binding.Json(RangeSetTask{}), apiSlotRangeSet)
    // 获取所有proxy的信息
	m.Get("/api/proxy/list", apiGetProxyList)
    // 获取所有proxy的状态信息
	m.Get("/api/proxy/debug/vars", apiGetProxyDebugVars)
    // 设置proxy状态
	m.Post("/api/proxy", binding.Json(models.ProxyInfo{}), apiSetProxyStatus)
    
    // 删除zk上的 aciton 和 ActionResponse 下的部分节点
	m.Get("/api/action/gc", apiActionGC)
    // 强制删除zk上的Lock节点
	m.Get("/api/force_remove_locks", apiForceRemoveLocks)
    // 删除fence节点下处于异常状态的节点信息
	m.Get("/api/remove_fence", apiRemoveFence)

	m.Get("/slots", pageSlots)
	m.Get("/", func(r render.Render) {
		r.Redirect("/admin")
	})

    // 建立和zk之间的连接
	zkBuilder := utils.NewConnBuilder(globalEnv.NewZkConn)
	safeZkConn = zkBuilder.GetSafeConn()
	unsafeZkConn = zkBuilder.GetUnsafeConn()

	// create temp node in ZK
	if err := createDashboardNode(); err != nil {
		log.PanicErrorf(err, "create zk node failed") // do not release dashborad node here
	}

	// create long live migrate manager
    // 这里会创建一个循环执行的协程，用于从 /zk/codis/db_xxx/migrate_tasks 读取迁移任务并执行
	globalMigrateManager = NewMigrateManager(safeZkConn, globalEnv.ProductName())

	go func() {
		tick := time.Tick(time.Second)
		var lastCnt, qps int64
		for _ = range tick {
            // 获取所有proxy的ops之和
			cnt := getAllProxyOps()
            log.Debugf("getAllProxyOps return: %d", cnt)
			if cnt > 0 {
				qps = cnt - lastCnt
				lastCnt = cnt
			} else {
				qps = 0
			}
			atomic.StoreInt64(&proxiesSpeed, qps)
		}
	}()

	m.RunOnAddr(addr)
}

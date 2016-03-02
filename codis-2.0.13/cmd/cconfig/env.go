// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"os"
	"strings"

	"github.com/c4pt0r/cfg"

	"github.com/wandoulabs/zkhelper"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

type Env interface {
	ProductName() string
	Password() string
	DashboardAddr() string
	NewZkConn() (zkhelper.Conn, error) // 创建新的zk连接
}

type CodisEnv struct {
	zkAddr        string // zk或者etcd连接地址
	passwd        string // 连接后端redis的密码
	dashboardAddr string // dashboard的地址
	productName   string // 集群的名称
	provider      string // zookeeper or etcd
}

func LoadCodisEnv(cfg *cfg.Cfg) Env {
	if cfg == nil {
		log.Panicf("config is nil")
	}

	// 集群名称
	productName, err := cfg.ReadString("product", "test")
	if err != nil {
		log.PanicErrorf(err, "config: 'product' not found")
	}

	// zk地址
	zkAddr, err := cfg.ReadString("zk", "localhost:2181")
	if err != nil {
		log.PanicErrorf(err, "config: 'zk' not found")
	}

	// 取本机hostname
	hostname, _ := os.Hostname()
	// dashboard绑定地址，如果没有，默认为hostname:18087
	dashboardAddr, err := cfg.ReadString("dashboard_addr", hostname+":18087")
	if err != nil {
		log.PanicErrorf(err, "config: 'dashboard_addr' not found")
	}

	// 启用zk还是etcd
	provider, err := cfg.ReadString("coordinator", "zookeeper")
	if err != nil {
		log.PanicErrorf(err, "config: 'coordinator' not found")
	}

	// 连接redis的密码
	passwd, _ := cfg.ReadString("password", "")

	return &CodisEnv{
		zkAddr:        zkAddr,
		passwd:        passwd,
		dashboardAddr: dashboardAddr,
		productName:   productName,
		provider:      provider,
	}
}

func (e *CodisEnv) ProductName() string {
	return e.productName
}

func (e *CodisEnv) Password() string {
	return e.passwd
}

func (e *CodisEnv) DashboardAddr() string {
	return e.dashboardAddr
}

func (e *CodisEnv) NewZkConn() (zkhelper.Conn, error) {
	switch e.provider {
	case "zookeeper":
		// 这里可以考虑从配置文件读取
		// modify by fatedier
		return zkhelper.ConnectToZk(e.zkAddr, 60000)
		// end
	case "etcd":
		addr := strings.TrimSpace(e.zkAddr)
		if !strings.HasPrefix(addr, "http://") {
			addr = "http://" + addr
		}
		return zkhelper.NewEtcdConn(addr, 30)
	}
	return nil, errors.Errorf("need coordinator in config file, %s", e)
}

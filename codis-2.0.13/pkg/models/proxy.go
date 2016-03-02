// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"

	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/wandoulabs/go-zookeeper/zk"
	"github.com/wandoulabs/zkhelper"
)

// proxy状态
const (
	PROXY_STATE_ONLINE       = "online"
	PROXY_STATE_OFFLINE      = "offline"
	PROXY_STATE_MARK_OFFLINE = "mark_offline"
)

type ProxyInfo struct {
	Id           string `json:"id"`             // proxy名字，用户在配置文件中配置
	Addr         string `json:"addr"`           // proxy服务地址，用于提供代理服务
	LastEvent    string `json:"last_event"`
	LastEventTs  int64  `json:"last_event_ts"`
	State        string `json:"state"`          // 当前状态
	Description  string `json:"description"`    // 描述
	DebugVarAddr string `json:"debug_var_addr"` // debug地址
	Pid          int    `json:"pid"`            // 进程pid
	StartAt      string `json:"start_at"`       // 启动时间
}

// 通过调用proxy的用于debug的http服务地址，来获取proxy的相关信息
func (p *ProxyInfo) Ops() (int64, error) {
	resp, err := http.Get("http://" + p.DebugVarAddr + "/debug/vars")
	if err != nil {
		return -1, errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return -1, errors.Trace(err)
	}

	m := make(map[string]interface{})
	err = json.Unmarshal(body, &m)
	if err != nil {
		return -1, errors.Trace(err)
	}

	if v, ok := m["router"]; ok {
		if vv, ok := v.(map[string]interface{})["ops"]; ok {
			return int64(vv.(float64)), nil
		}
	}

	return 0, nil
}

func (p *ProxyInfo) DebugVars() (map[string]interface{}, error) {
	resp, err := http.Get("http://" + p.DebugVarAddr + "/debug/vars")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	m := make(map[string]interface{})
	err = json.Unmarshal(body, &m)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return m, nil
}

// proxy信息在zk中存放的路径
func GetProxyPath(productName string) string {
	return fmt.Sprintf("/zk/codis/db_%s/proxy", productName)
}

// 在zk上创建proxy信息
func CreateProxyInfo(zkConn zkhelper.Conn, productName string, pi *ProxyInfo) (string, error) {
	data, err := json.Marshal(pi)
	if err != nil {
		return "", errors.Trace(err)
	}
	dir := GetProxyPath(productName)
	zkhelper.CreateRecursive(zkConn, dir, "", 0, zkhelper.DefaultDirACLs())
	return zkConn.Create(path.Join(dir, pi.Id), data, zk.FlagEphemeral, zkhelper.DefaultFileACLs())
}


func GetProxyFencePath(productName string) string {
	return fmt.Sprintf("/zk/codis/db_%s/fence", productName)
}

// 在fence节点下创建proxy信息
func CreateProxyFenceNode(zkConn zkhelper.Conn, productName string, pi *ProxyInfo) (string, error) {
	return zkhelper.CreateRecursive(zkConn, path.Join(GetProxyFencePath(productName), pi.Addr), "",
		0, zkhelper.DefaultFileACLs())
}

// 从zk中获取所有的proxy的信息，可以通过filter指定过滤函数
func ProxyList(zkConn zkhelper.Conn, productName string, filter func(*ProxyInfo) bool) ([]ProxyInfo, error) {
	ret := make([]ProxyInfo, 0)
	root := GetProxyPath(productName)
	proxies, _, err := zkConn.Children(root)
	if err != nil && !zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		return nil, errors.Trace(err)
	}

	for _, proxyName := range proxies {
		pi, err := GetProxyInfo(zkConn, productName, proxyName)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if filter == nil || filter(pi) == true {
			ret = append(ret, *pi)
		}
	}

	return ret, nil
}

// 获取fence节点下的所有proxy的信息，value为true
func GetFenceProxyMap(zkConn zkhelper.Conn, productName string) (map[string]bool, error) {
	children, _, err := zkConn.Children(GetProxyFencePath(productName))
	if err != nil {
		if err.Error() == zk.ErrNoNode.Error() {
			return make(map[string]bool), nil
		} else {
			return nil, err
		}
	}
	m := make(map[string]bool, len(children))
	for _, fenceNode := range children {
		m[fenceNode] = true
	}
	return m, nil
}

var ErrUnknownProxyStatus = errors.New("unknown status, should be (online offline)")

func SetProxyStatus(zkConn zkhelper.Conn, productName string, proxyName string, status string) error {
    // 根据proxyName获取proxy的详细信息
	p, err := GetProxyInfo(zkConn, productName, proxyName)
	if err != nil {
		return errors.Trace(err)
	}

	if status != PROXY_STATE_ONLINE && status != PROXY_STATE_MARK_OFFLINE && status != PROXY_STATE_OFFLINE {
		return errors.Errorf("%v, %s", ErrUnknownProxyStatus, status)
	}

	// check slot status before setting proxy online
    // 如果要设置proxy状态为online，需要检查所有slot的状态是否为online或者migrate
	if status == PROXY_STATE_ONLINE {
		slots, err := Slots(zkConn, productName)
		if err != nil {
			return errors.Trace(err)
		}
		for _, slot := range slots {
			if slot.State.Status != SLOT_STATUS_ONLINE && slot.State.Status != SLOT_STATUS_MIGRATE {
				return errors.Errorf("slot %v is not online or migrate", slot)
			}
			if slot.GroupId == INVALID_ID {
				return errors.Errorf("slot %v has invalid group id", slot)
			}
		}
	}

	p.State = status
	b, _ := json.Marshal(p)

	_, err = zkConn.Set(path.Join(GetProxyPath(productName), proxyName), b, -1)
	if err != nil {
		return errors.Trace(err)
	}

    // 如果是将proxy下线的操作，前面变更过proxy的状态了，这里监听在该节点，等待proxy退出将该节点删除
	if status == PROXY_STATE_MARK_OFFLINE {
		// wait for the proxy down
		for {
			_, _, c, err := zkConn.GetW(path.Join(GetProxyPath(productName), proxyName))
			if zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}
			<-c
			info, err := GetProxyInfo(zkConn, productName, proxyName)
			log.Info("mark_offline, check proxy status:", proxyName, info, err)
			if zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
				log.Info("shutdown proxy successful")
				return nil
			} else if err != nil {
				return errors.Trace(err)
			}
			if info.State == PROXY_STATE_OFFLINE {
				log.Infof("proxy: %s offline success!", proxyName)
				return nil
			}
		}
	}

	return nil
}

// 从zk的proxy的节点上获取该proxy的详细信息
func GetProxyInfo(zkConn zkhelper.Conn, productName string, proxyName string) (*ProxyInfo, error) {
	var pi ProxyInfo
	data, _, err := zkConn.Get(path.Join(GetProxyPath(productName), proxyName))
	if err != nil {
		return nil, errors.Trace(err)
	}

	if err := json.Unmarshal(data, &pi); err != nil {
		return nil, errors.Trace(err)
	}

	return &pi, nil
}

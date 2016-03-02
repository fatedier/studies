// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"encoding/json"
	"path"

	topo "github.com/wandoulabs/go-zookeeper/zk"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/wandoulabs/zkhelper"
)

// 集群拓扑结构相关

type TopoUpdate interface {
	OnGroupChange(groupId int)
	OnSlotChange(slotId int)
}

type ZkFactory func(zkAddr string, zkSessionTimeout int) (zkhelper.Conn, error)

// 集群拓扑信息管理对象，封装了一些获取集群信息的操作
type Topology struct {
	ProductName      string        // 集群项目名称
	zkAddr           string        // zk地址
	zkConn           zkhelper.Conn // zk连接
	fact             ZkFactory     // 通过此函数返回一个zk连接（考虑到兼容etcd）
	provider         string        // zk or etcd
	zkSessionTimeout int           // zk会话超时时间
}

func (top *Topology) GetGroup(groupId int) (*models.ServerGroup, error) {
	return models.GetGroup(top.zkConn, top.ProductName, groupId)
}

func (top *Topology) Exist(path string) (bool, error) {
	return zkhelper.NodeExists(top.zkConn, path)
}

// 获取指定id的slot信息，并且获取所在group的信息
func (top *Topology) GetSlotByIndex(i int) (*models.Slot, *models.ServerGroup, error) {
	slot, err := models.GetSlot(top.zkConn, top.ProductName, i)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	groupServer, err := models.GetGroup(top.zkConn, top.ProductName, slot.GroupId)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return slot, groupServer, nil
}

// 创建集群拓扑管理对象
func NewTopo(ProductName string, zkAddr string, f ZkFactory, provider string, zkSessionTimeout int) *Topology {
	t := &Topology{zkAddr: zkAddr, ProductName: ProductName, fact: f, provider: provider, zkSessionTimeout: zkSessionTimeout}
	if t.fact == nil {
		switch t.provider {
		case "etcd":
			t.fact = zkhelper.NewEtcdConn
		case "zookeeper":
			t.fact = zkhelper.ConnectToZk
		default:
			log.Panicf("coordinator not found in config")
		}
	}
	t.InitZkConn()
	return t
}

// 建立信息的zk连接
func (top *Topology) InitZkConn() {
	var err error
	top.zkConn, err = top.fact(top.zkAddr, top.zkSessionTimeout)
	if err != nil {
		log.PanicErrorf(err, "init failed")
	}
}

// 根据序号获取通知信息
func (top *Topology) GetActionWithSeq(seq int64) (*models.Action, error) {
	return models.GetActionWithSeq(top.zkConn, top.ProductName, seq, top.provider)
}

// 根据序号获取解析后的action对象
func (top *Topology) GetActionWithSeqObject(seq int64, act *models.Action) error {
	return models.GetActionObject(top.zkConn, top.ProductName, seq, act, top.provider)
}

func (top *Topology) GetActionSeqList(productName string) ([]int, error) {
	return models.GetActionSeqList(top.zkConn, productName)
}

func (top *Topology) IsChildrenChangedEvent(e interface{}) bool {
	return e.(topo.Event).Type == topo.EventNodeChildrenChanged
}

// 在zk上创建proxy信息
func (top *Topology) CreateProxyInfo(pi *models.ProxyInfo) (string, error) {
	return models.CreateProxyInfo(top.zkConn, top.ProductName, pi)
}

// 在fence节点下创建proxy信息
func (top *Topology) CreateProxyFenceNode(pi *models.ProxyInfo) (string, error) {
	return models.CreateProxyFenceNode(top.zkConn, top.ProductName, pi)
}

// 获取指定id的proxy的信息
func (top *Topology) GetProxyInfo(proxyName string) (*models.ProxyInfo, error) {
	return models.GetProxyInfo(top.zkConn, top.ProductName, proxyName)
}

// 根据 action 里的序号，返回 ActionResponse 里的路径
func (top *Topology) GetActionResponsePath(seq int) string {
	return path.Join(models.GetActionResponsePath(top.ProductName), top.zkConn.Seq2Str(int64(seq)))
}

func (top *Topology) SetProxyStatus(proxyName string, status string) error {
	return models.SetProxyStatus(top.zkConn, top.ProductName, proxyName, status)
}

// 关闭topology对象，删除zk上的相关节点
func (top *Topology) Close(proxyName string) {
	// delete fence znode
	// 删除fence节点上该proxy的信息
	pi, err := models.GetProxyInfo(top.zkConn, top.ProductName, proxyName)
	if err != nil {
		log.Errorf("killing fence error, proxy %s is not exists", proxyName)
	} else {
		zkhelper.DeleteRecursive(top.zkConn, path.Join(models.GetProxyFencePath(top.ProductName), pi.Addr), -1)
	}
	// delete ephemeral znode
	// 删除 proxy 节点上的该proxy的信息
	zkhelper.DeleteRecursive(top.zkConn, path.Join(models.GetProxyPath(top.ProductName), proxyName), -1)
	top.zkConn.Close()
}

// 回复通知，就是在 ActionResponse 的 seq 节点下创建以自己 proxy_id 命名的节点
func (top *Topology) DoResponse(seq int, pi *models.ProxyInfo) error {
	//create response node
	actionPath := top.GetActionResponsePath(seq)
	//log.Debug("actionPath:", actionPath)
	data, err := json.Marshal(pi)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = top.zkConn.Create(path.Join(actionPath, pi.Id), data,
		0, zkhelper.DefaultFileACLs())

	return err
}

// 监听函数，zk有变更会从 evtch 收到变更事件，处理后发送到 evtbus 通道中
func (top *Topology) doWatch(evtch <-chan topo.Event, evtbus chan interface{}) {
	e := <-evtch
	if e.State == topo.StateExpired || e.Type == topo.EventNotWatching {
		log.Panicf("session expired: %+v", e)
	}

	log.Warnf("topo event %+v", e)

	switch e.Type {
	//case topo.EventNodeCreated:
	//case topo.EventNodeDataChanged:
	case topo.EventNodeChildrenChanged: //only care children changed
		//todo:get changed node and decode event
	default:
		log.Warnf("%+v", e)
	}

	evtbus <- e
}

// 监听目录
func (top *Topology) WatchChildren(path string, evtbus chan interface{}) ([]string, error) {
	content, _, evtch, err := top.zkConn.ChildrenW(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	go top.doWatch(evtch, evtbus)
	return content, nil
}

// 监听一个指定节点
func (top *Topology) WatchNode(path string, evtbus chan interface{}) ([]byte, error) {
	content, _, evtch, err := top.zkConn.GetW(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	go top.doWatch(evtch, evtbus)
	return content, nil
}

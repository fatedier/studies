// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/wandoulabs/zkhelper"
)

// redis-server的状态类型
//
const (
	SERVER_TYPE_MASTER  string = "master"
	SERVER_TYPE_SLAVE   string = "slave"
	SERVER_TYPE_OFFLINE string = "offline"
)

// redis server instance
type Server struct {
	Type    string `json:"type"`     // 类型， master,slave,offline
	GroupId int    `json:"group_id"` // 所属group_id
	Addr    string `json:"addr"`     // 地址
}

// redis server group
type ServerGroup struct {
	Id          int       `json:"id"`           // group_id
	ProductName string    `json:"product_name"` // 集群名称
	Servers     []*Server `json:"servers"`      // 包含的所有redis-server信息
}

func (self *Server) String() string {
	b, _ := json.MarshalIndent(self, "", "  ")
	return string(b)
}

func (self *ServerGroup) String() string {
	b, _ := json.MarshalIndent(self, "", "  ")
	return string(b) + "\n"
}

// 给定路径，返回redis-server信息
func GetServer(zkConn zkhelper.Conn, zkPath string) (*Server, error) {
	data, _, err := zkConn.Get(zkPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	srv := Server{}
	if err := json.Unmarshal(data, &srv); err != nil {
		return nil, errors.Trace(err)
	}
	return &srv, nil
}

func NewServer(serverType string, addr string) *Server {
	return &Server{
		Type:    serverType,
		GroupId: INVALID_ID,
		Addr:    addr,
	}
}

func NewServerGroup(productName string, id int) *ServerGroup {
	return &ServerGroup{
		Id:          id,
		ProductName: productName,
	}
}

// 确认指定的group_id在zk上是否存在
func GroupExists(zkConn zkhelper.Conn, productName string, groupId int) (bool, error) {
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", productName, groupId)
	exists, _, err := zkConn.Exists(zkPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	return exists, nil
}

// 获取指定group_id的group信息，包括所有的内部的redis-server信息
func GetGroup(zkConn zkhelper.Conn, productName string, groupId int) (*ServerGroup, error) {
	exists, err := GroupExists(zkConn, productName, groupId)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !exists {
		return nil, errors.Errorf("group %d is not found", groupId)
	}

	group := &ServerGroup{
		ProductName: productName,
		Id:          groupId,
	}

	group.Servers, err = group.GetServers(zkConn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return group, nil
}

// 获取集群中所有groups的信息，包括内部所有redis-server的信息
func ServerGroups(zkConn zkhelper.Conn, productName string) ([]*ServerGroup, error) {
	var ret []*ServerGroup
	root := fmt.Sprintf("/zk/codis/db_%s/servers", productName)
	groups, _, err := zkConn.Children(root)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Buggy :X
	//zkhelper.ChildrenRecursive(*zkConn, root)

	for _, group := range groups {
		// parse group_1 => 1
		groupId, err := strconv.Atoi(strings.Split(group, "_")[1])
		if err != nil {
			return nil, errors.Trace(err)
		}
		g, err := GetGroup(zkConn, productName, groupId)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, g)
	}
	return ret, nil
}

// 返回此group中的类型为master的redis-server
func (self *ServerGroup) Master(zkConn zkhelper.Conn) (*Server, error) {
	servers, err := self.GetServers(zkConn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, s := range servers {
		// TODO check if there are two masters
		if s.Type == SERVER_TYPE_MASTER {
			return s, nil
		}
	}
	return nil, nil
}

// 删除group信息（要确定没有slot存储在这个group中）
func (self *ServerGroup) Remove(zkConn zkhelper.Conn) error {
	// check if this group is not used by any slot
	// 获取所有的slots信息，检查是否有slot在要删除的group中，如果有，不能删除
	slots, err := Slots(zkConn, self.ProductName)
	if err != nil {
		return errors.Trace(err)
	}

	for _, slot := range slots {
		if slot.GroupId == self.Id {
			return errors.Errorf("group %d is using by slot %d", slot.GroupId, slot.Id)
		}
		if (slot.State.Status == SLOT_STATUS_MIGRATE || slot.State.Status == SLOT_STATUS_PRE_MIGRATE) && slot.State.MigrateStatus.From == self.Id {
			return errors.Errorf("slot %d has residual data remain in group %d", slot.Id, self.Id)
		}
	}

	// do delete
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	err = zkhelper.DeleteRecursive(zkConn, zkPath, -1)

	// we know that there's no slots affected, so this action doesn't need proxy confirm
	err = NewAction(zkConn, self.ProductName, ACTION_TYPE_SERVER_GROUP_REMOVE, self, "", false)
	return errors.Trace(err)
}

// 从此group中删除指定地址的 redis-server
func (self *ServerGroup) RemoveServer(zkConn zkhelper.Conn, addr string) error {
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d/%s", self.ProductName, self.Id, addr)
	data, _, err := zkConn.Get(zkPath)
	if err != nil {
		return errors.Trace(err)
	}

	var s Server
	err = json.Unmarshal(data, &s)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info(s)
	// 不能删除master节点的redis-server
	if s.Type == SERVER_TYPE_MASTER {
		return errors.Errorf("cannot remove master, use promote first")
	}

	err = zkConn.Delete(zkPath, -1)
	if err != nil {
		return errors.Trace(err)
	}

	// update server list
	for i := 0; i < len(self.Servers); i++ {
		if self.Servers[i].Addr == s.Addr {
			self.Servers = append(self.Servers[:i], self.Servers[i+1:]...)
			break
		}
	}

	// remove slave won't need proxy confirm
	// 删除slave节点对一致性没有影响，不需要等待proxy回复
	err = NewAction(zkConn, self.ProductName, ACTION_TYPE_SERVER_GROUP_CHANGED, self, "", false)
	return errors.Trace(err)
}

// 提升指定地址的redis-server为这个group中的master
// 这里有个问题就是原来的master会被下线，并且其余的slave-server仍然slave of 旧的master，需要手动操作
func (self *ServerGroup) Promote(conn zkhelper.Conn, addr, passwd string) error {
	var s *Server
	exists := false
	// 检查要提升为master的server是否存在
	for i := 0; i < len(self.Servers); i++ {
		if self.Servers[i].Addr == addr {
			s = self.Servers[i]
			exists = true
			break
		}
	}

	if !exists {
		return errors.Errorf("no such addr %s", addr)
	}

	// 先取消此机器的 slave
	err := utils.SlaveNoOne(s.Addr, passwd)
	if err != nil {
		return errors.Trace(err)
	}

	// 先将原先的master的机器置为 offline
	// set origin master offline
	master, err := self.Master(conn)
	if err != nil {
		return errors.Trace(err)
	}

	// old master may be nil
	if master != nil {
		master.Type = SERVER_TYPE_OFFLINE
		err = self.AddServer(conn, master, passwd)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// promote new server to master
	// 将新的server提升为master
	s.Type = SERVER_TYPE_MASTER
	err = self.AddServer(conn, s, passwd)
	return errors.Trace(err)
}

// 在zk上新建group节点
func (self *ServerGroup) Create(zkConn zkhelper.Conn) error {
	if self.Id < 0 {
		return errors.Errorf("invalid server group id %d", self.Id)
	}
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	// 创建这个group的节点
	_, err := zkhelper.CreateOrUpdate(zkConn, zkPath, "", 0, zkhelper.DefaultDirACLs(), true)
	if err != nil {
		return errors.Trace(err)
	}
	// 创建一个通知
	err = NewAction(zkConn, self.ProductName, ACTION_TYPE_SERVER_GROUP_CHANGED, self, "", false)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// 从zk上根据group_id获取自身的信息，获取不到的表示不存在
func (self *ServerGroup) Exists(zkConn zkhelper.Conn) (bool, error) {
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	b, err := zkhelper.NodeExists(zkConn, zkPath)
	if err != nil {
		return false, errors.Trace(err)
	}
	return b, nil
}

var ErrNodeExists = errors.New("node already exists")

// 向此group中添加一台redis-server
func (self *ServerGroup) AddServer(zkConn zkhelper.Conn, s *Server, passwd string) error {
	s.GroupId = self.Id

	// 获取此group中已经存在的redis-server的信息
	servers, err := self.GetServers(zkConn)
	if err != nil {
		return errors.Trace(err)
	}
	var masterAddr string
	for _, server := range servers {
		if server.Type == SERVER_TYPE_MASTER {
			masterAddr = server.Addr
		}
	}

	// make sure there is only one master
	if s.Type == SERVER_TYPE_MASTER && len(masterAddr) > 0 {
		return errors.Trace(ErrNodeExists)
	}

	// if this group has no server. auto promote this server to master
	// 如果添加的这个server是这个group中的第一台，默认为master节点
	if len(servers) == 0 {
		s.Type = SERVER_TYPE_MASTER
	}

	val, err := json.Marshal(s)
	if err != nil {
		return errors.Trace(err)
	}

	// 在group节点下面创建server节点
	zkPath := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d/%s", self.ProductName, self.Id, s.Addr)
	_, err = zkhelper.CreateOrUpdate(zkConn, zkPath, string(val), 0, zkhelper.DefaultFileACLs(), true)
	if err != nil {
		return errors.Trace(err)
	}

	// update servers
	servers, err = self.GetServers(zkConn)
	if err != nil {
		return errors.Trace(err)
	}
	self.Servers = servers

	// 如果添加的是master节点，要通知所有proxies有节点变更，并且等待全部回复
	if s.Type == SERVER_TYPE_MASTER {
		err = NewAction(zkConn, self.ProductName, ACTION_TYPE_SERVER_GROUP_CHANGED, self, "", true)
		if err != nil {
			return errors.Trace(err)
		}
	} else if s.Type == SERVER_TYPE_SLAVE && len(masterAddr) > 0 {
		// send command slaveof to slave
		// 使这个server成为此group中master的slave
		err := utils.SlaveOf(s.Addr, passwd, masterAddr)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// 获取这个group中所有的redis-server的信息
func (self *ServerGroup) GetServers(zkConn zkhelper.Conn) ([]*Server, error) {
	var ret []*Server
	root := fmt.Sprintf("/zk/codis/db_%s/servers/group_%d", self.ProductName, self.Id)
	nodes, _, err := zkConn.Children(root)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, node := range nodes {
		nodePath := root + "/" + node
		s, err := GetServer(zkConn, nodePath)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ret = append(ret, s)
	}
	return ret, nil
}

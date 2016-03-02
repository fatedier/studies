// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-martini/martini"

	"github.com/wandoulabs/go-zookeeper/zk"
	"github.com/wandoulabs/zkhelper"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

// 迁移任务的管理对象
var globalMigrateManager *MigrateManager

// 分配slots到指定group
type RangeSetTask struct {
	FromSlot   int    `json:"from"`
	ToSlot     int    `json:"to"`
	NewGroupId int    `json:"new_group"`
	Status     string `json:"status"`
}

// 获取所有proxy的状态信息
func apiGetProxyDebugVars() (int, string) {
	m := getAllProxyDebugVars()
	if m == nil {
		return 500, "Error getting proxy debug vars"
	}

	b, err := json.MarshalIndent(m, " ", "  ")
	if err != nil {
		log.WarnErrorf(err, "to json failed")
		return 500, err.Error()
	}

	return 200, string(b)
}

// 获取总览信息，包括每个group中处于master身份的redis-server信息
func apiOverview() (int, string) {
	// get all server groups
	groups, err := models.ServerGroups(unsafeZkConn, globalEnv.ProductName())
	if err != nil && !zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		return 500, err.Error()
	}

	var instances []string

	for _, group := range groups {
		for _, srv := range group.Servers {
			if srv.Type == "master" {
				instances = append(instances, srv.Addr)
			}
		}
	}

	info := make(map[string]interface{})
	info["product"] = globalEnv.ProductName()
	info["ops"] = proxiesSpeed

	redisInfos := make([]map[string]string, 0)

	// 获取每一个group中的master redis-server的info信息
	if len(instances) > 0 {
		for _, instance := range instances {
			info, err := utils.GetRedisStat(instance, globalEnv.Password())
			if err != nil {
				log.ErrorErrorf(err, "get redis stat failed")
			}
			redisInfos = append(redisInfos, info)
		}
	}
	info["redis_infos"] = redisInfos

	b, err := json.MarshalIndent(info, " ", "  ")
	return 200, string(b)
}

// 获取集群中所有 group 的信息，包括所有redis-server的信息
func apiGetServerGroupList() (int, string) {
	groups, err := models.ServerGroups(safeZkConn, globalEnv.ProductName())
	if err != nil {
		log.ErrorErrorf(err, "get server groups failed")
		return 500, err.Error()
	}
	b, err := json.MarshalIndent(groups, " ", "  ")
	return 200, string(b)
}

// 初始化所有slot信息
func apiInitSlots(r *http.Request) (int, string) {
	r.ParseForm()
	isForce := false
	// 如果 is_force 参数为true，则即使slot已经存在，也强制初始化slot
	val := r.FormValue("is_force")
	if len(val) > 0 && (val == "1" || val == "true") {
		isForce = true
	}
	if !isForce {
		s, _ := models.Slots(safeZkConn, globalEnv.ProductName())
		if len(s) > 0 {
			return 500, "slots already initialized, you may use 'is_force' flag and try again."
		}
	}

	if err := models.InitSlotSet(safeZkConn, globalEnv.ProductName(), models.DEFAULT_SLOT_NUM); err != nil {
		log.ErrorErrorf(err, "init slot set failed")
		return 500, err.Error()
	}
	return jsonRetSucc()
}

// 获取具体某一台redis-server的info信息
func apiRedisStat(param martini.Params) (int, string) {
	addr := param["addr"]
	info, err := utils.GetRedisStat(addr, globalEnv.Password())
	if err != nil {
		return 500, err.Error()
	}
	b, _ := json.MarshalIndent(info, " ", "  ")
	return 200, string(b)
}

// slot迁移信息
type migrateTaskForm struct {
	From  int `json:"from"`
	To    int `json:"to"`
	Group int `json:"new_group"`
	Delay int `json:"delay"`
}

// 执行迁移slot的任务，依次迁移，每次迁移一个slot
func apiDoMigrate(form migrateTaskForm) (int, string) {
	// 将N多个slot的迁移工作拆分成一个slot一个迁移任务
	for i := form.From; i <= form.To; i++ {
		task := MigrateTaskInfo{
			SlotId:     i,
			Delay:      form.Delay,
			NewGroupId: form.Group,
			Status:     MIGRATE_TASK_PENDING,
			CreateAt:   strconv.FormatInt(time.Now().Unix(), 10),
		}
		// 在zk的 migrate_task 节点下创建顺序节点建立迁移任务
		globalMigrateManager.PostTask(&task)
	}
	// do migrate async
	return jsonRetSucc()
}

// 对slot进行负载均衡
func apiRebalance(param martini.Params) (int, string) {
	if len(globalMigrateManager.Tasks()) > 0 {
		return 500, "there are migration tasks running, you should wait them done"
	}
	if err := Rebalance(); err != nil {
		log.ErrorErrorf(err, "rebalance failed")
		return 500, err.Error()
	}
	return jsonRetSucc()
}

// 获取zk migrate_tasks节点下的所有迁移任务信息，每次只有一个slot处于迁移状态，其他任务都需要等待
func apiGetMigrateTasks() (int, string) {
	tasks := globalMigrateManager.Tasks()
	b, _ := json.MarshalIndent(tasks, " ", "  ")
	return 200, string(b)
}

// 获取指定id的group的信息，包括内部所有的redis-server
func apiGetServerGroup(param martini.Params) (int, string) {
	id := param["id"]
	groupId, err := strconv.Atoi(id)
	if err != nil {
		log.ErrorErrorf(err, "parse groupid failed")
		return 500, err.Error()
	}
	group, err := models.GetGroup(safeZkConn, globalEnv.ProductName(), groupId)
	if err != nil {
		log.ErrorErrorf(err, "get group %d failed", groupId)
		return 500, err.Error()
	}
	b, err := json.MarshalIndent(group, " ", "  ")
	return 200, string(b)
}

// 获取集群当前的迁移信息，每次只能有一个slot处于迁移状态
func apiMigrateStatus() (int, string) {
	// 获取状态处于 SLOT_STATUS_MIGRATE 和 SLOT_STATUS_PRE_MIGRATE 的节点信息
	migrateSlots, err := models.GetMigratingSlots(safeZkConn, globalEnv.ProductName())
	if err != nil && !zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
		return 500, err.Error()
	}

	b, err := json.MarshalIndent(map[string]interface{}{
		"migrate_slots": migrateSlots,
		"migrate_task":  globalMigrateManager.runningTask,
	}, " ", "  ")
	return 200, string(b)
}

// 获取指定redis-serer上的指定slotId的key的数量
func apiGetRedisSlotInfo(param martini.Params) (int, string) {
	addr := param["addr"]
	slotId, err := strconv.Atoi(param["id"])
	if err != nil {
		log.ErrorErrorf(err, "parse slotid failed")
		return 500, err.Error()
	}
	// 向某一台redis-sever请求，获取从 fromSlot 到 toSlot 之间的分别属于这些slotId的key的数量
	slotInfo, err := utils.SlotsInfo(addr, globalEnv.Password(), slotId, slotId)
	if err != nil {
		log.ErrorErrorf(err, "get slot info %d failed", slotId)
		return 500, err.Error()
	}
	out, _ := json.MarshalIndent(map[string]interface{}{
		"keys":    slotInfo[slotId],
		"slot_id": slotId,
	}, " ", "  ")
	return 200, string(out)
}

// 查询指定slotId在某一group中的key的数量
func apiGetRedisSlotInfoFromGroupId(param martini.Params) (int, string) {
	groupId, err := strconv.Atoi(param["group_id"])
	if err != nil {
		log.ErrorErrorf(err, "parse groupid failed")
		return 500, err.Error()
	}
	slotId, err := strconv.Atoi(param["slot_id"])
	if err != nil {
		log.ErrorErrorf(err, "parse slotid failed")
		return 500, err.Error()
	}
	// 获取group信息
	g, err := models.GetGroup(safeZkConn, globalEnv.ProductName(), groupId)
	if err != nil {
		log.ErrorErrorf(err, "get group %d failed", groupId)
		return 500, err.Error()
	}
	// 取到redis-server master的地址
	s, err := g.Master(safeZkConn)
	if err != nil {
		log.ErrorErrorf(err, "get master of group %d failed", groupId)
		return 500, err.Error()
	}

	if s == nil {
		return 500, "master not found"
	}

	slotInfo, err := utils.SlotsInfo(s.Addr, globalEnv.Password(), slotId, slotId)
	if err != nil {
		log.ErrorErrorf(err, "get slot info %d failed", slotId)
		return 500, err.Error()
	}

	out, _ := json.MarshalIndent(map[string]interface{}{
		"keys":     slotInfo[slotId],
		"slot_id":  slotId,
		"group_id": groupId,
		"addr":     s.Addr,
	}, " ", "  ")
	return 200, string(out)

}

// 删除group信息（要确定没有slot存储在这个group中）
func apiRemoveServerGroup(param martini.Params) (int, string) {
	lock := utils.GetZkLock(safeZkConn, globalEnv.ProductName())
	if err := lock.LockWithTimeout(0, fmt.Sprintf("removing group %s", param["id"])); err != nil {
		return 500, err.Error()
	}

	defer func() {
		err := lock.Unlock()
		if err != nil && err != zk.ErrNoNode {
			log.ErrorErrorf(err, "unlock node failed")
		}
	}()

	groupId, _ := strconv.Atoi(param["id"])
	serverGroup := models.NewServerGroup(globalEnv.ProductName(), groupId)
	if err := serverGroup.Remove(safeZkConn); err != nil {
		log.ErrorErrorf(err, "remove server group failed")
		return 500, err.Error()
	}

	return jsonRetSucc()
}

// create new server group
// 创建新的group节点信息
func apiAddServerGroup(newGroup models.ServerGroup) (int, string) {
	// 获取zk锁
	lock := utils.GetZkLock(safeZkConn, globalEnv.ProductName())
	if err := lock.LockWithTimeout(0, fmt.Sprintf("add group %+v", newGroup)); err != nil {
		return 500, err.Error()
	}

	defer func() {
		err := lock.Unlock()
		if err != nil && err != zk.ErrNoNode {
			log.ErrorErrorf(err, "unlock node failed")
		}
	}()

	newGroup.ProductName = globalEnv.ProductName()

	// 如果已经存在，忽略
	exists, err := newGroup.Exists(safeZkConn)
	if err != nil {
		log.ErrorErrorf(err, "check group exits failed")
		return 500, err.Error()
	}
	if exists {
		return 500, "group already exists"
	}
	// 在zk上创建该group的节点信息
	err = newGroup.Create(safeZkConn)
	if err != nil {
		log.ErrorErrorf(err, "create node for new group failed")
		return 500, err.Error()
	}
	return jsonRetSucc()
}

// add redis server to exist server group
// 添加指定redis-server到已经存在的group中
func apiAddServerToGroup(server models.Server, param martini.Params) (int, string) {
	groupId, _ := strconv.Atoi(param["id"])
	lock := utils.GetZkLock(safeZkConn, globalEnv.ProductName())
	if err := lock.LockWithTimeout(0, fmt.Sprintf("add server to group,  %+v", server)); err != nil {
		return 500, err.Error()
	}
	defer func() {
		err := lock.Unlock()
		if err != nil && err != zk.ErrNoNode {
			log.ErrorErrorf(err, "unlock node failed")
		}
	}()
	// check group exists first
	serverGroup := models.NewServerGroup(globalEnv.ProductName(), groupId)

	exists, err := serverGroup.Exists(safeZkConn)
	if err != nil {
		log.ErrorErrorf(err, "check group exits failed")
		return 500, err.Error()
	}

	// create new group if not exists
	// 如果不存在的话，在zk上创建一个新的group，之后再把server信息添加进去
	if !exists {
		if err := serverGroup.Create(safeZkConn); err != nil {
			return 500, err.Error()
		}
	}

	// 将redis-server信息添加到group中
	if err := serverGroup.AddServer(safeZkConn, &server, globalEnv.Password()); err != nil {
		log.ErrorErrorf(err, "add server to group failed")
		return 500, err.Error()
	}

	return jsonRetSucc()
}

// 将指定group_id中的指定地址的redis-server提升为master
// 这里有个问题就是原来的master会被下线，并且其余的slave-server仍然slave of 旧的master，需要手动操作
func apiPromoteServer(server models.Server, param martini.Params) (int, string) {
	lock := utils.GetZkLock(safeZkConn, globalEnv.ProductName())
	if err := lock.LockWithTimeout(0, fmt.Sprintf("promote server %+v", server)); err != nil {
		return 500, err.Error()
	}
	defer func() {
		err := lock.Unlock()
		if err != nil && err != zk.ErrNoNode {
			log.ErrorErrorf(err, "unlock node failed")
		}
	}()

	// 获取group信息
	group, err := models.GetGroup(safeZkConn, globalEnv.ProductName(), server.GroupId)
	if err != nil {
		log.ErrorErrorf(err, "get group %d failed", server.GroupId)
		return 500, err.Error()
	}
	// 将指定的redis-server提升为master
	err = group.Promote(safeZkConn, server.Addr, globalEnv.Password())
	if err != nil {
		log.ErrorErrorf(err, "promote group %d failed", server.GroupId)
		return 500, err.Error()
	}

	return jsonRetSucc()
}

// 从此group中删除指定地址的 redis-server
func apiRemoveServerFromGroup(server models.Server, param martini.Params) (int, string) {
	groupId, _ := strconv.Atoi(param["id"])
	lock := utils.GetZkLock(safeZkConn, globalEnv.ProductName())
	if err := lock.LockWithTimeout(0, fmt.Sprintf("removing server from group, %+v", server)); err != nil {
		return 500, err.Error()
	}
	defer func() {
		err := lock.Unlock()
		if err != nil && err != zk.ErrNoNode {
			log.ErrorErrorf(err, "unlock node failed")
		}
	}()

	serverGroup := models.NewServerGroup(globalEnv.ProductName(), groupId)
	err := serverGroup.RemoveServer(safeZkConn, server.Addr)
	if err != nil {
		log.ErrorErrorf(err, "remove group %d failed", groupId)
		return 500, err.Error()
	}
	return jsonRetSucc()
}

// 设置proxy的状态
func apiSetProxyStatus(proxy models.ProxyInfo, param martini.Params) (int, string) {
	err := models.SetProxyStatus(safeZkConn, globalEnv.ProductName(), proxy.Id, proxy.State)
	if err != nil {
		// if this proxy is not online, just return success
		if proxy.State == models.PROXY_STATE_MARK_OFFLINE && zkhelper.ZkErrorEqual(err, zk.ErrNoNode) {
			return jsonRetSucc()
		}
		log.ErrorErrorf(err, "set proxy states failed: %+v", proxy)
		return 500, err.Error()
	}
	return jsonRetSucc()
}

// 获取所有proxy的状态信息
func apiGetProxyList(param martini.Params) (int, string) {
	// 最后nil表示获取全部，不指定过滤函数
	proxies, err := models.ProxyList(safeZkConn, globalEnv.ProductName(), nil)
	if err != nil {
		log.ErrorErrorf(err, "get proxy list failed")
		return 500, err.Error()
	}
	b, err := json.MarshalIndent(proxies, " ", "  ")
	return 200, string(b)
}

// 获取指定id的slot的信息
func apiGetSingleSlot(param martini.Params) (int, string) {
	id, err := strconv.Atoi(param["id"])
	if err != nil {
		return 500, err.Error()
	}

	slot, err := models.GetSlot(safeZkConn, globalEnv.ProductName(), id)
	if err != nil {
		log.ErrorErrorf(err, "get slot %d failed", id)
		return 500, err.Error()
	}

	b, err := json.MarshalIndent(slot, " ", "  ")
	return 200, string(b)
}

// 获取所有slot的信息
func apiGetSlots() (int, string) {
	slots, err := models.Slots(safeZkConn, globalEnv.ProductName())
	if err != nil {
		log.ErrorErrorf(err, "Error getting slot info, try init slots first?")
		return 500, err.Error()
	}
	b, err := json.MarshalIndent(slots, " ", "  ")
	return 200, string(b)
}

// 分配 from-to slotId之间的slot到指定group
func apiSlotRangeSet(task RangeSetTask) (int, string) {
	lock := utils.GetZkLock(safeZkConn, globalEnv.ProductName())
	if err := lock.LockWithTimeout(0, fmt.Sprintf("set slot range, %+v", task)); err != nil {
		return 500, err.Error()
	}
	defer func() {
		err := lock.Unlock()
		if err != nil && err != zk.ErrNoNode {
			log.ErrorErrorf(err, "unlock node failed")
		}
	}()

	// default set online
	if len(task.Status) == 0 {
		task.Status = string(models.SLOT_STATUS_ONLINE)
	}

	// 用于初始化时分配slots到指定group
	err := models.SetSlotRange(safeZkConn, globalEnv.ProductName(), task.FromSlot, task.ToSlot, task.NewGroupId, models.SlotStatus(task.Status))
	if err != nil {
		log.ErrorErrorf(err, "set slot range [%d,%d] failed", task.FromSlot, task.ToSlot)
		return 500, err.Error()
	}

	return jsonRetSucc()
}

// actions
// 删除zk上的 aciton 和 ActionResponse 下的部分节点
func apiActionGC(r *http.Request) (int, string) {
	r.ParseForm()
	keep, _ := strconv.Atoi(r.FormValue("keep"))
	secs, _ := strconv.Atoi(r.FormValue("secs"))
	lock := utils.GetZkLock(safeZkConn, globalEnv.ProductName())
	if err := lock.LockWithTimeout(0, fmt.Sprintf("action gc")); err != nil {
		return 500, err.Error()
	}
	defer func() {
		err := lock.Unlock()
		if err != nil && err != zk.ErrNoNode {
			log.ErrorErrorf(err, "unlock node failed")
		}
	}()

	var err error
	// 按照保留个数删除，以及按照超过多长时间后删除两种方式
	if keep >= 0 {
		err = models.ActionGC(safeZkConn, globalEnv.ProductName(), models.GC_TYPE_N, keep)
	} else if secs > 0 {
		err = models.ActionGC(safeZkConn, globalEnv.ProductName(), models.GC_TYPE_SEC, secs)
	}
	if err != nil {
		return 500, err.Error()
	}
	return jsonRetSucc()
}

// 强制删除Lock节点
func apiForceRemoveLocks() (int, string) {
	err := models.ForceRemoveLock(safeZkConn, globalEnv.ProductName())
	if err != nil {
		log.ErrorErrorf(err, "force remove lock failed")
		return 500, err.Error()
	}
	return jsonRetSucc()
}

// 删除fence节点下处于异常状态的节点信息
func apiRemoveFence() (int, string) {
	err := models.ForceRemoveDeadFence(safeZkConn, globalEnv.ProductName())
	if err != nil {
		log.ErrorErrorf(err, "force remove fence failed")
		return 500, err.Error()
	}
	return jsonRetSucc()

}

// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"time"

	"github.com/wandoulabs/go-zookeeper/zk"
	"github.com/wandoulabs/zkhelper"

	"github.com/CodisLabs/codis/pkg/utils/log"
)

const (
	MAX_LOCK_TIMEOUT = 10 * time.Second
)

// 迁移状态
const (
	MIGRATE_TASK_PENDING   string = "pending"
	MIGRATE_TASK_MIGRATING string = "migrating"
	MIGRATE_TASK_FINISHED  string = "finished"
	MIGRATE_TASK_ERR       string = "error"
)

// check if migrate task is valid
type MigrateTaskCheckFunc func(t *MigrateTask) (bool, error)

// migrate task will store on zk
// 迁移任务管理，每一次操作信息都存储在zk上
type MigrateManager struct {
	runningTask *MigrateTask
	zkConn      zkhelper.Conn
	productName string
}

// 在zk中的存储路径
func getMigrateTasksPath(product string) string {
	return fmt.Sprintf("/zk/codis/db_%s/migrate_tasks", product)
}

// 启动迁移管理协程，创建用于管理迁移信息的zk路径
func NewMigrateManager(zkConn zkhelper.Conn, pn string) *MigrateManager {
	m := &MigrateManager{
		zkConn:      zkConn,
		productName: pn,
	}
	zkhelper.CreateRecursive(m.zkConn, getMigrateTasksPath(m.productName), "", 0, zkhelper.DefaultDirACLs())
	m.mayRecover()
	// 开始循环检查此路径下的任务，并执行迁移任务，通过向redis发送命令，迁移slot
	go m.loop()
	return m
}

// if there are tasks that is not pending, process them.
func (m *MigrateManager) mayRecover() error {
	// It may be not need to do anything now.
	return nil
}

//add a new task to zk
// 在zk的 migrate_task 节点下创建顺序节点建立迁移任务
func (m *MigrateManager) PostTask(info *MigrateTaskInfo) {
	b, _ := json.Marshal(info)
	p, _ := safeZkConn.Create(getMigrateTasksPath(m.productName)+"/", b, zk.FlagSequence, zkhelper.DefaultFileACLs())
	// zk节点名即为任务id
	_, info.Id = path.Split(p)
}

// 每隔一秒钟获取一次迁移任务并执行
func (m *MigrateManager) loop() error {
	for {
		time.Sleep(time.Second)
		info := m.NextTask()
		if info == nil {
			continue
		}

		// 构造 MigrateTask 对象
		t := GetMigrateTask(*info)
		// 集群中每次只能有一个slot处于迁移状态，这里做一下检查，看迁移任务和slots下的对应slot的状态是否一致
		err := t.preMigrateCheck()
		if err != nil {
			log.ErrorErrorf(err, "pre migrate check failed")
		}
		// 执行迁移任务，每个任务迁移一个slot
		err = t.run()
		if err != nil {
			log.ErrorErrorf(err, "migrate failed")
		}
	}
}

// 获取一个任务
func (m *MigrateManager) NextTask() *MigrateTaskInfo {
	ts := m.Tasks()
	if len(ts) == 0 {
		return nil
	}
	return &ts[0]
}

// 从zk里获取所有的迁移任务并返回
func (m *MigrateManager) Tasks() []MigrateTaskInfo {
	res := Tasks{}
	// 获取 /zk/codis/productName/migrate_tasks 下的所有节点，每个节点代表一个任务
	log.Debugf("Start get migrateTasks from zk...")
	tasks, _, _ := safeZkConn.Children(getMigrateTasksPath(m.productName))
	log.Debugf("End get migrateTasks from zk")
	for _, id := range tasks {
		// 获取每一个迁移任务的详细信息
		data, _, _ := safeZkConn.Get(getMigrateTasksPath(m.productName) + "/" + id)
		info := new(MigrateTaskInfo)
		json.Unmarshal(data, info)
		info.Id = id
		res = append(res, *info)
	}
	sort.Sort(res)
	return res
}

type Tasks []MigrateTaskInfo

func (t Tasks) Len() int {
	return len(t)
}

func (t Tasks) Less(i, j int) bool {
	return t[i].Id <= t[j].Id
}

func (t Tasks) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

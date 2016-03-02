// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/wandoulabs/zkhelper"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

// /zk/codis/productName/migrate_tasks 节点下每一个迁移任务的信息，json格式
type MigrateTaskInfo struct {
	SlotId     int    `json:"slot_id"`   // id
	NewGroupId int    `json:"new_group"` // 将要迁移到的group的id
	Delay      int    `json:"delay"`     // 每迁移一个key的延迟
	CreateAt   string `json:"create_at"` // 任务创建时间
	Percent    int    `json:"percent"`
	Status     string `json:"status"` // 迁移状态
	Id         string `json:"-"`      // zk中迁移任务对应节点的名称，即为id，创建时由zk顺序生成的
}

// 迁移操作
type SlotMigrateProgress struct {
	SlotId    int `json:"slot_id"` // id
	FromGroup int `json:"from"`    // 迁出的group
	ToGroup   int `json:"to"`      // 迁入的group
	Remain    int `json:"remain"`  // 此slot含有的key的数量
}

func (p SlotMigrateProgress) String() string {
	return fmt.Sprintf("migrate Slot: slot_%d From: group_%d To: group_%d remain: %d keys", p.SlotId, p.FromGroup, p.ToGroup, p.Remain)
}

type MigrateTask struct {
	MigrateTaskInfo
	zkConn       zkhelper.Conn
	productName  string
	progressChan chan SlotMigrateProgress
}

// 返回一个封装后的 MigrateTask 对象
func GetMigrateTask(info MigrateTaskInfo) *MigrateTask {
	return &MigrateTask{
		MigrateTaskInfo: info,
		productName:     globalEnv.ProductName(),
		zkConn:          safeZkConn,
	}
}

// 更新任务状态
func (t *MigrateTask) UpdateStatus(status string) {
	t.Status = status
	b, _ := json.Marshal(t.MigrateTaskInfo)
	t.zkConn.Set(getMigrateTasksPath(t.productName)+"/"+t.Id, b, -1)
}

// 任务结束，删除对应节点信息
func (t *MigrateTask) UpdateFinish() {
	t.Status = MIGRATE_TASK_FINISHED
	t.zkConn.Delete(getMigrateTasksPath(t.productName)+"/"+t.Id, -1)
}

// 迁移单个slot
func (t *MigrateTask) migrateSingleSlot(slotId int, to int) error {
	// set slot status
	s, err := models.GetSlot(t.zkConn, t.productName, slotId)
	if err != nil {
		log.ErrorErrorf(err, "get slot info failed")
		return err
	}
	if s.State.Status == models.SLOT_STATUS_OFFLINE {
		log.Warnf("status is offline: %+v", s)
		return nil
	}

	from := s.GroupId
	if s.State.Status == models.SLOT_STATUS_MIGRATE {
		from = s.State.MigrateStatus.From
	}

	// make sure from group & target group exists
	exists, err := models.GroupExists(t.zkConn, t.productName, from)
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		log.Errorf("src group %d not exist when migrate from %d to %d", from, from, to)
		return errors.Errorf("group %d not found", from)
	}

	exists, err = models.GroupExists(t.zkConn, t.productName, to)
	if err != nil {
		return errors.Trace(err)
	}
	if !exists {
		return errors.Errorf("group %d not found", to)
	}

	// cannot migrate to itself, just ignore
	if from == to {
		log.Warnf("from == to, ignore: %+v", s)
		return nil
	}

	// modify slot status
	if err := s.SetMigrateStatus(t.zkConn, from, to); err != nil {
		log.ErrorErrorf(err, "set migrate status failed")
		return err
	}

	// 执行迁移命令
	err = t.Migrate(s, from, to, func(p SlotMigrateProgress) {
		// on migrate slot progress
		if p.Remain%5000 == 0 {
			log.Infof("%+v", p)
		}
	})
	if err != nil {
		log.ErrorErrorf(err, "migrate slot failed")
		return err
	}

	// migrate done, change slot status back
	s.State.Status = models.SLOT_STATUS_ONLINE
	s.State.MigrateStatus.From = models.INVALID_ID
	s.State.MigrateStatus.To = models.INVALID_ID
	// 更新slot状态信息
	if err := s.Update(t.zkConn); err != nil {
		log.ErrorErrorf(err, "update zk status failed, should be: %+v", s)
		return err
	}
	return nil
}

// 执行迁移任务
func (t *MigrateTask) run() error {
	log.Infof("migration start: %+v", t.MigrateTaskInfo)
	to := t.NewGroupId
	// 更新任务状态为迁移中
	t.UpdateStatus(MIGRATE_TASK_MIGRATING)
	// 迁移单个slot数据
	err := t.migrateSingleSlot(t.SlotId, to)
	if err != nil {
		// 如果迁移出错，更新任务状态信息
		log.ErrorErrorf(err, "migrate single slot failed")
		t.UpdateStatus(MIGRATE_TASK_ERR)
		// 还原slot状态为ONLINE
		t.rollbackPremigrate()
		return err
	}
	// 删除zk上的迁移任务信息
	t.UpdateFinish()
	log.Infof("migration finished: %+v", t.MigrateTaskInfo)
	return nil
}

// 如果slot的状态没有变化，还原slot状态为 ONLINE
func (t *MigrateTask) rollbackPremigrate() {
	if s, err := models.GetSlot(t.zkConn, t.productName, t.SlotId); err == nil && s.State.Status == models.SLOT_STATUS_PRE_MIGRATE {
		s.State.Status = models.SLOT_STATUS_ONLINE
		err = s.Update(t.zkConn)
		if err != nil {
			log.Warn("rollback premigrate failed", err)
		} else {
			log.Infof("rollback slot %d from premigrate to online\n", s.Id)
		}
	}
}

var ErrGroupMasterNotFound = errors.New("group master not found")

// will block until all keys are migrated
func (task *MigrateTask) Migrate(slot *models.Slot, fromGroup, toGroup int, onProgress func(SlotMigrateProgress)) (err error) {
	// 获取group信息
	groupFrom, err := models.GetGroup(task.zkConn, task.productName, fromGroup)
	if err != nil {
		return err
	}
	groupTo, err := models.GetGroup(task.zkConn, task.productName, toGroup)
	if err != nil {
		return err
	}

	// from master redis-server
	fromMaster, err := groupFrom.Master(task.zkConn)
	if err != nil {
		return err
	}

	// to master redis-server
	toMaster, err := groupTo.Master(task.zkConn)
	if err != nil {
		return err
	}

	if fromMaster == nil || toMaster == nil {
		return errors.Trace(ErrGroupMasterNotFound)
	}

	// 建立redis连接
	c, err := utils.DialTo(fromMaster.Addr, globalEnv.Password())
	if err != nil {
		return err
	}

	defer c.Close()

	// 通过向redis发送 SLOTSMGRTTAGSLOT 命令，执行迁移操作
	_, remain, err := utils.SlotsMgrtTagSlot(c, slot.Id, toMaster.Addr)
	if err != nil {
		return err
	}

	for remain > 0 {
		// 每迁移完一个key，休眠一段时间
		if task.Delay > 0 {
			time.Sleep(time.Duration(task.Delay) * time.Millisecond)
		}
		_, remain, err = utils.SlotsMgrtTagSlot(c, slot.Id, toMaster.Addr)
		if remain >= 0 {
			// 每5000个key打印一下日志
			onProgress(SlotMigrateProgress{
				SlotId:    slot.Id,
				FromGroup: fromGroup,
				ToGroup:   toGroup,
				Remain:    remain,
			})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// 迁移前的检查
func (t *MigrateTask) preMigrateCheck() error {
	// 获取状态处于 SLOT_STATUS_MIGRATE 和 SLOT_STATUS_PRE_MIGRATE 的节点信息
	slots, err := models.GetMigratingSlots(safeZkConn, t.productName)
	if err != nil {
		return errors.Trace(err)
	}
	// check if there is migrating slot
	// codis集群中每一次只能有一个slot处于迁移状态
	if len(slots) > 1 {
		return errors.Errorf("more than one slots are migrating, unknown error")
	}
	if len(slots) == 1 {
		slot := slots[0]
		// 检查两者的slot是否一致，要迁移到的groupId是否一致
		if t.NewGroupId != slot.State.MigrateStatus.To || t.SlotId != slot.Id {
			return errors.Errorf("there is a migrating slot %+v, finish it first", slot)
		}
	}
	return nil
}

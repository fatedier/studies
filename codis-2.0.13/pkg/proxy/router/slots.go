// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"fmt"
	"sync"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

// slot相关的连接信息
type Slot struct {
	id int

    // slot所在redis-server的连接
	backend struct {
		addr string
		host []byte
		port []byte
		bc   *SharedBackendConn
	}
    // slot迁移时，原redis-server的连接
	migrate struct {
		from string
		bc   *SharedBackendConn
	}

	wait sync.WaitGroup
	lock struct {
		hold bool
		sync.RWMutex
	}
}

func (s *Slot) blockAndWait() {
	if !s.lock.hold {
		s.lock.hold = true
		s.lock.Lock()
	}
	s.wait.Wait()
}

func (s *Slot) unblock() {
	if !s.lock.hold {
		return
	}
	s.lock.hold = false
	s.lock.Unlock()
}

func (s *Slot) reset() {
	s.backend.addr = ""
	s.backend.host = nil
	s.backend.port = nil
	s.backend.bc = nil
	s.migrate.from = ""
	s.migrate.bc = nil
}

// 对redis-client的请求进行转发
func (s *Slot) forward(r *Request, key []byte) error {
	s.lock.RLock()
    // 执行redis命令前的准备工作，检查和后端redis连接是否存在，检查slot是否处于迁移状态中，如果是，强制迁移指定key到新的redis-server
	bc, err := s.prepare(r, key)
	s.lock.RUnlock()
	if err != nil {
		return err
	} else {
        // 转发redis命令
		bc.PushBack(r)
		return nil
	}
}

var ErrSlotIsNotReady = errors.New("slot is not ready, may be offline")

// 执行redis命令前的准备工作，检查和后端redis连接是否存在，检查slot是否处于迁移状态中，如果是，强制迁移指定key到新的redis-server
func (s *Slot) prepare(r *Request, key []byte) (*SharedBackendConn, error) {
	if s.backend.bc == nil {
		log.Infof("slot-%04d is not ready: key = %s", s.id, key)
		return nil, ErrSlotIsNotReady
	}
	if err := s.slotsmgrt(r, key); err != nil {
		log.Warnf("slot-%04d migrate from = %s to %s failed: key = %s, error = %s",
			s.id, s.migrate.from, s.backend.addr, key, err)
		return nil, err
	} else {
        // 操作可能涉及多个slot，需要等待所有slot完成操作
		r.slot = &s.wait
		r.slot.Add(1)
		return s.backend.bc, nil
	}
}

// 执行命令前需要先检查当前slot是否处于迁移中
// 如果key所属slot正在迁移中，需要发送SLOTSMGRTTAGONE命令到原来的redis-server，迁移一个指定的key，是原子操作
// 等待迁移成功后再去新的redis-server中执行此命令
func (s *Slot) slotsmgrt(r *Request, key []byte) error {
	if len(key) == 0 || s.migrate.bc == nil {
		return nil
	}
    // 向原来的redis-server发送 SLOTSMGRTTAGONE 命令
	m := &Request{
		Resp: redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("SLOTSMGRTTAGONE")),
			redis.NewBulkBytes(s.backend.host),
			redis.NewBulkBytes(s.backend.port),
			redis.NewBulkBytes([]byte("3000")),
			redis.NewBulkBytes(key),
		}),
		Wait: &sync.WaitGroup{},
	}
	s.migrate.bc.PushBack(m)

    // 等待请求完成
	m.Wait.Wait()

	resp, err := m.Response.Resp, m.Response.Err
	if err != nil {
		return err
	}
	if resp == nil {
		return ErrRespIsRequired
	}
	if resp.IsError() {
		return errors.New(fmt.Sprintf("error resp: %s", resp.Value))
	}
	if resp.IsInt() {
		log.Debugf("slot-%04d migrate from %s to %s: key = %s, resp = %s",
			s.id, s.migrate.from, s.backend.addr, key, resp.Value)
		return nil
	} else {
		return errors.New(fmt.Sprintf("error resp: should be integer, but got %s", resp.Type))
	}
}

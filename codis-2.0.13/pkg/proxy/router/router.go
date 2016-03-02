// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"strings"
	"sync"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

// slot数量最大值
const MaxSlotNum = models.DEFAULT_SLOT_NUM

type Router struct {
	mu sync.Mutex

	auth string                        // 访问redis密码
	pool map[string]*SharedBackendConn // 访问redis的共享连接池

	slots [MaxSlotNum]*Slot // slot信息

	closed bool // 结束标志
}

func New() *Router {
	return NewWithAuth("")
}

// 创建一个访问redis的路由
func NewWithAuth(auth string) *Router {
	s := &Router{
		auth: auth,
		pool: make(map[string]*SharedBackendConn),
	}
	for i := 0; i < len(s.slots); i++ {
		s.slots[i] = &Slot{id: i}
	}
	return s
}

func (s *Router) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	for i := 0; i < len(s.slots); i++ {
		s.resetSlot(i)
	}
	s.closed = true
	return nil
}

var errClosedRouter = errors.New("use of closed router")

func (s *Router) ResetSlot(i int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosedRouter
	}
	s.resetSlot(i)
	return nil
}

// 填充指定slot的信息，建立与所在redis-server的连接
func (s *Router) FillSlot(i int, addr, from string, lock bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosedRouter
	}
	s.fillSlot(i, addr, from, lock)
	return nil
}

// 对后端所有redis连接发送心跳包
func (s *Router) KeepAlive() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosedRouter
	}
	for _, bc := range s.pool {
		bc.KeepAlive()
	}
	return nil
}

// 基于路由规则，将指定的redis-client发过来的请求，转发给这个key所在slot对应的redis-server的连接
func (s *Router) Dispatch(r *Request) error {
	hkey := getHashKey(r.Resp, r.OpStr)
	slot := s.slots[hashSlot(hkey)]
	return slot.forward(r, hkey)
}

// 从连接池中获取地址为 addr 的连接，引用计数加1，没有就新建一个并加入连接池中
func (s *Router) getBackendConn(addr string) *SharedBackendConn {
	bc := s.pool[addr]
	if bc != nil {
		bc.IncrRefcnt()
	} else {
		bc = NewSharedBackendConn(addr, s.auth)
		s.pool[addr] = bc
	}
	return bc
}

// 将连接放回连接池
func (s *Router) putBackendConn(bc *SharedBackendConn) {
	if bc != nil && bc.Close() {
		delete(s.pool, bc.Addr())
	}
}

func (s *Router) isValidSlot(i int) bool {
	return i >= 0 && i < len(s.slots)
}

// 清空slot数据，包括连接
func (s *Router) resetSlot(i int) {
	if !s.isValidSlot(i) {
		return
	}
	slot := s.slots[i]
	slot.blockAndWait()

	s.putBackendConn(slot.backend.bc)
	s.putBackendConn(slot.migrate.bc)
	slot.reset()

	slot.unblock()
}

// 填充指定slot的信息，建立与所在redis-server的连接
func (s *Router) fillSlot(i int, addr, from string, lock bool) {
	if !s.isValidSlot(i) {
		return
	}
	slot := s.slots[i]
	slot.blockAndWait()

	// 将原来的连接放回连接池
	s.putBackendConn(slot.backend.bc)
	s.putBackendConn(slot.migrate.bc)
	// 清空原先的数据
	slot.reset()

	// 创建与此slot所在redis-server的连接，可以从连接池中获取
	if len(addr) != 0 {
		xx := strings.Split(addr, ":")
		if len(xx) >= 1 {
			slot.backend.host = []byte(xx[0])
		}
		if len(xx) >= 2 {
			slot.backend.port = []byte(xx[1])
		}
		slot.backend.addr = addr
		slot.backend.bc = s.getBackendConn(addr)
	}
	// 如果正处于迁移中的状态，需要记录下与原来所在redis-server的连接
	if len(from) != 0 {
		slot.migrate.from = from
		slot.migrate.bc = s.getBackendConn(from)
	}

	if !lock {
		slot.unblock()
	}

	if slot.migrate.bc != nil {
		log.Infof("fill slot %04d, backend.addr = %s, migrate.from = %s",
			i, slot.backend.addr, slot.migrate.from)
	} else {
		log.Infof("fill slot %04d, backend.addr = %s",
			i, slot.backend.addr)
	}
}

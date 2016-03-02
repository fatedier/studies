// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"encoding/json"
	"sync"

	"github.com/CodisLabs/codis/pkg/utils/atomic2"
)

// 操作统计信息
type OpStats struct {
	opstr string        // 操作命令
	calls atomic2.Int64 // 请求次数
	usecs atomic2.Int64 // 总耗时
}

func (s *OpStats) OpStr() string {
	return s.opstr
}

func (s *OpStats) Calls() int64 {
	return s.calls.Get()
}

func (s *OpStats) USecs() int64 {
	return s.usecs.Get()
}

func (s *OpStats) MarshalJSON() ([]byte, error) {
	var m = make(map[string]interface{})
	var calls = s.calls.Get()
	var usecs = s.usecs.Get()

	var perusecs int64 = 0
	if calls != 0 {
		perusecs = usecs / calls
	}

	m["cmd"] = s.opstr
	m["calls"] = calls
	m["usecs"] = usecs
	m["usecs_percall"] = perusecs
	return json.Marshal(m)
}

// 命令执行统计信息
var cmdstats struct {
	requests atomic2.Int64

	opmap map[string]*OpStats
	rwlck sync.RWMutex
}

func init() {
	cmdstats.opmap = make(map[string]*OpStats)
}

// 获取总的请求次数
func OpCounts() int64 {
	return cmdstats.requests.Get()
}

// 获取指定命令的统计信息
func GetOpStats(opstr string, create bool) *OpStats {
	cmdstats.rwlck.RLock()
	s := cmdstats.opmap[opstr]
	cmdstats.rwlck.RUnlock()

	if s != nil || !create {
		return s
	}

	cmdstats.rwlck.Lock()
	s = cmdstats.opmap[opstr]
	if s == nil {
		s = &OpStats{opstr: opstr}
		cmdstats.opmap[opstr] = s
	}
	cmdstats.rwlck.Unlock()
	return s
}

// 获取全部命令的统计信息
func GetAllOpStats() []*OpStats {
	var all = make([]*OpStats, 0, 128)
	cmdstats.rwlck.RLock()
	for _, s := range cmdstats.opmap {
		all = append(all, s)
	}
	cmdstats.rwlck.RUnlock()
	return all
}

// 更新指定命令的统计信息
func incrOpStats(opstr string, usecs int64) {
	s := GetOpStats(opstr, true)
	// 调用次数
	s.calls.Incr()
	s.usecs.Add(usecs)
	cmdstats.requests.Incr()
}

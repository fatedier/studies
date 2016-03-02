// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"sync"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/atomic2"
)

// 封装的redis的请求
type Dispatcher interface {
	Dispatch(r *Request) error
}

type Request struct {
	OpStr string            // redis命令
	Start int64             // 请求时间

	Resp *redis.Resp        // client请求对象

	Coalesce func() error   // 聚合返回数据的函数
	Response struct {       // redis返回对象
		Resp *redis.Resp
		Err  error
	}

	Wait *sync.WaitGroup    // 请求是否完成
	slot *sync.WaitGroup    // 命令可能涉及到多个slot，等待所有slot完成操作

	Failed *atomic2.Bool    // 请求是否失败
}

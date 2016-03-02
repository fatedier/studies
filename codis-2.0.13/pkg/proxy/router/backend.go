// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"fmt"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

// 和后端redis的连接
type BackendConn struct {
	addr string             // 连接redis的地址
	auth string             // 连接redis的密码
	stop sync.Once

	input chan *Request     // 用于接收redis请求的通道
}

// 建立和后端redis-server的连接，等待请求
func NewBackendConn(addr, auth string) *BackendConn {
	bc := &BackendConn{
		addr: addr, auth: auth,
		input: make(chan *Request, 1024),
	}
	go bc.Run()
	return bc
}

// 循环等待对后端的redis请求，执行后返回
func (bc *BackendConn) Run() {
	log.Infof("backend conn [%p] to %s, start service", bc, bc.addr)
	for k := 0; ; k++ {
        // 循环等待新的 redis 请求，发往后端 redis-server，并异步地等待redis返回内容后填充 request 的resp字段
		err := bc.loopWriter()
		if err == nil {
			break
		} else {
            // 由于后端redis的连接出现错误，对等待中的剩余的请求全部返回错误信息
			for i := len(bc.input); i != 0; i-- {
				r := <-bc.input
				bc.setResponse(r, nil, err)
			}
		}
        // 休眠 50ms 后重连
		log.WarnErrorf(err, "backend conn [%p] to %s, restart [%d]", bc, bc.addr, k)
		time.Sleep(time.Millisecond * 50)
	}
	log.Infof("backend conn [%p] to %s, stop and exit", bc, bc.addr)
}

func (bc *BackendConn) Addr() string {
	return bc.addr
}

// 关闭连接
func (bc *BackendConn) Close() {
	bc.stop.Do(func() {
		close(bc.input)
	})
}

// 将redis请求加入等待队列
func (bc *BackendConn) PushBack(r *Request) {
	if r.Wait != nil {
		r.Wait.Add(1)
	}
	bc.input <- r
}

// 向redis发送心跳包
func (bc *BackendConn) KeepAlive() bool {
    // 如果当前有redis请求，则没必要发心跳包
	if len(bc.input) != 0 {
		return false
	}
	r := &Request{
		Resp: redis.NewArray([]*redis.Resp{
			redis.NewBulkBytes([]byte("PING")),
		}),
	}

	select {
	case bc.input <- r:
		return true
	default:
		return false
	}
}

var ErrFailedRequest = errors.New("discard failed request")

// 循环等待新的 redis 请求，发往后端 redis-server，并异步地等待redis返回内容后填充 request 的resp字段
func (bc *BackendConn) loopWriter() error {
    // 如果连接close，ok会返回false
	r, ok := <-bc.input
	if ok {
        // 创建一个循环处理从redis返回内容的协程，向request中设置返回的信息
		c, tasks, err := bc.newBackendReader()
		if err != nil {
			return bc.setResponse(r, nil, err)
		}
		defer close(tasks)

        // 设置缓存刷新策略
		p := &FlushPolicy{
			Encoder:     c.Writer,
			MaxBuffered: 64,
			MaxInterval: 300,
		}
		for ok {
            // 如果后续没有等待中的请求了，强制刷新缓冲区
			var flush = len(bc.input) == 0
			if bc.canForward(r) {
				if err := p.Encode(r.Resp, flush); err != nil {
					return bc.setResponse(r, nil, err)
				}
                // 发到tasks通道的请求，会有另一个协程循环读取redis的返回，解析后放入request中
				tasks <- r
			} else {
				if err := p.Flush(flush); err != nil {
					return bc.setResponse(r, nil, err)
				}
				bc.setResponse(r, nil, ErrFailedRequest)
			}

			r, ok = <-bc.input
		}
	}
	return nil
}

// 创建一个循环处理从redis返回内容的协程，向request中设置返回的信息
func (bc *BackendConn) newBackendReader() (*redis.Conn, chan<- *Request, error) {
    // 建立和redis的连接
	c, err := redis.DialTimeout(bc.addr, 1024*512, time.Second)
	if err != nil {
		return nil, nil, err
	}
    // redis超时时间
	c.ReaderTimeout = time.Minute
	c.WriterTimeout = time.Minute

	if err := bc.verifyAuth(c); err != nil {
		c.Close()
		return nil, nil, err
	}

	tasks := make(chan *Request, 4096)
	go func() {
		defer c.Close()
		for r := range tasks {
            // 向redis发送命令
			resp, err := c.Reader.Decode()
            // 设置redis返回的状态和信息，因为redis是单线程的，命令都是顺序执行，所以这里的 request 和 response 可以一一对应上
			bc.setResponse(r, resp, err)
			if err != nil {
				// close tcp to tell writer we are failed and should quit
				c.Close()
			}
		}
	}()
	return c, tasks, nil
}

// 验证redis密码
func (bc *BackendConn) verifyAuth(c *redis.Conn) error {
	if bc.auth == "" {
		return nil
	}
	resp := redis.NewArray([]*redis.Resp{
		redis.NewBulkBytes([]byte("AUTH")),
		redis.NewBulkBytes([]byte(bc.auth)),
	})

	if err := c.Writer.Encode(resp, true); err != nil {
		return err
	}

	resp, err := c.Reader.Decode()
	if err != nil {
		return err
	}
	if resp == nil {
		return errors.New(fmt.Sprintf("error resp: nil response"))
	}
	if resp.IsError() {
		return errors.New(fmt.Sprintf("error resp: %s", resp.Value))
	}
	if resp.IsString() {
		return nil
	} else {
		return errors.New(fmt.Sprintf("error resp: should be string, but got %s", resp.Type))
	}
}

func (bc *BackendConn) canForward(r *Request) bool {
	if r.Failed != nil && r.Failed.Get() {
		return false
	} else {
		return true
	}
}

// 设置请求返回状态和信息
func (bc *BackendConn) setResponse(r *Request, resp *redis.Resp, err error) error {
	r.Response.Resp, r.Response.Err = resp, err
	if err != nil && r.Failed != nil {
		r.Failed.Set(true)
	}
	if r.Wait != nil {
		r.Wait.Done()
	}
	if r.slot != nil {
		r.slot.Done()
	}
	return err
}

// 连接复用对象
type SharedBackendConn struct {
	*BackendConn
	mu sync.Mutex

	refcnt int
}

// 创建连接复用对象
func NewSharedBackendConn(addr, auth string) *SharedBackendConn {
	return &SharedBackendConn{BackendConn: NewBackendConn(addr, auth), refcnt: 1}
}

func (s *SharedBackendConn) Close() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed, close too many times")
	}
	if s.refcnt == 1 {
		s.BackendConn.Close()
	}
	s.refcnt--
	return s.refcnt == 0
}

func (s *SharedBackendConn) IncrRefcnt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refcnt == 0 {
		log.Panicf("shared backend conn has been closed")
	}
	s.refcnt++
}

// 刷新策略
type FlushPolicy struct {
	*redis.Encoder      // 相当于 writer

	MaxBuffered int
	MaxInterval int64

	nbuffered int
	lastflush int64
}

// 返回是否需要刷新缓存
func (p *FlushPolicy) needFlush() bool {
    // 超过最大缓存区大小，或者超过指定时间间隔，需要再次刷新
	if p.nbuffered != 0 {
		if p.nbuffered > p.MaxBuffered {
			return true
		}
		if microseconds()-p.lastflush > p.MaxInterval {
			return true
		}
	}
	return false
}

// 刷新缓冲区，发送到redis
func (p *FlushPolicy) Flush(force bool) error {
	if force || p.needFlush() {
		if err := p.Encoder.Flush(); err != nil {
			return err
		}
		p.nbuffered = 0
		p.lastflush = microseconds()
	}
	return nil
}

// 往redis发送命令，并且解析返回内容
func (p *FlushPolicy) Encode(resp *redis.Resp, force bool) error {
	if err := p.Encoder.Encode(resp, false); err != nil {
		return err
	} else {
		p.nbuffered++
		return p.Flush(force)
	}
}

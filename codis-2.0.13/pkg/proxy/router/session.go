// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package router

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/atomic2"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

// 与redis客户端会话
type Session struct {
	*redis.Conn // 和redis客户端之间的连接

	Ops int64

	LastOpUnix int64
	CreateUnix int64

	auth       string
	authorized bool

	quit   bool // 退出标志
	failed atomic2.Bool
}

// 返回string格式session信息
func (s *Session) String() string {
	o := &struct {
		Ops        int64  `json:"ops"`    // 此会话的ops
		LastOpUnix int64  `json:"lastop"` // 最近一次操作时间戳
		CreateUnix int64  `json:"create"` // 会话创建时间戳
		RemoteAddr string `json:"remote"` // redis客户端的ip地址
	}{
		s.Ops, s.LastOpUnix, s.CreateUnix,
		s.Conn.Sock.RemoteAddr().String(),
	}
	b, _ := json.Marshal(o)
	return string(b)
}

func NewSession(c net.Conn, auth string) *Session {
	return NewSessionSize(c, auth, 1024*32, 1800)
}

// 返回一个redis-client的连接对象
func NewSessionSize(c net.Conn, auth string, bufsize int, timeout int) *Session {
	s := &Session{CreateUnix: time.Now().Unix(), auth: auth}
	s.Conn = redis.NewConnSize(c, bufsize)
	s.Conn.ReaderTimeout = time.Second * time.Duration(timeout)
	s.Conn.WriterTimeout = time.Second * 30
	log.Infof("session [%p] create: %s", s, s)
	return s
}

func (s *Session) Close() error {
	return s.Conn.Close()
}

// 针对一个redis-client连接的处理函数
func (s *Session) Serve(d Dispatcher, maxPipeline int) {
	var errlist errors.ErrorList
	defer func() {
		// 非正常结束
		if err := errlist.First(); err != nil {
			log.Infof("session [%p] closed: %s, error = %s", s, s, err)
		} else {
			// 连接正常结束
			log.Infof("session [%p] closed: %s, quit", s, s)
		}
		s.Close()
	}()

	// 利用通道的缓冲区实现对pipeline上限的限制
	tasks := make(chan *Request, maxPipeline)
	go func() {
		defer func() {
			for _ = range tasks {
			}
		}()
		// 请求处理结束后返回给 redis-client 的协程
		if err := s.loopWriter(tasks); err != nil {
			errlist.PushBack(err)
		}
		s.Close()
	}()

	defer close(tasks)
	// 循环从 redis-client 读取请求命令，转发给后端 redis-server，获取返回后通过 tasks 通道返回给client
	if err := s.loopReader(tasks, d); err != nil {
		errlist.PushBack(err)
	}
}

// 循环从 redis-client 读取请求命令，转发给后端 redis-server，获取返回后通过 tasks 通道返回给client
func (s *Session) loopReader(tasks chan<- *Request, d Dispatcher) error {
	if d == nil {
		return errors.New("nil dispatcher")
	}
	for !s.quit {
		// 从redis-client读取请求，并解析成 Resp 格式的对象
		resp, err := s.Reader.Decode()
		if err != nil {
			return err
		}
		// 处理一条redis-client的请求
		r, err := s.handleRequest(resp, d)
		if err != nil {
			return err
		} else {
			// 将请求处理结果通过task通道返回
			tasks <- r
		}
	}
	return nil
}

// 请求处理结束后返回给 redis-client 的协程
func (s *Session) loopWriter(tasks <-chan *Request) error {
	p := &FlushPolicy{
		Encoder:     s.Writer,
		MaxBuffered: 32,
		MaxInterval: 300,
	}
	for r := range tasks {
		// 处理redis-server执行完命令后返回的结果
		resp, err := s.handleResponse(r)
		if err != nil {
			return err
		}
		// 发送给 redis-client
		if err := p.Encode(resp, len(tasks) == 0); err != nil {
			return err
		}
	}
	return nil
}

var ErrRespIsRequired = errors.New("resp is required")

// 处理redis-server执行完命令后返回的结果
func (s *Session) handleResponse(r *Request) (*redis.Resp, error) {
	r.Wait.Wait()
	// 如果有聚合函数，对结果进行聚合后返回
	if r.Coalesce != nil {
		if err := r.Coalesce(); err != nil {
			return nil, err
		}
	}
	resp, err := r.Response.Resp, r.Response.Err
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, ErrRespIsRequired
	}
	// 更新统计信息
	incrOpStats(r.OpStr, microseconds()-r.Start)
	return resp, nil
}

// 处理一条redis-client的请求
func (s *Session) handleRequest(resp *redis.Resp, d Dispatcher) (*Request, error) {
	// 获取操作命令字符串
	opstr, err := getOpStr(resp)
	if err != nil {
		return nil, err
	}
	// 检查redis命令是否不支持
	if isNotAllowed(opstr) {
		return nil, errors.New(fmt.Sprintf("command <%s> is not allowed", opstr))
	}

	usnow := microseconds()
	s.LastOpUnix = usnow / 1e6
	s.Ops++

	// 构造request对象
	r := &Request{
		OpStr:  opstr,
		Start:  usnow,
		Resp:   resp,
		Wait:   &sync.WaitGroup{},
		Failed: &s.failed,
	}

	// 特殊命令的处理
	// 退出命令，这里截获请求，返回ok，断开连接
	if opstr == "QUIT" {
		return s.handleQuit(r)
	}
	if opstr == "AUTH" {
		return s.handleAuth(r)
	}

	if !s.authorized {
		if s.auth != "" {
			r.Response.Resp = redis.NewError([]byte("NOAUTH Authentication required."))
			return r, nil
		}
		s.authorized = true
	}

	switch opstr {
	case "SELECT":
		return s.handleSelect(r)
	case "PING":
		return s.handlePing(r)
	case "MGET":
		return s.handleRequestMGet(r, d)
	case "MSET":
		return s.handleRequestMSet(r, d)
	case "DEL":
		return s.handleRequestMDel(r, d)
	}
	// 基于路由规则，将指定的redis-client发过来的请求，转发给这个key所在slot对应的redis-server的连接
	return r, d.Dispatch(r)
}

// 退出命令，这里截获请求，返回ok，断开连接
func (s *Session) handleQuit(r *Request) (*Request, error) {
	s.quit = true
	r.Response.Resp = redis.NewString([]byte("OK"))
	return r, nil
}

// auth命令
func (s *Session) handleAuth(r *Request) (*Request, error) {
	if len(r.Resp.Array) != 2 {
		r.Response.Resp = redis.NewError([]byte("ERR wrong number of arguments for 'AUTH' command"))
		return r, nil
	}
	if s.auth == "" {
		r.Response.Resp = redis.NewError([]byte("ERR Client sent AUTH, but no password is set"))
		return r, nil
	}
	if s.auth != string(r.Resp.Array[1].Value) {
		s.authorized = false
		r.Response.Resp = redis.NewError([]byte("ERR invalid password"))
		return r, nil
	} else {
		s.authorized = true
		r.Response.Resp = redis.NewString([]byte("OK"))
		return r, nil
	}
}

// 检查select命令
func (s *Session) handleSelect(r *Request) (*Request, error) {
	// 参数数量不正确
	if len(r.Resp.Array) != 2 {
		r.Response.Resp = redis.NewError([]byte("ERR wrong number of arguments for 'SELECT' command"))
		return r, nil
	}
	// 不支持选择多少号库
	if db, err := strconv.Atoi(string(r.Resp.Array[1].Value)); err != nil {
		r.Response.Resp = redis.NewError([]byte("ERR invalid DB index"))
		return r, nil
	} else if db != 0 {
		r.Response.Resp = redis.NewError([]byte("ERR invalid DB index, only accept DB 0"))
		return r, nil
	} else {
		r.Response.Resp = redis.NewString([]byte("OK"))
		return r, nil
	}
}

// 检查 ping 命令，不转发给后端redis了
func (s *Session) handlePing(r *Request) (*Request, error) {
	if len(r.Resp.Array) != 1 {
		r.Response.Resp = redis.NewError([]byte("ERR wrong number of arguments for 'PING' command"))
		return r, nil
	}
	r.Response.Resp = redis.NewString([]byte("PONG"))
	return r, nil
}

// mget命令会被拆分成一个key一个任务，最后对返回结果进行聚合
func (s *Session) handleRequestMGet(r *Request, d Dispatcher) (*Request, error) {
	nkeys := len(r.Resp.Array) - 1
	if nkeys <= 1 {
		return r, d.Dispatch(r)
	}
	var sub = make([]*Request, nkeys)
	for i := 0; i < len(sub); i++ {
		sub[i] = &Request{
			OpStr: r.OpStr,
			Start: r.Start,
			Resp: redis.NewArray([]*redis.Resp{
				r.Resp.Array[0],
				r.Resp.Array[i+1],
			}),
			Wait:   r.Wait,
			Failed: r.Failed,
		}
		if err := d.Dispatch(sub[i]); err != nil {
			return nil, err
		}
	}
	// 对返回结果进行聚合的函数
	r.Coalesce = func() error {
		var array = make([]*redis.Resp, len(sub))
		for i, x := range sub {
			if err := x.Response.Err; err != nil {
				return err
			}
			resp := x.Response.Resp
			if resp == nil {
				return ErrRespIsRequired
			}
			if !resp.IsArray() || len(resp.Array) != 1 {
				return errors.New(fmt.Sprintf("bad mget resp: %s array.len = %d", resp.Type, len(resp.Array)))
			}
			array[i] = resp.Array[0]
		}
		r.Response.Resp = redis.NewArray(array)
		return nil
	}
	return r, nil
}

// 同 Mget
func (s *Session) handleRequestMSet(r *Request, d Dispatcher) (*Request, error) {
	nblks := len(r.Resp.Array) - 1
	if nblks <= 2 {
		return r, d.Dispatch(r)
	}
	if nblks%2 != 0 {
		r.Response.Resp = redis.NewError([]byte("ERR wrong number of arguments for 'MSET' command"))
		return r, nil
	}
	var sub = make([]*Request, nblks/2)
	for i := 0; i < len(sub); i++ {
		sub[i] = &Request{
			OpStr: r.OpStr,
			Start: r.Start,
			Resp: redis.NewArray([]*redis.Resp{
				r.Resp.Array[0],
				r.Resp.Array[i*2+1],
				r.Resp.Array[i*2+2],
			}),
			Wait:   r.Wait,
			Failed: r.Failed,
		}
		if err := d.Dispatch(sub[i]); err != nil {
			return nil, err
		}
	}
	r.Coalesce = func() error {
		for _, x := range sub {
			if err := x.Response.Err; err != nil {
				return err
			}
			resp := x.Response.Resp
			if resp == nil {
				return ErrRespIsRequired
			}
			if !resp.IsString() {
				return errors.New(fmt.Sprintf("bad mset resp: %s value.len = %d", resp.Type, len(resp.Value)))
			}
			r.Response.Resp = resp
		}
		return nil
	}
	return r, nil
}

// 同 Mget，会拆分成多个单个key的任务
func (s *Session) handleRequestMDel(r *Request, d Dispatcher) (*Request, error) {
	nkeys := len(r.Resp.Array) - 1
	if nkeys <= 1 {
		return r, d.Dispatch(r)
	}
	var sub = make([]*Request, nkeys)
	for i := 0; i < len(sub); i++ {
		sub[i] = &Request{
			OpStr: r.OpStr,
			Start: r.Start,
			Resp: redis.NewArray([]*redis.Resp{
				r.Resp.Array[0],
				r.Resp.Array[i+1],
			}),
			Wait:   r.Wait,
			Failed: r.Failed,
		}
		if err := d.Dispatch(sub[i]); err != nil {
			return nil, err
		}
	}
	r.Coalesce = func() error {
		var n int
		for _, x := range sub {
			if err := x.Response.Err; err != nil {
				return err
			}
			resp := x.Response.Resp
			if resp == nil {
				return ErrRespIsRequired
			}
			if !resp.IsInt() || len(resp.Value) != 1 {
				return errors.New(fmt.Sprintf("bad mdel resp: %s value.len = %d", resp.Type, len(resp.Value)))
			}
			if resp.Value[0] != '0' {
				n++
			}
		}
		r.Response.Resp = redis.NewInt([]byte(strconv.Itoa(n)))
		return nil
	}
	return r, nil
}

// 返回精确到毫秒的时间戳
func microseconds() int64 {
	return time.Now().UnixNano() / int64(time.Microsecond)
}

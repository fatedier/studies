// Copyright 2015 Wandoujia Inc. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package utils

import (
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/wandoulabs/go-zookeeper/zk"
	"github.com/wandoulabs/zkhelper"
)

const retryMaxOnOps = 10

type ConnBuilder interface {
	// Get a conn that will retry automatically when getting error caused by connection issues.
	// If retry can not rebuild the connection, there will be a fetal error
	// 出错后会自动重试，如果重连上限次数，程序会挂掉
	GetSafeConn() zkhelper.Conn

	// Get a conn that will return error caused by connection issues
	// It will try to rebuild the connection after return error.
	GetUnsafeConn() zkhelper.Conn
}

// zk连接相关
type connBuilder struct {
	connection         zkhelper.Conn                 // 和zk之间的连接
	builder            func() (zkhelper.Conn, error) // 建立zk连接的函数
	createdOn          time.Time                     // 最近一次zk连接建立时间
	lock               sync.RWMutex
	safeConnInstance   *safeConn   // 安全连接，出错后会自动重试，如果超过重连上限次数，程序会挂掉
	unsafeConnInstance *unsafeConn // 非安全连接
}

// 初始化zk连接相关
func NewConnBuilder(buildFunc func() (zkhelper.Conn, error)) ConnBuilder {
	b := &connBuilder{
		builder: buildFunc,
	}
	b.safeConnInstance = &safeConn{builder: b}
	b.unsafeConnInstance = &unsafeConn{builder: b}
	b.resetConnection()
	return b
}

// 重新建立连接
func (b *connBuilder) resetConnection() {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.builder == nil {
		log.Panicf("no connection builder")
	}
	// 避免短时间内重复重建连接，间隔时间为1s
	if time.Now().Before(b.createdOn.Add(time.Second)) {
		return
	}
	if b.connection != nil {
		b.connection.Close()
	}
	var err error
	b.connection, err = b.builder() // this is asnyc
	if err == nil {
		b.safeConnInstance.Conn = b.connection
		b.unsafeConnInstance.Conn = b.connection
		b.createdOn = time.Now()
		return
	}
	log.Panicf("can not build new zk session, exit")
}

func (b *connBuilder) GetSafeConn() zkhelper.Conn {
	return b.safeConnInstance
}

func (b *connBuilder) GetUnsafeConn() zkhelper.Conn {
	return b.unsafeConnInstance
}

// zk连接
type conn struct {
	zkhelper.Conn
	builder *connBuilder
}

type safeConn conn
type unsafeConn conn

// 是否是连接出错
func isConnectionError(e error) bool {
	return !zkhelper.ZkErrorEqual(zk.ErrNoNode, e) && !zkhelper.ZkErrorEqual(zk.ErrNodeExists, e) &&
		!zkhelper.ZkErrorEqual(zk.ErrNodeExists, e) && !zkhelper.ZkErrorEqual(zk.ErrNotEmpty, e)
}

// 获取指定路径节点的信息
func (c *safeConn) Get(path string) (data []byte, stat zk.Stat, err error) {
	for i := 0; i <= retryMaxOnOps; i++ {
		c.builder.lock.RLock()
		data, stat, err = c.Conn.Get(path)
		c.builder.lock.RUnlock()
		if err == nil || !isConnectionError(err) {
			return
		}
		c.builder.resetConnection()
	}
	log.PanicErrorf(err, "zk error after retries")
	return
}

func (c *safeConn) GetW(path string) (data []byte, stat zk.Stat, watch <-chan zk.Event, err error) {
	for i := 0; i <= retryMaxOnOps; i++ {
		c.builder.lock.RLock()
		data, stat, watch, err = c.Conn.GetW(path)
		c.builder.lock.RUnlock()
		if err == nil || !isConnectionError(err) {
			return
		}
		c.builder.resetConnection()
	}
	log.PanicErrorf(err, "zk error after retries")
	return
}

func (c *safeConn) Children(path string) (children []string, stat zk.Stat, err error) {
	for i := 0; i <= retryMaxOnOps; i++ {
		c.builder.lock.RLock()
		children, stat, err = c.Conn.Children(path)
		c.builder.lock.RUnlock()
		if err == nil || !isConnectionError(err) {
			return
		}
		c.builder.resetConnection()
	}
	log.PanicErrorf(err, "zk error after retries")
	return
}

func (c *safeConn) ChildrenW(path string) (children []string, stat zk.Stat, watch <-chan zk.Event, err error) {
	for i := 0; i <= retryMaxOnOps; i++ {
		c.builder.lock.RLock()
		children, stat, watch, err = c.Conn.ChildrenW(path)
		c.builder.lock.RUnlock()
		if err == nil || !isConnectionError(err) {
			return
		}
		c.builder.resetConnection()
	}
	log.PanicErrorf(err, "zk error after retries")
	return
}

func (c *safeConn) Exists(path string) (exist bool, stat zk.Stat, err error) {
	for i := 0; i <= retryMaxOnOps; i++ {
		c.builder.lock.RLock()
		exist, stat, err = c.Conn.Exists(path)
		c.builder.lock.RUnlock()
		if err == nil || !isConnectionError(err) {
			return
		}
		c.builder.resetConnection()
	}
	log.PanicErrorf(err, "zk error after retries")
	return
}

func (c *safeConn) ExistsW(path string) (exist bool, stat zk.Stat, watch <-chan zk.Event, err error) {
	for i := 0; i <= retryMaxOnOps; i++ {
		c.builder.lock.RLock()
		exist, stat, watch, err = c.Conn.ExistsW(path)
		c.builder.lock.RUnlock()
		if err == nil || !isConnectionError(err) {
			return
		}
		c.builder.resetConnection()
	}
	log.PanicErrorf(err, "zk error after retries")
	return
}

// 创建节点信息
func (c *safeConn) Create(path string, value []byte, flags int32, aclv []zk.ACL) (pathCreated string, err error) {
	for i := 0; i <= retryMaxOnOps; i++ {
		c.builder.lock.RLock()
		pathCreated, err = c.Conn.Create(path, value, flags, aclv)
		c.builder.lock.RUnlock()
		if err == nil || !isConnectionError(err) {
			return
		}
		c.builder.resetConnection()
	}
	log.PanicErrorf(err, "zk error after retries")
	return
}

func (c *safeConn) Set(path string, value []byte, version int32) (stat zk.Stat, err error) {
	for i := 0; i <= retryMaxOnOps; i++ {
		c.builder.lock.RLock()
		stat, err = c.Conn.Set(path, value, version)
		c.builder.lock.RUnlock()
		if err == nil || !isConnectionError(err) {
			return
		}
		c.builder.resetConnection()
	}
	log.PanicErrorf(err, "zk error after retries")
	return
}

func (c *safeConn) Delete(path string, version int32) (err error) {
	for i := 0; i <= retryMaxOnOps; i++ {
		c.builder.lock.RLock()
		err = c.Conn.Delete(path, version)
		c.builder.lock.RUnlock()
		if err == nil || !isConnectionError(err) {
			return
		}
		c.builder.resetConnection()
	}
	log.PanicErrorf(err, "zk error after retries")
	return
}

func (c *safeConn) Close() {
	log.Panicf("do not close zk connection by yourself")
}

func (c *safeConn) GetACL(path string) (acl []zk.ACL, stat zk.Stat, err error) {
	for i := 0; i <= retryMaxOnOps; i++ {
		c.builder.lock.RLock()
		acl, stat, err = c.Conn.GetACL(path)
		c.builder.lock.RUnlock()
		if err == nil || !isConnectionError(err) {
			return
		}
		c.builder.resetConnection()
	}
	log.PanicErrorf(err, "zk error after retries")
	return
}

func (c *safeConn) SetACL(path string, aclv []zk.ACL, version int32) (stat zk.Stat, err error) {
	for i := 0; i <= retryMaxOnOps; i++ {
		c.builder.lock.RLock()
		stat, err = c.Conn.SetACL(path, aclv, version)
		c.builder.lock.RUnlock()
		if err == nil || !isConnectionError(err) {
			return
		}
		c.builder.resetConnection()
	}
	log.PanicErrorf(err, "zk error after retries")
	return
}

func (c *safeConn) Seq2Str(seq int64) string {
	return c.Conn.Seq2Str(seq)
}

// unsafeConn 连接出错的话不会直接退出程序，而是返回错误信息，并且创建另外一个协程去重连
func (c *unsafeConn) Get(path string) (data []byte, stat zk.Stat, err error) {
	c.builder.lock.RLock()
	data, stat, err = c.Conn.Get(path)
	c.builder.lock.RUnlock()
	if err != nil && isConnectionError(err) {
		go c.builder.resetConnection()
	}
	return
}

func (c *unsafeConn) GetW(path string) (data []byte, stat zk.Stat, watch <-chan zk.Event, err error) {
	c.builder.lock.RLock()
	data, stat, watch, err = c.Conn.GetW(path)
	c.builder.lock.RUnlock()
	if err != nil && isConnectionError(err) {
		go c.builder.resetConnection()
	}
	return
}

func (c *unsafeConn) Children(path string) (children []string, stat zk.Stat, err error) {
	c.builder.lock.RLock()
	children, stat, err = c.Conn.Children(path)
	c.builder.lock.RUnlock()
	if err != nil && isConnectionError(err) {
		go c.builder.resetConnection()
	}
	return
}

func (c *unsafeConn) ChildrenW(path string) (children []string, stat zk.Stat, watch <-chan zk.Event, err error) {
	c.builder.lock.RLock()
	children, stat, watch, err = c.Conn.ChildrenW(path)
	c.builder.lock.RUnlock()
	if err != nil && isConnectionError(err) {
		go c.builder.resetConnection()
	}
	return
}

func (c *unsafeConn) Exists(path string) (exist bool, stat zk.Stat, err error) {
	c.builder.lock.RLock()
	exist, stat, err = c.Conn.Exists(path)
	c.builder.lock.RUnlock()
	if err != nil && isConnectionError(err) {
		go c.builder.resetConnection()
	}
	return
}

func (c *unsafeConn) ExistsW(path string) (exist bool, stat zk.Stat, watch <-chan zk.Event, err error) {
	c.builder.lock.RLock()
	exist, stat, watch, err = c.Conn.ExistsW(path)
	c.builder.lock.RUnlock()
	if err != nil && isConnectionError(err) {
		go c.builder.resetConnection()
	}
	return
}

func (c *unsafeConn) Create(path string, value []byte, flags int32, aclv []zk.ACL) (pathCreated string, err error) {
	c.builder.lock.RLock()
	pathCreated, err = c.Conn.Create(path, value, flags, aclv)
	c.builder.lock.RUnlock()
	if err != nil && isConnectionError(err) {
		go c.builder.resetConnection()
	}
	return
}

func (c *unsafeConn) Set(path string, value []byte, version int32) (stat zk.Stat, err error) {
	c.builder.lock.RLock()
	stat, err = c.Conn.Set(path, value, version)
	c.builder.lock.RUnlock()
	if err != nil && isConnectionError(err) {
		go c.builder.resetConnection()
	}
	return
}

func (c *unsafeConn) Delete(path string, version int32) (err error) {
	c.builder.lock.RLock()
	err = c.Conn.Delete(path, version)
	c.builder.lock.RUnlock()
	if err != nil && isConnectionError(err) {
		go c.builder.resetConnection()
	}
	return
}

func (c *unsafeConn) Close() {
	log.Panicf("do not close zk connection by yourself")
}

func (c *unsafeConn) GetACL(path string) (acl []zk.ACL, stat zk.Stat, err error) {
	c.builder.lock.RLock()
	acl, stat, err = c.Conn.GetACL(path)
	c.builder.lock.RUnlock()
	if err != nil && isConnectionError(err) {
		go c.builder.resetConnection()
	}
	return
}

func (c *unsafeConn) SetACL(path string, aclv []zk.ACL, version int32) (stat zk.Stat, err error) {
	c.builder.lock.RLock()
	stat, err = c.Conn.SetACL(path, aclv, version)
	c.builder.lock.RUnlock()
	if err != nil && isConnectionError(err) {
		go c.builder.resetConnection()
	}
	return
}

func (c *unsafeConn) Seq2Str(seq int64) string {
	return c.Conn.Seq2Str(seq)
}

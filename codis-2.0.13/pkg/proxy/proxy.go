// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy/router"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/wandoulabs/go-zookeeper/zk"
	topo "github.com/wandoulabs/go-zookeeper/zk"
)

// proxy-server
type Server struct {
	conf   *Config              // 配置信息
	topo   *Topology            // 集群拓扑信息管理对象
	info   models.ProxyInfo     // proxy信息
	groups map[int]int          // group信息

	lastActionSeq int           // 最近一次通知的序号

	evtbus   chan interface{}   // 用于监听zk节点，返回节点变更的事件
	router   *router.Router     // 用于访问后端redis的路由
	listener net.Listener

	kill chan interface{}       // 通过此通道通知close消息
	wait sync.WaitGroup         // 用于等待proxy结束
	stop sync.Once
}

// 创建一个 proxy-server
func New(addr string, debugVarAddr string, conf *Config) *Server {
	log.Infof("create proxy with config: %+v", conf)

	proxyHost := strings.Split(addr, ":")[0]
	debugHost := strings.Split(debugVarAddr, ":")[0]

    // 如果绑定的地址是 0.0.0.0 或者 127.0.0.1 就会采用 hostname
    // 因为这个地址是要存储到 zk 上，然后dashboard从zk上获取并访问
	hostname, err := os.Hostname()
	if err != nil {
		log.PanicErrorf(err, "get host name failed")
	}
	if proxyHost == "0.0.0.0" || strings.HasPrefix(proxyHost, "127.0.0.") || proxyHost == "" {
		proxyHost = hostname
	}
	if debugHost == "0.0.0.0" || strings.HasPrefix(debugHost, "127.0.0.") || debugHost == "" {
		debugHost = hostname
	}

	s := &Server{conf: conf, lastActionSeq: -1, groups: make(map[int]int)}

    // 创建集群拓扑信息管理对象
	s.topo = NewTopo(conf.productName, conf.zkAddr, conf.fact, conf.provider, conf.zkSessionTimeout)
    // 初始化proxy信息
	s.info.Id = conf.proxyId
	s.info.State = models.PROXY_STATE_OFFLINE
	s.info.Addr = proxyHost + ":" + strings.Split(addr, ":")[1]
	s.info.DebugVarAddr = debugHost + ":" + strings.Split(debugVarAddr, ":")[1]
	s.info.Pid = os.Getpid()
	s.info.StartAt = time.Now().String()
	s.kill = make(chan interface{})

	log.Infof("proxy info = %+v", s.info)

    // 监听代理端口
	if l, err := net.Listen(conf.proto, addr); err != nil {
		log.PanicErrorf(err, "open listener failed")
	} else {
		s.listener = l
	}
    // 创建一个访问后端redis的路由
	s.router = router.NewWithAuth(conf.passwd)
	s.evtbus = make(chan interface{}, 1024)

    // 在zk上注册自身的信息，包括proxy和fence节点
	s.register()

	s.wait.Add(1)
	go func() {
		defer s.wait.Done()
        // 启动proxy的主要处理函数
		s.serve()
	}()
	return s
}

// 通过调用dashboard将自己的状态设置为online
func (s *Server) SetMyselfOnline() error {
	log.Info("mark myself online")
	info := models.ProxyInfo{
		Id:    s.conf.proxyId,
		State: models.PROXY_STATE_ONLINE,
	}
	b, _ := json.Marshal(info)
	url := "http://" + s.conf.dashboardAddr + "/api/proxy"
	res, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return errors.New("response code is not 200")
	}
	return nil
}

// 启动proxy的主要逻辑
func (s *Server) serve() {
	defer s.close()

    // 每隔3s钟检查一次zk中此proxy的信息，直到状态变为online后返回true，或者接到 mark_offline 或者kill信号，返回false
    // 阻塞直到处于online状态返回true后正常工作
	if !s.waitOnline() {
		return
	}

    // 重新监听所有 proxy 节点的变更
	s.rewatchNodes()

    // 填充指定slot的信息，建立与所在redis-server的连接
	for i := 0; i < router.MaxSlotNum; i++ {
		s.fillSlot(i)
	}
	log.Info("proxy is serving")
	go func() {
		defer s.close()
        // 处理 redis 客户端的连接
		s.handleConns()
	}()

    
    // 循环等待事件触发
    // 这里做三件事
    // 1. 通过 s.kill 接收停止信号，设置 mark_offline状态
    // 2. 检测到zk上有状态变更，进行相应的处理
    // 3. 每隔指定间隔时间，向后端的redis-server发送心跳包
	s.loopEvents()
}

// 处理 redis 客户端的连接
func (s *Server) handleConns() {

	ch := make(chan net.Conn, 4096)
	defer close(ch)

	go func() {
		for c := range ch {
			x := router.NewSessionSize(c, s.conf.passwd, s.conf.maxBufSize, s.conf.maxTimeout)
            // 针对一个redis-client连接的处理函数，会将请求交由 s.router 转发给后端 redis-server
			go x.Serve(s.router, s.conf.maxPipeline)
		}
	}()

    // 循环处理 redis 客户端的连接，通过 ch 通道交给上面的 x.Server 函数处理
	for {
		c, err := s.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.WarnErrorf(err, "[%p] proxy accept new connection failed, get temporary error", s)
				time.Sleep(time.Millisecond*10)
				continue
			}
			log.WarnErrorf(err, "[%p] proxy accept new connection failed, get non-temporary error, must shutdown", s)
			return
		} else {
			ch <- c
		}
	}
}

func (s *Server) Info() models.ProxyInfo {
	return s.info
}

func (s *Server) Join() {
	s.wait.Wait()
}

func (s *Server) Close() error {
	s.close()
	s.wait.Wait()
	return nil
}

func (s *Server) close() {
    // 确保只执行一次
	s.stop.Do(func() {
		s.listener.Close()
        // 关闭和redis之间的路由
		if s.router != nil {
			s.router.Close()
		}
		close(s.kill)
	})
}

// 重新监听该proxy节点，关注自身的状态变更
func (s *Server) rewatchProxy() {
    // 监听zk上此proxy节点
	_, err := s.topo.WatchNode(path.Join(models.GetProxyPath(s.topo.ProductName), s.info.Id), s.evtbus)
	if err != nil {
		log.PanicErrorf(err, "watch node failed")
	}
}

// 监听 action 节点
func (s *Server) rewatchNodes() []string {
	nodes, err := s.topo.WatchChildren(models.GetWatchActionPath(s.topo.ProductName), s.evtbus)
	if err != nil {
		log.PanicErrorf(err, "watch children failed")
	}
	return nodes
}

// 在zk上注册自身的信息，包括proxy和fence节点
func (s *Server) register() {
    // 在zk上创建自身的proxy信息
	if _, err := s.topo.CreateProxyInfo(&s.info); err != nil {
		log.PanicErrorf(err, "create proxy node failed")
	}
    // 在fence节点上创建proxy信息
	if _, err := s.topo.CreateProxyFenceNode(&s.info); err != nil && err != zk.ErrNodeExists {
		log.PanicErrorf(err, "create fence node failed")
	}
	log.Warn("********** Attention **********")
	log.Warn("You should use `kill {pid}` rather than `kill -9 {pid}` to stop me,")
	log.Warn("or the node resisted on zk will not be cleaned when I'm quiting and you must remove it manually")
	log.Warn("*******************************")
}

// 将proxy状态修改为 offline，会删除zk上此proxy相关的节点
func (s *Server) markOffline() {
    // 删除zk上proxy相关的节点
	s.topo.Close(s.info.Id)
	s.info.State = models.PROXY_STATE_MARK_OFFLINE
}

// 每隔3s钟检查一次zk中此proxy的信息，直到状态变为online后返回true，或者接到 mark_offline 或者kill信号，返回false
func (s *Server) waitOnline() bool {
	for {
        // 获取zk上存储的自身的信息
		info, err := s.topo.GetProxyInfo(s.info.Id)
		if err != nil {
			log.PanicErrorf(err, "get proxy info failed: %s", s.info.Id)
		}

        // 状态有3种，online offline mark_offline
		switch info.State {
        // mark_offline，接到了被下线的事件
		case models.PROXY_STATE_MARK_OFFLINE:
			log.Infof("mark offline, proxy got offline event: %s", s.info.Id)
            // 将proxy状态修改为 offline，会删除zk上此proxy相关的节点
			s.markOffline()
			return false
        // 处于online，返回true
		case models.PROXY_STATE_ONLINE:
			s.info.State = info.State
			log.Infof("we are online: %s", s.info.Id)
            // 重新监听此proxy节点
			s.rewatchProxy()
			return true
		}
		select {
        // 结束信号，结束此循环，退出
		case <-s.kill:
			log.Infof("mark offline, proxy is killed: %s", s.info.Id)
			s.markOffline()
			return false
		default:
		}
		log.Infof("wait to be online: %s", s.info.Id)
		time.Sleep(3 * time.Second)
	}
}

// 获取有状态变更的路径
func getEventPath(evt interface{}) string {
	return evt.(topo.Event).Path
}

// 检查自己(proxy)是否在 receivers 中
func needResponse(receivers []string, self models.ProxyInfo) bool {
	var info models.ProxyInfo
	for _, v := range receivers {
		err := json.Unmarshal([]byte(v), &info)
        // receivers有可能不是一个 json字符串格式而只是proxy_id
		if err != nil {
			if v == self.Id {
				return true
			}
			return false
		}
        // 检查是否是同一个proxy
		if info.Id == self.Id && info.Pid == self.Pid && info.StartAt == self.StartAt {
			return true
		}
	}
	return false
}

// 获取一个group中处于master身份的redis-server的地址
func groupMaster(groupInfo models.ServerGroup) string {
	var master string
	for _, server := range groupInfo.Servers {
		if server.Type == models.SERVER_TYPE_MASTER {
			if master != "" {
				log.Panicf("two master not allowed: %+v", groupInfo)
			}
			master = server.Addr
		}
	}
	if master == "" {
		log.Panicf("master not found: %+v", groupInfo)
	}
	return master
}

// 重置指定slot信息
func (s *Server) resetSlot(i int) {
	s.router.ResetSlot(i)
}

// 填充指定slot的信息，建立与所在redis-server的连接
// 之后关于redis的操作会根据key映射到slot，再从slot中找到与其所在redis-server的连接
func (s *Server) fillSlot(i int) {
    // 获取指定id的slot信息，并且获取所在group的信息
	slotInfo, slotGroup, err := s.topo.GetSlotByIndex(i)
	if err != nil {
		log.PanicErrorf(err, "get slot by index failed", i)
	}

	var from string
    // 获取一个group中处于master身份的redis-server的地址
	var addr = groupMaster(*slotGroup)
	if slotInfo.State.Status == models.SLOT_STATUS_MIGRATE {
		fromGroup, err := s.topo.GetGroup(slotInfo.State.MigrateStatus.From)
		if err != nil {
			log.PanicErrorf(err, "get migrate from failed")
		}
		from = groupMaster(*fromGroup)
		if from == addr {
			log.Panicf("set slot %04d migrate from %s to %s", i, from, addr)
		}
	}

    // 将slot所在groupId加入到map中
	s.groups[i] = slotInfo.GroupId
    // 填充指定slot的信息，建立与所在redis-server的连接
	s.router.FillSlot(i, addr, from,
		slotInfo.State.Status == models.SLOT_STATUS_PRE_MIGRATE)
}

// 批量更新slots状态信息
func (s *Server) onSlotRangeChange(param *models.SlotMultiSetParam) {
	log.Infof("slotRangeChange %+v", param)
	for i := param.From; i <= param.To; i++ {
		switch param.Status {
		case models.SLOT_STATUS_OFFLINE:
			s.resetSlot(i)
		case models.SLOT_STATUS_ONLINE:
			s.fillSlot(i)
		default:
			log.Panicf("can not handle status %v", param.Status)
		}
	}
}

// group状态变更，需要将属于此group的slot的状态全部更新
func (s *Server) onGroupChange(groupId int) {
	log.Infof("group changed %d", groupId)
	for i, g := range s.groups {
		if g == groupId {
			s.fillSlot(i)
		}
	}
}

// 回复通知，就是在 ActionResponse 的 seq 节点下创建以自己 proxy_id 命名的节点
func (s *Server) responseAction(seq int64) {
	log.Infof("send response seq = %d", seq)
	err := s.topo.DoResponse(int(seq), &s.info)
	if err != nil {
		log.InfoErrorf(err, "send response seq = %d failed", seq)
	}
}

// 根据序号获取解析后的action对象，其实是为了将json解析成 target 对象
func (s *Server) getActionObject(seq int, target interface{}) {
	act := &models.Action{Target: target}
    // 根据序号获取解析后的action对象
	err := s.topo.GetActionWithSeqObject(int64(seq), act)
	if err != nil {
		log.PanicErrorf(err, "get action object failed, seq = %d", seq)
	}
	log.Infof("action %+v", act)
}

// 检查通知内容，有需要就更新slot状态信息
func (s *Server) checkAndDoTopoChange(seq int) bool {
    // 根据序号获取通知信息
	act, err := s.topo.GetActionWithSeq(int64(seq))
	if err != nil { //todo: error is not "not exist"
		log.PanicErrorf(err, "action failed, seq = %d", seq)
	}

    // 检查是否需要自己回复
	if !needResponse(act.Receivers, s.info) { //no need to response
		return false
	}

	log.Warnf("action %v receivers %v", seq, act.Receivers)

	switch act.Type {
    // slot状态变更，重新获取并更新slot信息
	case models.ACTION_TYPE_SLOT_MIGRATE, models.ACTION_TYPE_SLOT_CHANGED,
		models.ACTION_TYPE_SLOT_PREMIGRATE:
		slot := &models.Slot{}
		s.getActionObject(seq, slot)
		s.fillSlot(slot.Id)
    // group状态变更，将属于此group的slot的状态全部更新
	case models.ACTION_TYPE_SERVER_GROUP_CHANGED:
		serverGroup := &models.ServerGroup{}
		s.getActionObject(seq, serverGroup)
		s.onGroupChange(serverGroup.Id)
    // 因为要remove group必须要将所有slot迁走，所以不需要关心这个
	case models.ACTION_TYPE_SERVER_GROUP_REMOVE:
	//do not care

    // 批量迁移slot的通知，逐一更新slot的状态信息
	case models.ACTION_TYPE_MULTI_SLOT_CHANGED:
		param := &models.SlotMultiSetParam{}
		s.getActionObject(seq, param)
		s.onSlotRangeChange(param)
	default:
		log.Panicf("unknown action %+v", act)
	}
	return true
}

// 处理 zk 上的 watch 节点变更的通知，主要有两种，一种是自身proxy的状态变更，一种是 action 通知消息的更新
func (s *Server) processAction(e interface{}) {
    // 如果是单个proxy的变更通知
	if strings.Index(getEventPath(e), models.GetProxyPath(s.topo.ProductName)) == 0 {
        // 获取自身的proxy信息
		info, err := s.topo.GetProxyInfo(s.info.Id)
		if err != nil {
			log.PanicErrorf(err, "get proxy info failed: %s", s.info.Id)
		}
        // 根据变更后的状态进行相应操作
		switch info.State {
		case models.PROXY_STATE_MARK_OFFLINE:
			log.Infof("mark offline, proxy got offline event: %s", s.info.Id)
			s.markOffline()
		case models.PROXY_STATE_ONLINE:
			s.rewatchProxy()
		default:
			log.Panicf("unknown proxy state %v", info)
		}
		return
	}

    // action 目录通知信息
	// re-watch
    // 获取当前 actions 目录下所有的节点信息，并且重新监听
	nodes := s.rewatchNodes()

    // 将字符串序号数组转换成int数组，并且排序
	seqs, err := models.ExtraSeqList(nodes)
	if err != nil {
		log.PanicErrorf(err, "get seq list failed")
	}

	if len(seqs) == 0 || !s.topo.IsChildrenChangedEvent(e) {
		return
	}

	// get last pos
    // 定位到尚未读取的那一条action的序号
	index := -1
	for i, seq := range seqs {
		if s.lastActionSeq < seq {
			index = i
			//break
			//only handle latest action
		}
	}

	if index < 0 {
		return
	}

	actions := seqs[index:]
	for _, seq := range actions {
        // 根据 action 里的序号，返回 ActionResponse 里的路径
		exist, err := s.topo.Exist(path.Join(s.topo.GetActionResponsePath(seq), s.info.Id))
		if err != nil {
			log.PanicErrorf(err, "get action failed")
		}
		if exist {
			continue
		}
        // 检查通知内容，有需要就更新slot状态信息
		if s.checkAndDoTopoChange(seq) {
            // 需要回复的话，就在 ActionResponse 下创建节点完成回复
			s.responseAction(int64(seq))
		}
	}

    // 更新已经接收到的通知序号
	s.lastActionSeq = seqs[len(seqs)-1]
}

// 循环等待事件触发
// 这里做三件事
// 1. 通过 s.kill 接收停止信号，设置 mark_offline状态
// 2. 检测到zk上有状态变更，进行相应的处理
// 3. 每隔指定间隔时间，向后端的redis-server发送心跳包
func (s *Server) loopEvents() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var tick int = 0
	for s.info.State == models.PROXY_STATE_ONLINE {
		select {
		case <-s.kill:
            // proxy停止
			log.Infof("mark offline, proxy is killed: %s", s.info.Id)
			s.markOffline()
		case e := <-s.evtbus:
            // 检测到zk上有状态变更，进行相应的处理
			evtPath := getEventPath(e)
			log.Infof("got event %s, %v, lastActionSeq %d", s.info.Id, e, s.lastActionSeq)
			if strings.Index(evtPath, models.GetActionResponsePath(s.conf.productName)) == 0 {
				seq, err := strconv.Atoi(path.Base(evtPath))
				if err != nil {
					log.ErrorErrorf(err, "parse action seq failed")
				} else {
                    // 如果是重复的action，忽略
					if seq < s.lastActionSeq {
						log.Infof("ignore seq = %d", seq)
						continue
					}
				}
			}
            // 处理 zk 上的 watch 节点变更的通知，主要有两种，一种是自身proxy的状态变更，一种是 action 通知消息的更新
			s.processAction(e)
		case <-ticker.C:
            // 每隔5秒钟向后端 redis-server 发送心跳包
			if maxTick := s.conf.pingPeriod; maxTick != 0 {
				if tick++; tick >= maxTick {
					s.router.KeepAlive()
					tick = 0
				}
			}
		}
	}
}

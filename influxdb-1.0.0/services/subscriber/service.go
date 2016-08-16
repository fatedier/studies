package subscriber // import "github.com/influxdata/influxdb/services/subscriber"

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/services/meta"
)

// 这个服务用于将写入 influxDB 的数据复制一份写入其他服务，例如配合 Kapacitor 可以实现对数据的监控与告警

// Statistics for the Subscriber service.
const (
	statPointsWritten = "pointsWritten"
	statWriteFailures = "writeFailures"
)

// PointsWriter is an interface for writing points to a subscription destination.
// Only WritePoints() needs to be satisfied.
type PointsWriter interface {
	WritePoints(p *coordinator.WritePointsRequest) error
}

// unique set that identifies a given subscription
type subEntry struct {
	db   string		// 数据库名
	rp   string		// 保存策略
	name string
}

// Service manages forking the incoming data from InfluxDB
// to defined third party destinations.
// Subscriptions are defined per database and retention policy.
type Service struct {
	MetaClient interface {
		Databases() []meta.DatabaseInfo
		WaitForDataChanged() chan struct{}
	}
	NewPointsWriter func(u url.URL) (PointsWriter, error)	// 用于创建一个 PointsWriter 对象
	Logger          *log.Logger
	update          chan struct{}
	stats           *Statistics		// 写入操作的统计信息
	points          chan *coordinator.WritePointsRequest	// 用于向此 subscriber 写入 points 的通道
	wg              sync.WaitGroup
	closed          bool
	closing         chan struct{}
	mu              sync.Mutex
	conf            Config

	subs  map[subEntry]chanWriter
	subMu sync.RWMutex
}

// NewService returns a subscriber service with given settings
func NewService(c Config) *Service {
	s := &Service{
		Logger: log.New(os.Stderr, "[subscriber] ", log.LstdFlags),
		closed: true,
		stats:  &Statistics{},
		conf:   c,
	}
	s.NewPointsWriter = s.newPointsWriter
	return s
}

// Open starts the subscription service.
func (s *Service) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.MetaClient == nil {
		return errors.New("no meta store")
	}

	s.closed = false

	s.closing = make(chan struct{})
	s.update = make(chan struct{})
	s.points = make(chan *coordinator.WritePointsRequest, 100)

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		s.run()
	}()
	go func() {
		defer s.wg.Done()
		s.waitForMetaUpdates()
	}()

	s.Logger.Println("opened service")
	return nil
}

// Close terminates the subscription service
// Will panic if called multiple times or without first opening the service.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true

	close(s.points)
	close(s.closing)

	s.wg.Wait()
	s.Logger.Println("closed service")
	return nil
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (s *Service) SetLogOutput(w io.Writer) {
	s.Logger = log.New(w, "[subscriber] ", log.LstdFlags)
}

// Statistics maintains the statistics for the subscriber service.
type Statistics struct {
	WriteFailures int64
	PointsWritten int64
}

// Statistics returns statistics for periodic monitoring.
// 获取所有 PointsWriter 的写入信息
func (s *Service) Statistics(tags map[string]string) []models.Statistic {
	statistics := []models.Statistic{{
		Name: "subscriber",
		Tags: tags,
		Values: map[string]interface{}{
			statPointsWritten: atomic.LoadInt64(&s.stats.PointsWritten),
			statWriteFailures: atomic.LoadInt64(&s.stats.WriteFailures),
		},
	}}

	s.subMu.RLock()
	defer s.subMu.RUnlock()

	for _, sub := range s.subs {
		statistics = append(statistics, sub.Statistics(tags)...)
	}
	return statistics
}

// 当有元数据信息变更时，需要相应地创建或者删除对应的 PointsWriter
func (s *Service) waitForMetaUpdates() {
	for {
		ch := s.MetaClient.WaitForDataChanged()
		select {
		case <-ch:
			err := s.Update()
			if err != nil {
				s.Logger.Println("error updating subscriptions:", err)
			}
		case <-s.closing:
			return
		}
	}
}

// Update will start new and stop deleted subscriptions.
// 通知 service 需要 update
func (s *Service) Update() error {
	// signal update
	select {
	case s.update <- struct{}{}:
		return nil
	case <-s.closing:
		return errors.New("service closed cannot update")
	}
}

// 创建订阅者
func (s *Service) createSubscription(se subEntry, mode string, destinations []string) (PointsWriter, error) {
	var bm BalanceMode
	switch mode {
	case "ALL":
		bm = ALL
	case "ANY":
		bm = ANY
	default:
		return nil, fmt.Errorf("unknown balance mode %q", mode)
	}
	writers := make([]PointsWriter, len(destinations))
	stats := make([]writerStats, len(writers))
	for i, dest := range destinations {
		fmt.Printf("se.name: %s dest: %s\n", se.name, dest)
		u, err := url.Parse(dest)
		if err != nil {
			return nil, err
		}
		w, err := s.NewPointsWriter(*u)
		if err != nil {
			return nil, err
		}
		writers[i] = w
		stats[i].dest = dest
	}
	return &balancewriter{
		bm:      bm,
		writers: writers,
		stats:   stats,
		tags: map[string]string{
			"database":         se.db,
			"retention_policy": se.rp,
			"name":             se.name,
			"mode":             mode,
		},
	}, nil
}

// Points returns a channel into which write point requests can be sent.
// 通过这个函数返回的通道写入数据
func (s *Service) Points() chan<- *coordinator.WritePointsRequest {
	return s.points
}

// read points off chan and write them
func (s *Service) run() {
	var wg sync.WaitGroup
	s.subs = make(map[subEntry]chanWriter)
	// Perform initial update
	s.updateSubs(&wg)
	for {
		select {
		case <-s.update:
			err := s.updateSubs(&wg)
			if err != nil {
				s.Logger.Println("failed to update subscriptions:", err)
			}
		case p, ok := <-s.points:
			if !ok {
				// Close out all chanWriters
				s.close(&wg)
				return
			}
			for se, cw := range s.subs {
				if p.Database == se.db && p.RetentionPolicy == se.rp {
					select {
					case cw.writeRequests <- p:
					default:
						atomic.AddInt64(&s.stats.WriteFailures, 1)
					}
				}
			}
		}
	}
}

// close closes the existing channel writers
func (s *Service) close(wg *sync.WaitGroup) {
	s.subMu.Lock()
	defer s.subMu.Unlock()

	for _, cw := range s.subs {
		cw.Close()
	}
	// Wait for them to finish
	wg.Wait()
	s.subs = nil
}

func (s *Service) updateSubs(wg *sync.WaitGroup) error {
	s.subMu.Lock()
	defer s.subMu.Unlock()

	if s.subs == nil {
		s.subs = make(map[subEntry]chanWriter)
	}

	dbis := s.MetaClient.Databases()	// 返回所有的数据库信息
	allEntries := make(map[subEntry]bool, 0)
	// Add in new subscriptions
	// 创建新的订阅者

	// 遍历每一个数据库
	for _, dbi := range dbis {
		// 遍历每一个数据库中的存储策略
		for _, rpi := range dbi.RetentionPolicies {
			// 遍历每一个存储策略下的订阅者
			for _, si := range rpi.Subscriptions {
				se := subEntry{
					db:   dbi.Name,
					rp:   rpi.Name,
					name: si.Name,
				}
				allEntries[se] = true
				if _, ok := s.subs[se]; ok {
					continue
				}
				sub, err := s.createSubscription(se, si.Mode, si.Destinations)
				if err != nil {
					return err
				}
				cw := chanWriter{
					writeRequests: make(chan *coordinator.WritePointsRequest, 100),
					pw:            sub,
					pointsWritten: &s.stats.PointsWritten,
					failures:      &s.stats.WriteFailures,
					logger:        s.Logger,
				}
				wg.Add(1)
				go func() {
					defer wg.Done()
					cw.Run()
				}()
				s.subs[se] = cw
				s.Logger.Println("added new subscription for", se.db, se.rp)
			}
		}
	}

	// Remove deleted subs
	for se := range s.subs {
		if !allEntries[se] {
			// Close the chanWriter
			s.subs[se].Close()

			// Remove it from the set
			delete(s.subs, se)
			s.Logger.Println("deleted old subscription for", se.db, se.rp)
		}
	}

	return nil
}

// Creates a PointsWriter from the given URL
// 根据给定的 URL 对象返回一个 PointsWriter 对象
func (s *Service) newPointsWriter(u url.URL) (PointsWriter, error) {
	switch u.Scheme {
	case "udp":
		return NewUDP(u.Host), nil
	case "http", "https":
		return NewHTTP(u.String(), time.Duration(s.conf.HTTPTimeout))
	default:
		return nil, fmt.Errorf("unknown destination scheme %s", u.Scheme)
	}
}

// Sends WritePointsRequest to a PointsWriter received over a channel.
type chanWriter struct {
	writeRequests chan *coordinator.WritePointsRequest	// 用于接收要写入的数据
	pw            PointsWriter	// 用于写入数据，调用其 WritePoints 方法
	pointsWritten *int64		// 成功写入 point 的个数
	failures      *int64		// 写入失败的 point 个数
	logger        *log.Logger
}

// Close the chanWriter
func (c chanWriter) Close() {
	close(c.writeRequests)
}

// 循环从通道中读取要写入的数据，执行 WritePoints 操作写入数据
func (c chanWriter) Run() {
	for wr := range c.writeRequests {
		err := c.pw.WritePoints(wr)
		if err != nil {
			c.logger.Println(err)
			atomic.AddInt64(c.failures, 1)
		} else {
			atomic.AddInt64(c.pointsWritten, int64(len(wr.Points)))
		}
	}
}

// Statistics returns statistics for periodic monitoring.
// 获取写入的统计信息
func (c chanWriter) Statistics(tags map[string]string) []models.Statistic {
	if m, ok := c.pw.(monitor.Reporter); ok {
		return m.Statistics(tags)
	}
	return []models.Statistic{}
}

// BalanceMode sets what balance mode to use on a subscription.
// valid options are currently ALL or ANY
type BalanceMode int

//ALL is a Balance mode option
const (
	ALL BalanceMode = iota
	ANY
)

type writerStats struct {
	dest          string
	failures      int64
	pointsWritten int64
}

// balances writes across PointsWriters according to BalanceMode
// 根据负载均衡策略进行写入
type balancewriter struct {
	bm      BalanceMode
	writers []PointsWriter
	stats   []writerStats
	tags    map[string]string
	i       int
}

// 轮转的方式依次写入不同的 PointsWriter
func (b *balancewriter) WritePoints(p *coordinator.WritePointsRequest) error {
	var lastErr error
	for range b.writers {
		// round robin through destinations.
		i := b.i
		w := b.writers[i]
		b.i = (b.i + 1) % len(b.writers)

		// write points to destination.
		err := w.WritePoints(p)
		if err != nil {
			lastErr = err
			atomic.AddInt64(&b.stats[i].failures, 1)
		} else {
			atomic.AddInt64(&b.stats[i].pointsWritten, int64(len(p.Points)))
			if b.bm == ANY {
				break
			}
		}
	}
	return lastErr
}

// Statistics returns statistics for periodic monitoring.
// 获取写入的统计信息
func (b *balancewriter) Statistics(tags map[string]string) []models.Statistic {
	tags = models.Tags(tags).Merge(b.tags)

	statistics := make([]models.Statistic, len(b.stats))
	for i := range b.stats {
		statistics[i] = models.Statistic{
			Name: "subscriber",
			Tags: models.Tags(tags).Merge(map[string]string{"destination": b.stats[i].dest}),
			Values: map[string]interface{}{
				statPointsWritten: atomic.LoadInt64(&b.stats[i].pointsWritten),
				statWriteFailures: atomic.LoadInt64(&b.stats[i].failures),
			},
		}
	}
	return statistics
}

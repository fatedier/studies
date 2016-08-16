package run

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/coordinator"
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/monitor"
	"github.com/influxdata/influxdb/services/admin"
	"github.com/influxdata/influxdb/services/collectd"
	"github.com/influxdata/influxdb/services/continuous_querier"
	"github.com/influxdata/influxdb/services/graphite"
	"github.com/influxdata/influxdb/services/httpd"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/opentsdb"
	"github.com/influxdata/influxdb/services/precreator"
	"github.com/influxdata/influxdb/services/retention"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/services/subscriber"
	"github.com/influxdata/influxdb/services/udp"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	client "github.com/influxdata/usage-client/v1"
	// Initialize the engine packages
	_ "github.com/influxdata/influxdb/tsdb/engine"
)

var startTime time.Time

func init() {
	startTime = time.Now().UTC()
}

// BuildInfo represents the build details for the server code.
type BuildInfo struct {
	Version string
	Commit  string
	Branch  string
	Time    string
}

// Server represents a container for the metadata and storage data and services.
// It is built using a Config and it manages the startup and shutdown of all
// services in the proper order.

// 从配置文件中获取信息创建，用于管理 influxDB 中的各个服务，包括 metadata 和 storage data 服务
type Server struct {
	buildInfo BuildInfo

	err     chan error
	closing chan struct{}

	BindAddress string			// 用于接收其他服务连接的地址
	Listener    net.Listener	// 用于接收其他服务连接的 listener

	Logger *log.Logger

	MetaClient *meta.Client

	TSDBStore     *tsdb.Store
	QueryExecutor *influxql.QueryExecutor
	PointsWriter  *coordinator.PointsWriter
	Subscriber    *subscriber.Service

	Services []Service			// influxDB 的各种服务

	// These references are required for the tcp muxer.
	SnapshotterService *snapshotter.Service

	Monitor *monitor.Monitor

	// Server reporting and registration
	reportingDisabled bool

	// Profiling
	CPUProfile string
	MemProfile string

	// httpAPIAddr is the host:port combination for the main HTTP API for querying and writing data
	httpAPIAddr string

	// httpUseTLS specifies if we should use a TLS connection to the http servers
	httpUseTLS bool

	// tcpAddr is the host:port combination for the TCP listener that services mux onto
	tcpAddr string

	config *Config

	// logOutput is the writer to which all services should be configured to
	// write logs to after appension.
	logOutput io.Writer
}

// NewServer returns a new instance of Server built from a config.
func NewServer(c *Config, buildInfo *BuildInfo) (*Server, error) {
	// We need to ensure that a meta directory always exists even if
	// we don't start the meta store.  node.json is always stored under
	// the meta directory.
	
	// 创建 meta 目录
	if err := os.MkdirAll(c.Meta.Dir, 0777); err != nil {
		return nil, fmt.Errorf("mkdir all: %s", err)
	}

	// 0.10-rc1 and prior would sometimes put the node.json at the root
	// dir which breaks backup/restore and restarting nodes.  This moves
	// the file from the root so it's always under the meta dir.

	// 这部分主要是集群，现在的版本貌似已经不再起作用
	oldPath := filepath.Join(filepath.Dir(c.Meta.Dir), "node.json")
	newPath := filepath.Join(c.Meta.Dir, "node.json")

	if _, err := os.Stat(oldPath); err == nil {
		if err := os.Rename(oldPath, newPath); err != nil {
			return nil, err
		}
	}

	log.Printf("meta dir: %s", c.Meta.Dir)
	_, err := influxdb.LoadNode(c.Meta.Dir)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	// 如果使用之前的集群功能，这里会提示不再支持
	if err := raftDBExists(c.Meta.Dir); err != nil {
		return nil, err
	}

	// In 0.10.0 bind-address got moved to the top level. Check
	// The old location to keep things backwards compatible
	bind := c.BindAddress

	s := &Server{
		buildInfo: *buildInfo,
		err:       make(chan error),
		closing:   make(chan struct{}),

		BindAddress: bind,

		Logger: log.New(os.Stderr, "", log.LstdFlags),

		MetaClient: meta.NewClient(c.Meta),

		reportingDisabled: c.ReportingDisabled,

		httpAPIAddr: c.HTTPD.BindAddress,
		httpUseTLS:  c.HTTPD.HTTPSEnabled,
		tcpAddr:     bind,

		config:    c,
		logOutput: os.Stderr,
	}
	s.Monitor = monitor.New(s, c.Monitor)

	// meta 元数据信息加载，启动服务
	if err := s.MetaClient.Open(); err != nil {
		return nil, err
	}

	s.TSDBStore = tsdb.NewStore(c.Data.Dir)
	s.TSDBStore.EngineOptions.Config = c.Data

	// Copy TSDB configuration.
	// 对应配置文件中的 [data].engine 选项，可以设置存储引擎，目前是 tsm1
	s.TSDBStore.EngineOptions.EngineVersion = c.Data.Engine

	// Create the Subscriber service
	// 这个服务用于将写入 influxDB 的数据复制一份写入其他服务，例如配合 Kapacitor 可以实现对数据的监控与告警
	s.Subscriber = subscriber.NewService(c.Subscriber)

	// Initialize points writer.
	// 用于执行数据写入的服务
	s.PointsWriter = coordinator.NewPointsWriter()
	s.PointsWriter.WriteTimeout = time.Duration(c.Coordinator.WriteTimeout)
	s.PointsWriter.TSDBStore = s.TSDBStore
	s.PointsWriter.Subscriber = s.Subscriber

	// Initialize query executor.
	// 用于执行数据查询操作的服务
	s.QueryExecutor = influxql.NewQueryExecutor()
	s.QueryExecutor.StatementExecutor = &coordinator.StatementExecutor{
		MetaClient:        s.MetaClient,
		TaskManager:       s.QueryExecutor.TaskManager,
		TSDBStore:         coordinator.LocalTSDBStore{Store: s.TSDBStore},
		Monitor:           s.Monitor,
		PointsWriter:      s.PointsWriter,
		MaxSelectPointN:   c.Coordinator.MaxSelectPointN,
		MaxSelectSeriesN:  c.Coordinator.MaxSelectSeriesN,
		MaxSelectBucketsN: c.Coordinator.MaxSelectBucketsN,
	}
	s.QueryExecutor.TaskManager.QueryTimeout = time.Duration(c.Coordinator.QueryTimeout)
	s.QueryExecutor.TaskManager.LogQueriesAfter = time.Duration(c.Coordinator.LogQueriesAfter)
	s.QueryExecutor.TaskManager.MaxConcurrentQueries = c.Coordinator.MaxConcurrentQueries

	// Initialize the monitor
	s.Monitor.Version = s.buildInfo.Version
	s.Monitor.Commit = s.buildInfo.Commit
	s.Monitor.Branch = s.buildInfo.Branch
	s.Monitor.BuildTime = s.buildInfo.Time
	s.Monitor.PointsWriter = (*monitorPointsWriter)(s.PointsWriter)
	return s, nil
}

func (s *Server) Statistics(tags map[string]string) []models.Statistic {
	var statistics []models.Statistic
	statistics = append(statistics, s.QueryExecutor.Statistics(tags)...)
	statistics = append(statistics, s.TSDBStore.Statistics(tags)...)
	statistics = append(statistics, s.PointsWriter.Statistics(tags)...)
	statistics = append(statistics, s.Subscriber.Statistics(tags)...)
	for _, srv := range s.Services {
		if m, ok := srv.(monitor.Reporter); ok {
			statistics = append(statistics, m.Statistics(tags)...)
		}
	}
	return statistics
}

// 注册快照服务
func (s *Server) appendSnapshotterService() {
	srv := snapshotter.NewService()
	srv.TSDBStore = s.TSDBStore
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
	s.SnapshotterService = srv
}

// SetLogOutput sets the logger used for all messages. It must not be called
// after the Open method has been called.
func (s *Server) SetLogOutput(w io.Writer) {
	s.Logger = log.New(os.Stderr, "", log.LstdFlags)
	s.logOutput = w
}

// 注册监控服务
func (s *Server) appendMonitorService() {
	s.Services = append(s.Services, s.Monitor)
}

// RP 服务，用于指定 metric 的保存时间
func (s *Server) appendRetentionPolicyService(c retention.Config) {
	if !c.Enabled {
		return
	}
	srv := retention.NewService(c)
	srv.MetaClient = s.MetaClient
	srv.TSDBStore = s.TSDBStore
	s.Services = append(s.Services, srv)
}

// admin 管理服务，通过 http 或者浏览器可以管理 influxDB
func (s *Server) appendAdminService(c admin.Config) {
	if !c.Enabled {
		return
	}
	c.Version = s.buildInfo.Version
	srv := admin.NewService(c)
	s.Services = append(s.Services, srv)
}

// httpd 服务，用于提供 RESTFUL 的接口执行写入和查询等操作
func (s *Server) appendHTTPDService(c httpd.Config) {
	if !c.Enabled {
		return
	}
	srv := httpd.NewService(c)
	// 设置 http 处理所使用的一些对象
	srv.Handler.MetaClient = s.MetaClient
	srv.Handler.QueryAuthorizer = meta.NewQueryAuthorizer(s.MetaClient)
	srv.Handler.WriteAuthorizer = meta.NewWriteAuthorizer(s.MetaClient)
	srv.Handler.QueryExecutor = s.QueryExecutor
	srv.Handler.Monitor = s.Monitor
	srv.Handler.PointsWriter = s.PointsWriter
	srv.Handler.Version = s.buildInfo.Version

	// If a ContinuousQuerier service has been started, attach it.
	for _, srvc := range s.Services {
		if cqsrvc, ok := srvc.(continuous_querier.ContinuousQuerier); ok {
			srv.Handler.ContinuousQuerier = cqsrvc
		}
	}

	s.Services = append(s.Services, srv)
}

func (s *Server) appendCollectdService(c collectd.Config) {
	if !c.Enabled {
		return
	}
	srv := collectd.NewService(c)
	srv.MetaClient = s.MetaClient
	srv.PointsWriter = s.PointsWriter
	s.Services = append(s.Services, srv)
}

func (s *Server) appendOpenTSDBService(c opentsdb.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := opentsdb.NewService(c)
	if err != nil {
		return err
	}
	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendGraphiteService(c graphite.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := graphite.NewService(c)
	if err != nil {
		return err
	}

	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.MetaClient
	srv.Monitor = s.Monitor
	s.Services = append(s.Services, srv)
	return nil
}

// shard 服务预创建的服务
func (s *Server) appendPrecreatorService(c precreator.Config) error {
	if !c.Enabled {
		return nil
	}
	srv, err := precreator.NewService(c)
	if err != nil {
		return err
	}

	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
	return nil
}

func (s *Server) appendUDPService(c udp.Config) {
	if !c.Enabled {
		return
	}
	srv := udp.NewService(c)
	srv.PointsWriter = s.PointsWriter
	srv.MetaClient = s.MetaClient
	s.Services = append(s.Services, srv)
}

// CQ 服务，用于执行定期查询，之后写入另外的 metric
func (s *Server) appendContinuousQueryService(c continuous_querier.Config) {
	if !c.Enabled {
		return
	}
	srv := continuous_querier.NewService(c)
	srv.MetaClient = s.MetaClient
	srv.QueryExecutor = s.QueryExecutor
	s.Services = append(s.Services, srv)
}

// Err returns an error channel that multiplexes all out of band errors received from all services.
func (s *Server) Err() <-chan error { return s.err }

// Open opens the meta and data store and all services.
// 启动各种服务，包括底层存储引擎，写入查询，httpd 服务
func (s *Server) Open() error {
	// Start profiling, if set.
	startProfile(s.CPUProfile, s.MemProfile)

	log.Printf("Shared tcp bind address: %v", s.BindAddress)
	// Open shared TCP connection.
	ln, err := net.Listen("tcp", s.BindAddress)
	if err != nil {
		return fmt.Errorf("listen: %s", err)
	}
	s.Listener = ln

	// Multiplex listener.
	// 启动 tcp 连接复用器，所有服务的连接都通过 ln 建立，但是会根据第一个字节的不同分发到不同的处理handler中
	mux := tcp.NewMux()
	go mux.Serve(ln)

	// Append services.
	// 注册监控服务
	s.appendMonitorService()
	// shard 服务预创建的服务
	s.appendPrecreatorService(s.config.Precreator)
	// 注册快照服务
	s.appendSnapshotterService()
	// admin 管理服务，通过 http 或者浏览器可以管理 influxDB
	s.appendAdminService(s.config.Admin)
	// CQ 服务，用于执行定期查询，之后写入另外的 metric
	s.appendContinuousQueryService(s.config.ContinuousQuery)
	// httpd 服务，用于提供 RESTFUL 的接口执行写入和查询等操作
	s.appendHTTPDService(s.config.HTTPD)
	// RP 服务，用于指定 metric 的保存时间
	s.appendRetentionPolicyService(s.config.Retention)

	// 用于监控数据的展示，需要其他组件
	for _, i := range s.config.GraphiteInputs {
		if err := s.appendGraphiteService(i); err != nil {
			return err
		}
	}
	// collectd 数据采集服务，需要其他组件
	for _, i := range s.config.CollectdInputs {
		s.appendCollectdService(i)
	}
	// opentsdb兼容服务
	for _, i := range s.config.OpenTSDBInputs {
		if err := s.appendOpenTSDBService(i); err != nil {
			return err
		}
	}
	// udp接口
	for _, i := range s.config.UDPInputs {
		s.appendUDPService(i)
	}

	// subscriber 用于额外复制一份写入的数据给其他程序使用
	s.Subscriber.MetaClient = s.MetaClient
	s.PointsWriter.MetaClient = s.MetaClient
	s.Monitor.MetaClient = s.MetaClient

	// 注册快照服务的处理接口
	s.SnapshotterService.Listener = mux.Listen(snapshotter.MuxHeader)

	// Configure logging for all services and clients.
	// 设置日志输出配置
	w := s.logOutput
	if s.config.Meta.LoggingEnabled {
		s.MetaClient.SetLogOutput(w)
	}
	s.TSDBStore.SetLogOutput(w)
	if s.config.Data.QueryLogEnabled {
		s.QueryExecutor.SetLogOutput(w)
	}
	s.PointsWriter.SetLogOutput(w)
	s.Subscriber.SetLogOutput(w)
	for _, svc := range s.Services {
		svc.SetLogOutput(w)
	}
	s.SnapshotterService.SetLogOutput(w)
	s.Monitor.SetLogOutput(w)

	// Open TSDB store.
	// 启动存储引擎，创建或者加载已经存在的数据库，构建内存中的索引信息
	if err := s.TSDBStore.Open(); err != nil {
		return fmt.Errorf("open tsdb store: %s", err)
	}

	// Open the subcriber service
	if err := s.Subscriber.Open(); err != nil {
		return fmt.Errorf("open subscriber: %s", err)
	}

	// Open the points writer service
	// 启用数据写入服务
	if err := s.PointsWriter.Open(); err != nil {
		return fmt.Errorf("open points writer: %s", err)
	}

	// 启用其余服务
	for _, service := range s.Services {
		if err := service.Open(); err != nil {
			return fmt.Errorf("open service: %s", err)
		}
	}

	// Start the reporting service, if not disabled.
	if !s.reportingDisabled {
		go s.startServerReporting()
	}

	return nil
}

// Close shuts down the meta and data stores and all services.
func (s *Server) Close() error {
	stopProfile()

	// Close the listener first to stop any new connections
	if s.Listener != nil {
		s.Listener.Close()
	}

	// Close services to allow any inflight requests to complete
	// and prevent new requests from being accepted.
	for _, service := range s.Services {
		service.Close()
	}

	if s.PointsWriter != nil {
		s.PointsWriter.Close()
	}

	if s.QueryExecutor != nil {
		s.QueryExecutor.Close()
	}

	// Close the TSDBStore, no more reads or writes at this point
	if s.TSDBStore != nil {
		s.TSDBStore.Close()
	}

	if s.Subscriber != nil {
		s.Subscriber.Close()
	}

	if s.MetaClient != nil {
		s.MetaClient.Close()
	}

	close(s.closing)
	return nil
}

// startServerReporting starts periodic server reporting.
func (s *Server) startServerReporting() {
	s.reportServer()

	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.reportServer()
		}
	}
}

// reportServer reports usage statistics about the system.
func (s *Server) reportServer() {
	dis := s.MetaClient.Databases()
	numDatabases := len(dis)

	numMeasurements := 0
	numSeries := 0

	// Only needed in the case of a data node
	if s.TSDBStore != nil {
		for _, di := range dis {
			d := s.TSDBStore.DatabaseIndex(di.Name)
			if d == nil {
				// No data in this store for this database.
				continue
			}
			m, s := d.MeasurementSeriesCounts()
			numMeasurements += m
			numSeries += s
		}
	}

	clusterID := s.MetaClient.ClusterID()
	cl := client.New("")
	usage := client.Usage{
		Product: "influxdb",
		Data: []client.UsageData{
			{
				Values: client.Values{
					"os":               runtime.GOOS,
					"arch":             runtime.GOARCH,
					"version":          s.buildInfo.Version,
					"cluster_id":       fmt.Sprintf("%v", clusterID),
					"num_series":       numSeries,
					"num_measurements": numMeasurements,
					"num_databases":    numDatabases,
					"uptime":           time.Since(startTime).Seconds(),
				},
			},
		},
	}

	s.Logger.Printf("Sending usage statistics to usage.influxdata.com")

	go cl.Save(usage)
}

// monitorErrorChan reads an error channel and resends it through the server.
func (s *Server) monitorErrorChan(ch <-chan error) {
	for {
		select {
		case err, ok := <-ch:
			if !ok {
				return
			}
			s.err <- err
		case <-s.closing:
			return
		}
	}
}

// Service represents a service attached to the server.
type Service interface {
	SetLogOutput(w io.Writer)
	Open() error
	Close() error
}

// prof stores the file locations of active profiles.
var prof struct {
	cpu *os.File
	mem *os.File
}

// StartProfile initializes the cpu and memory profile, if specified.
func startProfile(cpuprofile, memprofile string) {
	if cpuprofile != "" {
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatalf("cpuprofile: %v", err)
		}
		log.Printf("writing CPU profile to: %s\n", cpuprofile)
		prof.cpu = f
		pprof.StartCPUProfile(prof.cpu)
	}

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("memprofile: %v", err)
		}
		log.Printf("writing mem profile to: %s\n", memprofile)
		prof.mem = f
		runtime.MemProfileRate = 4096
	}

}

// StopProfile closes the cpu and memory profiles if they are running.
func stopProfile() {
	if prof.cpu != nil {
		pprof.StopCPUProfile()
		prof.cpu.Close()
		log.Println("CPU profile stopped")
	}
	if prof.mem != nil {
		pprof.Lookup("heap").WriteTo(prof.mem, 0)
		prof.mem.Close()
		log.Println("mem profile stopped")
	}
}

type tcpaddr struct{ host string }

func (a *tcpaddr) Network() string { return "tcp" }
func (a *tcpaddr) String() string  { return a.host }

// monitorPointsWriter is a wrapper around `coordinator.PointsWriter` that helps
// to prevent a circular dependency between the `cluster` and `monitor` packages.
type monitorPointsWriter coordinator.PointsWriter

func (pw *monitorPointsWriter) WritePoints(database, retentionPolicy string, points models.Points) error {
	return (*coordinator.PointsWriter)(pw).WritePoints(database, retentionPolicy, models.ConsistencyLevelAny, points)
}

// 0.12.0 版本不再支持集群功能
func raftDBExists(dir string) error {
	// Check to see if there is a raft db, if so, error out with a message
	// to downgrade, export, and then import the meta data
	raftFile := filepath.Join(dir, "raft.db")
	if _, err := os.Stat(raftFile); err == nil {
		return fmt.Errorf("detected %s. To proceed, you'll need to either 1) downgrade to v0.11.x, export your metadata, upgrade to the current version again, and then import the metadata or 2) delete the file, which will effectively reset your database. For more assistance with the upgrade, see: https://docs.influxdata.com/influxdb/v0.12/administration/upgrading/", raftFile)
	}
	return nil
}

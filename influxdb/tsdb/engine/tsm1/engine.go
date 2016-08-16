package tsm1 // import "github.com/influxdata/influxdb/tsdb/engine/tsm1"

import (
	"archive/tar"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

//go:generate tmpl -data=@iterator.gen.go.tmpldata iterator.gen.go.tmpl
//go:generate tmpl -data=@file_store.gen.go.tmpldata file_store.gen.go.tmpl
//go:generate tmpl -data=@encoding.gen.go.tmpldata encoding.gen.go.tmpl

func init() {
	tsdb.RegisterEngine("tsm1", NewEngine)
}

// Ensure Engine implements the interface.
var _ tsdb.Engine = &Engine{}

const (
	// keyFieldSeparator separates the series key from the field name in the composite key
	// that identifies a specific field in series
	// 在一个 key 中的 seriesKey 和 filedName 的分隔符
	keyFieldSeparator = "#!~#"
)

// Statistics gathered by the engine.
const (
	statCacheCompactions              = "cacheCompactions"
	statCacheCompactionError          = "cacheCompactionErr"
	statCacheCompactionDuration       = "cacheCompactionDuration"
	statTSMLevel1Compactions          = "tsmLevel1Compactions"
	statTSMLevel1CompactionDuration   = "tsmLevel1CompactionDuration"
	statTSMLevel2Compactions          = "tsmLevel2Compactions"
	statTSMLevel2CompactionDuration   = "tsmLevel2CompactionDuration"
	statTSMLevel3Compactions          = "tsmLevel3Compactions"
	statTSMLevel3CompactionDuration   = "tsmLevel3CompactionDuration"
	statTSMOptimizeCompactions        = "tsmOptimizeCompactions"
	statTSMOptimizeCompactionDuration = "tsmOptimizeCompactionDuration"
	statTSMFullCompactions            = "tsmFullCompactions"
	statTSMFullCompactionDuration     = "tsmFullCompactionDuration"
)

// Engine represents a storage engine with compressed blocks.
type Engine struct {
	mu                 sync.RWMutex
	done               chan struct{}
	wg                 sync.WaitGroup
	compactionsEnabled bool

	path      string
	logger    *log.Logger
	logOutput io.Writer

	// TODO(benbjohnson): Index needs to be moved entirely into engine.
	index             *tsdb.DatabaseIndex					// 数据库索引信息，目前没和存储引擎放在一起，看起来后续会更改设计作为存储引擎的一部分
	measurementFields map[string]*tsdb.MeasurementFields	// 所有 measurement 对应的 fields 对象

	WAL            *WAL					// WAL 文件对象
	Cache          *Cache				// WAL 文件在内存中的缓存
	Compactor      *Compactor
	CompactionPlan CompactionPlanner
	FileStore      *FileStore			// 数据文件对象

	MaxPointsPerBlock int				// 每个 block 中最多存储的 Points 数量

	// CacheFlushMemorySizeThreshold specifies the minimum size threshodl for
	// the cache when the engine should write a snapshot to a TSM file
	CacheFlushMemorySizeThreshold uint64

	// CacheFlushWriteColdDuration specifies the length of time after which if
	// no writes have been committed to the WAL, the engine will write
	// a snapshot of the cache to a TSM file
	CacheFlushWriteColdDuration time.Duration

	// Controls whether to enabled compactions when the engine is open
	enableCompactionsOnOpen bool

	stats *EngineStatistics
}

// NewEngine returns a new instance of Engine.
// 根据指定的 shard 路径创建一个存储引擎，初始化相关的所有对象，例如 wal,FileStore, cache
func NewEngine(path string, walPath string, opt tsdb.EngineOptions) tsdb.Engine {
	w := NewWAL(walPath)
	w.LoggingEnabled = opt.Config.WALLoggingEnabled

	fs := NewFileStore(path)
	fs.traceLogging = opt.Config.DataLoggingEnabled

	cache := NewCache(uint64(opt.Config.CacheMaxMemorySize), path)

	c := &Compactor{
		Dir:       path,
		FileStore: fs,
	}

	e := &Engine{
		path:              path,
		measurementFields: make(map[string]*tsdb.MeasurementFields),

		WAL:   w,
		Cache: cache,

		FileStore: fs,
		Compactor: c,
		CompactionPlan: &DefaultPlanner{
			FileStore:                    fs,
			CompactFullWriteColdDuration: time.Duration(opt.Config.CompactFullWriteColdDuration),
		},
		MaxPointsPerBlock: opt.Config.MaxPointsPerBlock,

		CacheFlushMemorySizeThreshold: opt.Config.CacheSnapshotMemorySize,
		CacheFlushWriteColdDuration:   time.Duration(opt.Config.CacheSnapshotWriteColdDuration),
		enableCompactionsOnOpen:       true,
		stats: &EngineStatistics{},
	}
	e.SetLogOutput(os.Stderr)

	return e
}

func (e *Engine) SetEnabled(enabled bool) {
	e.enableCompactionsOnOpen = enabled
	e.SetCompactionsEnabled(enabled)
}

// SetCompactionsEnabled enables compactions on the engine.  When disabled
// all running compactions are aborted and new compactions stop running.
// 启用或停止压缩合并功能
// 如果为 true，会创建多个协程去定期检测是否有数据和文件需要压缩合并
func (e *Engine) SetCompactionsEnabled(enabled bool) {
	if enabled {
		e.mu.Lock()
		if e.compactionsEnabled {
			e.mu.Unlock()
			return
		}
		e.compactionsEnabled = true

		e.done = make(chan struct{})
		// 启用压缩合并功能
		e.Compactor.Open()

		e.mu.Unlock()

		// 启用另外的协程去定期检测是否需要进行压缩合并
		e.wg.Add(5)
		// 将 cache 中的内容刷新到磁盘上的新的 tsm 文件中
		go e.compactCache()
		// 下面是定期合并不同 level 的 tsm 文件
		go e.compactTSMFull()
		go e.compactTSMLevel(true, 1)
		go e.compactTSMLevel(true, 2)
		go e.compactTSMLevel(false, 3)
	} else {
		e.mu.Lock()
		if !e.compactionsEnabled {
			e.mu.Unlock()
			return
		}
		// Prevent new compactions from starting
		e.compactionsEnabled = false
		e.mu.Unlock()

		// 关闭处于后台以及正在运行中的压缩协程
		// Stop all background compaction goroutines
		close(e.done)

		// Abort any running goroutines (this could take a while)
		e.Compactor.Close()

		// Wait for compaction goroutines to exit
		e.wg.Wait()
	}
}

// Path returns the path the engine was opened with.
func (e *Engine) Path() string { return e.path }

// Index returns the database index.
func (e *Engine) Index() *tsdb.DatabaseIndex {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.index
}

// MeasurementFields returns the measurement fields for a measurement.
// 返回一个指定的 measurement 的所有 field 对象，如果不存在就创建一个空的
func (e *Engine) MeasurementFields(measurement string) *tsdb.MeasurementFields {
	e.mu.RLock()
	m := e.measurementFields[measurement]
	e.mu.RUnlock()

	if m != nil {
		return m
	}

	e.mu.Lock()
	m = e.measurementFields[measurement]
	if m == nil {
		m = tsdb.NewMeasurementFields()
		e.measurementFields[measurement] = m
	}
	e.mu.Unlock()
	return m
}

// Format returns the format type of this engine
func (e *Engine) Format() tsdb.EngineFormat {
	return tsdb.TSM1Format
}

// EngineStatistics maintains statistics for the engine.
type EngineStatistics struct {
	CacheCompactions              int64
	CacheCompactionErrors         int64
	CacheCompactionDuration       int64
	TSMCompactions                [3]int64
	TSMCompactionErrors           [3]int64
	TSMCompactionDuration         [3]int64
	TSMOptimizeCompactions        int64
	TSMOptimizeCompactionErrors   int64
	TSMOptimizeCompactionDuration int64
	TSMFullCompactions            int64
	TSMFullCompactionErrors       int64
	TSMFullCompactionDuration     int64
}

// Statistics returns statistics for periodic monitoring.
func (e *Engine) Statistics(tags map[string]string) []models.Statistic {
	statistics := make([]models.Statistic, 0, 4)
	statistics = append(statistics, models.Statistic{
		Name: "tsm1_engine",
		Tags: tags,
		Values: map[string]interface{}{
			statCacheCompactions:            atomic.LoadInt64(&e.stats.CacheCompactions),
			statCacheCompactionDuration:     atomic.LoadInt64(&e.stats.CacheCompactionDuration),
			statTSMLevel1Compactions:        atomic.LoadInt64(&e.stats.TSMCompactions[0]),
			statTSMLevel1CompactionDuration: atomic.LoadInt64(&e.stats.TSMCompactionDuration[0]),
			statTSMLevel2Compactions:        atomic.LoadInt64(&e.stats.TSMCompactions[1]),
			statTSMLevel2CompactionDuration: atomic.LoadInt64(&e.stats.TSMCompactionDuration[1]),
			statTSMLevel3Compactions:        atomic.LoadInt64(&e.stats.TSMCompactions[2]),
			statTSMLevel3CompactionDuration: atomic.LoadInt64(&e.stats.TSMCompactionDuration[2]),
			statTSMFullCompactions:          atomic.LoadInt64(&e.stats.TSMFullCompactions),
			statTSMFullCompactionDuration:   atomic.LoadInt64(&e.stats.TSMFullCompactionDuration),
		},
	})
	statistics = append(statistics, e.Cache.Statistics(tags)...)
	statistics = append(statistics, e.FileStore.Statistics(tags)...)
	statistics = append(statistics, e.WAL.Statistics(tags)...)
	return statistics
}

// Open opens and initializes the engine.
// 初始化存储相关的服务，包括 WAL, TSM file, Cache 等对象管理服务
func (e *Engine) Open() error {
	e.done = make(chan struct{})

	// 如果 shard 路径不存在，创建
	if err := os.MkdirAll(e.path, 0777); err != nil {
		return err
	}

	// 清理 shard ，目录下所有以 .tmp 结尾的目录和文件
	if err := e.cleanup(); err != nil {
		return err
	}

	// 打开 wal 文件管理对象
	if err := e.WAL.Open(); err != nil {
		return err
	}

	// 打开数据文件存储对象
	// 创建该 shard 下的所有 tsm 文件的读取对象
	if err := e.FileStore.Open(); err != nil {
		return err
	}

	// 将 wal 文件中的内容全部加载到内存中
	if err := e.reloadCache(); err != nil {
		return err
	}

	if e.enableCompactionsOnOpen {
		e.SetCompactionsEnabled(true)
	}

	return nil
}

// Close closes the engine. Subsequent calls to Close are a nop.
func (e *Engine) Close() error {
	e.SetCompactionsEnabled(false)

	// Lock now and close everything else down.
	e.mu.Lock()
	defer e.mu.Unlock()
	e.done = nil // Ensures that the channel will not be closed again.

	if err := e.FileStore.Close(); err != nil {
		return err
	}
	return e.WAL.Close()
}

// SetLogOutput sets the logger used for all messages. It must not be called
// after the Open method has been called.
func (e *Engine) SetLogOutput(w io.Writer) {
	e.logger = log.New(w, "[tsm1] ", log.LstdFlags)
	e.WAL.SetLogOutput(w)
	e.FileStore.SetLogOutput(w)
	e.logOutput = w
}

// LoadMetadataIndex loads the shard metadata into memory.
// 从指定的 shard 中的每一个文件的索引信息中加载所有 key 的Name，之后解析出 measurement 和 tags 并将其在内存中按照特定的数据结构做一个缓存
// 主要是 map[string]*Measurement 以及 map[string]*Series 这两个对象，加速之后的查询操作，可以快速定位到要查询的数据的位置
func (e *Engine) LoadMetadataIndex(shardID uint64, index *tsdb.DatabaseIndex) error {
	// Save reference to index for iterator creation.
	e.index = index

	// 遍历该 shard 中的每一个 key 执行下面的函数
	if err := e.FileStore.WalkKeys(func(key string, typ byte) error {
		// 获取数据类型
		fieldType, err := tsmFieldTypeToInfluxQLDataType(typ)
		if err != nil {
			return err
		}

		// 解析 key 的内容，拆分出 measurement,series key, field name，将这些信息添加到整个数据库的索引中，主要是 measurement 和 series 的索引
		if err := e.addToIndexFromKey(shardID, key, fieldType, index); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	// load metadata from the Cache
	e.Cache.RLock() // shouldn't need the lock, but just to be safe
	defer e.Cache.RUnlock()

	for key, entry := range e.Cache.Store() {

		fieldType, err := entry.values.InfluxQLType()
		if err != nil {
			e.logger.Printf("error getting the data type of values for key %s: %s", key, err.Error())
			continue
		}

		if err := e.addToIndexFromKey(shardID, key, fieldType, index); err != nil {
			return err
		}
	}

	return nil
}

// Backup will write a tar archive of any TSM files modified since the passed
// in time to the passed in writer. The basePath will be prepended to the names
// of the files in the archive. It will force a snapshot of the WAL first
// then perform the backup with a read lock against the file store. This means
// that new TSM files will not be able to be created in this shard while the
// backup is running. For shards that are still acively getting writes, this
// could cause the WAL to backup, increasing memory usage and evenutally rejecting writes.
func (e *Engine) Backup(w io.Writer, basePath string, since time.Time) error {
	path, err := e.CreateSnapshot()
	if err != nil {
		return err
	}

	// Remove the temporary snapshot dir
	defer os.RemoveAll(path)

	snapDir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer snapDir.Close()

	snapshotFiles, err := snapDir.Readdir(0)
	if err != nil {
		return err
	}

	var files []os.FileInfo
	// grab all the files and tombstones that have a modified time after since
	for _, f := range snapshotFiles {
		if f.ModTime().UnixNano() > since.UnixNano() {
			files = append(files, f)
		}
	}

	if len(files) == 0 {
		return nil
	}

	tw := tar.NewWriter(w)
	defer tw.Close()

	for _, f := range files {
		if err := e.writeFileToBackup(f, basePath, filepath.Join(path, f.Name()), tw); err != nil {
			return err
		}
	}

	return nil
}

// writeFileToBackup will copy the file into the tar archive. Files will use the shardRelativePath
// in their names. This should be the <db>/<retention policy>/<id> part of the path
func (e *Engine) writeFileToBackup(f os.FileInfo, shardRelativePath, fullPath string, tw *tar.Writer) error {
	h := &tar.Header{
		Name:    filepath.Join(shardRelativePath, f.Name()),
		ModTime: f.ModTime(),
		Size:    f.Size(),
		Mode:    int64(f.Mode()),
	}
	if err := tw.WriteHeader(h); err != nil {
		return err
	}
	fr, err := os.Open(fullPath)
	if err != nil {
		return err
	}

	defer fr.Close()

	_, err = io.CopyN(tw, fr, h.Size)

	return err
}

// Restore will read a tar archive generated by Backup().
// Only files that match basePath will be copied into the directory. This obtains
// a write lock so no operations can be performed while restoring.
func (e *Engine) Restore(r io.Reader, basePath string) error {
	// Copy files from archive while under lock to prevent reopening.
	if err := func() error {
		e.mu.Lock()
		defer e.mu.Unlock()

		tr := tar.NewReader(r)
		for {
			if err := e.readFileFromBackup(tr, basePath); err == io.EOF {
				break
			} else if err != nil {
				return err
			}
		}

		return syncDir(e.path)
	}(); err != nil {
		return err
	}

	return nil
}

// readFileFromBackup copies the next file from the archive into the shard.
// The file is skipped if it does not have a matching shardRelativePath prefix.
func (e *Engine) readFileFromBackup(tr *tar.Reader, shardRelativePath string) error {
	// Read next archive file.
	hdr, err := tr.Next()
	if err != nil {
		return err
	}

	// Skip file if it does not have a matching prefix.
	if !filepath.HasPrefix(hdr.Name, shardRelativePath) {
		return nil
	}
	path, err := filepath.Rel(shardRelativePath, hdr.Name)
	if err != nil {
		return err
	}

	destPath := filepath.Join(e.path, path)
	tmp := destPath + ".tmp"

	// Create new file on disk.
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	// Copy from archive to the file.
	if _, err := io.CopyN(f, tr, hdr.Size); err != nil {
		return err
	}

	// Sync to disk & close.
	if err := f.Sync(); err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return err
	}

	return renameFile(tmp, destPath)
}

// addToIndexFromKey will pull the measurement name, series key, and field name from a composite key and add it to the
// database index and measurement fields
// 解析 key 的内容，拆分出 measurement,series key, field name，将这些信息添加到整个数据库的索引中，主要是 measurement 和 series 的索引
func (e *Engine) addToIndexFromKey(shardID uint64, key string, fieldType influxql.DataType, index *tsdb.DatabaseIndex) error {
	// 从 key 中解析出 series key 以及 field Name
	seriesKey, field := seriesAndFieldFromCompositeKey(key)
	// 从 series key 中解析出 measurement
	measurement := tsdb.MeasurementFromSeriesKey(seriesKey)

	// 如果 measurement 的信息不存在，就创建一个
	m := index.CreateMeasurementIndexIfNotExists(measurement)
	m.SetFieldName(field)

	// measurement 对应的 field 对象，如果不存在，就新建
	mf := e.measurementFields[measurement]
	if mf == nil {
		mf = tsdb.NewMeasurementFields()
		e.measurementFields[measurement] = mf
	}

	// 将 field 信息添加到索引中
	if err := mf.CreateFieldIfNotExists(field, fieldType, false); err != nil {
		return err
	}

	// Have we already indexed this series?
	ss := index.Series(seriesKey)
	if ss != nil {
		// Add this shard to the existing series
		// 如果这个 seriesKey 已经被索引，分配到一个指定的 shardID 后返回
		ss.AssignShard(shardID)
		return nil
	}

	// ignore error because ParseKey returns "missing fields" and we don't have
	// fields (in line protocol format) in the series key
	// 如果这个 seriesKey 还没有被索引，创建一个新的
	_, tags, _ := models.ParseKey(seriesKey)

	s := tsdb.NewSeries(seriesKey, tags)
	// 将 series 信息加入到内存索引中
	index.CreateSeriesIndexIfNotExists(measurement, s)
	s.AssignShard(shardID)

	return nil
}

// WritePoints writes metadata and point data into the engine.
// Returns an error if new points are added to an existing key.
// 向 memtable 以及 wal 文件中写入数据
func (e *Engine) WritePoints(points []models.Point) error {
	values := map[string][]Value{}
	for _, p := range points {
		// 一条插入语句中一个 series 对应的多个 value 会被拆分出来，形成多条数据
		for k, v := range p.Fields() {
			// 这里的 key 是 seriesKey + 分隔符 + filedName
			key := string(p.Key()) + keyFieldSeparator + k
			values[key] = append(values[key], NewValue(p.Time().UnixNano(), v))
		}
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// first try to write to the cache
	// 向 memtable 中写入 value 数据，如果超过了内存阀值上限，返回错误
	err := e.Cache.WriteMulti(values)
	if err != nil {
		return err
	}

	// 将数据写入 wal 文件中
	_, err = e.WAL.WritePoints(values)
	return err
}

// ContainsSeries returns a map of keys indicating whether the key exists and
// has values or not.
func (e *Engine) ContainsSeries(keys []string) (map[string]bool, error) {
	// keyMap is used to see if a given key exists.  keys
	// are the measurement + tagset (minus separate & field)
	keyMap := map[string]bool{}
	for _, k := range keys {
		keyMap[k] = false
	}

	for _, k := range e.Cache.Keys() {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		keyMap[seriesKey] = true
	}

	if err := e.FileStore.WalkKeys(func(k string, _ byte) error {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		if _, ok := keyMap[seriesKey]; ok {
			keyMap[seriesKey] = true
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return keyMap, nil
}

// DeleteSeries removes all series keys from the engine.
func (e *Engine) DeleteSeries(seriesKeys []string) error {
	return e.DeleteSeriesRange(seriesKeys, math.MinInt64, math.MaxInt64)
}

// DeleteSeriesRange removes the values between min and max (inclusive) from all series.
func (e *Engine) DeleteSeriesRange(seriesKeys []string, min, max int64) error {
	if len(seriesKeys) == 0 {
		return nil
	}

	// Disable and abort running compactions so that tombstones added existing tsm
	// files don't get removed.  This would cause deleted measurements/series to
	// re-appear once the compaction completed.
	e.SetCompactionsEnabled(false)
	defer e.SetCompactionsEnabled(true)

	e.mu.RLock()
	defer e.mu.RUnlock()

	// keyMap is used to see if a given key should be deleted.  seriesKey
	// are the measurement + tagset (minus separate & field)
	keyMap := map[string]struct{}{}
	for _, k := range seriesKeys {
		keyMap[k] = struct{}{}
	}

	var deleteKeys []string
	// go through the keys in the file store
	if err := e.FileStore.WalkKeys(func(k string, _ byte) error {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		if _, ok := keyMap[seriesKey]; ok {
			deleteKeys = append(deleteKeys, k)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := e.FileStore.DeleteRange(deleteKeys, min, max); err != nil {
		return err
	}

	// find the keys in the cache and remove them
	walKeys := make([]string, 0)
	e.Cache.RLock()
	s := e.Cache.Store()
	for k, _ := range s {
		seriesKey, _ := seriesAndFieldFromCompositeKey(k)
		if _, ok := keyMap[seriesKey]; ok {
			walKeys = append(walKeys, k)
		}
	}
	e.Cache.RUnlock()

	e.Cache.DeleteRange(walKeys, min, max)

	// delete from the WAL
	_, err := e.WAL.DeleteRange(walKeys, min, max)

	return err
}

// DeleteMeasurement deletes a measurement and all related series.
func (e *Engine) DeleteMeasurement(name string, seriesKeys []string) error {
	e.mu.Lock()
	delete(e.measurementFields, name)
	e.mu.Unlock()

	return e.DeleteSeries(seriesKeys)
}

// SeriesCount returns the number of series buckets on the shard.
func (e *Engine) SeriesCount() (n int, err error) {
	return 0, nil
}

func (e *Engine) WriteTo(w io.Writer) (n int64, err error) { panic("not implemented") }

// WriteSnapshot will snapshot the cache and write a new TSM file with its contents, releasing the snapshot when done.
// 将 cache 中的内容保存为一个快照，之后写入到一个新的 tsm 文件中，完成后将快照释放
func (e *Engine) WriteSnapshot() error {
	// Lock and grab the cache snapshot along with all the closed WAL
	// filenames associated with the snapshot

	var started *time.Time

	defer func() {
		if started != nil {
			e.Cache.UpdateCompactTime(time.Now().Sub(*started))
		}
	}()

	closedFiles, snapshot, err := func() ([]string, *Cache, error) {
		e.mu.Lock()
		defer e.mu.Unlock()

		now := time.Now()
		started = &now

		// 关闭当前的 wal 分片文件，并且创建一个新的
		if err := e.WAL.CloseSegment(); err != nil {
			return nil, nil, err
		}

		// 获取被关闭了的分片文件，当前正在写入的不算在内
		segments, err := e.WAL.ClosedSegments()
		if err != nil {
			return nil, nil, err
		}

		// 获取当前 cache 的快照内容
		snapshot, err := e.Cache.Snapshot()
		if err != nil {
			return nil, nil, err
		}

		return segments, snapshot, nil
	}()

	if err != nil {
		return err
	}

	// The snapshotted cache may have duplicate points and unsorted data.  We need to deduplicate
	// it before writing the snapshot.  This can be very expensive so it's done while we are not
	// holding the engine write lock.
	// 将 cache 的快照去重以及排序
	snapshot.Deduplicate()

	return e.writeSnapshotAndCommit(closedFiles, snapshot)
}

// CreateSnapshot will create a temp directory that holds
// temporary hardlinks to the underylyng shard files
// 创建硬链接
func (e *Engine) CreateSnapshot() (string, error) {
	if err := e.WriteSnapshot(); err != nil {
		return "", err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.FileStore.CreateSnapshot()
}

// writeSnapshotAndCommit will write the passed cache to a new TSM file and remove the closed WAL segments
// 将 cache 的快照内容写入新的 tsm 文件，并且将关闭了的 wal 分片文件关闭
func (e *Engine) writeSnapshotAndCommit(closedFiles []string, snapshot *Cache) (err error) {
	// 最后将快照清空
	defer func() {
		if err != nil {
			e.Cache.ClearSnapshot(false)
		}
	}()
	// write the new snapshot files
	// 通过 compactor 对象
	// 将 cache 的快照写入一个或多个新的 tsm 文件
	// 返回文件名，仍然是 .tmp 结尾
	newFiles, err := e.Compactor.WriteSnapshot(snapshot)
	if err != nil {
		e.logger.Printf("error writing snapshot from compactor: %v", err)
		return err
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	// update the file store with these new files
	// 创建 newFiles 中的文件，重命名，将之后的 .tmp 去掉
	// 之后 oldFiles 中的文件移除，这部分文件已经被合并过了
	// 这里 oldFiles 为 nil，不对之前的文件进行任何操作
	if err := e.FileStore.Replace(nil, newFiles); err != nil {
		e.logger.Printf("error adding new TSM files from snapshot: %v", err)
		return err
	}

	// clear the snapshot from the in-memory cache, then the old WAL files
	e.Cache.ClearSnapshot(true)

	if err := e.WAL.Remove(closedFiles); err != nil {
		e.logger.Printf("error removing closed wal segments: %v", err)
	}

	return nil
}

// compactCache continually checks if the WAL cache should be written to disk
// 持续检查 cache 中的内容是否需要刷新到磁盘
func (e *Engine) compactCache() {
	defer e.wg.Done()
	// 每秒钟检查一次
	for {
		select {
		case <-e.done:
			return

		default:
			// 更新缓存据上一次快照时间的间隔时间
			e.Cache.UpdateAge()
			if e.ShouldCompactCache(e.WAL.LastWriteTime()) {
				start := time.Now()
				err := e.WriteSnapshot()
				if err != nil && err != errCompactionsDisabled {
					e.logger.Printf("error writing snapshot: %v", err)
					atomic.AddInt64(&e.stats.CacheCompactionErrors, 1)
				} else {
					atomic.AddInt64(&e.stats.CacheCompactions, 1)
				}
				atomic.AddInt64(&e.stats.CacheCompactionDuration, time.Since(start).Nanoseconds())
			}
		}
		time.Sleep(time.Second)
	}
}

// ShouldCompactCache returns true if the Cache is over its flush threshold
// or if the passed in lastWriteTime is older than the write cold threshold
// 根据两个条件来判断，一个是大小达到阀值上限
// 或者超过一个指定时间内还没有数据写入
func (e *Engine) ShouldCompactCache(lastWriteTime time.Time) bool {
	sz := e.Cache.Size()

	if sz == 0 {
		return false
	}

	return sz > e.CacheFlushMemorySizeThreshold ||
		time.Now().Sub(lastWriteTime) > e.CacheFlushWriteColdDuration
}

// 根据级别进行压缩
func (e *Engine) compactTSMLevel(fast bool, level int) {
	defer e.wg.Done()

	for {
		select {
		case <-e.done:
			return

		default:
			// 返回一个指定级别的合并压缩所需要操作的所有 tsm 文件
			tsmFiles := e.CompactionPlan.PlanLevel(level)

			// 如果没有需要压缩的 tsm file group，休眠1s后继续
			if len(tsmFiles) == 0 {
				time.Sleep(time.Second)
				continue
			}

			// Keep track of the start time for statistics.
			start := time.Now()

			var wg sync.WaitGroup
			for i, group := range tsmFiles {
				wg.Add(1)
				go func(groupNum int, group CompactionGroup) {
					// 对文件进行压缩合并
					defer wg.Done()
					start := time.Now()
					e.logger.Printf("beginning level %d compaction of group %d, %d TSM files", level, groupNum, len(group))
					for i, f := range group {
						e.logger.Printf("compacting level %d group (%d) %s (#%d)", level, groupNum, f, i)
					}

					var files []string
					var err error

					if fast {
						// 压缩合并多个 tsm 文件
						files, err = e.Compactor.CompactFast(group)
						if err != nil && err != errCompactionsDisabled {
							e.logger.Printf("error compacting TSM files: %v", err)
							atomic.AddInt64(&e.stats.TSMCompactionErrors[level-1], 1)
							time.Sleep(time.Second)
							return
						}
					} else {
						files, err = e.Compactor.CompactFull(group)
						if err != nil && err != errCompactionsDisabled {
							e.logger.Printf("error compacting TSM files: %v", err)
							atomic.AddInt64(&e.stats.TSMCompactionErrors[level-1], 1)
							time.Sleep(time.Second)
							return
						}
					}

					// 删除旧的文件，将 .tmp 文件重命名为新文件
					if err := e.FileStore.Replace(group, files); err != nil {
						e.logger.Printf("error replacing new TSM files: %v", err)
						atomic.AddInt64(&e.stats.TSMCompactionErrors[level-1], 1)
						time.Sleep(time.Second)
						return
					}

					for i, f := range files {
						e.logger.Printf("compacted level %d group (%d) into %s (#%d)", level, groupNum, f, i)
					}
					atomic.AddInt64(&e.stats.TSMCompactions[level-1], 1)
					e.logger.Printf("compacted level %d group %d of %d files into %d files in %s",
						level, groupNum, len(group), len(files), time.Since(start))
				}(i, group)
			}
			wg.Wait()

			// Track the amount of time spent compacting the groups.
			atomic.AddInt64(&e.stats.TSMCompactionDuration[level-1], time.Since(start).Nanoseconds())
		}
	}
}

func (e *Engine) compactTSMFull() {
	defer e.wg.Done()

	for {
		select {
		case <-e.done:
			return

		default:
			optimize := false
			logDesc := "full"
			tsmFiles := e.CompactionPlan.Plan(e.WAL.LastWriteTime())

			if len(tsmFiles) == 0 {
				optimize = true
				logDesc = "optimize"
				tsmFiles = e.CompactionPlan.PlanOptimize()
			}

			if len(tsmFiles) == 0 {
				time.Sleep(time.Second)
				continue
			}

			// Keep track of the start time for statistics.
			start := time.Now()

			var wg sync.WaitGroup
			for i, group := range tsmFiles {
				wg.Add(1)
				go func(groupNum int, group CompactionGroup) {
					defer wg.Done()
					start := time.Now()
					e.logger.Printf("beginning %s compaction of group %d, %d TSM files", logDesc, groupNum, len(group))
					for i, f := range group {
						e.logger.Printf("compacting %s group (%d) %s (#%d)", logDesc, groupNum, f, i)
					}

					var (
						files []string
						err   error
					)
					if optimize {
						files, err = e.Compactor.CompactFast(group)
						if err != nil && err != errCompactionsDisabled {
							e.logger.Printf("error compacting TSM files: %v", err)
							atomic.AddInt64(&e.stats.TSMOptimizeCompactionErrors, 1)

							time.Sleep(time.Second)
							return
						}
					} else {
						files, err = e.Compactor.CompactFull(group)
						if err != nil && err != errCompactionsDisabled {
							e.logger.Printf("error compacting TSM files: %v", err)
							atomic.AddInt64(&e.stats.TSMFullCompactionErrors, 1)

							time.Sleep(time.Second)
							return
						}
					}

					if err := e.FileStore.Replace(group, files); err != nil {
						e.logger.Printf("error replacing new TSM files: %v", err)
						atomic.AddInt64(&e.stats.TSMFullCompactionErrors, 1)
						time.Sleep(time.Second)
						return
					}

					for i, f := range files {
						e.logger.Printf("compacted %s group (%d) into %s (#%d)", logDesc, groupNum, f, i)
					}

					if optimize {
						atomic.AddInt64(&e.stats.TSMOptimizeCompactions, 1)
					} else {
						atomic.AddInt64(&e.stats.TSMFullCompactions, 1)
					}
					e.logger.Printf("compacted %s %d files into %d files in %s",
						logDesc, len(group), len(files), time.Since(start))
				}(i, group)
			}
			wg.Wait()

			// Track the amount of time spent compacting the groups.
			if optimize {
				atomic.AddInt64(&e.stats.TSMOptimizeCompactionDuration, time.Since(start).Nanoseconds())
			} else {
				atomic.AddInt64(&e.stats.TSMFullCompactionDuration, time.Since(start).Nanoseconds())
			}

		}
	}
}

// reloadCache reads the WAL segment files and loads them into the cache.
// 将 wal 文件中的内容全部加载到内存中
func (e *Engine) reloadCache() error {
	files, err := segmentFileNames(e.WAL.Path())
	if err != nil {
		return err
	}

	// 加载结束后设置成实际的 cache 最大值
	limit := e.Cache.MaxSize()
	defer func() {
		e.Cache.SetMaxSize(limit)
	}()

	// Disable the max size during loading
	// 加载过程中临时将最大值限制关闭
	e.Cache.SetMaxSize(0)

	// 创建 WAL 文件缓存对象
	loader := NewCacheLoader(files)
	loader.SetLogOutput(e.logOutput)
	// 从 wal 文件中加载日志记录并且更新到 Cache 对象中
	if err := loader.Load(e.Cache); err != nil {
		return err
	}

	return nil
}

// 清理所有以 .tmp 结尾的目录和文件
func (e *Engine) cleanup() error {
	allfiles, err := ioutil.ReadDir(e.path)
	if err != nil {
		return err
	}

	// 遍历所有 tsm 文件
	for _, f := range allfiles {
		// Check to see if there are any `.tmp` directories that were left over from failed shard snapshots
		// 清除以 .tmp 结尾的目录，可能是失败的快照操作
		if f.IsDir() && strings.HasSuffix(f.Name(), ".tmp") {
			if err := os.RemoveAll(filepath.Join(e.path, f.Name())); err != nil {
				return fmt.Errorf("error removing tmp snapshot directory %q: %s", f.Name(), err)
			}
		}
	}

	// CompactionTempExtension = "tmp"
	// 这里获取所有 .tmp 结尾的文件
	files, err := filepath.Glob(filepath.Join(e.path, fmt.Sprintf("*.%s", CompactionTempExtension)))
	if err != nil {
		return fmt.Errorf("error getting compaction temp files: %s", err.Error())
	}

	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return fmt.Errorf("error removing temp compaction files: %v", err)
		}
	}

	return nil
}

func (e *Engine) KeyCursor(key string, t int64, ascending bool) *KeyCursor {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.FileStore.KeyCursor(key, t, ascending)
}

// 根据查询选项创建一个相应的迭代器，只作用于当前存储引擎，tsm1 中每一个 shard 对应一个底层的存储引擎
func (e *Engine) CreateIterator(opt influxql.IteratorOptions) (influxql.Iterator, error) {
	fmt.Printf("engine CreateIterator\n")
	// 如果是 call 表达式
	if call, ok := opt.Expr.(*influxql.Call); ok {
		refOpt := opt
		refOpt.Expr = call.Args[0].(*influxql.VarRef)
		inputs, err := e.createVarRefIterator(refOpt, true)
		fmt.Printf("createVarRefIterator 1\n")
		if err != nil {
			return nil, err
		} else if len(inputs) == 0 {
			return nil, nil
		}

		// Wrap each series in a call iterator.
		// 每一个 series 创建一个 Interrupt 迭代器以及 Call 迭代器
		for i, input := range inputs {
			if opt.InterruptCh != nil {
				input = influxql.NewInterruptIterator(input, opt.InterruptCh)
				fmt.Printf("NewInterruptIterator 1\n")
			}

			itr, err := influxql.NewCallIterator(input, opt)
			fmt.Printf("NewCallIterator 1\n")
			if err != nil {
				return nil, err
			}
			inputs[i] = itr
		}

		fmt.Printf("NewParallelMergeIterator 1\n")
		// 并行合并迭代器
		return influxql.NewParallelMergeIterator(inputs, opt, runtime.GOMAXPROCS(0)), nil
	}

	// 如果是普通数值型表达式的迭代器
	itrs, err := e.createVarRefIterator(opt, false)
	fmt.Printf("createVarRefIterator 2\n")
	if err != nil {
		return nil, err
	}

	// 排序合并迭代器
	itr := influxql.NewSortedMergeIterator(itrs, opt)
	fmt.Printf("NewSortedMergeIterator 2\n")
	if itr != nil && opt.InterruptCh != nil {
		itr = influxql.NewInterruptIterator(itr, opt.InterruptCh)
	}
	return itr, nil
}

// createVarRefIterator creates an iterator for a variable reference.
// The aggregate argument determines this is being created for an aggregate.
// If this is an aggregate, the limit optimization is disabled temporarily. See #6661.
// 数据迭代器，会根据 measurements 以及 tagsSet 创建对应的 TagSetIterators
// aggregate 表示对后续数据是否进行聚合计算
func (e *Engine) createVarRefIterator(opt influxql.IteratorOptions, aggregate bool) ([]influxql.Iterator, error) {
	ref, _ := opt.Expr.(*influxql.VarRef)

	var itrs []influxql.Iterator
	if err := func() error {
		// 获取相关的 measurements 数据
		mms := tsdb.Measurements(e.index.MeasurementsByName(influxql.Sources(opt.Sources).Names()))

		for _, mm := range mms {
			// Determine tagsets for this measurement based on dimensions and filters.
			// 过滤出符合要求的 tags
			tagSets, err := mm.TagSets(opt.Dimensions, opt.Condition)
			if err != nil {
				return err
			}

			// Calculate tag sets and apply SLIMIT/SOFFSET.
			// 如果有 SLIMIT/SOFFSET，进行计算，保留部分
			tagSets = influxql.LimitTagSets(tagSets, opt.SLimit, opt.SOffset)

			// 为每一个 tagSet 创建一个迭代器
			for _, t := range tagSets {
				inputs, err := e.createTagSetIterators(ref, mm, t, opt)
				if err != nil {
					return err
				}

				if !aggregate && len(inputs) > 0 && (opt.Limit > 0 || opt.Offset > 0) {
					// 如果数据获取迭代器超过一个，需要用一个 SortedMergeIterator 将其包裹起来
					itrs = append(itrs, newLimitIterator(influxql.NewSortedMergeIterator(inputs, opt), opt))
				} else {
					itrs = append(itrs, inputs...)
				}
			}
		}
		return nil
	}(); err != nil {
		influxql.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createTagSetIterators creates a set of iterators for a tagset.
// 底层是创建一批 TagSetGroupIterators
func (e *Engine) createTagSetIterators(ref *influxql.VarRef, mm *tsdb.Measurement, t *influxql.TagSet, opt influxql.IteratorOptions) ([]influxql.Iterator, error) {
	// Set parallelism by number of logical cpus.
	parallelism := runtime.GOMAXPROCS(0)
	if parallelism > len(t.SeriesKeys) {
		parallelism = len(t.SeriesKeys)
	}

	// Create series key groupings w/ return error.
	groups := make([]struct {
		keys    []string
		filters []influxql.Expr
		itrs    []influxql.Iterator
		err     error
	}, parallelism)

	// Group series keys.
	n := len(t.SeriesKeys) / parallelism
	for i := 0; i < parallelism; i++ {
		group := &groups[i]

		if i < parallelism-1 {
			group.keys = t.SeriesKeys[i*n : (i+1)*n]
			group.filters = t.Filters[i*n : (i+1)*n]
		} else {
			group.keys = t.SeriesKeys[i*n:]
			group.filters = t.Filters[i*n:]
		}

		group.itrs = make([]influxql.Iterator, 0, len(group.keys))
	}

	// Read series groups in parallel.
	var wg sync.WaitGroup
	for i := range groups {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 创建 TagSetGroupIterators
			groups[i].itrs, groups[i].err = e.createTagSetGroupIterators(ref, mm, groups[i].keys, t, groups[i].filters, opt)
		}(i)
	}
	wg.Wait()

	// Determine total number of iterators so we can allocate only once.
	var itrN int
	for _, group := range groups {
		itrN += len(group.itrs)
	}

	// Combine all iterators together and check for errors.
	var err error
	itrs := make([]influxql.Iterator, 0, itrN)
	for _, group := range groups {
		if group.err != nil {
			err = group.err
		}
		itrs = append(itrs, group.itrs...)
	}

	// If an error occurred, make sure we close all created iterators.
	if err != nil {
		influxql.Iterators(itrs).Close()
		return nil, err
	}

	return itrs, nil
}

// createTagSetGroupIterators creates a set of iterators for a subset of a tagset's series.
func (e *Engine) createTagSetGroupIterators(ref *influxql.VarRef, mm *tsdb.Measurement, seriesKeys []string, t *influxql.TagSet, filters []influxql.Expr, opt influxql.IteratorOptions) ([]influxql.Iterator, error) {
	conditionFields := make([]influxql.VarRef, len(influxql.ExprNames(opt.Condition)))

	itrs := make([]influxql.Iterator, 0, len(seriesKeys))
	for i, seriesKey := range seriesKeys {
		fields := 0
		if filters[i] != nil {
			// Retrieve non-time fields from this series filter and filter out tags.
			for _, f := range influxql.ExprNames(filters[i]) {
				conditionFields[fields] = f
				fields++
			}
		}

		itr, err := e.createVarRefSeriesIterator(ref, mm, seriesKey, t, filters[i], conditionFields[:fields], opt)
		if err != nil {
			return itrs, err
		} else if itr == nil {
			continue
		}
		itrs = append(itrs, itr)
	}
	return itrs, nil
}

// createVarRefSeriesIterator creates an iterator for a variable reference for a series.
func (e *Engine) createVarRefSeriesIterator(ref *influxql.VarRef, mm *tsdb.Measurement, seriesKey string, t *influxql.TagSet, filter influxql.Expr, conditionFields []influxql.VarRef, opt influxql.IteratorOptions) (influxql.Iterator, error) {
	tags := influxql.NewTags(e.index.TagsForSeries(seriesKey))

	// Create options specific for this series.
	itrOpt := opt
	itrOpt.Condition = filter

	// Build auxilary cursors.
	// Tag values should be returned if the field doesn't exist.
	var aux []cursorAt
	if len(opt.Aux) > 0 {
		aux = make([]cursorAt, len(opt.Aux))
		for i, ref := range opt.Aux {
			// Create cursor from field if a tag wasn't requested.
			if ref.Type != influxql.Tag {
				cur := e.buildCursor(mm.Name, seriesKey, &ref, opt)
				if cur != nil {
					aux[i] = newBufCursor(cur, opt.Ascending)
					continue
				}

				// If a field was requested, use a nil cursor of the requested type.
				switch ref.Type {
				case influxql.Float, influxql.AnyField:
					aux[i] = &floatNilLiteralCursor{}
					continue
				case influxql.Integer:
					aux[i] = &integerNilLiteralCursor{}
					continue
				case influxql.String:
					aux[i] = &stringNilLiteralCursor{}
					continue
				case influxql.Boolean:
					aux[i] = &booleanNilLiteralCursor{}
					continue
				}
			}

			// If field doesn't exist, use the tag value.
			if v := tags.Value(ref.Val); v == "" {
				// However, if the tag value is blank then return a null.
				aux[i] = &stringNilLiteralCursor{}
			} else {
				aux[i] = &stringLiteralCursor{value: v}
			}
		}
	}

	// Build conditional field cursors.
	// If a conditional field doesn't exist then ignore the series.
	var conds []cursorAt
	if len(conditionFields) > 0 {
		conds = make([]cursorAt, len(conditionFields))
		for i, ref := range conditionFields {
			// Create cursor from field if a tag wasn't requested.
			if ref.Type != influxql.Tag {
				cur := e.buildCursor(mm.Name, seriesKey, &ref, opt)
				if cur != nil {
					conds[i] = newBufCursor(cur, opt.Ascending)
					continue
				}

				// If a field was requested, use a nil cursor of the requested type.
				switch ref.Type {
				case influxql.Float, influxql.AnyField:
					conds[i] = &floatNilLiteralCursor{}
					continue
				case influxql.Integer:
					conds[i] = &integerNilLiteralCursor{}
					continue
				case influxql.String:
					conds[i] = &stringNilLiteralCursor{}
					continue
				case influxql.Boolean:
					conds[i] = &booleanNilLiteralCursor{}
					continue
				}
			}

			// If field doesn't exist, use the tag value.
			if v := tags.Value(ref.Val); v == "" {
				// However, if the tag value is blank then return a null.
				conds[i] = &stringNilLiteralCursor{}
			} else {
				conds[i] = &stringLiteralCursor{value: v}
			}
		}
	}
	condNames := influxql.VarRefs(conditionFields).Strings()

	// Limit tags to only the dimensions selected.
	tags = tags.Subset(opt.Dimensions)

	// If it's only auxiliary fields then it doesn't matter what type of iterator we use.
	if ref == nil {
		return newFloatIterator(mm.Name, tags, itrOpt, nil, aux, conds, condNames), nil
	}

	// Build main cursor.
	cur := e.buildCursor(mm.Name, seriesKey, ref, opt)

	// If the field doesn't exist then don't build an iterator.
	if cur == nil {
		return nil, nil
	}

	switch cur := cur.(type) {
	case floatCursor:
		return newFloatIterator(mm.Name, tags, itrOpt, cur, aux, conds, condNames), nil
	case integerCursor:
		return newIntegerIterator(mm.Name, tags, itrOpt, cur, aux, conds, condNames), nil
	case stringCursor:
		return newStringIterator(mm.Name, tags, itrOpt, cur, aux, conds, condNames), nil
	case booleanCursor:
		return newBooleanIterator(mm.Name, tags, itrOpt, cur, aux, conds, condNames), nil
	default:
		panic("unreachable")
	}
}

// buildCursor creates an untyped cursor for a field.
func (e *Engine) buildCursor(measurement, seriesKey string, ref *influxql.VarRef, opt influxql.IteratorOptions) cursor {
	// Look up fields for measurement.
	e.mu.RLock()
	mf := e.measurementFields[measurement]
	e.mu.RUnlock()

	if mf == nil {
		return nil
	}

	// Find individual field.
	f := mf.Field(ref.Val)
	if f == nil {
		return nil
	}

	// Check if we need to perform a cast. Performing a cast in the
	// engine (if it is possible) is much more efficient than an automatic cast.
	if ref.Type != influxql.Unknown && ref.Type != influxql.AnyField && ref.Type != f.Type {
		switch ref.Type {
		case influxql.Float:
			switch f.Type {
			case influxql.Integer:
				cur := e.buildIntegerCursor(measurement, seriesKey, ref.Val, opt)
				return &floatCastIntegerCursor{cursor: cur}
			}
		case influxql.Integer:
			switch f.Type {
			case influxql.Float:
				cur := e.buildFloatCursor(measurement, seriesKey, ref.Val, opt)
				return &integerCastFloatCursor{cursor: cur}
			}
		}
		return nil
	}

	// Return appropriate cursor based on type.
	switch f.Type {
	case influxql.Float:
		return e.buildFloatCursor(measurement, seriesKey, ref.Val, opt)
	case influxql.Integer:
		return e.buildIntegerCursor(measurement, seriesKey, ref.Val, opt)
	case influxql.String:
		return e.buildStringCursor(measurement, seriesKey, ref.Val, opt)
	case influxql.Boolean:
		return e.buildBooleanCursor(measurement, seriesKey, ref.Val, opt)
	default:
		panic("unreachable")
	}
}

// buildFloatCursor creates a cursor for a float field.
func (e *Engine) buildFloatCursor(measurement, seriesKey, field string, opt influxql.IteratorOptions) floatCursor {
	cacheValues := e.Cache.Values(SeriesFieldKey(seriesKey, field))
	keyCursor := e.KeyCursor(SeriesFieldKey(seriesKey, field), opt.SeekTime(), opt.Ascending)
	return newFloatCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// buildIntegerCursor creates a cursor for an integer field.
func (e *Engine) buildIntegerCursor(measurement, seriesKey, field string, opt influxql.IteratorOptions) integerCursor {
	cacheValues := e.Cache.Values(SeriesFieldKey(seriesKey, field))
	keyCursor := e.KeyCursor(SeriesFieldKey(seriesKey, field), opt.SeekTime(), opt.Ascending)
	return newIntegerCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// buildStringCursor creates a cursor for a string field.
func (e *Engine) buildStringCursor(measurement, seriesKey, field string, opt influxql.IteratorOptions) stringCursor {
	cacheValues := e.Cache.Values(SeriesFieldKey(seriesKey, field))
	keyCursor := e.KeyCursor(SeriesFieldKey(seriesKey, field), opt.SeekTime(), opt.Ascending)
	return newStringCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// buildBooleanCursor creates a cursor for a boolean field.
func (e *Engine) buildBooleanCursor(measurement, seriesKey, field string, opt influxql.IteratorOptions) booleanCursor {
	cacheValues := e.Cache.Values(SeriesFieldKey(seriesKey, field))
	keyCursor := e.KeyCursor(SeriesFieldKey(seriesKey, field), opt.SeekTime(), opt.Ascending)
	return newBooleanCursor(opt.SeekTime(), opt.Ascending, cacheValues, keyCursor)
}

// SeriesFieldKey combine a series key and field name for a unique string to be hashed to a numeric ID
func SeriesFieldKey(seriesKey, field string) string {
	return seriesKey + keyFieldSeparator + field
}

// tsm 数据类型转换成 InfluxQL 的数据类型
func tsmFieldTypeToInfluxQLDataType(typ byte) (influxql.DataType, error) {
	switch typ {
	case BlockFloat64:
		return influxql.Float, nil
	case BlockInteger:
		return influxql.Integer, nil
	case BlockBoolean:
		return influxql.Boolean, nil
	case BlockString:
		return influxql.String, nil
	default:
		return influxql.Unknown, fmt.Errorf("unknown block type: %v", typ)
	}
}

// 从 key 中解析出 series key 以及 field Name
func seriesAndFieldFromCompositeKey(key string) (string, string) {
	// series key 和 filedName 在 key 中以固定分隔符分开
	sep := strings.Index(key, keyFieldSeparator)
	if sep == -1 {
		// No field???
		return key, ""
	}
	return key[:sep], key[sep+len(keyFieldSeparator):]
}
package influxql

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/influxdata/influxdb/models"
)

var (
	// ErrInvalidQuery is returned when executing an unknown query type.
	ErrInvalidQuery = errors.New("invalid query")

	// ErrNotExecuted is returned when a statement is not executed in a query.
	// This can occur when a previous statement in the same query has errored.
	ErrNotExecuted = errors.New("not executed")

	// ErrQueryInterrupted is an error returned when the query is interrupted.
	ErrQueryInterrupted = errors.New("query interrupted")

	// ErrMaxConcurrentQueriesReached is an error when a query cannot be run
	// because the maximum number of queries has been reached.
	ErrMaxConcurrentQueriesReached = errors.New("max concurrent queries reached")

	// ErrQueryEngineShutdown is an error sent when the query cannot be
	// created because the query engine was shutdown.
	ErrQueryEngineShutdown = errors.New("query engine shutdown")

	// ErrMaxPointsReached is an error when a query hits the maximum number of
	// points.
	ErrMaxPointsReached = errors.New("max number of points reached")

	// ErrQueryTimeoutReached is an error when a query hits the timeout.
	ErrQueryTimeoutReached = errors.New("query timeout reached")
)

// Statistics for the QueryExecutor
const (
	statQueriesActive          = "queriesActive"   // Number of queries currently being executed
	statQueryExecutionDuration = "queryDurationNs" // Total (wall) time spent executing queries
)

// ErrDatabaseNotFound returns a database not found error for the given database name.
func ErrDatabaseNotFound(name string) error { return fmt.Errorf("database not found: %s", name) }

// ErrMeasurementNotFound returns a measurement not found error for the given measurement name.
func ErrMeasurementNotFound(name string) error { return fmt.Errorf("measurement not found: %s", name) }

// ExecutionOptions contains the options for executing a query.
type ExecutionOptions struct {
	// The database the query is running against.
	Database string

	// The requested maximum number of points to return in each result.
	ChunkSize int

	// If this query is being executed in a read-only context.
	ReadOnly bool

	// Node to execute on.
	NodeID uint64
}

// ExecutionContext contains state that the query is currently executing with.
type ExecutionContext struct {
	// The statement ID of the executing query.
	StatementID int

	// The query ID of the executing query.
	QueryID uint64

	// The query task information available to the StatementExecutor.
	Query *QueryTask

	// Output channel where results and errors should be sent.
	Results chan *Result

	// Hold the query executor's logger.
	Log *log.Logger

	// A channel that is closed when the query is interrupted.
	InterruptCh <-chan struct{}

	// Options used to start this query.
	ExecutionOptions
}

// StatementExecutor executes a statement within the QueryExecutor.
// 用于执行一条查询语句
type StatementExecutor interface {
	// ExecuteStatement executes a statement. Results should be sent to the
	// results channel in the ExecutionContext.
	// 执行一条查询语句，结果通过 ctx.Results 通道返回
	ExecuteStatement(stmt Statement, ctx ExecutionContext) error
}

// StatementNormalizer normalizes a statement before it is executed.
type StatementNormalizer interface {
	// NormalizeStatement adds a default database and policy to the
	// measurements in the statement.
	// 向查询语句中加入默认的数据库和存储策略
	NormalizeStatement(stmt Statement, database string) error
}

// QueryExecutor executes every statement in an Query.
// 查询执行服务
type QueryExecutor struct {
	// Used for executing a statement in the query.
	// 执行一条语句
	StatementExecutor StatementExecutor

	// Used for tracking running queries.
	// 跟踪正在执行中的查询操作
	TaskManager *TaskManager

	// Logger to use for all logging.
	// Defaults to discarding all log output.
	Logger *log.Logger

	// expvar-based stats.
	stats *QueryStatistics
}

// NewQueryExecutor returns a new instance of QueryExecutor.
func NewQueryExecutor() *QueryExecutor {
	return &QueryExecutor{
		TaskManager: NewTaskManager(),
		Logger:      log.New(ioutil.Discard, "[query] ", log.LstdFlags),
		stats:       &QueryStatistics{},
	}
}

// QueryStatistics keeps statistics related to the QueryExecutor.
type QueryStatistics struct {
	ActiveQueries          int64
	QueryExecutionDuration int64
}

// Statistics returns statistics for periodic monitoring.
func (e *QueryExecutor) Statistics(tags map[string]string) []models.Statistic {
	return []models.Statistic{{
		Name: "queryExecutor",
		Tags: tags,
		Values: map[string]interface{}{
			statQueriesActive:          atomic.LoadInt64(&e.stats.ActiveQueries),
			statQueryExecutionDuration: atomic.LoadInt64(&e.stats.QueryExecutionDuration),
		},
	}}
}

// Close kills all running queries and prevents new queries from being attached.
func (e *QueryExecutor) Close() error {
	return e.TaskManager.Close()
}

// SetLogOutput sets the writer to which all logs are written. It must not be
// called after Open is called.
func (e *QueryExecutor) SetLogOutput(w io.Writer) {
	e.Logger = log.New(w, "[query] ", log.LstdFlags)
	e.TaskManager.Logger = e.Logger
}

// ExecuteQuery executes each statement within a query.
// 执行每一条查询命令，返回一个通道，用于之后将查询到的结果返回
func (e *QueryExecutor) ExecuteQuery(query *Query, opt ExecutionOptions, closing chan struct{}) <-chan *Result {
	results := make(chan *Result)
	go e.executeQuery(query, opt, closing, results)
	return results
}

// 执行 query 中的每一条查询命令，将结果通过 results 通道返回
func (e *QueryExecutor) executeQuery(query *Query, opt ExecutionOptions, closing <-chan struct{}, results chan *Result) {
	defer close(results)
	defer e.recover(query, results)

	atomic.AddInt64(&e.stats.ActiveQueries, 1)
	defer func(start time.Time) {
		atomic.AddInt64(&e.stats.ActiveQueries, -1)
		atomic.AddInt64(&e.stats.QueryExecutionDuration, time.Since(start).Nanoseconds())
	}(time.Now())

	// 将查询操作附加到任务管理器中
	qid, task, err := e.TaskManager.AttachQuery(query, opt.Database, closing)
	if err != nil {
		results <- &Result{Err: err}
		return
	}
	// 结束查询后从任务管理器中删除
	defer e.TaskManager.KillQuery(qid)

	// Setup the execution context that will be used when executing statements.
	// 任务执行的环境信息
	ctx := ExecutionContext{
		QueryID:          qid,
		Query:            task,
		Results:          results,
		Log:              e.Logger,
		InterruptCh:      task.closing,
		ExecutionOptions: opt,
	}

	var i int
	// 依次执行每一条查询请求
	for ; i < len(query.Statements); i++ {
		ctx.StatementID = i
		stmt := query.Statements[i]

		// If a default database wasn't passed in by the caller, check the statement.
		// 如果没有在最外层指定数据库名，那么使用查询语句中的数据库名
		defaultDB := opt.Database
		if defaultDB == "" {
			if s, ok := stmt.(HasDefaultDatabase); ok {
				defaultDB = s.DefaultDatabase()
			}
		}

		// Rewrite statements, if necessary.
		// This can occur on meta read statements which convert to SELECT statements.
		// 将 show sth 的查询元数据的语句转换成标准的 SELECT 语句
		// 例如 "SHOW MEASUREMENTS" 将会被转换为 "SELECT _name AS "name" FROM db_name.autogen._measurements"
		newStmt, err := RewriteStatement(stmt)
		if err != nil {
			results <- &Result{Err: err}
			break
		}
		stmt = newStmt

		// Normalize each statement if possible.
		// 如果有必要的话，向查询语句中加入默认的数据库和存储策略
		if normalizer, ok := e.StatementExecutor.(StatementNormalizer); ok {
			if err := normalizer.NormalizeStatement(stmt, defaultDB); err != nil {
				results <- &Result{Err: err}
				break
			}
		}

		// Log each normalized statement.
		e.Logger.Println(stmt.String())

		// Send any other statements to the underlying statement executor.
		// 通过底层的查询引擎查询数据
		err = e.StatementExecutor.ExecuteStatement(stmt, ctx)
		if err == ErrQueryInterrupted {
			// Query was interrupted so retrieve the real interrupt error from
			// the query task if there is one.
			if qerr := task.Error(); qerr != nil {
				err = qerr
			}
		}

		// Send an error for this result if it failed for some reason.
		if err != nil {
			results <- &Result{
				StatementID: i,
				Err:         err,
			}
			// Stop after the first error.
			// 有一个出错就立即返回
			break
		}
	}

	// Send error results for any statements which were not executed.
	// 由于前面出错就立即返回，这里后续没执行的查询请求都返回错误
	for ; i < len(query.Statements)-1; i++ {
		results <- &Result{
			StatementID: i,
			Err:         ErrNotExecuted,
		}
	}
}

func (e *QueryExecutor) recover(query *Query, results chan *Result) {
	if err := recover(); err != nil {
		e.Logger.Printf("%s [panic:%s] %s", query.String(), err, debug.Stack())
		results <- &Result{
			StatementID: -1,
			Err:         fmt.Errorf("%s [panic:%s]", query.String(), err),
		}
	}
}

// QueryMonitorFunc is a function that will be called to check if a query
// is currently healthy. If the query needs to be interrupted for some reason,
// the error should be returned by this function.
type QueryMonitorFunc func(<-chan struct{}) error

// QueryTask is the internal data structure for managing queries.
// For the public use data structure that gets returned, see QueryTask.
type QueryTask struct {
	query     string
	database  string
	startTime time.Time
	closing   chan struct{}
	monitorCh chan error
	err       error
	mu        sync.Mutex
}

// Monitor starts a new goroutine that will monitor a query. The function
// will be passed in a channel to signal when the query has been finished
// normally. If the function returns with an error and the query is still
// running, the query will be terminated.
func (q *QueryTask) Monitor(fn QueryMonitorFunc) {
	go q.monitor(fn)
}

// Error returns any asynchronous error that may have occured while executing
// the query.
func (q *QueryTask) Error() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.err
}

func (q *QueryTask) setError(err error) {
	q.mu.Lock()
	q.err = err
	q.mu.Unlock()
}

func (q *QueryTask) monitor(fn QueryMonitorFunc) {
	if err := fn(q.closing); err != nil {
		select {
		case <-q.closing:
		case q.monitorCh <- err:
		}
	}
}

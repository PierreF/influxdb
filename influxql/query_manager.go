package influxql

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/influxdb/models"
)

var (
	ErrNoQueryManager = errors.New("no query manager available")
)

// QueryTaskInfo holds information about a currently running query.
type QueryTaskInfo struct {
	ID       uint64
	Query    string
	Database string
	Duration time.Duration
}

// QueryParams holds the parameters used for creating a new query.
type QueryParams struct {
	// The query to be tracked. Required.
	Query *Query

	// The database this query is being run in. Required.
	Database string

	// The channel to watch for when this query is finished.
	// Not required, but highly recommended. If this channel is not
	// set, the query needs to be manually managed by the caller.
	DetachCh chan struct{}
}

type QueryManager interface {
	// AttachQuery attaches a running query to be managed by the query manager.
	// Returns the query id of the newly attached query or an error if it was
	// unable to assign a query id or attach the query to the query manager.
	// This function also returns a channel that will be closed when this
	// query finishes running.
	//
	// After a query finishes running, the system is free to reuse a query id.
	AttachQuery(params *QueryParams) (qid uint64, closing chan struct{}, err error)

	// DetachQuery removes a query from the query manager. If a DetachCh is used
	// to attach the query, this method will be called automatically when the
	// DetachCh is closed. This method can also be used to forcefully terminate
	// a running query.
	DetachQuery(qid uint64) error

	// ListQueries lists the currently running tasks.
	ListQueries() []QueryTaskInfo
}

func DefaultQueryManager() QueryManager {
	return &defaultQueryManager{
		queries: make(map[uint64]*queryTask),
		nextID:  1,
	}
}

func ExecuteShowQueriesStatement(qm QueryManager, q *ShowQueriesStatement) (models.Rows, error) {
	if qm == nil {
		return nil, ErrNoQueryManager
	}

	queries := qm.ListQueries()
	values := make([][]interface{}, len(queries))
	for i, qi := range queries {
		values[i] = []interface{}{
			qi.ID,
			qi.Query,
			qi.Database,
			FormatHumanReadableDuration(qi.Duration),
		}
	}

	return []*models.Row{{
		Columns: []string{"qid", "query", "database", "duration"},
		Values:  values,
	}}, nil
}

type queryTask struct {
	query     string
	database  string
	startTime time.Time
	closing   chan struct{}
	once      sync.Once
}

type defaultQueryManager struct {
	queries map[uint64]*queryTask
	nextID  uint64
	mu      sync.Mutex
}

func (qm *defaultQueryManager) AttachQuery(params *QueryParams) (uint64, chan struct{}, error) {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	qid := qm.nextID
	query := &queryTask{
		query:     params.Query.String(),
		database:  params.Database,
		startTime: time.Now(),
		closing:   make(chan struct{}),
	}
	qm.queries[qid] = query

	if params.DetachCh != nil {
		go qm.waitForQuery(qid, params.DetachCh)
	}
	qm.nextID++
	return qid, query.closing, nil
}

func (qm *defaultQueryManager) waitForQuery(qid uint64, closing chan struct{}) {
	<-closing
	qm.DetachQuery(qid)
}

func (qm *defaultQueryManager) DetachQuery(qid uint64) error {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	query, ok := qm.queries[qid]
	if !ok {
		return fmt.Errorf("no such query id: %d", qid)
	}

	close(query.closing)
	delete(qm.queries, qid)
	return nil
}

func (qm *defaultQueryManager) ListQueries() []QueryTaskInfo {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	now := time.Now()

	queries := make([]QueryTaskInfo, 0, len(qm.queries))
	for qid, task := range qm.queries {
		queries = append(queries, QueryTaskInfo{
			ID:       qid,
			Query:    task.query,
			Database: task.database,
			Duration: now.Sub(task.startTime),
		})
	}
	return queries
}

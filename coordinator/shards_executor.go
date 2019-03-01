package coordinator

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/pkg/tracing"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxql"
	"go.uber.org/zap"
)

type ClusterExecutorImpl struct {
	TSDBStore
	Node               *influxdb.Node
	MetaClient         MetaClient
	RemoteNodeExecutor RemoteNodeExecutor
	Logger             *zap.Logger
}

func NewClusterExecutor(n *influxdb.Node, s TSDBStore, m MetaClient, Config Config) *ClusterExecutorImpl {
	return &ClusterExecutorImpl{
		Node:       n,
		TSDBStore:  s,
		MetaClient: m,
		RemoteNodeExecutor: &RemoteNodeExecutorImpl{
			MetaClient:         m,
			DailTimeout:        time.Duration(Config.DailTimeout),
			ShardReaderTimeout: time.Duration(Config.ShardReaderTimeout),
			ClusterTracing:     Config.ClusterTracing,
		},
		Logger: zap.NewNop(),
	}
}

func (me *ClusterExecutorImpl) WithLogger(log *zap.Logger) {
	me.Logger = log.With(zap.String("service", "ClusterExecutor"))
}

type RemoteNodeExecutor interface {
	TagKeys(nodeId uint64, ShardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error)
	TagValues(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error)
	MeasurementNames(nodeId uint64, database string, cond influxql.Expr) ([][]byte, error)
	SeriesCardinality(nodeId uint64, database string) (int64, error)
	DeleteSeries(nodeId uint64, database string, sources []influxql.Source, condition influxql.Expr) error
	DeleteDatabase(nodeId uint64, database string) error
	DeleteMeasurement(nodeId uint64, database, name string) error
	ExecuteStatement(nodeId uint64, stmt influxql.Statement, database string) error
	FieldDimensions(nodeId uint64, m *influxql.Measurement, shardIds []uint64) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error)
	IteratorCost(nodeId uint64, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.IteratorCost, error)
	MapType(nodeId uint64, m *influxql.Measurement, field string, shardIds []uint64) (influxql.DataType, error)
	CreateIterator(nodeId uint64, ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.Iterator, error)
	TaskManagerStatement(nodeId uint64, stmt influxql.Statement) (*query.Result, error)
}

type RemoteNodeExecutorImpl struct {
	MetaClient         MetaClient
	DailTimeout        time.Duration
	ShardReaderTimeout time.Duration
	ClusterTracing     bool
}

func (me *RemoteNodeExecutorImpl) CreateIterator(nodeId uint64, ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.Iterator, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.ShardReaderTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return nil, err
	}

	var resp CreateIteratorResponse
	if err := func() error {
		var spanCtx *tracing.SpanContext
		span := tracing.SpanFromContext(ctx)
		if span != nil {
			spanCtx = new(tracing.SpanContext)
			*spanCtx = span.Context()
		}
		if err := EncodeTLV(conn, createIteratorRequestMessage, &CreateIteratorRequest{
			ShardIDs:    shardIds,
			Measurement: *m, //TODO:改为Sources
			Opt:         opt,
			SpanContex:  spanCtx,
		}); err != nil {
			return err
		}

		// Read the response.
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != nil {
			return err
		}

		return nil
	}(); err != nil {
		conn.Close()
		return nil, err
	}

	if resp.DataType == influxql.Unknown {
		return nil, nil
	}
	stats := query.IteratorStats{SeriesN: resp.SeriesN}
	itr := query.NewReaderIterator(ctx, conn, resp.DataType, stats)
	return itr, nil
}

func (me *RemoteNodeExecutorImpl) MapType(nodeId uint64, m *influxql.Measurement, field string, shardIds []uint64) (influxql.DataType, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return influxql.Unknown, err
	}
	defer conn.Close()

	measurement := *m
	if measurement.SystemIterator != "" {
		measurement.Name = measurement.SystemIterator
	}

	var resp MapTypeResponse
	if err := func() error {
		if err := EncodeTLV(conn, mapTypeRequestMessage, &MapTypeRequest{
			Sources:  influxql.Sources([]influxql.Source{&measurement}),
			Field:    field,
			ShardIDs: shardIds,
		}); err != nil {
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return influxql.Unknown, err
	}

	return resp.DataType, nil
}

func (me *RemoteNodeExecutorImpl) IteratorCost(nodeId uint64, m *influxql.Measurement, opt query.IteratorOptions, shardIds []uint64) (query.IteratorCost, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return query.IteratorCost{}, err
	}
	defer conn.Close()

	var resp IteratorCostResponse
	if err := func() error {
		if err := EncodeTLV(conn, iteratorCostRequestMessage, &IteratorCostRequest{
			Sources:  influxql.Sources([]influxql.Source{m}),
			Opt:      opt,
			ShardIDs: shardIds,
		}); err != nil {
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return query.IteratorCost{}, err
	}

	return resp.Cost, nil
}

func (me *RemoteNodeExecutorImpl) FieldDimensions(nodeId uint64, m *influxql.Measurement, shardIds []uint64) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return nil, nil, err
	}
	defer conn.Close()

	var resp FieldDimensionsResponse
	if err := func() error {
		var req FieldDimensionsRequest
		req.ShardIDs = shardIds
		req.Sources = influxql.Sources([]influxql.Source{m})
		if err := EncodeTLV(conn, fieldDimensionsRequestMessage, &req); err != nil {
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != nil {
			return resp.Err
		}

		return nil
	}(); err != nil {
		return nil, nil, err
	}

	return resp.Fields, resp.Dimensions, nil
}

func (me *RemoteNodeExecutorImpl) TaskManagerStatement(nodeId uint64, stmt influxql.Statement) (*query.Result, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var resp TaskManagerStatementResponse
	if err := func() error {
		var req TaskManagerStatementRequest
		req.SetStatement(stmt.String())
		req.SetDatabase("")
		if err := EncodeTLV(conn, executeTaskManagerRequestMessage, &req); err != nil {
			return err
		}

		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return nil, err
	}

	result := &query.Result{}
	*result = resp.Result
	return result, nil
}

func (me *RemoteNodeExecutorImpl) ExecuteStatement(nodeId uint64, stmt influxql.Statement, database string) error {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := func() error {
		var req ExecuteStatementRequest
		req.SetStatement(stmt.String())
		req.SetDatabase(database)
		if err := EncodeTLV(conn, executeStatementRequestMessage, &req); err != nil {
			return err
		}

		var resp ExecuteStatementResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Code() != 0 {
			return errors.New(resp.Message())
		}

		return nil
	}(); err != nil {
		return err
	}

	return nil
}

func (me *RemoteNodeExecutorImpl) DeleteMeasurement(nodeId uint64, database, name string) error {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := func() error {
		if err := EncodeTLV(conn, deleteMeasurementRequestMessage, &DeleteMeasurementRequest{
			Database: database,
			Name:     name,
		}); err != nil {
			return err
		}

		var resp DeleteMeasurementResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return err
	}

	return nil
}

func (me *RemoteNodeExecutorImpl) DeleteDatabase(nodeId uint64, database string) error {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := func() error {
		if err := EncodeTLV(conn, deleteDatabaseRequestMessage, &DeleteDatabaseRequest{
			Database: database,
		}); err != nil {
			return err
		}

		var resp DeleteDatabaseResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return err
	}

	return nil
}

func (me *RemoteNodeExecutorImpl) DeleteSeries(nodeId uint64, database string, sources []influxql.Source, cond influxql.Expr) error {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := func() error {
		if err := EncodeTLV(conn, deleteSeriesRequestMessage, &DeleteSeriesRequest{
			Database: database,
			Sources:  influxql.Sources(sources),
			Cond:     cond,
		}); err != nil {
			return err
		}

		var resp DeleteSeriesResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		return nil
	}(); err != nil {
		return err
	}

	return nil
}
func (me *RemoteNodeExecutorImpl) SeriesCardinality(nodeId uint64, database string) (int64, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	var n int64
	if err := func() error {
		if err := EncodeTLV(conn, seriesCardinalityRequestMessage, &SeriesCardinalityRequest{
			Database: database,
		}); err != nil {
			return err
		}

		var resp SeriesCardinalityResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		n = resp.N
		return nil
	}(); err != nil {
		return -1, err
	}

	return n, nil
}

func (me *RemoteNodeExecutorImpl) MeasurementNames(nodeId uint64, database string, cond influxql.Expr) ([][]byte, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var names [][]byte
	if err := func() error {
		if err := EncodeTLV(conn, measurementNamesRequestMessage, &MeasurementNamesRequest{
			Database: database,
			Cond:     cond,
		}); err != nil {
			return err
		}

		var resp MeasurementNamesResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		names = resp.Names
		return nil
	}(); err != nil {
		return nil, err
	}

	return names, nil
}

func (me *RemoteNodeExecutorImpl) TagValues(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagValues, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	var tagValues []tsdb.TagValues
	if err := func() error {
		if err := EncodeTLV(conn, tagValuesRequestMessage, &TagValuesRequest{
			TagKeysRequest{
				ShardIDs: shardIDs,
				Cond:     cond,
			},
		}); err != nil {
			return err
		}

		var resp TagValuesResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		tagValues = resp.TagValues
		return nil
	}(); err != nil {
		return nil, err
	}

	return tagValues, nil
}

func (me *RemoteNodeExecutorImpl) TagKeys(nodeId uint64, shardIDs []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error) {
	dialer := &NodeDialer{
		MetaClient: me.MetaClient,
		Timeout:    me.DailTimeout,
	}

	conn, err := dialer.DialNode(nodeId)
	if err != nil {
		return nil, err
	}

	defer conn.Close()

	var tagKeys []tsdb.TagKeys
	if err := func() error {
		if err := EncodeTLV(conn, tagKeysRequestMessage, &TagKeysRequest{
			ShardIDs: shardIDs,
			Cond:     cond,
		}); err != nil {
			return err
		}

		var resp TagKeysResponse
		if _, err := DecodeTLV(conn, &resp); err != nil {
			return err
		} else if resp.Err != "" {
			return errors.New(resp.Err)
		}

		tagKeys = resp.TagKeys
		return nil
	}(); err != nil {
		return nil, err
	}

	return tagKeys, nil
}

type NodeIds []uint64

func NewNodeIdsByNodes(nodeInfos []meta.NodeInfo) NodeIds {
	var ids []uint64
	for _, ni := range nodeInfos {
		ids = append(ids, ni.ID)
	}
	return NodeIds(ids)
}

//TODO:取个达意的名字
func NewNodeIdsByShards(Shards []meta.ShardInfo) NodeIds {
	m := make(map[uint64]struct{})
	for _, si := range Shards {
		for _, owner := range si.Owners {
			m[owner.NodeID] = struct{}{}
		}
	}

	nodes := make([]uint64, 0, len(m))
	for n, _ := range m {
		nodes = append(nodes, n)
	}
	return nodes
}

func (me NodeIds) Apply(fn func(nodeId uint64)) {
	for _, nodeID := range me {
		fn(nodeID)
	}
}

type Node2ShardIDs map[uint64][]uint64

//TODO: 只选择活跃的Node
func NewNode2ShardIDs(mc MetaClient, localNode *influxdb.Node, shards []meta.ShardInfo) Node2ShardIDs {
	allNodes := make([]uint64, 0)
	for _, si := range shards {
		if si.OwnedBy(localNode.ID) {
			continue
		}

		for _, owner := range si.Owners {
			allNodes = append(allNodes, owner.NodeID)
		}
	}

	//选出 active node
	activeNodes := make(map[uint64]struct{})
	var wg sync.WaitGroup
	var mutex sync.Mutex
	for _, id := range allNodes {
		wg.Add(1)
		go func(id uint64) {
			defer wg.Add(-1)
			dialer := &NodeDialer{
				MetaClient: mc,
				Timeout:    100 * time.Millisecond, //TODO: from config
			}

			conn, err := dialer.DialNode(id)
			if err != nil {
				return
			}
			defer conn.Close()
			mutex.Lock()
			activeNodes[id] = struct{}{}
			mutex.Unlock()
		}(id)
	}

	wg.Wait()

	shardIDsByNodeID := make(map[uint64][]uint64)
	for _, si := range shards {
		var nodeID uint64
		if si.OwnedBy(localNode.ID) {
			nodeID = localNode.ID
		} else if len(si.Owners) > 0 {
			nodeID = si.Owners[rand.Intn(len(si.Owners))].NodeID
			if _, ok := activeNodes[nodeID]; !ok {
				//利用map的顺序不确定特性，随机选一个active的owner
				randomOwners := make(map[uint64]struct{})
				for _, owner := range si.Owners {
					randomOwners[owner.NodeID] = struct{}{}
				}
				for id, _ := range randomOwners {
					if _, ok := activeNodes[id]; ok {
						nodeID = id
						break
					}
				}
			}
		} else {
			continue
		}
		shardIDsByNodeID[nodeID] = append(shardIDsByNodeID[nodeID], si.ID)
	}

	return shardIDsByNodeID
}

type uint64Slice []uint64

func (a uint64Slice) Len() int           { return len(a) }
func (a uint64Slice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a uint64Slice) Less(i, j int) bool { return a[i] < a[j] }

func (me Node2ShardIDs) Apply(fn func(nodeId uint64, shardIDs []uint64)) {
	for nodeID, shardIDs := range me {
		// Sort shard IDs so we get more predicable execution.
		sort.Sort(uint64Slice(shardIDs))
		fn(nodeID, shardIDs)
	}
}

func (me *ClusterExecutorImpl) TaskManagerStatement(tm *query.TaskManager, stmt influxql.Statement, ctx *query.ExecutionContext) error {
	type Result struct {
		qr  *query.Result
		err error
	}

	nodeInfos, err := me.MetaClient.DataNodes()
	if err != nil {
		return err
	}

	nodes := NewNodeIdsByNodes(nodeInfos)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		host := "unkown"
		node, _ := me.MetaClient.DataNode(nodeId)
		if node != nil {
			host = node.Host
		}

		switch t := stmt.(type) {
		case *influxql.KillQueryStatement:
			if t.Host != "" && t.Host != host {
				return
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var err error
			var qr *query.Result
			if nodeId == me.Node.ID {
				recvCtx := &query.ExecutionContext{
					Context: context.Background(),
					Results: make(chan *query.Result, 1),
				}
				err = tm.ExecuteStatement(stmt, recvCtx)
				if err == nil {
					qr = <-recvCtx.Results
				}
			} else {
				qr, err = me.RemoteNodeExecutor.TaskManagerStatement(nodeId, stmt)
			}

			if qr != nil && len(qr.Series) > 0 {
				qr.Series[0].Columns = append(qr.Series[0].Columns, "host")
				for i := 0; i < len(qr.Series[0].Values); i++ {
					qr.Series[0].Values[i] = append(qr.Series[0].Values[i], host)
				}
			}

			mutex.Lock()
			results[nodeId] = &Result{qr: qr, err: err}
			mutex.Unlock()
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	//merge result
	row := new(models.Row)
	err = nil
	switch stmt.(type) {
	case *influxql.ShowQueriesStatement:
		for _, r := range results {
			if r.err != nil {
				err = r.err
				break
			}
			if len(r.qr.Series) == 0 {
				continue
			}
			if len(row.Columns) == 0 {
				row.Columns = r.qr.Series[0].Columns
			}
			row.Values = append(row.Values, r.qr.Series[0].Values...)
		}
	case *influxql.KillQueryStatement:
		allFail := true
		for _, r := range results {
			if r.err != nil {
				err = r.err
			} else {
				allFail = false
			}
		}
		if !allFail {
			err = nil
		}
	}

	if err != nil {
		return err
	}
	ctx.Send(&query.Result{Series: models.Rows{row}})
	return nil
}

func (me *ClusterExecutorImpl) ExecuteStatement(stmt influxql.Statement, database string) error {
	type Result struct {
		err error
	}

	nodeInfos, err := me.MetaClient.DataNodes()
	if err != nil {
		return err
	}

	nodes := NewNodeIdsByNodes(nodeInfos)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var err error
			if nodeId == me.Node.ID {
				switch t := stmt.(type) {
				case *influxql.DropShardStatement:
					err = me.TSDBStore.DeleteShard(t.ID)
				case *influxql.DropRetentionPolicyStatement:
					err = me.TSDBStore.DeleteRetentionPolicy(t.Database, t.Name)
				case *influxql.DropDatabaseStatement:
					err = me.TSDBStore.DeleteDatabase(t.Name)
				case *influxql.DropMeasurementStatement:
					err = me.TSDBStore.DeleteMeasurement(database, t.Name)
				case *influxql.DropSeriesStatement:
					err = me.TSDBStore.DeleteSeries(database, t.Sources, t.Condition)
				default:
					err = query.ErrInvalidQuery
				}
			} else {
				err = me.RemoteNodeExecutor.ExecuteStatement(nodeId, stmt, database)
			}

			mutex.Lock()
			results[nodeId] = &Result{err: err}
			mutex.Unlock()
			return
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return r.err
		}
	}
	return nil
}

func (me *ClusterExecutorImpl) DeleteMeasurement(database, name string) error {
	type Result struct {
		err error
	}

	shards, err := DatabaseShards(me.MetaClient, database)
	if err != nil {
		return err
	}
	nodes := NewNodeIdsByShards(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var err error
			if nodeId == me.Node.ID {
				err = me.TSDBStore.DeleteMeasurement(database, name)
			} else {
				err = me.RemoteNodeExecutor.DeleteMeasurement(nodeId, database, name)
			}

			mutex.Lock()
			results[nodeId] = &Result{err: err}
			mutex.Unlock()
			return
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return r.err
		}
	}
	return nil
}

func (me *ClusterExecutorImpl) DeleteDatabase(database string) error {
	type Result struct {
		err error
	}

	shards, err := DatabaseShards(me.MetaClient, database)
	if err != nil {
		return err
	}
	nodes := NewNodeIdsByShards(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var err error
			if nodeId == me.Node.ID {
				err = me.TSDBStore.DeleteDatabase(database)
			} else {
				err = me.RemoteNodeExecutor.DeleteDatabase(nodeId, database)
			}

			mutex.Lock()
			results[nodeId] = &Result{err: err}
			mutex.Unlock()
			return
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return r.err
		}
	}
	return nil
}

func (me *ClusterExecutorImpl) DeleteSeries(database string, sources []influxql.Source, cond influxql.Expr) error {
	type Result struct {
		err error
	}

	shards, err := DatabaseShards(me.MetaClient, database)
	if err != nil {
		return err
	}
	nodes := NewNodeIdsByShards(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var err error
			if nodeId == me.Node.ID {
				// Convert "now()" to current time.
				cond = influxql.Reduce(cond, &influxql.NowValuer{Now: time.Now().UTC()})
				err = me.TSDBStore.DeleteSeries(database, sources, cond)
			} else {
				err = me.RemoteNodeExecutor.DeleteSeries(nodeId, database, sources, cond)
			}

			mutex.Lock()
			results[nodeId] = &Result{err: err}
			mutex.Unlock()
			return
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	for _, r := range results {
		if r.err != nil {
			return r.err
		}
	}
	return nil
}

func (me *ClusterExecutorImpl) SeriesCardinality(database string) (int64, error) {
	type Result struct {
		n   int64
		err error
	}

	shards, err := DatabaseShards(me.MetaClient, database)
	if err != nil {
		return -1, err
	}
	nodes := NewNodeIdsByShards(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var n int64
			var err error
			if nodeId == me.Node.ID {
				n, err = me.TSDBStore.SeriesCardinality(database)
			} else {
				n, err = me.RemoteNodeExecutor.SeriesCardinality(nodeId, database)
			}

			mutex.Lock()
			results[nodeId] = &Result{n: n, err: err}
			mutex.Unlock()
			return
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	var sum int64
	for _, r := range results {
		if r.err != nil {
			return -1, r.err
		}
		sum += r.n
	}
	return sum, nil
}

func DatabaseShards(c MetaClient, db string) ([]meta.ShardInfo, error) {
	dbInfo := c.Database(db)
	if dbInfo == nil {
		return nil, fmt.Errorf("not find database %s", db)
	}
	var shards []meta.ShardInfo
	for _, rp := range dbInfo.RetentionPolicies {
		for _, sg := range rp.ShardGroups {
			shards = append(shards, sg.Shards...)
		}
	}

	return shards, nil
}

func (me *ClusterExecutorImpl) MeasurementNames(auth query.Authorizer, database string, cond influxql.Expr) ([][]byte, error) {
	type Result struct {
		names [][]byte
		err   error
	}

	shards, err := DatabaseShards(me.MetaClient, database)
	if err != nil {
		return nil, err
	}
	nodes := NewNodeIdsByShards(shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var names [][]byte
			var err error
			if nodeId == me.Node.ID {
				names, err = me.TSDBStore.MeasurementNames(auth, database, cond)
			} else {
				names, err = me.RemoteNodeExecutor.MeasurementNames(nodeId, database, cond)
			}

			mutex.Lock()
			results[nodeId] = &Result{names: names, err: err}
			mutex.Unlock()
			return
		}()
	}

	nodes.Apply(fn)
	wg.Wait()

	uniq := make(map[string]struct{})
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		for _, name := range r.names {
			uniq[string(name)] = struct{}{}
		}
	}

	strNames := make([]string, 0, len(uniq))
	for name, _ := range uniq {
		strNames = append(strNames, name)
	}
	sort.Sort(StringSlice(strNames))

	names := make([][]byte, 0, len(uniq))
	for _, name := range strNames {
		names = append(names, []byte(name))
	}

	return names, nil
}

func (me *ClusterExecutorImpl) TagValues(auth query.Authorizer, ids []uint64, cond influxql.Expr) ([]tsdb.TagValues, error) {
	type TagValuesResult struct {
		values []tsdb.TagValues
		err    error
	}

	shards, err := GetShardInfoByIds(me.MetaClient, ids)
	if err != nil {
		return nil, err
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	result := make(map[uint64]*TagValuesResult)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var tagValues []tsdb.TagValues
			var err error
			if nodeId == me.Node.ID {
				tagValues, err = me.TSDBStore.TagValues(auth, shardIDs, cond)
			} else {
				tagValues, err = me.RemoteNodeExecutor.TagValues(nodeId, shardIDs, cond)
			}

			mutex.Lock()
			result[nodeId] = &TagValuesResult{values: tagValues, err: err}
			mutex.Unlock()
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	extractKeyFn := func(kv tsdb.KeyValue) string {
		//TODO:
		return fmt.Sprintf("key=%s+-*/value=%s", kv.Key, kv.Value)
	}

	uniq := make(map[string]map[string]tsdb.KeyValue)
	for nodeId, r := range result {
		if r.err != nil {
			return nil, fmt.Errorf("TagKeys fail, nodeId %d, err:%s", nodeId, r.err)
		}

		for _, tagValue := range r.values {
			m, ok := uniq[tagValue.Measurement]
			if !ok {
				m = make(map[string]tsdb.KeyValue)
			}
			for _, kv := range tagValue.Values {
				m[extractKeyFn(kv)] = kv
			}
			uniq[tagValue.Measurement] = m
		}
	}

	tagValues := make([]tsdb.TagValues, len(uniq))
	idx := 0
	for m, kvs := range uniq {
		tagv := &tagValues[idx]
		tagv.Measurement = m
		for _, kv := range kvs {
			tagv.Values = append(tagv.Values, kv)
		}
		sort.Sort(tsdb.KeyValues(tagv.Values))
		idx = idx + 1
	}

	sort.Sort(tsdb.TagValuesSlice(tagValues))
	return tagValues, nil
}

func (me *ClusterExecutorImpl) CreateIterator(ctx context.Context, m *influxql.Measurement, opt query.IteratorOptions, shards []meta.ShardInfo) (query.Iterator, error) {
	type Result struct {
		iter query.Iterator
		err  error
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var iter query.Iterator
			var err error
			if nodeId == me.Node.ID {
				//localCtx only use for local node
				localCtx := ctx
				iter, err = func() (query.Iterator, error) {
					span := tracing.SpanFromContext(localCtx)
					if span != nil {
						span = span.StartSpan(fmt.Sprintf("local_node_id: %d", me.Node.ID))
						defer span.Finish()

						localCtx = tracing.NewContextWithSpan(localCtx, span)
					}
					sg := me.TSDBStore.ShardGroup(shardIDs)
					if m.Regex != nil {
						measurements := sg.MeasurementsByRegex(m.Regex.Val)
						inputs := make([]query.Iterator, 0, len(measurements))
						if err := func() error {
							for _, measurement := range measurements {
								mm := m.Clone()
								mm.Name = measurement
								input, err := sg.CreateIterator(localCtx, mm, opt)
								if err != nil {
									return err
								}
								inputs = append(inputs, input)
							}
							return nil
						}(); err != nil {
							query.Iterators(inputs).Close()
							return nil, err
						}

						return query.Iterators(inputs).Merge(opt)
					}
					return sg.CreateIterator(localCtx, m, opt)
				}()
			} else {
				iter, err = me.RemoteNodeExecutor.CreateIterator(nodeId, ctx, m, opt, shardIDs)
			}

			mutex.Lock()
			results[nodeId] = &Result{iter: iter, err: err}
			mutex.Unlock()
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	seriesN := 0
	inputs := make([]query.Iterator, 0, len(results))
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if r.iter != nil {
			stats := r.iter.Stats()
			seriesN += stats.SeriesN
			inputs = append(inputs, r.iter)
		}
	}

	if opt.MaxSeriesN > 0 && seriesN > opt.MaxSeriesN {
		query.Iterators(inputs).Close()
		return nil, fmt.Errorf("max-select-series limit exceeded: (%d/%d)", seriesN, opt.MaxSeriesN)
	}
	return query.Iterators(inputs).Merge(opt)
}

func (me *ClusterExecutorImpl) MapType(m *influxql.Measurement, field string, shards []meta.ShardInfo) influxql.DataType {
	type Result struct {
		dataType influxql.DataType
		err      error
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	var result Result
	fn := func(nodeId uint64, shardIDs []uint64) {
		if nodeId == me.Node.ID {
			sg := me.TSDBStore.ShardGroup(shardIDs)
			var names []string
			if m.Regex != nil {
				names = sg.MeasurementsByRegex(m.Regex.Val)
			} else {
				names = []string{m.Name}
			}

			typ := influxql.Unknown
			for _, name := range names {
				if m.SystemIterator != "" {
					name = m.SystemIterator
				}
				t := sg.MapType(name, field)
				if typ.LessThan(t) {
					typ = t
				}
			}
			result.dataType = typ
		}
		return
	}

	n2s.Apply(fn)
	if result.dataType != influxql.Unknown {
		return result.dataType
	}

	//本地失败, 尝试从remote node获取
	results := make(map[uint64]*Result)
	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn = func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			if nodeId != me.Node.ID {
				mutex.Lock()
				typ, err := me.RemoteNodeExecutor.MapType(nodeId, m, field, shardIDs)
				results[nodeId] = &Result{dataType: typ, err: err}
				mutex.Unlock()
			}
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	typ := influxql.Unknown
	for nodeId, r := range results {
		if r.err != nil {
			me.Logger.Warn("results have error", zap.Error(r.err), zap.Uint64("node", nodeId))
			continue
		}
		if typ.LessThan(r.dataType) {
			typ = r.dataType
		}
	}

	return typ
}

func (me *ClusterExecutorImpl) IteratorCost(m *influxql.Measurement, opt query.IteratorOptions, shards []meta.ShardInfo) (query.IteratorCost, error) {
	type Result struct {
		cost query.IteratorCost
		err  error
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var cost query.IteratorCost
			var err error
			if nodeId == me.Node.ID {
				sg := me.TSDBStore.ShardGroup(shardIDs)
				if m.Regex != nil {
					cost, err = func() (query.IteratorCost, error) {
						var costs query.IteratorCost
						measurements := sg.MeasurementsByRegex(m.Regex.Val)
						for _, measurement := range measurements {
							c, err := sg.IteratorCost(measurement, opt)
							if err != nil {
								return c, err
							}
							costs = costs.Combine(c)
						}
						return costs, nil
					}()
				} else {
					cost, err = sg.IteratorCost(m.Name, opt)
				}
			} else {
				cost, err = me.RemoteNodeExecutor.IteratorCost(nodeId, m, opt, shardIDs)
			}

			mutex.Lock()
			results[nodeId] = &Result{cost: cost, err: err}
			mutex.Unlock()
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	var costs query.IteratorCost
	for _, r := range results {
		if r.err != nil {
			return costs, r.err
		}
		costs = costs.Combine(r.cost)
	}
	return costs, nil
}

func (me *ClusterExecutorImpl) FieldDimensions(m *influxql.Measurement, shards []meta.ShardInfo) (fields map[string]influxql.DataType, dimensions map[string]struct{}, err error) {
	type Result struct {
		fields     map[string]influxql.DataType
		dimensions map[string]struct{}
		err        error
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	results := make(map[uint64]*Result)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var fields map[string]influxql.DataType
			var dimensions map[string]struct{}
			var err error
			if nodeId == me.Node.ID {
				sg := me.TSDBStore.ShardGroup(shardIDs)
				var measurements []string
				if m.Regex != nil {
					measurements = sg.MeasurementsByRegex(m.Regex.Val)
				} else {
					measurements = []string{m.Name}
				}
				fields, dimensions, err = sg.FieldDimensions(measurements)
			} else {
				fields, dimensions, err = me.RemoteNodeExecutor.FieldDimensions(nodeId, m, shardIDs)
			}

			mutex.Lock()
			results[nodeId] = &Result{fields: fields, dimensions: dimensions, err: err}
			mutex.Unlock()
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	fields = make(map[string]influxql.DataType)
	dimensions = make(map[string]struct{})
	for _, r := range results {
		//TODO: merge err
		if r.err != nil {
			return nil, nil, r.err
		}

		for f, t := range r.fields {
			fields[f] = t
		}
		for d, _ := range r.dimensions {
			dimensions[d] = struct{}{}
		}
	}

	return fields, dimensions, nil
}

func GetShardInfoByIds(MetaClient MetaClient, ids []uint64) ([]meta.ShardInfo, error) {
	var shards []meta.ShardInfo
	for _, id := range ids {
		_, _, sgi := MetaClient.ShardOwner(id)
		if sgi == nil {
			return nil, fmt.Errorf("not find shard %d", id)
		}
		for _, shard := range sgi.Shards {
			if shard.ID == id {
				shards = append(shards, shard)
				break
			}
		}
	}
	return shards, nil
}

func (me *ClusterExecutorImpl) TagKeys(auth query.Authorizer, ids []uint64, cond influxql.Expr) ([]tsdb.TagKeys, error) {
	type TagKeysResult struct {
		keys []tsdb.TagKeys
		err  error
	}

	shards, err := GetShardInfoByIds(me.MetaClient, ids)
	if err != nil {
		return nil, err
	}

	n2s := NewNode2ShardIDs(me.MetaClient, me.Node, shards)
	result := make(map[uint64]*TagKeysResult)

	var mutex sync.Mutex
	var wg sync.WaitGroup
	fn := func(nodeId uint64, shardIDs []uint64) {
		wg.Add(1)
		go func() {
			defer wg.Add(-1)

			var tagKeys []tsdb.TagKeys
			var err error
			if nodeId == me.Node.ID {
				tagKeys, err = me.TSDBStore.TagKeys(auth, shardIDs, cond)
			} else {
				tagKeys, err = me.RemoteNodeExecutor.TagKeys(nodeId, shardIDs, cond)
			}

			if err != nil {
				me.Logger.Error("TagKeys fail", zap.Error(err), zap.Uint64("node", nodeId))
			}
			mutex.Lock()
			result[nodeId] = &TagKeysResult{keys: tagKeys, err: err}
			mutex.Unlock()
			return
		}()
	}

	n2s.Apply(fn)
	wg.Wait()

	uniqKeys := make(map[string]map[string]struct{})
	for _, r := range result {
		if r.err != nil {
			return nil, r.err
		}

		for _, tagKey := range r.keys {
			m, ok := uniqKeys[tagKey.Measurement]
			if !ok {
				m = make(map[string]struct{})
			}
			for _, key := range tagKey.Keys {
				m[key] = struct{}{}
			}
			uniqKeys[tagKey.Measurement] = m
		}
	}

	tagKeys := make([]tsdb.TagKeys, len(uniqKeys))
	idx := 0
	for m, keys := range uniqKeys {
		tagKey := &tagKeys[idx]
		tagKey.Measurement = m
		for k, _ := range keys {
			tagKey.Keys = append(tagKey.Keys, k)
		}
		sort.Sort(StringSlice(tagKey.Keys))
		idx = idx + 1
	}

	sort.Sort(tsdb.TagKeysSlice(tagKeys))
	return tagKeys, nil
}

type StringSlice []string

func (a StringSlice) Len() int           { return len(a) }
func (a StringSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a StringSlice) Less(i, j int) bool { return a[i] < a[j] }

// NodeDialer dials connections to a given node.
type NodeDialer struct {
	MetaClient MetaClient
	Timeout    time.Duration
}

// DialNode returns a connection to a node.
func (d *NodeDialer) DialNode(nodeID uint64) (net.Conn, error) {
	ni, err := d.MetaClient.DataNode(nodeID)
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", ni.TCPHost)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(d.Timeout))

	// Write the cluster multiplexing header byte
	if _, err := conn.Write([]byte{MuxHeader}); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

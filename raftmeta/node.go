package raftmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/angopher/chronus/raftmeta/internal"
	imeta "github.com/angopher/chronus/services/meta"
	"github.com/angopher/chronus/x"
	"github.com/coreos/etcd/pkg/wait"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/options"
	"github.com/dgraph-io/dgraph/raftwal"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
	"golang.org/x/net/trace"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

var errInternalRetry = errors.New("Retry Raft proposal internally")

type proposalCtx struct {
	ch      chan error
	ctx     context.Context
	retData interface{}
	err     error
	index   uint64 // RAFT index for the proposal.
}

type proposals struct {
	sync.RWMutex
	all map[string]*proposalCtx
}

func newProposals() *proposals {
	return &proposals{
		all: make(map[string]*proposalCtx),
	}
}

func (p *proposals) Store(key string, pctx *proposalCtx) bool {
	p.Lock()
	defer p.Unlock()
	if _, has := p.all[key]; has {
		return false
	}
	p.all[key] = pctx
	return true
}

func (p *proposals) Delete(key string) {
	if len(key) == 0 {
		return
	}
	p.Lock()
	defer p.Unlock()
	delete(p.all, key)
}

func (p *proposals) pctx(key string) *proposalCtx {
	p.RLock()
	defer p.RUnlock()
	if pctx := p.all[key]; pctx != nil {
		return pctx
	}
	return new(proposalCtx)
}

func (p *proposals) Done(key string, err error) {
	if len(key) == 0 {
		return
	}
	p.Lock()
	defer p.Unlock()
	pd, has := p.all[key]
	if !has {
		// If we assert here, there would be a race condition between a context
		// timing out, and a proposal getting applied immediately after. That
		// would cause assert to fail. So, don't assert.
		return
	}
	x.AssertTrue(pd.index != 0)
	if err != nil {
		pd.err = err
	}
	delete(p.all, key)
	pd.ch <- pd.err
}

type lockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func (r *lockedSource) Int63() int64 {
	r.lk.Lock()
	defer r.lk.Unlock()
	return r.src.Int63()
}

func (r *lockedSource) Seed(seed int64) {
	r.lk.Lock()
	defer r.lk.Unlock()
	r.src.Seed(seed)
}

type Checksum struct {
	needVerify bool //leader use to trigger checksum verify
	index      uint64
	checksum   string
}

type RaftNode struct {
	rwMutex       sync.RWMutex
	MetaCli       MetaClient
	leases        *meta.Leases
	ID            uint64
	Val           string
	Node          raft.Node
	RaftConfState *raftpb.ConfState
	RaftCtx       *internal.RaftContext
	RaftConfig    *raft.Config
	Config        Config
	Storage       *raftwal.DiskStorage
	PeersAddr     map[uint64]string
	Done          chan struct{}
	props         *proposals
	rand          *rand.Rand
	applyCh       chan *internal.EntryWrapper
	appliedIndex  uint64

	lastChecksum Checksum

	applyWait wait.WaitTime
	// a chan to send out readState
	readStateC chan raft.ReadState

	leaderChanged   chan struct{}
	leaderChangedMu sync.RWMutex

	Logger *zap.Logger
}

func NewRaftNode(c *raft.Config, config Config) *RaftNode {
	walDir := config.WalDir
	x.Checkf(os.MkdirAll(walDir, 0700), "Error while creating WAL dir.")
	kvOpt := badger.DefaultOptions
	kvOpt.SyncWrites = true
	kvOpt.Dir = config.WalDir
	kvOpt.ValueDir = config.WalDir
	kvOpt.TableLoadingMode = options.MemoryMap

	walStore, err := badger.Open(kvOpt)
	x.Checkf(err, "Error while creating badger KV WAL store")

	if c.ID == 0 {
		id, err := raftwal.RaftId(walStore)
		x.Check(err)
		c.ID = id
	}

	rc := &internal.RaftContext{
		Addr: config.MyAddr,
		ID:   c.ID,
	}

	//storage := raft.NewMemoryStorage()
	storage := raftwal.Init(walStore, c.ID, 0)
	c.Storage = storage
	return &RaftNode{
		Logger:        logger.New(os.Stderr).With(zap.String("raftmeta", "RaftNode")),
		leases:        meta.NewLeases(meta.DefaultLeaseDuration),
		ID:            c.ID,
		RaftConfig:    c,
		Config:        config,
		RaftCtx:       rc,
		Storage:       storage,
		PeersAddr:     make(map[uint64]string),
		Done:          make(chan struct{}),
		props:         newProposals(),
		rand:          rand.New(&lockedSource{src: rand.NewSource(time.Now().UnixNano())}),
		applyCh:       make(chan *internal.EntryWrapper, config.NumPendingProposals),
		readStateC:    make(chan raft.ReadState, 1),
		applyWait:     wait.NewTimeList(),
		leaderChanged: make(chan struct{}),
		lastChecksum:  Checksum{needVerify: false, index: 0, checksum: ""},
	}
}

// uniqueKey is meant to be unique across all the replicas.
func (s *RaftNode) uniqueKey() string {
	return fmt.Sprintf("%02d-%d", s.ID, s.rand.Uint64())
}

func (s *RaftNode) setAppliedIndex(v uint64) {
	atomic.StoreUint64(&s.appliedIndex, v)
	s.applyWait.Trigger(v)
}

func (s *RaftNode) AppliedIndex() uint64 {
	return atomic.LoadUint64(&s.appliedIndex)
}

func (s *RaftNode) ReadIndex(ctx context.Context, rctx []byte) error {
	return s.Node.ReadIndex(ctx, rctx)
}

func (s *RaftNode) WaitIndex(index uint64) <-chan struct{} {
	return s.applyWait.Wait(index)
}

func (s *RaftNode) ReadState() <-chan raft.ReadState {
	return s.readStateC
}

func (s *RaftNode) leaderChangedNotify() <-chan struct{} {
	s.leaderChangedMu.RLock()
	s.leaderChangedMu.RUnlock()
	return s.leaderChanged
}

func (s *RaftNode) restoreFromSnapshot() {
	s.Logger.Info("restore from snapshot")
	sp, err := s.Storage.Snapshot()
	x.Checkf(err, "Unable to get existing snapshot")

	if raft.IsEmptySnap(sp) {
		s.Logger.Info("empty snapshot. ignore")
		return
	}
	s.SetConfState(&sp.Metadata.ConfState)
	s.setAppliedIndex(sp.Metadata.Index)

	var sndata internal.SnapshotData
	err = json.Unmarshal(sp.Data, &sndata)
	x.Checkf(err, "internal.SnapshotData UnmarshalBinary fail")

	s.SetPeers(sndata.PeersAddr)

	metaData := &imeta.Data{}
	err = metaData.UnmarshalBinary(sndata.Data)
	x.Checkf(err, "meta data UnmarshalBinary fail")

	err = s.MetaCli.ReplaceData(metaData)
	x.Checkf(err, "meta cli ReplaceData fail")
}

func (s *RaftNode) InitAndStartNode(peers []raft.Peer) {
	idx, restart, err := s.PastLife()
	x.Check(err)
	s.setAppliedIndex(idx)

	if restart {
		s.Logger.Info("Restarting node")
		s.restoreFromSnapshot()
		s.Node = raft.RestartNode(s.RaftConfig)
	} else {
		s.Logger.Info("Starting node")
		if len(peers) == 0 {
			data, err := json.Marshal(s.RaftCtx)
			x.Check(err)
			s.Node = raft.StartNode(s.RaftConfig, []raft.Peer{{ID: s.ID, Context: data}})
		} else {
			err := s.joinPeers(peers)
			x.Checkf(err, "join peers fail")
			s.Logger.Info("join peers success")
			s.Node = raft.StartNode(s.RaftConfig, nil)
		}
	}
}

func (s *RaftNode) joinPeers(peers []raft.Peer) error {
	x.AssertTrue(len(peers) > 0)
	addr := ""
	for _, p := range peers {
		rc := internal.RaftContext{}
		x.Check(json.Unmarshal(p.Context, &rc))
		addr = rc.Addr
		s.SetPeer(rc.ID, rc.Addr)
	}

	url := fmt.Sprintf("http://%s/update_cluster?op=add", addr)
	data, err := json.Marshal(s.RaftCtx)
	x.Checkf(err, "encode internal.RaftContext fail")
	return Request(url, data)
}

func (s *RaftNode) Run() {
	go s.processApplyCh()

	snapshotTicker := time.NewTicker(time.Duration(s.Config.SnapshotIntervalSec) * time.Second)
	defer snapshotTicker.Stop()

	checkSumTicker := time.NewTicker(time.Duration(s.Config.ChecksumIntervalSec) * time.Second)
	defer checkSumTicker.Stop()

	t := time.NewTicker(time.Duration(s.Config.TickTimeMs) * time.Millisecond)
	defer t.Stop()

	var leader uint64

	for {
		select {
		case <-snapshotTicker.C:
			if leader == s.ID {
				go func() {
					err := s.trigerSnapshot(1000)
					s.Logger.Error("calculateSnapshot fail", zap.Error(err))
				}()
			}
		case <-checkSumTicker.C:
			if leader == s.ID {
				s.triggerChecksum()
			}
		case <-t.C:
			s.Node.Tick()
		case rd := <-s.Node.Ready():
			if len(rd.ReadStates) != 0 {
				select {
				case s.readStateC <- rd.ReadStates[len(rd.ReadStates)-1]:
				case <-time.After(time.Second):
					s.Logger.Info("timed out sending read state")
				}
			}

			if rd.SoftState != nil {
				if rd.SoftState.Lead != raft.None && leader != rd.SoftState.Lead {
					//leaderChanges.Inc()
					s.leaderChangedMu.Lock()
					lc := s.leaderChanged
					s.leaderChanged = make(chan struct{})
					close(lc)
					s.leaderChangedMu.Unlock()
					if leader == s.ID {
						s.lastChecksum.needVerify = false
					}
				}
				leader = rd.SoftState.Lead
			}

			if leader == s.ID {
				// Leader can send messages in parallel with writing to disk.
				s.send(rd.Messages)
			}

			x.Checkf(s.saveToStorage(rd.HardState, rd.Entries, rd.Snapshot), "terrible! cannot save to storage.")

			if !raft.IsEmptySnap(rd.Snapshot) {
				s.Logger.Info("recv raft snapshot")
				ew := &internal.EntryWrapper{Restore: true}
				s.applyCh <- ew
			}
			for _, entry := range rd.CommittedEntries {
				s.Logger.Info("process entry", zap.Uint64("term", entry.Term), zap.Uint64("index", entry.Index), zap.String("type", entry.Type.String()))
				ew := &internal.EntryWrapper{Entry: entry, Restore: false}
				s.applyCh <- ew
			}

			if s.ID != leader {
				s.send(rd.Messages)
			}

			s.Node.Advance()
		case <-s.Done:
			return
		}
	}
}

//TODO:optimize
func (s *RaftNode) triggerChecksum() {
	s.Logger.Info("trigger check sum")

	if s.lastChecksum.needVerify {
		go func() {
			var verify internal.VerifyChecksum
			verify.Index = s.lastChecksum.index
			verify.Checksum = s.lastChecksum.checksum
			verify.NodeID = s.ID
			data, err := json.Marshal(&verify)
			x.Check(err)

			proposal := &internal.Proposal{
				Type: internal.VerifyChecksumMsg,
				Data: data,
			}
			err = s.ProposeAndWait(context.Background(), proposal, nil)
			if err != nil {
				s.Logger.Error("s.ProposeAndWait fail", zap.Error(err))
			}
		}()
	} else {
		go func() {
			proposal := &internal.Proposal{
				Type: internal.CreateChecksumMsg,
			}
			err := s.ProposeAndWait(context.Background(), proposal, nil)
			if err != nil {
				s.Logger.Error("s.ProposeAndWait fail", zap.Error(err))
			}
		}()
	}
}

func (s *RaftNode) trigerSnapshot(keepN int) error {
	s.Logger.Info("trigerSnapshot")
	var sn internal.CreateSnapshot
	//_, err = s.Storage.LastIndex()
	//x.Check(err)
	data, err := json.Marshal(sn)
	x.Check(err)
	proposal := &internal.Proposal{
		Type: internal.SnapShot,
		Data: data,
	}

	return s.ProposeAndWait(context.Background(), proposal, nil)
}

func (s *RaftNode) processApplyCh() {
	for ew := range s.applyCh {
		if ew.Restore {
			s.restoreFromSnapshot()
			continue
		}

		e := ew.Entry
		appliedIndex := s.AppliedIndex()
		if e.Index <= appliedIndex {
			s.Logger.Info("ignored old index", zap.Uint64("index", e.Index), zap.Uint64("applied:", appliedIndex))
			continue
		}
		x.AssertTruef(appliedIndex == e.Index-1, fmt.Sprintf("sync error happend. index:%d, applied:%d", e.Index, appliedIndex))

		if e.Type == raftpb.EntryConfChange {
			s.applyConfChange(&e)
		} else if len(e.Data) == 0 {
			s.Logger.Info("empty entry. ignored")
		} else {
			proposal := &internal.Proposal{}
			if err := json.Unmarshal(e.Data, &proposal); err != nil {
				x.Fatalf("Unable to unmarshal proposal: %v %q\n", err, e.Data)
			}
			err := s.applyCommitted(proposal, e.Index)
			s.Logger.Info("Applied proposal", zap.String("key", proposal.Key), zap.Uint64("index", e.Index), zap.Error(err))

			s.props.Done(proposal.Key, err)
		}
		s.setAppliedIndex(e.Index)
	}
}

func (s *RaftNode) applyConfChange(e *raftpb.Entry) {
	var cc raftpb.ConfChange
	cc.Unmarshal(e.Data)
	s.Logger.Info(fmt.Sprintf("conf change: %+v", cc))

	if cc.Type == raftpb.ConfChangeRemoveNode {
		s.DeletePeer(cc.NodeID)
	} else if len(cc.Context) > 0 {
		var rc internal.RaftContext
		x.Check(json.Unmarshal(cc.Context, &rc))
		s.SetPeer(rc.ID, rc.Addr)
	}
	cs := s.Node.ApplyConfChange(cc)
	s.SetConfState(cs)
}

func (s *RaftNode) SetConfState(cs *raftpb.ConfState) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.Logger.Info(fmt.Sprintf("Setting conf state to %+v", cs))
	s.RaftConfState = cs
}

func (s *RaftNode) ConfState() *raftpb.ConfState {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.RaftConfState
}

func (s *RaftNode) SetPeers(peers map[uint64]string) {
	s.Logger.Info(fmt.Sprintf("SetPeers:%+v", peers))
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.PeersAddr = peers
}

func (s *RaftNode) SetPeer(id uint64, addr string) {
	x.AssertTruef(addr != "", "SetPeer for peer %d has empty addr.", id)
	s.Logger.Info("SetPeer", zap.Uint64("ID:", id), zap.String("addr", addr))
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.PeersAddr[id] = addr
}

func (s *RaftNode) DeletePeer(id uint64) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	delete(s.PeersAddr, id)
}

func (s *RaftNode) Peer(id uint64) (string, bool) {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	addr, ok := s.PeersAddr[id]
	return addr, ok
}

func (s *RaftNode) ClonePeers() map[uint64]string {
	peers := make(map[uint64]string)
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	for k, v := range s.PeersAddr {
		peers[k] = v
	}
	return peers
}

func (s *RaftNode) saveToStorage(h raftpb.HardState, es []raftpb.Entry, sn raftpb.Snapshot) error {
	return s.Storage.Save(h, es, sn)
}

func (s *RaftNode) send(messages []raftpb.Message) {
	for _, msg := range messages {
		data, err := msg.Marshal()
		if err != nil {
			panic(err)
		}

		addr, ok := s.Peer(msg.To)
		if !ok {
			s.Logger.Warn("peer not find", zap.Uint64("peer", msg.To))
			continue
		}
		url := fmt.Sprintf("http://%s/message", addr)
		err = Request(url, data)
		if err != nil {
			s.Logger.Error("Request fail:", zap.Error(err), zap.String("url", url))
		}
	}
}

func (s *RaftNode) RecvRaftRPC(ctx context.Context, m raftpb.Message) error {
	return s.Node.Step(ctx, m)
}

func (s *RaftNode) Propose(ctx context.Context, data []byte) error {
	return s.Node.Propose(ctx, data)
}

func (s *RaftNode) ProposeConfChange(ctx context.Context, cc raftpb.ConfChange) error {
	return s.Node.ProposeConfChange(ctx, cc)
}

func (s *RaftNode) ProposeAndWait(ctx context.Context, proposal *internal.Proposal, retData interface{}) error {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	che := make(chan error, 1)
	pctx := &proposalCtx{
		ch:      che,
		ctx:     cctx,
		retData: retData,
	}
	key := s.uniqueKey()
	x.AssertTruef(s.props.Store(key, pctx), "Found existing proposal with key: [%v]", key)
	defer s.props.Delete(key)

	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("Proposing data with key: %s", key) //TODO: what's the output?
	}

	proposal.Key = key
	data, err := json.Marshal(proposal)
	x.Check(err)

	if err = s.Propose(cctx, data); err != nil {
		return x.Wrapf(err, "While proposing")
	}

	if tr, ok := trace.FromContext(ctx); ok {
		tr.LazyPrintf("Waiting for the proposal.")
	}

	select {
	case err = <-che:
		// We arrived here by a call to n.props.Done().
		if tr, ok := trace.FromContext(ctx); ok {
			tr.LazyPrintf("Done with error: %v", err)
		}
		return err
	case <-ctx.Done():
		if tr, ok := trace.FromContext(ctx); ok {
			tr.LazyPrintf("External context timed out with error: %v.", ctx.Err())
		}
		return ctx.Err()
	case <-cctx.Done():
		if tr, ok := trace.FromContext(ctx); ok {
			tr.LazyPrintf("Internal context timed out with error: %v. Retrying...", cctx.Err())
		}
		return errInternalRetry
	}

	return nil
}
func (s *RaftNode) PastLife() (idx uint64, restart bool, rerr error) {
	var sp raftpb.Snapshot
	sp, rerr = s.Storage.Snapshot()
	if rerr != nil {
		return
	}
	if !raft.IsEmptySnap(sp) {
		s.Logger.Info(fmt.Sprintf("Found Snapshot, Metadata: %+v", sp.Metadata))
		restart = true
		idx = sp.Metadata.Index
	}

	var hd raftpb.HardState
	hd, rerr = s.Storage.HardState()
	if rerr != nil {
		return
	}
	if !raft.IsEmptyHardState(hd) {
		s.Logger.Info(fmt.Sprintf("Found hardstate: %+v", hd))
		restart = true
	}

	var num int
	num, rerr = s.Storage.NumEntries()
	if rerr != nil {
		return
	}
	s.Logger.Info(fmt.Sprintf("found %d entries", num))
	// We'll always have at least one entry.
	if num > 1 {
		restart = true
	}
	return
}

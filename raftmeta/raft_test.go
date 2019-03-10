package raftmeta_test

import (
	"testing"
	"fmt"
    "os"

	"github.com/coreos/etcd/raft"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/influxdata/influxdb/logger"

	"github.com/angopher/chronus/raftmeta"
	"github.com/angopher/chronus/x"
	"github.com/angopher/chronus/raftmeta/internal"
	imeta "github.com/angopher/chronus/services/meta"
)

type applyData struct {
	proposal *internal.Proposal
	index    uint64
}

var s1 *raftmeta.MetaService
var s2 *raftmeta.MetaService
var s3 *raftmeta.MetaService

func TestMain(t *testing.T) {
	s1 = OpenOneService(1, []raftmeta.Peer{})
	s2 = OpenOneService(2, []raftmeta.Peer{
		{Addr: s1.Node.RaftCtx.Addr, RaftId: s1.Node.RaftCtx.ID},
	})
	s3 = OpenOneService(3, []raftmeta.Peer{
		{Addr: s1.Node.RaftCtx.Addr, RaftId: s1.Node.RaftCtx.ID},
		{Addr: s2.Node.RaftCtx.Addr, RaftId: s2.Node.RaftCtx.ID},
	})
}

func OpenOneService(id uint64, peers []raftmeta.Peer) *raftmeta.MetaService {
	c := raftmeta.NewConfig()
	c.RaftId = id
	c.WalDir = fmt.Sprintf("/tmp/.wal%d", id)
	c.MyAddr = fmt.Sprintf("127.0.0.1:%d", 2347+id-1)
	c.Peers = peers
	t := &fakeTransport{}
	t.ch = make(chan *applyData, 10000)

	return startService(c, t, func(proposal *internal.Proposal, index uint64) {
		data := &applyData{
			proposal: proposal,
			index:    index,
		}
		t.ch <- data
	})
}

func startService(config raftmeta.Config, t *fakeTransport, cb func(proposal *internal.Proposal, index uint64)) *raftmeta.MetaService {
	metaCli := imeta.NewClient(&meta.Config{
		RetentionAutoCreate: config.RetentionAutoCreate,
		LoggingEnabled:      true,
	})
	err := metaCli.Open()
	x.Check(err)
	log := logger.New(os.Stderr)

	node := raftmeta.NewRaftNode(config)
	node.MetaCli = metaCli
	node.ApplyCallBack = cb
	node.WithLogger(log)

	node.Transport = t
	node.InitAndStartNode()
	go node.Run()

	linearRead := raftmeta.NewLinearizabler(node)
	go linearRead.ReadLoop()

	service := raftmeta.NewMetaService(metaCli, node, linearRead)
	service.WithLogger(log)
	go service.Start(config.MyAddr)
	return service
}

type fakeTransport struct {
	ch            chan *applyData
	SetPeersFn    func(peers map[uint64]string)
	SetPeerFn     func(id uint64, addr string)
	DeletePeerFn  func(id uint64)
	PeerFn        func(id uint64) (string, bool)
	ClonePeersFn  func() map[uint64]string
	SendMessageFn func(messages []raftpb.Message)
	RecvMessageFn func(message raftpb.Message)
	JoinClusterFn func(ctx *internal.RaftContext, peers []raft.Peer) error
}

func (f *fakeTransport) SetPeers(peers map[uint64]string) {
	f.SetPeersFn(peers)
}

func (f *fakeTransport) SetPeer(id uint64, addr string) {
	f.SetPeerFn(id, addr)
}

func (f *fakeTransport) DeletePeer(id uint64) {
	f.DeletePeerFn(id)
}

func (f *fakeTransport) Peer(id uint64) (string, bool) {
	return f.PeerFn(id)
}

func (f *fakeTransport) ClonePeers() map[uint64]string {
	return f.ClonePeersFn()
}

func (f *fakeTransport) SendMessage(messages []raftpb.Message) {
	f.SendMessageFn(messages)
}

func (f *fakeTransport) RecvMessage(message raftpb.Message) {
	f.RecvMessageFn(message)
}

func (f *fakeTransport) JoinCluster(ctx *internal.RaftContext, peers []raft.Peer) error {
	return f.JoinClusterFn(ctx, peers)
}

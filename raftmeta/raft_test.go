package raftmeta_test

import (
	"testing"
)

type applyData struct {
	proposal *internal.Proposal
	index uint64
}

func TestLinearizability(t *testing.T) {
	c1 := raftmeta.NewConfig()
	c1.RaftId = 1
	c1.WalDir = "/tmp/.wal1"
	c1.MyAddr = "127.0.0.1:2347"
	t1 := raftmeta.NewTransport()
	ch1 := make(chan *applyData, 10000)

	go startService(c1, t1, func(proposal *internal.Proposal, index uint64) {
		data := &applyData {
			proposal: proposal,
			index: index,
		}
		ch1 <- data
	})


	c2 := raftmeta.NewConfig()
	c2.RaftId = 2
	c2.WalDir = "/tmp/.wal2"
	c2.MyAddr = "127.0.0.1:2348"
	t2 := raftmeta.NewTransport()
	ch2 := make(chan *applyData, 10000)

	go startService(config, t2, func(proposal *internal.Proposal, index uint64) {
		data := &applyData {
			proposal: proposal,
			index: index,
		}
		ch2 <- data
	})

	c3 := raftmeta.NewConfig()
	c3.RaftId = 3
	c3.WalDir = "/tmp/.wal3"
	c3.MyAddr = "127.0.0.1:2349"
	t3 := raftmeta.NewTransport()
	ch3 := make(chan *applyData, 10000)

	go startService(config, t3, func(proposal *internal.Proposal, index uint64) {
		data := &applyData {
			proposal: proposal,
			index: index,
		}
		ch3 <- data
	})
}

func startService(config rafmeta.Config, t *raftmeta.Transport, cb func(proposal *internal.Proposal, index uint64)) {
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

	t.Node = node
	t.WithLogger(log)

	node.Transport = t
	node.InitAndStartNode()
	go node.Run()

	linearRead := raftmeta.NewLinearizabler(node)
	go linearRead.ReadLoop()

	service := raftmeta.NewMetaService(metaCli, node, linearRead)
	service.WithLogger(log)
	service.Start(config.MyAddr)
}

type fakeTransport struct {
	SetPeersFn func(peers map[uint64]string)
	SetPeerFn func(id uint64, addr string)
	DeletePeerFn func(id uint64)
	PeerFn func(id uint64) (string, bool)
	ClonePeersFn func() map[uint64]string
	SendMessageFn func(messages []raftpb.Message)
	RecvMessageFn func(message raftpb.Message)
	JoinClusterFn func (ctx *internal.RaftContext, peers []raft.Peer) error
}

func (f *fakeTransport) SetPeers(peers map[uint64]string) {
	return f.SetPeersFn(peers)
}

func (f *fakeTransport) SetPeer(id uint64, addr string) {
	return f.SetPeerFn(id, addr)
}

func (f *fakeTransport) DeletePeer(id uint64) {
	return f.DeletePeerFn(id)
}

func (f *fakeTransport) Peer(id uint64) (string, bool) {
	return f.PeerFn(id)
}

func (f *fakeTransport) ClonePeers() map[uint64]string {
	return f.ClonePeersFn()
}

func (f *fakeTransport) SendMessage(messages []raftpb.Message) {
	return f.SendMessageFn(messages)
}

func (f *fakeTransport) RecvMessage(message raftpb.Message) {
	return f.RecvMessageFn(message)
}

func (f *fakeTransport) JoinCluster(ctx *internal.RaftContext, peers []raft.Peer) error {
	return f.JoinClusterFn(ctx, peers)
}


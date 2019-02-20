package raftmeta

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/angopher/chronus/raftmeta/internal"
	"github.com/angopher/chronus/x"
	"github.com/coreos/etcd/raft/raftpb"
	"go.uber.org/zap"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"time"
)

func (s *RaftNode) HandleUpdateCluster(w http.ResponseWriter, r *http.Request) {
	fmt.Println("recv join cluster")
	resp := &CommonResp{}
	resp.RetCode = -1
	resp.RetMsg = "fail"
	defer WriteResp(w, &resp)

	var err error
	typ := raftpb.ConfChangeAddNode
	data := []byte{}
	var nodeId uint64
	op := r.FormValue("op")
	if op == "add" || op == "update" {
		data, err = ioutil.ReadAll(r.Body)
		if err != nil {
			resp.RetMsg = err.Error()
			return
		}
		var rc internal.RaftContext
		err = json.Unmarshal(data, &rc)
		x.Check(err)
		nodeId = rc.ID
	} else if op == "remove" {
		typ = raftpb.ConfChangeRemoveNode
		nodeId, err = strconv.ParseUint(r.FormValue("node_id"), 10, 64)
		fmt.Printf("%+v\n", r.Form)
		x.Check(err)
	} else {
		resp.RetMsg = fmt.Sprintf("unkown op:%s", op)
		return
	}

	if nodeId == 0 {
		resp.RetMsg = "invalid node id 0"
		return
	}

	cc := raftpb.ConfChange{
		ID:      s.ID,
		Type:    typ,
		NodeID:  nodeId,
		Context: data,
	}
	err = s.ProposeConfChange(context.Background(), cc)
	if err != nil {
		resp.RetMsg = err.Error()
		return
	}
	resp.RetCode = 0
	resp.RetMsg = "ok"
}

func (s *RaftNode) HandleMessage(w http.ResponseWriter, r *http.Request) {
	resp := &CommonResp{}
	resp.RetCode = 0
	resp.RetMsg = "ok"
	defer WriteResp(w, &resp)

	data, err := ioutil.ReadAll(r.Body)
	x.Check(err)

	var msg raftpb.Message
	err = msg.Unmarshal(data)
	x.Check(err)
	if msg.Type != raftpb.MsgHeartbeat && msg.Type != raftpb.MsgHeartbeatResp {
		s.Logger.Info("recv message", zap.String("type:", msg.Type.String()))
	}
	s.RecvRaftRPC(context.Background(), msg)
}

func Request(url string, data []byte) error {
	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")

	client := http.Client{
		Transport: &http.Transport{
			Dial: func(netw, addr string) (net.Conn, error) {
				deadline := time.Now().Add(10 * time.Second) //TODO: timeout from config
				c, err := net.DialTimeout(netw, addr, time.Second)
				if err != nil {
					return nil, err
				}
				c.SetDeadline(deadline)
				return c, nil
			},
		},
	}

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	resData, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	resp := &CommonResp{RetCode: -1}
	err = json.Unmarshal(resData, resp)
	if resp.RetCode != 0 {
		return fmt.Errorf("fail. err:%s", resp.RetMsg)
	}
	return nil
}

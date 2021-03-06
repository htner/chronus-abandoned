package action

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/angopher/chronus/coordinator"
	"github.com/angopher/chronus/services/controller"
)

func CopyShard(srcAddr, dstAddr, shardID string) error {
	id, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return err
	}

	req := &controller.CopyShardRequest{
		SourceNodeAddr: srcAddr,
		DestNodeAddr:   dstAddr,
		ShardID:        id,
	}

	var resp controller.CopyShardResponse
	respTyp := byte(controller.ResponseCopyShard)
	reqTyp := byte(controller.RequestCopyShard)
	if err := RequestAndWaitResp(dstAddr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func TruncateShards(delay string, addr string) error {
	delaySec, err := strconv.ParseInt(delay, 10, 64)
	if err != nil {
		return err
	}

	req := &controller.TruncateShardRequest{DelaySec: delaySec}
	reqTyp := byte(controller.RequestTruncateShard)

	respTyp := byte(controller.ResponseTruncateShard)
	var resp controller.CopyShardStatusResponse
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func CopyShardStatus(addr string) error {
	var resp controller.CopyShardStatusResponse
	respTyp := byte(controller.ResponseCopyShardStatus)
	reqTyp := byte(controller.RequestCopyShardStatus)
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, struct{}{}, &resp); err != nil {
		return err
	}

	fmt.Printf("%+v\n", resp)
	return nil
}

func KillCopyShard(srcAddr, dstAddr, shardID string) error {
	id, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return err
	}

	req := &controller.KillCopyShardRequest{
		SourceNodeAddr: srcAddr,
		DestNodeAddr:   dstAddr,
		ShardID:        id,
	}

	var resp controller.KillCopyShardResponse
	respTyp := byte(controller.ResponseKillCopyShard)
	reqTyp := byte(controller.RequestKillCopyShard)
	if err := RequestAndWaitResp(dstAddr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func RemoveShard(addr, shardID string) error {
	id, err := strconv.ParseUint(shardID, 10, 64)
	if err != nil {
		return err
	}

	req := &controller.RemoveShardRequest{
		DataNodeAddr: addr,
		ShardID:      id,
	}

	var resp controller.RemoveShardResponse
	respTyp := byte(controller.ResponseRemoveShard)
	reqTyp := byte(controller.RequestRemoveShard)
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func RemoveDataNode(addr string) error {
	req := &controller.RemoveDataNodeRequest{
		DataNodeAddr: addr,
	}

	var resp controller.RemoveDataNodeResponse
	respTyp := byte(controller.ResponseRemoveDataNode)
	reqTyp := byte(controller.RequestRemoveDataNode)
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func ShowDataNodes(addr string) error {
	var resp controller.ShowDataNodesResponse
	respTyp := byte(controller.ResponseShowDataNodes)
	reqTyp := byte(controller.RequestShowDataNodes)
	if err := RequestAndWaitResp(addr, reqTyp, respTyp, struct{}{}, &resp); err != nil {
		return err
	}

	fmt.Printf("msg:%s, data nodes:%+v\n", resp.Msg, resp.DataNodes)
	return nil
}

func RequestAndWaitResp(addr string, reqTyp, respTyp byte, req interface{}, resp interface{}) error {
	conn, err := Dial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	buf, _ := json.Marshal(req)
	if err := coordinator.WriteTLV(conn, reqTyp, buf); err != nil {
		return err
	}

	return DecodeTLV(conn, respTyp, resp)
}

func DecodeTLV(r io.Reader, expTyp byte, v interface{}) error {
	typ, err := coordinator.ReadType(r)
	if err != nil {
		return err
	}
	if expTyp != typ {
		return fmt.Errorf("invalid type, exp: %s, got: %s", expTyp, typ)
	}

	buf, err := coordinator.ReadLV(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(buf, v); err != nil {
		return err
	}
	return nil
}

func Dial(addr string) (net.Conn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	conn.SetDeadline(time.Now().Add(time.Second))

	// Write the cluster multiplexing header byte
	if _, err := conn.Write([]byte{controller.MuxHeader}); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

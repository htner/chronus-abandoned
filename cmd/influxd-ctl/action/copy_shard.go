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

	conn, err := Dial(dstAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	req := &controller.CopyShardRequest{
		SourceNodeAddr: srcAddr,
		DestNodeAddr:   dstAddr,
		ShardID:        id,
	}

	var resp controller.CopyShardResponse
	respTyp := byte(controller.ResponseCopyShard)
	reqTyp := byte(controller.RequestCopyShard)
	if err := RequestAndWaitResp(conn, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func TruncateShards(delaySec int64, host string) error {
	conn, err := Dial(host)
	if err != nil {
		return err
	}
	defer conn.Close()

	req := &controller.TruncateShardRequest{DelaySec: delaySec}
	reqTyp := byte(controller.RequestTruncateShard)

	respTyp := byte(controller.ResponseTruncateShard)
	var resp controller.CopyShardStatusResponse
	if err := RequestAndWaitResp(conn, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func CopyShardStatus(host string) error {
	conn, err := Dial(host)
	if err != nil {
		return err
	}
	defer conn.Close()

	var resp controller.CopyShardStatusResponse
	respTyp := byte(controller.ResponseCopyShardStatus)
	reqTyp := byte(controller.RequestCopyShardStatus)
	if err := RequestAndWaitResp(conn, reqTyp, respTyp, struct{}{}, &resp); err != nil {
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

	conn, err := Dial(dstAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	req := &controller.KillCopyShardRequest{
		SourceNodeAddr: srcAddr,
		DestNodeAddr:   dstAddr,
		ShardID:        id,
	}

	var resp controller.KillCopyShardResponse
	respTyp := byte(controller.ResponseKillCopyShard)
	reqTyp := byte(controller.RequestKillCopyShard)
	if err := RequestAndWaitResp(conn, reqTyp, respTyp, req, &resp); err != nil {
		return err
	}

	fmt.Println(resp.Msg)
	return nil
}

func RequestAndWaitResp(r io.ReadWriter, reqTyp, respTyp byte, req interface{}, resp interface{}) error {
	buf, _ := json.Marshal(req)
	if err := coordinator.WriteTLV(r, reqTyp, buf); err != nil {
		return err
	}

	return DecodeTLV(r, respTyp, resp)
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

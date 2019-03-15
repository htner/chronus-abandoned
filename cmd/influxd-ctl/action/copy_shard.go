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

	buf, _ := json.Marshal(req)
	err = coordinator.WriteTLV(conn, byte(controller.RequestCopyShard), buf)
	if err != nil {
		return err
	}

	var resp controller.CopyShardResponse
	if err := DecodeTLV(conn, byte(controller.ResponseCopyShard), &resp); err != nil {
		return err
	}

	if resp.Code != 0 {
		fmt.Println("failed:", resp.Msg)
	}

	return nil
}

func TruncateShards(delaySec int) error {
	return nil
}

func CopyShardStatus() error {
	return nil
}

func KillCopyShard(srcAddr, dstAddr, shardID string) error {
	return nil
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

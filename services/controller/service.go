//Package ctl provides influxd-ctl service
package controller

import (
	"encoding/json"
	"github.com/angopher/chronus/coordinator"
	"github.com/influxdata/influxdb"
	"go.uber.org/zap"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	// MuxHeader is the header byte used for the TCP muxer.
	MuxHeader = 4
)

type Service struct {
	wg sync.WaitGroup

	Node *influxdb.Node

	MetaClient interface {
		TruncateShardGroups(t time.Time) error
	}

	Listener net.Listener
	Logger   *zap.Logger

	ShardCarrier interface {
		CopyShard(sourceAddr string, shardId uint64) error
		Query() ([]uint64, []string)
		Kill(shardId uint64)
	}
}

// NewService returns a new instance of Service.
func NewService() *Service {
	return &Service{
		Logger: zap.NewNop(),
	}
}

// Open starts the service.
func (s *Service) Open() error {
	s.Logger.Info("Starting snapshot service")

	s.wg.Add(1)
	go s.serve()
	return nil
}

// Close implements the Service interface.
func (s *Service) Close() error {
	if s.Listener != nil {
		if err := s.Listener.Close(); err != nil {
			return err
		}
	}
	s.wg.Wait()
	return nil
}

// WithLogger sets the logger on the service.
func (s *Service) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "ctl"))
}

// serve serves snapshot requests from the listener.
func (s *Service) serve() {
	defer s.wg.Done()

	for {
		// Wait for next connection.
		conn, err := s.Listener.Accept()
		if err != nil && strings.Contains(err.Error(), "connection closed") {
			s.Logger.Info("Listener closed")
			return
		} else if err != nil {
			s.Logger.Info("Error accepting snapshot request", zap.Error(err))
			continue
		}

		// Handle connection in separate goroutine.
		s.wg.Add(1)
		go func(conn net.Conn) {
			defer s.wg.Done()
			defer conn.Close()
			if err := s.handleConn(conn); err != nil {
				s.Logger.Info(err.Error())
			}
		}(conn)
	}
}

// handleConn processes conn. This is run in a separate goroutine.
func (s *Service) handleConn(conn net.Conn) error {
	var typ [1]byte
	_, err := conn.Read(typ[:])
	if err != nil {
		return err
	}

	switch RequestType(typ[0]) {
	case RequestTruncateShard:
		err = s.handleTruncateShard(conn)
		s.truncateShardResponse(conn, err)
	case RequestCopyShard:
		err = s.handleCopyShard(conn)
		s.copyShardResponse(conn, err)
	case RequestCopyShardStatus:
		tasks := s.handleCopyShardStatus(conn)
		s.copyShardStatusResponse(conn, tasks)
	case RequestKillCopyShard:
		err = s.handleKillCopyShard(conn)
		s.killCopyShardResponse(conn, err)
	}

	return nil
}

func (s *Service) handleTruncateShard(conn net.Conn) error {
	buf, err := coordinator.ReadLV(conn)
	if err != nil {
		s.Logger.Error("unable to read length-value", zap.Error(err))
		return err
	}

	var req TruncateShardRequest
	if err := json.Unmarshal(buf, &req); err != nil {
		return err
	}

	t := time.Now().Add(time.Duration(req.DelaySec) * time.Second)
	if err := s.MetaClient.TruncateShardGroups(t); err != nil {
		return err
	}
	return nil
}

func (s *Service) truncateShardResponse(w io.Writer, e error) {
	// Build response.
	var resp TruncateShardResponse
	if e != nil {
		resp.Code = 1
		resp.Msg = e.Error()
	} else {
		resp.Code = 0
		resp.Msg = "ok"
	}

	// Marshal response to binary.
	buf, err := json.Marshal(&resp)
	if err != nil {
		s.Logger.Error("error marshalling truncate shard response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(ResponseTruncateShard), buf); err != nil {
		s.Logger.Error("truncate shard WriteTLV fail", zap.Error(err))
	}
}

func (s *Service) handleCopyShard(conn net.Conn) error {
	buf, err := coordinator.ReadLV(conn)
	if err != nil {
		s.Logger.Error("unable to read length-value", zap.Error(err))
		return err
	}

	var req CopyShardRequest
	if err := json.Unmarshal(buf, &req); err != nil {
		return err
	}

	s.ShardCarrier.CopyShard(req.SourceNodeAddr, req.ShardID)
	return nil
}

func (s *Service) copyShardResponse(w io.Writer, e error) {
	// Build response.
	var resp CommonResp
	if e != nil {
		resp.Code = 1
		resp.Msg = e.Error()
	} else {
		resp.Code = 0
		resp.Msg = "ok"
	}

	// Marshal response to binary.
	buf, err := json.Marshal(&resp)
	if err != nil {
		s.Logger.Error("error marshalling copy shard response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(ResponseCopyShard), buf); err != nil {
		s.Logger.Error("copy shard WriteTLV fail", zap.Error(err))
	}
}

func (s *Service) handleCopyShardStatus(conn net.Conn) []CopyShardTask {
	var tasks []CopyShardTask
	shardIds, sources := s.ShardCarrier.Query()
	for i, id := range shardIds {
		t := CopyShardTask{
			ShardID: id,
			Source:  sources[i],
		}
		tasks = append(tasks, t)
	}

	return tasks
}

func (s *Service) copyShardStatusResponse(w io.Writer, tasks []CopyShardTask) {
	// Build response.
	var resp CopyShardStatusResponse
	resp.Code = 0
	resp.Msg = "ok"
	resp.Tasks = tasks

	// Marshal response to binary.
	buf, err := json.Marshal(&resp)
	if err != nil {
		s.Logger.Error("error marshalling show copy shard response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(ResponseCopyShardStatus), buf); err != nil {
		s.Logger.Error("show copy shard WriteTLV fail", zap.Error(err))
	}
}

func (s *Service) handleKillCopyShard(conn net.Conn) error {
	buf, err := coordinator.ReadLV(conn)
	if err != nil {
		s.Logger.Error("unable to read length-value", zap.Error(err))
		return err
	}

	var req KillCopyShardRequest
	if err := json.Unmarshal(buf, &req); err != nil {
		return err
	}

	s.ShardCarrier.Kill(req.ShardID)
	return nil
}

func (s *Service) killCopyShardResponse(w io.Writer, e error) {
	// Build response.
	var resp KillCopyShardResponse
	if e != nil {
		resp.Code = 1
		resp.Msg = e.Error()
	} else {
		resp.Code = 0
		resp.Msg = "ok"
	}

	// Marshal response to binary.
	buf, err := json.Marshal(&resp)
	if err != nil {
		s.Logger.Error("error marshalling show copy shard response", zap.Error(err))
		return
	}

	// Write to connection.
	if err := coordinator.WriteTLV(w, byte(ResponseKillCopyShard), buf); err != nil {
		s.Logger.Error("kill copy shard WriteTLV fail", zap.Error(err))
	}
}

type CommonResp struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type TruncateShardRequest struct {
	DelaySec int64 `json:"delay_sec"`
}
type TruncateShardResponse struct {
	CommonResp
}

type CopyShardRequest struct {
	SourceNodeAddr string `json:"source_node_address"`
	DestNodeAddr   string `json:"dest_node_address"`
	ShardID        uint64 `json:"shard_id"`
}

type CopyShardResponse struct {
	CommonResp
}

type CopyShardTask struct {
	ShardID uint64 `json:"shard_id"`
	Source  string `json:"source"`
}

type CopyShardStatusResponse struct {
	CommonResp
	Tasks []CopyShardTask `json:"tasks"`
}

type KillCopyShardRequest struct {
	SourceNodeAddr string `json:"source_node_address"`
	DestNodeAddr   string `json:"dest_node_address"`
	ShardID        uint64 `json:"shard_id"`
}

type KillCopyShardResponse struct {
	CommonResp
}

// RequestType indicates the typeof ctl request.
type RequestType byte

const (
	// RequestTruncateShard represents a request for truncating shard.
	RequestTruncateShard   RequestType = 1
	RequestCopyShard                   = 2
	RequestCopyShardStatus             = 3
	RequestKillCopyShard               = 4
)

type ResponseType byte

const (
	ResponseTruncateShard   ResponseType = 1
	ResponseCopyShard                    = 2
	ResponseCopyShardStatus              = 3
	ResponseKillCopyShard                = 4
)

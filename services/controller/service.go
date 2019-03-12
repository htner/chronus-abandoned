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

	ShardManager interface {
		CopyShard(sourceAddr string, shardId uint64) error
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
		if err := s.handleTruncateShard(conn); err != nil {
			s.truncateShardResponse(conn, err)
			return err
		}
	case RequestCopyShard:
		return s.handleCopyShard(conn)
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
		s.Logger.Error("write shard WriteTLV fail", zap.Error(err))
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

	s.ShardManager.CopyShard(req.SourceNodeAddr, req.ShardId)
	return nil
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
	ShardId        uint64 `json:"shard_id"`
}
type CopyShardResponse struct {
	CommonResp
}

// RequestType indicates the typeof ctl request.
type RequestType byte

const (
	// RequestTruncateShard represents a request for truncating shard.
	RequestTruncateShard RequestType = iota
	RequestCopyShard
)

type ResponseType byte

const (
	ResponseTruncateShard ResponseType = iota
	ReponseCopyShard
)

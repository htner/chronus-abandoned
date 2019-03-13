package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/cmd/influxd/backup_util"
	tarstream "github.com/influxdata/influxdb/pkg/tar"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/services/snapshotter"
	"github.com/influxdata/influxdb/tcp"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

type shardCarryTask struct {
	shardId uint64
	source  string
	closer  interface {
		Close() error
	}
}

type carryManager struct {
	mutex      sync.Mutex
	tasks      map[uint64]*shardCarryTask
	maxRunning int
}

func NewCarryManager(max int) *carryManager {
	return &carryManager{
		maxRunning: max,
		tasks:      make(map[uint64]*shardCarryTask),
	}
}

func (c *carryManager) Add(task *shardCarryTask) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	_, ok := c.tasks[task.shardId]
	if ok {
		return errors.New("shard task already exists")
	}
	c.tasks[task.shardId] = task
	return nil
}

func (c *carryManager) Remove(id uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	delete(c.tasks, id)
	return
}

func (c *carryManager) Tasks() []*shardCarryTask {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var tasks []*shardCarryTask
	for _, task := range c.tasks {
		tasks = append(tasks, task)
	}
	return tasks
}

func (c *carryManager) Kill(id uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	t, ok := c.tasks[id]
	if !ok {
		return
	}
	t.closer.Close()
	delete(c.tasks, id)
}

type ShardCarrier struct {
	Node    *influxdb.Node
	Logger  *zap.Logger
	manager interface {
		Add(task *shardCarryTask) error
		Remove(id uint64)
		Tasks() []*shardCarryTask
		Kill(id uint64)
	}

	MetaClient interface {
		ShardOwner(shardID uint64) (database, policy string, sgi *meta.ShardGroupInfo)
		AddShardOwner(shardID, nodeID uint64) error
	}
	TSDBStore interface {
		Path() string
		ShardRelativePath(id uint64) (string, error)
		CreateShard(database, retentionPolicy string, shardID uint64, enabled bool) error
		Shard(id uint64) *tsdb.Shard
	}
}

func (s *ShardCarrier) WithLogger(log *zap.Logger) {
	s.Logger = log.With(zap.String("service", "ShardCarrier"))
}

func fileExists(fileName string) bool {
	_, err := os.Stat(fileName)
	return err == nil
}

func (s *ShardCarrier) Query() ([]uint64, []string) {
	var shardIds []uint64
	var sources []string
	tasks := s.manager.Tasks()
	for _, t := range tasks {
		shardIds = append(shardIds, t.shardId)
		sources = append(sources, t.source)
	}
	return shardIds, sources
}

func (s *ShardCarrier) Kill(shardId uint64) {
	s.manager.Kill(shardId)
}

func (s *ShardCarrier) CopyShard(sourceAddr string, shardId uint64) error {
	// 1.检查本地是否已经存在shard
	// 2.检查是否可以进行此shard的copy任务: 任务管理器
	// 3.检查本地是否有残留的脏数据, 并清理掉
	// 4.下载shard备份
	// 5.解压shard数据包
	// 6.创建shard

	db, rp, sgi := s.MetaClient.ShardOwner(shardId)
	if sgi == nil {
		return fmt.Errorf("shard %d not exists", shardId)
	}

	path := filepath.Join(s.TSDBStore.Path(), db, rp, strconv.FormatUint(shardId, 10))
	if fileExists(path) {
		return fmt.Errorf("path:[%s] exists", path)
	}

	copyDir := s.TSDBStore.Path() + "/.copy_shard"
	os.MkdirAll(copyDir, 0755)
	tmpPath := fmt.Sprintf("%s/shard_%d", copyDir, shardId)
	defer os.RemoveAll(tmpPath)

	if !fileExists(tmpPath) {
		req := &snapshotter.Request{
			Type:                  snapshotter.RequestShardBackup,
			BackupDatabase:        db,
			BackupRetentionPolicy: rp,
			ShardID:               shardId,
			//Since:                 cmd.since,
			//ExportStart:           cmd.start,
			//ExportEnd:             cmd.end,
		}
		fmt.Println("----------- tmpPath:", tmpPath)
		if err := s.downloadAndVerify(req, sourceAddr, tmpPath, nil); err != nil {
			return err
		}
	}

	fmt.Println("----------- shard path:", path)
	if _, err := os.Stat(tmpPath); err == nil || os.IsNotExist(err) {
		if err := s.unpackTar(tmpPath, path); err != nil {
			return err
		}
	}

	sh := s.TSDBStore.Shard(shardId)
	if sh == nil {
		if err := s.TSDBStore.CreateShard(db, rp, shardId, true); err != nil {
			return err
		}
	}

	return s.MetaClient.AddShardOwner(shardId, s.Node.ID)
}

func (s *ShardCarrier) downloadAndVerify(req *snapshotter.Request, host, path string, validator func(string) error) error {
	tmppath := path + backup_util.Suffix
	if err := s.download(req, host, tmppath); err != nil {
		os.Remove(tmppath)
		return err
	}

	if validator != nil {
		if err := validator(tmppath); err != nil {
			if rmErr := os.Remove(tmppath); rmErr != nil {
				s.Logger.Sugar().Errorf("Error cleaning up temporary file: %v", rmErr)
			}
			return err
		}
	}

	f, err := os.Stat(tmppath)
	if err != nil {
		return err
	}

	// There was nothing downloaded, don't create an empty backup file.
	if f.Size() == 0 {
		return os.Remove(tmppath)
	}

	// Rename temporary file to final path.
	if err := os.Rename(tmppath, path); err != nil {
		return fmt.Errorf("rename: %s", err)
	}

	return nil
}

// unpackTar will restore a single tar archive to the data dir
func (s *ShardCarrier) unpackTar(tarFile, shardPath string) error {
	s.Logger.Sugar().Infof("Restoring from backup %s\n", shardPath)
	f, err := os.Open(tarFile)
	if err != nil {
		return err
	}
	defer f.Close()

	os.MkdirAll(shardPath, 0755)

	return tarstream.Restore(f, shardPath)
}

func (s *ShardCarrier) download(req *snapshotter.Request, host, path string) error {
	// Create local file to write to.
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("open temp file: %s", err)
	}
	defer f.Close()

	min := 2 * time.Second
	for i := 0; i < 2; i++ {
		if err = func() error {
			// Connect to snapshotter service.
			conn, err := tcp.Dial("tcp", host, snapshotter.MuxHeader)
			if err != nil {
				return err
			}
			defer conn.Close()

			task := &shardCarryTask{
				shardId: req.ShardID,
				source:  host,
				closer:  conn,
			}
			s.manager.Add(task)
			defer s.manager.Remove(task.shardId)

			_, err = conn.Write([]byte{byte(req.Type)})
			if err != nil {
				return err
			}

			// Write the request
			if err := json.NewEncoder(conn).Encode(req); err != nil {
				return fmt.Errorf("encode snapshot request: %s", err)
			}

			// Read snapshot from the connection
			if n, err := io.Copy(f, conn); err != nil || n == 0 {
				return fmt.Errorf("copy backup to file: err=%v, n=%d", err, n)
			}
			return nil
		}(); err == nil {
			break
		} else if err != nil {
			backoff := time.Duration(math.Pow(3.8, float64(i))) * time.Millisecond
			if backoff < min {
				backoff = min
			}
			s.Logger.Sugar().Errorf("Download shard %v failed %s.  Waiting %v and retrying (%d)...\n", req.ShardID, err, backoff, i)
			time.Sleep(backoff)
		}
	}

	return err
}
